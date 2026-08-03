"""The queue itself.

Design in one breath: three Redis lists are the hot / warm / cold lanes. A
worker *claims* a job — atomically popping it from the highest non-empty lane
and parking it in an in-flight set with a deadline — then *acks* it when done.
If a worker dies, the deadline lapses and a reaper puts the job back (or sends
it to the dead-letter queue once it has failed too many times).

Every operation that has to be atomic across multiple keys is a small Lua
script, so correctness doesn't depend on client-side timing or round-trips.

Keys (under a configurable prefix, default ``purser``):

* ``{p}:q:hot`` / ``:warm`` / ``:cold``  — the lanes (lists of job JSON)
* ``{p}:inflight``       — ZSET of ``job id -> deadline`` (claimed, not yet acked)
* ``{p}:inflight:jobs``  — HASH of ``job id -> job JSON``
* ``{p}:dlq``            — list of jobs that exhausted their attempts
* ``{p}:seen``           — SET for dedup on enqueue
"""

from __future__ import annotations

import time
from dataclasses import dataclass
from enum import Enum

import redis

from .job import TIERS, Job

# --- Lua: enqueue (dedup + backpressure + push, atomic) ----------------------
# KEYS: lane, seen        ARGV: job_json, payload, max_depth, dedup(1/0)
# return: 1 enqueued, 0 duplicate, -1 full
_ENQUEUE = """
if tonumber(ARGV[4]) == 1 then
  if redis.call('SADD', KEYS[2], ARGV[2]) == 0 then return 0 end
end
if tonumber(ARGV[3]) > 0 and redis.call('LLEN', KEYS[1]) >= tonumber(ARGV[3]) then
  if tonumber(ARGV[4]) == 1 then redis.call('SREM', KEYS[2], ARGV[2]) end
  return -1
end
redis.call('LPUSH', KEYS[1], ARGV[1])
return 1
"""

# --- Lua: claim (priority pop + register in-flight, atomic) ------------------
# KEYS: hot, warm, cold, inflight, inflight_jobs   ARGV: deadline
# return: job JSON or false
_CLAIM = """
for i = 1, 3 do
  local v = redis.call('RPOP', KEYS[i])
  if v then
    local id = cjson.decode(v)['id']
    redis.call('ZADD', KEYS[4], ARGV[1], id)
    redis.call('HSET', KEYS[5], id, v)
    return v
  end
end
return false
"""

# --- Lua: ack (drop from in-flight) ------------------------------------------
# KEYS: inflight, inflight_jobs   ARGV: id
_ACK = """
redis.call('ZREM', KEYS[1], ARGV[1])
redis.call('HDEL', KEYS[2], ARGV[1])
return 1
"""

# --- Lua: settle one in-flight job (nack) ------------------------------------
# KEYS: inflight, inflight_jobs, hot, warm, cold, dlq
# ARGV: id, max_attempts, requeue(1/0)
# return: 'requeued' | 'dead' | 'gone'
_NACK = """
local id = ARGV[1]
local v = redis.call('HGET', KEYS[2], id)
redis.call('ZREM', KEYS[1], id)
redis.call('HDEL', KEYS[2], id)
if not v then return 'gone' end
local job = cjson.decode(v)
job['attempts'] = (job['attempts'] or 0) + 1
if tonumber(ARGV[3]) == 1 and job['attempts'] <= tonumber(ARGV[2]) then
  local lane = KEYS[3]
  if job['tier'] == 'warm' then lane = KEYS[4]
  elseif job['tier'] == 'cold' then lane = KEYS[5] end
  redis.call('LPUSH', lane, cjson.encode(job))
  return 'requeued'
else
  redis.call('LPUSH', KEYS[6], cjson.encode(job))
  return 'dead'
end
"""

# --- Lua: reap expired in-flight jobs ----------------------------------------
# KEYS: inflight, inflight_jobs, hot, warm, cold, dlq   ARGV: now, max_attempts
# return: {requeued_count, dead_count}
_REAP = """
local ids = redis.call('ZRANGEBYSCORE', KEYS[1], '-inf', ARGV[1])
local requeued, dead = 0, 0
for _, id in ipairs(ids) do
  local v = redis.call('HGET', KEYS[2], id)
  redis.call('ZREM', KEYS[1], id)
  redis.call('HDEL', KEYS[2], id)
  if v then
    local job = cjson.decode(v)
    job['attempts'] = (job['attempts'] or 0) + 1
    if job['attempts'] > tonumber(ARGV[2]) then
      redis.call('LPUSH', KEYS[6], cjson.encode(job))
      dead = dead + 1
    else
      local lane = KEYS[3]
      if job['tier'] == 'warm' then lane = KEYS[4]
      elseif job['tier'] == 'cold' then lane = KEYS[5] end
      redis.call('LPUSH', lane, cjson.encode(job))
      requeued = requeued + 1
    end
  end
end
return {requeued, dead}
"""


class EnqueueResult(str, Enum):
    ENQUEUED = "enqueued"
    DUPLICATE = "duplicate"
    FULL = "full"


@dataclass
class Stats:
    hot: int
    warm: int
    cold: int
    inflight: int
    dlq: int
    seen: int

    @property
    def ready(self) -> int:
        return self.hot + self.warm + self.cold

    def as_dict(self) -> dict:
        return {
            "hot": self.hot, "warm": self.warm, "cold": self.cold,
            "ready": self.ready, "inflight": self.inflight,
            "dlq": self.dlq, "seen": self.seen,
        }


class Purser:
    def __init__(
        self,
        url: str = "redis://127.0.0.1:6379",
        prefix: str = "purser",
        max_attempts: int = 5,
        visibility_timeout: float = 300.0,
        max_depth: int = 0,
        dedup: bool = True,
        client: "redis.Redis | None" = None,
    ) -> None:
        self.r = client or redis.from_url(url, decode_responses=True)
        self.prefix = prefix
        self.max_attempts = max_attempts
        self.visibility_timeout = visibility_timeout
        self.max_depth = max_depth
        self.dedup = dedup

        self._lane = {t: f"{prefix}:q:{t}" for t in TIERS}
        self._inflight = f"{prefix}:inflight"
        self._inflight_jobs = f"{prefix}:inflight:jobs"
        self._dlq = f"{prefix}:dlq"
        self._seen = f"{prefix}:seen"

        self._enqueue = self.r.register_script(_ENQUEUE)
        self._claim = self.r.register_script(_CLAIM)
        self._ack = self.r.register_script(_ACK)
        self._nack = self.r.register_script(_NACK)
        self._reap = self.r.register_script(_REAP)

    # -- producer side --------------------------------------------------------

    def enqueue(self, payload: str, tier: str = "warm", meta: dict | None = None) -> EnqueueResult:
        if tier not in TIERS:
            raise ValueError(f"tier must be one of {TIERS}, got {tier!r}")
        job = Job(payload=payload, tier=tier, meta=meta or {})
        rv = self._enqueue(
            keys=[self._lane[tier], self._seen],
            args=[job.to_json(), payload, self.max_depth, 1 if self.dedup else 0],
        )
        return {1: EnqueueResult.ENQUEUED, 0: EnqueueResult.DUPLICATE, -1: EnqueueResult.FULL}[int(rv)]

    # -- worker side ----------------------------------------------------------

    def dequeue(self, timeout: float = 0.0, poll_interval: float = 0.25) -> Job | None:
        """Claim the highest-priority job. Blocks up to ``timeout`` seconds
        (``0`` = return immediately, negative = block forever)."""
        deadline = None if timeout < 0 else time.monotonic() + timeout
        while True:
            v = self._claim(
                keys=[self._lane["hot"], self._lane["warm"], self._lane["cold"],
                      self._inflight, self._inflight_jobs],
                args=[time.time() + self.visibility_timeout],
            )
            if v:
                return Job.from_json(v)
            if timeout == 0 or (deadline is not None and time.monotonic() >= deadline):
                return None
            nap = poll_interval
            if deadline is not None:
                nap = min(nap, max(0.0, deadline - time.monotonic()))
            time.sleep(nap)

    def ack(self, job: Job) -> None:
        self._ack(keys=[self._inflight, self._inflight_jobs], args=[job.id])

    def nack(self, job: Job, requeue: bool = True) -> str:
        return str(self._nack(
            keys=[self._inflight, self._inflight_jobs,
                  self._lane["hot"], self._lane["warm"], self._lane["cold"], self._dlq],
            args=[job.id, self.max_attempts, 1 if requeue else 0],
        ))

    # -- maintenance ----------------------------------------------------------

    def requeue_expired(self) -> tuple[int, int]:
        """Return (requeued, dead) for in-flight jobs whose deadline passed."""
        requeued, dead = self._reap(
            keys=[self._inflight, self._inflight_jobs,
                  self._lane["hot"], self._lane["warm"], self._lane["cold"], self._dlq],
            args=[time.time(), self.max_attempts],
        )
        return int(requeued), int(dead)

    def stats(self) -> Stats:
        pipe = self.r.pipeline()
        for t in TIERS:
            pipe.llen(self._lane[t])
        pipe.zcard(self._inflight)
        pipe.llen(self._dlq)
        pipe.scard(self._seen)
        hot, warm, cold, inflight, dlq, seen = pipe.execute()
        return Stats(hot, warm, cold, inflight, dlq, seen)

    def peek(self, tier: str, n: int = 10) -> list[Job]:
        if tier == "dlq":
            raw = self.r.lrange(self._dlq, 0, n - 1)
        else:
            if tier not in TIERS:
                raise ValueError(f"tier must be one of {TIERS + ('dlq',)}")
            raw = self.r.lrange(self._lane[tier], 0, n - 1)
        return [Job.from_json(x) for x in raw]

    def drain_dlq(self, tier: str = "warm", limit: int | None = None) -> int:
        """Move jobs from the DLQ back onto a lane (attempts reset). Returns count."""
        moved = 0
        while limit is None or moved < limit:
            raw = self.r.rpop(self._dlq)
            if raw is None:
                break
            job = Job.from_json(raw)
            job.attempts = 0
            job.tier = tier
            self.r.lpush(self._lane[tier], job.to_json())
            moved += 1
        return moved

    def purge(self) -> int:
        """Delete every Purser key under this prefix. Destructive."""
        keys = list(self._lane.values()) + [
            self._inflight, self._inflight_jobs, self._dlq, self._seen
        ]
        return int(self.r.delete(*keys))
