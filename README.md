# Purser

A Redis-backed priority work queue with retries and a dead-letter queue. The
quartermaster of the [Eyry](https://eyry.io) recon suite.

Purser sits between a producer and its workers. It takes hosts from
[Foretop](https://github.com/eyry-security/foretop), holds them in **hot / warm
/ cold** priority lanes, and hands them out to workers like
[Vedette](https://github.com/eyry-security/vedette) with **at-least-once
delivery**. Jobs that fail are retried; jobs that keep failing land in a
**dead-letter queue**. Duplicates are dropped on the way in, and per-lane depth
caps give you backpressure.

MIT licensed. Pure Redis — no broker, no server to run beyond Redis itself.

## Why

A plain Redis list (`LPUSH` / `BRPOP`) is a fine queue until you need any of:
priority, "retry if the worker crashed mid-job", "stop growing when we're
behind", or "show me what keeps failing." Purser adds exactly those, and
nothing else, on top of Redis.

Delivery is reliable via a claim/ack cycle: a worker *claims* a job (atomically
popped from the highest non-empty lane into an in-flight set with a deadline)
and *acks* it when finished. If the worker dies, the deadline lapses and a
reaper puts the job back — or dead-letters it once it has failed too many times.
Every multi-key step is a Lua script, so it's atomic inside Redis.

## Install

```sh
git clone https://github.com/eyry-security/purser
cd purser
pip install -e .
```

Requires Python 3.9+ and a reachable Redis (5+).

## The pipeline

```
Foretop  --LPUSH-->  [ purser ingest ]  ==>  hot / warm / cold  ==>  [ purser feed ]  --LPUSH-->  Vedette
                                                     |
                                              retries → DLQ
```

Wire Foretop's output straight through Purser and into Vedette without changing
either tool:

```sh
# 1. Foretop drops hosts onto a plain list
foretop --scope '*.example.com' --redis redis://127.0.0.1:6379 --queue purser:in

# 2. Purser ingests them into the warm lane (dedup + backpressure here)
purser ingest --from purser:in --tier warm

# 3. Purser feeds claimed hosts to Vedette's list, acking as it goes
purser feed --to vedette:hosts

# 4. Vedette probes them
vedette --redis redis://127.0.0.1:6379 --queue vedette:hosts -o live.jsonl

# keep a reaper running so crashed work comes back
purser reap
```

Prefer to integrate directly? Use the library and do your own claim/ack.

## Library

```python
from purser import Purser

q = Purser("redis://127.0.0.1:6379", max_attempts=5, visibility_timeout=300)

# producer
q.enqueue("api.example.com", tier="hot")
q.enqueue("blog.example.com")                # defaults to warm

# worker
job = q.dequeue(timeout=-1)                   # block until something is ready
try:
    do_work(job.payload)
    q.ack(job)
except Exception:
    q.nack(job)                               # retry, or DLQ if out of attempts
```

`enqueue` returns an `EnqueueResult` — `ENQUEUED`, `DUPLICATE`, or `FULL`.

## CLI

| Command | What it does |
| --- | --- |
| `purser enqueue <payload...>` | Push payloads onto a lane (`--tier hot\|warm\|cold`); reads stdin if none given |
| `purser ingest --from <list>` | Drain a plain producer list into the queue (`--once` to stop when empty) |
| `purser feed --to <list>` | Claim jobs and `LPUSH` the bare payload downstream, acking each |
| `purser worker` | Demo consumer: claim, print as JSON, ack |
| `purser reap` | Requeue in-flight jobs whose deadline lapsed (`--interval`, `--once`) |
| `purser stats` | Lane depths (`--json`, `--watch N`) |
| `purser peek <tier>` | Show jobs without claiming them (`tier` includes `dlq`) |
| `purser drain-dlq` | Move dead-lettered jobs back onto a lane |
| `purser purge --yes` | Delete all Purser keys (destructive) |

Common flags: `--redis`, `--prefix`, `--max-attempts`, `--visibility`,
`--max-depth`, `--no-dedup`.

```sh
purser enqueue api.example.com --tier hot
cat hosts.txt | purser enqueue --tier cold
purser stats --watch 1
```

## Job format

Jobs are JSON. Only `payload` matters downstream (the `feed` bridge and
Vedette's `BRPOP` reader see just the bare string); the rest is bookkeeping.

```json
{"id":"a1b2c3…","payload":"api.example.com","tier":"hot","attempts":0,"enqueued_at":1785726585.9,"meta":{}}
```

## Keys

Everything lives under a prefix (default `purser`):

| Key | Type | Purpose |
| --- | --- | --- |
| `purser:q:hot` / `:warm` / `:cold` | list | the priority lanes |
| `purser:inflight` | zset | claimed jobs, scored by their deadline |
| `purser:inflight:jobs` | hash | id → job JSON for in-flight jobs |
| `purser:dlq` | list | jobs that exhausted their attempts |
| `purser:seen` | set | dedup of payloads seen on enqueue |

## Where it fits

```
Foretop (new hosts) → Purser (queue) → Vedette (probe) → Aplomado (AI review)
```

Purser is the layer that lets the pipeline run at scale: prioritize what to look
at first, survive worker crashes, and keep a paper trail of what failed. See the
suite at [github.com/eyry-security](https://github.com/eyry-security).

## Roadmap

- Per-worker tier selection (pull only from chosen lanes)
- Time-windowed dedup (forget a payload after a TTL)
- Delayed / scheduled jobs (enqueue for later)
- Prometheus metrics endpoint

## License

MIT © Eyry
