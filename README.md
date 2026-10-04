# Purser

A Redis-backed priority work queue with retries and a dead-letter queue.
Part of [Eyry](https://eyry.io).

A *purser* was the ship's quartermaster — the officer who allocates and
distributes provisions. This Purser does the same for recon work: a firehose of
discovered hosts goes in, an orderly, survivable work stream comes out.

## What it does

- Holds jobs in **hot / warm / cold** priority lanes; workers always claim the
  highest non-empty lane first.
- **Claim/ack delivery**: a worker claims a job (atomically, via Lua) and acks
  it when done. Crashed workers time out and their jobs come back.
- **Retries with a dead-letter queue**: jobs that keep failing past
  `--max-attempts` land in the DLQ instead of looping forever.
- **Dedup on enqueue**: the same payload is never queued twice (unless you
  pass `--no-dedup`).
- **Backpressure**: per-lane `--max-depth` caps; a full lane rejects new
  payloads with `FULL` instead of growing without bound.
- Every multi-key step runs as a Lua script — atomic inside Redis. There is no
  server to run beyond Redis itself.

## Install

```sh
git clone https://github.com/eyry-security/purser
cd purser
pip install -e .
```

Requires Python 3.9+ and a Redis 5+ reachable at `redis://127.0.0.1:6379`
(override with `--redis`).

## Quickstart

Push hosts onto a lane, watch the depths, claim them:

```sh
$ purser enqueue api.example.com --tier hot
[purser] 1 enqueued, 0 dup, 0 rejected (full)

$ cat hosts.txt | purser enqueue --tier cold
[purser] 12 enqueued, 0 dup, 0 rejected (full)

$ purser enqueue api.example.com --tier hot
[purser] 0 enqueued, 1 dup, 0 rejected (full)

$ purser stats
ready=4 (hot=1 warm=1 cold=2)  inflight=0  dlq=0  seen=4

$ purser peek hot
{"id":"9582c842e2614bb89085ca6bedc175fd","payload":"api.example.com","tier":"hot","attempts":0,"enqueued_at":1791099817.43,"meta":{}}
```

### Wiring the pipeline: ingest → feed → reap

Purser sits between Foretop (the producer) and Vedette (the consumer). Neither
tool changes: `ingest` drains a plain Redis list into lanes, `feed` claims jobs
and pushes bare payloads downstream, and `reap` runs on a loop so crashed work
comes back.

```sh
# 1. Foretop drops new hosts onto a plain list
foretop --scope '*.example.com' --redis redis://127.0.0.1:6379 --queue purser:in

# 2. Purser ingests them into the warm lane (dedup + backpressure applied here)
purser ingest --from purser:in --tier warm

# 3. Purser claims jobs and LPUSHes the bare payload to Vedette's list
purser feed --to vedette:hosts

# 4. Vedette probes them off that list
vedette --redis redis://127.0.0.1:6379 --queue vedette:hosts -o live.jsonl

# 5. A reaper on a loop requeues in-flight jobs whose deadline lapsed
purser reap
```

Run each of `ingest`, `feed`, and `reap` as a long-lived process (or under your
process supervisor); they block until interrupted.

## CLI

| Command | What it does |
| --- | --- |
| `purser enqueue <payload...>` | Push payloads onto a lane; reads stdin (one per line) if none given |
| `purser ingest --from <list>` | Drain a plain producer list into the queue (`--once` to stop when empty) |
| `purser feed --to <list>` | Claim jobs, `LPUSH` the bare payload downstream, ack each |
| `purser worker` | Demo consumer: claim, print job as JSON, ack |
| `purser reap` | Requeue in-flight jobs whose deadline lapsed (`--interval`, `--once`) |
| `purser stats` | Lane depths (`--json`, `--watch N` to refresh every N seconds) |
| `purser peek <tier>` | Show jobs without claiming them (`tier` can also be `dlq`; `-n` for count) |
| `purser drain-dlq` | Move dead-lettered jobs back onto a lane (`--tier`, `--limit`) |
| `purser purge --yes` | Delete all Purser keys under the prefix (destructive) |

Shared flags on every command: `--redis`, `--prefix`, `--max-attempts` (5),
`--visibility` (300s in-flight timeout), `--max-depth` (0 = unlimited),
`--no-dedup`.

## Key concepts

**Claim/ack.** A worker claims with `dequeue()`: one Lua script pops the job
from the highest non-empty lane and puts it in an in-flight sorted set with a
deadline of `now + visibility`. The worker acks on success. `nack(job)` sends it
straight back for another attempt (or to the DLQ once attempts are exhausted).

**The reaper.** `purser reap` sweeps the in-flight set for expired deadlines.
Expired jobs are requeued onto their original lane — or dead-lettered once
`attempts > max_attempts`. This is what makes delivery at-least-once: a worker
that dies mid-job leaves the job in-flight until its deadline lapses, and then
the reaper brings it back.

**Lanes.** `hot` for things that need probing now (new hosts on a watched
scope), `warm` for the steady stream, `cold` for bulk/low-priority work. Within
a lane it's FIFO; across lanes it's strict priority.

**Lua atomicity.** Claim, ack, nack, enqueue, and reap are each a single Redis
Lua script — no multi-step race between two workers, even with dozens of them
running.

## Redis keys

Everything lives under one prefix (default `purser`; change with `--prefix`):

| Key | Type | Purpose |
| --- | --- | --- |
| `purser:q:hot` / `:warm` / `:cold` | list | the priority lanes |
| `purser:inflight` | zset | claimed jobs, scored by deadline |
| `purser:inflight:jobs` | hash | id → job JSON for in-flight jobs |
| `purser:dlq` | list | jobs that exhausted their attempts |
| `purser:seen` | set | dedup of payloads seen on enqueue |

Jobs are JSON; only `payload` matters downstream (the `feed` bridge and
Vedette's reader see just the bare string). The rest is bookkeeping:

```json
{"id":"9582c842…","payload":"api.example.com","tier":"hot","attempts":0,"enqueued_at":1791099817.43,"meta":{}}
```

## Library

```python
from purser import Purser

q = Purser("redis://127.0.0.1:6379", max_attempts=5, visibility_timeout=300)

# producer
q.enqueue("api.example.com", tier="hot")
q.enqueue("blog.example.com")                      # defaults to warm

# worker
job = q.dequeue(timeout=-1)                         # block forever
try:
    do_work(job.payload)
    q.ack(job)
except Exception:
    q.nack(job)                                     # retry, or DLQ if out of attempts
```

`enqueue` returns an `EnqueueResult` — `ENQUEUED`, `DUPLICATE`, or `FULL`.
`dequeue(timeout=0)` returns immediately if the queue is empty; a negative
timeout blocks forever.

## Where it fits

`Foretop (new hosts) → Purser (queue) → Vedette (probe) → Rutt (store) → Aplomado (AI review)`

Purser is the layer that lets the pipeline run at scale: prioritize what to
probe first, survive worker crashes without losing work, and keep a paper trail
of everything that failed.

## The Eyry suite

- **eyry**: one CLI that wires the data plane together — discover → queue → probe → store
- **vedette**: fast, multi-threaded HTTP prober (Rust) — confirms what is live and fingerprints it
- **foretop**: pluggable live feed of new hosts, starting with Certificate Transparency logs
- **purser**: Redis-backed priority work queue — hot/warm/cold lanes, retries, dead-letter queue
- **rutt**: Postgres store for the host lifecycle (discovered → probed → reviewed) with an append-only scan log
- **pinnace**: general multi-turn agent runtime — compaction, tools, Docker sandbox, resumable sessions
- **aplomado**: AI security reviewer built on Pinnace — target in, structured findings out
- **quarterdeck**: agent control plane — scheduler, wake/sleep, identity and memory, IRC-style chat, ChatOps, pipeline orchestration
## Roadmap

- Per-worker lane selection (claim only from chosen lanes)
- Time-windowed dedup (forget a payload after a TTL)
- Delayed / scheduled jobs (enqueue for later)
- Prometheus metrics

## License

MIT © Eyry
