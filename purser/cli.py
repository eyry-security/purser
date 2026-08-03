"""Command-line interface.

Subcommands:
  enqueue   push one or more payloads onto a lane
  ingest    drain a plain producer list (e.g. Foretop's output) into the queue
  feed      pull jobs and LPUSH the bare payload to a downstream list (e.g. Vedette)
  worker    demo consumer: claim, print, ack
  reap      requeue in-flight jobs whose deadline lapsed (run on a loop)
  stats     show lane depths
  peek      look at jobs without claiming them
  drain-dlq move dead-lettered jobs back onto a lane
  purge     delete all Purser keys (destructive)
"""

from __future__ import annotations

import argparse
import json
import sys
import time

from . import __version__
from .job import TIERS
from .queue import Purser


def _add_common(sp: argparse.ArgumentParser) -> None:
    sp.add_argument("--redis", default="redis://127.0.0.1:6379", help="Redis URL")
    sp.add_argument("--prefix", default="purser", help="key namespace (default: purser)")
    sp.add_argument("--max-attempts", type=int, default=5, help="deliveries before a job is dead-lettered")
    sp.add_argument("--visibility", type=float, default=300.0, help="in-flight timeout in seconds")
    sp.add_argument("--max-depth", type=int, default=0, help="per-lane cap for backpressure (0 = unlimited)")
    sp.add_argument("--no-dedup", action="store_true", help="don't dedup payloads on enqueue")


def _purser(args) -> Purser:
    return Purser(
        url=args.redis, prefix=args.prefix, max_attempts=args.max_attempts,
        visibility_timeout=args.visibility, max_depth=args.max_depth,
        dedup=not args.no_dedup,
    )


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        prog="purser",
        description="Redis-backed priority work queue with retries and a dead-letter queue.",
    )
    p.add_argument("--version", action="version", version=f"purser {__version__}")
    sub = p.add_subparsers(dest="cmd", required=True)

    sp = sub.add_parser("enqueue", help="push payloads onto a lane")
    _add_common(sp)
    sp.add_argument("payloads", nargs="*", help="payloads (or read stdin, one per line, if omitted)")
    sp.add_argument("--tier", choices=TIERS, default="warm")

    sp = sub.add_parser("ingest", help="drain a plain producer list into the queue")
    _add_common(sp)
    sp.add_argument("--from", dest="src", required=True, help="source Redis list key")
    sp.add_argument("--tier", choices=TIERS, default="warm")
    sp.add_argument("--once", action="store_true", help="stop when the source list is empty")

    sp = sub.add_parser("feed", help="pull jobs and push bare payloads to a downstream list")
    _add_common(sp)
    sp.add_argument("--to", dest="dst", required=True, help="downstream Redis list key (e.g. vedette:hosts)")

    sp = sub.add_parser("worker", help="demo consumer: claim, print, ack")
    _add_common(sp)

    sp = sub.add_parser("reap", help="requeue expired in-flight jobs on a loop")
    _add_common(sp)
    sp.add_argument("--interval", type=float, default=10.0, help="seconds between sweeps")
    sp.add_argument("--once", action="store_true", help="sweep once and exit")

    sp = sub.add_parser("stats", help="show lane depths")
    _add_common(sp)
    sp.add_argument("--json", action="store_true", help="emit JSON")
    sp.add_argument("--watch", type=float, default=0.0, help="refresh every N seconds")

    sp = sub.add_parser("peek", help="look at jobs without claiming them")
    _add_common(sp)
    sp.add_argument("tier", choices=TIERS + ("dlq",))
    sp.add_argument("-n", type=int, default=10)

    sp = sub.add_parser("drain-dlq", help="move dead-lettered jobs back onto a lane")
    _add_common(sp)
    sp.add_argument("--tier", choices=TIERS, default="warm")
    sp.add_argument("--limit", type=int, default=None)

    sp = sub.add_parser("purge", help="delete all Purser keys (destructive)")
    _add_common(sp)
    sp.add_argument("--yes", action="store_true", help="confirm deletion")

    return p


def _log(msg: str) -> None:
    print(f"[purser] {msg}", file=sys.stderr, flush=True)


def cmd_enqueue(args) -> int:
    q = _purser(args)
    payloads = args.payloads or [l.strip() for l in sys.stdin if l.strip()]
    counts = {"enqueued": 0, "duplicate": 0, "full": 0}
    for pl in payloads:
        counts[q.enqueue(pl, tier=args.tier).value] += 1
    _log(f"{counts['enqueued']} enqueued, {counts['duplicate']} dup, {counts['full']} rejected (full)")
    return 0


def cmd_ingest(args) -> int:
    q = _purser(args)
    n = 0
    _log(f"ingesting {args.src} -> {args.tier} lane")
    try:
        while True:
            if args.once:
                item = q.r.rpop(args.src)
                if item is None:
                    break
            else:
                popped = q.r.brpop(args.src, timeout=5)
                if popped is None:
                    continue
                item = popped[1]
            q.enqueue(item, tier=args.tier)
            n += 1
            if n % 100 == 0:
                _log(f"ingested {n}")
    except KeyboardInterrupt:
        pass
    _log(f"done: ingested {n}")
    return 0


def cmd_feed(args) -> int:
    q = _purser(args)
    n = 0
    _log(f"feeding claimed payloads -> {args.dst}")
    try:
        while True:
            job = q.dequeue(timeout=-1)
            if job is None:
                continue
            q.r.lpush(args.dst, job.payload)
            q.ack(job)
            n += 1
            if n % 100 == 0:
                _log(f"fed {n}")
    except KeyboardInterrupt:
        pass
    _log(f"done: fed {n}")
    return 0


def cmd_worker(args) -> int:
    q = _purser(args)
    _log("worker up; ctrl-c to stop")
    n = 0
    try:
        while True:
            job = q.dequeue(timeout=-1)
            if job is None:
                continue
            print(job.to_json(), flush=True)
            q.ack(job)
            n += 1
    except KeyboardInterrupt:
        pass
    _log(f"done: processed {n}")
    return 0


def cmd_reap(args) -> int:
    q = _purser(args)
    try:
        while True:
            requeued, dead = q.requeue_expired()
            if requeued or dead:
                _log(f"reaped: {requeued} requeued, {dead} dead-lettered")
            if args.once:
                break
            time.sleep(args.interval)
    except KeyboardInterrupt:
        pass
    return 0


def _print_stats(q: Purser, as_json: bool) -> None:
    s = q.stats().as_dict()
    if as_json:
        print(json.dumps(s), flush=True)
    else:
        print(
            f"ready={s['ready']} (hot={s['hot']} warm={s['warm']} cold={s['cold']})  "
            f"inflight={s['inflight']}  dlq={s['dlq']}  seen={s['seen']}",
            flush=True,
        )


def cmd_stats(args) -> int:
    q = _purser(args)
    if args.watch > 0:
        try:
            while True:
                _print_stats(q, args.json)
                time.sleep(args.watch)
        except KeyboardInterrupt:
            pass
    else:
        _print_stats(q, args.json)
    return 0


def cmd_peek(args) -> int:
    q = _purser(args)
    for job in q.peek(args.tier, args.n):
        print(job.to_json(), flush=True)
    return 0


def cmd_drain_dlq(args) -> int:
    q = _purser(args)
    moved = q.drain_dlq(tier=args.tier, limit=args.limit)
    _log(f"moved {moved} job(s) from dlq -> {args.tier}")
    return 0


def cmd_purge(args) -> int:
    q = _purser(args)
    if not args.yes:
        _log("refusing to purge without --yes")
        return 2
    _log(f"purged {q.purge()} key(s) under prefix {args.prefix!r}")
    return 0


_DISPATCH = {
    "enqueue": cmd_enqueue, "ingest": cmd_ingest, "feed": cmd_feed,
    "worker": cmd_worker, "reap": cmd_reap, "stats": cmd_stats,
    "peek": cmd_peek, "drain-dlq": cmd_drain_dlq, "purge": cmd_purge,
}


def main(argv=None) -> int:
    args = build_parser().parse_args(argv)
    try:
        return _DISPATCH[args.cmd](args)
    except KeyboardInterrupt:
        return 130


if __name__ == "__main__":
    raise SystemExit(main())
