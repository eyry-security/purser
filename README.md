# purser

> Redis-backed priority queue and work distributor (Python).

Part of **[Eyry](https://eyry.io)**: open-source, agentic recon and offensive security tooling for
bug bounty hunters, red teamers, and pentesters. Purser hands out the work: it prioritizes and distributes hosts to prober and scanner workers.

## Status

🚧 **Early development.** Structure and APIs will change. Star the repo to follow along, and
see [eyry.io](https://eyry.io).

## What it does

- Hot, warm, cold, and dead-letter priority tiers on Redis
- Dedup, backpressure, and retries to a dead-letter queue
- Distributes work to Vedette and Aplomado workers

## Install

Coming soon.

## The Eyry suite

- **Vedette**: fast, multi-threaded HTTP prober (Rust)
- **Foretop**: configurable producer of new hosts from pluggable feeds (certstream first)
- **Purser**: Redis-backed priority queue and work distributor (hot/warm/cold/DLQ)
- **Pinnace**: general multi-turn agent runtime with compaction, tools, and a Docker sandbox
- **Aplomado**: AI security scanner and reviewer built on Pinnace
- **Quarterdeck**: agent control plane, scheduler, events, IRC-style chat, and pipeline orchestration

## License

MIT, Eyry.
