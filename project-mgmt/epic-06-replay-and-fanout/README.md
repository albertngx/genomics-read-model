# Epic 06 — Replay and fan-out

**Goal:** the payoff epic. Build the correctness oracle (the verifier), then cash the
design's three cheques: rebuild a derived store from the log, change a derivation with
zero downtime, and add a consumer nobody planned for without touching the producer.
Everything before this epic was setup; this is the demo reel.

**Done when:** `make verify` is the arbiter of "in sync" for both stores (and has caught
planted bugs), `make demo-rebuild` passes — including while the generator is running —
`specimens_v2` goes live via an alias flip with an unbroken query probe, and the analytics
consumer lands with a diff proving zero producer changes.

## Stories

1. [Verifier](01-verifier.md) — the correctness oracle: PG ↔ ES ↔ Redis diff, lag vs. drift (L)
2. [Rebuild from log](02-rebuild-from-log.md) — the headline demo: delete, replay, verify (M)
3. [Zero-downtime reindex](03-zero-downtime-reindex.md) — specimens_v2, alias flip, query probe (L)
4. [The late consumer](04-analytics-fanout.md) — analytics → DuckDB, zero producer changes (M)

## Field notes

*(fill in as you build)*
