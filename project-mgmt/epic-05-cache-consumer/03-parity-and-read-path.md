# Story 05.3 — Parity checks and the read path

- **Status:** todo
- **Effort:** M
- **Depends on:** 05.2

## Context

The cache exists to make reads fast; this story proves both halves — that it agrees with
Postgres, and that it is actually faster, with numbers. The spot-check tooling built here
is the seed of the epic-06 verifier's Redis leg; write it with that absorption in mind.

## Tasks

- [ ] Spot-check script (`tools/spot_check_redis.py`): key-set comparison (counts, missing,
      extra) plus sampled content-hash comparison against Postgres, with readable diff
      output. Factor the "expected Redis state from PG rows" function so the epic-06
      verifier can reuse it verbatim.
- [ ] Read-path measurement: `GET`/`HGETALL` p50/p99 under `make load RATE=1000` versus
      the equivalent Postgres point query on the same host. Table into field notes — this
      is the number that justifies the cache's existence.
- [ ] **Consumer-independence demo:** stop the cache consumer for 60s under load. The
      indexer is unaffected; on restart the cache drains its lag and the spot-check comes
      back clean. Consumers positioned independently on the log (§1) — demonstrated, not
      recited.
- [ ] Ignorance check still green: `grep -riE "redis" src/cdc_pipeline/api/` returns
      nothing. The cache has its own readers; the tracking service is not one of them.

## Acceptance criteria

- [ ] Spot-check reports zero drift after a `steady` run and after a `delete-wave` run
      (post-drain).
- [ ] The latency table exists with real numbers and a one-sentence takeaway.
- [ ] The independence demo is written up in field notes with the observed lag and drain
      time.

## What this teaches

Read-model economics — what the cache buys, measured rather than assumed — and
consumer-group independence on a shared log, the property the whole fan-out design rests
on and the reason epic 06's late-added consumer will cost nothing.
