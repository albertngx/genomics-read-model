# Story 05.1 — Cache model and consumer skeleton

- **Status:** todo
- **Effort:** M
- **Depends on:** epic 03 (reuses `consumers/base.py` from story 04.1 when available — see epic README)

## Context

The cache serves point lookups: queue dashboards and tools that need run/specimen metadata
fast without touching the governed OLTP database (ARCHITECTURE.md §1). Before any Redis
write happens, the projection must be *designed*: what is cached, what deliberately isn't,
and why freshness comes from the change stream rather than expiry.

## Tasks

- [ ] Key scheme: `run:{id}` and `specimen:{id}` as Redis hashes, fields mirroring the row
      plus `_lsn` (the guard field for story 05.2). `volume_ul` stays the D6 string —
      Redis is string-typed anyway, so the decimal survives untouched end to end.
- [ ] **No TTL — a decision, documented.** This is a derived store whose correctness comes
      from CDC, not expiry; a TTL would quietly mask exactly the sync bugs the verifier
      exists to catch. One paragraph on this in the epic field notes.
- [ ] What is deliberately NOT cached: no query-shaped secondary structures (status sets,
      per-run specimen lists). Queries are the search index's job; the cache answers point
      reads. Write the boundary down — it mirrors the §0 discipline of saying what a
      system is not.
- [ ] Consumer skeleton on the shared base: group `cache-consumer`, subscribed to both
      topics, `auto.offset.reset=earliest` (required by the epic-06 rebuild), handler
      stubbed to log key + op + LSN. Graceful shutdown and rebalance behavior inherited.
- [ ] Compose service + healthcheck, same shape as the indexer.

## Acceptance criteria

- [ ] Consumer runs against live `make load`, logging events from both topics; SIGTERM
      under load resumes with ≤ one batch of redelivery — the base-class contract holds
      for a second real consumer.
- [ ] The base class needed zero changes; if it did need changes, they landed as their own
      commit with a note explaining what the first consumer got wrong.
- [ ] The design decisions (key scheme, no-TTL, not-cached list) are written down before
      story 05.2 starts.

## What this teaches

Cache design as projection design — deciding what a read model is *for* before writing it —
and the discipline of a derived store whose freshness is guaranteed by the log rather than
by expiry heuristics.
