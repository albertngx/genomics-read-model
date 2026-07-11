# Epic 05 — Cache consumer

**Goal:** the second consumer: CDC events → Redis metadata cache with the same guarantees
as the search indexer — commit-after-write, LSN-guarded idempotency, both delete paths —
against a store with completely different primitives (Redis has no external versioning;
the compare-and-set guard is hand-built in Lua). This epic is deliberately cheap: it
re-applies the epic-04 pattern, and its cheapness *is* the evidence the pattern is sound.
Anywhere it turns out not to be cheap is a finding — write it down.

**Sequencing:** depends only on epics 01–03 (see the project README); the canonical order
is after epic 04, so `consumers/base.py` and the pattern exist to be reused. If pulled
earlier, story 05.1 builds the shared base instead of borrowing it.

**Done when:** under sustained generator load with deletes, Postgres and Redis agree
(spot-check scripts until the epic-06 verifier exists), through kill -9 restarts — and the
cache demonstrates the read-latency win it exists for, with numbers.

## Stories

1. [Cache model and consumer skeleton](01-cache-model-and-skeleton.md) — keys, no-TTL decision, skeleton on the shared base (M)
2. [CAS idempotency and the delete lifecycle](02-cas-idempotency-and-deletes.md) — Lua version guards, the disposal-policy decision (L)
3. [Parity checks and the read path](03-parity-and-read-path.md) — spot-checks, latency numbers, consumer independence (M)

## Field notes

*(fill in as you build)*
