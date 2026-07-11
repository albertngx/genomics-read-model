# Epic 04 — Search indexer

**Goal:** the flagship consumer: CDC events → denormalized Elasticsearch documents, correct
under redelivery, crash, rebalance, deletes, cross-topic races, and poison messages.
Every hard idea in the project (idempotency, commit discipline, DLQ, the denormalization
problem) is implemented here first; epic 05 then repeats the pattern cheaply against
Redis, which is itself the proof the pattern is sound.

**Done when:** under sustained generator load with deletes and reassignment storms, the
verifier (built in epic 06 — until then, spot-check scripts) reports Postgres and
Elasticsearch identical, through crashes and rebalances.

## Stories

1. [Consumer skeleton](01-consumer-skeleton.md) — group, commit discipline, graceful shutdown (L)
2. [Idempotent upserts and deletes](02-idempotent-upserts-and-deletes.md) — LSN version guards, tombstones (L)
3. [Batching and backpressure](03-batching-and-backpressure.md) — `_bulk`, throughput measurement (M)
4. [Denormalized documents](04-denormalized-documents.md) — the cross-topic join, reassignment storms (L)
5. [Retries and DLQ](05-retries-and-dlq.md) — poison messages, quarantine, re-injection (M)

## Field notes

*(fill in as you build)*
