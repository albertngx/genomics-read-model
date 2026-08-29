# Docs — the project wiki

Longer-form, narrative explanations of how and why each part of the system works —
the "Confluence space" for this project. Read these to *understand* a part; read the
others for different purposes:

| Where | Purpose | Style |
|---|---|---|
| [`../ARCHITECTURE.md`](../ARCHITECTURE.md) | The authoritative spec: components, topic design, delivery semantics, decision log (D1–D8). | Terse, normative — "this is how it is." |
| **`docs/` (here)** | Explanatory wiki: the story of each part, with diagrams. | Narrative — "here's how it works and why." |
| [`adr/`](adr/) | Architecture Decision Records: one decision, its context and consequences. | Point-in-time, immutable. |
| [`../learnings/`](../learnings/) | Short "aha, *that's* what that flag does" notes. | Bite-sized gotchas. |

If a fact is normative, it lives in `ARCHITECTURE.md` and these pages *link* to it rather
than restating it — so there is one source of truth and the wiki explains around it.

## Contents

### Start here
- [System overview](overview.md) — the whole pipeline in one picture, and the dual-write
  problem it exists to solve.
- [Data lifecycle](data-lifecycle.md) — the end-to-end journey of a single change, from an
  operator's click to a search hit.

### Platform (built in epic 01 — running now)
- [Postgres & CDC](platform/postgres-and-cdc.md) — source of truth; how logical replication
  turns its WAL into a change stream.
- [Apache Kafka](platform/kafka.md) — the durable, ordered, replayable log; KRaft, listeners,
  topics, compaction.
- [Kafka Connect + Debezium](platform/kafka-connect-debezium.md) — the CDC producer; snapshot
  vs. streaming, event anatomy, LSN.
  - [EVENTS.md](EVENTS.md) — the **change-event contract**: real captured payloads for all
    five event kinds, annotated field by field; the `REPLICA IDENTITY` (D4) and decimal-mode
    (D6) experiments. Written from observed events (story 03.2).
- [Elasticsearch](platform/elasticsearch.md) — the search read-model; external versioning,
  denormalized docs, projection-deletes.
- [Redis](platform/redis.md) — the cache read-model; compare-and-set idempotency.

### Pipeline (built in later epics — pages are design-ahead, marked 📐 planned)
- [Tracking service](pipeline/tracking-service.md) — the boring, ignorant sole writer (epic 02).
- [Load generator (`cdc-load`)](pipeline/load-generator.md) — async, skewed, reproducible write
  load; the harness epics 04–07 reuse (epic 02.3).
- [Search indexer](pipeline/search-indexer.md) — the hardest consumer; the cross-topic join (epic 04).
- [Cache consumer](pipeline/cache-consumer.md) — Redis sync; the CAS idempotency pattern (epic 05).
- [Verifier & replay](pipeline/verifier-and-replay.md) — the correctness oracle, rebuild, reindex,
  fan-out (epic 06).

## Status

This wiki is seeded alongside the build. **Platform** pages describe what's running today
(epic 01). **Pipeline** pages are design-ahead — marked 📐 planned — and will be revised as
those epics land. Every page states its **status** at the top, so the space stays honest
about what is real vs. planned.
