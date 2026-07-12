# Genomics Read-Model Pipeline

Stream Postgres changes through Debezium into Apache Kafka and keep derived stores — an
Elasticsearch search index and a Redis cache — correct, rebuildable, and provably in sync
with the source of truth.

**Domain:** a **population-scale genomic sequencing platform**. A CLIA/CAP-governed
sequencing operation — a biobank or national sequencing programme — owns *sequencing runs*
and their *specimens* in Postgres; scientists and operators need programme-wide specimen
search and fast metadata reads across **millions** of specimens without hammering the
clinical system of record. That is a textbook CDC read-model, and the biobank scale is what
makes it *necessary* rather than ornamental: at one lab's volume you could serve every read
from Postgres, but across a whole programme the governed OLTP database cannot also be the
query engine for facility-wide search and analytics. So the read-model is justified twice
over — by **scale** and by **correctness**.

**Two things this is deliberately NOT** (both senior distinctions — see
[ARCHITECTURE.md §0](ARCHITECTURE.md)):

- not an **audit trail** — it derives current-state projections, not append-only history
  (that is event sourcing);
- not the **raw sequence-data firehose** — the high-volume base-call/FASTQ/BAM stream off
  the sequencers is a stream-processing problem, a separate system. This tracks
  specimen/run *state*.

**The problem:** writing to Postgres, Elasticsearch, and Redis from the application is the
dual-write problem — no transaction spans them, so they *will* drift, with no record of
what was lost. The correct answer: the database's WAL as the single source of truth,
everything else derived from its change stream.

**The demo that proves it:** delete the entire Elasticsearch index, reset the consumer
group to the earliest offset, and watch it rebuild to state identical to Postgres —
verified by an automated diff tool, not by eyeballing.

## Status

Phase 1 (Debezium-based pipeline) — in planning. See [project-mgmt/](project-mgmt/) for the
epic/story breakdown and [project-mgmt/roadmap.md](project-mgmt/roadmap.md) for what comes
after (own `pgoutput` connector, `postgres-only` comparison, observability layer).

## Repository layout

```
ARCHITECTURE.md      the system design: components, topic design, delivery
                     semantics, failure modes, decision log — read this first
project-mgmt/        epics (one folder each) and stories (one file each)
  roadmap.md         phase 2+ : own pgoutput connector, postgres-only
                     comparison, observability layer
src/                 (created by epic 01) services and consumers
deploy/              (created by epic 01) docker-compose stack
tools/               (created by epic 06) verifier, replay scripts, demos
```

## Architecture at a glance

```
FastAPI tracking ──► Postgres ──WAL──► Debezium ──► Kafka topics (keyed by PK)
service (CRUD on       │                               │
runs/specimens,        │              ┌────────────────┼────────────────┐
no knowledge of        │              ▼                ▼                ▼
downstream)            │        search indexer   cache consumer   analytics
                       │        → Elasticsearch   → Redis          → DuckDB
                       │              ▲                                (added last,
                       └── verifier ──┘  (automated PG ↔ ES ↔ Redis     producer
                              diff — the correctness oracle)            untouched)
```

Full design, rationale, and failure-mode analysis: [ARCHITECTURE.md](ARCHITECTURE.md).

## Quickstart

Prerequisites: Docker (via Colima) and [`uv`](https://docs.astral.sh/uv/) — see
[PREREQUISITES.md](PREREQUISITES.md). Run all commands from the repository root.

```
make up      # start the stack (Postgres, Kafka+Connect, Elasticsearch, Redis, kafka-ui, tracking API)
make seed    # apply migrations and load sample data (~30 runs, ~5k specimens); idempotent
make down    # stop the stack, keep data (volumes survive)
make nuke    # stop the stack and delete all data (removes the volumes)
```

The tracking API is served at `http://localhost:8000`, with interactive API docs (Swagger
UI) at [`/docs`](http://localhost:8000/docs) and a set of example requests in
[`requests.http`](requests.http). `make help` lists every target.

`make nuke` removes the Postgres volume, so after the next `make up` the database is empty;
re-run `make seed` before calling the API. Later stories add `make load`, `make verify`, and
`make demo`.
