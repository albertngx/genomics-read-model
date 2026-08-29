# Genomics Read-Model Pipeline

A working reference implementation of a **change-data-capture (CDC) read-model**. It streams
every change in a Postgres database into Apache Kafka using Debezium, and uses that change
stream to keep two derived data stores — an Elasticsearch search index and a Redis cache —
continuously in sync with the source database. Because the derived stores are built entirely
from the change stream, any of them can be deleted and rebuilt from scratch, and an automated
tool checks that the rebuilt state matches Postgres exactly.

## The problem it solves

Many applications need to serve reads from more than one place: the system of record in a
relational database, full-text search in a search engine, a hot cache in front of both. The
obvious approach is to have the application write to all of them. This is the **dual-write
problem**: there is no single transaction that spans the separate systems, so a crash or a
race between the writes will eventually leave them disagreeing — and usually with no record of
what was lost.

The robust answer, and the one this project implements, is to make the database's
**write-ahead log (WAL) the single source of truth** and derive everything else from it:

- the application writes only to Postgres;
- Debezium reads Postgres's WAL and publishes each row change onto Kafka topics;
- independent consumers read those changes and project them into Elasticsearch and Redis.

Every derived store is therefore a pure function of the change log. That is what makes them
**rebuildable**: delete the search index, replay the log from the beginning, and it comes back
identical — with a verifier to prove it rather than trusting appearances.

## The domain

The example domain is a **population-scale genomic sequencing platform** — think of a biobank
or a national sequencing programme that records its *sequencing runs* and the *specimens* they
process. At that scale there are millions of specimens, and scientists and operators need fast,
programme-wide specimen search and metadata lookups. Running those queries directly against the
regulated clinical database doesn't scale and competes with the writes it must protect, so the
search index and cache exist to carry that read load while staying faithful to the source.

The domain is a realistic backdrop, but the pipeline itself is general: the same design fits any
system that needs searchable, cacheable read-models kept in sync with a transactional database.

### What's intentionally out of scope

Two neighbouring problems are deliberately left out to keep the design focused:

- **Not an audit log.** The derived stores hold *current state* — the latest version of each
  row — not append-only history. Capturing full history is event sourcing, a different design.
- **Not the raw sequence-data firehose.** The high-volume base-call / FASTQ / BAM data coming
  off the sequencers is a separate stream-processing problem. This project tracks specimen and
  run *state*, not that data.

## How it works

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

- **Tracking API (FastAPI)** — the only writer to Postgres. It does CRUD on runs and specimens
  and knows nothing about Kafka or the stores downstream of it.
- **Postgres** — the source of truth. Logical replication exposes its WAL as a change stream.
- **Debezium / Kafka Connect** — reads the WAL and publishes each row change to Kafka topics,
  keyed by primary key. The exact shape of those events is documented in
  [docs/EVENTS.md](docs/EVENTS.md).
- **Search indexer → Elasticsearch** and **cache consumer → Redis** — independent consumers
  that project the change stream into their stores. Each is idempotent, so replaying or
  redelivering an event is always safe.
- **Verifier** — an automated diff between Postgres and each derived store. It is the arbiter
  of whether the system is "in sync," and the check at the end of every rebuild demo.

The signature demonstration: delete the entire Elasticsearch index, reset the indexer's Kafka
consumer group to the earliest offset, and let it re-consume the whole change log. It rebuilds
to a state identical to Postgres — confirmed by the verifier.

Full design, rationale, topic layout, and failure-mode analysis are in
[ARCHITECTURE.md](ARCHITECTURE.md).

## Status

**Phase 1 (the Debezium-based pipeline) is in progress.** The platform stack and the source
service are complete, and CDC ingestion is underway: the Debezium connector is registered and
streaming, and the change-event contract is documented in [docs/EVENTS.md](docs/EVENTS.md).
Topic compaction and the failure drills come next, followed by the search indexer, cache
consumer, verifier, and rebuild demos.

See [project-mgmt/](project-mgmt/) for the full epic/story breakdown and current progress, and
[project-mgmt/roadmap.md](project-mgmt/roadmap.md) for what follows Phase 1: a hand-written
`pgoutput` connector to replace Debezium, a no-Kafka `postgres-only` comparison, and an
observability layer.

## Repository layout

```
ARCHITECTURE.md   system design: components, topic design, delivery semantics,
                  failure modes, and the decision log — read this first
docs/             explanatory wiki: how each part works, the EVENTS.md event
                  contract, and architecture decision records (ADRs)
project-mgmt/     the epic/story breakdown, plus roadmap.md for what's beyond Phase 1
learnings/        short notes on the CDC / Kafka / Postgres gotchas found while building
src/              the tracking API, database models and migrations, and CLI tooling
deploy/           the docker-compose stack and the Debezium connector config
tests/            unit, integration, and smoke tests
Makefile          a single entrypoint for every workflow (run `make help`)
```

## Quickstart

Prerequisites: Docker (via Colima) and [`uv`](https://docs.astral.sh/uv/) — see
[PREREQUISITES.md](PREREQUISITES.md). Run all commands from the repository root.

```
make up      # start the stack (Postgres, Kafka+Connect, Elasticsearch, Redis, kafka-ui, tracking API)
make seed    # apply migrations and load sample data (~30 runs, ~5k specimens); idempotent
make down    # stop the stack, keep data (volumes survive)
make nuke    # stop the stack and delete all data (removes the volumes)
```

The tracking API is served at `http://localhost:8000`, with interactive API docs (Swagger UI)
at [`/docs`](http://localhost:8000/docs) and a set of example requests in
[`requests.http`](requests.http). `make help` lists every target.

`make nuke` removes the Postgres volume, so after the next `make up` the database is empty;
re-run `make seed` before calling the API. Later stories add `make load`, `make verify`, and
`make demo`.
