# Story 02.2 — Tracking API

- **Status:** done
- **Effort:** M
- **Depends on:** 02.1

## Context

The producer side of the whole system — and its defining property is ignorance. It writes
to Postgres and does nothing else. Every time a later epic adds a consumer without touching
this service, the architecture's core claim is demonstrated. Resist all temptation to make
this service clever.

## Tasks

- [x] FastAPI app: CRUD for runs and specimens (create, get, list with basic filters,
      update), SQLAlchemy + psycopg, plain transactions.
- [x] Specimen lifecycle endpoints that produce the two delete paths (ARCHITECTURE.md §3, §7):
      - **hard delete** of a `registered` specimen (`DELETE /specimens/{id}` — 409 if past
        registered): the source row is physically removed (`op:"d"` + tombstone downstream).
      - **dispose** a specimen (`POST /specimens/{id}/dispose` → status `disposed`): the row
        survives; it is an `UPDATE` (chain-of-custody record retained).
- [x] `PATCH /runs/{id}` supports re-assignment (change `sequencer`) and protocol
      amendment — this endpoint is the trigger for the §7 denormalization storm later; it
      deserves a test now.
- [x] Healthcheck endpoint; request logging; container image + compose service.
- [x] Unit/integration tests against the compose Postgres, covering both delete paths.

## Acceptance criteria

- [x] Full CRUD + hard-delete + dispose round-trips via `curl`, documented in a
      `requests.http` file.
- [x] `grep -riE "kafka|redis|elasticsearch" src/cdc_pipeline/api/` returns nothing — the
      ignorance property, checkable.
- [x] Hard-deleting a non-`registered` specimen is rejected (the state machine holds).
- [x] p50 write latency under light load measured and noted in field notes (baseline for
      later comparisons).

## What this teaches

Little that's new in the CRUD — deliberately. The lesson is architectural restraint (the
whole pipeline's correctness rests on a service that never heard of it) plus modeling a
domain-authentic state machine whose transitions map to distinct CDC event shapes.
