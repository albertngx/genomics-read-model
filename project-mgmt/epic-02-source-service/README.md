# Epic 02 — Source service and workload

**Goal:** the "boring by design" tracking service — schema, CRUD API, seed data — plus a
load generator that can produce realistic sustained write traffic. The service must know
nothing about the pipeline; the load generator is what will make lag, batching, and
failure behavior *observable* instead of theoretical in every later epic.

**Domain:** runs and specimens (ARCHITECTURE.md §3). The service is the sequencing-platform
stand-in — the system of record operators write to as specimens move through the lab.

**Done when:** the API serves CRUD on runs/specimens, and the generator sustains a
configurable ≥1k writes/s mixed workload against it with a live throughput readout.

## Stories

1. [Schema and migrations](01-schema-and-migrations.md) — tables, replica identity, seed (M)
2. [Tracking API](02-tracking-api.md) — FastAPI CRUD, deliberately pipeline-ignorant (M)
3. [Load generator](03-load-generator.md) — rates, mixes, scenarios, skew (L)

## Field notes

*(fill in as you build)*
