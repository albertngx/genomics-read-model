# Story 02.1 — Schema and migrations

- **Status:** todo
- **Effort:** M
- **Depends on:** epic 01

## Context

The schema from ARCHITECTURE.md §3: `runs` (parent) and `specimens` (child), with its three
deliberate difficulties — hard deletes on registered specimens (tombstone handling), soft
disposal of specimens (projection-level delete), and the denormalization-bait `run_id` FK.
This is also where the CDC-specific table configuration lives.

## Tasks

- [ ] Alembic migrations for `runs` and `specimens` exactly as in ARCHITECTURE.md §3. Keep
      `volume_ul NUMERIC(10,2)` — do not "simplify" to float (decision D6). Model
      `specimens.status` as `registered | in_prep | on_instrument | completed | disposed` and
      `runs.status` as `planned | active | completed | aborted` (CHECK or enum).
- [ ] `ALTER TABLE ... REPLICA IDENTITY FULL` on both tables, in the migration itself with
      a comment explaining why (D4).
- [ ] Create the Debezium publication in a migration:
      `CREATE PUBLICATION cdc_pub FOR TABLE runs, specimens` — the schema owns what is
      captured, not connector config.
- [ ] Seed script: ~30 runs across a few sequencers/assays, ~5,000 specimens with plausible
      labels/descriptions, specimen types, and assays (e.g. WGS PCR-free, WES, RNA-seq).
      Searchable text matters — the Elasticsearch demos need queries that feel real (e.g.
      "PCR-free WGS on sequencer SEQ-07 awaiting library prep"). Mix of statuses, including
      some `registered` (hard-deletable) and some `disposed`.
- [ ] `make seed` = migrate + seed, idempotent.

## Acceptance criteria

- [ ] `make seed` from a fresh stack; row counts correct; re-running changes nothing.
- [ ] `\d+ specimens` shows replica identity FULL; `\dRp+` shows both tables in `cdc_pub`.
- [ ] Before closing: run `SELECT * FROM pg_publication_tables` and understand what a
      publication actually is.

## What this teaches

The Postgres side of CDC: publications, replica identity and its WAL-volume cost, and why
the *schema migration* is the right home for capture configuration. Plus modeling a specimen
status lifecycle that drives two different projection-delete paths downstream.
