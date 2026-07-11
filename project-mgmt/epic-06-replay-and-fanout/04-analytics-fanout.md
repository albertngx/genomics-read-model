# Story 06.4 — The late consumer (analytics fan-out)

- **Status:** todo
- **Effort:** M
- **Depends on:** 06.1 (deliberately scheduled last in the epic — the lateness is the point)

## Context

ARCHITECTURE.md §2: the analytics consumer's late arrival *is* the fan-out proof. A
consumer nobody planned for at producer-design time — change events → DuckDB, answering
questions the OLTP schema is the wrong shape for — added with zero changes to the tracking
service, the connector, or the topics. Tag the repo `pre-analytics` before starting; the
empty diff against that tag is the deliverable.

## Tasks

- [ ] `consumers/analytics.py` on the shared base: group `analytics`, both topics, from
      earliest — the compacted log **is** its initial snapshot. No backfill job, no
      producer coordination, no ceremony.
- [ ] DuckDB file with current-state tables `runs` and `specimens` (row fields + `_lsn`),
      upserted with an LSN guard: `INSERT ... ON CONFLICT DO UPDATE ... WHERE
      excluded._lsn > _lsn`. Third store, third idempotency mechanism, same guarantee
      (§5).
- [ ] Delete lifecycle: hard deletes remove the row; disposed specimens stay — dispose rate
      is a metric, and analytics wants the full active-plus-disposed picture.
- [ ] `make analytics-report`: three or four real queries printed as tables — specimens per
      sequencer per day, status funnel, dispose rate by assay, hottest runs. Questions a
      lab manager would actually ask.
- [ ] **The proof, captured:** `git diff pre-analytics..HEAD -- src/cdc_pipeline/api
      deploy/connectors` is empty. Paste the (empty) output into field notes; it reappears
      in the final README.
- [ ] Parity spot-check vs Postgres (counts + sampled hashes) after drain, and the kill -9
      redelivery test — the money test, third store edition.

## Acceptance criteria

- [ ] A fresh `analytics` group consuming from offset 0 produces a full current-state
      replica; parity spot-check clean.
- [ ] Kill -9 under load, restart: redelivery absorbed by the DuckDB LSN guard,
      spot-check clean.
- [ ] The empty-diff proof is captured in field notes.
- [ ] `make analytics-report` returns plausible numbers under a seeded workload.

## What this teaches

What fan-out buys, made falsifiable: a new read model at zero producer cost, bootstrapped
entirely from a compacted topic. And a third variation on version-guarded idempotency —
which by now should feel like a pattern you own, not a trick you copied.
