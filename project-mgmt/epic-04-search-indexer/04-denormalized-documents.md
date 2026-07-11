# Story 04.4 — Denormalized documents (the cross-topic join)

- **Status:** todo
- **Effort:** L
- **Depends on:** 04.3

## Context

ARCHITECTURE.md §7 — the hardest and most instructive problem in the epic. Search wants
run fields (`sequencer`, `assay_protocol`, `operator`) inside the specimen document; run
changes arrive on a different topic with no cross-topic ordering guarantee. This is a
stateful stream join done by hand, and the `reassignment-storm` scenario exists to break
naive versions of it.

## Tasks

- [ ] Indexer subscribes to both topics; maintains a local run lookup
      (id → {sequencer, assay_protocol, operator, lsn}). Decide and document where it lives
      — an in-memory dict rebuilt from the compacted `cdc.public.runs` topic on startup is
      the honest simple answer (compacted topics as bootstrap state is itself a pattern
      worth knowing).
- [ ] Specimen events enrich from the lookup. Unknown run (race: specimen arrived first):
      index with run fields null + mark the doc `_pending_enrichment`, never block the
      partition (§7 policy).
- [ ] Run change events (sequencer re-assignment / protocol amendment): update the lookup,
      then repair — update-by-query on Elasticsearch setting the new values for `run_id=X`
      (decision D5), including any `_pending_enrichment` docs.
- [ ] Think through and document the repair/versioning interaction: update-by-query bypasses
      per-doc LSN guards — why is that acceptable here? (Run LSN ordering + idempotent
      repair. Write the argument down; it's a good one to have to make.)
- [ ] Test with `reassignment-storm`: hot-run sequencer re-assignments under 1k/s specimen
      load.

## Acceptance criteria

- [ ] After a `reassignment-storm` run and drain, zero documents carry a stale run field and
      zero remain `_pending_enrichment` (script-checked).
- [ ] The race is demonstrated, not just handled: a test contrives specimen-before-run arrival
      and shows placeholder → repaired.
- [ ] §7's policy questions all have written answers in the story or field notes.

## What this teaches

Stateful stream processing without a framework: cross-stream races, join-state bootstrap
from compacted topics, repair strategies — the fundamental reason tools like Kafka
Streams/Flink exist, learned by needing one. In domain terms: keeping the specimen search
index consistent when a sequencer goes down and its run is re-assigned — a real lab event.
