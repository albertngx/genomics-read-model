# Story 06.2 — Rebuild from log

- **Status:** todo
- **Effort:** M
- **Depends on:** 06.1

## Context

The headline claim from the project README, executed: delete the derived store, replay the
log, arrive at state identical to Postgres — affordable because of compaction (D3),
correct because of per-key ordering (§4) and because snapshot and live events flow through
the same consumer code (story 04.2). If this demo works, the architecture works.

## Tasks

- [ ] `make demo-rebuild`: stop the indexer → delete the ES index → reset the
      `search-indexer` group to earliest (`kafka-consumer-groups.sh --reset-offsets
      --to-earliest`) → restart → wait for lag zero → `make verify` strict. Scripted with
      narrated echo output, timed end to end.
- [ ] Same procedure for Redis: `FLUSHDB` + reset the `cache-consumer` group → rebuild →
      verify. One derived store's rebuild must not require touching the other.
- [ ] Record the numbers: events replayed vs. live keys (the observed compaction ratio),
      wall-clock rebuild time, events/s during replay — field notes now, final README in
      epic 07.
- [ ] **The flex:** rebuild the ES index while the generator keeps running; finish with
      `--converge` verification. This also exercises the run-lookup cold bootstrap and
      `_pending_enrichment` repair from story 04.4 under realistic conditions.
- [ ] Runbook section: when to rebuild in place (this story) vs. reindex into a new index
      (story 06.3).

## Acceptance criteria

- [ ] `make demo-rebuild` green from a dirty stack (e.g. straight after a chaos run),
      twice in a row.
- [ ] The under-load rebuild passes with `--converge`.
- [ ] The numbers table (compaction ratio, rebuild time, replay throughput) is written up
      in field notes.

## What this teaches

The log as rebuildable truth — the entire point of the architecture, executed as a routine
scripted operation rather than a slide bullet. The compaction-ratio measurement makes D3
concrete: replay cost scales with the keyspace, not with history.
