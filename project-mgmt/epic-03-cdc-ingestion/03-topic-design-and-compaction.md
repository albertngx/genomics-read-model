# Story 03.3 — Topic design and compaction

- **Status:** todo
- **Effort:** M
- **Depends on:** 03.1

## Context

Implements ARCHITECTURE.md §4 and decision D3. Compaction is what makes "replay from
offset 0" affordable forever; per-key partitioning is what makes it correct. Neither is
a default — both must be configured and then *demonstrated*.

## Tasks

- [ ] Pre-create topics with explicit config (compose init or `make topics`, via
      `kafka-topics.sh`): `cdc.public.specimens` (6 partitions, `cleanup.policy=compact`),
      `cdc.public.runs` (3, compact). Explicit creation, not broker auto-create —
      auto-created topics get default partitions and delete-policy, silently breaking D3.
- [ ] Tune compaction to be observable in a lab session (`segment.ms`,
      `min.cleanable.dirty.ratio` low enough that compaction actually runs — note the
      values and why in field notes).
- [ ] **Ordering demo:** with the generator's zipfian updates running, consume one hot
      key's partition and assert LSN-monotonicity per key. Then document (no need to
      execute) what re-keying by, say, `run_id` would break.
- [ ] **Compaction demo:** update one specimen 1,000 times, wait for compaction, show the
      partition retains ~1 event for that key; hard-delete a registered specimen, show the
      tombstone and its eventual disappearance.

## Acceptance criteria

- [ ] `kafka-topics.sh --describe` output for both topics captured in field notes,
      matching §4.
- [ ] Ordering demo passes as a small pytest (it becomes a permanent regression test).
- [ ] Compaction demo observed and written up with actual segment/offset numbers.

## What this teaches

Kafka's storage model for real: partitions as the unit of ordering, compaction as
retention-by-key, tombstone lifecycle — the mental model every consumer decision in
epic 04 rests on.
