# Story 03.2 — Event anatomy

- **Status:** done
- **Effort:** M
- **Depends on:** 03.1
- **Deliverable:** [`docs/EVENTS.md`](../../docs/EVENTS.md)

## Context

Every consumer in epics 04–06 is a function over these events; misreading them there
means debugging in the worst place. This story produces `docs/EVENTS.md` — written from
*captured real events*, not from Debezium's documentation — which becomes the event
contract that phase 2's hand-written connector must reproduce.

## Tasks

- [x] Trigger and capture one of each: snapshot read (`op:"r"`), insert (`c`), update
      (`u`), delete (`d`), and the tombstone that follows the delete.
- [x] For each, annotate in `docs/EVENTS.md`: key structure, `before`/`after` payloads,
      `op`, and the `source` block — especially `source.lsn` (the idempotency token for
      every consumer, ARCHITECTURE.md §5) and `source.ts_ms` (the end-to-end latency
      clock for epic 07).
- [x] Document the D4 experiment: temporarily set `REPLICA IDENTITY DEFAULT`, capture an
      update/delete, show what disappears from `before`, revert. One paragraph on why
      FULL was chosen.
- [x] Document the D6 evidence: what `volume_ul` looks like as a string, and what
      `decimal.handling.mode=double` would have done to a value like `2048.75`.
- [x] Note the delete/tombstone two-event sequence and why both exist (consumers react
      to `op:"d"`; compaction needs the null-value tombstone).

## Acceptance criteria

- [x] `docs/EVENTS.md` exists with real captured payloads for all five event kinds,
      annotated field by field.
- [x] A reader could write a correct consumer from this document alone, without Debezium
      docs — that is its bar, and phase 2 will hold it to that bar literally.

## What this teaches

Deep familiarity with the change-event contract — the difference between "I configured
Debezium" and "I know exactly what it emits and why," which is the foundation for
rebuilding it later.
