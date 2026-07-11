# Story 05.2 — CAS idempotency and the delete lifecycle

- **Status:** todo
- **Effort:** L
- **Depends on:** 05.1

## Context

Elasticsearch handed us idempotency via `version_type=external`; Redis hands us nothing.
This story builds the monotonic guard by hand (ARCHITECTURE.md §5): a Lua compare-and-set
that rejects any write whose LSN is ≤ the stored one. Same delete subtleties as story
04.2 — with one policy difference from the search index that deserves its own decision.

## Tasks

- [ ] Lua `cas_write`: if the stored `_lsn` is absent or numerically less than the event
      LSN, write the full hash + `_lsn`; otherwise reject. Rejections are counted as
      `duplicates_skipped` and are success, not error. Load via `SCRIPT LOAD`/`EVALSHA`
      with a `NOSCRIPT` fallback.
- [ ] Lua `cas_delete`: delete only if the stored `_lsn` is less than the event LSN.
      Hard deletes (`op:"d"`) go through it; the tombstone record itself (null value) is
      skipped before deserialization, as in 04.2.
- [ ] **Disposal policy — decide and record.** Unlike the search projection (§7), disposal
      here is a plain update: a point lookup on a disposed specimen must still resolve —
      it is a chain-of-custody record. One source event, two projection outcomes across
      stores, both correct. This is architecture-level: add it to the ARCHITECTURE.md
      decision log (D9) per the working agreement, then implement.
- [ ] Snapshot events (`op:"r"`) go through the same upsert path — rebuild and live
      streaming are the same code, as everywhere else.
- [ ] Batching via pipelined `EVALSHA`; offsets committed only after all pipeline results
      are checked — the base-class contract from 04.3, honored here too.
- [ ] Redelivery test: apply a captured batch twice; assert identical Redis state and
      non-zero `duplicates_skipped`.
- [ ] Kill -9 mid-load, restart: the money test, third store edition.

## Acceptance criteria

- [ ] `make load SCENARIO=delete-wave` for 2 minutes: PG vs Redis spot-check shows zero
      drift, where "present in Redis" means every non-hard-deleted row — *including*
      disposed specimens (the D9 policy, checkable).
- [ ] Kill -9 under load, restart: redelivery absorbed, `duplicates_skipped > 0`,
      spot-check clean.
- [ ] Sustained consumer throughput ≥ the generator rate with the pipeline batching on;
      the number goes in field notes (Redis should never be the bottleneck — if it is,
      that's a finding).

## What this teaches

Building the idempotency primitive yourself when the store doesn't provide one — the same
guarantee as epic 04 through entirely different mechanics (the analytics consumer in epic
06 will make it three) — and that projection policy is per-store: "deleted in the source"
deliberately fans out to different outcomes in different read models.
