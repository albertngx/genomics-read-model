# Story 06.3 — Zero-downtime reindex

- **Status:** todo
- **Effort:** L
- **Depends on:** 06.2

## Context

ARCHITECTURE.md §6 demo 2: the derivation logic changed. The production answer is replay
into a brand-new index under a new consumer group, verify, then flip an alias atomically —
blue/green for read models. In a regulated lab, "reindex without a maintenance window" is
a genuine operational win, and downtime here is *measured* by a live probe, not asserted.

## Tasks

- [ ] Introduce a real mapping change — something visible in query results (e.g. a new
      analyzer for `label`, or prefix search on `barcode`) — so the flip demonstrably
      changed behavior, not just metadata.
- [ ] All clients (demo scripts, the verifier's ES leg) query via an alias `specimens`. If
      story 04.2 created a bare index, retrofit the alias first as its own commit — that
      retrofit is itself the lesson in why you start with an alias.
- [ ] Create `specimens_v2` with the new mapping; run a second indexer instance under a new
      group `search-indexer-v2` from earliest into it; wait for drain; verify v2 (the
      verifier parameterized by target index).
- [ ] **Continuous query probe** for the entire procedure: 1 query/s against the alias,
      asserting zero errors and no empty-result window. This is the "zero downtime" in
      the story title, quantified.
- [ ] Atomic alias flip: one `_aliases` call with remove + add. Old group and v1 retired
      only after a soak period; the rollback (flip back) documented and rehearsed once.
- [ ] Runbook section: the full procedure including rollback.

## Acceptance criteria

- [ ] The probe reports zero failed queries and no gap across the entire flip.
- [ ] The new mapping's behavior is demonstrably live post-flip (one query that behaves
      differently, shown before/after).
- [ ] `make verify` green against v2 both before and after the flip; v1 deleted at the
      end; the rebuild demo (06.2) still passes against the new layout.

## What this teaches

The production pattern for evolving a derived schema — replay-into-new beats
migrate-in-place because the log makes the new index a pure function you can verify before
anyone sees it — plus Elasticsearch alias discipline and what "atomic cutover" actually
requires.
