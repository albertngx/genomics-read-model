# Story 04.2 — Idempotent upserts and deletes

- **Status:** todo
- **Effort:** L
- **Depends on:** 04.1

## Context

At-least-once delivery is now a measured fact (story 03.4). This story makes redelivery
harmless: every write to Elasticsearch is version-guarded by the event's `source.lsn`, so
replaying any suffix of the log is a no-op for already-applied changes. Deletes are the
half tutorials skip — and this domain has *two* delete paths that both resolve to "remove
from index" (ARCHITECTURE.md §3, §7): a registered-specimen **hard DELETE** (`op:"d"`) and a
specimen **disposal** (`UPDATE status='disposed'`).

## Tasks

- [ ] Index mapping for `specimens` (explicit, in repo — no dynamic mapping): analyzed
      `label`/`description`, keyword `barcode`/`specimen_type`/`assay`/`status`, numeric
      `volume_ul` (parsed from the D6 string), `run_id`, plus the denormalized run fields
      (populated properly in story 04.4).
- [ ] Upsert path: index with `version_type=external`, `version=<source.lsn>`. Version
      conflicts (HTTP 409) are logged at debug and counted as `duplicates_skipped` — they
      are success, not error.
- [ ] Delete path #1 — hard delete: handle `op:"d"` with a versioned delete; ignore the
      tombstone record itself (null value — it's for compaction, skip before deserialization).
- [ ] Delete path #2 — disposal: an `op:"u"` moving `status` to `disposed` removes the
      document from the active index (versioned delete). Both paths converge on the same
      internal `remove_from_index(id, lsn)` — write it once.
- [ ] Snapshot events (`op:"r"`) go through the same upsert path — rebuild and live
      streaming are deliberately the same code. (A specimen already `disposed` at snapshot
      time must end up absent from the index; check this — it's an easy miss.)
- [ ] Redelivery test: process a captured event batch twice; assert identical index state
      and non-zero `duplicates_skipped`.
- [ ] Delete tests: (a) register → hard delete; (b) advance to completed → dispose. Document
      appears then disappears in both; replaying the events changes nothing.

## Acceptance criteria

- [ ] `make load SCENARIO=delete-wave` for 2 minutes: spot-check script (PG active-set vs
      ES counts + sampled content) shows zero drift, where "active" excludes hard-deleted
      and disposed specimens.
- [ ] Kill the indexer mid-load (kill -9, not SIGTERM), restart: redelivered batch is
      absorbed, `duplicates_skipped > 0`, spot-check still clean. This is the story's money
      test.

## What this teaches

Idempotency as a *construction* (monotonic version guards) rather than a hope, and the
delete lifecycle in its full messiness — including the read-model insight that "deleted in
the projection" and "deleted in the source" are different events that must map to the same
outcome.
