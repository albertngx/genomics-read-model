# Story 04.3 — Batching and backpressure

- **Status:** todo
- **Effort:** M
- **Depends on:** 04.2

## Context

Per-event round-trips to Elasticsearch cap throughput at a few hundred events/s; the
generator produces thousands. Batching via `_bulk` is the fix, but it changes the
commit-discipline math: the unit of atomicity is now the batch, and partial bulk
failures must be handled explicitly. This story is also the first real throughput
measurement — perf-testing muscle applied to a consumer.

## Tasks

- [ ] Batch accumulation: flush at N events or T ms, whichever first (both configurable;
      defaults chosen by measurement, not vibes).
- [ ] `_bulk` request with per-item error handling: a bulk response is 200 even when
      items failed. Version conflicts → `duplicates_skipped`; retryable item errors →
      bounded retry of the failed subset; permanent failures → park for story 04.5 (log
      loudly for now).
- [ ] Offsets committed only after the entire batch is settled (written / skipped /
      parked) — extend the base-class contract.
- [ ] Measure: sustained events/s and end-to-end p50/p99 (event `source.ts_ms` →
      document visible) at batch sizes 1 / 100 / 1000 under `make load RATE=2000`.
      Numbers go in field notes; this table reappears in the final README.
- [ ] Consumer lag under a `burst` scenario: watch lag grow and drain in kafka-ui (or
      `kafka-consumer-groups.sh --describe`); note drain rate — this is the backpressure
      story in practice (the log absorbs the spike; the consumer catches up at its own pace).

## Acceptance criteria

- [ ] ≥2k events/s sustained on the laptop with clean spot-checks afterwards.
- [ ] The batch-size measurement table exists with real numbers and a chosen default,
      justified in one sentence.
- [ ] kill -9 mid-bulk-flush, restart: no gaps, duplicates absorbed (re-run of the 04.2
      money test at full batch size).

## What this teaches

Throughput engineering on the consumer side — batching trade-offs (latency vs.
throughput), partial-failure handling in bulk APIs, and what "the log as a shock
absorber" actually looks like on a lag graph.
