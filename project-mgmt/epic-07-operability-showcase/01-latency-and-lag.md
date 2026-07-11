# Story 07.1 — Latency and lag measurement

- **Status:** todo
- **Effort:** M
- **Depends on:** epic 06

## Context

Every claim so far has been about correctness; this story is about performance honesty.
`source.ts_ms` (documented in story 03.2) is the clock: end-to-end latency is the event's
source timestamp → the change visible in the derived store. The numbers built here are
the ones an operator would be paged on — and they populate the final README's table.

## Tasks

- [ ] Per-consumer end-to-end latency instrumentation: `source.ts_ms` → post-sink-write,
      as a histogram with p50/p99 logged periodically. Document the single-host clock
      caveat (everything shares a clock here; in production this needs care).
- [ ] `make status`: one readout combining consumer-group lag per group (admin API or
      parsed `kafka-consumer-groups.sh --describe`), replication-slot lag (reuse story
      03.4's watchdog), and each consumer's latest latency snapshot.
- [ ] Steady-state matrix: 500 / 1000 / 2000 writes/s × three consumers → p50/p99 latency
      and peak lag. Field notes now; the final README table in story 07.3.
- [ ] **Find the knee:** raise the rate until the weakest consumer stops keeping up.
      Identify the bottleneck by measurement, not vibes — sink round-trips? consumer CPU?
      batch sizing? — make one tuning change, and show before/after numbers.
- [ ] Re-run the `burst` scenario watching `make status`: lag grows, then drains — the
      shock-absorber story from 04.3, now visible at system level across all consumers.

## Acceptance criteria

- [ ] The matrix exists with real numbers; anomalies are explained or filed as issues.
- [ ] The knee is found and written up: rate, bottleneck named, one tuning delta with
      before/after measurements.
- [ ] `make status` shows sensible values in all three states: at rest, under steady load,
      and during a burst drain.

## What this teaches

Performance measurement as a first-class deliverable: establishing the numbers you'd be
paged about before anyone is paged, and the difference between a benchmark and an honest
number (stated conditions, known bottleneck, reproducible run).
