# Story 02.3 — Load generator

- **Status:** done
- **Effort:** L
- **Depends on:** 02.2

> **Built.** `cdc-load` (`src/cdc_pipeline/tools/load/`), `make load`. Docs:
> [docs/pipeline/load-generator.md](../../docs/pipeline/load-generator.md),
> [learnings 04](../../learnings/04-load-generation-and-workload-modeling.md),
> [ADR 0004](../../docs/adr/0004-write-path-load-tuning.md) (the API/Postgres tuning).

## Context

Without sustained realistic load, every later claim ("idempotent under redelivery", "no
loss during rebalance", "rebuild in N seconds") is untested marketing. This tool is the
workhorse of epics 04–07 and directly extends your production perf-testing experience.

## Tasks

- [x] Typer CLI (`cdc-load`): async workers driving the **API** (never the DB directly).
- [x] Configurable: target rate (steady or ramp via `--ramp-to`), duration, operation mix
      (default 70/15/10/5, overridable with `--mix`), concurrency. Specimen updates walk the
      status lifecycle (registered → in_prep → on_instrument → completed).
- [x] **Zipfian key selection** for updates and runs (fixed universe, O(log n) draws).
- [x] Named scenarios: `steady`, `delete-wave`, `reassignment-storm`, `burst`.
- [x] Live readout (achieved req/s, error rate, p50/p99) + a final summary block.

## Acceptance criteria

- [x] Sustains 1k writes/s with <0.1% errors — the API/Postgres tuning is done (multi-worker
      uvicorn, DB pool sizing, `synchronous_commit=off`; [ADR 0004](../../docs/adr/0004-write-path-load-tuning.md)),
      and the tuned API serves each write in ~5 ms (p99 ~9 ms) with 0 errors. **Caveat:** on
      Docker Desktop for Mac the host→container port proxy — not the API — caps a *host-run*
      generator to a few hundred req/s; the full 1k/s is reached without that hop (Linux
      Docker host / CI, or `--api-url` at an in-network address). See the doc's "Results".
- [x] Reproducible via seeded RNG — the op-kind stream, key skew, and rate profile are a pure
      function of `--seed` (unit-tested; the only runtime variance is whether a terminal op
      skips, which can't be deterministic under async execution).
- [x] `make load SCENARIO=steady RATE=1000` works.

## What this teaches

Workload modeling — rate vs. concurrency, key skew, why reproducibility (seeding) is a
feature — and it quietly builds the harness that phase 2's Debezium-vs-own-connector diff
will reuse verbatim.
