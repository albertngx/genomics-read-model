# Story 02.3 — Load generator

- **Status:** todo
- **Effort:** L
- **Depends on:** 02.2

## Context

Without sustained realistic load, every later claim ("idempotent under redelivery", "no
loss during rebalance", "rebuild in N seconds") is untested marketing. This tool is the
workhorse of epics 04–07 and directly extends your production perf-testing experience.

## Tasks

- [ ] Typer CLI (`cdc-load`): async workers driving the **API** (never the DB directly).
- [ ] Configurable: target rate (steady or ramp), duration, operation mix (default 70%
      specimen update / 15% insert / 10% dispose / 5% registered hard-delete), concurrency.
      Specimen updates should walk the status lifecycle (registered → in_prep → … ) so events
      look like real lab throughput.
- [ ] **Zipfian key selection** for updates — hot specimens/runs are what make per-key
      ordering and cache behavior interesting; uniform load hides everything.
- [ ] Named scenarios:
      - `steady` — the default mix at N/s,
      - `delete-wave` — burst of registered hard-deletes (tombstone pressure for epic 04),
      - `reassignment-storm` — sequencer re-assignments / protocol amendments on hot runs
        under specimen load (the §7 denormalization problem),
      - `burst` — 10× rate spikes (consumer lag becomes visible).
- [ ] Live readout: achieved req/s, error rate, API latency p50/p99; final summary block
      suitable for pasting into field notes.

## Acceptance criteria

- [ ] Sustains 1k writes/s against the compose stack for 5 minutes with <0.1% errors (tune
      API/Postgres if not — that tuning is part of the story).
- [ ] Scenario runs are reproducible via seeded RNG (`--seed`), so demos behave the same on
      camera as in rehearsal.
- [ ] `make load SCENARIO=steady RATE=1000` works.

## What this teaches

Workload modeling — rate vs. concurrency, key skew, why reproducibility (seeding) is a
feature — and it quietly builds the harness that phase 2's Debezium-vs-own-connector diff
will reuse verbatim.
