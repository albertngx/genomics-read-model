# Story 07.2 — Chaos drill suite

- **Status:** todo
- **Effort:** L
- **Depends on:** 07.1

## Context

ARCHITECTURE.md §8 is a table of promises; this story collects on them. Individual drills
already exist across the epics (03.4's restarts and slot lag, 04.5's poison loop, the
kill -9 money tests) — here they become one scripted, repeatable suite in which every
drill ends with the same sentence: *the verifier reports zero diffs*.

## Tasks

- [ ] `tools/chaos/` + `make chaos`: each drill = inject failure under
      `steady RATE=1000` → observe → drain → `make verify` strict. Standard per-drill
      report: what was injected, duration, duplicates absorbed, peak lag, verifier
      verdict.
- [ ] Drills:
      1. kill -9 each consumer mid-batch (indexer, cache, analytics),
      2. rebalance under load: scale the indexer 1 → 3 → 1,
      3. broker restart,
      4. Connect restart (03.4 drill 1, now suite-ified),
      5. Elasticsearch down 60s (transient ≠ poison, from 04.5),
      6. Redis down 60s,
      7. poison batch → DLQ → re-inject (wraps `make demo-dlq`).
- [ ] **§8 coverage matrix** (in this epic's README or `docs/`): failure mode → drill →
      story where first exercised → suite-covered. The working agreement — every §8 row
      exercised before phase 1 closes — is enforced by this table having no blanks.
- [ ] Flakiness policy: the suite must pass twice consecutively. Any flake is a bug
      (timing assumption, insufficient drain wait) and gets fixed, never retried into
      submission.

## Acceptance criteria

- [ ] `make chaos` green twice in a row from a fresh `make up seed connector`.
- [ ] The coverage matrix is complete — every §8 failure mode maps to a passing drill.
- [ ] Per-drill reports show non-trivial evidence the failure actually happened
      (duplicates > 0 where redelivery is expected, a visible lag spike during the broker
      restart). A drill that can't show its failure occurred doesn't count.

## What this teaches

Chaos engineering in miniature: failure injection paired with an oracle, drills as
regression tests for *operational* properties, and the subtle requirement that a drill
must prove its failure happened — otherwise it only proves the script ran.
