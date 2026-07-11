# Story 04.5 — Retries and DLQ

- **Status:** todo
- **Effort:** M
- **Depends on:** 04.3

## Context

A poison message — one that fails permanently — must neither block its partition forever
nor vanish silently (ARCHITECTURE.md §5, §8). This story builds the full operational
loop: bounded retries → dead-letter topic with forensics → human inspects → fix → re-inject.
The loop matters more than the DLQ itself; a DLQ nobody can drain is a landfill.

## Tasks

- [ ] Classify errors in the batch path: transient (Elasticsearch unavailable, timeouts) →
      retry with backoff, indefinitely if the sink is down (that's backpressure, not
      failure); permanent (deserialization, mapping conflicts, impossible values) → DLQ
      after N attempts.
- [ ] DLQ producer: original key/value plus headers — error class, message, stack hash,
      source topic/partition/offset, timestamp, consumer git SHA.
- [ ] Quarantine CLI (`cdc-dlq`): `list` (grouped by error class), `show <offset>`
      (decoded event + error), `reinject <offset|--all-matching>` (produce back to the
      source topic — idempotency guards make re-injection safe; say so in the help text).
- [ ] Poison injection for testing: a generator flag writing a specimen whose event the
      indexer's parser rejects (e.g. malformed via a test-only mapping conflict).
      Document the chosen mechanism.
- [ ] Drill: inject 10 poison events under load → partition keeps flowing → `cdc-dlq
      list` shows 10 → fix → `reinject` → verifier-clean.

## Acceptance criteria

- [ ] Under poison injection, healthy events on the same partition keep flowing (lag
      does not grow unboundedly) — the non-blocking property, measured.
- [ ] The full drill (inject → inspect → fix → re-inject → clean) is scripted as
      `make demo-dlq` and passes.
- [ ] Elasticsearch stopped for 60s under load: consumer retries, no DLQ entries (transient
      ≠ poison), catches up when it returns.

## What this teaches

Failure taxonomy (transient vs. permanent — most real outages are misclassified), and
operational tooling as part of the system rather than an afterthought. The re-inject
safety argument is the idempotency design paying rent.
