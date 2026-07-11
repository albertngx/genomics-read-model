# Story 03.4 — Failure drills: restarts and slot lag

- **Status:** todo
- **Effort:** L
- **Depends on:** 03.1

## Context

Two of the failure modes from ARCHITECTURE.md §8, provoked on purpose. The second —
an abandoned replication slot forcing Postgres to retain WAL until the disk fills — is
*the* canonical CDC production incident; having caused and handled it in a lab is worth
more than any tutorial.

## Tasks

- [ ] **Drill 1 — connector restart under load:** run `make load`, kill the Connect
      container mid-stream, restart it. Observe: where does Debezium resume from
      (`confirmed_flush_lsn`)? Count events for a marked window of writes — expect
      possible duplicates, zero gaps. Write down the duplicate count: this is the
      at-least-once contract consumers must absorb.
- [ ] **Drill 2 — abandoned slot:** stop Connect entirely, keep the generator running.
      Watch `pg_replication_slots` / `pg_wal` disk usage grow. Document the math (WAL
      bytes retained per minute at N writes/s), then restart Connect and watch it drain.
- [ ] **Guardrail:** small watchdog script (`tools/slot_watch.py`) polling slot lag
      (`pg_current_wal_lsn() - confirmed_flush_lsn`), warning above a threshold. Runs
      as a compose service. This is the seed of the observability layer (phase 4).
- [ ] Runbook: `docs/RUNBOOK.md` section for each drill — symptom, diagnosis query,
      remediation, prevention.

## Acceptance criteria

- [ ] Both drills executed and written up with real numbers (resume LSN, duplicate
      count, WAL growth rate).
- [ ] Watchdog visibly alerts during a re-run of drill 2.
- [ ] Zero-gap claim in drill 1 verified by comparing a marked write set against
      consumed events, not by inspection.

## What this teaches

At-least-once semantics as an observed fact rather than a slogan, the slot/WAL retention
contract (which phase 2's hand-written connector must manage itself), and
incident-runbook discipline.
