# Story 03.1 — Connector deployment

- **Status:** done
- **Effort:** M
- **Depends on:** epic 02

## Context

Debezium runs inside Kafka Connect and is configured via a REST-registered JSON document.
Every field in that document is a decision; this story is about making them deliberately
rather than pasting a tutorial config.

## Tasks

- [x] Connector config under `deploy/connectors/genomics.json`, registered via
      `make connector` (idempotent PUT). Key choices, each with a one-line comment in an
      adjacent notes file:
      - `plugin.name=pgoutput`, `publication.name=cdc_pub` (from story 02.1),
        `slot.name=cdc_genomics`,
      - `topic.prefix=cdc` → topics `cdc.public.runs` / `cdc.public.specimens`,
      - `snapshot.mode=initial`,
      - `decimal.handling.mode=string` (decision D6),
      - `tombstones.on.delete=true` (needed by compaction, story 03.3),
      - JSON converter, `schemas.enable=false` (decision: inspectability, see §9).
- [x] Verify the initial snapshot: 5,000 seeded specimens appear as `op:"r"` events.
- [x] Run `make load` and watch live events in kafka-ui.
- [x] `make connector-status` (wraps the Connect REST status endpoint).

## Acceptance criteria

- [x] Fresh stack: `make seed connector` → snapshot events for every seeded run and
      specimen.
- [x] INSERT/UPDATE/DELETE via the API each produce a visible, correctly keyed event
      within ~1s.
- [x] `SELECT slot_name, active, confirmed_flush_lsn FROM pg_replication_slots` shows the
      slot active and advancing under load — check it, understand each column.

## What this teaches

The full CDC handshake: publication → slot → connector → topics, what an initial snapshot
is, and where Debezium's at-least-once behavior comes from (the confirmed-flush LSN — the
thing you'll implement yourself in phase 2).
