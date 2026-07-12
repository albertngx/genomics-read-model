# 0003 — CDC capture configuration lives in schema migrations, not connector config

- **Status:** Accepted
- **Date:** 2026-07-12
- **Context story:** 02.1 (schema and migrations)

## Context

CDC needs two pieces of *per-table* configuration on the Postgres side, beyond the
server-wide `wal_level=logical` that already lives in compose:

1. **`REPLICA IDENTITY FULL`** on each captured table, so UPDATE/DELETE WAL records carry
   the complete before-image (decision D4).
2. A **publication** — `CREATE PUBLICATION cdc_pub FOR TABLE runs, specimens` — the
   server-side declaration of which tables' changes are offered on the logical stream.

Both could plausibly be owned by Debezium instead of the schema. Debezium can auto-create a
publication from its `table.include.list` (`publication.autocreate.mode`), and teams often
treat "which tables are captured" as connector configuration. This ADR records the choice
to put both in the Alembic migration alongside the `CREATE TABLE`s.

## Decision

**The schema migration owns CDC capture configuration.** Migration `0002` issues the
`ALTER TABLE … REPLICA IDENTITY FULL` and `CREATE PUBLICATION cdc_pub FOR TABLE …`
statements. The Debezium connector (epic 03) will run with
`publication.autocreate.mode=disabled` and `publication.name=cdc_pub`, consuming a
publication it does not own.

## Rationale

- **What's captured is a property of the schema.** Adding a table to CDC is a schema
  change — it belongs in a reviewed migration with a git history, next to the tables it
  names, not in a connector JSON field that no code review sees.
- **Least-privileged connector.** Publication auto-create requires the Debezium role to own
  the tables (or be superuser). Pre-creating the publication lets the connector be a
  read-only consumer of the stream.
- **Reconstructible source.** `make seed` from nothing produces a database that is *already*
  a correct CDC source — replica identity set, publication present — before Debezium is
  pointed at it. Capture config is never a manual post-step to forget.
- **`CREATE PUBLICATION` has no `IF NOT EXISTS`.** It is naturally single-shot; Alembic's
  applied-revision tracking is exactly the right mechanism to run it once, whereas an
  idempotent "ensure" script or connector auto-create would have to special-case it.

## Consequences

- Migration `0002` is Postgres-specific (raw `op.execute` DDL for publication and replica
  identity). That's acceptable — this project is Postgres-only by design, and the DDL is
  self-documenting.
- Enabling CDC on a new table is a two-line migration (`REPLICA IDENTITY FULL` +
  `ALTER PUBLICATION cdc_pub ADD TABLE …`), reviewed like any schema change.
- Epic 03's connector config must match the pre-created publication name and disable
  auto-create; a mismatch is a wiring bug caught there.
- The `alembic_version` table now lives in the source database. It is not captured (not in
  `cdc_pub`), so it never appears on the change stream.

## Alternatives considered

- **Debezium auto-creates the publication (`publication.autocreate.mode=filtered`):**
  rejected. It requires table ownership for the connector, hides "what's captured" in
  connector config outside version control, and splits capture config across two homes
  (replica identity would still be a migration/DDL concern).
- **A standalone SQL bootstrap script run after migrations:** rejected. It duplicates what
  Alembic already does (ordered, tracked, one-shot application) and reintroduces the
  `CREATE PUBLICATION` idempotency problem the migration framework solves for free.
