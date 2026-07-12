# 02 — Postgres publications, and why capture config lives in the migration

*Context: story 02.1 (schema and migrations). The schema lands, and with it the two
pieces of CDC config that are per-table rather than server-wide: `REPLICA IDENTITY FULL`
(covered conceptually in [01](01-postgres-logical-replication.md)) and the **publication**.
This note is about the publication — the thing the acceptance criteria tell you to stop
and actually understand — and about the deliberate choice to put both in the Alembic
migration.*

## What a publication actually is

A **publication** is a named, server-side set of tables whose row changes Postgres offers
to logical-replication subscribers. It is the *supply side* of logical replication: the
database declaring "these tables' changes are available on the logical stream." It is not
a process, not a connection, not a copy of data — just a catalog entry naming tables (and,
optionally, which operations and a row filter).

You create one with plain DDL:

```sql
CREATE PUBLICATION cdc_pub FOR TABLE runs, specimens;
```

Debezium (epic 03) is one *subscriber* to that publication: it creates a replication slot
(the position cursor — see [01](01-postgres-logical-replication.md)) and streams the
changes for exactly the tables the publication names. Slot and publication are different
things and often confused:

- **Publication** = *what* is captured (which tables/operations). Shared, declarative,
  cheap. Lives in the catalog.
- **Replication slot** = *how far* a given consumer has read. Per-consumer, stateful, and
  the dangerous one (it pins WAL until consumed).

Inspect what's there on the running stack:

```
\dRp+                              -- publications and their tables (psql meta-command)
SELECT * FROM pg_publication_tables;   -- one row per (publication, table), with attnames
```

`\dRp+` shows `cdc_pub` owns `public.runs` and `public.specimens` and captures all four of
insert/update/delete/truncate. `pg_publication_tables` additionally lists the captured
column names — a good sanity check that the whole row is in scope.

`relreplident` in `pg_class` is where replica identity shows up in the catalog: `d`
(default, PK only), `n` (nothing), `i` (using index), **`f` (full)**. After the migration
both tables read `f`:

```sql
SELECT relname, relreplident FROM pg_class WHERE relname IN ('runs','specimens');  -- → f, f
```

> One sharp edge worth knowing: `CREATE PUBLICATION` has **no `IF NOT EXISTS`** (unlike
> most `CREATE` statements). Re-running the migration would error — which is a non-issue
> here because Alembic records applied revisions and never re-runs one. That is *why* the
> publication belongs in a migration and not in an idempotent "ensure" script.

## Why the migration is the right home

The tempting alternative is to let the **connector** own capture: Debezium can auto-create
a publication (`publication.autocreate.mode`) from its `table.include.list`. We deliberately
don't. What's captured is a property of the **schema**, so it lives with the schema:

- **One authority, with history.** Adding a table to CDC becomes a reviewed schema change
  with a migration and a git history — not a field edited in connector JSON that no code
  review ever sees. `CREATE PUBLICATION … FOR TABLE runs, specimens` is right next to the
  `CREATE TABLE`s it refers to.
- **The connector can run least-privileged.** Auto-create needs the Debezium role to own
  the tables (or be superuser). With the publication pre-created by the migration, the
  connector only needs to *read* the stream — `publication.autocreate.mode=disabled`,
  `publication.name=cdc_pub` (epic 03).
- **Reconstructible.** `make seed` from nothing yields a database that is *already* a
  correct CDC source — replica identity set, publication present — before Debezium is ever
  pointed at it. Capture config isn't a manual post-step you can forget.

`REPLICA IDENTITY FULL` rides in the same migration for the same reason: it's per-table
capture config (how complete the before-image is — D4), so it belongs with the table, not
in server flags (unlike `wal_level`, which *is* server-wide and lives in compose).

## Takeaway

- A **publication** is a declarative catalog entry naming the tables whose changes are
  offered to logical-replication subscribers — the supply side. A **slot** is a consumer's
  read position. Don't conflate them.
- Verify with `\dRp+`, `pg_publication_tables`, and `relreplident = 'f'`.
- Capture config (`CREATE PUBLICATION`, `REPLICA IDENTITY FULL`) lives in the **schema
  migration**, not connector config: the schema owns what is captured, with review and
  history, and the connector stays a least-privileged reader. See
  [ADR-0003](../docs/adr/0003-cdc-capture-config-in-migrations.md).
