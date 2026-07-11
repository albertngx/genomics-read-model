# 01 — Postgres logical replication: `wal_level` and `REPLICA IDENTITY`

*Context: story 01.1 (compose stack). These are the two Postgres settings the whole
CDC pipeline depends on. Get them wrong and epic 03 fails in confusing ways — the
connector either won't start or emits change events that are missing data.*

## Background: what the WAL is

Every change to a Postgres database is first written to the **Write-Ahead Log (WAL)** —
an append-only journal on disk — *before* the data files are touched. It exists for
crash recovery (replay the WAL to get back to a consistent state) and for replication
(ship the WAL to another server). Change Data Capture piggybacks on this: instead of
polling tables or adding triggers, Debezium **reads the WAL** and turns each recorded
row change into a Kafka message. The database's own durability log becomes our change
stream — that's the elegant core of the whole design.

But the WAL has *levels* of detail, and by default it doesn't record enough for CDC.

## `wal_level = logical`

`wal_level` controls how much information Postgres writes to the WAL:

| Level | Records | Enough for |
|---|---|---|
| `minimal` | Just what crash recovery needs | Single-server recovery |
| `replica` (default) | + info to stream the WAL byte-for-byte | Physical (binary) replicas, PITR |
| **`logical`** | + info to **decode** the WAL into logical row changes | **Logical replication / CDC** |

Physical replication ships raw block changes — a replica is a byte-identical copy, and
you can't ask it "which rows in the `specimens` table changed?" **Logical** replication
adds enough metadata that a decoder can reconstruct row-level events —
`INSERT id=42, barcode=…`, `UPDATE …`, `DELETE …` — independent of on-disk layout.
Debezium consumes exactly that logical stream.

**Why we must set it:** it's the one non-negotiable requirement. With `wal_level=replica`
(the default), creating a logical replication slot fails outright, and the Debezium
connector cannot start. It's a **server-wide** setting requiring a **restart** — which is
why it lives in the compose file (`postgres -c wal_level=logical`) from day one, not
something we bolt on later.

Two companion settings ride along, both raised to 8 in the compose file:
- **`max_wal_senders`** — how many processes can stream WAL at once. Each logical
  replication connection (Debezium is one) uses a WAL sender.
- **`max_replication_slots`** — replication slots are a **first-class, finite server
  resource**, not free-floating cursors. A *slot* remembers how far a consumer has read
  so Postgres knows which WAL is still needed. The default (10 in PG16) is plenty; we set
  8 explicitly to make the requirement visible and self-documenting. ≥4 is the story's bar.

> ⚠️ The classic CDC production incident (ARCHITECTURE §8): a replication slot whose
> consumer has gone away still pins the WAL. Postgres **cannot recycle WAL past the
> slot's position**, so the disk fills until the server stops. A slot is a promise to
> keep WAL around — abandon it and you've promised forever. We drill this in epic 03.

Verify it's on:

```bash
docker compose -f deploy/docker-compose.yml exec postgres \
  psql -U cdc -d genomics -c "SHOW wal_level;"   # → logical
```

## `REPLICA IDENTITY FULL`

This one is **per-table**, so it doesn't apply yet in 01.1 (no tables exist until
epic 02) — but it's the other half of the story and the compose stack is built to
support it, so it's worth understanding now.

`REPLICA IDENTITY` controls **what Postgres writes to the WAL for an `UPDATE` or `DELETE`** —
specifically, how much of the *old* row (the "before image") is logged:

| Setting | Old-row data logged on UPDATE/DELETE |
|---|---|
| `DEFAULT` | Only the **primary key** columns |
| `USING INDEX idx` | The columns of a chosen unique index |
| `NOTHING` | Nothing |
| **`FULL`** | **The entire old row, every column** |

With the default, a `DELETE` event tells you only *which key* was deleted, and an
`UPDATE` gives you the new values but only the key from the old row. That's often not
enough for a downstream consumer or a verifier that wants to reason about *before → after*.

**Why we choose `FULL`** (recorded as decision **D4**):

- **Complete before-images.** Every update and delete carries the full prior row.
  The **verifier** (our correctness oracle, epic 06) and the consumers can compare old
  vs. new state and reason about exactly what changed — no guessing, no re-reading the
  source.
- **Deletes are self-describing.** A hard-deleted `registered` specimen (ARCHITECTURE §3)
  produces a delete event containing the whole row that was removed, which the search
  indexer and cache consumer use to do the right projection-delete.

**The cost** (an honest trade-off, not a free lunch): `FULL` means every UPDATE/DELETE
writes the *entire old row* to the WAL, so **WAL volume grows**. On a busy production
table that's a real tuning decision. Here it's acceptable and deliberately chosen for
correctness and inspectability — and knowing this knob exists (and what it costs) is
itself a production-grade CDC lesson.

It gets set per captured table in epic 02/03, e.g.:

```sql
ALTER TABLE specimens REPLICA IDENTITY FULL;
ALTER TABLE runs      REPLICA IDENTITY FULL;
```

## Takeaway

- `wal_level=logical` — **server-wide, set now, requires restart.** Without it there is
  no CDC at all. This is why it's baked into the compose file.
- `REPLICA IDENTITY FULL` — **per-table, set in epic 02/03.** Decides how complete the
  before-image is; we pay extra WAL for complete old-row data because the verifier and
  consumers want it. That's D4.
- A **replication slot** is a finite server resource that pins WAL until consumed —
  respect it, or it fills the disk.
