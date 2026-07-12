# 05 — The CDC handshake: publication → slot → connector → topics

*Context: story 03.1 finally registers the Debezium connector. [Learning 03](03-kafka-connect-and-connectors.md)
explained what a connector *is* and its two interfaces in the abstract; this note is the
concrete handshake now that one is running — what happens between `make connector` and the
first event in kafka-ui, what the initial snapshot is, and how to read `pg_replication_slots`
to see the connector's promise to Postgres with your own eyes.*

## The four pieces, and the order they come together

Registering the connector is the moment four independently-built pieces click into one
pipeline. Three of them already existed before the connector did:

| Piece | Built in | What it is |
|---|---|---|
| **publication** `cdc_pub` | migration 0002 (epic 02) | the server-side *set of tables* whose changes are offered — a filter. Declarative, in the catalog, shared. |
| **replica identity** `FULL` | migration 0002 (epic 02) | a per-table property sizing the *before-image* each UPDATE/DELETE writes to the WAL. |
| **`wal_level=logical`** | compose (epic 01) | the server-wide switch that makes row-level change data exist on the WAL at all. |
| **replication slot** `cdc_genomics` | **created by the connector** on first start | this consumer's *cursor* into the WAL — where it has read to, and a promise Postgres keeps WAL for. |

The connector config ([`deploy/connectors/genomics.json`](../deploy/connectors/genomics.json))
is the thing that binds them: `plugin.name=pgoutput` + `publication.name=cdc_pub` +
`slot.name=cdc_genomics` says *"decode the WAL with pgoutput, filtered to cdc_pub, and
remember my position in a slot called cdc_genomics."* `publication.autocreate.mode=disabled`
is the deliberate refusal to let the connector invent the publication — the schema owns that
([learning 02](02-publications-and-capture-config.md)).

The slot did not exist until now. That's why the connector, not the migration, creates it:
a slot is *per-consumer state* (a position), not schema. Delete the connector and the slot
survives (it keeps pinning WAL); drop the slot and the connector must re-snapshot.

## Snapshot, then stream — and why `op:"r"`

`snapshot.mode=initial` means the connector runs in two phases the first time it sees a slot:

1. **Snapshot.** It `SELECT`s the current contents of `runs` and `specimens` and emits every
   row as an event with **`op:"r"`** — `r` for *read*, not *create*. This is the tell that an
   event came from the snapshot, not from a live INSERT (`op:"c"`). It's how a brand-new
   consumer starts from a *complete* picture instead of only future changes. On our seed, that
   is exactly 5,000 `op:"r"` events on `cdc.public.specimens`.
2. **Stream.** From the LSN the snapshot ended at, it tails the WAL through the slot forever,
   emitting `c`/`u`/`d` as rows change — each within ~1s.

The four `op` values you'll see in kafka-ui: `r` (snapshot read), `c` (insert), `u` (update),
`d` (delete). A delete is additionally followed by a **tombstone** (same key, `null` value)
because `tombstones.on.delete=true` — the thing compaction needs later (story 03.3).

## Reading `pg_replication_slots` (`make slot-status`)

This one catalog view is where the connector's promise to Postgres is visible. The columns
that matter:

```sql
SELECT slot_name, active, active_pid, confirmed_flush_lsn,
       pg_size_pretty(pg_wal_lsn_diff(pg_current_wal_lsn(), confirmed_flush_lsn)) AS retained_wal
FROM pg_replication_slots;
```

| Column | Reading it |
|---|---|
| `slot_name` | `cdc_genomics` — matches `slot.name` in the config. |
| `active` | `t` when a WAL sender is attached (the connector is connected). `f` if the connector is stopped/deleted — **and the slot still exists**, still pinning WAL. |
| `active_pid` | the backend PID streaming to the connector — proof a real session is attached. |
| `confirmed_flush_lsn` | the LSN the connector has **acknowledged**. This is *the* number: it advances as the connector commits offsets, and Postgres will not recycle WAL below it. It is exactly the "confirmed-flush LSN" the phase-2 hand-written connector must maintain itself. |
| `retained_wal` (derived) | how far `confirmed_flush_lsn` lags `pg_current_wal_lsn()`, in bytes — **the WAL the slot is pinning.** Under load it grows and shrinks as the connector keeps up; if the connector stalls, it grows without bound. This is the [sharp edge](01-postgres-logical-replication.md): an abandoned slot fills the disk. |

Run `make load` in one terminal and `make slot-status` a few times in another: you'll watch
`confirmed_flush_lsn` climb as the connector acknowledges events, and `retained_wal` stay
small because it's keeping up.

## Where at-least-once comes from

`confirmed_flush_lsn` is also the origin of the pipeline's at-least-once guarantee. The
connector's Kafka-side offset *is* the acknowledged LSN, stored in the internal
`connect_offsets` topic ([learning 03](03-kafka-connect-and-connectors.md)). On a Connect
restart it resumes from the last committed offset — which is usually slightly *behind* what it
already produced, so a handful of events get **re-emitted**. That's not a bug; it's the
contract. Every consumer downstream (epics 04–05) must therefore be idempotent, keyed on
`source.lsn` as an external version (ARCHITECTURE §5, §8).

## Takeaway

- Registering the connector binds four pieces: `wal_level` (server), publication (which
  tables), replica identity (how complete), and a **slot it creates itself** (how far it's
  read). The first three predate it; the slot is per-consumer position, so it's the
  connector's to own.
- `snapshot.mode=initial` ⇒ every existing row arrives once as **`op:"r"`**, then live changes
  stream as `c`/`u`/`d` (+ tombstone on delete).
- `pg_replication_slots` shows the whole promise: `active` (attached), `confirmed_flush_lsn`
  (acknowledged position, monotonic), and the WAL it's pinning. `make slot-status` under
  `make load` makes it move.
- The acknowledged LSN is both the resume point and the reason re-emission (hence
  at-least-once, hence idempotent consumers) is normal.
