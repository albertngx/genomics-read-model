# EVENTS.md — the change-event contract

> **Status:** written from **real captured events** (story 03.2) · connector `genomics`,
> Debezium **2.7.3.Final**, `pgoutput` · **Spec:** [ARCHITECTURE.md](../ARCHITECTURE.md)
> §5 (idempotency), §7 (denormalization), D4, D6 · **Topics:** `cdc.public.specimens`,
> `cdc.public.runs`

This is the **event contract**: exactly what Debezium emits for our tables, field by field,
captured from a live stack — not paraphrased from Debezium's docs. Every consumer in epics
04–06 is a pure function over these events, so misreading them here means debugging in the
worst possible place. It is also the bar phase 2's hand-written `pgoutput` connector must
reproduce **byte-for-byte in shape**: if a consumer written from this document alone works
against Debezium, it must work unchanged against our connector.

Every payload below is copy-pasted from the topic. To regenerate them yourself, see
[How these were captured](#how-these-were-captured).

---

## 1. The envelope (shared by every event)

With `value.converter.schemas.enable=false` (decision §9 — inspectability), the value is the
**bare Debezium envelope**, no embedded schema wrapper. Every non-tombstone event on both
topics has this identical top-level shape:

| Field | Type | Meaning |
|---|---|---|
| `before` | object \| null | Row state *before* the change. `null` for creates and snapshot reads. Fully populated for updates/deletes **only because of `REPLICA IDENTITY FULL`** (§4 below). |
| `after` | object \| null | Row state *after* the change. `null` for deletes. |
| `source` | object | Provenance and the two tokens consumers live by — `lsn` and `ts_ms`. See [§3](#3-the-source-block). |
| `op` | string | `r` snapshot read · `c` create/insert · `u` update · `d` delete. (A tombstone has **no** `op` — it has no value at all.) |
| `ts_ms` / `ts_us` / `ts_ns` | int | When the **connector processed** the event (ms/µs/ns twins). Distinct from `source.ts_ms` — the difference is connector lag, not end-to-end latency. |
| `transaction` | object \| null | Null here (transaction metadata is not enabled). |

The key and the `source` block are the two parts every consumer touches first, so they come
before the per-op payloads.

## 2. The key

The message **key** is what Kafka partitions and compacts by. With
`key.converter.schemas.enable=false` it is the bare primary key:

```json
{"id":121}
```

- Identical structure on both topics: `{"id":<pk>}` for `specimens`, `{"id":<pk>}` for `runs`.
- It is the **same for every event about a given row** — snapshot, create, update, delete,
  and the delete's tombstone all share one key. That is what makes per-key ordering and log
  compaction (story 03.3) work: the latest value per key wins, and a null-valued tombstone
  on that key is what compaction eventually reaps.
- Consumers use this `id` as the document `_id` in Elasticsearch and the key in Redis, so
  the source PK is the identity all the way through the pipeline.

## 3. The `source` block

Captured verbatim from the snapshot event below:

```json
"source": {
  "version": "2.7.3.Final",
  "connector": "postgresql",
  "name": "cdc",                     // = topic.prefix
  "ts_ms": 1787997840121,            // ← DB commit time (epoch ms). THE latency clock.
  "snapshot": "true",               // "true" | "false" | "first_in_data_collection" | "last"
  "db": "genomics",
  "sequence": "[null,\"29464976\"]",
  "ts_us": 1787997840121058,
  "ts_ns": 1787997840121058000,
  "schema": "public",
  "table": "specimens",              // which table → which topic
  "txId": 734,
  "lsn": 29464976,                   // ← THE idempotency token (WAL position)
  "xmin": null
}
```

Two fields matter enormously; the rest are context.

### `source.lsn` — the idempotency token (ARCHITECTURE §5)

`lsn` is the change's position in the Postgres WAL, a monotonically increasing 64-bit
integer. **Every consumer's idempotency guard is built on it**: Elasticsearch indexes with
`version_type=external, version=<lsn>`; Redis stores the value with its `lsn` and a Lua CAS
rejects any write whose `lsn` ≤ the stored one. A replayed older event (Debezium restart,
consumer redelivery) loses the version check and is skipped — duplicates become harmless.

Two behaviours you must know to use it correctly, **both observed in our capture**:

- **Streaming events carry the real per-change WAL position, monotonic per key.** For demo
  specimen `id=5001`: create `lsn=29464960` → update `29465440` → delete `29465808`. For a
  given key, versions only ever move forward — which is exactly what the guard needs.
- **Snapshot (`op:"r"`) reads all share one watermark LSN, not a per-row position.** Every
  one of the 5 000 snapshot events carried `lsn=29464976`; the first was tagged
  `snapshot:"first_in_data_collection"` and the last (`id=5000`) `snapshot:"last"` with a
  boundary `lsn=29464920`. So `lsn` **cannot order rows within the snapshot** — don't try.
  It doesn't need to: there is exactly one snapshot event per key, and any later streaming
  change to that key arrives with a strictly higher `lsn`. (Corollary: don't compare the
  snapshot watermark against an *unrelated* new key's insert LSN — they sit within a few
  bytes of each other at the boundary and mean nothing across keys. The guard is **per
  key**.)

### `source.ts_ms` — the end-to-end latency clock (epic 07)

`source.ts_ms` is the **database commit time** (epoch ms). It is the clock epic 07 uses to
measure end-to-end pipeline latency: `consumer_write_time − source.ts_ms`. Do **not** use
the top-level `ts_ms` for this — that is when the *connector* handled the event (here
1787998059130 vs the source's 1787998058723, ~400 ms of connector lag on this run). During
the initial snapshot, `source.ts_ms` is the snapshot time, not the row's original write
time — another reason snapshot events are "current state," not history.

---

## 4. The five event kinds

All five below are for the **same demo specimen** (`id=5001`, barcode `EVT-DEMO-A`) except
the snapshot read, so you can watch one row's whole life. Payloads are unedited.

### `op:"r"` — snapshot read

Emitted once per existing row when the connector first starts (`snapshot.mode=initial`), so a
brand-new consumer begins from a complete picture. `before` is null; `after` is the whole row.

```json
{
  "before": null,
  "after": {
    "id": 121,
    "barcode": "SPEC-000121",
    "label": "Participant NGP-85344 aliquot 1 (saliva)",
    "description": "RNA-seq on sequencer SEQ-01 in library preparation — saliva gDNA, pharmacogenomics cohort (run RUN-2026-001).",
    "specimen_type": "saliva",
    "assay": "RNA-seq",
    "volume_ul": "229.72",
    "status": "in_prep",
    "run_id": 1,
    "updated_at": "2026-08-29T10:03:59.534482Z"
  },
  "source": { "…": "snapshot:\"true\", ts_ms=<snapshot time>, lsn=29464976 (see §3)" },
  "op": "r",
  "ts_ms": 1787997840182
}
```

- `volume_ul` is the string `"229.72"`, not a number — see [§6, D6](#6-d6--decimal-handling-string-vs-double).
- `run_id: 1` is the denormalization FK: this specimen's search document embeds fields from
  run 1 (§7). Some rows carry `run_id: null` (registered control/scratch specimens).
- A consumer treats `r` **exactly like `c`** — an upsert of `after`. There is no separate
  "load" path.

### `op:"c"` — create (INSERT)

```json
{
  "before": null,
  "after": {
    "id": 5001,
    "barcode": "EVT-DEMO-A",
    "label": "Event-anatomy demo aliquot A",
    "description": "created to capture op:c",
    "specimen_type": "blood",
    "assay": "WES",
    "volume_ul": "512.50",
    "status": "registered",
    "run_id": 1,
    "updated_at": "2026-08-29T10:07:38.721900Z"
  },
  "source": { "snapshot": "false", "ts_ms": 1787998058723, "lsn": 29464960, "txId": 735, "…": "…" },
  "op": "c",
  "ts_ms": 1787998059130
}
```

Insert: `before` null, `after` the new row. Idempotent handling = upsert keyed by `id`,
guarded by `lsn`.

### `op:"u"` — update (the `REPLICA IDENTITY FULL` payoff)

A PATCH that raised `volume_ul` 512.50 → 1024.50 and edited the label. **Both `before` and
`after` are fully populated** — that is D4 (FULL) at work; under Postgres defaults `before`
would be absent (proven in [§5](#5-d4--replica-identity-full-vs-default)).

```json
{
  "before": {
    "id": 5001, "barcode": "EVT-DEMO-A", "label": "Event-anatomy demo aliquot A",
    "description": "created to capture op:c", "specimen_type": "blood", "assay": "WES",
    "volume_ul": "512.50", "status": "registered", "run_id": 1,
    "updated_at": "2026-08-29T10:07:38.721900Z"
  },
  "after": {
    "id": 5001, "barcode": "EVT-DEMO-A", "label": "Event-anatomy demo aliquot A (topped up)",
    "description": "created to capture op:c", "specimen_type": "blood", "assay": "WES",
    "volume_ul": "1024.50", "status": "registered", "run_id": 1,
    "updated_at": "2026-08-29T10:07:38.740271Z"
  },
  "source": { "snapshot": "false", "ts_ms": 1787998058742, "lsn": 29465440, "txId": 736, "…": "…" },
  "op": "u"
}
```

The full `before` is what lets a consumer diff an update — e.g. detect that `run_id` changed
and move the document between parents, or that `status` crossed into `disposed` (a
projection-delete, §7) — without a database round-trip.

### `op:"d"` — delete (hard delete)

A hard DELETE of the still-`registered` specimen. `before` is the complete last-known row
(again, FULL); `after` is null.

```json
{
  "before": {
    "id": 5001, "barcode": "EVT-DEMO-A", "label": "Event-anatomy demo aliquot A (topped up)",
    "description": "created to capture op:c", "specimen_type": "blood", "assay": "WES",
    "volume_ul": "1024.50", "status": "registered", "run_id": 1,
    "updated_at": "2026-08-29T10:07:38.740271Z"
  },
  "after": null,
  "source": { "snapshot": "false", "ts_ms": 1787998058827, "lsn": 29465808, "txId": 737, "…": "…" },
  "op": "d"
}
```

A consumer maps `op:"d"` to "remove the document for this `id`." Note the domain twist (§3,
§7): a **soft delete** (disposal) is *not* an `op:"d"` — it is an `op:"u"` with
`status:"disposed"`, and the source row survives. Two different source events, one projection
outcome ("remove from the active index"); the consumer must handle both.

### The tombstone (the delete's second event)

`tombstones.on.delete=true`, so a hard delete emits **two** messages on the key, in order:

| # | Offset | Key | Value | `op` |
|---|---|---|---|---|
| 1 | 5002 | `{"id":5001}` | the `op:"d"` envelope above | `d` |
| 2 | 5003 | `{"id":5001}` | **`null`** | *(none — no value)* |

The second message is the tombstone: **same key, null value, no envelope at all.** The two
exist for two different readers:

- **Consumers** react to the `op:"d"` event (message 1) — that is the semantic "row deleted."
- **Log compaction** (story 03.3) needs the null-valued tombstone (message 2): a compacted
  topic drops a key once it has seen a null value for it, so the key can eventually leave the
  topic entirely instead of living forever as a delete marker.

**Consumer rule:** a `null` value is not an error and not an update — skip it for projection
purposes (the `op:"d"` already did the removal). Deserializers must null-check the value
before parsing; blindly `json.loads(value)` on a tombstone is the classic crash.

## 5. D4 — `REPLICA IDENTITY FULL` vs `DEFAULT`

Migration `0002` sets `ALTER TABLE specimens REPLICA IDENTITY FULL`. To prove what that buys,
we flipped the table to `DEFAULT`, ran an update + delete on a fresh specimen (`id=5002`),
captured them, and reverted to `FULL`. The before-images collapse:

**Update under `DEFAULT` — `before` is gone:**

```json
{ "before": null,
  "after": { "id": 5002, "volume_ul": "888.00", "status": "registered", "…": "…" },
  "op": "u" }
```

**Delete under `DEFAULT` — `before` is the primary key and nothing else:**

```json
{ "before": {
    "id": 5002,
    "barcode": "", "label": "", "description": null, "specimen_type": "",
    "assay": "", "volume_ul": null, "status": "", "run_id": null,
    "updated_at": "1970-01-01T00:00:00.000000Z"
  },
  "after": null, "op": "d" }
```

Only `id` (the replica-identity key) is real; every other column is a **type-default
placeholder** — `""` for text, `null` for nullable, epoch `1970-01-01` for the timestamp —
because Postgres never wrote the old values to the WAL, so Debezium has nothing to fill them
with. (Postgres logs only the identity for `DEFAULT`; `FULL` logs the entire old tuple.)

**Why FULL was chosen.** Our consumers and the epic-06 verifier want the complete old row:
to diff updates (did `run_id`/`status` change? → move or projection-delete the document
without a DB read), and to have a self-describing delete. `DEFAULT` would force every such
decision into a lookup against the very OLTP database this read-model exists to offload. The
cost is honest and real — `FULL` writes the whole before-image into the WAL, so more WAL
volume per change (D4) — but at this scale it is the right trade, and it is a production
tuning knob worth knowing by feel. The `""`/epoch placeholders above are exactly the silent
data-corruption a consumer would ingest if someone quietly reverted the `REPLICA IDENTITY`.

## 6. D6 — decimal handling (`string` vs `double`)

`volume_ul` is Postgres `NUMERIC(10,2)` — an **exact** decimal (aliquot volumes are a lab
data-integrity concern). `decimal.handling.mode=string` (D6) emits it as a JSON **string** so
no precision is lost on the wire. To show the alternative, a throwaway second connector with
`decimal.handling.mode=double` was run against the same two rows. Captured, same specimens,
both connectors:

| Row (`volume_ul`) | `string` mode (ours) | `double` mode | JSON type |
|---|---|---|---|
| `2048.75` | `"volume_ul":"2048.75"` | `"volume_ul":2048.75` | string vs **number** |
| `220.96`  | `"volume_ul":"220.96"`  | `"volume_ul":220.96`  | string vs **number** |

The trap is that the `double` **text often looks identical**, so the bug hides in plain
sight. But the type changed from string to IEEE-754 `double`, and the value is now a binary
approximation:

- `2048.75` is exactly representable in binary → the double really is `2048.75`.
- `220.96` is **not**: the stored double is
  `220.9600000000000079580786405131220817565917968750`. JSON serialisation prints the
  shortest round-tripping form (`220.96`), so the payload *reads* fine — and a consumer that
  does arithmetic inherits the error anyway. Summing `220.96` a thousand times as a double
  yields `220959.99999999837` where exact decimal gives `220960.00`. In a chain of custody
  that reconciles aliquot volumes, that drift is a defect.

`string` mode keeps the exact decimal on the wire; consumers parse it into their own exact
type (Python `Decimal`) and precision is preserved end-to-end. This is why the source API and
schemas also treat `volume_ul` as `Decimal`/string, never `float` (`schemas.py`,
`ARCHITECTURE.md` §3).

## 7. The `runs` topic (same envelope, different columns)

`cdc.public.runs` uses the **identical** envelope and key shape; only `after`/`before`
columns differ. One captured snapshot read:

```json
{
  "op": "r",
  "after": {
    "id": 1, "run_code": "RUN-2026-001",
    "assay_protocol": "WGS PCR-free (TruSeq DNA PCR-Free)",
    "sequencer": "SEQ-01", "operator": "L. Haddad",
    "status": "planned", "updated_at": "2026-08-29T10:03:59.534482Z"
  },
  "source": { "table": "runs", "lsn": 29464976, "…": "…" }
}
```

`sequencer`, `assay_protocol`, and `operator` are the fields the specimen search document
denormalizes (§7). A change to any of them arrives **here**, on a different topic with no
ordering relationship to specimen events — which is the whole reason the indexer keeps a
local run lookup and repairs specimen docs on run events. A run `UPDATE` to `sequencer`
therefore fans out to a re-index of every specimen in that run (the "denormalization storm,"
decision D5).

## How these were captured

Fresh stack, then drive the source API and read the topics back. `<pw>`/user come from `.env`.

```bash
make up && make seed && make connector          # snapshot + streaming live

# snapshot read: first messages on the topic
kafka-console-consumer --topic cdc.public.specimens --from-beginning --max-messages 1 \
  --property print.key=true            # (run inside the kafka container, or any client)

# c / u / d / tombstone: drive one registered specimen through its whole life
curl -s -XPOST localhost:8000/specimens -H 'content-type: application/json' \
  -d '{"barcode":"EVT-DEMO-A","label":"…","specimen_type":"blood","assay":"WES","volume_ul":"512.50","status":"registered","run_id":1}'
curl -s -XPATCH localhost:8000/specimens/<id> -d '{"volume_ul":"1024.50"}'   # → op:u
curl -s -XDELETE localhost:8000/specimens/<id>                               # → op:d + tombstone

# D4: flip the before-image off and back on
docker compose exec postgres psql -U <user> -d genomics \
  -c "ALTER TABLE specimens REPLICA IDENTITY DEFAULT;"   # capture an update+delete, then:
  # -c "ALTER TABLE specimens REPLICA IDENTITY FULL;"

# D6: a throwaway connector with decimal.handling.mode=double on its own slot + topic prefix,
#     same rows, diff the volume_ul type (string "220.96" vs number 220.96).
```

The exact automated capture used for this document lives in the story 03.2 work log; the
values above are its output, unedited.
