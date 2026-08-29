# Epic 03 — CDC ingestion (Debezium)

**Goal:** row changes in Postgres appear as well-shaped events on correctly designed
Kafka topics, the initial snapshot works, and the two classic CDC production incidents
(connector restart, abandoned replication slot) have been provoked deliberately and
survived.

This epic is where "I never touched Kafka" gets fixed against a battle-tested producer,
before any consumer code exists.

**Done when:** the load generator runs while topics fill in real time, event anatomy is
documented from observation, and both failure drills pass.

## Stories

1. ✅ [Connector deployment](01-connector-deployment.md) — Debezium registered and streaming (M)
2. ✅ [Event anatomy](02-event-anatomy.md) — [`docs/EVENTS.md`](../../docs/EVENTS.md) written from real captured events (M)
3. ✅ [Topic design and compaction](03-topic-design-and-compaction.md) — partitions, cleanup policy, tombstones (M)
4. ⬜ [Failure drills: restarts and slot lag](04-failure-drills.md) — the disk-fill footgun, exercised (L)

**Progress:** 3 / 4 stories done. Next: 03.4 (failure drills: restarts and slot lag).

## Field notes

- **Default topic retention silently empties the snapshot (motivates 03.3).** Topics are
  auto-created with the broker default `retention.ms=7d` and `cleanup.policy=delete`. After
  a stack sat idle past that window, `cdc.public.specimens` reported `earliest == latest`
  (log start had advanced past every snapshot event) — kafka-ui showed nothing and a
  from-beginning read hit "offset out of range." Nothing was broken; the state-of-the-world
  had just aged out. This is exactly the case for **compaction** (story 03.3, decision D3):
  `cleanup.policy=compact` keeps the latest event per key indefinitely, so offset-0 rebuild
  stays possible forever and the topic never silently empties.
- **`REPLICA IDENTITY DEFAULT` deletes don't null the old row — they *placeholder* it.** The
  before-image of a delete under `DEFAULT` had `id` real and every other column a type
  default: `""` for `NOT NULL` text, epoch `1970-01-01` for the timestamp, `null` only where
  the column was nullable. A consumer that quietly lost `FULL` would ingest empty strings and
  1970 timestamps, not obvious nulls — a silent-corruption footgun. Captured side-by-side in
  EVENTS.md §5.
- **`decimal.handling.mode=double`'s damage is invisible in the payload.** Same rows through
  string vs double: the double text round-trips identically (`220.96` prints `220.96`), so
  nothing looks wrong — but the JSON type flipped string→number and the value is now a binary
  approximation (`220.96` is really `220.9600000000000079…`). The bug only surfaces in
  arithmetic: summing 1 000×`220.96` as a double drifts to `220959.99999999837`. The trap is
  precisely that it looks fine. EVENTS.md §6.
- **Snapshot reads share one watermark LSN.** All 5 000 `op:"r"` events carried the same
  `source.lsn` (with `snapshot` markers `first_in_data_collection` … `last`), so `lsn` can't
  order rows *within* a snapshot — only streaming events carry real per-change WAL positions.
  Per-key idempotency still holds (one snapshot event per key, later changes strictly higher).
  This is a phase-2 gotcha: the hand-written connector's snapshot LSN semantics must match.

### Story 03.3 — topic design and compaction

- **`kafka-topics.sh --describe`, both topics, matching §4** (`make topics-describe`):

  ```
  Topic: cdc.public.specimens  PartitionCount: 6  ReplicationFactor: 1
    Configs: cleanup.policy=compact,max.compaction.lag.ms=60000,min.compaction.lag.ms=0,
             min.cleanable.dirty.ratio=0.01,delete.retention.ms=60000,segment.ms=60000
  Topic: cdc.public.runs       PartitionCount: 3  ReplicationFactor: 1
    Configs: (identical)
  ```

  The seeded snapshot spread **846 / 819 / 845 / 830 / 816 / 844** across the six specimen
  partitions (5 000 exactly) and **14 / 10 / 6** across the three run partitions. Even hash
  balance is a large-numbers property — 30 runs is not a large number.

- **Compaction has three gates, and the default `segment.ms` shuts all your demos.** The
  cleaner never touches a partition's *active* segment, and a segment only rolls when a
  record arrives after `segment.ms` — default **7 days**. So on a quiet lab topic, nothing
  ever compacts and it looks like `cleanup.policy=compact` did nothing. Lab values, chosen
  to open the gates within a coffee break (and recorded as lab-only in
  [`deploy/topics/topics.notes.md`](../../deploy/topics/topics.notes.md)): `segment.ms=60000`,
  `min.cleanable.dirty.ratio=0.01` (default 0.5 = half the log must be garbage first),
  `max.compaction.lag.ms=60000`, `delete.retention.ms=60000` (default 24 h). The demo script
  keeps writing while it waits for exactly this reason — appends are what force the roll.

- **Compaction demo, real numbers** (`make compaction-demo`, 1 000 updates to specimen 5387,
  which hashed to partition 3):

  | | records for the key | partition 3 |
  |---|---|---|
  | after 1 000 updates | **1 001** at offsets 1004–2004 | 1 875 records over offsets 0–2005 |
  | 15 s later | **1** (survivor at offset 2004) | 850 records over offsets 0–2017 |

  1 167 offsets are simply gone: **the log is deliberately non-contiguous** — a survivor
  keeps its original offset, so reading start-to-end yields far fewer records than
  `high − low`. The broker's own cleaner log names the pass:

  ```
  Log cleaner thread 0 cleaned log cdc.public.specimens-3 (dirty section = [983, 2005])
      Start size: 1.6 MB (1,875 messages)   End size: 0.7 MB (838 messages)
      57.9% size reduction (55.3% fewer messages)
  ```

  On disk, partition 3 collapsed to one cleaned 689 KB segment (`00000000000000000000.log`)
  plus a fresh 15 KB active segment (`00000000000000002110.log`).

- **A delete is a two-stage lifecycle, and the stages are ~a minute apart.** Hard-deleting a
  `registered` specimen produced **3** records on its key — `op:"c"`, `op:"d"`, and the
  null-valued tombstone (offsets 1066/1067/1068). Then: at **60 s** the create and delete
  events were compacted away and *only the tombstone remained*; at a further **75 s** the
  tombstone itself was reaped and the key left the log entirely (3 → 1 → 0). Stage 2's delay
  is `delete.retention.ms` — not overhead, but the window in which an offline consumer can
  still learn the key was deleted. At the 24-hour default you would never see stage 2 in a
  lab; at 60 s a consumer that was down for two minutes would miss the delete forever.

- **Two Docker engines will quietly serve you two different Kafkas.** Colima and Docker
  Desktop were both running, each publishing 9092 — Colima on IPv4, Desktop on IPv6. Since
  `localhost` resolves to both, roughly every other client connection landed on a *different
  cluster*, which surfaced as `cdc-topics` intermittently reporting "live topic has 1
  partitions" for a topic that demonstrably had 6. `kafka-topics.sh` inside the container
  never saw it, because it never leaves the VM. ADR 0002 already says "do not run both at
  once"; this is what ignoring it looks like from the client side, and it is worth knowing
  because *any* host-side tool in this repo (psql, the load generator, the smoke suite) is
  equally exposed. Diagnosis: `lsof -nP -iTCP:9092 -sTCP:LISTEN` showing two listeners.

- **You cannot grow partitions on a keyed topic.** Kafka permits it; the design does not.
  `partition = murmur2(key) % n`, so changing `n` re-hashes the key space: a row's existing
  records stay put while its future records route elsewhere, leaving two "latest" values
  compaction can never merge and no error anywhere. `cdc-topics create` therefore refuses
  and points at delete-and-rebuild — which means re-snapshotting to refill. That is also why
  topics are created *before* the connector: a snapshot that already landed in a wrong-shaped
  topic cannot be fixed in place.
