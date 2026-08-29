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
3. ⬜ [Topic design and compaction](03-topic-design-and-compaction.md) — partitions, cleanup policy, tombstones (M)
4. ⬜ [Failure drills: restarts and slot lag](04-failure-drills.md) — the disk-fill footgun, exercised (L)

**Progress:** 2 / 4 stories done. Next: 03.3 (topic design and compaction).

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
