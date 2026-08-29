# topics.json — every setting is a decision

The CDC topics are **designed, not defaulted**. Their shape lives in
[`topics.json`](topics.json) and is applied by `make topics` (idempotent: creates what's
missing, converges config on what exists). JSON can't carry comments, so this file is the
one-line-per-setting rationale — the same arrangement as
[`../connectors/genomics.notes.md`](../connectors/genomics.notes.md).

The normative shape is [ARCHITECTURE.md §4](../../ARCHITECTURE.md) and decision **D3**;
`tests/unit/test_topic_spec.py` pins this file to it, and the smoke suite asserts the live
cluster matches.

## Why explicit creation at all

The broker's `auto.create.topics.enable` is **`false`** in
[`../docker-compose.yml`](../docker-compose.yml). Left on, Kafka would happily invent
`cdc.public.specimens` the moment Debezium produced its first event — with **1 partition**
and **`cleanup.policy=delete`, `retention.ms=7d`**. Both are wrong here, and both fail
*silently*:

- **1 partition** still preserves ordering (trivially), so nothing breaks today — but it
  caps consumer parallelism at one instance, so epic 04's multi-instance scaling and
  rebalance demos have nothing to run on.
- **`delete` + 7-day retention** quietly empties the log. Epic 03's field notes record it
  happening: a stack sat idle past the window and `cdc.public.specimens` reported
  `earliest == latest` — the entire 5 000-row snapshot had aged out and a from-beginning
  read failed with *offset out of range*. Rebuild-from-offset-0, the payoff of the whole
  design (epic 06), was simply gone.

With auto-create off, a missing topic is a loud connector failure instead. `make connector`
depends on `make topics`, so the normal flow is unchanged.

## Shape

| Topic | Key | Partitions | Why |
|---|---|---|---|
| `cdc.public.specimens` | specimen id | **6** | Deliberate slack: enough to demonstrate multi-instance consumer scaling and rebalancing in epic 04. Not a capacity plan. |
| `cdc.public.runs` | run id | **3** | Far fewer runs than specimens, and the indexer's run lookup (§7) reads the whole topic anyway. |

The key is the **primary key**, set by Debezium — so every change to one row hashes to one
partition, and per-row ordering is guaranteed. Cross-row and cross-table ordering is *not*,
and consumers must never assume it (§7 is where that bites).

**Partition counts are effectively immutable.** Kafka can only *grow* one, and growing it
re-hashes the key space: existing records for a key stay in their old partition while new
ones may route elsewhere. The key then has two "latest" values that compaction can never
merge, and per-key ordering is gone with no error anywhere. `cdc-topics create` therefore
refuses to change a count in place; `make topics-recreate` deletes and rebuilds, which
means re-snapshotting to refill.

## Config

| Setting | Value | Why |
|---|---|---|
| `cleanup.policy` | `compact` | **D3.** Keep the latest event per key *forever*, drop superseded ones. Latest-state-per-key is exactly sufficient to rebuild read models (they are functions of current row state, not history — §0), and it is what keeps offset-0 replay affordable instead of unbounded. |
| *(no `retention.ms`)* | — | Deliberately absent. A time-based retention alongside compaction would re-introduce the very aging-out compaction exists to prevent — it would eventually delete the *only* remaining event for a key. Under a pure `compact` policy, `retention.ms` does not apply at all; leaving it unset says so. |
| `min.cleanable.dirty.ratio` | `0.01` | **Lab tuning.** The cleaner only visits a partition once ≥ this fraction of the log is superseded records; the default `0.5` means half the log must be garbage first, which on a small lab topic can be never. At 0.01 compaction runs almost as soon as there's anything to clean. Production default is `0.5` for a reason — cleaning is I/O. |
| `segment.ms` | `60000` | **Lab tuning.** The cleaner *never touches a partition's active segment*, so nothing compacts until a segment rolls. The default is 7 days: a lab session would see no compaction at all and conclude it was misconfigured. One minute makes it observable. Costs many small segments; production leaves this at the default. |
| `max.compaction.lag.ms` | `60000` | Upper bound on how long a superseded record may linger before the cleaner must act, regardless of the dirty ratio. Belt-and-braces so the demo is bounded in time rather than dependent on write volume. |
| `min.compaction.lag.ms` | `0` | No enforced minimum age before a record is eligible. Stated explicitly rather than inherited, because it is the knob you would *raise* in production to guarantee slow consumers a window in which they can still see superseded versions. |
| `delete.retention.ms` | `60000` | How long a **tombstone** survives after the cleaner first compacts it. This is the window a consumer has to observe the delete before the key vanishes entirely; the default is 24 hours, which is far too long to watch in a lab and far too *short* to be safe for a consumer that has been down for two days. Real deletes are a two-stage lifecycle, and this setting is stage two. |

The four lab-tuned values exist so a compaction demo finishes in minutes rather than days
(story 03.3). They are recorded here as *lab* settings, not recommendations: a production
deployment would run the defaults and accept that compaction is a background process you
observe in metrics rather than in a terminal.

## Not configured here (yet)

- **`<consumer>.dlq` topics** — §4 specifies 1 partition, `cleanup.policy=delete`,
  30-day retention (a dead letter is *history*, not state, so compaction would be exactly
  wrong for it). They arrive with the retry/DLQ story, epic 04.5; this file is where they
  will be declared.
- **Replication factor** stays `1` — single-node lab broker. Never production.
