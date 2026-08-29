# 06 — Topic design: partitions as ordering, compaction as retention-by-key

*Context: story 03.3. Until now the CDC topics were whatever Kafka invented when Debezium
first produced to them. This note is what changed when they were designed on purpose —
what a partition actually guarantees, what compaction actually does, and the three reasons
a correctly-configured compacted topic still looks like it isn't compacting.*

Two settings carry this whole story, and **neither is a default**:

| | Default Kafka gives you | What §4 requires | What the default costs |
|---|---|---|---|
| partitions | `1` | `6` (specimens), `3` (runs) | consumer parallelism capped at one instance |
| `cleanup.policy` | `delete` + `retention.ms=7d` | `compact` | the log silently empties; offset-0 rebuild dies |

The dangerous part is that **both wrong values behave perfectly until they don't**. A
1-partition topic preserves ordering trivially. A `delete`-policy topic serves every read
correctly — right up to the day the snapshot ages out and `earliest == latest`. Epic 03's
field notes record that happening here.

## A partition is the unit of ordering. That's the whole guarantee.

Kafka guarantees ordering **within a partition** and nowhere else. There is no such thing as
topic-wide ordering, and there is certainly no ordering across topics.

So the ordering you *want* is bought entirely by the **key**, because the producer routes
`partition = murmur2(key) % partition_count`:

```
key {"id":5001} ──hash──▶ partition 5 ──▶ [ …, c, u, u, u, d, ␀ ]   ← this row's whole life,
key {"id":5002} ──hash──▶ partition 2                                 in commit order
key {"id":5003} ──▶ partition 5   (different key, same partition — no ordering between them)
```

Debezium keys every event by the row's primary key, so:

- **Guaranteed:** all changes to one specimen arrive in the order Postgres committed them.
- **Not guaranteed, ever:** ordering between two specimens, or between a specimen and its
  run (a different topic entirely). This is not a gap to be closed — it is the design.
  ARCHITECTURE.md §7 is the story of a consumer that has to live with it.

Our seeded 5 000 specimens landed 846 / 819 / 845 / 830 / 816 / 844 across the six
partitions — an even hash spread. The 30 runs landed 14 / 10 / 6 across three, which is a
useful reminder that hash balance is a large-numbers property, not a promise.

### The thought experiment: what if we keyed specimens by `run_id`?

Tempting, because it would give something *stronger*: all specimens of a run in one
partition, so run-and-specimen changes would be mutually ordered and the §7 cross-topic join
would evaporate. It would also be catastrophic, in four separate ways:

1. **Compaction would destroy the read model.** Compaction keeps the latest record *per
   key*. With `run_id` as the key, "latest" means the last specimen change in that run — and
   every other specimen's state is discarded as superseded. A topic that can no longer
   reconstruct per-specimen state cannot rebuild the search index, which is the entire point
   of the pipeline (D3, §0).
2. **Tombstones would delete the wrong thing.** Hard-deleting one specimen emits a
   null-valued tombstone on its key. Keyed by `run_id`, that tombstone reads as *"this run
   has no state"* and compaction would eventually reap the whole run's entry.
3. **A specimen moving between runs would split its own history.** Re-assigning a specimen
   changes its key, so its past lives in the old run's partition and its future in the new
   one. Per-key ordering breaks precisely at the moment the row is most interesting.
4. **Skew becomes partition hotspotting.** Lab traffic is Zipfian over runs (the load
   generator's `reassignment-storm` uses `run_alpha=1.6` on purpose). Keying by `run_id`
   maps that skew directly onto partitions: a few partitions take nearly all the traffic
   while others idle, and consumer parallelism collapses to the number of *hot runs*.

The general rule: **key by the entity whose current state the topic is supposed to hold.**
Anything coarser trades a real guarantee (per-row state) for a convenience (cross-row
ordering) that a well-written consumer doesn't need.

## Compaction is retention by key, not by time

A `delete`-policy topic answers *"how long do I keep messages?"*. A `compact` topic answers
a different question: *"how many versions of each key do I keep?"* — and the answer is one,
the latest, **forever**.

```
before   [ k1:v1  k2:v1  k1:v2  k3:v1  k1:v3  k2:v2 ]
after    [               k3:v1  k1:v3  k2:v2 ]
offsets    0      1      2      3      4      5      ← survivors KEEP their original offsets
```

Two consequences that surprise people:

- **The log becomes non-contiguous.** Offsets 0, 1 and 2 above simply don't exist any more.
  A consumer reading start-to-end gets fewer records than `high − low`, and that gap *is*
  the compacted-away set. (The log-start offset does not move — compaction rewrites
  segments in place; only a `delete` policy advances the start.)
- **Latest-per-key is exactly enough for us, and not enough for everyone.** Our read models
  are functions of *current* row state, so replaying a compacted log rebuilds them perfectly.
  A consumer that needed *history* — a real audit trail — would get a mutilated stream. That
  is D3's trade-off, and §0's reason this project is not event sourcing.

## Why a correctly-configured compacted topic looks like it isn't compacting

This is the actual lesson of the story. A record only gets cleaned once it passes **three
independent gates**, and in a lab all three are usually shut:

1. **It must not be in the active segment.** The cleaner never touches the segment currently
   being written. And a segment only rolls when a record arrives *after* `segment.ms` has
   elapsed — so on a quiet topic it never rolls, and nothing ever compacts. Default
   `segment.ms` is **7 days**.
2. **The partition must be dirty enough.** The cleaner picks the partition with the highest
   ratio of superseded records, and only if it exceeds `min.cleanable.dirty.ratio` —
   default **0.5**, i.e. half the log must be garbage first.
3. **Its own lag windows must have passed** — `min.compaction.lag.ms` (a floor on record
   age) and, for tombstones only, `delete.retention.ms`.

Gate 1 is the one that wastes an afternoon: you set `cleanup.policy=compact`, write a
thousand updates, watch nothing happen, and conclude the setting is broken. It isn't — the
whole thousand is sitting in an active segment that will roll next Tuesday. Our lab values
(`segment.ms=60000`, `min.cleanable.dirty.ratio=0.01`, `max.compaction.lag.ms=60000`) exist
purely to open those gates within a coffee break, and the compaction demo *keeps writing
while it waits* for exactly this reason — appends are what force the roll.

## A delete is a two-stage lifecycle

`tombstones.on.delete=true` means a hard DELETE produces **two** records on the same key:

```
op:"d"  (before-image, so consumers can act)   ␀ tombstone (null value, so compaction can act)
```

They serve different audiences, and they die at different times:

| Stage | What happens | Gated by |
|---|---|---|
| 1 | The `op:"d"` event and every earlier version of the key are compacted away. **The tombstone survives.** | the three gates above |
| 2 | The tombstone itself is removed and the key leaves the log entirely. | `delete.retention.ms` after stage 1 |

Stage 2's delay is not an inefficiency — it is the window in which a consumer that has been
offline can still learn the key was deleted. Set it too short and a slow consumer replaying
from an old offset sees a key that simply never appears, and keeps a stale document forever.
The default is 24 hours for that reason; we run 60 s to make the lifecycle watchable.

## Partition counts are effectively immutable

Kafka will let you *grow* a partition count. Don't, on a keyed topic: `murmur2(key) % n`
changes with `n`, so a key's existing records stay put while its future records route
somewhere else. The key now has two "latest" values in two partitions, compaction can never
merge them, and per-key ordering — the guarantee everything downstream rests on — is gone
with no error emitted anywhere. `cdc-topics create` refuses the operation and points at
delete-and-rebuild (`make topics-recreate`), which means re-snapshotting to refill.

This is also why the topics are created **before** the connector is registered, and why
`auto.create.topics.enable=false`: a snapshot that has already landed in a wrong-shaped
topic cannot be fixed in place.

## What this buys later

- **Epic 04** can run several indexer instances, because there are six partitions to divide
  between them — and each instance can rely on per-key ordering within the ones it owns.
- **Epic 06's rebuild-from-offset-0** stays cheap forever: the log holds one record per live
  key, not one per change ever made.
- **Every consumer's idempotency guard** (§5) is per key by construction, because that is
  the only scope in which `source.lsn` is comparable at all.

## See also

- [ARCHITECTURE.md §4](../ARCHITECTURE.md) and **D3** — the normative spec.
- [`deploy/topics/topics.notes.md`](../deploy/topics/topics.notes.md) — every setting, one line each.
- [`docs/EVENTS.md`](../docs/EVENTS.md) — the key structure and the delete/tombstone pair.
- [Learning 05](05-cdc-handshake-and-slots.md) — how events get onto these topics in the first place.
