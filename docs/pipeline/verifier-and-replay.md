# Verifier & replay

> **Status:** 📐 planned — [epic 06](../../project-mgmt/epic-06-replay-and-fanout/) · **Spec:**
> [ARCHITECTURE.md](../../ARCHITECTURE.md) §6, D3

This is where the whole design pays off. The **verifier** makes "in sync" a program's verdict,
and **replay** proves any derived store can be rebuilt from the log. Together they turn the
CDC thesis from a claim into a demo.

## The verifier: the correctness oracle

```mermaid
flowchart TB
  pg[("Postgres<br/>source of truth")] --> v{{"Verifier"}}
  es[("Elasticsearch")] --> v
  rd[("Redis")] --> v
  v -->|"by key + content hash"| verdict{"any diff?"}
  verdict -->|"no"| ok["exit 0 ✅ in sync"]
  verdict -->|"yes"| bad["exit non-zero ❌ + report"]
```

The verifier diffs full Postgres state against each derived store — **by key and by content
hash** — and exits non-zero on any discrepancy. It is not optional tooling; it *is* the
definition of "in sync," and it's the assertion at the end of every chaos drill (epic 07).
"The stores agree" is never a human's impression.

## Rebuild from log (story 06.2)

```mermaid
flowchart LR
  drop["Delete the ES index"] --> reset["Reset indexer group<br/>to earliest offset"]
  reset --> consume["Consume to end<br/>(compaction makes it cheap)"]
  consume --> verify["Verifier"]
  verify --> zero["Zero diffs ✅"]
```

Delete the entire Elasticsearch index, reset the consumer group to offset 0, let it rebuild,
and the verifier confirms it matches Postgres exactly. **Compaction** (latest-per-key,
[D3](../../ARCHITECTURE.md)) is what makes offset-0 replay affordable — see
[Kafka](../platform/kafka.md#log-compaction-bounded-but-never-forgets-current-state).

## Zero-downtime reindex (story 06.3)

```mermaid
flowchart LR
  change["Mapping change"] --> v2["Replay into specimens_v2<br/>(new consumer group)"]
  v2 --> verify["Verify v2"]
  verify --> flip["Flip alias v1 → v2 (atomic)"]
  flip --> retire["Retire v1 when confident"]
```

The production pattern for "the derivation logic changed": build the new index alongside the
old, verify, then flip an Elasticsearch **alias** atomically — no maintenance window. See
[Elasticsearch](../platform/elasticsearch.md#zero-downtime-reindex-epic-06).

## Analytics fan-out (story 06.4): the late-arrival proof

```mermaid
flowchart LR
  k{{"Kafka (unchanged)"}} --> an["Analytics consumer<br/>(added last)"]
  an --> duck[("DuckDB facts")]
  note["Producer + existing consumers: ZERO changes"]
  an --- note
```

Adding a brand-new DuckDB analytics consumer **after** everything else — with no change to the
producer or the other consumers — is the concrete proof that the log truly decouples
producers from consumers. Its late arrival is the whole point.

## See also

- [Search indexer](search-indexer.md) · [Cache consumer](cache-consumer.md) — what it verifies
- [Kafka](../platform/kafka.md) — compaction, the thing that makes replay cheap
- [System overview](../overview.md) · [Data lifecycle](../data-lifecycle.md)
