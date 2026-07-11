# Architecture

This document nails down the canonical CDC read-model example, instantiated in a domain
close to home: a **population-scale genomic sequencing platform** whose source of truth is
Postgres, with a programme-wide specimen search index (Elasticsearch) and a metadata cache
(Redis) derived from the change stream. It is the reference for every epic in
`project-mgmt/`; stories implement decisions made here.

**The pattern is domain-agnostic.** The textbook framing is a product catalog
(category → product). We instantiate it as a genomic sequencing tracker (run → specimen)
because it is a credible, regulated, genuinely large-scale system in the author's target
domain (lab automation / genomics) and makes the portfolio piece speak that domain. The
engineering is identical; only the nouns change.

**Why the scale matters** A single lab tracking a
few thousand specimens does not need Kafka, Debezium, Elasticsearch, and Redis — Postgres
alone would serve its search, its point reads, and its analytics. This design targets the
scale where that stops being true: a biobank or national sequencing programme holding
**millions of specimens** (each participant contributing multiple aliquots and per-specimen
QC/variant state), where programme-wide search and analytics genuinely cannot run against
the governed clinical OLTP database without threatening the write path that runs the lab.
At that scale the read-model is forced by **read volume and query-shape mismatch**, on top
of the correctness argument (dual-write, §1) that holds at any volume. The stack is built
laptop-sized on purpose — so the correctness machinery is inspectable — but the *design*
is sized for the programme, not the bench.

## 0. What this is NOT

Two boundaries, both deliberate, both the kind of distinction that separates a senior
design from a muddled one:

- **This is a read-model.** It derives *current-state*, query-optimized
  projections. An audit trail is append-only *history* — that is event sourcing, a
  different pattern with different storage. Our topics are **compacted** (latest state per
  key); reconstructing full history from them is out of scope and would be the wrong tool.
  A clinical-genomics lab's real audit trail (CLIA/CAP) lives elsewhere; this project feeds
  its **search and read paths**.
- **This is the specimen-tracking *state* layer, not the raw sequence-data firehose.** The
  high-volume base-call / FASTQ / BAM stream off the sequencers is a stream-processing
  problem (windowed aggregation over volume) — a different project. Here we track the
  *state* of specimens and runs (status, location, assignment, QC), which is genuinely
  state-shaped and the right fit for CDC. Keeping these two systems separate is the correct
  architecture.

## 1. Purpose and the problem being solved

A genomic sequencing platform tracks runs and their specimens in Postgres (the
transactional, CLIA/CAP-governed source of truth). Operators and scientists need to **search
across the whole programme** (full-text over specimen labels/descriptions, filter by
assay / sequencer / status / run) and to load run and specimen metadata **fast** for
accessioning and queue dashboards, without every query hitting the governed OLTP database.

The naive design — the platform writes to Postgres, Elasticsearch, and Redis itself — is
the **dual-write problem**: no transaction spans Postgres and Elasticsearch, so a crash
between writes leaves them permanently inconsistent, with no record of what was lost. In a
regulated genomics lab, "the specimen search quietly disagrees with the system of record"
is not a cosmetic bug — it is a chain-of-custody defect.

The correct design derives every secondary store from the database's own change log:

- Postgres is the **only** store the application writes to.
- Debezium tails the WAL via logical replication and publishes each row change to Kafka.
- Each derived store has its own consumer, positioned independently on the log.
- Any derived store can be deleted and rebuilt from the log at any time — demonstrated,
  not asserted.

The dual-write correctness argument holds at any volume; the **scale** argument (a
programme's read and analytics load must not run against the clinical OLTP database) is what
makes the read-model non-optional here rather than a nice-to-have.

## 2. Components and responsibilities

| Component | Responsibility | Explicitly NOT responsible for |
|---|---|---|
| tracking service (FastAPI) | CRUD on runs/specimens in Postgres | knowing downstream stores exist |
| Postgres | source of truth; emits logical replication stream | anything derived |
| Debezium (Kafka Connect) | WAL → Kafka change events, initial snapshot | transformation, business logic |
| Apache Kafka (KRaft) | durable, ordered, replayable log | permanent history (see §5 compaction) |
| search indexer | change events → Elasticsearch documents (denormalized) | reading Postgres directly (except the run lookup, §7) |
| cache consumer | change events → Redis entries | cache-aside logic in the API |
| analytics consumer (late-added) | change events → DuckDB facts | existing at design time — its late arrival is the fan-out proof |
| verifier | automated PG ↔ Elasticsearch ↔ Redis diff | being optional — it *is* the definition of "in sync" |
| load generator | configurable write workload against the API | bypassing the API |

The tracking service is deliberately boring. Its total ignorance of the pipeline is the
architectural point: adding the analytics consumer in epic 06 must require zero producer
changes.

## 3. Data model

```
runs(id PK, run_code UNIQUE, assay_protocol, sequencer, operator,
     status,                 -- planned | active | completed | aborted
     updated_at)
specimens(id PK, barcode UNIQUE, label, description, specimen_type, assay,
          volume_ul NUMERIC(10,2),
          status,            -- registered | in_prep | on_instrument | completed | disposed
          run_id FK, updated_at)
```

Three deliberate sources of difficulty, each domain-authentic:

- **Hard deletes on registered specimens.** A `registered` specimen accessioned in error, or
  a calibration/control scratch specimen never committed to a run, can be physically deleted
  by the operator. That produces a real Debezium delete + tombstone, so consumers must handle
  the delete/compaction lifecycle. Therefore scoped to not-yet-processed data.
- **Soft delete on disposed specimens (projection-level delete).** A `disposed` specimen
  (consumed by extraction or discarded) is a chain-of-custody record and is *never*
  physically deleted; disposal is an `UPDATE` to `status='disposed'`. The source keeps the
  row; the **search projection** must remove it from the active index anyway. So "delete in
  the source" and "delete in the read-model" are decoupled — a more interesting and honest
  situation than a hard delete alone. See §7.
- **Cross-entity derived documents.** The Elasticsearch specimen document embeds the run's
  `sequencer`, `assay_protocol`, and `operator` (search wants denormalized docs — "all
  specimens on sequencer SEQ-07"). A **sequencer re-assignment** (sequencer breakdown) or
  protocol amendment must therefore fan out to a re-index of every specimen in that run —
  the hardest real problem in search-index sync, addressed in §7.

`volume_ul` is `NUMERIC(10,2)` on purpose: aliquot/elution volumes are precise decimals, and
turning them into lossy JSON floats is a real lab data-integrity bug. It forces the
decimal-handling decision (D6).

## 4. Topic and partitioning design

| Topic | Key | Partitions | Cleanup policy |
|---|---|---|---|
| `cdc.public.specimens` | specimen id | 6 | `compact` |
| `cdc.public.runs` | run id | 3 | `compact` |
| `<consumer>.dlq` | original key | 1 | `delete` (30d) |

- **Key = primary key** ⇒ all changes to one row land in one partition ⇒ per-row ordering
  is guaranteed. Cross-row and cross-table ordering is explicitly NOT guaranteed, and
  consumers must not assume it (see §7 for where this bites).
- **Log compaction** keeps the latest event per key forever while letting the log stay
  bounded. Latest-state-per-key is exactly sufficient to rebuild our read models, because
  they are functions of current row state, not of history (§0). Debezium's tombstones
  (`tombstones.on.delete=true`) let compaction eventually drop deleted keys. Trade-off
  accepted and recorded as D3: replay reconstructs *state*, not the full change *history* —
  a consumer needing full history (a real audit log) would use `delete`-policy topics with
  long retention, i.e. a different project.
- 6 partitions is deliberate slack: enough to demonstrate multi-instance consumer scaling
  and rebalancing without pretending this is a capacity plan.

## 5. Delivery semantics and idempotency

Everything here is **at-least-once**. Duplicates are normal (Debezium restart from an
unacknowledged LSN, consumer crash mid-batch, rebalance). Every consumer must therefore be
idempotent, and the strategy is uniform:

**Version-guarded writes using the source LSN.** Every Debezium event carries
`source.lsn`, a monotonically increasing 64-bit position of that change in the WAL.

- Elasticsearch: index with `version_type=external`, `version=<lsn>`. A replayed older
  event loses the version conflict (409) and is skipped — idempotency enforced by the store
  itself.
- Redis: value stored with its LSN; a Lua compare-and-set script rejects writes whose LSN
  is ≤ the stored one.
- Deletes use the same guard (versioned delete / CAS-delete).

**Commit-after-write, never before.** Consumers poll a batch, write it to the sink, *then*
commit offsets. A crash between write and commit causes redelivery, which the version guard
absorbs. Committing before writing would convert crashes into silent data loss — that
ordering is the single most important line in every consumer.

Poison messages (permanent failures — deserialization, a mapping bug) go to the consumer's
DLQ topic with error metadata after bounded retries; a replayer tool re-injects them after
a fix. A poison message must never block the partition forever, and never be silently
dropped.

## 6. Replay and rebuild model

Rebuildability is the payoff of the whole design, delivered as scripted demos:

1. **Rebuild from log:** delete the Elasticsearch index → reset the indexer consumer group
   to earliest → consume to end → run the verifier → zero diffs. Compaction (§4) is what
   makes offset-0 replay affordable.
2. **Derivation change (zero-downtime reindex):** change the document mapping, replay into a
   brand-new index `specimens_v2` under a *new consumer group*, verify, then flip an
   Elasticsearch alias from v1 to v2 atomically. The old index stays until confidence, then
   dies. This is the production pattern for "the derivation logic changed" — and in a
   regulated lab, "reindex without a maintenance window" is a real operational win.

The **verifier** is the correctness oracle for both: it diffs full Postgres state against
each derived store (by key and by content hash) and exits non-zero on any discrepancy. "In
sync" is a program's verdict, never a human's impression. It is also the assertion at the
end of every chaos drill (epic 07).

## 7. The denormalization problem (cross-topic joins) and projection-deletes

The specimen search document embeds run fields (`sequencer`, `assay_protocol`, `operator`),
but run changes arrive on a *different topic* with no ordering relationship to specimen
changes. Plus the soft-delete rule from §3. Consequences the indexer must handle:

- It maintains a local run lookup (id → {sequencer, assay_protocol, operator, lsn}), built
  from `cdc.public.runs`, to enrich specimen documents.
- A specimen event can arrive before its run is known (snapshot ordering, partition timing).
  Policy: enrich with placeholders and mark `_pending_enrichment`; rely on the run event to
  trigger repair — never block the partition waiting.
- A **sequencer re-assignment / protocol amendment** must re-index all specimens in that run,
  via an Elasticsearch update-by-query (decision D5; documented trade-off: touches the sink
  outside the per-event path).
- **Projection-delete:** a specimen `UPDATE` to `status='disposed'` (source row survives)
  must *remove* the document from the active index. A `registered` **hard DELETE** (source
  row gone, `op:"d"`) must also remove it. Two different source events, same projection
  outcome — the indexer maps both to "delete from index," which is exactly the
  read-model/source decoupling this domain makes concrete.

This section exists because it is the part of search-index sync that tutorials skip and
production systems get wrong.

## 8. Failure modes (designed for, then drilled)

| Failure | Consequence if unhandled | Design answer | Drilled in |
|---|---|---|---|
| consumer crash mid-batch | loss (commit-first) or dupes (write-first) | commit-after-write + LSN version guards | epic 07 |
| Debezium/Connect restart | re-emission from last acknowledged LSN | consumers idempotent by construction | epic 03 |
| replication slot abandoned | **Postgres retains WAL until disk fills** | slot-lag watchdog + documented runbook; the classic CDC production incident | epic 03 |
| poison message | partition blocked forever, or silent drop | bounded retries → DLQ → re-inject tool | epic 04 |
| consumer group rebalance under load | duplicate processing of in-flight batch | graceful shutdown (commit on revoke), version guards absorb the rest | epic 04, 07 |
| broker restart | producer/consumer stalls | rely on client retry semantics; verify no loss with oracle | epic 07 |
| run/specimen event races | stale or placeholder run fields in docs | repair-on-run-event policy (§7) | epic 04 |

## 9. Non-goals (phase 1)

Explicitly out of scope — each is a planned extension, not an omission
(see [project-mgmt/roadmap.md](project-mgmt/roadmap.md)):

- **No hand-written CDC connector.** Phase 1 uses Debezium as a correct reference
  implementation; phase 2 replaces it with an own `pgoutput` client validated by diffing
  against Debezium's output.
- **No audit trail / event history**, and **no raw sequence-data firehose** (§0). Both are
  different systems.
- **No `postgres-only` comparison branch yet** (transactional outbox + polling relay).
- **No schema registry / Avro.** Debezium JSON without embedded schemas, for
  inspectability. Schema evolution is a roadmap item.
- **No Kubernetes.** docker-compose only; the observability/K8s layer is its own project.
- **No security hardening** (auth on Elasticsearch/Redis, TLS). Local lab.

## 10. Decision log

- **D1 — Apache Kafka (KRaft mode) as the broker.** The deliberately boring, industry-
  standard incumbent: learn its real operational surface (KRaft, partition reassignment,
  `kafka-*.sh` / `kafka-consumer-groups.sh`, `kcat`) rather than a friendlier compatible
  reimplementation. No bundled console, so **kafka-ui** is added for inspection. Nothing in
  the project depends on Kafka internals; the client code is vanilla Kafka protocol.
- **D2 — Debezium as the phase-1 connector.** Learn Kafka wiring against a battle-tested
  producer first; it later becomes the correctness oracle for the hand-written connector.
- **D3 — Compacted CDC topics.** Latest-per-key is sufficient for state-shaped read models
  and keeps offset-0 replay cheap. Full-history consumers are a different topic
  configuration, out of scope (§0).
- **D4 — `REPLICA IDENTITY FULL` on captured tables.** Updates/deletes carry complete
  before-images, which consumers and the verifier want. Cost: larger WAL volume —
  acceptable here, and a real production tuning knob worth knowing.
- **D5 — Sequencer re-assignments/amendments repaired via update-by-query** in Elasticsearch
  rather than replaying the specimens topic: bounded work, no offset gymnastics. Trade-off:
  the indexer touches the sink outside the per-event path; the verifier keeps it honest.
- **D6 — `decimal.handling.mode=string`** in Debezium. Postgres `NUMERIC` (`volume_ul`) must
  not become a lossy JSON float; consumers parse the string. A classic CDC gotcha kept
  deliberately visible — and in a genomics lab, aliquot-volume fidelity is a data-integrity
  concern.
- **D7 — Python consumers on `confluent-kafka`** (librdkafka): manual commit control and
  production-grade client behavior over the friendlier-but-thinner asyncio clients.
- **D8 — Elasticsearch as the search store** (over OpenSearch): more widely recognized, and
  the design's one hard requirement — external versioning (`version_type=external`) — is
  supported identically, so the choice is cost-free architecturally.
