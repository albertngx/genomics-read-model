# Roadmap — the open-ended upside

Phase 1 (the 7 epics in this folder) delivers a closed, working, verifiable system.
Everything below is a deliberate extension seam: the system is designed so each of these
can be added without reworking phase 1.

## Phase 2 — own CDC connector (the flagship extension)

Replace Debezium with a hand-written connector speaking Postgres logical replication
(`pgoutput`) directly. Debezium stops being infrastructure and becomes the **correctness
oracle**: run both against the same workload, diff the emitted events.

Rough shape (to be broken into epics when phase 1 closes):

1. Replication-protocol client: `pgoutput` binary message parsing (Begin / Relation /
   Insert / Update / Delete / Commit) over a `psycopg` replication connection.
2. Transaction reassembly: buffer row changes, emit in commit order.
3. LSN lifecycle: durable progress tracking, confirmed-flush feedback, correct restart
   from the replication slot — the at-least-once cursor, done by hand.
4. Producing to the same topics with the same event contract, so **phase-1 consumers run
   unmodified** against the new connector. That swap is the demo.
5. Capstone: consistent initial snapshot coordinated with the stream (the DBLog problem).

## Phase 3 — the `postgres-only` comparison

Branch implementing the same fan-out with no Kafka and no Debezium: transactional outbox
table + polling relay + per-consumer offset rows. Load-test both, write `COMPARISON.md`
with honest numbers: sync latency, behavior at 5k writes/s, replay ergonomics,
operational surface. The deliverable is the sentence "below N, the simple version wins."

## Phase 4 — observability layer (separate project, this system as workload)

Deploy this pipeline on k3d/kind, instrument with OpenTelemetry — trace context
propagated through Kafka headers so one trace spans API → broker → consumer — plus
consumer-lag and slot-lag dashboards, SLOs with burn-rate alerts. Corresponds to backlog
proposal #5; this project was built to be its workload.

## Phase 5 — the raw sequence-data firehose (the sibling system)

The deliberate counterpart to this project (ARCHITECTURE.md §0): the high-volume base-call /
FASTQ / BAM firehose off the sequencers, as a *stream-processing* system — windowed
aggregation, event-time watermarks, backpressure over volume rather than fan-out. Built
separately, it composes with this one into a single lab platform: this project answers
"what is the state of specimen X," the sequence-data project answers "what did sequencer Y
emit, aggregated over the last minute." Corresponds to backlog proposal #3. Keeping the
two as distinct systems — state read-model vs. event stream — is the architecture, not an
accident.

## Smaller seams

- **Schema evolution:** add a column to `specimens` live (e.g. `qc_flag`); handle it through
  Debezium schema events without downtime; then Avro + schema registry as the grown-up
  version.
- **Consumer scaling story:** run 2–3 indexer instances, demonstrate partition assignment
  and rebalancing under load (partly covered by chaos drills, could go deeper with KEDA).
- **A second source table family** (e.g. `specimen_transfers` — aliquoting / custody moves)
  shaped as events rather than state, to contrast state-shaped vs event-shaped topics and
  compaction choices — a lab-authentic way to explore the distinction without leaving the
  domain.
