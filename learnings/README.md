# Learnings

Short notes explaining the *why* behind bits of this system as we build them —
the concepts that are easy to configure blindly and painful to misunderstand.
One file per topic. Distinct from:

- **ARCHITECTURE.md** — the up-front design and decision log (D1–D8).
- **docs/adr/** — Architecture Decision Records: one decision, its context,
  alternatives, and consequences, dated and immutable once accepted.

Learnings are the "aha, *that's* what that flag does" layer.

## Index

- [01 — Postgres logical replication: `wal_level` and `REPLICA IDENTITY`](01-postgres-logical-replication.md)
  — the two Postgres settings CDC lives or dies on.
- [02 — Postgres publications, and why capture config lives in the migration](02-publications-and-capture-config.md)
  — what a publication actually is, and why the schema (not the connector) owns what's captured.
- [03 — Kafka connectors, and how Debezium is fed by Postgres](03-kafka-connect-and-connectors.md)
  — what a connector is, why Debezium runs inside Kafka Connect, and its Postgres/Kafka interfaces.
- [04 — Load generation: workload modeling, skew, and reproducibility](04-load-generation-and-workload-modeling.md)
  — rate vs. concurrency, why Zipf skew and seeding are features, and why the API was never the slow part.
