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

1. [Connector deployment](01-connector-deployment.md) — Debezium registered and streaming (M)
2. [Event anatomy](02-event-anatomy.md) — EVENTS.md written from real captured events (M)
3. [Topic design and compaction](03-topic-design-and-compaction.md) — partitions, cleanup policy, tombstones (M)
4. [Failure drills: restarts and slot lag](04-failure-drills.md) — the disk-fill footgun, exercised (L)

## Field notes

*(fill in as you build)*
