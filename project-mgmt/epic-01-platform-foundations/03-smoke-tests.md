# Story 01.3 — Smoke tests

- **Status:** todo
- **Effort:** M
- **Depends on:** 01.1, 01.2

## Context

"The stack is up" must be a program's verdict. These tests also become the canary that
catches config drift when images are bumped later.

## Tasks

- [ ] `make smoke` runs a pytest suite that, against the live stack:
      - connects to Postgres and asserts `wal_level = logical`,
      - creates a throwaway topic on Kafka, produces 100 messages, consumes them back,
        asserts count and ordering per key, deletes the topic,
      - indexes and retrieves a document in Elasticsearch,
      - SET/GET/DEL round-trip on Redis,
      - hits the Kafka Connect REST API and asserts the Debezium plugin is installed
        (`/connector-plugins` contains `PostgresConnector`).
- [ ] Clear failure messages: each failing check says what is misconfigured and where.

## Acceptance criteria

- [ ] `make up && make smoke` green from cold, twice in a row (idempotent — no leftover
      topics/docs/keys).
- [ ] Stopping Redis makes exactly the Redis check fail with a readable message.

## What this teaches

First contact with the Kafka client API (produce/consume/admin) against real Apache Kafka
in a low-stakes setting, and the habit of executable environment assertions — the same
muscle the verifier and chaos drills use later.
