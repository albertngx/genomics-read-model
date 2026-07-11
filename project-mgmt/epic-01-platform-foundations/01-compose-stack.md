# Story 01.1 — Compose stack

- **Status:** done
- **Effort:** L
- **Depends on:** —

## Context

Everything downstream assumes these services exist and are correctly configured for CDC.
The two configuration details that matter most are buried here: Postgres must run with
`wal_level=logical`, and captured tables will need `REPLICA IDENTITY FULL`
(ARCHITECTURE.md D4) — get them wrong and epic 03 fails mysteriously.

## Tasks

- [ ] `deploy/docker-compose.yml` with: Postgres 16 (`wal_level=logical`,
      `max_wal_senders`/`max_replication_slots` ≥ 4), Apache Kafka (KRaft mode, single
      node, no ZooKeeper), kafka-ui, Kafka Connect (Debezium image, connector NOT yet
      registered — that's epic 03), Elasticsearch (single node, security disabled),
      Redis 7.
- [ ] Healthchecks on every service; dependent services use `depends_on: condition:
      service_healthy` so `make up` means *ready*, not just *started*.
- [ ] Named volumes for Postgres/Kafka/Elasticsearch; `make down` keeps data,
      `make nuke` destroys it.
- [ ] Pin image versions. No `latest`.
- [ ] `deploy/README.md`: one paragraph per service — why it's here, its port, its UI.

## Acceptance criteria

- [ ] `make up` from a fresh clone reaches all-healthy without manual steps.
- [ ] `docker compose ps` shows every service healthy; kafka-ui reachable in a browser.
- [ ] `SHOW wal_level` in Postgres returns `logical`.
- [ ] `make nuke && make up` works — the stack is fully reconstructible.

## What this teaches

What a CDC-capable Postgres actually requires (WAL levels, replication slots as a
first-class server resource), how a single-node Kafka runs in KRaft mode without
ZooKeeper, and the discipline of a lab you can destroy and rebuild — which the replay
demos will depend on constantly.
