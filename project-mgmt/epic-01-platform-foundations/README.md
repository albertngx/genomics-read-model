# Epic 01 — Platform foundations

**Goal:** a one-command local stack (`make up`) with every infrastructure piece running,
health-checked, and smoke-tested — so no later story ever loses a session to environment
yak-shaving.

**Components:** Postgres (logical replication enabled), Apache Kafka (KRaft mode) + kafka-ui,
Kafka Connect with Debezium, Elasticsearch, Redis. Python tooling for the repo.

**Done when:** a fresh clone on a clean machine reaches green smoke tests with
`make up && make smoke`, and CI runs lint + tests on every push.

## Stories

1. [Compose stack](01-compose-stack.md) — all services up, configured, health-checked (L)
2. [Repo tooling](02-repo-tooling.md) — Python project scaffolding, Makefile, CI (M)
3. [Smoke tests](03-smoke-tests.md) — automated proof the stack actually works (M)

## Field notes

- **01.1 — container runtime saga.** Docker Desktop first hung (`docker info` never
  returned) → installed Colima (CLI-only VM) which worked first try → Docker Desktop then
  recovered and we settled on it, keeping Colima documented as a fallback. Running both at
  once made containers appear to "vanish" (port contention + Docker Desktop stealing the
  active `docker` context). Final: **Docker Desktop**, context `desktop-linux`. Full story
  in [ADR-0002](../../docs/adr/0002-colima-over-docker-desktop.md). The runtime is a
  swappable detail — the compose stack never changed.
- **01.1 — `.env` loading.** `docker compose -f deploy/docker-compose.yml` loads `.env`
  from the *compose file's* directory (`deploy/`), not the repo root, so `${VAR}` came
  out blank. Fixed by pinning `--env-file .env` in the Makefile.
- **01.1 — storage.** Named Docker volumes over in-repo bind mounts (macOS Postgres/ES
  bind-mount permission & fsync problems). [ADR-0001](../../docs/adr/0001-postgres-storage-named-volumes.md).
- **01.1 — Kafka listeners.** Dual listener setup: INTERNAL `kafka:9092` for
  in-network clients (connect, kafka-ui), EXTERNAL `localhost:9092` (host→container
  9094) for tooling on the Mac. All six services reach healthy; `wal_level=logical`
  and `make nuke && make up` verified.
