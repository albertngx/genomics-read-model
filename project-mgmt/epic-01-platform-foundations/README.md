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

*(fill in as you build — surprises, gotchas, decisions)*
