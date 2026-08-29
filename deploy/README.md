# deploy/ — the local stack

One `docker-compose.yml` brings up every service, health-checked, on the ports
locked in [`../.env.example`](../.env.example). Run everything through the Makefile
from the repo root:

```bash
make up     # start all services, wait until every one is healthy
make ps     # docker compose ps — see health status
make down   # stop, KEEP data (named volumes survive)
make nuke   # stop and DESTROY all data (fully reconstructible test)
make logs   # tail all logs
```

`make up` uses `docker compose up -d --wait`, so it returns only when the stack is
*ready*, not merely *started* — dependent services wait on `service_healthy`.

## Services

| Service | Image | Port | Why it's here / UI |
|---|---|---|---|
| **postgres** | `postgres:16.4` | 5432 | Source of truth. Runs with `wal_level=logical` so Debezium can tail row changes off the WAL. Connect with `psql "$DATABASE_URL"`. |
| **kafka** | `apache/kafka:3.8.1` | 9092 | The durable, ordered, replayable log. Single node, KRaft mode (broker+controller, no ZooKeeper). Reach the broker at `localhost:9092`. Topic **auto-create is off** — the CDC topics come from [`topics/topics.json`](topics/topics.json) via `make topics` (story 03.3). |
| **kafka-ui** | `provectuslabs/kafka-ui:v0.7.2` | 8080 | Browser console for topics/messages/consumer groups/Connect — Apache Kafka ships none. Open <http://localhost:8080>. |
| **connect** | `debezium/connect:2.7.3.Final` | 8083 | Kafka Connect with the Debezium plugin. The CDC connector is registered from [`connectors/genomics.json`](connectors/genomics.json) via `make connector` (story 03.1). REST API at <http://localhost:8083>. |
| **elasticsearch** | `docker.elastic.co/.../8.15.0` | 9200 | Search read-model. Single node, security disabled (local lab only). REST at <http://localhost:9200>. |
| **redis** | `redis:7.4` | 6379 | Cache read-model. `redis-cli -p 6379 ping`. |

## Topics are designed, not defaulted

The broker runs with `auto.create.topics.enable=false`, so the CDC topics exist only
because `make topics` created them from [`topics/topics.json`](topics/topics.json) with the
partition counts and `cleanup.policy=compact` that ARCHITECTURE §4 requires. Left on
auto-create, Kafka would invent them with one partition and a 7-day delete policy — which
works fine right up until the snapshot ages out of the log and an offset-0 rebuild becomes
impossible. `make connector` depends on `make topics`, so the ordering is automatic;
every setting is justified in [`topics/topics.notes.md`](topics/topics.notes.md).

```bash
make topics            # create/converge (idempotent)
make topics-describe   # kafka-topics.sh --describe, plus a drift verdict
make compaction-demo   # watch 1,000 updates collapse to one record, and a tombstone die
```

## The one setting that matters

Postgres runs with **`wal_level=logical`** plus `max_wal_senders`/`max_replication_slots`
raised to 8. Without logical WAL, Debezium (epic 03) cannot capture changes at all —
it would fail mysteriously later. Verify any time the stack is up:

```bash
docker compose -f deploy/docker-compose.yml exec postgres psql -U cdc -d genomics -c "SHOW wal_level;"
# → logical
```

The companion setting, **`REPLICA IDENTITY FULL`**, is set per-table and so belongs
to epic 02/03 (there are no tables yet). Both are explained in
[`../learnings/01-postgres-logical-replication.md`](../learnings/01-postgres-logical-replication.md).

## Data / volumes

State lives in Docker-managed **named volumes** (`pg_data`, `kafka_data`, `es_data`,
`redis_data`), not in the repo. `make down` keeps them; `make nuke` destroys them so
you can prove the stack rebuilds from nothing. Rationale for named volumes over
in-repo bind mounts: [`../docs/adr/0001-postgres-storage-named-volumes.md`](../docs/adr/0001-postgres-storage-named-volumes.md).

## Gotchas

- **Docker.app must be running** before `make up` (the engine, not just the CLI).
- **Elasticsearch memory:** give Docker Desktop ≥6 GB RAM (Settings → Resources), or
  ES may exit on startup. If ES logs mention `vm.max_map_count`, that's a host kernel
  setting; on Docker Desktop it's usually already sufficient.
- **Ports in use:** if 5432/8080/9092/9200/6379/8083 are taken, change them in `.env`
  (not here) and re-run `make up`.
- **Never run two container engines at once.** Colima and Docker Desktop each publish the
  same host ports (one on IPv4, one on IPv6), and `localhost` resolves to both — so host-side
  clients silently alternate between *two different clusters*. Check with
  `lsof -nP -iTCP:9092 -sTCP:LISTEN`; see
  [`../docs/adr/0002-colima-over-docker-desktop.md`](../docs/adr/0002-colima-over-docker-desktop.md).
