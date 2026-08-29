# genomics.json — every field is a decision

The Debezium Postgres connector is configured entirely by the flat JSON map in
[`genomics.json`](genomics.json), registered via `make connector` (an idempotent
`PUT /connectors/genomics/config`). JSON can't carry comments, so this file is the
one-line-per-field rationale the config can't hold itself.

`${…}` values are placeholders substituted from the repo-root `.env` at registration
time (by `cdc_pipeline.tools.connector`), so no credential is hardcoded here — same
"change it once, in `.env`" rule the rest of the stack follows.

| Field | Value | Why |
|---|---|---|
| `connector.class` | `io.debezium.connector.postgresql.PostgresConnector` | The Debezium plugin baked into the `connect` image (story 01.3 asserts it's installed). |
| `tasks.max` | `1` | Postgres logical replication is a single ordered stream — there is nothing to parallelise. |
| `plugin.name` | `pgoutput` | Postgres's built-in logical-decoding output plugin (no `wal2json`/contrib to install). Chosen in story 02.1. |
| `database.hostname` / `database.port` | `postgres` / `5432` | The **compose-internal** address Connect uses to reach Postgres — not `localhost:5432` (that's the host-facing `DATABASE_URL`). Topology, not a secret, so it's a literal. |
| `database.user` / `.password` / `.dbname` | from `.env` | The one source of truth for credentials (`POSTGRES_USER/PASSWORD/DB`). |
| `topic.prefix` | `cdc` | Names the change topics `cdc.public.runs` and `cdc.public.specimens` (`<prefix>.<schema>.<table>`). |
| `publication.name` | `cdc_pub` | The publication the schema owns (migration 0002 `CREATE PUBLICATION cdc_pub FOR TABLE runs, specimens`). Debezium filters the stream to these tables. |
| `publication.autocreate.mode` | `disabled` | The connector must **not** create or alter the publication — the migration is the single authority on what's captured (adding a table is a reviewed schema change). Migration 0002 mandates this. |
| `slot.name` | `cdc_genomics` | This consumer's private replication slot — its resumable WAL cursor. Distinct from the publication: the slot is *how far we've read*, the publication is *which tables come down*. It also pins WAL until acknowledged (`make slot-status`). |
| `table.include.list` | `public.runs,public.specimens` | Explicit belt-and-suspenders alongside the publication filter — the captured set is stated in one glance, not inferred. |
| `snapshot.mode` | `initial` | On first start, read every current row and emit it as an `op:"r"` event (so a new consumer starts from a complete picture), then stream WAL changes forever. |
| `decimal.handling.mode` | `string` | **D6.** Postgres `NUMERIC` (`volume_ul`) must not become a lossy JSON float — emit it as a string; consumers parse it. Aliquot-volume fidelity is a data-integrity concern. |
| `tombstones.on.delete` | `true` | A DELETE emits both a delete event *and* a null-valued tombstone on the same key, which is what lets Kafka log compaction eventually drop the key (story 03.3). |
| `key.converter` / `value.converter` | `JsonConverter` | Plain JSON on the wire (D1/§9) — no schema registry, no Avro. |
| `key.converter.schemas.enable` / `value.converter.schemas.enable` | `false` | Emit the bare payload without Debezium's verbose embedded-schema wrapper, so events are **inspectable** — readable and self-explanatory in kafka-ui (ARCHITECTURE §9). |

## Not configured here (on purpose)

- **Topic compaction / partitioning / retention** — owned by story 03.3, not by this
  config: the topics are declared in [`../topics/topics.json`](../topics/topics.json) and
  created by `make topics` *before* the connector is registered (broker auto-create is
  off, so `make connector` depends on `make topics`). Debezium produces into topics that
  already exist with their designed shape; it never invents one. `tombstones.on.delete=true`
  above is this config's half of that bargain — it emits the null-valued record compaction
  needs to eventually drop a deleted key.
- **Heartbeats / `snapshot.mode` variants / signal tables** — not needed at this scale.
