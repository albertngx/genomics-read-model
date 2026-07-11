# 0001 — Stack storage: Docker named volumes over in-repo bind mounts

- **Status:** Accepted
- **Date:** 2026-07-11
- **Context story:** 01.1 (compose stack)

## Context

The stateful services — Postgres, Kafka, Elasticsearch, Redis — need persistent
storage that survives container restarts but can be wiped on demand (the
rebuild-from-nothing demos depend on a clean destroy). Docker offers two ways to do
this:

1. **Named volumes** — Docker-managed storage living inside the Docker Desktop VM,
   referenced by name (`pg_data`, etc.).
2. **Bind mounts** — a host directory mapped into the container, e.g. an in-repo
   `deploy/data/postgres/` (which our `.gitignore` already anticipates via `data/`
   and `deploy/**/data/`).

The in-repo bind-mount option is tempting: the data is visible in the working tree
and feels tangible.

## Decision

Use **named volumes** for all four stateful services.

## Rationale

- **Postgres + macOS bind mounts are genuinely problematic.** On Docker Desktop for
  macOS the host filesystem is shared into the Linux VM over a virtualization layer
  (gRPC-FUSE / VirtioFS). Postgres is sensitive to the resulting **file-permission and
  `fsync` semantics** and can refuse to initialize its data directory or suffer poor,
  inconsistent I/O. Named volumes live on a native Linux filesystem inside the VM and
  sidestep this entirely.
- **Elasticsearch has uid/permission requirements** on its data directory (it runs as
  a non-root user); a fresh host bind mount frequently lands root-owned and ES fails to
  start. Named volumes inherit correct ownership from the image.
- **Performance.** Native volume I/O in the VM is faster than the shared-filesystem
  path a bind mount crosses on macOS — relevant with Kafka + ES + Postgres together.
- **Clean lifecycle.** `docker compose down` keeps named volumes; `down -v`
  (`make nuke`) removes exactly them. No stray gitignored data directory to reason
  about, and the rebuild-from-nothing demos get a truly clean slate.

## Consequences

- Data is **not visible in the repo**. To inspect it you go through the service
  (`psql`, kafka-ui, ES REST) or `docker volume` commands — which is the right
  interface anyway; poking at raw data files is never how we read these stores.
- `make nuke` is the way to reset; `docker volume ls` shows the `cdc_*` volumes.
- The `.gitignore` entries for `data/` / `deploy/**/data/` are kept as a harmless
  safety net in case a future story deliberately introduces a bind mount (e.g. seeding
  a file into a container); no data is expected there by default.

## Alternatives considered

- **In-repo bind mounts (`deploy/data/...`, gitignored):** rejected for the macOS
  Postgres/Elasticsearch reliability problems above. The visibility benefit doesn't
  justify a stack that may not start on a clean machine — which is the exact promise
  epic 01 makes.
