# Architecture Decision Records

One file per decision, numbered and dated. An ADR captures a choice that was not
obvious — its context, the alternatives, and the consequences we accept — so that
future-us (and reviewers) can see *why*, not just *what*.

These complement the up-front decision log in
[`../../ARCHITECTURE.md`](../../ARCHITECTURE.md) §10 (D1–D8), which covers the big
design-time choices. ADRs here record decisions that surface *while building*.

Once accepted, an ADR is immutable; if we change our minds, we add a new ADR that
supersedes it rather than editing history.

## Index

- [0001 — Postgres/stack storage: named volumes over in-repo bind mounts](0001-postgres-storage-named-volumes.md) — Accepted, 2026-07-11
- [0002 — Container runtime: Docker Desktop, with Colima as fallback](0002-colima-over-docker-desktop.md) — Accepted, 2026-07-12
- [0003 — CDC capture config lives in schema migrations, not connector config](0003-cdc-capture-config-in-migrations.md) — Accepted, 2026-07-12
- [0004 — Sustaining 1k writes/s: API workers, pool sizing, and `synchronous_commit`](0004-write-path-load-tuning.md) — Accepted, 2026-07-12
