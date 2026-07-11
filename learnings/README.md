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
