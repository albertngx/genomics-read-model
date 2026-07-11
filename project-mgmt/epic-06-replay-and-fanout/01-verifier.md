# Story 06.1 — Verifier

- **Status:** todo
- **Effort:** L
- **Depends on:** epics 04 and 05

## Context

ARCHITECTURE.md §6: "in sync" is a program's verdict, never a human's impression. The
verifier diffs full Postgres state against each derived store by key set and content hash
and exits non-zero on any discrepancy. Two subtleties make it a real story: expected state
differs per store (the ES active set excludes disposed and hard-deleted specimens and embeds
run fields; Redis keeps disposed rows per D9), and a live system always has in-flight
events — the verifier must distinguish lag from drift.

## Tasks

- [ ] `tools/verifier`: expected-state functions from a single Postgres snapshot query
      (repeatable-read transaction): expected ES documents (active specimens, denormalized
      run fields derived from current PG rows) and expected Redis hashes (all rows
      including disposed). Reuse story 05.3's expected-state function for the Redis leg.
- [ ] Canonical serialization + content hash per key — decimal strings (D6), normalized
      timestamps — with a per-field diff printed on mismatch, not just "hash differs".
- [ ] Fetch derived state efficiently: ES `search_after`, Redis `SCAN`. Full run over the
      5k-specimen seed set must take seconds, not minutes.
- [ ] Two modes: `--strict` (quiesced: generator stopped, consumer lag zero — any diff
      fails) and `--converge T` (under load: a differing key is re-checked and must
      converge within T seconds, otherwise it is drift). `make verify` wraps strict mode,
      including waiting for lag to drain.
- [ ] Readable report: missing / extra / mismatched per store, exit non-zero on any
      confirmed diff.
- [ ] **Test the oracle.** Plant bugs by hand — corrupt one field in an ES doc, delete one
      Redis key, leave one stale document — and confirm the verifier catches all three and
      names them precisely. An oracle that has never caught a planted bug is a hope, not
      an oracle.

## Acceptance criteria

- [ ] Fresh seeded stack, lag drained: `make verify` green for both stores.
- [ ] All three planted-bug classes caught, each with a report that names the key and the
      field.
- [ ] `--converge` mode finishes green under `make load RATE=1000` — lag demonstrably not
      reported as drift.
- [ ] Runtime on the seed set recorded in field notes (seconds, not minutes).

## What this teaches

What "consistent" means operationally: reconciliation as an engineering artifact, the
lag-vs-drift distinction that every real sync checker must make, and the habit of testing
the test before trusting it — this tool is the assertion at the end of every later drill.
