# Story 04.1 — Consumer skeleton

- **Status:** todo
- **Effort:** L
- **Depends on:** epic 03

## Context

The consumer loop's structure *is* its correctness. Commit-after-write (ARCHITECTURE.md
§5) decides whether crashes cause duplicates (absorbable) or data loss (not). This
skeleton — poll, process, flush, commit, handle shutdown and rebalance — will be reused
by every consumer in the project, so it's worth building as a small shared base
(`consumers/base.py`).

## Tasks

- [ ] `confluent-kafka` consumer: `enable.auto.commit=false`, explicit
      `commit(asynchronous=False)` only after the sink write succeeds. A comment at the
      commit site explaining what reordering these two lines would cause.
- [ ] Poll/process loop with clean structure: deserialize → handle → flush → commit.
      (Handler is a stub for now: log event key + op + LSN.)
- [ ] **Graceful shutdown:** SIGTERM/SIGINT → finish in-flight batch, commit, close with
      final offsets. Verify compose stop doesn't lose or re-process more than one batch.
- [ ] **Rebalance callbacks:** `on_partitions_revoked` commits before giving partitions
      up; log assignments. Run two instances, watch partitions split; kill one, watch
      them return.
- [ ] Consumer config documented choice by choice (`group.id`, `auto.offset.reset=
      earliest` — required for the epic-06 rebuild demo, `max.poll.interval.ms`).

## Acceptance criteria

- [ ] Two instances share the 6 partitions; killing one triggers rebalance and full
      coverage resumes — shown in logs.
- [ ] SIGTERM under load: restart resumes with ≤ one batch of redelivery, zero gaps
      (verified by marked-write counting as in story 03.4).
- [ ] The base class is used unchanged by a trivial second consumer (a stdout printer)
      to prove it's actually generic.

## What this teaches

Consumer groups, offsets, and rebalancing — the Kafka consumer mental model — plus the
commit-discipline reasoning that separates at-least-once from at-most-loss.
