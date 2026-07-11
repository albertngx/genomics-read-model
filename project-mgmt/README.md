# Project management

Phase 1 is broken into 7 epics, each a subfolder containing an epic charter (`README.md`)
and one file per story. Stories are the unit of work: sized for one focused session to
one weekend day, with acceptance criteria that a script or test can check wherever
possible.

## Epic sequence

```
epic-01-platform-foundations      the running stack: compose, tooling, smoke tests
epic-02-source-service            tracking API, schema, load generator
epic-03-cdc-ingestion             Debezium: connector, event anatomy, topics, failure drills
epic-04-search-indexer            the flagship consumer: idempotency, batching, joins, DLQ
epic-05-cache-consumer            second consumer: same guarantees, different store
epic-06-replay-and-fanout         verifier, rebuild demos, late-added analytics consumer
epic-07-operability-showcase      latency/lag measurement, chaos drills, final docs
```

Dependencies are linear at the epic level (each epic assumes the previous ones), with one
exception: epic 05 depends only on 01–03 and can be built before or in parallel with 04.

## Story format

Each story file contains:

- **Header** — status (`todo | in-progress | done`), effort (S ≈ 1–2h, M ≈ half day,
  L ≈ full day), dependencies.
- **Context** — why this story exists; links back to the relevant ARCHITECTURE.md section.
- **Tasks** — the concrete work.
- **Acceptance criteria** — checkable statements. A story is done when these pass, not
  when the code exists.
- **What this teaches** — the skill or concept the story is really for. This project's
  goal is technical growth; if a story has nothing to put here, it should be questioned.

## Working agreements

- ARCHITECTURE.md is upstream of stories. If implementation reveals the design is wrong,
  amend the decision log first, then the story.
- Acceptance criteria over vibes: "the verifier reports zero diffs under load" beats
  "sync works".
- Every failure mode listed in ARCHITECTURE.md §8 must be exercised by some story before
  phase 1 is called done.
- Findings that surprised you go into the epic README's "field notes" as you go — they
  are the raw material for the final write-up and the interview stories.
