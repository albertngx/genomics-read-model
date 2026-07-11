# Story 07.3 — Final docs and the scripted demo

- **Status:** todo
- **Effort:** M
- **Depends on:** 07.2

## Context

The repo is a portfolio piece; its story must survive without the author narrating over
someone's shoulder. This story is also the phase-1 close-out: working agreements checked,
statuses honest, roadmap corrected by what was actually learned.

## Tasks

- [ ] `make demo`: the README's headline (rebuild-from-log, story 06.2) as a narrated
      script — echoes what it is about to do, pauses at the money moments (index deleted;
      group reset; verifier verdict), seeded so it behaves the same on camera as in
      rehearsal.
- [ ] README rewrite: quickstart re-verified from a fresh clone on a clean machine; real
      numbers in (rebuild time and compaction ratio from 06.2, the throughput matrix from
      07.1, a pointer to the chaos matrix); the architecture-at-a-glance diagram checked
      against what was actually built.
- [ ] Distill the field notes from all seven epics into `docs/WRITEUP.md`: the dual-write
      argument, at-least-once as an observed fact, the denormalization repair story, the
      numbers — the interview material the working agreements promised.
- [ ] Runbook consolidation: 03.4's drills, 06.2/06.3's rebuild-vs-reindex, and the DLQ
      loop into one `docs/RUNBOOK.md` with a table of contents.
- [ ] Close-out: every story's acceptance criteria re-checked and statuses flipped to
      `done` only where they pass *now*; the §8 matrix green; `roadmap.md` re-read and
      amended with anything phase 1 learned that changes the phase-2 connector plan.

## Acceptance criteria

- [ ] The quickstart, followed verbatim on a clean machine (or VM), reaches a green
      `make demo`.
- [ ] The README contains zero unmeasured numbers and zero stale claims.
- [ ] Phase-1 close-out checklist fully ticked: all stories done, §8 matrix complete,
      roadmap updated.

## What this teaches

Finishing — the difference between "it works on my machine" and an artifact someone else
can clone, run, judge, and ask you hard questions about. The write-up is where seven epics
of field notes become the stories you tell in the interview.
