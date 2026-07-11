# Epic 07 — Operability showcase

**Goal:** turn a working pipeline into an *operated* one: honest latency and lag numbers
under load, a scripted chaos suite where every drill ends with the verifier's verdict, and
the final documentation pass that lets the repo tell its story without the author in the
room. This epic closes phase 1 — the working agreement that every ARCHITECTURE.md §8
failure mode is exercised gets checked off here, against a coverage matrix.

**Done when:** `make chaos` is green twice in a row on a fresh stack, every number in the
README comes from a measurement rather than an estimate, and `make demo` runs the
rebuild-from-log demo on camera without human improvisation.

## Stories

1. [Latency and lag measurement](01-latency-and-lag.md) — end-to-end numbers, `make status`, finding the knee (M)
2. [Chaos drill suite](02-chaos-suite.md) — scripted failure injection, the §8 coverage matrix (L)
3. [Final docs and the scripted demo](03-final-docs-and-demo.md) — `make demo`, README with real numbers, write-up, close-out (M)

## Field notes

*(fill in as you build)*
