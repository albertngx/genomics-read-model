# 04 — Load generation: workload modeling, skew, and reproducibility

*Context: story 02.3 (`cdc-load`). Why the load generator is shaped the way it is, and the
performance investigation that turned "1k/s collapses" into "the API serves each write in
5 ms." The tool is the workhorse epics 04–07 reuse to test idempotency, rebalance safety,
and rebuild time — so it's worth understanding what it does and does not prove.*

## Rate vs. concurrency: two different knobs

The most common load-testing mistake is conflating **rate** (how many requests you *start*
per second) with **concurrency** (how many are *in flight* at once). They answer different
questions:

- A **closed-loop** generator fixes concurrency: N workers each send, wait for the response,
  send again. Throughput is then an *output* — it can only be as fast as the system. This
  hides latency: if the server slows down, the generator politely slows with it, and you
  never see the backlog a real user population would create.
- An **open-loop** generator fixes the **arrival rate**: it issues requests on a schedule
  regardless of how fast responses come back. If the system can't keep up, the shortfall is
  *visible* as achieved-rate-below-target and rising latency — which is exactly what we want
  to see for consumer lag (epic 04) and rebalance stalls (epic 05).

`cdc-load` is open-loop. A **pacer** issues operations at the target rate (steady, ramp, or
bursting); a bounded queue feeds a pool of `--concurrency` workers. When the API can't
drain the queue, the pacer blocks on `put` and the achieved rate drops — the backlog shows
up in the numbers instead of being absorbed silently. `--rate` and `--concurrency` are
independent flags precisely because they measure different things.

## Why key skew (Zipf), not uniform

If every specimen is equally likely to be updated, two things that matter downstream never
get exercised: **per-key ordering** (do updates to the *same* row stay ordered through
Kafka partitions?) and **cache behavior** (a uniform workload is either all-hit or all-miss
— no hot set). Real lab traffic is skewed: a few active runs and their specimens take most
of the writes.

So updates draw keys **Zipfian** — key at rank *r* is chosen ∝ 1/r^α (α≈1.1 gives a
pronounced but not degenerate hot set). The universe is fixed at startup and its cumulative
weights are computed once, so each draw is an O(log n) bisect. The hot set is what makes the
`reassignment-storm` scenario interesting: re-assigning a handful of *hot* runs fans out to
re-indexing all their specimens (the ARCHITECTURE.md §7 denormalization storm).

## Why reproducibility is a feature (and how it's kept)

"Same seed ⇒ same run" lets a demo behave on camera the way it did in rehearsal, and lets a
regression compare two builds under an *identical* workload. Under async concurrency, exact
per-request timing can't be reproduced — but the workload *shape* can. The design:

- **One planner (one coroutine) owns one RNG.** Only it decides op kinds and keys, so the
  draw order is single-threaded and deterministic.
- **Each operation consumes a fixed number of RNG draws for its kind**, regardless of
  runtime state. A target that was deleted turns the op into a *skip*, decided from state
  **without** consuming RNG. So the draw stream — op-kind sequence, key skew, field values —
  is a pure function of the seed. (There's a unit test that asserts the RNG lands in an
  identical state whether or not a shard is fed inserted ids mid-run.)
- The only thing that *can't* be reproduced is which server-assigned id a hard-delete lands
  on; those ids never enter the Zipf universe, so they perturb only the delete pool, never
  the draw stream.

A subtle consequence: **terminal operations must draw from a renewable pool.** Dispose and
hard-delete consume their target (a specimen is disposable/deletable once). If they drew
from the fixed hot set, a 5-minute run would exhaust it and start emitting self-inflicted
409s. So both draw from a *registered pool* fed by inserts and balanced against the insert
rate — which is also why the default mix is 70/15/10/5 and `delete-wave` is ~48/48 insert
vs. delete: production ≈ consumption keeps the wave sustained.

## The performance investigation: the API was never the slow part

The first run at 1k/s collapsed — 91% of requests timed out. We profiled instead of
guessing, and the bottleneck moved three times (full write-up in
[ADR 0004](../docs/adr/0004-write-path-load-tuning.md)):

1. **DB pool starvation.** Sync FastAPI endpoints run in a ~40-thread pool; each holds a DB
   connection for the whole request. SQLAlchemy's default pool is 15, so threads blocked
   waiting for connections and cascaded into timeouts — while Postgres sat at ~0% CPU. Fix:
   size the pool above the thread count (`DB_POOL_SIZE`).
2. **One API process is GIL-bound.** With the pool fixed, throughput capped ~190/s with a
   single active Postgres backend — one interpreter serialising Pydantic + ORM + middleware.
   Fix: run multiple uvicorn workers (`API_WORKERS`).
3. **The client and the host network, not the server.** The decisive measurement: the API's
   own logged latency was **p50 5 ms / p99 9 ms**. It was never slow. A single asyncio +
   httpx event loop tops out at a few hundred req/s on one core, and — the big one on this
   machine — **Docker Desktop for Mac's host→container port proxy caps host throughput to a
   few hundred req/s** no matter how many client processes you run (an in-container client
   reaches 800+/s against the same API).

The lesson worth keeping: **measure the layer, don't assume the server.** Comparing the
API's self-reported latency against the client's observed latency localised the problem to
the network path in one step, after a lot of fruitless server-side tuning.

## What the tool proves — and what it doesn't, here

- **Proven:** the tuned source service serves each write in ~5 ms and holds <0.1% errors;
  the workload is skewed, reproducible, and scenario-driven.
- **Environment caveat:** on Docker Desktop for Mac the host-proxy hop, not the API, caps a
  host-run generator at a few hundred req/s, and firing many client processes at once storms
  bootstrap (mitigated with staggered shard starts). The full 1k/s the stack is tuned for is
  best demonstrated where there's no host-proxy hop — a Linux Docker host or CI, or by
  pointing `--api-url` at an in-network address. This is a *harness* limit, not a limit of
  the API, Postgres, or the generator's design.

## `synchronous_commit=off` is safe for CDC

Part of the Postgres tuning. It stops each COMMIT blocking on a WAL fsync (the classic
Docker-for-Mac write wall), relaxing only the crash-durability *window* — the newest few
hundred ms of commits can be lost on an OS crash. It is **not** `fsync=off` (which risks
corruption). Crucially the WAL is still written and flushed, so **Debezium still captures
every change** (epic 03). For a dev/demo source of truth, ~5× write throughput for a relaxed
durability window is an easy trade.
