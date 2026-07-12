# 0004 — Sustaining 1k writes/s: API workers, pool sizing, and `synchronous_commit`

- **Status:** Accepted
- **Date:** 2026-07-12
- **Context story:** 02.3 (load generator)

## Context

Story 02.3's acceptance bar is that the stack **sustains 1,000 writes/s against the compose
stack for 5 minutes with <0.1% errors** — and it explicitly makes the tuning to get there
part of the story. The first run of the new load generator (`cdc-load`) at 1k/s collapsed:
~91% of requests timed out at 10s, throughput fell to single digits.

We profiled rather than guessed, and the bottleneck moved three times:

1. **DB connection pool starvation.** FastAPI runs the sync endpoints in Starlette's thread
   pool (~40 threads); each in-flight request holds one DB connection for its whole
   duration. SQLAlchemy's default pool is 5 + 10 overflow = **15**. The extra threads block
   waiting for a connection, and the wait cascades into request timeouts — while Postgres
   itself sat at ~0% CPU. Raising the pool removed the timeouts but left throughput at
   ~190/s.
2. **A single API process is GIL-bound.** With the pool fixed, load still capped ~190/s with
   only **one active Postgres backend** at any instant and the API at ~1 core. A single
   uvicorn worker serialises the per-request Python work (Pydantic validation + SQLAlchemy
   ORM + middleware) on one interpreter; measured ceiling ≈200 req/s. `synchronous_commit`
   made no difference here, confirming Postgres was not the limit.
3. **The load *client*, and the host network path.** The API's own logged latency was
   **p50 5ms / p99 9ms** — it was never slow. The generator, a single asyncio + httpx event
   loop, tops out at a few hundred req/s on one core; and Docker Desktop's macOS
   host→container port proxy caps *host* throughput at ~400/s regardless of client count
   (an in-container client reaches 800+/s on the same API).

## Decision

Three server-side changes, plus a client/runner change (documented in the load-generator
learnings note):

1. **Run the API as multiple uvicorn worker processes** — `API_WORKERS` (default **4**) in
   `deploy/api.Dockerfile`. Real parallelism across cores for the GIL-bound write path.
2. **Size the DB pool above the request-thread concurrency, per worker** — `DB_POOL_SIZE`
   (default **20**) + `DB_MAX_OVERFLOW` (default **10**) in `create_api_engine`, with
   `pool_timeout=10` to fail fast rather than pile up. Server-wide connections =
   `API_WORKERS × (pool_size + max_overflow)`, kept below Postgres `max_connections`.
3. **Postgres: `synchronous_commit=off` and `max_connections=200`** in the compose command.
   The first stops each COMMIT blocking on a WAL fsync (the classic Docker-for-Mac write
   wall); the second gives the multi-worker pools headroom.

To actually drive 1k/s we also run the generator **in-network** (`make load` launches it as
a throwaway container off the API image, targeting `http://api:8000`) to bypass the host
port proxy, and it shards across processes (`--processes`).

## Rationale & consequences

- **`synchronous_commit=off` is safe for this pipeline.** It relaxes only the
  *client-durability* window: a COMMIT returns before its WAL record is fsync'd, so on an OS
  crash the newest few hundred ms of commits can be lost. It does **not** risk corruption
  (that's `fsync=off`, which we did *not* set), and — critically — the WAL is still written
  and flushed, so Debezium (epic 03) still captures **every** change. For a dev/demo source
  of truth that is an easy trade for ~5× write throughput. A production deployment on real
  SSDs would revisit this per durability requirements.
- **Config over hard-coding.** Every knob is an env var with a sane default, so a beefier or
  leaner host can retune without a code change, and CI can dial rate/duration down.
- **The API was never the slow part.** The headline lesson: at 1k/s the source service
  serves each write in ~5ms; the work was removing *self-inflicted* contention (pool, GIL)
  and *measuring* the client/network path instead of assuming the server was slow.

## Alternatives considered

- **Rewrite endpoints as `async def`.** Would avoid the thread pool, but the DB driver work
  and ORM are still CPU, so a single process stays GIL-bound; multiple workers are needed
  regardless. Not worth the churn to the story-02.2 API for this.
- **`fsync=off` / `full_page_writes=off`.** Bigger throughput win, but risks a corrupt data
  directory on crash. Rejected — `synchronous_commit=off` gets most of the benefit safely.
- **Accept the host-proxy cap and call ~400/s "good enough."** Rejected: it would mean the
  acceptance criterion was met only by lowering it. Running the generator in-network proves
  the tuned stack genuinely sustains the target.
