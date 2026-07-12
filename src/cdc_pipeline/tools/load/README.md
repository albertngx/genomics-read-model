# `cdc-load` — write-load generator

Async, open-loop load generator that drives the tracking **API** (story 02.3). Console
script: `cdc-load`. Design rationale and findings live in
[docs/pipeline/load-generator.md](../../../../docs/pipeline/load-generator.md) and
[learnings 04](../../../../learnings/04-load-generation-and-workload-modeling.md); this file
is just how to run it.

## Prerequisites

The stack must be up, migrated, and seeded (the generator bootstraps its key pool from
existing specimens/runs via the API):

```bash
make up && make seed        # brings the stack healthy and loads ~30 runs / ~5k specimens
```

## Quick start

```bash
make load                                        # steady mix, RATE=1000, DURATION=60
make load SCENARIO=burst RATE=200 DURATION=120
make load SCENARIO=reassignment-storm RATE=300 SEED=7 DURATION=90
```

`make load` variables: `SCENARIO RATE DURATION CONCURRENCY PROCESSES SEED`.

## Direct invocation

```bash
uv run cdc-load --help                            # full flag reference
uv run cdc-load steady --rate 1000 --duration 300 --concurrency 48 --seed 3
uv run cdc-load steady --rate 500 --ramp-to 2000  # ramp the rate over the run
uv run cdc-load steady --mix "update=80,insert=20,delete=0,dispose=0"
uv run cdc-load delete-wave --rate 400 --seed 1   # tombstone pressure
```

### Scenarios

- `steady` — default 70/15/10/5 mix at a constant rate.
- `delete-wave` — inserts feeding a torrent of registered hard-deletes (tombstones).
- `reassignment-storm` — sequencer/protocol changes on hot runs under specimen load (§7).
- `burst` — steady mix with 10× rate spikes.

### Useful flags

| Flag | Default | Meaning |
|---|---|---|
| `--rate, -r` | 1000 | target requests/second (split across `--processes`) |
| `--duration, -d` | 60 | run length, seconds |
| `--concurrency, -c` | 64 | async workers per process (in-flight cap) |
| `--processes, -p` | 1 | client processes across cores (see caveat below) |
| `--seed, -s` | 0 | RNG seed — makes the run reproducible |
| `--ramp-to` | — | ramp rate linearly from `--rate` to this value |
| `--mix` | scenario default | `update=…,insert=…,dispose=…,delete=…,run_update=…` |
| `--api-url` | `$API_URL` or `http://localhost:$API_PORT` | API base URL |
| `--pool-size` | 3000 | max existing specimens loaded into the Zipf key universe |

## Reproducibility

Same `--seed` (and `--processes`) ⇒ the same op-kind stream, key skew, and rate profile.
Exact per-request timing varies under async concurrency, but the workload *shape* repeats —
useful for demos and A/B comparisons of two builds under an identical load.

## macOS caveat (why `--processes` defaults to 1)

On Docker Desktop for Mac the host→container port proxy caps a host-run generator to a few
hundred req/s and stalls under the connection storm of several client processes. A single
process reliably drives ~420–570 req/s here; the API itself serves each write in ~5 ms. To
drive the full 1k/s the stack is tuned for, run on a Linux Docker host / CI (where
`--processes 3–6` scales cleanly) or point `--api-url` at an in-network address. Details:
[docs/pipeline/load-generator.md](../../../../docs/pipeline/load-generator.md).

## Layout

| Module | Responsibility |
|---|---|
| `cli.py` | Typer CLI, live readout, summary block, multiprocess fan-out |
| `planner.py` | the seeded, deterministic operation stream (reproducibility contract) |
| `scenarios.py` | named scenarios + op-mix parsing |
| `keys.py` | Zipfian key sampler |
| `rates.py` | steady/ramp/burst rate profiles + the pacer |
| `client.py` | async httpx client + startup key-pool bootstrap |
| `runner.py` | dispatcher + worker pool + readout orchestration |
| `parallel.py` | multi-process sharding + aggregate stats |
| `stats.py` | latency percentiles + throughput bookkeeping |
| `model.py` | shared `OpKind` / `Operation` / `Result` types |
