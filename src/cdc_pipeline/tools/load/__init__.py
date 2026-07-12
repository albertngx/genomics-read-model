"""``cdc-load`` — the write-load generator for the tracking API (story 02.3).

Async workers drive the **API** (never the database directly, ARCHITECTURE.md §2), so the
load exercises the same write path production traffic would. The workload is:

- **rate-driven** (open loop): a pacer issues operations at a target req/s — steady, ramp,
  or bursting — independently of how fast the API responds, so consumer lag and tail
  latency become visible instead of being hidden by a closed loop;
- **skewed**: hot specimens and runs are selected Zipfian, because uniform load hides the
  per-key ordering and cache behaviour the later epics care about;
- **reproducible**: every random decision comes from one seeded RNG owned by a single
  dispatcher, so the *shape* of a run (op-kind stream, key skew, rate profile) repeats
  bit-for-bit given ``--seed`` (see :mod:`cdc_pipeline.tools.load.planner`).

The public surface is the Typer CLI in :mod:`cdc_pipeline.tools.load.cli` (console script
``cdc-load``); everything else here is a testable building block.
"""
