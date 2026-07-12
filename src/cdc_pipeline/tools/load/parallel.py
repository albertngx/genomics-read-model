"""Multi-process load generation.

A single Python process is one asyncio event loop on one core; through httpx it tops out at
a few hundred req/s, well short of the story's 1k/s target — the loop, not the API, is the
ceiling (the API serves each write in ~5 ms). Real load tools scale by running many workers
across cores; this module does the same with :mod:`multiprocessing`.

Each child process is an independent shard: its own event loop, HTTP client, and seeded
planner. The target rate and worker count are split across shards. Reproducibility holds at
the shard level — child *i* uses ``seed + i`` — so a run is reproducible given ``(seed,
processes)``; changing the process count changes the sharding (and is reported).

Live progress uses two shared counters (total, errors) that children bump each readout tick;
the parent prints an aggregate line. Full per-shard stats (including every latency sample)
come back over a queue at the end and are merged for the final summary.
"""

from __future__ import annotations

import multiprocessing as mp
import time
from collections import Counter
from dataclasses import dataclass

from cdc_pipeline.tools.load.model import OpKind
from cdc_pipeline.tools.load.stats import StatsCollector


@dataclass
class ShardSpec:
    """Everything a child needs to run its shard. Must stay picklable (spawn start method)."""

    index: int
    api_url: str
    scenario_name: str
    mix_spec: str | None  # raw --mix override, or None to use the scenario default
    rate: float  # this shard's target rate (already divided by process count)
    ramp_to: float | None  # this shard's ramp target, or None
    duration: float
    concurrency: int
    seed: int
    pool_size: int
    timeout: float
    insert_prefix: str


def _run_shard(spec: ShardSpec, total: "mp.Value", errors: "mp.Value", result_q: "mp.Queue") -> None:
    """Child entrypoint: run one shard quietly, streaming counters and a final result."""
    import asyncio

    from cdc_pipeline.tools.load.client import LoadClient
    from cdc_pipeline.tools.load.planner import WorkloadPlanner
    from cdc_pipeline.tools.load.rates import Pacer, RampRate
    from cdc_pipeline.tools.load.runner import run_load
    from cdc_pipeline.tools.load.scenarios import get_scenario, parse_mix
    from cdc_pipeline.tools.load.stats import Snapshot

    scenario = get_scenario(spec.scenario_name)
    mix = parse_mix(spec.mix_spec) if spec.mix_spec else scenario.mix
    profile = (
        RampRate(spec.rate, spec.ramp_to, spec.duration)
        if spec.ramp_to is not None
        else scenario.rate_factory(spec.rate, spec.duration)
    )
    stats = StatsCollector()

    def on_readout(snap: Snapshot, elapsed: float) -> None:
        with total.get_lock():
            total.value += snap.count
        with errors.get_lock():
            errors.value += snap.errors

    async def _go() -> None:
        async with LoadClient(spec.api_url, concurrency=spec.concurrency, timeout=spec.timeout) as c:
            bootstrap = await c.bootstrap(spec.pool_size)
            planner = WorkloadPlanner.build(
                scenario, mix, bootstrap, spec.seed, insert_prefix=spec.insert_prefix
            )
            await run_load(
                planner=planner,
                client=c,
                pacer=Pacer(profile),
                duration=spec.duration,
                concurrency=spec.concurrency,
                stats=stats,
                on_readout=on_readout,
            )

    asyncio.run(_go())
    result_q.put(_serialize(stats))


def _serialize(stats: StatsCollector) -> dict:
    """Reduce a shard's stats to a picklable dict for the parent to merge."""
    return {
        "total": stats.total,
        "errors": stats.errors,
        "skipped": stats.skipped,
        "latencies": stats.latencies,
        "by_kind": {k.value: v for k, v in stats.by_kind.items()},
        "errors_by_kind": {k.value: v for k, v in stats.errors_by_kind.items()},
        "error_reasons": dict(stats.error_reasons),
    }


def _merge(results: list[dict], elapsed: float) -> StatsCollector:
    """Combine shard results into one :class:`StatsCollector` for the final summary."""
    merged = StatsCollector(started_at=time.perf_counter() - elapsed)
    for r in results:
        merged.total += r["total"]
        merged.errors += r["errors"]
        merged.skipped += r["skipped"]
        merged.latencies.extend(r["latencies"])
        merged.by_kind.update({OpKind(k): v for k, v in r["by_kind"].items()})
        merged.errors_by_kind.update({OpKind(k): v for k, v in r["errors_by_kind"].items()})
        merged.error_reasons.update(r["error_reasons"])
    return merged


def _context() -> "mp.context.BaseContext":
    """Prefer ``fork`` (Linux, and the container `make load` runs in): it inherits the shared
    Value/Queue and, unlike ``spawn``, never re-imports the console-script ``__main__`` —
    which would otherwise re-enter the CLI and recursively spawn. Falls back to the platform
    default where fork is unavailable."""
    try:
        return mp.get_context("fork")
    except ValueError:  # pragma: no cover - non-fork platforms (Windows)
        return mp.get_context()


def run_parallel(
    specs: list[ShardSpec], duration: float, readout_interval: float = 1.0
) -> StatsCollector:
    """Fork one process per shard, print an aggregate live readout, and return merged stats."""
    ctx = _context()
    total = ctx.Value("q", 0)
    errors = ctx.Value("q", 0)
    result_q: mp.Queue = ctx.Queue()

    procs = [ctx.Process(target=_run_shard, args=(spec, total, errors, result_q)) for spec in specs]
    for i, p in enumerate(procs):
        p.start()
        # Stagger starts so N shards don't bootstrap their key pools (and open N connection
        # pools) simultaneously — a thundering herd that stalls startup at higher process
        # counts, especially through a host→container proxy.
        if i + 1 < len(procs):
            time.sleep(0.25)

    start = time.perf_counter()
    # A hard deadline so a wedged shard can never hang the run: duration plus generous grace
    # for startup (each shard bootstraps its key pool) and shutdown drain.
    deadline = start + duration + max(30.0, specs[0].timeout * 2 if specs else 30.0)
    last_total = 0
    last_t = start
    # Aggregate readout while children run. Latency percentiles aren't cheap to share live,
    # so the live line reports throughput + error rate; the final summary has full latencies.
    while any(p.is_alive() for p in procs) and time.perf_counter() < deadline:
        time.sleep(readout_interval)
        now = time.perf_counter()
        with total.get_lock():
            t = total.value
        with errors.get_lock():
            e = errors.value
        rate = (t - last_total) / max(now - last_t, 1e-9)
        err_rate = (e / t * 100) if t else 0.0
        print(
            f"[{now - start:6.1f}s] {rate:8.1f} req/s  err {err_rate:5.2f}%  "
            f"(aggregate over {len(procs)} processes)"
        )
        last_total, last_t = t, now

    for p in procs:  # terminate any straggler that blew the deadline
        if p.is_alive():
            p.terminate()

    results: list[dict] = []
    for _ in procs:
        try:
            results.append(result_q.get(timeout=5))
        except Exception:  # queue.Empty (or a shard that died before reporting)
            break
    for p in procs:
        p.join(timeout=5)

    return _merge(results, elapsed=time.perf_counter() - start)
