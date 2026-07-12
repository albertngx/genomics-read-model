"""The async engine: one dispatcher paces the deterministic op stream, a pool of workers
executes it, and a readout task samples the stats window once a second.

Why a single dispatcher plus a worker pool (rather than N self-pacing workers): only the
dispatcher calls :meth:`WorkloadPlanner.next`, so the RNG is driven from one place and the
run stays reproducible (see :mod:`cdc_pipeline.tools.load.planner`). The workers only
execute — they add nothing to the draw stream except feeding insert ids back via
:meth:`WorkloadPlanner.on_inserted`.

The queue is bounded, so when the API can't keep up the dispatcher blocks on ``put`` — the
shortfall surfaces as achieved-rate below target rather than an unbounded in-memory backlog.
"""

from __future__ import annotations

import asyncio
import time
from collections.abc import Callable

from cdc_pipeline.tools.load.client import LoadClient
from cdc_pipeline.tools.load.planner import WorkloadPlanner
from cdc_pipeline.tools.load.rates import Pacer
from cdc_pipeline.tools.load.stats import Snapshot, StatsCollector

ReadoutCallback = Callable[[Snapshot, float], None]


async def _dispatch(
    planner: WorkloadPlanner,
    pacer: Pacer,
    queue: asyncio.Queue,
    stats: StatsCollector,
    duration: float,
) -> None:
    deadline = time.perf_counter() + duration
    while time.perf_counter() < deadline:
        await pacer.tick()
        op = planner.next()
        if op is None:
            stats.record_skip()  # no eligible target for the drawn op — not an API error
            continue
        remaining = deadline - time.perf_counter()
        if remaining <= 0:
            return
        try:
            # A bounded put so a saturated API (all workers stuck) can't make the run
            # overrun its --duration: if the queue stays full past the deadline, stop.
            await asyncio.wait_for(queue.put(op), timeout=remaining)
        except asyncio.TimeoutError:
            return


async def _worker(
    client: LoadClient, planner: WorkloadPlanner, queue: asyncio.Queue, stats: StatsCollector
) -> None:
    while True:
        op = await queue.get()
        if op is None:  # sentinel — drain complete
            queue.task_done()
            return
        result = await client.execute(op)
        stats.record(result)
        if result.created_id is not None:
            planner.on_inserted(result.created_id)
        queue.task_done()


async def _readout(
    stats: StatsCollector, interval: float, on_readout: ReadoutCallback, stop: asyncio.Event
) -> None:
    start = time.perf_counter()
    while not stop.is_set():
        try:
            await asyncio.wait_for(stop.wait(), timeout=interval)
        except asyncio.TimeoutError:
            pass
        if stop.is_set():
            return
        on_readout(stats.snapshot(), time.perf_counter() - start)


async def run_load(
    *,
    planner: WorkloadPlanner,
    client: LoadClient,
    pacer: Pacer,
    duration: float,
    concurrency: int,
    stats: StatsCollector,
    on_readout: ReadoutCallback,
    readout_interval: float = 1.0,
) -> StatsCollector:
    """Drive the workload for ``duration`` seconds and return the populated ``stats``.

    Cancelling the awaiting task (Ctrl-C) still lets the caller report the partial stats.
    """
    queue: asyncio.Queue = asyncio.Queue(maxsize=concurrency * 2)
    stop = asyncio.Event()

    workers = [
        asyncio.create_task(_worker(client, planner, queue, stats)) for _ in range(concurrency)
    ]
    readout = asyncio.create_task(_readout(stats, readout_interval, on_readout, stop))

    try:
        await _dispatch(planner, pacer, queue, stats, duration)
    finally:
        await _shutdown_workers(queue, workers, grace=client.timeout + 1.0)
        stop.set()
        await readout
    return stats


async def _shutdown_workers(
    queue: asyncio.Queue, workers: list[asyncio.Task], grace: float
) -> None:
    """Drain the queue and stop workers, giving in-flight requests up to ``grace`` seconds
    to finish before cancelling stragglers (so a wedged API can't hang shutdown)."""

    async def _drain() -> None:
        for _ in workers:
            await queue.put(None)  # one sentinel each
        await asyncio.gather(*workers)

    try:
        await asyncio.wait_for(_drain(), timeout=grace)
    except asyncio.TimeoutError:
        for worker in workers:
            worker.cancel()
        await asyncio.gather(*workers, return_exceptions=True)
