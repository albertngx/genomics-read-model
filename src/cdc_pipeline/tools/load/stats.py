"""Latency/throughput bookkeeping: a numpy-free percentile and a :class:`StatsCollector`
that serves both the once-a-second live readout and the final summary block.

The collector keeps two views of the same stream: cumulative totals for the summary, and a
resettable *interval* window for the live line. All updates are synchronous and cheap
(append + counter bump), so recording from many worker coroutines on one event loop needs
no locking.
"""

from __future__ import annotations

import time
from collections import Counter
from dataclasses import dataclass, field

from cdc_pipeline.tools.load.model import OpKind, Result


def percentile(values: list[float], pct: float) -> float:
    """Return the ``pct`` percentile (0–100) of ``values`` using nearest-rank.

    Nearest-rank (rather than interpolation) is deliberate: for latency it reports a real
    observed sample, which is what a p99 SLO check wants. Returns ``0.0`` for an empty list.
    """
    if not values:
        return 0.0
    ordered = sorted(values)
    if pct <= 0:
        return ordered[0]
    if pct >= 100:
        return ordered[-1]
    # nearest-rank: rank = ceil(pct/100 * n), 1-indexed.
    rank = -(-len(ordered) * int(pct * 100) // 10000)  # ceil without float rounding error
    return ordered[max(1, rank) - 1]


@dataclass
class Snapshot:
    """A read-only view of the interval window, produced for one live-readout line."""

    seconds: float
    count: int
    errors: int
    achieved_rps: float
    error_rate: float
    p50_ms: float
    p99_ms: float


@dataclass
class StatsCollector:
    """Accumulates :class:`Result`s. Thread-unsafe by design (single event loop)."""

    started_at: float = field(default_factory=time.perf_counter)

    # Cumulative, for the final summary.
    total: int = 0
    errors: int = 0
    latencies: list[float] = field(default_factory=list)
    by_kind: Counter[OpKind] = field(default_factory=Counter)
    errors_by_kind: Counter[OpKind] = field(default_factory=Counter)
    error_reasons: Counter[str] = field(default_factory=Counter)
    skipped: int = 0  # ops the planner could not satisfy (e.g. no delete target available)

    # Interval window, reset by each snapshot().
    _win_count: int = 0
    _win_errors: int = 0
    _win_latencies: list[float] = field(default_factory=list)
    _win_start: float = field(default_factory=time.perf_counter)

    def record(self, result: Result) -> None:
        self.total += 1
        self.latencies.append(result.latency_ms)
        self.by_kind[result.kind] += 1
        self._win_count += 1
        self._win_latencies.append(result.latency_ms)
        if not result.ok:
            self.errors += 1
            self.errors_by_kind[result.kind] += 1
            self.error_reasons[result.error or "unknown"] += 1
            self._win_errors += 1

    def record_skip(self) -> None:
        """A planned op could not be built (no eligible key). Not an API error."""
        self.skipped += 1

    def snapshot(self) -> Snapshot:
        """Return the current interval window and reset it for the next tick."""
        now = time.perf_counter()
        seconds = max(now - self._win_start, 1e-9)
        snap = Snapshot(
            seconds=seconds,
            count=self._win_count,
            errors=self._win_errors,
            achieved_rps=self._win_count / seconds,
            error_rate=(self._win_errors / self._win_count) if self._win_count else 0.0,
            p50_ms=percentile(self._win_latencies, 50),
            p99_ms=percentile(self._win_latencies, 99),
        )
        self._win_count = 0
        self._win_errors = 0
        self._win_latencies = []
        self._win_start = now
        return snap

    @property
    def elapsed(self) -> float:
        return max(time.perf_counter() - self.started_at, 1e-9)

    @property
    def error_rate(self) -> float:
        return (self.errors / self.total) if self.total else 0.0

    @property
    def achieved_rps(self) -> float:
        return self.total / self.elapsed
