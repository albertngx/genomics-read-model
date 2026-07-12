"""Rate profiles and the pacer that turns one into real time.

A profile is just ``rate(t) -> req/s`` over the run. The three shapes cover the story's
scenarios:

- :class:`SteadyRate` — constant N/s (the default, and the 1k/s acceptance target);
- :class:`RampRate` — linear from a start to an end rate over the duration, for finding the
  knee where errors begin;
- :class:`BurstRate` — a steady base with periodic 10× spikes, so consumer lag becomes
  visible when the API briefly outruns what downstream can drain.

:class:`Pacer` integrates the instantaneous rate into inter-arrival sleeps. It is *open
loop*: it schedules the next issue time from the target rate, not from how long the last
request took, so a slow API shows up as achieved-rate shortfall rather than being masked.
"""

from __future__ import annotations

import asyncio
import math
import time
from dataclasses import dataclass
from typing import Protocol


class RateProfile(Protocol):
    def rate_at(self, t: float) -> float:
        """Instantaneous target rate (req/s) at ``t`` seconds into the run."""
        ...

    def describe(self) -> str: ...


@dataclass(frozen=True)
class SteadyRate:
    rps: float

    def rate_at(self, t: float) -> float:
        return self.rps

    def describe(self) -> str:
        return f"steady {self.rps:g}/s"


@dataclass(frozen=True)
class RampRate:
    start_rps: float
    end_rps: float
    duration: float

    def rate_at(self, t: float) -> float:
        if self.duration <= 0:
            return self.end_rps
        frac = min(max(t / self.duration, 0.0), 1.0)
        return self.start_rps + (self.end_rps - self.start_rps) * frac

    def describe(self) -> str:
        return f"ramp {self.start_rps:g}→{self.end_rps:g}/s over {self.duration:g}s"


@dataclass(frozen=True)
class BurstRate:
    base_rps: float
    multiplier: float = 10.0
    period: float = 20.0  # one spike every ``period`` seconds
    spike_fraction: float = 0.25  # spike occupies this fraction of each period

    def rate_at(self, t: float) -> float:
        phase = (t % self.period) / self.period
        return self.base_rps * self.multiplier if phase < self.spike_fraction else self.base_rps

    def describe(self) -> str:
        return f"burst {self.base_rps:g}/s with {self.multiplier:g}× spikes every {self.period:g}s"


class Pacer:
    """Emits issue times matching a :class:`RateProfile`, in real (monotonic) time.

    ``await pacer.tick()`` returns once it is time to issue the next operation. The next
    issue time is advanced by ``1/rate(t)`` sampled at the current schedule position, so a
    changing rate (ramp/burst) is followed without integrating a closed-form inverse.
    """

    def __init__(self, profile: RateProfile):
        self._profile = profile
        self._start = time.perf_counter()
        self._next = self._start

    async def tick(self) -> None:
        now = time.perf_counter()
        elapsed = self._next - self._start
        rate = max(self._profile.rate_at(elapsed), 1e-9)
        self._next += 1.0 / rate
        sleep_for = self._next - now
        if sleep_for > 0:
            await asyncio.sleep(sleep_for)
        elif sleep_for < -1.0:
            # We have fallen more than a second behind schedule (the API can't keep up).
            # Reset the clock to "now" so we don't issue a thundering backlog the instant
            # it recovers — the shortfall already shows up as achieved < target.
            self._next = now

    def rate_at(self, t: float) -> float:
        return self._profile.rate_at(t)

    def describe(self) -> str:
        return self._profile.describe()


def total_expected(profile: RateProfile, duration: float, step: float = 0.5) -> int:
    """Rough count of operations a profile implies over ``duration`` (for progress display).

    A coarse Riemann sum — accurate enough for a "≈N ops planned" note, and never used for
    correctness.
    """
    if duration <= 0:
        return 0
    steps = max(1, math.ceil(duration / step))
    dt = duration / steps
    return round(sum(profile.rate_at(i * dt) * dt for i in range(steps)))
