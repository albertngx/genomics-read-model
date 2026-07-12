"""Zipfian key selection.

Uniform key selection hides everything the later epics want to see: with every specimen
equally likely, per-key ordering never matters and a cache is either all-hit or all-miss.
Real lab traffic is skewed — a handful of active runs and their specimens take most of the
writes — so updates here draw keys Zipfian: key at rank *r* (1-indexed) is chosen with
probability ∝ 1/r^alpha. ``alpha≈1.1`` gives a pronounced but not degenerate hot set.

The universe is fixed at construction (bootstrapped from the API), so the cumulative-weight
table is built **once** and each draw is an O(log n) bisect — no rebuilds on the hot path.
The key order is shuffled with the seeded RNG at build time so the hot set is a
pseudo-random subset rather than "the lowest ids", which keeps hot specimens spread across
runs (interesting for the reassignment-storm scenario).
"""

from __future__ import annotations

import random
from bisect import bisect_left
from itertools import accumulate


class ZipfSampler:
    """Samples from a fixed key universe with a Zipf(alpha) rank distribution."""

    def __init__(self, keys: list[int], alpha: float, *, shuffle_rng: random.Random | None = None):
        if alpha < 0:
            raise ValueError(f"alpha must be >= 0, got {alpha}")
        self._keys = list(keys)
        if shuffle_rng is not None:
            shuffle_rng.shuffle(self._keys)
        # weight of rank r (1-indexed) is 1/r^alpha; cumulative for bisect sampling.
        weights = [1.0 / (r**alpha) for r in range(1, len(self._keys) + 1)]
        self._cumulative = list(accumulate(weights))
        self._total = self._cumulative[-1] if self._cumulative else 0.0

    def __len__(self) -> int:
        return len(self._keys)

    def sample(self, rng: random.Random) -> int:
        """Draw one key. Consumes exactly one ``rng.random()`` — the single-draw contract
        the planner relies on for a reproducible RNG stream."""
        if not self._keys:
            raise IndexError("cannot sample from an empty key universe")
        target = rng.random() * self._total
        idx = bisect_left(self._cumulative, target)
        if idx >= len(self._keys):  # guard the exact-1.0 boundary
            idx = len(self._keys) - 1
        return self._keys[idx]
