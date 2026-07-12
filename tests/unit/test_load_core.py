"""Unit tests for the load generator's pure building blocks — no network, no stack.

Covers the percentile, the Zipf sampler's determinism and skew, op-mix parsing/selection,
and the rate profiles. The planner's reproducibility contract is tested in
``test_load_planner.py``.
"""

from __future__ import annotations

import random

import pytest

from cdc_pipeline.tools.load.client import resolve_api_url
from cdc_pipeline.tools.load.keys import ZipfSampler
from cdc_pipeline.tools.load.model import OpKind
from cdc_pipeline.tools.load.rates import BurstRate, RampRate, SteadyRate, total_expected
from cdc_pipeline.tools.load.scenarios import DEFAULT_MIX, SCENARIOS, parse_mix
from cdc_pipeline.tools.load.stats import percentile


# ── percentile ────────────────────────────────────────────────────────────────


def test_percentile_empty_is_zero():
    assert percentile([], 50) == 0.0


def test_percentile_nearest_rank():
    values = [float(n) for n in range(1, 101)]  # 1..100
    assert percentile(values, 50) == 50.0
    assert percentile(values, 99) == 99.0
    assert percentile(values, 100) == 100.0
    assert percentile(values, 0) == 1.0


def test_percentile_unsorted_input():
    assert percentile([5.0, 1.0, 3.0, 2.0, 4.0], 100) == 5.0


# ── Zipf sampler ────────────────────────────────────────────────────────────────


def test_zipf_is_deterministic_for_a_seed():
    keys = list(range(1, 51))
    a = ZipfSampler(keys, 1.1)
    b = ZipfSampler(keys, 1.1)
    rng_a, rng_b = random.Random(7), random.Random(7)
    draws_a = [a.sample(rng_a) for _ in range(200)]
    draws_b = [b.sample(rng_b) for _ in range(200)]
    assert draws_a == draws_b


def test_zipf_consumes_exactly_one_draw():
    sampler = ZipfSampler(list(range(1, 20)), 1.0)
    rng = random.Random(1)
    probe = random.Random(1)
    sampler.sample(rng)
    probe.random()  # one draw
    assert rng.getstate() == probe.getstate()


def test_zipf_is_skewed_not_uniform():
    keys = list(range(1000))
    sampler = ZipfSampler(keys, 1.3, shuffle_rng=random.Random(0))
    rng = random.Random(0)
    counts: dict[int, int] = {}
    for _ in range(20_000):
        k = sampler.sample(rng)
        counts[k] = counts.get(k, 0) + 1
    ranked = sorted(counts.values(), reverse=True)
    top10 = sum(ranked[:10])
    # A pronounced hot set: the 10 hottest keys (of 1000) take a large share of traffic.
    assert top10 / 20_000 > 0.25


def test_zipf_empty_universe_raises():
    with pytest.raises(IndexError):
        ZipfSampler([], 1.0).sample(random.Random(0))


# ── op mix ──────────────────────────────────────────────────────────────────────


def test_parse_mix_roundtrip():
    mix = parse_mix("update=70,insert=15,dispose=10,delete=5")
    pct = mix.as_percentages()
    assert pct[OpKind.SPECIMEN_UPDATE] == pytest.approx(70.0)
    assert pct[OpKind.SPECIMEN_HARD_DELETE] == pytest.approx(5.0)


def test_parse_mix_rejects_unknown_kind():
    with pytest.raises(ValueError, match="unknown op kind"):
        parse_mix("frobnicate=100")


def test_parse_mix_rejects_bad_weight():
    with pytest.raises(ValueError, match="not a number"):
        parse_mix("update=lots")


def test_mix_choose_matches_weights():
    rng = random.Random(0)
    counts = {k: 0 for k in DEFAULT_MIX.kinds}
    for _ in range(20_000):
        counts[DEFAULT_MIX.choose(rng)] += 1
    # ~70% updates, ~5% hard-deletes — loose bounds so the test isn't flaky.
    assert 0.65 < counts[OpKind.SPECIMEN_UPDATE] / 20_000 < 0.75
    assert 0.03 < counts[OpKind.SPECIMEN_HARD_DELETE] / 20_000 < 0.07


def test_all_scenarios_have_valid_mixes():
    for scenario in SCENARIOS.values():
        assert scenario.mix.kinds  # at least one positive-weight kind


# ── rate profiles ─────────────────────────────────────────────────────────────


def test_steady_rate_is_constant():
    r = SteadyRate(500)
    assert r.rate_at(0) == 500
    assert r.rate_at(123) == 500


def test_ramp_rate_interpolates():
    r = RampRate(100, 1100, duration=10)
    assert r.rate_at(0) == pytest.approx(100)
    assert r.rate_at(5) == pytest.approx(600)
    assert r.rate_at(10) == pytest.approx(1100)
    assert r.rate_at(999) == pytest.approx(1100)  # clamped past the end


def test_burst_rate_spikes_then_drops():
    r = BurstRate(base_rps=100, multiplier=10, period=20, spike_fraction=0.25)
    assert r.rate_at(0) == 1000  # in the spike
    assert r.rate_at(1) == 1000
    assert r.rate_at(10) == 100  # base
    assert r.rate_at(20) == 1000  # next period's spike


def test_total_expected_matches_steady():
    assert total_expected(SteadyRate(100), duration=10) == pytest.approx(1000, rel=0.05)


# ── API url resolution ───────────────────────────────────────────────────────


def test_resolve_api_url_explicit_wins(monkeypatch):
    monkeypatch.setenv("API_URL", "http://ignored:9999")
    assert resolve_api_url("http://explicit:1234/") == "http://explicit:1234"


def test_resolve_api_url_from_api_url_env(monkeypatch):
    monkeypatch.setenv("API_URL", "http://from-env:8000")
    assert resolve_api_url(None) == "http://from-env:8000"
