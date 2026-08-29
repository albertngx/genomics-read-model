"""Unit tests for the topic spec and the create/reconfigure plan (epic 03.3).

No infrastructure: these pin the committed ``deploy/topics/topics.json`` to
ARCHITECTURE.md §4 and decision D3, and exercise the planner that decides what
``make topics`` will do — including its refusal to change a partition count in place,
which is the one operation that would silently break per-key ordering.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from cdc_pipeline.tools.topics import (
    _SPEC_PATH,
    TopicError,
    TopicSpec,
    load_spec,
    plan,
)


def _write(tmp_path: Path, payload: object) -> Path:
    path = tmp_path / "topics.json"
    path.write_text(json.dumps(payload))
    return path


# ── The committed spec IS the architecture ──────────────────────────────────────


def test_committed_spec_matches_architecture_section_4() -> None:
    """§4's table, asserted: specimens 6/compact, runs 3/compact."""
    by_name = {s.name: s for s in load_spec(_SPEC_PATH)}
    assert set(by_name) == {"cdc.public.specimens", "cdc.public.runs"}

    specimens = by_name["cdc.public.specimens"]
    assert specimens.partitions == 6, "§4 fixes specimens at 6 partitions"
    runs = by_name["cdc.public.runs"]
    assert runs.partitions == 3, "§4 fixes runs at 3 partitions"

    for spec in (specimens, runs):
        # D3: latest-per-key forever. `delete` would age the snapshot out of the log and
        # make an offset-0 rebuild impossible — the failure epic 03's field notes recorded.
        assert spec.config["cleanup.policy"] == "compact", f"{spec.name} must be compacted"
        # A compacted topic must never also carry a time-based retention; that would
        # reintroduce exactly the aging-out the policy exists to prevent.
        assert "retention.ms" not in spec.config, (
            f"{spec.name} must not set retention.ms alongside cleanup.policy=compact"
        )


def test_committed_spec_is_tuned_to_be_observable() -> None:
    """The lab tuning (story 03.3) — compaction that never runs teaches nothing."""
    for spec in load_spec(_SPEC_PATH):
        assert int(spec.config["segment.ms"]) <= 60_000, "segments must roll within a session"
        assert float(spec.config["min.cleanable.dirty.ratio"]) <= 0.1
        # Tombstones must outlive a consumer's poll but die within a demo.
        assert 0 < int(spec.config["delete.retention.ms"]) <= 300_000


# ── Loading and validation ──────────────────────────────────────────────────────


def test_load_spec_normalises_config_values_to_strings(tmp_path: Path) -> None:
    # Kafka's admin protocol is all strings; JSON numbers must not reach it as ints.
    path = _write(
        tmp_path, {"topics": [{"name": "t", "partitions": 2, "config": {"segment.ms": 60000}}]}
    )
    (spec,) = load_spec(path)
    assert spec.config == {"segment.ms": "60000"}
    assert spec.replication_factor == 1, "replication_factor defaults to 1"


@pytest.mark.parametrize(
    ("payload", "match"),
    [
        ({}, "non-empty 'topics'"),
        ({"topics": []}, "non-empty 'topics'"),
        ({"topics": [{"partitions": 1}]}, "has no 'name'"),
        ({"topics": [{"name": "t", "partitions": 0}]}, "must be an integer >= 1"),
        ({"topics": [{"name": "t", "partitions": "6"}]}, "must be an integer >= 1"),
        ({"topics": [{"name": "t", "partitions": 1, "config": []}]}, "must be an object"),
        (
            {"topics": [{"name": "t", "partitions": 1}, {"name": "t", "partitions": 2}]},
            "declared twice",
        ),
    ],
)
def test_load_spec_rejects_malformed_specs(tmp_path: Path, payload: object, match: str) -> None:
    # A bad spec must fail before it half-applies itself against a live broker.
    with pytest.raises(TopicError, match=match):
        load_spec(_write(tmp_path, payload))


def test_load_spec_reports_a_missing_file(tmp_path: Path) -> None:
    with pytest.raises(TopicError, match="not found"):
        load_spec(tmp_path / "absent.json")


# ── The plan ────────────────────────────────────────────────────────────────────

_SPEC = TopicSpec("cdc.public.specimens", 6, 1, {"cleanup.policy": "compact"})


def test_plan_creates_a_topic_that_does_not_exist() -> None:
    (action,) = plan([_SPEC], {})
    assert action.kind == "create"
    assert "6 partitions" in action.detail


def test_plan_reconfigures_a_topic_with_the_right_partition_count() -> None:
    # Config is safe to converge in place — it moves no data.
    (action,) = plan([_SPEC], {"cdc.public.specimens": 6})
    assert action.kind == "reconfigure"


def test_plan_refuses_to_change_a_partition_count() -> None:
    """The story's core hazard: growing partitions re-hashes the key space.

    Existing records for a key stay in their old partition while new ones may route
    elsewhere, so the key ends up with two "latest" values compaction can never merge and
    per-key ordering is silently gone. The planner must surface this, never fix it.
    """
    (action,) = plan([_SPEC], {"cdc.public.specimens": 1})
    assert action.kind == "partition-conflict"
    assert "1 partitions" in action.detail and "6" in action.detail


def test_plan_is_per_topic() -> None:
    runs = TopicSpec("cdc.public.runs", 3, 1, {})
    actions = plan([_SPEC, runs], {"cdc.public.runs": 3})
    assert [a.kind for a in actions] == ["create", "reconfigure"]
