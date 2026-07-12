"""Unit tests for the workload planner — especially the reproducibility contract that makes
seeded runs repeatable (the whole point of ``--seed``)."""

from __future__ import annotations

from cdc_pipeline.tools.load.model import OpKind
from cdc_pipeline.tools.load.planner import STATUS_WALK, Bootstrap, WorkloadPlanner, _successor
from cdc_pipeline.tools.load.scenarios import DEFAULT_MIX, get_scenario


def _bootstrap() -> Bootstrap:
    ids = list(range(1, 201))
    status = {i: ("registered" if i <= 50 else "completed") for i in ids}
    return Bootstrap(
        specimen_ids=ids,
        specimen_status=status,
        registered_ids=list(range(1, 51)),
        run_ids=list(range(1, 11)),
    )


def _make_planner(seed: int = 0, scenario_name: str = "steady") -> WorkloadPlanner:
    scenario = get_scenario(scenario_name)
    return WorkloadPlanner.build(scenario, scenario.mix, _bootstrap(), seed, insert_prefix="T")


# ── reproducibility ─────────────────────────────────────────────────────────────


def test_same_seed_same_operation_stream():
    a = _make_planner(seed=42)
    b = _make_planner(seed=42)
    ops_a = [a.next() for _ in range(500)]
    ops_b = [b.next() for _ in range(500)]
    assert ops_a == ops_b


def test_different_seed_differs():
    a = [op for _ in range(200) if (op := _make_planner(seed=1).next())]
    b = [op for _ in range(200) if (op := _make_planner(seed=2).next())]
    assert a != b


def test_draw_stream_is_independent_of_execution_feedback():
    """The core contract: neither insert-id feedback nor skips change how much RNG is
    consumed, so the draw stream (hence the whole workload shape) is a pure function of the
    seed. We prove it by showing the RNG lands in the same state whether or not a shard is
    fed inserted ids as it runs."""
    plain = _make_planner(seed=7)
    fed = _make_planner(seed=7)
    fake_id = 10_000
    for _ in range(600):
        plain.next()
        op = fed.next()
        if op is not None and op.kind is OpKind.SPECIMEN_INSERT:
            fed.on_inserted(fake_id)  # runtime feedback the plain planner never gets
            fake_id += 1
    assert plain.rng.getstate() == fed.rng.getstate()


# ── operation semantics ─────────────────────────────────────────────────────────


def test_successor_walks_then_stops():
    assert _successor("registered") == "in_prep"
    assert _successor("in_prep") == "on_instrument"
    assert _successor("on_instrument") == "completed"
    assert _successor("completed") is None  # terminal — updates become benign field edits
    assert _successor("disposed") is None
    assert STATUS_WALK[0] == "registered"


def test_hard_delete_skips_when_no_registered_then_uses_inserted():
    planner = WorkloadPlanner.build(
        get_scenario("steady"),
        DEFAULT_MIX,
        Bootstrap(specimen_ids=[1, 2, 3], specimen_status={}, registered_ids=[], run_ids=[1]),
        seed=0,
        insert_prefix="T",
    )
    assert planner._hard_delete() is None  # nothing registered yet → skip
    planner.on_inserted(555)
    op = planner._hard_delete()
    assert op is not None and op.method == "DELETE" and op.path == "/specimens/555"


def test_dispose_marks_status_and_consumes_pool():
    planner = _make_planner(seed=0)
    op = planner._dispose()
    assert op is not None and op.path.endswith("/dispose")
    disposed_id = int(op.path.split("/")[2])
    assert planner._status[disposed_id] == "disposed"


def test_insert_has_unique_incrementing_barcodes():
    planner = _make_planner(seed=0)
    barcodes = [planner._insert().json["barcode"] for _ in range(5)]
    assert barcodes == [f"T-{n:07d}" for n in range(1, 6)]
    assert len(set(barcodes)) == 5


def test_update_advances_lifecycle_status():
    planner = _make_planner(seed=0)
    # Force a known registered key into the walk by driving updates until one advances it.
    seen_status_change = False
    for _ in range(300):
        op = planner._update()
        if op.json.get("status") in ("in_prep", "on_instrument", "completed"):
            seen_status_change = True
            break
    assert seen_status_change


def test_reassignment_storm_emits_run_updates():
    planner = _make_planner(seed=0, scenario_name="reassignment-storm")
    kinds = {op.kind for _ in range(300) if (op := planner.next())}
    assert OpKind.RUN_UPDATE in kinds
