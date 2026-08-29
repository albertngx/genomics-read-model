"""Smoke: the CDC topics are shaped as designed, and per-key ordering actually holds (03.3).

Runs against the *live* stack after ``make up seed connector`` (which pulls in ``make
topics``). Three claims from ARCHITECTURE.md §4, each asserted rather than eyeballed:

  1. Both topics match ``deploy/topics/topics.json`` — 6/3 partitions, ``cleanup.policy=compact``.
     A topic Kafka auto-created would have 1 partition and a 7-day delete policy, and nothing
     downstream would complain until the snapshot aged out of the log.
  2. Every event for one row lands in **exactly one partition**. That is the whole reason
     per-row ordering exists: the partition is Kafka's unit of ordering, and the key is the
     primary key.
  3. Within that partition, a row's changes arrive in the order they were committed — proven
     twice over, by a monotonic counter written into the row *and* by ``source.lsn``, the
     token every consumer's idempotency guard is built on (§5).

Claim 3 is the story's ordering demo, kept as a permanent regression test: it writes a
skewed, concurrent workload (one hot key plus background writers — the shape the load
generator produces) so ordering is asserted under contention, not in a quiet lab.

Idempotent: every row it creates is deleted in teardown, and it only ever reads the slice of
the log its own writes produced.
"""

from __future__ import annotations

import json
import time
import uuid
from collections.abc import Iterator
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from decimal import Decimal

import psycopg
import pytest
from confluent_kafka import Consumer, TopicPartition
from confluent_kafka.admin import AdminClient

from cdc_pipeline.config import Settings
from cdc_pipeline.tools.topics import (
    describe_topics,
    live_partition_counts,
    load_spec,
)

pytestmark = pytest.mark.smoke

SPECIMENS_TOPIC = "cdc.public.specimens"

HOT_UPDATES = 150  # the hot key: enough writes that any reordering would show
BACKGROUND_KEYS = 6  # concurrent writers on other keys — contention, like real traffic
BACKGROUND_UPDATES = 40
_DELIVERY_DEADLINE_S = 90.0


# ── Capturing one slice of the log ──────────────────────────────────────────────


@dataclass(frozen=True)
class Event:
    """One consumed change event, reduced to what an ordering claim needs."""

    partition: int
    offset: int
    lsn: int
    counter: int | None  # the monotonic value we wrote into volume_ul


@dataclass(frozen=True)
class Capture:
    hot_id: int
    events: dict[int, list[Event]]  # specimen id -> events, in the order they were read


def _consumer(settings: Settings) -> Consumer:
    return Consumer(
        {
            "bootstrap.servers": settings.kafka_bootstrap_servers,
            "group.id": f"cdc-topics-smoke-{uuid.uuid4().hex}",  # fresh group → no offsets
            "enable.auto.commit": False,
            "auto.offset.reset": "earliest",
        }
    )


def _insert(conn: psycopg.Connection, barcode: str) -> int:
    row = conn.execute(
        "INSERT INTO specimens (barcode, label, specimen_type, assay, status, volume_ul) "
        "VALUES (%s, %s, 'blood', 'wgs', 'registered', 0) RETURNING id",
        (barcode, f"topic design smoke {barcode}"),
    ).fetchone()
    assert row is not None
    return int(row[0])


def _drive(database_url: str, specimen_id: int, updates: int) -> None:
    """Write ``updates`` strictly increasing counters into one row, one txn each."""
    with psycopg.connect(database_url, connect_timeout=10, autocommit=True) as conn:
        for i in range(1, updates + 1):
            conn.execute(
                "UPDATE specimens SET volume_ul = %s, updated_at = now() WHERE id = %s",
                (i, specimen_id),
            )


@pytest.fixture(scope="module")
def capture(settings: Settings) -> Iterator[Capture]:
    """Drive a skewed concurrent workload, then read back exactly the log it produced."""
    tag = uuid.uuid4().hex[:8]
    ids: list[int] = []
    with psycopg.connect(settings.database_url, connect_timeout=10, autocommit=True) as conn:
        _require_publication(conn)
        # Start offsets FIRST, so the window we read is bounded by our own writes and the
        # test never depends on how much history the topic happens to hold.
        starts = _end_offsets(settings)
        hot_id = _insert(conn, f"ORDER-HOT-{tag}")
        ids.append(hot_id)
        for i in range(BACKGROUND_KEYS):
            ids.append(_insert(conn, f"ORDER-BG-{tag}-{i}"))

    # Concurrency is the point: the hot key is written while other keys are, so the
    # per-key ordering claim is tested against contention rather than a quiet topic.
    with ThreadPoolExecutor(max_workers=BACKGROUND_KEYS + 1) as pool:
        futures = [pool.submit(_drive, settings.database_url, hot_id, HOT_UPDATES)]
        futures += [
            pool.submit(_drive, settings.database_url, sid, BACKGROUND_UPDATES) for sid in ids[1:]
        ]
        for fut in futures:
            fut.result()

    try:
        yield _read_window(settings, starts, hot_id, expected_hot=HOT_UPDATES + 1)
    finally:
        with psycopg.connect(settings.database_url, connect_timeout=10, autocommit=True) as conn:
            conn.execute("DELETE FROM specimens WHERE id = ANY(%s)", (ids,))


def _require_publication(conn: psycopg.Connection) -> None:
    if conn.execute("SELECT 1 FROM pg_publication WHERE pubname = 'cdc_pub'").fetchone() is None:
        pytest.fail("publication 'cdc_pub' missing — run `make seed` (migration 0002 owns it).")


def _end_offsets(settings: Settings) -> dict[int, int]:
    consumer = _consumer(settings)
    try:
        md = consumer.list_topics(SPECIMENS_TOPIC, timeout=15)
        tmd = md.topics.get(SPECIMENS_TOPIC)
        if tmd is None or tmd.error is not None:
            pytest.fail(
                f"topic {SPECIMENS_TOPIC!r} does not exist — run `make topics` (auto-create is "
                f"off on purpose, story 03.3) and `make connector`."
            )
        return {
            p: consumer.get_watermark_offsets(
                TopicPartition(SPECIMENS_TOPIC, p), timeout=15, cached=False
            )[1]
            for p in tmd.partitions
        }
    finally:
        consumer.close()


def _read_window(
    settings: Settings, starts: dict[int, int], hot_id: int, *, expected_hot: int
) -> Capture:
    """Consume from the recorded offsets until the hot key's writes have all arrived."""
    consumer = _consumer(settings)
    events: dict[int, list[Event]] = {}
    try:
        consumer.assign([TopicPartition(SPECIMENS_TOPIC, p, o) for p, o in starts.items()])
        deadline = time.time() + _DELIVERY_DEADLINE_S
        while time.time() < deadline:
            msg = consumer.poll(1.0)
            if msg is None:
                if len(events.get(hot_id, ())) >= expected_hot:
                    break
                continue
            if msg.error() or msg.value() is None:  # tombstones carry no payload
                continue
            key = json.loads(msg.key())["id"]
            payload = json.loads(msg.value())
            after = payload.get("after") or {}
            volume = after.get("volume_ul")
            events.setdefault(key, []).append(
                Event(
                    partition=msg.partition(),
                    offset=msg.offset(),
                    lsn=int(payload["source"]["lsn"]),
                    # D6: NUMERIC arrives as a *string* so it isn't a lossy float.
                    counter=int(Decimal(volume)) if volume is not None else None,
                )
            )
    finally:
        consumer.close()

    hot = events.get(hot_id, [])
    if len(hot) < expected_hot:
        pytest.fail(
            f"only {len(hot)} of {expected_hot} events for the hot specimen arrived within "
            f"{_DELIVERY_DEADLINE_S:.0f}s — is the connector streaming? "
            f"Check `make connector-status` and `make slot-status`."
        )
    return Capture(hot_id=hot_id, events=events)


# ── Claim 1: the topics are shaped as designed ──────────────────────────────────


def test_live_topics_match_the_committed_spec(settings: Settings) -> None:
    """§4's table, asserted against the live cluster — the acceptance criterion, automated."""
    admin = AdminClient({"bootstrap.servers": settings.kafka_bootstrap_servers})
    specs = load_spec()
    names = [s.name for s in specs]
    partitions = live_partition_counts(admin, names)
    configs = describe_topics(admin, names)

    for spec in specs:
        assert spec.name in partitions, (
            f"topic {spec.name!r} does not exist. Auto-create is disabled on purpose "
            f"(story 03.3) — run `make topics`."
        )
        assert partitions[spec.name] == spec.partitions, (
            f"{spec.name} has {partitions[spec.name]} partitions, §4 requires "
            f"{spec.partitions}. A wrong count silently re-hashes keys; see `make topics`."
        )
        live = configs[spec.name]
        for key, want in spec.config.items():
            assert live.get(key) == want, (
                f"{spec.name}: {key} is {live.get(key)!r}, spec wants {want!r}. "
                f"Run `make topics` to converge it."
            )


def test_compaction_is_on_so_the_log_never_ages_out(settings: Settings) -> None:
    """D3, stated as its consequence: no time-based retention may apply to a CDC topic.

    A `delete` policy (or a `retention.ms` alongside compaction) would eventually drop the
    only remaining event for a key, and an offset-0 rebuild — the payoff of the whole design,
    epic 06 — would silently produce an incomplete read-model.
    """
    admin = AdminClient({"bootstrap.servers": settings.kafka_bootstrap_servers})
    for name, live in describe_topics(admin, [s.name for s in load_spec()]).items():
        assert "compact" in live["cleanup.policy"], (
            f"{name} has cleanup.policy={live['cleanup.policy']!r}; D3 requires compaction."
        )
        assert "delete" not in live["cleanup.policy"], (
            f"{name} also deletes by time (cleanup.policy={live['cleanup.policy']!r}) — "
            f"the snapshot would age out of the log."
        )


# ── Claims 2 and 3: partitioning and ordering ───────────────────────────────────


def test_every_event_for_one_row_lands_in_one_partition(capture: Capture) -> None:
    """Key = primary key ⇒ one partition per row. This is *why* ordering is guaranteed."""
    for key, events in capture.events.items():
        partitions = {e.partition for e in events}
        assert len(partitions) == 1, (
            f"specimen {key} has events in partitions {sorted(partitions)}. A row's history "
            f"must live in exactly one partition — split history means no ordering and a key "
            f"compaction can never collapse."
        )


def test_a_hot_keys_changes_arrive_in_commit_order(capture: Capture) -> None:
    """The ordering demo: 150 updates to one key, under concurrent load, arrive in order.

    Each update wrote a strictly increasing counter into ``volume_ul``, so this compares the
    consumed order against the *committed* order directly — not merely against itself.
    """
    hot = capture.events[capture.hot_id]
    counters = [e.counter for e in hot if e.counter is not None]
    assert counters == sorted(counters), (
        f"the hot key's updates arrived out of order: {counters[:20]}… "
        f"Per-partition ordering is broken — check the topic's partition count."
    )
    assert counters[-1] == HOT_UPDATES, (
        f"last counter seen is {counters[-1]}, expected {HOT_UPDATES} — events are missing."
    )


def test_source_lsn_is_monotonic_per_key(capture: Capture) -> None:
    """§5's idempotency token, verified for every key in the window, not just the hot one.

    Every consumer's guard (Elasticsearch external versioning, the Redis CAS) assumes a
    key's LSNs only move forward. If that were false, a replayed event could beat a newer
    one and the read-model would silently regress. Note the guard is deliberately *per key*:
    LSNs are not comparable across keys (EVENTS.md §3).
    """
    for key, events in capture.events.items():
        lsns = [e.lsn for e in events]
        assert lsns == sorted(lsns), f"specimen {key}: source.lsn went backwards: {lsns[:20]}…"
        assert len(set(lsns)) == len(lsns), (
            f"specimen {key}: duplicate source.lsn values — the version guard could not "
            f"distinguish two distinct changes."
        )
