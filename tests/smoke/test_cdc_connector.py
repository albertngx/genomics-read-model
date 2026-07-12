"""Smoke: the full CDC handshake works end-to-end (epic 03.1).

Runs against the *live* stack after ``make up seed`` (the connector subscribes to the
``cdc_pub`` publication the migration owns and snapshots the seeded rows). Self-contained:
it registers the connector idempotently, then asserts the three acceptance criteria —

  1. Initial snapshot: every seeded specimen appears on ``cdc.public.specimens`` as ``op:"r"``.
  2. Streaming: a fresh INSERT produces a correctly keyed ``op:"c"`` event within ~1s.
  3. The replication slot ``cdc_genomics`` is active and its confirmed_flush_lsn is set.

Idempotent: re-running registers over the same connector, reads the same snapshot from the
topic, and cleans up the specimen it inserts.
"""

from __future__ import annotations

import json
import time
import uuid

import httpx
import psycopg
import pytest
from confluent_kafka import OFFSET_BEGINNING, OFFSET_END, Consumer, TopicPartition

from cdc_pipeline.config import Settings
from cdc_pipeline.db.seed import N_SPECIMENS
from cdc_pipeline.tools.connector import CONNECTOR_NAME, render_config

pytestmark = pytest.mark.smoke

SPECIMENS_TOPIC = "cdc.public.specimens"
_STREAM_DEADLINE_S = 15.0  # generous ceiling; the event itself lands in ~1s


def _require_seeded(database_url: str) -> None:
    """Fail readably if the schema/publication/seed aren't in place yet."""
    try:
        with psycopg.connect(database_url, connect_timeout=10) as conn:
            pub = conn.execute("SELECT 1 FROM pg_publication WHERE pubname = 'cdc_pub'").fetchone()
            count = conn.execute("SELECT count(*) FROM specimens").fetchone()
    except psycopg.OperationalError as exc:
        pytest.fail(f"cannot connect to Postgres at {database_url!r}: {exc}. Run `make up`.")
    if pub is None:
        pytest.fail("publication 'cdc_pub' missing — run `make seed` (migration 0002 owns it).")
    assert count is not None
    if count[0] < N_SPECIMENS:
        pytest.fail(f"only {count[0]} specimens present (<{N_SPECIMENS}); run `make seed`.")


def _register_and_wait_running(settings: Settings) -> None:
    """Idempotent PUT, then poll the status endpoint until connector + task are RUNNING."""
    base = settings.kafka_connect_url.rstrip("/")
    try:
        resp = httpx.put(
            f"{base}/connectors/{CONNECTOR_NAME}/config", json=render_config(), timeout=30.0
        )
        resp.raise_for_status()
    except httpx.HTTPStatusError as exc:
        pytest.fail(
            f"Connect rejected the config (HTTP {exc.response.status_code}): {exc.response.text}"
        )
    except httpx.HTTPError as exc:
        pytest.fail(f"cannot reach Kafka Connect at {base!r}: {exc}. Is `connect` up? `make ps`.")

    deadline = time.time() + 60.0
    last = "(no status yet)"
    while time.time() < deadline:
        status = httpx.get(f"{base}/connectors/{CONNECTOR_NAME}/status", timeout=15.0).json()
        conn_state = status.get("connector", {}).get("state")
        tasks = [t.get("state") for t in status.get("tasks", [])]
        last = f"connector={conn_state} tasks={tasks}"
        if conn_state == "RUNNING" and tasks and all(t == "RUNNING" for t in tasks):
            return
        if conn_state == "FAILED" or "FAILED" in tasks:
            pytest.fail(f"connector entered FAILED state: {json.dumps(status, indent=2)}")
        time.sleep(1.0)
    pytest.fail(f"connector did not reach RUNNING within 60s — last status: {last}")


def _consumer(settings: Settings) -> Consumer:
    return Consumer(
        {
            "bootstrap.servers": settings.kafka_bootstrap_servers,
            "group.id": f"cdc-smoke-{uuid.uuid4().hex}",  # fresh group → no committed offsets
            "enable.auto.commit": False,
            "auto.offset.reset": "earliest",
        }
    )


def _partitions(consumer: Consumer, topic: str) -> list[int]:
    md = consumer.list_topics(topic, timeout=15)
    tmd = md.topics.get(topic)
    if tmd is None or tmd.error is not None:
        pytest.fail(
            f"topic {topic!r} not found — did the snapshot start? Check `make connector-status`."
        )
    return list(tmd.partitions.keys())


def test_snapshot_emits_read_events_for_every_seeded_specimen(settings: Settings) -> None:
    _require_seeded(settings.database_url)
    _register_and_wait_running(settings)

    consumer = _consumer(settings)
    try:
        parts = _partitions(consumer, SPECIMENS_TOPIC)
        consumer.assign([TopicPartition(SPECIMENS_TOPIC, p, OFFSET_BEGINNING) for p in parts])

        op_r = 0
        idle_deadline = time.time() + 10.0  # extend on every message; snapshot streams fast
        hard_deadline = time.time() + 120.0
        while time.time() < min(idle_deadline, hard_deadline):
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                continue
            idle_deadline = time.time() + 10.0
            if msg.value() is None:  # tombstone
                continue
            payload = json.loads(msg.value())
            if payload.get("op") == "r":
                op_r += 1
                assert msg.key() is not None, (
                    "snapshot event has no key — events must be keyed by PK"
                )
    finally:
        consumer.close()

    # Snapshot is one-time per slot: op:"r" count == specimens present when it ran. On a
    # fresh `make up seed connector` that is exactly the seed; never fewer.
    assert op_r >= N_SPECIMENS, (
        f"snapshot produced {op_r} op:'r' events on {SPECIMENS_TOPIC}, expected >= {N_SPECIMENS}. "
        f"Was the connector registered before the seed finished?"
    )


def test_insert_streams_a_correctly_keyed_create_event(settings: Settings) -> None:
    _require_seeded(settings.database_url)
    _register_and_wait_running(settings)

    barcode = f"SMOKE-{uuid.uuid4().hex[:12]}"
    consumer = _consumer(settings)
    specimen_id: int | None = None
    try:
        # Seek to the END first, so we only see events produced *after* our insert.
        parts = _partitions(consumer, SPECIMENS_TOPIC)
        consumer.assign([TopicPartition(SPECIMENS_TOPIC, p, OFFSET_END) for p in parts])
        consumer.poll(0)  # force the assignment/seek to take effect

        with psycopg.connect(settings.database_url, connect_timeout=10, autocommit=True) as conn:
            row = conn.execute(
                "INSERT INTO specimens (barcode, label, specimen_type, assay, status) "
                "VALUES (%s, %s, 'blood', 'wgs', 'registered') RETURNING id",
                (barcode, f"smoke insert {barcode}"),
            ).fetchone()
            assert row is not None
            specimen_id = row[0]

        found = None
        deadline = time.time() + _STREAM_DEADLINE_S
        while time.time() < deadline and found is None:
            msg = consumer.poll(1.0)
            if msg is None or msg.error() or msg.value() is None:
                continue
            payload = json.loads(msg.value())
            after = payload.get("after") or {}
            if payload.get("op") == "c" and after.get("barcode") == barcode:
                found = msg
        assert found is not None, (
            f"no op:'c' event for {barcode!r} within {_STREAM_DEADLINE_S:.0f}s — "
            f"streaming stalled? Check `make connector-status` and `make slot-status`."
        )
        # Correctly keyed: the event key is the primary key.
        key = json.loads(found.key())
        assert key == {"id": specimen_id}, f"event key {key} is not the specimen PK {specimen_id}"
    finally:
        consumer.close()
        if specimen_id is not None:
            with psycopg.connect(
                settings.database_url, connect_timeout=10, autocommit=True
            ) as conn:
                conn.execute("DELETE FROM specimens WHERE id = %s", (specimen_id,))


def test_replication_slot_is_active_and_advancing(settings: Settings) -> None:
    _require_seeded(settings.database_url)
    _register_and_wait_running(settings)

    with psycopg.connect(settings.database_url, connect_timeout=10) as conn:
        row = conn.execute(
            "SELECT active, confirmed_flush_lsn FROM pg_replication_slots WHERE slot_name = %s",
            ("cdc_genomics",),
        ).fetchone()

    assert row is not None, (
        "replication slot 'cdc_genomics' does not exist — is the connector running?"
    )
    active, confirmed_flush_lsn = row
    assert active is True, "slot 'cdc_genomics' is not active — no WAL sender is attached."
    assert confirmed_flush_lsn is not None, (
        "slot has no confirmed_flush_lsn — nothing acknowledged yet."
    )
