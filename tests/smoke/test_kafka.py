"""Smoke: full Kafka round-trip against real Apache Kafka.

Create a throwaway topic, produce 100 keyed messages, consume them back, assert the
count and per-key ordering, then delete the topic. A fresh topic name per run plus
delete-in-finally keeps the check idempotent (green twice in a row, no leftovers).
"""

from __future__ import annotations

import time
import uuid

import pytest
from confluent_kafka import Consumer, KafkaException, Producer
from confluent_kafka.admin import AdminClient, NewTopic

from cdc_pipeline.config import Settings

pytestmark = pytest.mark.smoke

MESSAGE_COUNT = 100
PARTITIONS = 4
KEYS = ["A", "B", "C", "D"]  # len must divide evenly into partitions for clean routing


def _admin(settings: Settings) -> AdminClient:
    return AdminClient({"bootstrap.servers": settings.kafka_bootstrap_servers})


def _create_topic(admin: AdminClient, topic: str) -> None:
    futures = admin.create_topics(
        [NewTopic(topic, num_partitions=PARTITIONS, replication_factor=1)]
    )
    futures[topic].result(timeout=30)


def _delete_topic(admin: AdminClient, topic: str) -> None:
    futures = admin.delete_topics([topic], operation_timeout=30)
    try:
        futures[topic].result(timeout=30)
    except Exception:  # noqa: BLE001 - best-effort cleanup; nothing to do if it's already gone
        pass


def test_produce_consume_roundtrip(settings: Settings) -> None:
    bootstrap = settings.kafka_bootstrap_servers
    topic = f"cdc-smoke-{uuid.uuid4().hex[:8]}"
    admin = _admin(settings)

    try:
        _create_topic(admin, topic)
    except KafkaException as exc:
        pytest.fail(
            f"cannot create topic on Kafka at {bootstrap!r}: {exc}. "
            f"Is the kafka service up? Check `make ps`."
        )

    try:
        producer = Producer({"bootstrap.servers": bootstrap})
        for i in range(MESSAGE_COUNT):
            producer.produce(topic, key=KEYS[i % len(KEYS)], value=str(i))
        remaining = producer.flush(30)
        assert remaining == 0, f"{remaining} messages were not delivered within 30s"

        consumer = Consumer(
            {
                "bootstrap.servers": bootstrap,
                "group.id": f"cdc-smoke-{uuid.uuid4().hex[:8]}",
                "auto.offset.reset": "earliest",
                "enable.auto.commit": False,
            }
        )
        consumer.subscribe([topic])
        received: list[tuple[str, int]] = []
        deadline = time.monotonic() + 30
        while len(received) < MESSAGE_COUNT and time.monotonic() < deadline:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                raise KafkaException(msg.error())
            received.append((msg.key().decode(), int(msg.value().decode())))
        consumer.close()

        assert len(received) == MESSAGE_COUNT, (
            f"consumed {len(received)} of {MESSAGE_COUNT} messages back from {topic!r}"
        )
        # Each key hashes to a single partition, so its messages must arrive in the
        # order they were produced (0, 4, 8, ... for key A) — strictly increasing.
        for key in KEYS:
            values = [v for k, v in received if k == key]
            assert values == sorted(values), (
                f"messages for key {key!r} arrived out of per-key order: {values}"
            )
    finally:
        _delete_topic(admin, topic)
