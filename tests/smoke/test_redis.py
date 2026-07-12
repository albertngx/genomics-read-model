"""Smoke: Redis SET/GET/DEL round-trip. This is the check the story asks to fail
readably when Redis is stopped."""

from __future__ import annotations

import uuid

import pytest
import redis

from cdc_pipeline.config import Settings

pytestmark = pytest.mark.smoke


def test_set_get_del_roundtrip(settings: Settings) -> None:
    client = redis.Redis.from_url(settings.redis_url, socket_connect_timeout=5)

    try:
        client.ping()
    except redis.exceptions.RedisError as exc:
        pytest.fail(
            f"cannot reach Redis at {settings.redis_url!r}: {exc}. "
            f"Is the redis service up? Check `make ps`."
        )

    key = f"cdc:smoke:{uuid.uuid4().hex}"
    try:
        client.set(key, "ok")
        assert client.get(key) == b"ok", "Redis GET did not return the value just SET"
        assert client.delete(key) == 1, "Redis DEL did not remove the key"
        assert client.get(key) is None, "key still present after DEL"
    finally:
        client.delete(key)  # belt-and-suspenders: never leave a smoke key behind
        client.close()
