"""Smoke: Postgres is reachable and configured for logical replication (the one
non-negotiable CDC setting — ARCHITECTURE D4)."""

from __future__ import annotations

import psycopg
import pytest

from cdc_pipeline.config import Settings

pytestmark = pytest.mark.smoke


def test_wal_level_is_logical(settings: Settings) -> None:
    try:
        with psycopg.connect(settings.database_url, connect_timeout=10) as conn:
            row = conn.execute("SHOW wal_level").fetchone()
    except psycopg.OperationalError as exc:
        pytest.fail(
            f"cannot connect to Postgres at {settings.database_url!r}: {exc}. "
            f"Is the stack up? Run `make up`."
        )

    assert row is not None, "SHOW wal_level returned no row"
    assert row[0] == "logical", (
        f"Postgres wal_level is {row[0]!r}, expected 'logical'. CDC will fail without it — "
        f"check the postgres `command:` flags in deploy/docker-compose.yml."
    )
