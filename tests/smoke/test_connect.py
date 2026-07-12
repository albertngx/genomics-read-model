"""Smoke: Kafka Connect is up and the Debezium Postgres plugin is installed.

We do NOT register a connector here (that's epic 03) — we only assert the plugin the
CDC pipeline will depend on is present. Uses stdlib urllib so no extra HTTP dependency.
"""

from __future__ import annotations

import json
import urllib.error
import urllib.request

import pytest

from cdc_pipeline.config import Settings

pytestmark = pytest.mark.smoke


def test_debezium_postgres_plugin_installed(settings: Settings) -> None:
    url = settings.kafka_connect_url.rstrip("/") + "/connector-plugins"
    try:
        with urllib.request.urlopen(url, timeout=15) as resp:  # noqa: S310 - fixed local URL
            plugins = json.load(resp)
    except urllib.error.URLError as exc:
        pytest.fail(
            f"cannot reach Kafka Connect at {url!r}: {exc}. "
            f"Is the connect service up? Check `make ps`."
        )

    plugin_classes = [p.get("class", "") for p in plugins]
    assert any("PostgresConnector" in cls for cls in plugin_classes), (
        f"Debezium PostgresConnector not found among Connect plugins at {url}. "
        f"Installed plugins: {plugin_classes}"
    )
