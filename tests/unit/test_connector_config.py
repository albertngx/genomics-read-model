"""Unit tests for connector config rendering (epic 03.1).

No infrastructure: these exercise the ``${VAR}`` substitution that ``make connector``
relies on, plus a check that the committed ``genomics.json`` carries the exact CDC
decisions the story pins (D6, tombstones, inspectable JSON, schema-owned publication).
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from cdc_pipeline.tools.connector import (
    _CONFIG_PATH,
    ConnectorError,
    render_config,
)

_ENV = {"POSTGRES_USER": "cdc", "POSTGRES_PASSWORD": "s3cret", "POSTGRES_DB": "genomics"}


def test_render_substitutes_credentials_from_env(tmp_path: Path) -> None:
    cfg = tmp_path / "c.json"
    cfg.write_text(
        json.dumps(
            {
                "database.user": "${POSTGRES_USER}",
                "database.password": "${POSTGRES_PASSWORD}",
                "database.hostname": "postgres",
                "tasks.max": "1",
            }
        )
    )
    out = render_config(cfg, env=_ENV)
    assert out["database.user"] == "cdc"
    assert out["database.password"] == "s3cret"
    # Literals (topology, non-string) pass through untouched.
    assert out["database.hostname"] == "postgres"
    assert out["tasks.max"] == "1"


def test_render_is_strict_on_missing_var(tmp_path: Path) -> None:
    cfg = tmp_path / "c.json"
    cfg.write_text(json.dumps({"database.user": "${POSTGRES_USER}"}))
    # Missing var must raise — never silently PUT an empty credential.
    with pytest.raises(ConnectorError, match="POSTGRES_USER"):
        render_config(cfg, env={})


def test_render_missing_file_raises() -> None:
    with pytest.raises(ConnectorError, match="not found"):
        render_config(Path("/nonexistent/connector.json"), env=_ENV)


def test_render_bad_json_raises(tmp_path: Path) -> None:
    cfg = tmp_path / "c.json"
    cfg.write_text("{not json")
    with pytest.raises(ConnectorError, match="not valid JSON"):
        render_config(cfg, env=_ENV)


def test_committed_config_pins_the_cdc_decisions() -> None:
    """The real deploy/connectors/genomics.json carries the story's key choices."""
    cfg = render_config(_CONFIG_PATH, env=_ENV)
    assert cfg["connector.class"].endswith("PostgresConnector")
    assert cfg["plugin.name"] == "pgoutput"
    assert cfg["publication.name"] == "cdc_pub"
    # The schema (migration 0002) owns the publication — the connector must not touch it.
    assert cfg["publication.autocreate.mode"] == "disabled"
    assert cfg["slot.name"] == "cdc_genomics"
    assert cfg["topic.prefix"] == "cdc"
    assert cfg["snapshot.mode"] == "initial"
    # D6: NUMERIC must not become a lossy float.
    assert cfg["decimal.handling.mode"] == "string"
    # Needed by compaction (story 03.3).
    assert cfg["tombstones.on.delete"] == "true"
    # §9 inspectability: JSON without embedded schemas.
    assert cfg["value.converter"].endswith("JsonConverter")
    assert cfg["value.converter.schemas.enable"] == "false"
    assert cfg["key.converter.schemas.enable"] == "false"
