"""Unit tests for cdc_pipeline.config — pure, no infrastructure required."""

from __future__ import annotations

import pytest

from cdc_pipeline.config import ConfigError, Settings

_COMPLETE_ENV = {
    "DATABASE_URL": "postgresql://cdc:cdc@localhost:5432/genomics",
    "KAFKA_BOOTSTRAP_SERVERS": "localhost:9092",
    "KAFKA_CONNECT_URL": "http://localhost:8083",
    "ELASTICSEARCH_URL": "http://localhost:9200",
    "REDIS_URL": "redis://localhost:6379/0",
    "API_PORT": "8000",
}


def test_from_env_reads_every_field() -> None:
    settings = Settings.from_env(_COMPLETE_ENV)

    assert settings.database_url == _COMPLETE_ENV["DATABASE_URL"]
    assert settings.kafka_bootstrap_servers == "localhost:9092"
    assert settings.kafka_connect_url == "http://localhost:8083"
    assert settings.elasticsearch_url == "http://localhost:9200"
    assert settings.redis_url == "redis://localhost:6379/0"
    assert settings.api_port == 8000  # coerced to int


def test_settings_is_frozen() -> None:
    settings = Settings.from_env(_COMPLETE_ENV)
    with pytest.raises(AttributeError):
        settings.database_url = "changed"  # type: ignore[misc]


@pytest.mark.parametrize("missing", sorted(_COMPLETE_ENV))
def test_missing_variable_names_itself(missing: str) -> None:
    env = {k: v for k, v in _COMPLETE_ENV.items() if k != missing}
    with pytest.raises(ConfigError, match=missing):
        Settings.from_env(env)


def test_empty_variable_is_rejected() -> None:
    env = {**_COMPLETE_ENV, "REDIS_URL": ""}
    with pytest.raises(ConfigError, match="REDIS_URL"):
        Settings.from_env(env)


def test_non_integer_api_port_is_rejected() -> None:
    env = {**_COMPLETE_ENV, "API_PORT": "not-a-number"}
    with pytest.raises(ConfigError, match="API_PORT"):
        Settings.from_env(env)
