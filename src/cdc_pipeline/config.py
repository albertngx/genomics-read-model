"""Runtime configuration, read from the environment.

Every connection string in this project lives in ``.env.example`` (the single source
of truth) and is surfaced to code exclusively through :class:`Settings`. No module
should read ``os.environ`` directly or embed a host/port/URL literal — go through here
so a value changes in exactly one place.
"""

from __future__ import annotations

import os
from collections.abc import Mapping
from dataclasses import dataclass


class ConfigError(RuntimeError):
    """A required environment variable is missing or malformed."""


def _require(env: Mapping[str, str], name: str) -> str:
    try:
        value = env[name]
    except KeyError as exc:
        raise ConfigError(
            f"missing required environment variable {name!r}. "
            f"Copy .env.example to .env (scripts/bootstrap.sh does this) and set it."
        ) from exc
    if not value:
        raise ConfigError(f"environment variable {name!r} is set but empty.")
    return value


@dataclass(frozen=True)
class Settings:
    """Connection strings and ports for every service in the stack."""

    database_url: str
    kafka_bootstrap_servers: str
    kafka_connect_url: str
    elasticsearch_url: str
    redis_url: str
    api_port: int

    @classmethod
    def from_env(cls, env: Mapping[str, str] | None = None) -> Settings:
        """Build settings from ``env`` (defaults to ``os.environ``).

        Raises :class:`ConfigError` with an actionable message on the first missing
        or malformed variable.
        """
        env = os.environ if env is None else env
        api_port_raw = _require(env, "API_PORT")
        try:
            api_port = int(api_port_raw)
        except ValueError as exc:
            raise ConfigError(f"API_PORT must be an integer, got {api_port_raw!r}.") from exc
        return cls(
            database_url=_require(env, "DATABASE_URL"),
            kafka_bootstrap_servers=_require(env, "KAFKA_BOOTSTRAP_SERVERS"),
            kafka_connect_url=_require(env, "KAFKA_CONNECT_URL"),
            elasticsearch_url=_require(env, "ELASTICSEARCH_URL"),
            redis_url=_require(env, "REDIS_URL"),
            api_port=api_port,
        )
