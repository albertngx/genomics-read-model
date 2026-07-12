"""Engine/URL helpers shared by Alembic and the seed.

The ``.env`` ``DATABASE_URL`` is a plain ``postgresql://…`` string — the canonical form
used by ``psql``, smoke tests, and every non-Python client. SQLAlchemy, however, needs
the driver named in the scheme so it doesn't fall back to the (absent) psycopg2. This
module is the one place that bridges the two, so no other code embeds ``+psycopg``.
"""

from __future__ import annotations

import os
from pathlib import Path

from sqlalchemy import Engine, create_engine

from cdc_pipeline.config import ConfigError, Settings, load_dotenv

_REPO_ROOT = Path(__file__).resolve().parents[3]


def sqlalchemy_url(database_url: str) -> str:
    """Return ``database_url`` with the psycopg (v3) driver named in the scheme.

    ``postgresql://…`` → ``postgresql+psycopg://…``. Already-qualified URLs (any
    ``postgresql+driver://``) are passed through untouched.
    """
    prefix = "postgresql://"
    if database_url.startswith(prefix):
        return "postgresql+psycopg://" + database_url[len(prefix) :]
    return database_url


def database_url_from_env() -> str:
    """Load the repo-root ``.env`` (without clobbering) and return the raw DATABASE_URL."""
    load_dotenv(_REPO_ROOT / ".env")
    return Settings.from_env().database_url


def create_source_engine(*, echo: bool = False) -> Engine:
    """Build a SQLAlchemy engine for the source database from the environment."""
    return create_engine(sqlalchemy_url(database_url_from_env()), echo=echo, future=True)


def create_api_engine(*, echo: bool = False) -> Engine:
    """Build the engine for the tracking API, which needs ONLY its own database.

    Deliberately *not* :func:`create_source_engine` / :meth:`Settings.from_env`: those
    require the whole stack's connection strings (Kafka, Redis, Elasticsearch). The
    tracking service must boot knowing nothing about any of them — that ignorance is the
    architectural point (ARCHITECTURE.md §2). So this reads exactly one variable,
    ``DATABASE_URL``, and would still start on a machine where no other service exists.

    ``pool_pre_ping`` guards against stale connections (e.g. Postgres restarted by a
    chaos drill in epic 07) without the API needing to know it happened.
    """
    load_dotenv(_REPO_ROOT / ".env")
    url = os.environ.get("DATABASE_URL")
    if not url:
        raise ConfigError(
            "missing required environment variable 'DATABASE_URL'. "
            "Copy .env.example to .env (scripts/bootstrap.sh does this) and set it."
        )
    return create_engine(sqlalchemy_url(url), echo=echo, future=True, pool_pre_ping=True)
