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

    **Pool sizing (tuned in story 02.3).** FastAPI runs the sync endpoints in Starlette's
    thread pool (~40 threads); each in-flight request holds one DB connection for its whole
    duration. With SQLAlchemy's default pool (5 + 10 overflow = 15) the extra threads block
    waiting for a connection under load, and at ~1k writes/s the wait cascades into request
    timeouts even though Postgres itself is nearly idle. Sizing the pool to *exceed* the
    thread pool removes that self-inflicted contention — the load generator (story 02.3)
    then sustains 1k writes/s. ``DB_POOL_SIZE`` / ``DB_MAX_OVERFLOW`` override the defaults;
    keep ``pool_size + max_overflow`` below Postgres ``max_connections`` (100).
    """
    load_dotenv(_REPO_ROOT / ".env")
    url = os.environ.get("DATABASE_URL")
    if not url:
        raise ConfigError(
            "missing required environment variable 'DATABASE_URL'. "
            "Copy .env.example to .env (scripts/bootstrap.sh does this) and set it."
        )
    # Per-worker pool. With API_WORKERS uvicorn processes, the server-wide connection count
    # is API_WORKERS × (pool_size + max_overflow); keep that below Postgres max_connections.
    pool_size = _int_env("DB_POOL_SIZE", 20)
    max_overflow = _int_env("DB_MAX_OVERFLOW", 10)
    return create_engine(
        sqlalchemy_url(url),
        echo=echo,
        future=True,
        pool_pre_ping=True,
        pool_size=pool_size,
        max_overflow=max_overflow,
        pool_timeout=10,  # fail fast rather than pile up if the pool is ever truly exhausted
    )


def _int_env(name: str, default: int) -> int:
    """Read a small integer tuning knob from the environment, falling back to ``default``."""
    raw = os.environ.get(name)
    if not raw:
        return default
    try:
        return int(raw)
    except ValueError as exc:
        raise ConfigError(f"{name} must be an integer, got {raw!r}.") from exc
