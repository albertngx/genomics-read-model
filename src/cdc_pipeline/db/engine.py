"""Engine/URL helpers shared by Alembic and the seed.

The ``.env`` ``DATABASE_URL`` is a plain ``postgresql://…`` string — the canonical form
used by ``psql``, smoke tests, and every non-Python client. SQLAlchemy, however, needs
the driver named in the scheme so it doesn't fall back to the (absent) psycopg2. This
module is the one place that bridges the two, so no other code embeds ``+psycopg``.
"""

from __future__ import annotations

from pathlib import Path

from sqlalchemy import Engine, create_engine

from cdc_pipeline.config import Settings, load_dotenv

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
