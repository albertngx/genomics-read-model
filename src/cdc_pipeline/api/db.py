"""Database wiring for the API: a lazily-built session-per-request dependency.

The engine/sessionmaker are created on first use, not at import, so importing the app (or
any router) never touches the environment or opens a pool — unit tests and OpenAPI
generation stay infrastructure-free. Integration tests override :func:`get_session` to
bind a session to their own rolled-back transaction.

Transactions are plain and explicit: each request gets one :class:`Session`; write
endpoints call ``commit()`` themselves; the context manager rolls back on any exception.
"""

from __future__ import annotations

from collections.abc import Iterator
from functools import lru_cache

from sqlalchemy.orm import Session, sessionmaker

from cdc_pipeline.db.engine import create_api_engine


@lru_cache(maxsize=1)
def _session_factory() -> sessionmaker[Session]:
    """Build (once) the sessionmaker bound to the API's own database engine."""
    return sessionmaker(bind=create_api_engine(), expire_on_commit=False, future=True)


def get_session() -> Iterator[Session]:
    """FastAPI dependency: yield a request-scoped session, closed afterwards."""
    with _session_factory()() as session:
        yield session
