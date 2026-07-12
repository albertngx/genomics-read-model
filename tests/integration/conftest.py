"""Fixtures for the API integration suite (marked ``integration``).

These run against the *live* compose Postgres — ``make up && make migrate`` first. Each
test gets a :class:`TestClient` whose session is bound to an outer transaction that is
**rolled back** at the end, so nothing the tests write (or hard-delete) ever touches the
seeded database. Endpoint ``commit()`` calls land in a savepoint
(``join_transaction_mode="create_savepoint"``), keeping that outer transaction open until
we discard it.
"""

from __future__ import annotations

from collections.abc import Iterator

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import Engine
from sqlalchemy.orm import Session, sessionmaker

from cdc_pipeline.api.app import create_app
from cdc_pipeline.api.db import get_session
from cdc_pipeline.db.engine import create_api_engine


@pytest.fixture(scope="session")
def engine() -> Engine:
    return create_api_engine()


@pytest.fixture
def client(engine: Engine) -> Iterator[TestClient]:
    connection = engine.connect()
    transaction = connection.begin()
    make_session = sessionmaker(
        bind=connection,
        expire_on_commit=False,
        join_transaction_mode="create_savepoint",
    )

    def override_get_session() -> Iterator[Session]:
        with make_session() as session:
            yield session

    app = create_app()
    app.dependency_overrides[get_session] = override_get_session

    with TestClient(app) as test_client:
        yield test_client

    transaction.rollback()  # discard everything the test did
    connection.close()
