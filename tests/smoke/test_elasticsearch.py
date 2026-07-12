"""Smoke: index a document into Elasticsearch and read it back."""

from __future__ import annotations

import pytest
from elastic_transport import TransportError
from elasticsearch import Elasticsearch

from cdc_pipeline.config import Settings

pytestmark = pytest.mark.smoke

_INDEX = "cdc-smoke"


def test_index_and_get_document(settings: Settings) -> None:
    es = Elasticsearch(settings.elasticsearch_url, request_timeout=15)

    try:
        if not es.ping():
            pytest.fail(
                f"cannot reach Elasticsearch at {settings.elasticsearch_url!r}. "
                f"Is the elasticsearch service up? Check `make ps`."
            )

        # Fixed index name + cleanup on both ends keeps the test idempotent across runs.
        es.indices.delete(index=_INDEX, ignore_unavailable=True)
        doc = {"specimen_id": "SMOKE-1", "status": "sequenced"}
        es.index(index=_INDEX, id="1", document=doc, refresh=True)

        got = es.get(index=_INDEX, id="1")
        assert got["_source"] == doc, f"round-tripped document differs: {got['_source']} != {doc}"
    except TransportError as exc:
        pytest.fail(
            f"Elasticsearch smoke check failed against {settings.elasticsearch_url!r}: {exc}"
        )
    finally:
        es.indices.delete(index=_INDEX, ignore_unavailable=True)
        es.close()
