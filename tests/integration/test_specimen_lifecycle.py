"""The heart of story 02.2: the two delete paths and a full CRUD round-trip.

Every test runs inside a rolled-back transaction (see conftest), so it may freely create
and hard-delete rows without disturbing the seeded database.
"""

from __future__ import annotations

import pytest
from fastapi.testclient import TestClient

pytestmark = pytest.mark.integration


def _make_run(client: TestClient, code: str = "RUN-TEST-001") -> int:
    resp = client.post(
        "/runs",
        json={
            "run_code": code,
            "assay_protocol": "WES (Twist Exome 2.0)",
            "sequencer": "SEQ-07",
            "operator": "A. Okafor",
        },
    )
    assert resp.status_code == 201, resp.text
    return resp.json()["id"]


def _make_specimen(client: TestClient, barcode: str, *, status: str, run_id: int) -> int:
    resp = client.post(
        "/specimens",
        json={
            "barcode": barcode,
            "label": "Participant NGP-00042 aliquot 1 (blood)",
            "specimen_type": "blood",
            "assay": "WES",
            "volume_ul": "125.50",
            "status": status,
            "run_id": run_id,
        },
    )
    assert resp.status_code == 201, resp.text
    return resp.json()["id"]


def test_full_crud_roundtrip(client: TestClient) -> None:
    run_id = _make_run(client)
    spec_id = _make_specimen(client, "SPEC-TEST-0001", status="registered", run_id=run_id)

    # read
    got = client.get(f"/specimens/{spec_id}")
    assert got.status_code == 200
    body = got.json()
    assert body["barcode"] == "SPEC-TEST-0001"
    assert body["volume_ul"] == "125.50"  # Decimal preserved as string, never a float (D6)

    # list with a filter
    listed = client.get("/specimens", params={"run_id": run_id, "status": "registered"})
    assert listed.status_code == 200
    assert spec_id in [s["id"] for s in listed.json()]

    # update
    patched = client.patch(f"/specimens/{spec_id}", json={"status": "in_prep"})
    assert patched.status_code == 200
    assert patched.json()["status"] == "in_prep"


def test_hard_delete_registered_removes_row(client: TestClient) -> None:
    run_id = _make_run(client)
    spec_id = _make_specimen(client, "SPEC-TEST-0002", status="registered", run_id=run_id)

    deleted = client.delete(f"/specimens/{spec_id}")
    assert deleted.status_code == 204

    # the row is physically gone (op:"d" + tombstone downstream)
    assert client.get(f"/specimens/{spec_id}").status_code == 404


def test_hard_delete_non_registered_is_rejected(client: TestClient) -> None:
    run_id = _make_run(client)
    spec_id = _make_specimen(client, "SPEC-TEST-0003", status="on_instrument", run_id=run_id)

    rejected = client.delete(f"/specimens/{spec_id}")
    assert rejected.status_code == 409  # the state machine holds

    # and the row still exists — nothing was removed
    assert client.get(f"/specimens/{spec_id}").status_code == 200


def test_dispose_is_a_soft_delete_row_survives(client: TestClient) -> None:
    run_id = _make_run(client)
    spec_id = _make_specimen(client, "SPEC-TEST-0004", status="on_instrument", run_id=run_id)

    disposed = client.post(f"/specimens/{spec_id}/dispose")
    assert disposed.status_code == 200
    assert disposed.json()["status"] == "disposed"

    # source row SURVIVES (chain of custody) — only the projection removes it downstream
    still_there = client.get(f"/specimens/{spec_id}")
    assert still_there.status_code == 200
    assert still_there.json()["status"] == "disposed"


def test_dispose_twice_is_rejected(client: TestClient) -> None:
    run_id = _make_run(client)
    spec_id = _make_specimen(client, "SPEC-TEST-0005", status="registered", run_id=run_id)

    assert client.post(f"/specimens/{spec_id}/dispose").status_code == 200
    assert client.post(f"/specimens/{spec_id}/dispose").status_code == 409


def test_duplicate_barcode_is_rejected(client: TestClient) -> None:
    run_id = _make_run(client)
    _make_specimen(client, "SPEC-TEST-0006", status="registered", run_id=run_id)
    dup = client.post(
        "/specimens",
        json={
            "barcode": "SPEC-TEST-0006",
            "label": "dup",
            "specimen_type": "blood",
            "assay": "WES",
            "status": "registered",
        },
    )
    assert dup.status_code == 409
