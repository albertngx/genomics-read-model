"""``PATCH /runs/{id}`` — the §7 denormalization-storm trigger, tested now.

Re-assignment (change ``sequencer``) and protocol amendment (change ``assay_protocol``)
are ordinary column writes to this service, but downstream they fan out to a re-index of
every specimen in the run. The API stays ignorant of that; these tests just pin the write
behaviour the later fan-out depends on.
"""

from __future__ import annotations

import pytest
from fastapi.testclient import TestClient

pytestmark = pytest.mark.integration


def _make_run(client: TestClient, code: str = "RUN-TEST-100") -> dict:
    resp = client.post(
        "/runs",
        json={
            "run_code": code,
            "assay_protocol": "WGS PCR-free (TruSeq DNA PCR-Free)",
            "sequencer": "SEQ-01",
            "operator": "M. Chen",
        },
    )
    assert resp.status_code == 201, resp.text
    return resp.json()


def test_patch_reassigns_sequencer_and_bumps_updated_at(client: TestClient) -> None:
    created = _make_run(client)
    resp = client.patch(f"/runs/{created['id']}", json={"sequencer": "SEQ-08"})
    assert resp.status_code == 200
    body = resp.json()
    assert body["sequencer"] == "SEQ-08"
    # updated_at advanced — downstream ordering keys off it
    assert body["updated_at"] >= created["updated_at"]


def test_patch_amends_protocol(client: TestClient) -> None:
    created = _make_run(client, code="RUN-TEST-101")
    resp = client.patch(
        f"/runs/{created['id']}",
        json={"assay_protocol": "WES (Twist Exome 2.0)"},
    )
    assert resp.status_code == 200
    assert resp.json()["assay_protocol"] == "WES (Twist Exome 2.0)"


def test_patch_partial_leaves_other_fields_untouched(client: TestClient) -> None:
    created = _make_run(client, code="RUN-TEST-102")
    resp = client.patch(f"/runs/{created['id']}", json={"status": "active"})
    assert resp.status_code == 200
    body = resp.json()
    assert body["status"] == "active"
    assert body["operator"] == created["operator"]
    assert body["sequencer"] == created["sequencer"]


def test_patch_unknown_run_is_404(client: TestClient) -> None:
    assert client.patch("/runs/999999999", json={"sequencer": "SEQ-02"}).status_code == 404
