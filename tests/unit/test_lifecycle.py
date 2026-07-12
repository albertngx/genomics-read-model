"""Unit tests for the specimen state machine — pure, no database required.

The rules here are the only real business logic in the tracking service, so they earn a
fast test that runs without the stack.
"""

from __future__ import annotations

import pytest
from fastapi import HTTPException

from cdc_pipeline.api.lifecycle import (
    ensure_disposable,
    ensure_hard_deletable,
    validate_run_status,
    validate_specimen_status,
)
from cdc_pipeline.db.models import SPECIMEN_STATUSES


def test_hard_delete_allowed_only_when_registered() -> None:
    ensure_hard_deletable("registered")  # does not raise


@pytest.mark.parametrize("status", [s for s in SPECIMEN_STATUSES if s != "registered"])
def test_hard_delete_rejected_past_registered(status: str) -> None:
    with pytest.raises(HTTPException) as exc:
        ensure_hard_deletable(status)
    assert exc.value.status_code == 409


def test_dispose_rejected_when_already_disposed() -> None:
    with pytest.raises(HTTPException) as exc:
        ensure_disposable("disposed")
    assert exc.value.status_code == 409


@pytest.mark.parametrize("status", [s for s in SPECIMEN_STATUSES if s != "disposed"])
def test_dispose_allowed_from_any_live_status(status: str) -> None:
    ensure_disposable(status)  # does not raise


def test_status_validators_accept_known_values() -> None:
    assert validate_specimen_status("registered") == "registered"
    assert validate_run_status("active") == "active"


def test_status_validators_reject_unknown_values() -> None:
    with pytest.raises(HTTPException) as spec_exc:
        validate_specimen_status("teleported")
    assert spec_exc.value.status_code == 422
    with pytest.raises(HTTPException) as run_exc:
        validate_run_status("vaporised")
    assert run_exc.value.status_code == 422
