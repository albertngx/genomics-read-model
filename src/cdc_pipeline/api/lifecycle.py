"""Specimen state-machine guards.

Two transitions map to the downstream *delete* paths (ARCHITECTURE.md §3, §7):

- **hard delete** — physically removing a ``registered`` specimen. Produces a Debezium
  ``op:"d"`` and a tombstone; consumers drop the document. Permitted only while the
  specimen is ``registered`` (for example accessioned in error, or a control specimen never
  committed to a run). In any later status the specimen is a chain-of-custody record.
- **dispose** — an ``UPDATE`` to ``status='disposed'``. The source row is retained; the
  search projection removes the document regardless.

These functions are pure (no database, no I/O), so the guards are unit-testable without the
stack. They raise :class:`fastapi.HTTPException`, which callers allow to propagate.
"""

from __future__ import annotations

from fastapi import HTTPException
from fastapi import status as http_status

from cdc_pipeline.db.models import RUN_STATUSES, SPECIMEN_STATUSES

# A specimen may be hard-deleted only in this status; every later status is retained.
HARD_DELETABLE_STATUS = "registered"
DISPOSED_STATUS = "disposed"


def validate_run_status(value: str) -> str:
    """Return ``value`` if it is a legal run status, else raise 422."""
    return _validate_status(value, RUN_STATUSES, "run")


def validate_specimen_status(value: str) -> str:
    """Return ``value`` if it is a legal specimen status, else raise 422."""
    return _validate_status(value, SPECIMEN_STATUSES, "specimen")


def ensure_hard_deletable(current_status: str) -> None:
    """Guard :http:delete:`/specimens/{id}`.

    A hard delete is legal only while the specimen is ``registered``. Anything past that
    is a retained record — reject with 409 so the state machine holds (a specimen that has
    been on an instrument cannot be made to vanish).
    """
    if current_status != HARD_DELETABLE_STATUS:
        raise HTTPException(
            status_code=http_status.HTTP_409_CONFLICT,
            detail=(
                f"cannot hard-delete a specimen with status {current_status!r}; "
                f"only {HARD_DELETABLE_STATUS!r} specimens may be physically removed. "
                f"Dispose it instead (POST /specimens/{{id}}/dispose)."
            ),
        )


def ensure_disposable(current_status: str) -> None:
    """Guard :http:post:`/specimens/{id}/dispose`. Disposing an already-disposed specimen
    is a no-op change and would emit a spurious CDC event, so reject it with 409."""
    if current_status == DISPOSED_STATUS:
        raise HTTPException(
            status_code=http_status.HTTP_409_CONFLICT,
            detail="specimen is already disposed.",
        )


def _validate_status(value: str, allowed: tuple[str, ...], kind: str) -> str:
    if value not in allowed:
        raise HTTPException(
            status_code=422,  # Unprocessable Content (literal: the named constant was renamed)
            detail=f"invalid {kind} status {value!r}; allowed: {', '.join(allowed)}.",
        )
    return value
