"""CRUD and lifecycle endpoints for ``specimens``, the child entity.

Two lifecycle endpoints produce distinct source events that both result in a removal from
the downstream search projection (ARCHITECTURE.md §3, §7):

- :http:delete:`/specimens/{id}` — a **hard delete**: the row is physically removed
  (``op:"d"`` plus a tombstone). Permitted only while the specimen is ``registered``; the
  guard is in ``lifecycle.py``.
- :http:post:`/specimens/{id}/dispose` — a **soft delete**: an ``UPDATE`` to
  ``status='disposed'``. The source row is retained (chain of custody); the projection
  removes the document regardless.

Ordinary field edits use :http:patch:`/specimens/{id}`. Disposal and hard-delete are
separate endpoints so the two change-data paths remain explicit and independently testable.
"""

from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException, Query, Response, status
from sqlalchemy import func, select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from cdc_pipeline.api.db import get_session
from cdc_pipeline.api.lifecycle import (
    DISPOSED_STATUS,
    ensure_disposable,
    ensure_hard_deletable,
    validate_specimen_status,
)
from cdc_pipeline.api.schemas import SpecimenCreate, SpecimenRead, SpecimenUpdate
from cdc_pipeline.db.models import Specimen

router = APIRouter(prefix="/specimens", tags=["specimens"])


def _get_or_404(session: Session, specimen_id: int) -> Specimen:
    specimen = session.get(Specimen, specimen_id)
    if specimen is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="specimen not found")
    return specimen


@router.post("", response_model=SpecimenRead, status_code=status.HTTP_201_CREATED)
def create_specimen(payload: SpecimenCreate, session: Session = Depends(get_session)) -> Specimen:
    validate_specimen_status(payload.status)
    specimen = Specimen(**payload.model_dump())
    session.add(specimen)
    try:
        session.commit()
    except IntegrityError as exc:
        session.rollback()
        # Either a duplicate barcode or a run_id pointing at no run — both are the
        # caller's error, not ours.
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail=f"barcode {payload.barcode!r} already exists or run_id is unknown",
        ) from exc
    session.refresh(specimen)
    return specimen


@router.get("", response_model=list[SpecimenRead])
def list_specimens(
    session: Session = Depends(get_session),
    status_filter: str | None = Query(default=None, alias="status"),
    run_id: int | None = None,
    assay: str | None = None,
    specimen_type: str | None = None,
    limit: int = Query(default=50, ge=1, le=500),
    offset: int = Query(default=0, ge=0),
) -> list[Specimen]:
    stmt = select(Specimen).order_by(Specimen.id)
    if status_filter is not None:
        stmt = stmt.where(Specimen.status == status_filter)
    if run_id is not None:
        stmt = stmt.where(Specimen.run_id == run_id)
    if assay is not None:
        stmt = stmt.where(Specimen.assay == assay)
    if specimen_type is not None:
        stmt = stmt.where(Specimen.specimen_type == specimen_type)
    stmt = stmt.limit(limit).offset(offset)
    return list(session.scalars(stmt))


@router.get("/{specimen_id}", response_model=SpecimenRead)
def get_specimen(specimen_id: int, session: Session = Depends(get_session)) -> Specimen:
    return _get_or_404(session, specimen_id)


@router.patch("/{specimen_id}", response_model=SpecimenRead)
def update_specimen(
    specimen_id: int, payload: SpecimenUpdate, session: Session = Depends(get_session)
) -> Specimen:
    specimen = _get_or_404(session, specimen_id)
    changes = payload.model_dump(exclude_unset=True)
    if "status" in changes:
        validate_specimen_status(changes["status"])
    for field, value in changes.items():
        setattr(specimen, field, value)
    specimen.updated_at = func.now()
    try:
        session.commit()
    except IntegrityError as exc:
        session.rollback()
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail="update violates a constraint (duplicate barcode or unknown run_id)",
        ) from exc
    session.refresh(specimen)
    return specimen


@router.post("/{specimen_id}/dispose", response_model=SpecimenRead)
def dispose_specimen(specimen_id: int, session: Session = Depends(get_session)) -> Specimen:
    """Soft delete: UPDATE status → ``disposed``. The row survives (chain of custody);
    the search projection removes it anyway (§3/§7)."""
    specimen = _get_or_404(session, specimen_id)
    ensure_disposable(specimen.status)
    specimen.status = DISPOSED_STATUS
    specimen.updated_at = func.now()
    session.commit()
    session.refresh(specimen)
    return specimen


@router.delete("/{specimen_id}", status_code=status.HTTP_204_NO_CONTENT)
def hard_delete_specimen(specimen_id: int, session: Session = Depends(get_session)) -> Response:
    """Hard delete: physically remove a ``registered`` specimen (op:"d" + tombstone).
    Rejected with 409 if the specimen is past ``registered`` — the state machine holds."""
    specimen = _get_or_404(session, specimen_id)
    ensure_hard_deletable(specimen.status)
    session.delete(specimen)
    session.commit()
    return Response(status_code=status.HTTP_204_NO_CONTENT)
