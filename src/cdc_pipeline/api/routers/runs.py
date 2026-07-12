"""CRUD for ``runs``, the parent entity.

:http:patch:`/runs/{id}` changes a run's ``sequencer`` (re-assignment) or ``assay_protocol``
(amendment). Downstream, the search projection embeds these run fields in every specimen
document, so such a change fans out to a re-index of the run's specimens (ARCHITECTURE.md
§7). This service performs only the column update; the fan-out is handled by the indexer.
"""

from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException, Query, status
from sqlalchemy import func, select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from cdc_pipeline.api.db import get_session
from cdc_pipeline.api.lifecycle import validate_run_status
from cdc_pipeline.api.schemas import RunCreate, RunRead, RunUpdate
from cdc_pipeline.db.models import Run

router = APIRouter(prefix="/runs", tags=["runs"])


def _get_or_404(session: Session, run_id: int) -> Run:
    run = session.get(Run, run_id)
    if run is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="run not found")
    return run


@router.post("", response_model=RunRead, status_code=status.HTTP_201_CREATED)
def create_run(payload: RunCreate, session: Session = Depends(get_session)) -> Run:
    validate_run_status(payload.status)
    run = Run(**payload.model_dump())
    session.add(run)
    try:
        session.commit()
    except IntegrityError as exc:
        session.rollback()
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail=f"run_code {payload.run_code!r} already exists",
        ) from exc
    session.refresh(run)
    return run


@router.get("", response_model=list[RunRead])
def list_runs(
    session: Session = Depends(get_session),
    status_filter: str | None = Query(default=None, alias="status"),
    sequencer: str | None = None,
    limit: int = Query(default=50, ge=1, le=500),
    offset: int = Query(default=0, ge=0),
) -> list[Run]:
    stmt = select(Run).order_by(Run.id)
    if status_filter is not None:
        stmt = stmt.where(Run.status == status_filter)
    if sequencer is not None:
        stmt = stmt.where(Run.sequencer == sequencer)
    stmt = stmt.limit(limit).offset(offset)
    return list(session.scalars(stmt))


@router.get("/{run_id}", response_model=RunRead)
def get_run(run_id: int, session: Session = Depends(get_session)) -> Run:
    return _get_or_404(session, run_id)


@router.patch("/{run_id}", response_model=RunRead)
def update_run(run_id: int, payload: RunUpdate, session: Session = Depends(get_session)) -> Run:
    run = _get_or_404(session, run_id)
    changes = payload.model_dump(exclude_unset=True)
    if "status" in changes:
        validate_run_status(changes["status"])
    for field, value in changes.items():
        setattr(run, field, value)
    run.updated_at = func.now()  # bump so downstream ordering reflects this write
    session.commit()
    session.refresh(run)
    return run
