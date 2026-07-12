"""Liveness/readiness probe. The compose healthcheck and later ops dashboards hit this.

It confirms the one thing the service depends on — its database — is reachable, and
nothing else (the service has no other dependencies to check, by design).
"""

from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import Session

from cdc_pipeline.api.db import get_session

router = APIRouter(tags=["health"])


@router.get("/health", summary="Liveness + database reachability")
def health(session: Session = Depends(get_session)) -> dict[str, str]:
    try:
        session.execute(text("SELECT 1"))
    except SQLAlchemyError as exc:  # pragma: no cover - exercised only when PG is down
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="database unreachable",
        ) from exc
    return {"status": "ok", "database": "up"}
