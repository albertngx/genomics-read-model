"""Request/response schemas for the tracking API (Pydantic v2).

These mirror the ORM models in :mod:`cdc_pipeline.db.models` but are a *separate* wire
contract on purpose: the API decides what a caller may set (``updated_at`` is
server-owned, ``id`` is assigned) independently of the table's columns.

``volume_ul`` is a :class:`~decimal.Decimal` end to end — never a float. Pydantic v2
serialises it to a JSON *string* (``"123.45"``), which preserves the exact two-decimal
aliquot volume across the wire; turning it into a binary float would be the D6
data-integrity bug the schema exists to prevent (ARCHITECTURE.md §3).
"""

from __future__ import annotations

import datetime as dt
from decimal import Decimal
from typing import Annotated

from pydantic import BaseModel, ConfigDict, Field

from cdc_pipeline.db.models import RUN_STATUSES, SPECIMEN_STATUSES

# Reused constrained-string aliases so every "status" field validates against the one
# lifecycle definition in models.py (the same source as the DB CHECK constraint).
RunStatus = Annotated[str, Field(examples=[RUN_STATUSES[0]])]
SpecimenStatus = Annotated[str, Field(examples=[SPECIMEN_STATUSES[0]])]


# ── runs ─────────────────────────────────────────────────────────────────────────


class RunCreate(BaseModel):
    run_code: str = Field(max_length=64, examples=["RUN-2026-900"])
    assay_protocol: str = Field(max_length=128, examples=["WES (Twist Exome 2.0)"])
    sequencer: str = Field(max_length=64, examples=["SEQ-07"])
    operator: str = Field(max_length=128, examples=["A. Okafor"])
    status: RunStatus = "planned"


class RunUpdate(BaseModel):
    """Partial update. Every field optional; only those provided are changed.

    Setting ``sequencer`` is the **re-assignment** and ``assay_protocol`` the **protocol
    amendment** that fan out to a re-index of the run's specimens downstream — the §7
    "denormalization storm" trigger. To the API they are just column writes; it stays
    ignorant of the storm it sets off.
    """

    assay_protocol: str | None = Field(default=None, max_length=128)
    sequencer: str | None = Field(default=None, max_length=64)
    operator: str | None = Field(default=None, max_length=128)
    status: RunStatus | None = None


class RunRead(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: int
    run_code: str
    assay_protocol: str
    sequencer: str
    operator: str
    status: str
    updated_at: dt.datetime


# ── specimens ────────────────────────────────────────────────────────────────────


class SpecimenCreate(BaseModel):
    barcode: str = Field(max_length=64, examples=["SPEC-900001"])
    label: str = Field(max_length=256, examples=["Participant NGP-00042 aliquot 1 (blood)"])
    description: str | None = None
    specimen_type: str = Field(max_length=64, examples=["blood"])
    assay: str = Field(max_length=64, examples=["WES"])
    volume_ul: Decimal | None = Field(default=None, ge=0, examples=["125.50"])
    status: SpecimenStatus = "registered"
    run_id: int | None = None


class SpecimenUpdate(BaseModel):
    """Partial update. Note: disposal and hard-delete are *not* done here — they have
    dedicated lifecycle endpoints so the two CDC delete paths are explicit (§3, §7)."""

    label: str | None = Field(default=None, max_length=256)
    description: str | None = None
    specimen_type: str | None = Field(default=None, max_length=64)
    assay: str | None = Field(default=None, max_length=64)
    volume_ul: Decimal | None = Field(default=None, ge=0)
    status: SpecimenStatus | None = None
    run_id: int | None = None


class SpecimenRead(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: int
    barcode: str
    label: str
    description: str | None
    specimen_type: str
    assay: str
    volume_ul: Decimal | None
    status: str
    run_id: int | None
    updated_at: dt.datetime
