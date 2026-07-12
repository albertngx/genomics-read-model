"""SQLAlchemy models for the source of truth — ``runs`` (parent) and ``specimens``
(child), exactly as ARCHITECTURE.md §3.

The migration in ``migrations/versions`` is the authoritative DDL; these models mirror
it so the seed (this story) and the tracking API (epic 02.2) share one definition. The
two must stay in lockstep — Alembic autogenerate diffs the models against the database
and will flag any drift.

Deliberate modeling choices, all from ARCHITECTURE.md §3:

- ``volume_ul`` is ``NUMERIC(10, 2)`` — never a float. Aliquot volumes are precise
  decimals; lossy JSON floats are a real lab data-integrity bug (decision D6).
- ``status`` on both tables is a CHECK-constrained string, not a native ENUM. A CHECK
  is simpler to migrate than a Postgres enum type and equally authoritative; the
  allowed values live in one place (:data:`RUN_STATUSES` / :data:`SPECIMEN_STATUSES`).
- ``specimens.run_id`` is nullable: a ``registered`` control/scratch specimen may never
  be committed to a run (and is therefore hard-deletable — §3).
"""

from __future__ import annotations

import datetime as dt
from decimal import Decimal

from sqlalchemy import (
    BigInteger,
    CheckConstraint,
    DateTime,
    ForeignKey,
    Numeric,
    String,
    Text,
    func,
)
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship

# The status lifecycles. Single source of truth for the CHECK constraints below, the
# seed's status mix, and the API's state machine (epic 02.2). Order is lifecycle order.
RUN_STATUSES: tuple[str, ...] = ("planned", "active", "completed", "aborted")
SPECIMEN_STATUSES: tuple[str, ...] = (
    "registered",  # accessioned; hard-deletable (op:"d" + tombstone downstream) — §3
    "in_prep",
    "on_instrument",
    "completed",
    "disposed",  # consumed/discarded; row SURVIVES (chain of custody) — soft delete §3/§7
)


def _in_clause(column: str, allowed: tuple[str, ...]) -> str:
    """Render a ``col IN ('a', 'b', ...)`` CHECK body from allowed values."""
    values = ", ".join(f"'{value}'" for value in allowed)
    return f"{column} IN ({values})"


class Base(DeclarativeBase):
    """Declarative base; ``Base.metadata`` is what Alembic autogenerate compares against."""


class Run(Base):
    """A sequencing run — the parent entity. One run has many specimens."""

    __tablename__ = "runs"
    __table_args__ = (CheckConstraint(_in_clause("status", RUN_STATUSES), name="ck_runs_status"),)

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    run_code: Mapped[str] = mapped_column(String(64), unique=True, nullable=False)
    assay_protocol: Mapped[str] = mapped_column(String(128), nullable=False)
    sequencer: Mapped[str] = mapped_column(String(64), nullable=False)
    operator: Mapped[str] = mapped_column(String(128), nullable=False)
    status: Mapped[str] = mapped_column(String(32), nullable=False)
    updated_at: Mapped[dt.datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )

    specimens: Mapped[list[Specimen]] = relationship(back_populates="run")


class Specimen(Base):
    """A specimen (aliquot) — the child entity, and the star of the read model.

    ``run_id`` is the "denormalization-bait" FK (ARCHITECTURE.md §3, §7): the search
    document embeds fields from the referenced run, so a run change fans out to a
    re-index of every specimen pointing at it.
    """

    __tablename__ = "specimens"
    __table_args__ = (
        CheckConstraint(_in_clause("status", SPECIMEN_STATUSES), name="ck_specimens_status"),
    )

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    barcode: Mapped[str] = mapped_column(String(64), unique=True, nullable=False)
    label: Mapped[str] = mapped_column(String(256), nullable=False)
    description: Mapped[str | None] = mapped_column(Text, nullable=True)
    specimen_type: Mapped[str] = mapped_column(String(64), nullable=False)
    assay: Mapped[str] = mapped_column(String(64), nullable=False)
    # NUMERIC(10,2), mapped to Decimal — never float. Forces the D6 decimal handling.
    volume_ul: Mapped[Decimal | None] = mapped_column(Numeric(10, 2), nullable=True)
    status: Mapped[str] = mapped_column(String(32), nullable=False)
    run_id: Mapped[int | None] = mapped_column(
        BigInteger, ForeignKey("runs.id", ondelete="SET NULL"), nullable=True, index=True
    )
    updated_at: Mapped[dt.datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )

    run: Mapped[Run | None] = relationship(back_populates="specimens")
