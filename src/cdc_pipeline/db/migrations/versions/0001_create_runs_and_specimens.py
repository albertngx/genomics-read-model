"""Create runs and specimens (ARCHITECTURE.md §3)

The source-of-truth schema: ``runs`` (parent) and ``specimens`` (child). Deliberate
choices carried from the architecture — ``volume_ul NUMERIC(10,2)`` (never a float,
decision D6), status lifecycles as CHECK constraints, and a nullable ``run_id`` FK so a
``registered`` scratch/control specimen can exist un-committed to any run (§3).

Revision ID: 0001
Revises:
Create Date: 2026-07-12
"""

from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

from cdc_pipeline.db.models import RUN_STATUSES, SPECIMEN_STATUSES

revision: str = "0001"
down_revision: str | None = None
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def _in_clause(column: str, allowed: tuple[str, ...]) -> str:
    values = ", ".join(f"'{value}'" for value in allowed)
    return f"{column} IN ({values})"


def upgrade() -> None:
    op.create_table(
        "runs",
        sa.Column("id", sa.BigInteger(), primary_key=True, autoincrement=True),
        sa.Column("run_code", sa.String(length=64), nullable=False),
        sa.Column("assay_protocol", sa.String(length=128), nullable=False),
        sa.Column("sequencer", sa.String(length=64), nullable=False),
        sa.Column("operator", sa.String(length=128), nullable=False),
        sa.Column("status", sa.String(length=32), nullable=False),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            server_default=sa.func.now(),
            nullable=False,
        ),
        sa.UniqueConstraint("run_code", name="uq_runs_run_code"),
        sa.CheckConstraint(_in_clause("status", RUN_STATUSES), name="ck_runs_status"),
    )

    op.create_table(
        "specimens",
        sa.Column("id", sa.BigInteger(), primary_key=True, autoincrement=True),
        sa.Column("barcode", sa.String(length=64), nullable=False),
        sa.Column("label", sa.String(length=256), nullable=False),
        sa.Column("description", sa.Text(), nullable=True),
        sa.Column("specimen_type", sa.String(length=64), nullable=False),
        sa.Column("assay", sa.String(length=64), nullable=False),
        # NUMERIC(10,2), never float — aliquot volumes are precise decimals (D6).
        sa.Column("volume_ul", sa.Numeric(precision=10, scale=2), nullable=True),
        sa.Column("status", sa.String(length=32), nullable=False),
        # Nullable so a registered scratch/control specimen can exist with no run (§3).
        # ON DELETE SET NULL: deleting a run must not silently delete its specimens.
        sa.Column(
            "run_id",
            sa.BigInteger(),
            sa.ForeignKey("runs.id", ondelete="SET NULL", name="fk_specimens_run_id"),
            nullable=True,
        ),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            server_default=sa.func.now(),
            nullable=False,
        ),
        sa.UniqueConstraint("barcode", name="uq_specimens_barcode"),
        sa.CheckConstraint(_in_clause("status", SPECIMEN_STATUSES), name="ck_specimens_status"),
    )
    # FK lookups and the §7 "re-index every specimen in this run" fan-out both scan by run_id.
    op.create_index("ix_specimens_run_id", "specimens", ["run_id"])


def downgrade() -> None:
    op.drop_index("ix_specimens_run_id", table_name="specimens")
    op.drop_table("specimens")
    op.drop_table("runs")
