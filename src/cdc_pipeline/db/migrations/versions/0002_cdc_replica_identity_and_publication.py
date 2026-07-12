"""CDC capture config: REPLICA IDENTITY FULL + Debezium publication (D4)

This migration is where the *schema* owns what CDC captures — not the connector config.
Two pieces:

1. ``REPLICA IDENTITY FULL`` on both tables. By default Postgres logs only the primary
   key in the WAL for UPDATE/DELETE. FULL logs the entire *before-image*, so delete and
   update events are self-describing and the verifier (epic 06) and consumers get the
   complete old row. Cost, stated honestly: more WAL volume per change (decision D4) —
   a real production tuning knob, acceptable at this scale.

2. ``CREATE PUBLICATION cdc_pub FOR TABLE runs, specimens``. A publication is a named,
   server-side set of tables whose changes are offered to logical-replication
   subscribers (Debezium is one). Putting it in the migration means the schema — not
   connector JSON — is the single authority on which tables are captured; adding a table
   to CDC is a schema change with a review and a history, as it should be.

Revision ID: 0002
Revises: 0001
Create Date: 2026-07-12
"""

from __future__ import annotations

from collections.abc import Sequence

from alembic import op

revision: str = "0002"
down_revision: str | None = "0001"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

# The publication Debezium subscribes to (epic 03 points the connector at this name via
# publication.name=cdc_pub, publication.autocreate.mode=disabled — the schema owns it).
PUBLICATION = "cdc_pub"


def upgrade() -> None:
    # FULL before-images so UPDATE/DELETE WAL records carry the complete old row (D4).
    op.execute("ALTER TABLE runs REPLICA IDENTITY FULL")
    op.execute("ALTER TABLE specimens REPLICA IDENTITY FULL")

    # The schema owns what is captured. (Postgres has no CREATE PUBLICATION IF NOT EXISTS;
    # re-running `make seed` is safe because Alembic won't re-apply an applied revision.)
    op.execute(f"CREATE PUBLICATION {PUBLICATION} FOR TABLE runs, specimens")


def downgrade() -> None:
    op.execute(f"DROP PUBLICATION IF EXISTS {PUBLICATION}")
    op.execute("ALTER TABLE specimens REPLICA IDENTITY DEFAULT")
    op.execute("ALTER TABLE runs REPLICA IDENTITY DEFAULT")
