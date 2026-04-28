"""add roc_pct to distributions for YieldMax classification breakdown

Existing `classification` (TEXT) collapses ROC vs ordinary into a single
dominant label. `roc_pct` (NUMERIC, 0-100) preserves the full breakdown
that YieldMax publishes per distribution — needed for accurate after-tax
yield calculations in Phase 2-3.

Revision ID: 004
Revises: 003
Create Date: 2026-04-28

"""
from alembic import op

revision = "004"
down_revision = "003"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("ALTER TABLE distributions ADD COLUMN roc_pct NUMERIC")


def downgrade() -> None:
    op.execute("ALTER TABLE distributions DROP COLUMN IF EXISTS roc_pct")
