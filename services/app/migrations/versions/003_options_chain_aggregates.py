"""extend options_chain with full Polygon aggregate fields

Adds open/high/low/vwap/transactions so we capture every field the
Polygon Aggregates endpoint returns (o, h, l, c, v, vw, n). The existing
`last` column continues to hold close (c) for compatibility.

Revision ID: 003
Revises: 002
Create Date: 2026-04-28

"""
from alembic import op

revision = "003"
down_revision = "002"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("""
        ALTER TABLE options_chain
            ADD COLUMN open         NUMERIC,
            ADD COLUMN high         NUMERIC,
            ADD COLUMN low          NUMERIC,
            ADD COLUMN vwap         NUMERIC,
            ADD COLUMN transactions INTEGER
    """)


def downgrade() -> None:
    op.execute("""
        ALTER TABLE options_chain
            DROP COLUMN IF EXISTS open,
            DROP COLUMN IF EXISTS high,
            DROP COLUMN IF EXISTS low,
            DROP COLUMN IF EXISTS vwap,
            DROP COLUMN IF EXISTS transactions
    """)
