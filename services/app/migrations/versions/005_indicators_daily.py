"""Phase 2 indicators_daily — one row per date, all indicators as columns.

Single wide table is intentional: indicators are computed in lockstep from
the same base tables, queried together for the daily briefing, and small
enough (~3000 rows over 8y) that columnar layout per row is fine.

Forward-compatible: columns that depend on data we don't yet compute
(mstr_iv30, beta_iv, equity_premium, regime) are nullable so D1 can
populate the BTC-side and mNAV slice without blocking on Polygon-derived
work.

Revision ID: 005
Revises: 004
Create Date: 2026-04-29

"""
from alembic import op

revision = "005"
down_revision = "004"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("""
        CREATE TABLE indicators_daily (
            date            DATE PRIMARY KEY,

            -- anchor prices (denormalised for fast join-free briefings)
            btc_close       NUMERIC,
            mstr_close      NUMERIC,

            -- BTC vol surface
            btc_rv20        NUMERIC,
            btc_iv30        NUMERIC,
            btc_vrp         NUMERIC,

            -- MSTR vol surface (rv now, iv after Polygon-driven D2-D3)
            mstr_rv20       NUMERIC,
            mstr_iv30       NUMERIC,

            -- IV decomposition: mstr_iv30 ≈ beta_iv * btc_iv30 + equity_premium
            beta_iv         NUMERIC,
            equity_premium  NUMERIC,

            -- mNAV = mcap / (btc_qty * btc_close)
            -- Phase 2 D1 uses approximate shares_out (yfinance snapshot);
            -- Phase 2.5 will swap to historical SEC 10-Q values.
            mnav            NUMERIC,

            -- regime label (assigned by classifier in D5+)
            regime          TEXT,

            updated_at      TIMESTAMPTZ NOT NULL DEFAULT now()
        )
    """)
    op.execute(
        "SELECT create_hypertable('indicators_daily', 'date', "
        "chunk_time_interval => INTERVAL '1 year')"
    )


def downgrade() -> None:
    op.execute("DROP TABLE IF EXISTS indicators_daily CASCADE")
