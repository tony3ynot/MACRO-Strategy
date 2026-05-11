"""Phase 2 intraday — sub-daily prices for equities (yfinance) and BTC (Coinbase).

Single unified table for all tickers (MSTR, MSTU, MSTY, MSTZ, BTC).
Used by the intraday alert engine: big-move detection, mNAV bucket
crossings, pre-close panic warnings.

We deliberately store *just close + minute timestamp*, not full OHLCV.
For our alerts we only need point-in-time price; storing high/low/volume
per minute would double the row size for no analytic value at this
phase.  If we later need bars for charting, we can backfill from
yfinance / Coinbase candles into a separate _1m table.

Chunked at 7 days — minutes accumulate fast (~7k rows/day across 5
tickers).

Revision ID: 006
Revises: 005
Create Date: 2026-05-11
"""
from alembic import op

revision = "006"
down_revision = "005"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("""
        CREATE TABLE intraday_prices (
            ticker      TEXT NOT NULL,
            ts          TIMESTAMPTZ NOT NULL,
            close       NUMERIC NOT NULL,
            source      TEXT NOT NULL,
            ingested_at TIMESTAMPTZ NOT NULL DEFAULT now(),
            PRIMARY KEY (ticker, ts)
        )
    """)
    op.execute(
        "SELECT create_hypertable('intraday_prices', 'ts', "
        "chunk_time_interval => INTERVAL '7 days')"
    )
    op.execute("CREATE INDEX intraday_prices_ticker_ts_desc ON intraday_prices (ticker, ts DESC)")


def downgrade() -> None:
    op.execute("DROP TABLE IF EXISTS intraday_prices CASCADE")
