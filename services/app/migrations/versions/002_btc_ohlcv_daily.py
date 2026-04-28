"""add btc_ohlcv_daily

Daily-resolution BTC OHLCV table. Separate from btc_ohlcv_1m so that
RV calculations and minute-bar microstructure live in distinct stores.

Revision ID: 002
Revises: 001
Create Date: 2026-04-28

"""
from alembic import op

revision = "002"
down_revision = "001"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("""
        CREATE TABLE btc_ohlcv_daily (
            date        DATE NOT NULL,
            source      TEXT NOT NULL,
            open        NUMERIC NOT NULL,
            high        NUMERIC NOT NULL,
            low         NUMERIC NOT NULL,
            close       NUMERIC NOT NULL,
            volume      NUMERIC NOT NULL,
            ingested_at TIMESTAMPTZ NOT NULL DEFAULT now(),
            PRIMARY KEY (source, date)
        )
    """)
    op.execute(
        "SELECT create_hypertable('btc_ohlcv_daily', 'date', "
        "chunk_time_interval => INTERVAL '1 year')"
    )


def downgrade() -> None:
    op.execute("DROP TABLE IF EXISTS btc_ohlcv_daily CASCADE")
