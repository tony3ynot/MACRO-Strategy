"""initial schema: 9 hypertables + reference + audit

Revision ID: 001
Revises:
Create Date: 2026-04-27

"""
from alembic import op

revision = "001"
down_revision = None
branch_labels = None
depends_on = None


def upgrade() -> None:
    # ─── Equity OHLCV ─────────────────────────────────────────────
    op.execute("""
        CREATE TABLE equity_ohlcv (
            ticker      TEXT NOT NULL,
            ts          DATE NOT NULL,
            open        NUMERIC NOT NULL,
            high        NUMERIC NOT NULL,
            low         NUMERIC NOT NULL,
            close       NUMERIC NOT NULL,
            adj_close   NUMERIC NOT NULL,
            volume      BIGINT  NOT NULL,
            source      TEXT    NOT NULL,
            ingested_at TIMESTAMPTZ NOT NULL DEFAULT now(),
            PRIMARY KEY (ticker, ts)
        )
    """)
    op.execute(
        "SELECT create_hypertable('equity_ohlcv', 'ts', "
        "chunk_time_interval => INTERVAL '1 year')"
    )

    # ─── Distributions (dividends, ROC, splits) ──────────────────
    op.execute("""
        CREATE TABLE distributions (
            ticker         TEXT NOT NULL,
            ex_date        DATE NOT NULL,
            pay_date       DATE,
            amount         NUMERIC NOT NULL,
            type           TEXT NOT NULL,
            classification TEXT,
            source         TEXT NOT NULL,
            ingested_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
            PRIMARY KEY (ticker, ex_date, type)
        )
    """)

    # ─── MSTR options chain (heavy → compressed after 60d) ───────
    op.execute("""
        CREATE TABLE options_chain (
            underlying  TEXT NOT NULL,
            ts          DATE NOT NULL,
            expiry      DATE NOT NULL,
            strike      NUMERIC NOT NULL,
            type        CHAR(1) NOT NULL,
            bid         NUMERIC,
            ask         NUMERIC,
            last        NUMERIC,
            iv          NUMERIC,
            delta       NUMERIC,
            gamma       NUMERIC,
            vega        NUMERIC,
            theta       NUMERIC,
            oi          BIGINT,
            volume      BIGINT,
            source      TEXT NOT NULL,
            ingested_at TIMESTAMPTZ NOT NULL DEFAULT now(),
            PRIMARY KEY (underlying, ts, expiry, strike, type)
        )
    """)
    op.execute(
        "SELECT create_hypertable('options_chain', 'ts', "
        "chunk_time_interval => INTERVAL '3 months')"
    )
    op.execute("""
        ALTER TABLE options_chain SET (
            timescaledb.compress,
            timescaledb.compress_segmentby = 'underlying,expiry,type'
        )
    """)
    op.execute(
        "SELECT add_compression_policy('options_chain', INTERVAL '60 days')"
    )

    # ─── BTC OHLCV 1m (compressed after 7d) ──────────────────────
    op.execute("""
        CREATE TABLE btc_ohlcv_1m (
            ts     TIMESTAMPTZ NOT NULL,
            source TEXT NOT NULL,
            open   NUMERIC NOT NULL,
            high   NUMERIC NOT NULL,
            low    NUMERIC NOT NULL,
            close  NUMERIC NOT NULL,
            volume NUMERIC NOT NULL,
            PRIMARY KEY (source, ts)
        )
    """)
    op.execute(
        "SELECT create_hypertable('btc_ohlcv_1m', 'ts', "
        "chunk_time_interval => INTERVAL '1 month')"
    )
    op.execute("""
        ALTER TABLE btc_ohlcv_1m SET (
            timescaledb.compress,
            timescaledb.compress_segmentby = 'source'
        )
    """)
    op.execute(
        "SELECT add_compression_policy('btc_ohlcv_1m', INTERVAL '7 days')"
    )

    # ─── Deribit BTC options ─────────────────────────────────────
    op.execute("""
        CREATE TABLE deribit_options (
            ts          TIMESTAMPTZ NOT NULL,
            expiry      DATE NOT NULL,
            strike      NUMERIC NOT NULL,
            type        CHAR(1) NOT NULL,
            mark_price  NUMERIC,
            mark_iv     NUMERIC,
            delta       NUMERIC,
            gamma       NUMERIC,
            vega        NUMERIC,
            oi          NUMERIC,
            ingested_at TIMESTAMPTZ NOT NULL DEFAULT now(),
            PRIMARY KEY (ts, expiry, strike, type)
        )
    """)
    op.execute(
        "SELECT create_hypertable('deribit_options', 'ts', "
        "chunk_time_interval => INTERVAL '1 month')"
    )

    # ─── BTC DVOL ────────────────────────────────────────────────
    op.execute("""
        CREATE TABLE btc_dvol (
            ts   TIMESTAMPTZ NOT NULL PRIMARY KEY,
            dvol NUMERIC NOT NULL
        )
    """)
    op.execute(
        "SELECT create_hypertable('btc_dvol', 'ts', "
        "chunk_time_interval => INTERVAL '6 months')"
    )

    # ─── Crypto perp funding/OI ──────────────────────────────────
    op.execute("""
        CREATE TABLE crypto_perp_funding (
            venue        TEXT NOT NULL,
            symbol       TEXT NOT NULL,
            ts           TIMESTAMPTZ NOT NULL,
            funding_rate NUMERIC,
            mark_price   NUMERIC,
            oi_usd       NUMERIC,
            PRIMARY KEY (venue, symbol, ts)
        )
    """)
    op.execute(
        "SELECT create_hypertable('crypto_perp_funding', 'ts', "
        "chunk_time_interval => INTERVAL '1 month')"
    )

    # ─── MSTR fundamentals ───────────────────────────────────────
    op.execute("""
        CREATE TABLE mstr_btc_holdings (
            date               DATE PRIMARY KEY,
            btc_qty            NUMERIC NOT NULL,
            cumulative_cost    NUMERIC,
            last_purchase_date DATE,
            source_filing      TEXT,
            ingested_at        TIMESTAMPTZ NOT NULL DEFAULT now()
        )
    """)

    op.execute("""
        CREATE TABLE mstr_capital_structure (
            date                 DATE PRIMARY KEY,
            shares_out           BIGINT,
            total_debt           NUMERIC,
            convertibles_face    NUMERIC,
            conversion_price_avg NUMERIC,
            source_filing        TEXT,
            ingested_at          TIMESTAMPTZ NOT NULL DEFAULT now()
        )
    """)

    # ─── Reference: market calendar ──────────────────────────────
    op.execute("""
        CREATE TABLE market_calendar (
            date      DATE NOT NULL,
            market    TEXT NOT NULL,
            is_open   BOOLEAN NOT NULL,
            open_utc  TIMESTAMPTZ,
            close_utc TIMESTAMPTZ,
            PRIMARY KEY (date, market)
        )
    """)

    # ─── Audit: ingestion runs ───────────────────────────────────
    op.execute("""
        CREATE TABLE ingestion_runs (
            id            BIGSERIAL PRIMARY KEY,
            source        TEXT NOT NULL,
            mode          TEXT NOT NULL,
            started_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
            ended_at      TIMESTAMPTZ,
            status        TEXT NOT NULL,
            rows_ingested BIGINT,
            error         TEXT,
            metadata      JSONB
        )
    """)
    op.execute(
        "CREATE INDEX ix_ingestion_runs_source_started "
        "ON ingestion_runs (source, started_at DESC)"
    )


def downgrade() -> None:
    op.execute("DROP TABLE IF EXISTS ingestion_runs CASCADE")
    op.execute("DROP TABLE IF EXISTS market_calendar CASCADE")
    op.execute("DROP TABLE IF EXISTS mstr_capital_structure CASCADE")
    op.execute("DROP TABLE IF EXISTS mstr_btc_holdings CASCADE")
    op.execute("DROP TABLE IF EXISTS crypto_perp_funding CASCADE")
    op.execute("DROP TABLE IF EXISTS btc_dvol CASCADE")
    op.execute("DROP TABLE IF EXISTS deribit_options CASCADE")
    op.execute("DROP TABLE IF EXISTS btc_ohlcv_1m CASCADE")
    op.execute("DROP TABLE IF EXISTS options_chain CASCADE")
    op.execute("DROP TABLE IF EXISTS distributions CASCADE")
    op.execute("DROP TABLE IF EXISTS equity_ohlcv CASCADE")
