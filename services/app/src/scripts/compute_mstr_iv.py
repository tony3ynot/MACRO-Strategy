"""Phase 2 D2 — compute MSTR IV30 from Polygon options + UPSERT into
indicators_daily.mstr_iv30.

Independent of compute_indicators (D1) because this depends on Polygon
data and may want to run on a different cadence (after each Polygon
ingest).

Usage:
    python -m scripts.compute_mstr_iv                # full Polygon range
    python -m scripts.compute_mstr_iv --lookback 30  # last 30 days only
"""
from __future__ import annotations

import argparse
import logging
import sys
from datetime import date, timedelta

import pandas as pd
from sqlalchemy import text

from core.db import make_sync_engine
from quant.indicators.mstr_iv import compute_mstr_iv30_daily
from quant.risk_free import fetch_dgs1mo_series

logger = logging.getLogger(__name__)


def load_options_close(engine, start: date | None) -> pd.DataFrame:
    where = "WHERE underlying='MSTR' AND last IS NOT NULL"
    if start:
        where += f" AND ts >= '{start}'"
    sql = text(f"SELECT ts, expiry, strike, type, last FROM options_chain {where}")
    with engine.connect() as conn:
        df = pd.read_sql(sql, conn)
    df["strike"] = df["strike"].astype(float)
    df["last"] = df["last"].astype(float)
    return df


def load_mstr_close(engine, start: date | None) -> pd.Series:
    where = "WHERE ticker='MSTR'" + (f" AND ts >= '{start}'" if start else "")
    sql = text(f"SELECT ts, close FROM equity_ohlcv {where} ORDER BY ts")
    with engine.connect() as conn:
        df = pd.read_sql(sql, conn)
    df["ts"] = pd.to_datetime(df["ts"]).dt.date
    return df.set_index("ts")["close"].astype(float)


def upsert_mstr_iv30(engine, series: pd.Series) -> int:
    if series.empty:
        return 0
    rows = [{"date": d, "mstr_iv30": float(v)} for d, v in series.items() if pd.notna(v)]
    sql = text("""
        INSERT INTO indicators_daily (date, mstr_iv30, updated_at)
        VALUES (:date, :mstr_iv30, now())
        ON CONFLICT (date) DO UPDATE SET
            mstr_iv30  = EXCLUDED.mstr_iv30,
            updated_at = now()
    """)
    with engine.begin() as conn:
        conn.execute(sql, rows)
    return len(rows)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--lookback", type=int, default=None)
    parser.add_argument("--log-level", default="INFO")
    args = parser.parse_args()

    logging.basicConfig(
        level=getattr(logging, args.log_level.upper()),
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    engine = make_sync_engine()
    start = date.today() - timedelta(days=args.lookback) if args.lookback else None

    logger.info("loading options + MSTR close (start=%s)", start)
    options = load_options_close(engine, start)
    mstr_close = load_mstr_close(engine, start)
    logger.info("options=%d rows  mstr_close=%d days", len(options), len(mstr_close))

    rates = fetch_dgs1mo_series()
    logger.info(
        "FRED DGS1MO: %d rows (%s → %s, latest=%.4f)",
        len(rates), rates.index.min(), rates.index.max(), rates.iloc[-1],
    )

    iv30 = compute_mstr_iv30_daily(options, mstr_close, rates)
    logger.info("computed IV30 on %d dates; upserting", len(iv30))

    n = upsert_mstr_iv30(engine, iv30)
    print(f"  upserted {n} rows in indicators_daily.mstr_iv30")
    return 0


if __name__ == "__main__":
    sys.exit(main())
