"""Phase 2 D1 — compute daily indicators from base tables and upsert.

Indicators populated in this iteration:
- btc_close, mstr_close (denormalised anchors)
- btc_rv20 (Coinbase close-to-close, 365-annualised)
- btc_iv30 (Deribit DVOL daily-last / 100)
- btc_vrp (= btc_iv30 - btc_rv20)
- mstr_rv20 (yfinance close, 252-annualised)
- mnav (simple, current shares_out × close / treasury)

Left NULL for now — populated by D2-D3 once Polygon-derived MSTR IV30
is in place: mstr_iv30, beta_iv, equity_premium, regime.

Usage:
    python -m scripts.compute_indicators                # full history
    python -m scripts.compute_indicators --lookback 30  # last 30 days only
"""
from __future__ import annotations

import argparse
import logging
import sys
from datetime import date, timedelta

import pandas as pd
import yfinance as yf
from sqlalchemy import text

from core.db import make_sync_engine
from quant.indicators.btc_vrp import btc_vrp
from quant.indicators.mnav import mnav_simple
from quant.indicators.realized_vol import (
    CRYPTO_CALENDAR_DAYS,
    EQUITY_TRADING_DAYS,
    realised_vol,
)

logger = logging.getLogger(__name__)


def load_base_data(engine, start: date | None) -> dict[str, pd.DataFrame]:
    """Pull all base series we need, indexed by date."""
    where_btc = "WHERE source='coinbase'" + (f" AND date >= '{start}'" if start else "")
    where_eq = "WHERE ticker='MSTR'" + (f" AND ts >= '{start}'" if start else "")
    where_dvol = f"WHERE ts >= '{start}'" if start else ""
    where_hold = f"WHERE date >= '{start}'" if start else ""

    with engine.connect() as conn:
        btc = pd.read_sql(
            text(f"SELECT date, close FROM btc_ohlcv_daily {where_btc} ORDER BY date"),
            conn,
        )
        mstr = pd.read_sql(
            text(f"SELECT ts AS date, close FROM equity_ohlcv {where_eq} ORDER BY ts"),
            conn,
        )
        dvol = pd.read_sql(
            text(f"SELECT ts, dvol FROM btc_dvol {where_dvol} ORDER BY ts"), conn
        )
        # Holdings query intentionally ignores `start` so we can ffill back.
        # The forward-fill anchor must precede the requested window.
        holdings = pd.read_sql(
            text("SELECT date, btc_qty FROM mstr_btc_holdings ORDER BY date"), conn
        )

    btc["date"] = pd.to_datetime(btc["date"]).dt.date
    mstr["date"] = pd.to_datetime(mstr["date"]).dt.date
    dvol["ts"] = pd.to_datetime(dvol["ts"], utc=True)
    holdings["date"] = pd.to_datetime(holdings["date"]).dt.date

    return {
        "btc": btc.set_index("date")["close"].astype(float),
        "mstr": mstr.set_index("date")["close"].astype(float),
        "dvol": dvol.set_index("ts")["dvol"].astype(float),
        "holdings": holdings.set_index("date")["btc_qty"].astype(float),
    }


def fetch_mstr_shares_outstanding() -> float:
    """yfinance snapshot — see docstring of mnav.py for limitation."""
    info = yf.Ticker("MSTR").info
    shares = info.get("sharesOutstanding") or info.get("impliedSharesOutstanding")
    if not shares:
        raise RuntimeError("yfinance returned no sharesOutstanding for MSTR")
    return float(shares)


def compute_indicators(data: dict[str, pd.DataFrame], shares_out: float) -> pd.DataFrame:
    btc_close = data["btc"]
    mstr_close = data["mstr"]

    # IV30: DVOL last-of-day, % → decimal
    if not data["dvol"].empty:
        dvol_daily = data["dvol"].resample("1D").last().dropna()
        dvol_daily.index = dvol_daily.index.date
        btc_iv30 = (dvol_daily / 100.0).rename("btc_iv30")
    else:
        btc_iv30 = pd.Series(dtype=float, name="btc_iv30")

    btc_rv20 = realised_vol(btc_close, 20, CRYPTO_CALENDAR_DAYS).rename("btc_rv20")
    mstr_rv20 = realised_vol(mstr_close, 20, EQUITY_TRADING_DAYS).rename("mstr_rv20")
    vrp = btc_vrp(btc_iv30, btc_rv20).rename("btc_vrp")
    mnav = mnav_simple(mstr_close, btc_close, data["holdings"], shares_out).rename("mnav")

    out = pd.concat(
        [
            btc_close.rename("btc_close"),
            mstr_close.rename("mstr_close"),
            btc_rv20,
            btc_iv30,
            vrp,
            mstr_rv20,
            mnav,
        ],
        axis=1,
    ).sort_index()
    out.index.name = "date"
    # Drop rows where everything is NaN (no anchor at all)
    out = out.dropna(how="all")
    return out


def upsert_indicators(engine, df: pd.DataFrame) -> int:
    if df.empty:
        return 0
    rows = [
        {"date": idx, **{k: (None if pd.isna(v) else float(v)) for k, v in row.items()}}
        for idx, row in df.iterrows()
    ]
    sql = text("""
        INSERT INTO indicators_daily (
            date, btc_close, mstr_close,
            btc_rv20, btc_iv30, btc_vrp,
            mstr_rv20, mnav, updated_at
        ) VALUES (
            :date, :btc_close, :mstr_close,
            :btc_rv20, :btc_iv30, :btc_vrp,
            :mstr_rv20, :mnav, now()
        )
        ON CONFLICT (date) DO UPDATE SET
            btc_close = EXCLUDED.btc_close,
            mstr_close = EXCLUDED.mstr_close,
            btc_rv20 = EXCLUDED.btc_rv20,
            btc_iv30 = EXCLUDED.btc_iv30,
            btc_vrp = EXCLUDED.btc_vrp,
            mstr_rv20 = EXCLUDED.mstr_rv20,
            mnav = EXCLUDED.mnav,
            updated_at = now()
    """)
    with engine.begin() as conn:
        conn.execute(sql, rows)
    return len(rows)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--lookback",
        type=int,
        default=None,
        help="Only recompute the last N days (default: full history)",
    )
    parser.add_argument("--log-level", default="INFO")
    args = parser.parse_args()

    logging.basicConfig(
        level=getattr(logging, args.log_level.upper()),
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    engine = make_sync_engine()
    start = date.today() - timedelta(days=args.lookback) if args.lookback else None
    # RV20 needs ≥20 prior days, so widen the window for indicator math
    pull_start = (start - timedelta(days=40)) if start else None

    logger.info("loading base data (start=%s)", pull_start)
    data = load_base_data(engine, pull_start)
    logger.info(
        "btc=%d mstr=%d dvol=%d holdings=%d rows",
        len(data["btc"]), len(data["mstr"]), len(data["dvol"]), len(data["holdings"]),
    )

    shares_out = fetch_mstr_shares_outstanding()
    logger.info("MSTR shares_outstanding (snapshot): %.0f", shares_out)

    df = compute_indicators(data, shares_out)
    if start:
        df = df.loc[df.index >= start]
    logger.info("computed %d indicator rows; upserting", len(df))

    n = upsert_indicators(engine, df)
    print(f"  upserted {n} rows in indicators_daily")
    return 0


if __name__ == "__main__":
    sys.exit(main())
