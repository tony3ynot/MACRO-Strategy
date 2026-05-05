"""Phase 2 D3 — fit β(t) and EquityPremium(t) from MSTR_IV30 / BTC_IV30.

Runs lag analysis at the start, picks the lag with peak correlation,
fits rolling OLS at that lag, UPSERTs `beta_iv` and `equity_premium`
columns of indicators_daily.

Usage:
    python -m scripts.compute_iv_decomposition
    python -m scripts.compute_iv_decomposition --window 90 --lag 1
"""
from __future__ import annotations

import argparse
import logging
import sys

import pandas as pd
from sqlalchemy import text

from core.db import make_sync_engine
from quant.indicators.iv_decomposition import (
    DEFAULT_WINDOW,
    best_lag,
    compute_decomposition,
)

logger = logging.getLogger(__name__)


def load_paired_iv(engine) -> tuple[pd.Series, pd.Series]:
    sql = text("""
        SELECT date, mstr_iv30, btc_iv30
        FROM indicators_daily
        WHERE mstr_iv30 IS NOT NULL OR btc_iv30 IS NOT NULL
        ORDER BY date
    """)
    with engine.connect() as conn:
        df = pd.read_sql(sql, conn)
    df["date"] = pd.to_datetime(df["date"]).dt.date
    df = df.set_index("date").astype(float)
    return df["mstr_iv30"], df["btc_iv30"]


def upsert_decomposition(engine, df: pd.DataFrame) -> int:
    rows = [
        {
            "date": idx,
            "beta_iv": (None if pd.isna(r["beta_iv"]) else float(r["beta_iv"])),
            "equity_premium": (
                None if pd.isna(r["equity_premium"]) else float(r["equity_premium"])
            ),
        }
        for idx, r in df.iterrows()
    ]
    if not rows:
        return 0
    sql = text("""
        INSERT INTO indicators_daily (date, beta_iv, equity_premium, updated_at)
        VALUES (:date, :beta_iv, :equity_premium, now())
        ON CONFLICT (date) DO UPDATE SET
            beta_iv        = EXCLUDED.beta_iv,
            equity_premium = EXCLUDED.equity_premium,
            updated_at     = now()
    """)
    with engine.begin() as conn:
        conn.execute(sql, rows)
    return len(rows)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--window", type=int, default=DEFAULT_WINDOW)
    parser.add_argument(
        "--lag",
        type=int,
        default=None,
        help="Force a specific lag; default auto-picks max-correlation lag.",
    )
    parser.add_argument("--log-level", default="INFO")
    args = parser.parse_args()

    logging.basicConfig(
        level=getattr(logging, args.log_level.upper()),
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    engine = make_sync_engine()
    mstr_iv, btc_iv = load_paired_iv(engine)
    paired = pd.concat([mstr_iv, btc_iv], axis=1).dropna()
    logger.info("paired sample: %d days (%s → %s)",
                len(paired), paired.index.min(), paired.index.max())

    if args.lag is None:
        chosen_lag, lag_corrs = best_lag(mstr_iv, btc_iv)
        logger.info("lag correlations: %s",
                    {k: round(v, 4) for k, v in sorted(lag_corrs.items())})
        logger.info("chosen lag: %d (ρ=%.4f)", chosen_lag, lag_corrs[chosen_lag])
    else:
        chosen_lag = args.lag
        logger.info("forced lag: %d", chosen_lag)

    decomp = compute_decomposition(mstr_iv, btc_iv, lag=chosen_lag, window=args.window)
    valid = decomp.dropna(subset=["beta_iv", "equity_premium"], how="all")
    logger.info("decomposition: %d rows with at least one fitted value",
                len(valid))

    n = upsert_decomposition(engine, valid)
    print(f"  upserted {n} rows: lag={chosen_lag}, window={args.window}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
