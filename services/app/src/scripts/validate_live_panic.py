"""Historical validation of the Phase C1 live-panic trigger.

We don't have minute-bar history, so we proxy "intraday worst point"
with the daily LOW from equity_ohlcv.  For each day in the production
backtest:

  • equity_at_low ≈ prev_equity × (1 + mstr_weight × (low - prev_close) / prev_close)
  • dd_at_low     = equity_at_low / equity_peak − 1
  • dd_at_close   = today's close-based DD (the daily strategy's view)

Counts of interest:
  • live_trig_only : dd_at_low ≤ -15% AND dd_at_close > -15%
                    → live panic fires but daily disagrees = false alarm
  • both_trig      : dd_at_low ≤ -15% AND dd_at_close ≤ -15%
                    → live panic fires and daily confirms = correct + faster
  • daily_only     : dd_at_close ≤ -15% but low didn't breach
                    → would never happen (close worse than low is impossible)
  • neither        : safe day

If `live_trig_only / (live_trig_only + both_trig)` is small, the live
trigger is mostly a faster version of the daily panic.  If it's
large, the live trigger introduces too many whipsaws.

Run via:
    docker compose exec app python -m scripts.validate_live_panic
"""
from __future__ import annotations

import sys
from datetime import date

import pandas as pd
from sqlalchemy import text

from core.db import make_sync_engine
from quant.backtesting.data import assemble_full_panel
from quant.backtesting.engine import run_backtest
from quant.backtesting.strategies.macro_trend_v5 import (
    make_macro_trend_v5_with_breaker,
)

THRESHOLD = -0.15


def load_mstr_lows(engine, start: date) -> pd.Series:
    sql = text("""
        SELECT ts AS date, low, close
        FROM equity_ohlcv
        WHERE ticker = 'MSTR' AND ts >= :start
        ORDER BY ts
    """)
    with engine.connect() as conn:
        df = pd.read_sql(sql, conn, params={"start": start.isoformat()})
    df["date"] = pd.to_datetime(df["date"])
    return df.set_index("date")


def main() -> int:
    engine = make_sync_engine()
    panel, indicators = assemble_full_panel(engine)
    res = run_backtest(
        name="validate",
        panel=panel, indicators=indicators,
        strategy=make_macro_trend_v5_with_breaker(),
        cost_bps=10.0,
    )

    mstr = load_mstr_lows(engine, res.equity.index[0].date())
    # Strategy MSTR weight on each day
    if "MSTR" not in res.weights.columns:
        print("No MSTR weights in backtest!")
        return 1
    mstr_w = res.weights["MSTR"].reindex(res.equity.index).fillna(0.0)

    equity = res.equity
    peak = equity.cummax()
    dd_at_close = equity / peak - 1.0

    # Live DD proxy: equity at today's intraday low
    prev_close = mstr["close"].shift(1)
    pct_move_to_low = (mstr["low"] - prev_close) / prev_close
    pct_move_to_low = pct_move_to_low.reindex(equity.index).fillna(0.0)

    prev_eq = equity.shift(1)
    eq_at_low = prev_eq * (1.0 + mstr_w.shift(1).fillna(0.0) * pct_move_to_low)
    dd_at_low = eq_at_low / peak - 1.0

    # Classify each day
    live_trig = dd_at_low <= THRESHOLD
    daily_trig = dd_at_close <= THRESHOLD

    live_only = (live_trig & ~daily_trig).sum()
    both = (live_trig & daily_trig).sum()
    daily_only = (daily_trig & ~live_trig).sum()
    neither = (~live_trig & ~daily_trig).sum()
    total = len(equity)

    print(f"backtest window: {equity.index[0].date()} → {equity.index[-1].date()}")
    print(f"days total       : {total}")
    print()
    print(f"live + daily AGREE  : {both:>5}  ({both/total*100:.1f}%) — live = faster panic, correct")
    print(f"live trig only      : {live_only:>5}  ({live_only/total*100:.1f}%) — false alarms")
    print(f"daily only          : {daily_only:>5}  ({daily_only/total*100:.1f}%) — live missed (impossible)")
    print(f"neither             : {neither:>5}  ({neither/total*100:.1f}%) — safe days")
    print()
    trig_days = live_only + both
    if trig_days > 0:
        fa_rate = live_only / trig_days
        print(f"live false-alarm rate: {fa_rate*100:.1f}%")
        if fa_rate < 0.10:
            print("→ LOW. Live trigger is a faithful faster version of daily panic.")
        elif fa_rate < 0.30:
            print("→ MEDIUM. Some whipsaw risk; sustained req (15 min) should mitigate.")
        else:
            print("→ HIGH. Live trigger may cause meaningful whipsaw.  Reconsider scope.")
    else:
        print("(no panic-triggering days in this backtest window)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
