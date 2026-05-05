"""Backtest data layer — assemble price + indicator panel for MSTR family.

Real ticker availability (from yfinance, set in equity_ohlcv):
    MSTR : 1998-…    ← long history
    MSTU : 2024-04-… ← 2x long ETF (T-Rex 2X Long MSTR)
    MSTY : 2024-02-… ← YieldMax MSTR Option Income ETF
    MSTZ : 2024-04-… ← T-Rex 2X Inverse MSTR

For backtests longer than the leveraged-ETF history we synthesise the
missing tickers from MSTR daily returns:

    MSTU_synth(t) = MSTU_synth(t-1) · (1 + 2 · r_MSTR(t) − fee_drag(t))
    MSTZ_synth(t) = MSTZ_synth(t-1) · (1 + (-2) · r_MSTR(t) − fee_drag(t))

The fee_drag captures borrow + management cost (~0.95 % p.a.) AND the
volatility decay specific to daily-rebalanced leveraged products.
fee_drag is approximated as a small daily haircut, which is enough for
*relative* strategy comparison; it under-states the true vol drag in
extreme-vol regimes (use real MSTU/MSTZ for the live window).

MSTY is harder — option income ETFs aren't replicable from underlying
returns alone. We anchor synthetic MSTY to MSTR with a covered-call
overlay approximation (MSTR return capped at +k % monthly, plus a flat
dividend yield equal to the historical MSTY distribution rate). This
under-represents tail upside but matches MSTY's realised behaviour in
the live window reasonably well.
"""
from __future__ import annotations

import logging
from datetime import date

import numpy as np
import pandas as pd
from sqlalchemy import text
from sqlalchemy.engine import Engine

logger = logging.getLogger(__name__)

REAL_TICKERS = ("MSTR", "MSTU", "MSTY", "MSTZ")
LEVERAGED_FEE_BPS_PER_YEAR = 95.0      # ~0.95 % p.a. for 2× ETFs
TRADING_DAYS = 252


def _daily_drag(annual_bps: float) -> float:
    return (annual_bps / 1e4) / TRADING_DAYS


def load_real_prices(engine: Engine) -> pd.DataFrame:
    """Wide DataFrame: date × ticker, daily close, real data only."""
    sql = text(f"""
        SELECT ts AS date, ticker, close
        FROM equity_ohlcv
        WHERE ticker IN ({",".join(f"'{t}'" for t in REAL_TICKERS)})
        ORDER BY date
    """)
    with engine.connect() as conn:
        df = pd.read_sql(sql, conn)
    df["date"] = pd.to_datetime(df["date"])
    return df.pivot(index="date", columns="ticker", values="close").astype(float)


def synthesise_leveraged(
    mstr_close: pd.Series,
    leverage: float,
    fee_bps: float = LEVERAGED_FEE_BPS_PER_YEAR,
) -> pd.Series:
    """Compound daily-leveraged returns from MSTR.

    Note: this is the canonical model for daily-rebalanced leveraged
    ETFs (MSTU, MSTZ, TQQQ, …). It captures the **path-dependent**
    drag that hurts these instruments in choppy markets.
    """
    rets = mstr_close.pct_change().fillna(0.0)
    drag = _daily_drag(fee_bps)
    levered = (1.0 + leverage * rets - drag).clip(lower=0.0)  # protect from total loss
    out = levered.cumprod() * mstr_close.iloc[0]
    out.iloc[0] = mstr_close.iloc[0]
    return out


def synthesise_msty(
    mstr_close: pd.Series,
    monthly_cap_pct: float = 0.06,       # cap underlying upside @ +6 %/mo
    annual_dist_yield: float = 0.80,     # ~80 % gross yield in live data
) -> pd.Series:
    """Approximate MSTR Option Income ETF.

    Model:
        - Daily return = clipped underlying return (mimics covered-call cap)
        - + flat daily distribution accrual

    This captures MSTY's three salient features:
        1. Strong upside cap → underperforms MSTR in rallies
        2. Slightly cushioned downside (premium offsets some loss)
        3. Steady distribution yield ≈ 60-80 % of NAV annualised

    Calibration note: the live-window match is decent but coverage is
    only 2024-02 →; treat synthetic MSTY before that as indicative only.
    """
    rets = mstr_close.pct_change().fillna(0.0)
    daily_cap = monthly_cap_pct / 21.0
    daily_floor = -monthly_cap_pct / 21.0 * 0.7      # asymmetric: less downside cushion
    dist_daily = annual_dist_yield / TRADING_DAYS
    capped = rets.clip(lower=daily_floor, upper=daily_cap) + dist_daily
    out = (1.0 + capped).clip(lower=0.0).cumprod() * mstr_close.iloc[0]
    out.iloc[0] = mstr_close.iloc[0]
    return out


def build_synth_panel(
    mstr_close: pd.Series,
    real_prices: pd.DataFrame | None = None,
) -> pd.DataFrame:
    """Build a date × ticker price panel using real data where available
    and synthetic fills before each ticker's launch."""
    panel = pd.DataFrame(index=mstr_close.index)
    panel["MSTR"] = mstr_close

    panel["MSTU"] = synthesise_leveraged(mstr_close, leverage=2.0)
    panel["MSTZ"] = synthesise_leveraged(mstr_close, leverage=-2.0)
    panel["MSTY"] = synthesise_msty(mstr_close)

    # Splice in real data over the synthetic where available
    if real_prices is not None:
        for t in ("MSTU", "MSTY", "MSTZ"):
            if t not in real_prices.columns:
                continue
            real = real_prices[t].dropna()
            if real.empty:
                continue
            # Anchor: scale real series so it joins the synthetic seamlessly
            launch = real.index.min()
            if launch in panel.index:
                synth_at_launch = panel.loc[launch, t]
                if synth_at_launch and not pd.isna(synth_at_launch):
                    scale = synth_at_launch / real.iloc[0]
                    panel.loc[real.index, t] = real * scale
    return panel


def load_indicators(engine: Engine) -> pd.DataFrame:
    """Date-indexed wide DataFrame of all macro indicators we computed."""
    sql = text("""
        SELECT date, btc_close, mstr_close,
               btc_rv20, btc_iv30, btc_vrp,
               mstr_rv20, mstr_iv30,
               beta_iv, equity_premium,
               mnav, regime
        FROM indicators_daily
        ORDER BY date
    """)
    with engine.connect() as conn:
        df = pd.read_sql(sql, conn)
    df["date"] = pd.to_datetime(df["date"])
    return df.set_index("date")


def add_technical_indicators(panel: pd.DataFrame) -> pd.DataFrame:
    """Compute MAs and a rolling MAX on each ticker's close.

    Returns a MultiIndex-columns DataFrame with columns
        (ticker, indicator)  where indicator ∈ {close, MA25, MA50, MA200, MAX}.
    """
    out = {}
    for t in panel.columns:
        s = panel[t].astype(float)
        out[(t, "close")] = s
        out[(t, "MA25")] = s.rolling(25, min_periods=1).mean()
        out[(t, "MA50")] = s.rolling(50, min_periods=1).mean()
        out[(t, "MA200")] = s.rolling(200, min_periods=1).mean()
        out[(t, "MAX")] = s.cummax()
    return pd.concat(out, axis=1).sort_index(axis=1)


def assemble_full_panel(engine: Engine) -> tuple[pd.DataFrame, pd.DataFrame]:
    """One-stop loader: returns (panel, indicators) ready for run_backtest.

    `panel.columns` is MultiIndex (ticker, indicator) where indicator
    includes 'close', 'MA25', 'MA200', 'MAX'.

    `indicators` is a date-indexed wide DataFrame of macro features
    (mnav, btc_vrp, beta_iv, equity_premium, …).
    """
    real = load_real_prices(engine)
    if "MSTR" not in real.columns:
        raise RuntimeError("No MSTR in equity_ohlcv — backfill first.")
    mstr = real["MSTR"].dropna()
    prices = build_synth_panel(mstr, real)
    panel = add_technical_indicators(prices)
    indicators = load_indicators(engine).reindex(panel.index).ffill(limit=3)
    return panel, indicators
