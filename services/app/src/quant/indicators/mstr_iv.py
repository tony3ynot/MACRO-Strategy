"""MSTR ATM IV30 — one number per date, derived from Polygon EOD options.

Pipeline:
1. For each (date, expiry), pick the nearest-to-money call and put by
   |ln(K / S)|. Average their inverted IVs ⇒ one ATM IV per expiry.
2. Per date, linearly interpolate ATM IVs in *variance space* to 30 DTE:
       σ_target² · T_target = (1-w)·σ_1²·T1 + w·σ_2²·T2,
   where T1 < T_target < T2 and w = (T_target - T1)/(T2 - T1).
3. If 30 DTE is outside the available expiry range we fall back to the
   nearest expiry's IV (no extrapolation — too unreliable on the wings).

Notes / limitations:
- We invert from the day's *close* price, not bid-ask mid (Polygon Basic
  doesn't publish quotes). On thin contracts the close can be stale and
  the inverted IV is junk; the bracket check in `implied_vol` returns
  NaN for unbracketed prices and we drop NaNs before averaging, which
  filters most of the mess.
- Risk-free rate sourced from FRED DGS1MO.
- Dividend yield assumed 0 (MSTR pays none).
"""
from __future__ import annotations

import logging
import math
from datetime import date

import numpy as np
import pandas as pd

from quant.blackscholes import implied_vol

logger = logging.getLogger(__name__)

TARGET_DTE = 30
MIN_DTE = 5            # discard sub-week — gamma noise dominates IV inversion
MAX_DTE = 120          # discard very long — our listing window only ~45 DTE anyway
ATM_LOG_MONEYNESS = 0.10  # only use strikes within ±10 % of spot for ATM averaging


def _atm_iv_per_expiry(
    snapshot: pd.DataFrame, spot: float, r: float,
) -> pd.DataFrame:
    """For one trade date, return one ATM IV per expiry."""
    snapshot = snapshot.copy()
    snapshot["log_money"] = np.log(snapshot["strike"].astype(float) / spot)
    snapshot = snapshot.loc[snapshot["log_money"].abs() <= ATM_LOG_MONEYNESS]
    if snapshot.empty:
        return pd.DataFrame()

    out = []
    for expiry, grp in snapshot.groupby("expiry"):
        T = (expiry - snapshot["ts"].iloc[0]).days / 365.0
        if not (MIN_DTE / 365.0 <= T <= MAX_DTE / 365.0):
            continue
        # nearest-to-money call and put
        ivs = []
        for opt_type in ("C", "P"):
            sub = grp.loc[grp["type"] == opt_type]
            if sub.empty:
                continue
            row = sub.iloc[sub["log_money"].abs().argmin()]
            iv = implied_vol(
                price=float(row["last"]),
                S=spot,
                K=float(row["strike"]),
                T=T,
                r=r,
                option_type=opt_type,
            )
            if not math.isnan(iv) and 0.05 < iv < 4.0:
                ivs.append(iv)
        if ivs:
            out.append({"expiry": expiry, "T": T, "iv": float(np.mean(ivs))})
    return pd.DataFrame(out)


def _interp_iv30(per_expiry: pd.DataFrame) -> float | None:
    """Linear interp in variance space between expiries straddling 30 DTE."""
    if per_expiry.empty:
        return None
    target_T = TARGET_DTE / 365.0
    df = per_expiry.sort_values("T").reset_index(drop=True)

    if target_T <= df["T"].iloc[0]:
        return float(df["iv"].iloc[0])
    if target_T >= df["T"].iloc[-1]:
        return float(df["iv"].iloc[-1])

    upper = df.loc[df["T"] >= target_T].iloc[0]
    lower = df.loc[df["T"] < target_T].iloc[-1]
    w = (target_T - lower["T"]) / (upper["T"] - lower["T"])
    var_target = (1.0 - w) * lower["iv"] ** 2 * lower["T"] + w * upper["iv"] ** 2 * upper["T"]
    return float(math.sqrt(var_target / target_T))


def compute_mstr_iv30_daily(
    options: pd.DataFrame,
    mstr_close: pd.Series,
    rates: pd.Series,
) -> pd.Series:
    """Return date-indexed IV30 series.

    `options`: DataFrame with columns [ts, expiry, strike, type, last]
    `mstr_close`: date-indexed Series of MSTR close prices
    `rates`: date-indexed Series of decimal risk-free rates
    """
    if options.empty:
        return pd.Series(dtype=float, name="mstr_iv30")

    options = options.copy()
    options["ts"] = pd.to_datetime(options["ts"]).dt.date
    options["expiry"] = pd.to_datetime(options["expiry"]).dt.date

    out: dict[date, float] = {}
    for ts, day in options.groupby("ts"):
        spot = mstr_close.get(ts)
        if spot is None or pd.isna(spot):
            continue
        r = rates.get(ts)
        if r is None or pd.isna(r):
            continue
        per_expiry = _atm_iv_per_expiry(day, float(spot), float(r))
        iv30 = _interp_iv30(per_expiry)
        if iv30 is not None:
            out[ts] = iv30

    return pd.Series(out, name="mstr_iv30").sort_index()
