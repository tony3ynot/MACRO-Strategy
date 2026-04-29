"""BTC variance-risk-premium = IV30 − RV20.

Notes:
- Deribit DVOL is already a 30-day forward-looking IV expressed as a
  percentage (e.g. 65 = 65 % annualised). We divide by 100 to match
  RV's decimal scale before subtracting.
- We resample DVOL to daily by taking the *last* observation of the UTC
  day. DVOL ticks are sub-hourly, but for daily indicators the close is
  the canonical anchor.
"""
from __future__ import annotations

import pandas as pd


def btc_vrp(iv30_daily: pd.Series, rv20_daily: pd.Series) -> pd.Series:
    """Both inputs in *decimal* annualised vol, date-indexed."""
    aligned = pd.concat([iv30_daily, rv20_daily], axis=1, join="inner")
    aligned.columns = ["iv30", "rv20"]
    return aligned["iv30"] - aligned["rv20"]


def dvol_ticks_to_daily_iv(dvol_ticks: pd.Series) -> pd.Series:
    """Resample DVOL (raw % units, sub-hourly) to daily decimal vol.

    Last value of each UTC day → /100 to convert 65.0 → 0.65.
    """
    if dvol_ticks.empty:
        return dvol_ticks.copy()
    daily_pct = dvol_ticks.resample("1D").last().dropna()
    daily_pct.index = daily_pct.index.tz_convert("UTC").date if hasattr(
        daily_pct.index, "tz_convert"
    ) else daily_pct.index.date
    return daily_pct / 100.0
