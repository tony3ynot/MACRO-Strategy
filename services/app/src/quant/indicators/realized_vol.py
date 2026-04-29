"""Close-to-close annualised realised volatility.

We use log-returns (geometric) and the standard 252-day equity / 365-day
crypto annualisation factors. Returned series is aligned to the *last*
date of each window, so RV20 on date D uses returns from D-19..D.
"""
from __future__ import annotations

import numpy as np
import pandas as pd

EQUITY_TRADING_DAYS = 252
CRYPTO_CALENDAR_DAYS = 365


def realised_vol(
    close: pd.Series,
    window: int = 20,
    annualisation: int = EQUITY_TRADING_DAYS,
) -> pd.Series:
    """Annualised stdev of log returns over rolling `window`.

    `close` must be a date-indexed Series. Returns NaN for the first
    `window` rows (insufficient history).
    """
    if close.empty:
        return close.copy()
    log_ret = np.log(close.astype(float)).diff()
    return log_ret.rolling(window).std() * np.sqrt(annualisation)
