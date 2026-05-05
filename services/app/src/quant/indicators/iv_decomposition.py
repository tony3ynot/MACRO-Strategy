"""IV decomposition — β·BTC_IV + EquityPremium.

Model (per Carr-Lee-style proxy):

    MSTR_IV(t)  ≈  β(t) · BTC_IV(t - Δ)   +   EquityPremium(t)

where:
  • β(t)              — rolling OLS slope; how much MSTR vol breathes
                        with BTC vol over the last `window` days.
  • EquityPremium(t)  — residual after removing the BTC-explained piece;
                        captures the structural extras MSTR vol carries
                        on top of BTC: convertible-bond gamma, ATM
                        equity issuance reflexivity, balance-sheet
                        leverage, etc.
  • Δ                 — lag in trading days; we measure correlation at
                        candidate lags and pick the one that maximises
                        ρ(MSTR_IV, BTC_IV(t-Δ)) over the joint sample.

Rolling OLS is closed-form so we don't need scipy/statsmodels:

    β = ( n·Σxy − Σx·Σy ) / ( n·Σx² − (Σx)² )
    α = ( Σy − β·Σx ) / n

We expose β(t) and EquityPremium(t) ready for upsert into
indicators_daily; α isn't stored separately because it's encoded in
EquityPremium = MSTR_IV − β·BTC_IV (i.e. α + ε).
"""
from __future__ import annotations

import logging
from collections.abc import Iterable

import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)

DEFAULT_WINDOW = 60         # trading days
DEFAULT_LAGS = (0, 1, 2, 3, 5)


def best_lag(
    mstr_iv: pd.Series, btc_iv: pd.Series, lags: Iterable[int] = DEFAULT_LAGS,
) -> tuple[int, dict[int, float]]:
    """Return (best_lag, {lag: correlation}) over the joint sample.

    Larger lag → BTC leads MSTR by `lag` days.  We try lag=0 (contemp)
    plus a small lead window because BTC vol regime shifts often
    front-run the equity reaction by 1-3 sessions.
    """
    correlations: dict[int, float] = {}
    for lag in lags:
        shifted = btc_iv.shift(lag)
        joined = pd.concat([mstr_iv, shifted], axis=1).dropna()
        if len(joined) < 30:
            continue
        joined.columns = ["mstr", "btc"]
        correlations[lag] = float(joined["mstr"].corr(joined["btc"]))
    if not correlations:
        raise ValueError("insufficient overlap to estimate correlation at any lag")
    chosen = max(correlations, key=correlations.get)
    return chosen, correlations


def rolling_ols(
    y: pd.Series, x: pd.Series, window: int = DEFAULT_WINDOW,
) -> tuple[pd.Series, pd.Series]:
    """Closed-form rolling OLS over a *date-aligned* (y, x).

    Returns (alpha_series, beta_series) indexed by y's index. Values
    before `window` non-NaN observations are accumulated are NaN.
    """
    df = pd.concat([y.rename("y"), x.rename("x")], axis=1).dropna()
    if len(df) < window:
        return (
            pd.Series(np.nan, index=y.index, name="alpha"),
            pd.Series(np.nan, index=y.index, name="beta"),
        )
    n = window
    sx = df["x"].rolling(n).sum()
    sy = df["y"].rolling(n).sum()
    sxy = (df["x"] * df["y"]).rolling(n).sum()
    sx2 = (df["x"] ** 2).rolling(n).sum()

    denom = n * sx2 - sx ** 2
    beta = (n * sxy - sx * sy) / denom
    alpha = (sy - beta * sx) / n

    # Project back onto the original index (NaN where not estimable)
    return (
        alpha.reindex(y.index).rename("alpha"),
        beta.reindex(y.index).rename("beta"),
    )


def compute_decomposition(
    mstr_iv: pd.Series,
    btc_iv: pd.Series,
    lag: int,
    window: int = DEFAULT_WINDOW,
) -> pd.DataFrame:
    """Return DataFrame with columns ['beta_iv', 'equity_premium'].

    Both series are NaN where the rolling window doesn't have enough
    paired observations or where MSTR_IV / BTC_IV is itself missing
    on that date.
    """
    btc_lagged = btc_iv.shift(lag)
    alpha, beta = rolling_ols(mstr_iv, btc_lagged, window=window)
    out = pd.DataFrame(
        {
            "mstr_iv": mstr_iv,
            "btc_lagged": btc_lagged,
            "beta_iv": beta,
            "alpha": alpha,
        }
    )
    out["equity_premium"] = out["mstr_iv"] - out["beta_iv"] * out["btc_lagged"]
    return out[["beta_iv", "equity_premium"]]
