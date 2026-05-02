"""Black-Scholes pricer + bisection-based implied-vol inverter.

We avoid scipy here so the quant package stays free of a heavy dep for one
use case. `math.erf` from stdlib is enough for the normal CDF.

Conventions:
- All rates and vols are continuous-compounded annualised decimals.
- Time-to-expiry T in years (calendar-day basis: days/365).
- Dividend yield q defaults to 0 (MSTR pays none; YieldMax MSTY ETF dist.
  is irrelevant here).
"""
from __future__ import annotations

import math


def _norm_cdf(x: float) -> float:
    return 0.5 * (1.0 + math.erf(x / math.sqrt(2.0)))


def bs_price(
    S: float, K: float, T: float, r: float, sigma: float,
    q: float = 0.0, option_type: str = "C",
) -> float:
    if T <= 0 or sigma <= 0:
        if option_type == "C":
            return max(S * math.exp(-q * T) - K * math.exp(-r * T), 0.0)
        return max(K * math.exp(-r * T) - S * math.exp(-q * T), 0.0)

    sqrt_t = math.sqrt(T)
    d1 = (math.log(S / K) + (r - q + 0.5 * sigma * sigma) * T) / (sigma * sqrt_t)
    d2 = d1 - sigma * sqrt_t
    if option_type == "C":
        return S * math.exp(-q * T) * _norm_cdf(d1) - K * math.exp(-r * T) * _norm_cdf(d2)
    return K * math.exp(-r * T) * _norm_cdf(-d2) - S * math.exp(-q * T) * _norm_cdf(-d1)


def implied_vol(
    price: float, S: float, K: float, T: float, r: float,
    q: float = 0.0, option_type: str = "C",
    tol: float = 1e-5, max_iter: int = 100,
    sigma_lo: float = 1e-4, sigma_hi: float = 5.0,
) -> float:
    """Return σ s.t. bs_price(σ) ≈ price, or NaN if unbracketed.

    The price must lie within the no-arbitrage envelope; if not (e.g.
    deep ITM with stale `last`), we return NaN rather than clamping —
    callers should drop NaN before averaging.
    """
    if not (price > 0 and S > 0 and K > 0 and T > 0):
        return float("nan")

    p_lo = bs_price(S, K, T, r, sigma_lo, q, option_type)
    p_hi = bs_price(S, K, T, r, sigma_hi, q, option_type)
    if price < p_lo - tol or price > p_hi + tol:
        return float("nan")

    lo, hi = sigma_lo, sigma_hi
    for _ in range(max_iter):
        mid = 0.5 * (lo + hi)
        p_mid = bs_price(S, K, T, r, mid, q, option_type)
        if abs(p_mid - price) < tol:
            return mid
        if p_mid < price:
            lo = mid
        else:
            hi = mid
    return 0.5 * (lo + hi)
