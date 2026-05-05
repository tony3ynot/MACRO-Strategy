"""macro_trend v5c — dynamic continuous sizing.

Three deliberate changes from v5:

1. **MSTR base** is now a smooth continuous function of mNAV instead
   of 5 discrete buckets. Below NAV the book is fully long; above NAV
   the size declines linearly down to a 0.50 floor at mNAV ≈ 2.25.

2. **MSTY size** scales linearly with VRP excess instead of being
   fixed at 0.35. Stronger vol-seller signal → bigger MSTY tilt.
   The four boolean entry conditions (IV > 40 %, RV < 50 %, sideways
   MSTR, VRP > 3 %) remain.

3. **Tactical MSTU** is reintroduced under a deliberately narrow
   triple-AND trigger:
      mNAV < 0.95   (deep discount)
      AND MSTR > MA50 > MA200   (Faber uptrend confirmed)
      AND last 5-day MSTR return > +5 %   (momentum confirms)
   Plus a 5-day hard hold limit — daily-2× decay only kicks in over
   weeks, so a forced exit after a week of holding caps the downside
   from any single-event reversal.

The drawdown circuit breaker is unchanged from v5_breaker (-15 % /
-10 % hysteresis, ×0.50 panic scale).
"""
from __future__ import annotations

from dataclasses import dataclass


# ── trend filter ──────────────────────────────────────────────────────
MA_FAST = 50
MA_SLOW = 200

# ── MSTR continuous sizing ────────────────────────────────────────────
MSTR_FLOOR = 0.50
MSTR_PREMIUM_SLOPE = 0.40  # base drops 0.40 per +1.0 mNAV above 1.0

# ── MSTY scaling ──────────────────────────────────────────────────────
MSTY_VRP_FLOOR = 0.03
MSTY_RV_CEIL = 0.50
MSTY_IV_FLOOR = 0.40
MSTY_BAND = 0.15
MSTY_VRP_GAIN = 5.0          # weight = 5 × (VRP − 0.03), capped at MSTY_MAX
MSTY_MAX = 0.35

# ── MSTU narrow tactical entry ────────────────────────────────────────
MSTU_MNAV_CEIL = 0.95
MSTU_MOMENTUM_FLOOR = 0.05    # last-5d MSTR return ≥ +5 %
MSTU_GAP_OVERHEAT = 0.20
MSTU_HOLD_LIMIT_DAYS = 5
MSTU_SIZE = 0.20

# ── HEDGE (cash, no MSTZ) ─────────────────────────────────────────────
HEDGE_VRP = 0.00
HEDGE_MSTR_SCALE = 0.50

# ── Circuit breaker (same as v5b) ─────────────────────────────────────
DD_PANIC_TRIGGER = -0.15
DD_PANIC_EXIT = -0.10
PANIC_SCALE = 0.50


@dataclass
class TrendV5cParams:
    ma_fast: int = MA_FAST
    ma_slow: int = MA_SLOW
    mstr_floor: float = MSTR_FLOOR
    mstr_premium_slope: float = MSTR_PREMIUM_SLOPE
    msty_max: float = MSTY_MAX
    msty_vrp_floor: float = MSTY_VRP_FLOOR
    msty_rv_ceil: float = MSTY_RV_CEIL
    msty_iv_floor: float = MSTY_IV_FLOOR
    msty_band: float = MSTY_BAND
    msty_vrp_gain: float = MSTY_VRP_GAIN
    mstu_mnav_ceil: float = MSTU_MNAV_CEIL
    mstu_momentum_floor: float = MSTU_MOMENTUM_FLOOR
    mstu_gap_overheat: float = MSTU_GAP_OVERHEAT
    mstu_hold_limit_days: int = MSTU_HOLD_LIMIT_DAYS
    mstu_size: float = MSTU_SIZE
    hedge_vrp: float = HEDGE_VRP
    hedge_mstr_scale: float = HEDGE_MSTR_SCALE
    dd_panic_trigger: float = DD_PANIC_TRIGGER
    dd_panic_exit: float = DD_PANIC_EXIT
    panic_scale: float = PANIC_SCALE


def _safe(state, ticker, idx, name):
    if not state.has(ticker, idx, name):
        return None
    return state.indicator(ticker, idx, name)


def _mstr_base_continuous(mnav: float | None, p: TrendV5cParams) -> float:
    if mnav is None:
        return 0.85
    if mnav <= 1.0:
        return 1.0
    return max(p.mstr_floor, 1.0 - p.mstr_premium_slope * (mnav - 1.0))


def make_macro_trend_v5c(params: TrendV5cParams | None = None):
    p = params or TrendV5cParams()
    holder = {"panic": False, "mstu_days": 0}

    def strat(state, idx):
        if not state.has("MSTR", idx, "close"):
            return state.last_weights or {"MSTR": 1.0}
        mstr_price = state.price("MSTR", idx)
        ma_fast = _safe(state, "MSTR", idx, f"MA{p.ma_fast}")
        ma_slow = _safe(state, "MSTR", idx, f"MA{p.ma_slow}")
        if ma_fast is None or ma_slow is None:
            return {"MSTR": 1.0}

        gap_slow = (mstr_price - ma_slow) / ma_slow if ma_slow > 0 else 0.0
        uptrend = mstr_price > ma_fast > ma_slow
        downtrend = mstr_price < ma_fast < ma_slow

        mnav = state.macro("mnav", idx)
        vrp = state.macro("btc_vrp", idx)
        btc_rv = state.macro("btc_rv20", idx)
        btc_iv = state.macro("btc_iv30", idx)

        # 5-day MSTR momentum — needed for tactical MSTU
        mstr_5d_mom = 0.0
        if idx >= 5 and state.has("MSTR", idx - 5, "close"):
            past = state.price("MSTR", idx - 5)
            if past > 0:
                mstr_5d_mom = mstr_price / past - 1.0

        # 1. MSTR base (continuous)
        mstr_w = _mstr_base_continuous(mnav, p)

        # 2. MSTY (linear in VRP excess, capped)
        msty_w = 0.0
        if (
            vrp is not None and btc_rv is not None and btc_iv is not None
            and vrp > p.msty_vrp_floor
            and btc_rv < p.msty_rv_ceil
            and btc_iv > p.msty_iv_floor
            and abs(gap_slow) <= p.msty_band
        ):
            msty_w = max(0.0, min(p.msty_max, p.msty_vrp_gain * (vrp - p.msty_vrp_floor)))

        # 3. Tactical MSTU (narrow + 5-day hold limit)
        mstu_w = 0.0
        mstu_eligible = (
            mnav is not None and mnav < p.mstu_mnav_ceil
            and uptrend
            and mstr_5d_mom > p.mstu_momentum_floor
            and gap_slow < p.mstu_gap_overheat
        )
        if mstu_eligible:
            holder["mstu_days"] += 1
            if holder["mstu_days"] <= p.mstu_hold_limit_days:
                mstu_w = p.mstu_size
            else:
                # Forced exit after hold limit (decay protection); reset
                # so we'll need a fresh trigger sequence to re-enter.
                mstu_w = 0.0
        else:
            holder["mstu_days"] = 0

        # HEDGE (cash) — confirmed downtrend + non-positive VRP
        if downtrend and vrp is not None and vrp <= p.hedge_vrp:
            mstr_w *= p.hedge_mstr_scale
            msty_w *= p.hedge_mstr_scale
            mstu_w = 0.0

        # MSTU and MSTY don't co-exist in pure sideways
        if msty_w > 0 and abs(gap_slow) <= 0.05:
            mstu_w = 0.0

        # Drawdown circuit breaker (state.equity is engine-supplied)
        dd = state.drawdown
        if not holder["panic"] and dd <= p.dd_panic_trigger:
            holder["panic"] = True
        elif holder["panic"] and dd >= p.dd_panic_exit:
            holder["panic"] = False

        weights: dict[str, float] = {}
        if mstr_w > 0: weights["MSTR"] = mstr_w
        if mstu_w > 0: weights["MSTU"] = mstu_w
        if msty_w > 0: weights["MSTY"] = msty_w

        if holder["panic"]:
            weights = {t: w * p.panic_scale for t, w in weights.items()}

        return weights

    return strat
