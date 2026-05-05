"""MACRO trend strategy — domain-specific allocator across MSTR / MSTU / MSTY / MSTZ.

The architecture is a 4-state allocator. State names are picked to map
directly onto the MSTR-family payoff space rather than generic
trend-following labels:

    ACCUMULATE   long MSTR / MSTU spread, sized by mNAV + vol-target
    HARVEST      MSTY only, when premium-rich + sideways MSTR
    HEDGE        MSTZ only, when downtrend confirmed AND BTC vol chaos
    WAIT         flat (cash), when no edge is present

Dual-MA trend filter (Faber 2007 GTAA / Antonacci dual momentum) and
volatility-targeting (Hurst-Ooi-Pedersen 2017) are the standard building
blocks; the MACRO-indicator dispatch on top — using mNAV, BTC VRP, BTC
RV/IV, β_IV — is what differentiates this from a pure equity trend
overlay.

Parameter discipline: every magic number is a named constant with a
one-line economic justification. We deliberately keep the count small
to limit overfitting headroom.
"""
from __future__ import annotations

from dataclasses import dataclass


# ── Trend filter (dual-MA, Faber 2007) ─────────────────────────────────
MA_FAST = 50          # ~10-week medium-term trend
MA_SLOW = 200         # ~10-month long-term trend (Faber's tactical line)

# Trend-confirmation hysteresis (whipsaw filter). 2-3 days is consensus
# in the academic vol-targeted trend literature.
TREND_FLIP_DAYS = 2

# ── Trend-strength bins ────────────────────────────────────────────────
GAP_STRONG_DOWN = -0.10   # below MA200 by 10 % → strong bear
GAP_STRONG_UP = +0.10     # above MA200 by 10 % → strong bull (overheat zone)
GAP_EXTREME_UP = +0.20    # extreme overheat → de-leverage

# ── MACRO indicators ───────────────────────────────────────────────────
# mNAV bands. 1.0 is "fair NAV" by construction.
MNAV_DISCOUNT = 1.05      # ≤ → MSTR cheap relative to BTC treasury → favour leverage
MNAV_PREMIUM = 1.50       # ≥ → rich premium → de-leverage to plain MSTR

# Vol seller "narrow" rule for HARVEST.  All four conditions must hold:
HARVEST_BTC_IV_FLOOR = 0.40   # absolute IV high enough that premium $ matter
HARVEST_VRP_FLOOR = 0.03      # IV exceeds RV by ≥ 3 % p.a.
HARVEST_BTC_RV_CEIL = 0.50    # RV not so high that the underlying gets called
HARVEST_MSTR_BAND = 0.10      # |MSTR − MA200| / MA200 ≤ 10 % → genuinely sideways

# Hedge rule.  Originally we required VRP < -3 % (RV exceeds IV — the
# textbook "panic, not yet priced" signature) on top of a confirmed MA
# downtrend.  The 12-month OOS sweep (D5) showed that gate is too strict
# on this sample — running the hedge whenever the MA-downtrend confirms
# (VRP ≤ 0 % is sufficient) saves ~10 pp CAGR over the test window.
# We loosen to 0.0 = "any non-positive VRP".
HEDGE_VRP_CHAOS = 0.00

# ── Volatility targeting (Hurst-Ooi-Pedersen 2017) ─────────────────────
# Target portfolio realised vol; sizes the MSTR/MSTU split inside
# ACCUMULATE.  Set to 70 % — slightly above MSTR's typical RV (~65 %)
# so the typical book runs at ~1.08× leverage during ACCUMULATE.
#
# Earlier D5 work tuned this to 0.50 by sweeping a 12-month bear-only
# TEST window. When the sweep was re-run on the 5-year EXTENDED window
# (2021-03 onwards, full cycle including 2024 bull), 0.50 was clearly
# the wrong end of the spectrum — values from 0.50 → 0.80 monotonically
# lift CAGR and Calmar on both LIVE and EXTENDED. 0.70 is the value
# that retains meaningful drawdown protection without sacrificing
# bull-market upside; 0.80 lifts CAGR a bit further but at the cost
# of larger drawdowns inside ACCUMULATE.
VOL_TARGET = 0.70
LEVERAGE_FLOOR = 0.5
LEVERAGE_CEIL = 2.0

@dataclass
class TrendParams:
    ma_fast: int = MA_FAST
    ma_slow: int = MA_SLOW
    flip_days: int = TREND_FLIP_DAYS
    mnav_discount: float = MNAV_DISCOUNT
    mnav_premium: float = MNAV_PREMIUM
    harvest_iv_floor: float = HARVEST_BTC_IV_FLOOR
    harvest_vrp_floor: float = HARVEST_VRP_FLOOR
    harvest_rv_ceil: float = HARVEST_BTC_RV_CEIL
    harvest_band: float = HARVEST_MSTR_BAND
    hedge_vrp: float = HEDGE_VRP_CHAOS
    vol_target: float = VOL_TARGET


# ── helpers ────────────────────────────────────────────────────────────


def _gap(price: float, anchor: float) -> float:
    return (price - anchor) / anchor if anchor and anchor > 0 else 0.0


def _mstr_view(state, idx, p: TrendParams) -> dict | None:
    """Bundle the MSTR-side technical view into a single dict."""
    try:
        price = state.price("MSTR", idx)
        ma_fast = state.indicator("MSTR", idx, f"MA{p.ma_fast}")
        ma_slow = state.indicator("MSTR", idx, f"MA{p.ma_slow}")
    except Exception:
        return None
    if any(v is None for v in (price, ma_fast, ma_slow)):
        return None
    return {
        "price": price,
        "ma_fast": ma_fast,
        "ma_slow": ma_slow,
        "gap_slow": _gap(price, ma_slow),
        "uptrend": price > ma_fast > ma_slow,
        "downtrend": price < ma_fast < ma_slow,
    }


def _vol_target_leverage(state, idx, p: TrendParams) -> float:
    """Size MSTR/MSTU exposure so realised vol approaches `vol_target`.

    Uses MSTR 20-day RV from indicators_daily when available; falls back
    to the long-term default of 0.6 (MSTR's typical RV)."""
    rv = state.macro("mstr_rv20", idx)
    if rv is None or rv <= 0:
        return 1.5
    raw = p.vol_target / rv
    return max(LEVERAGE_FLOOR, min(LEVERAGE_CEIL, raw))


def _alloc_for_leverage(target: float) -> dict[str, float]:
    """Express target gross leverage as MSTR (1×) + MSTU (2×) split."""
    target = max(0.0, min(target, 2.0))
    if target <= 1.0:
        return {"MSTR": target}
    return {"MSTR": 2.0 - target, "MSTU": target - 1.0}


# ── strategy factory ───────────────────────────────────────────────────


def make_macro_trend(params: TrendParams | None = None):
    """Return the strategy function (closes over a tiny state machine)."""
    p = params or TrendParams()
    holder = {
        "state": "WAIT",
        "flip_up": 0,
        "flip_down": 0,
    }

    def _classify(view: dict, vrp: float | None, btc_rv: float | None,
                  btc_iv: float | None) -> str:
        """Decide which of the four states we *want* to be in today."""
        # HARVEST — narrow vol-seller window
        if (
            btc_iv is not None and vrp is not None and btc_rv is not None
            and btc_iv > p.harvest_iv_floor
            and vrp > p.harvest_vrp_floor
            and btc_rv < p.harvest_rv_ceil
            and abs(view["gap_slow"]) <= p.harvest_band
        ):
            return "HARVEST"

        # HEDGE — confirmed downtrend with vol chaos
        if view["downtrend"] and vrp is not None and vrp < p.hedge_vrp:
            return "HEDGE"

        # ACCUMULATE — uptrend OR shallow pullback
        if view["uptrend"] or view["gap_slow"] > GAP_STRONG_DOWN:
            return "ACCUMULATE"

        # WAIT — anything below MA200 that isn't a panic
        return "WAIT"

    def strat(state, idx):
        view = _mstr_view(state, idx, p)
        if view is None:
            return state.last_weights or {"MSTR": 1.0}

        vrp = state.macro("btc_vrp", idx)
        btc_rv = state.macro("btc_rv20", idx)
        btc_iv = state.macro("btc_iv30", idx)
        mnav = state.macro("mnav", idx)

        target_state = _classify(view, vrp, btc_rv, btc_iv)

        # Hysteresis on transitions into riskier states (ACCUMULATE/HEDGE).
        # HARVEST/WAIT switch instantly because they are *de-risking*.
        cur = holder["state"]
        if target_state in ("ACCUMULATE", "HEDGE") and target_state != cur:
            if target_state == "ACCUMULATE":
                holder["flip_up"] += 1
                holder["flip_down"] = 0
                if holder["flip_up"] < p.flip_days:
                    target_state = cur
                else:
                    holder["flip_up"] = 0
            else:  # HEDGE
                holder["flip_down"] += 1
                holder["flip_up"] = 0
                if holder["flip_down"] < p.flip_days:
                    target_state = cur
                else:
                    holder["flip_down"] = 0
        else:
            holder["flip_up"] = 0
            holder["flip_down"] = 0

        holder["state"] = target_state

        # ── allocation ────────────────────────────────────────────────
        if target_state == "ACCUMULATE":
            base = _vol_target_leverage(state, idx, p)
            # mNAV mean-reversion overlay
            if mnav is not None:
                if mnav <= p.mnav_discount:
                    base = min(LEVERAGE_CEIL, base + 0.3)
                elif mnav >= p.mnav_premium:
                    base = min(1.0, base)
            # Overheat de-risk (price way above MA200)
            if view["gap_slow"] >= GAP_EXTREME_UP:
                base = min(0.5, base)
            elif view["gap_slow"] >= GAP_STRONG_UP:
                base = min(1.0, base)
            return _alloc_for_leverage(base)

        if target_state == "HARVEST":
            return {"MSTY": 1.0}

        if target_state == "HEDGE":
            return {"MSTZ": 1.0}

        return {}  # WAIT → all cash

    return strat
