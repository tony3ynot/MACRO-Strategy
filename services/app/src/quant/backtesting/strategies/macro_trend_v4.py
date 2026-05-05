"""macro_trend v4 — drop MSTZ, expand MSTU + MSTY usage.

Findings from the v3 attribution (5-year EXTENDED window):
- MSTU was held 0 days. The `mNAV ≤ 1.20` gate blocked it from
  triggering during 2024's bull because mNAV expanded to 2-3× as
  premium normally does in BTC bulls.
- MSTZ was held 90 days but contributed -5.2 pp net. Even within the
  recent 2025-Q4 / 2026-Q1 bear, MSTZ episodes were dominated by
  daily-2× decay during chop (e.g. 2026-02-06 → 02-13: MSTR -0.8 %,
  MSTZ -3.7 % because of pure vol drag).
- Cash was the residual ~70 % of the time — opportunity cost.

v4 changes
- HEDGE no longer holds MSTZ.  Confirmed downtrend halves the MSTR
  base; the freed allocation becomes cash (zero return, zero decay).
  This matches the OOSIT-style "PSQ + cash" defence at the structural
  cost (PSQ is 1× inverse with negligible decay, MSTZ is 2× with
  punitive decay; with no 1× inverse MSTR ETF available to Korean
  retail, cash is the right replacement).
- MSTU gate loses the mNAV ceiling. A confirmed Faber-style uptrend
  (price > MA50 > MA200) plus "not in extreme overheat" is enough to
  enter; size scales with how early the trend is.
- MSTY band widens (±15 % around MA200 instead of ±10 %) and the
  fixed allocation rises to 0.25 — still mutually exclusive with
  MSTU so we don't bet on uptrend and sideways simultaneously.

The continuous-weight skeleton and circuit breaker (when wrapped by
v3-style halving) are unchanged. v4 is meant to be combined with the
same circuit breaker via `make_macro_trend_v4_with_breaker`.
"""
from __future__ import annotations

from dataclasses import dataclass


# ── trend filter ──────────────────────────────────────────────────────
MA_FAST = 50
MA_SLOW = 200

# Strict overheat — past this we want less, not more, leverage
GAP_OVERHEAT = +0.20

# MSTU sizing curve based on how early the trend is
GAP_FRESH_TREND = +0.05      # < +5 % above MA200 → max MSTU
GAP_MID_TREND = +0.10        # +5–10 % → 2/3 max
# Above +10 % but below +20 % → 1/3 max
# Above +20 % → 0


# ── MSTY narrow rule ──────────────────────────────────────────────────
MSTY_VRP_FLOOR = 0.03
MSTY_RV_CEIL = 0.50
MSTY_IV_FLOOR = 0.40
MSTY_BAND = 0.15             # widened from 0.10 — captures more sideways days


# ── HEDGE definition (no MSTZ) ────────────────────────────────────────
HEDGE_VRP = 0.00             # downtrend + non-positive VRP → halve MSTR base
HEDGE_MSTR_SCALE = 0.50      # how much of the mNAV-bucket weight to keep


# ── mNAV → base sizing curve ─────────────────────────────────────────
def _mstr_base(mnav: float | None) -> float:
    if mnav is None:
        return 0.85
    if mnav <= 0.95: return 1.00
    if mnav <= 1.20: return 0.90
    if mnav <= 1.50: return 0.80
    if mnav <= 2.00: return 0.65
    return 0.50


@dataclass
class TrendV4Params:
    ma_fast: int = MA_FAST
    ma_slow: int = MA_SLOW
    mstu_max: float = 0.30
    msty_max: float = 0.25
    gap_overheat: float = GAP_OVERHEAT
    msty_vrp_floor: float = MSTY_VRP_FLOOR
    msty_rv_ceil: float = MSTY_RV_CEIL
    msty_iv_floor: float = MSTY_IV_FLOOR
    msty_band: float = MSTY_BAND
    hedge_vrp: float = HEDGE_VRP
    hedge_mstr_scale: float = HEDGE_MSTR_SCALE


def _safe(state, ticker, idx, name):
    if not state.has(ticker, idx, name):
        return None
    return state.indicator(ticker, idx, name)


def make_macro_trend_v4(params: TrendV4Params | None = None):
    p = params or TrendV4Params()

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

        # ── MSTR base ──
        mstr_w = _mstr_base(mnav)

        # ── MSTU overlay (no mNAV cap) ──
        mstu_w = 0.0
        if uptrend and gap_slow < p.gap_overheat:
            if gap_slow <= GAP_FRESH_TREND:
                mstu_w = p.mstu_max
            elif gap_slow <= GAP_MID_TREND:
                mstu_w = p.mstu_max * 2 / 3
            else:
                mstu_w = p.mstu_max / 3

        # ── MSTY overlay (wider band) ──
        msty_w = 0.0
        if (
            vrp is not None and btc_rv is not None and btc_iv is not None
            and vrp > p.msty_vrp_floor
            and btc_rv < p.msty_rv_ceil
            and btc_iv > p.msty_iv_floor
            and abs(gap_slow) <= p.msty_band
        ):
            msty_w = p.msty_max

        # ── HEDGE (cash, not MSTZ) ──
        if downtrend and vrp is not None and vrp <= p.hedge_vrp:
            mstr_w *= p.hedge_mstr_scale
            mstu_w = 0.0
            msty_w *= p.hedge_mstr_scale  # also trim sideways harvest

        # MSTU and MSTY mutually exclusive in pure sideways territory.
        # Allow both only when MSTR is mildly trending (uptrend + IV-rich).
        if msty_w > 0 and abs(gap_slow) <= 0.05:
            mstu_w = 0.0

        weights: dict[str, float] = {}
        if mstr_w > 0: weights["MSTR"] = mstr_w
        if mstu_w > 0: weights["MSTU"] = mstu_w
        if msty_w > 0: weights["MSTY"] = msty_w
        return weights

    return strat


# ── v4 + drawdown circuit breaker ────────────────────────────────────


def make_macro_trend_v4_with_breaker(
    params: TrendV4Params | None = None,
    dd_panic_trigger: float = -0.20,
    dd_panic_exit: float = -0.10,
    panic_scale: float = 0.50,
):
    """v4 base wrapped with the same circuit breaker logic as v3."""
    base = make_macro_trend_v4(params)
    holder = {"panic": False}

    def strat(state, idx):
        dd = state.drawdown
        if not holder["panic"] and dd <= dd_panic_trigger:
            holder["panic"] = True
        elif holder["panic"] and dd >= dd_panic_exit:
            holder["panic"] = False

        weights = base(state, idx) or {}
        if holder["panic"]:
            return {t: w * panic_scale for t, w in weights.items()}
        return weights

    return strat
