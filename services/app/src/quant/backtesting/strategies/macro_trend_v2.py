"""macro_trend v2 — continuous-weight allocator with always-on MSTR base.

What v1 got wrong (per EXTENDED-window attribution)
---------------------------------------------------
- Spent ~27 % of trading days in WAIT (cash) — guaranteed to miss every
  recovery rally that started below MA200.
- Vol-targeting capped leverage exactly when bull rallies most needed
  it: rising MSTR → rising RV → leverage shrinks.
- Binary 100 %-of-portfolio-in-one-thing amplified every signal error.
- Net: trend captured 56 % of MSTR's bull return over 5 years and only
  reduced bear losses modestly. BH MSTR won on raw CAGR by ~5 pp.

v2 architecture
---------------
Always hold an MSTR base position; layer tactical overlays for upside
(MSTU), income (MSTY), and asymmetric hedge (MSTZ). The size of each
slice is a continuous function of the indicators rather than an
on/off state. Cash is reserved for genuine premium overheat.

  base_MSTR : 0.50 - 1.00,  driven by mNAV (deep discount → 1.0,
                              extreme premium → 0.50)

  overlay_MSTU : 0 - 0.30,  ON when uptrend + mNAV ≤ 1.20 + not overheat
  overlay_MSTY : 0 - 0.20,  ON when sideways + IV high + RV calm + VRP+
  overlay_MSTZ : 0 - 0.15,  ON when downtrend confirmed + VRP ≤ 0

Mutually exclusive:
  - MSTZ active → no MSTU, no MSTY (clear bear bet)
  - MSTY active → no MSTU (sideways, no leveraged trend bet)

This loosens nothing about the *trigger* logic — same MA50/MA200,
same VRP / IV / RV thresholds — but it changes the *response* from
"jump fully into asset X" to "tilt the book toward asset X".
"""
from __future__ import annotations

from dataclasses import dataclass


# ── trend filter ──────────────────────────────────────────────────────
MA_FAST = 50
MA_SLOW = 200
GAP_OVERHEAT = +0.20      # MSTR 20 % above MA200 → no MSTU overlay
GAP_PREMIUM_BAND = +0.10  # MSTR 10 % above MA200 → MSTY blocks MSTU territory

# ── mNAV → base sizing curve ──────────────────────────────────────────
# 5 buckets; each is one-line rationale.
def _mstr_base(mnav: float | None) -> float:
    if mnav is None:
        return 0.85               # neutral when mNAV unavailable (pre-2020-08)
    if mnav <= 0.95: return 1.00  # deep discount → max long base
    if mnav <= 1.20: return 0.90  # mild discount / fair
    if mnav <= 1.50: return 0.80  # mild premium
    if mnav <= 2.00: return 0.65  # rich premium
    return 0.50                   # extreme premium → start de-risking


# ── overlay thresholds ────────────────────────────────────────────────
MSTU_MNAV_CAP = 1.20         # only lever when not at premium
MSTY_VRP_FLOOR = 0.03
MSTY_RV_CEIL = 0.50
MSTY_IV_FLOOR = 0.40
MSTY_BAND = 0.10             # MSTR within ±10 % of MA200 → sideways
MSTZ_VRP = 0.00              # any non-positive VRP + downtrend


@dataclass
class TrendV2Params:
    ma_fast: int = MA_FAST
    ma_slow: int = MA_SLOW
    mstu_max: float = 0.30
    msty_max: float = 0.20
    mstz_max: float = 0.15
    gap_overheat: float = GAP_OVERHEAT
    msty_vrp_floor: float = MSTY_VRP_FLOOR
    msty_rv_ceil: float = MSTY_RV_CEIL
    msty_iv_floor: float = MSTY_IV_FLOOR
    msty_band: float = MSTY_BAND
    mstu_mnav_cap: float = MSTU_MNAV_CAP
    mstz_vrp: float = MSTZ_VRP


# ── helpers ───────────────────────────────────────────────────────────


def _safe(state, ticker, idx, name):
    if not state.has(ticker, idx, name):
        return None
    return state.indicator(ticker, idx, name)


def _macro(state, name, idx):
    return state.macro(name, idx)


# ── strategy factory ──────────────────────────────────────────────────


def make_macro_trend_v2(params: TrendV2Params | None = None):
    p = params or TrendV2Params()

    def strat(state, idx):
        # Required: MSTR price + MAs
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

        # Macro indicators (may be None pre-2021-03 or pre-2020-08)
        mnav = _macro(state, "mnav", idx)
        vrp = _macro(state, "btc_vrp", idx)
        btc_rv = _macro(state, "btc_rv20", idx)
        btc_iv = _macro(state, "btc_iv30", idx)

        # ── MSTR base ──
        mstr_w = _mstr_base(mnav)

        # ── MSTU overlay (uptrend + room for leverage) ──
        mstu_w = 0.0
        if uptrend and gap_slow < p.gap_overheat \
                and mnav is not None and mnav <= p.mstu_mnav_cap:
            # Scale by how attractive the discount is
            if mnav < 1.00:
                mstu_w = p.mstu_max
            elif mnav < 1.05:
                mstu_w = p.mstu_max * 0.66
            else:
                mstu_w = p.mstu_max * 0.33

        # ── MSTY overlay (sideways + premium-rich) ──
        msty_w = 0.0
        if (
            vrp is not None and btc_rv is not None and btc_iv is not None
            and vrp > p.msty_vrp_floor
            and btc_rv < p.msty_rv_ceil
            and btc_iv > p.msty_iv_floor
            and abs(gap_slow) <= p.msty_band
        ):
            msty_w = p.msty_max

        # ── MSTZ overlay (clear panic) ──
        mstz_w = 0.0
        if downtrend and vrp is not None and vrp <= p.mstz_vrp:
            mstz_w = p.mstz_max

        # Mutual-exclusion: a clear bearish hedge dominates the side bets.
        if mstz_w > 0:
            mstu_w = 0.0
            msty_w = 0.0
            # Trim MSTR base too — we are explicitly hedging, not adding
            mstr_w = min(mstr_w, 0.65)

        # MSTY (sideways) and MSTU (uptrend) are mutually contradictory
        if msty_w > 0:
            mstu_w = 0.0

        weights: dict[str, float] = {}
        if mstr_w > 0:
            weights["MSTR"] = mstr_w
        if mstu_w > 0:
            weights["MSTU"] = mstu_w
        if msty_w > 0:
            weights["MSTY"] = msty_w
        if mstz_w > 0:
            weights["MSTZ"] = mstz_w
        return weights

    return strat
