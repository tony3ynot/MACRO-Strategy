"""macro_trend v5 — drop both leveraged ETFs.

The cumulative attribution learning from v3 / v4:
- MSTU contributed -12 pp over EXTENDED 5 y under v4's "any uptrend"
  trigger. Fakeouts in 2021-11, 2023-03, etc. each cost 200-400 bps;
  the wins were not enough to offset.
- MSTZ contributed -5 pp under v3's "downtrend + VRP≤0" trigger. Even
  inside the 2025 Q4 / 2026 Q1 bear, daily-2× decay during chop ate
  most gains.
- MSTR + MSTY + cash were the only contributors with positive EV.

v5 is the structural minimum: keep what works.

  base   : MSTR    0.50 - 1.00     ← mNAV bucket curve
  overlay: MSTY    0   - 0.25     ← narrow vol-seller window (v4 widened band kept)
  hedge  : cash                    ← halve MSTR base in confirmed downtrend
  breaker: drawdown circuit       ← v3-style ×0.5 on every weight at -20% DD

No MSTU, no MSTZ. The strategy gives up the *theoretical* upside of
leveraged tools in exchange for removing two structurally negative-EV
slots from the book. For Korean retail with no 1× inverse alternative,
this is the right shape.
"""
from __future__ import annotations

from dataclasses import dataclass


# ── trend filter ──────────────────────────────────────────────────────
MA_FAST = 50
MA_SLOW = 200

# ── MSTY narrow rule (v5b-tuned, post-EXTENDED parameter sweep) ──────
# msty_max raised from 0.25 → 0.35 after EXTENDED+LIVE sweep showed
# consistent +1-2 pp CAGR with no MDD penalty.  Band, RV ceil, IV floor,
# and VRP floor unchanged because the sweep showed minimal sensitivity.
MSTY_VRP_FLOOR = 0.03
MSTY_RV_CEIL = 0.50
MSTY_IV_FLOOR = 0.40
MSTY_BAND = 0.15
MSTY_MAX = 0.35


# ── HEDGE definition (cash, no MSTZ) ──────────────────────────────────
HEDGE_VRP = 0.00
HEDGE_MSTR_SCALE = 0.50


def _mstr_base(mnav: float | None) -> float:
    if mnav is None:
        return 0.85
    if mnav <= 0.95: return 1.00
    if mnav <= 1.20: return 0.90
    if mnav <= 1.50: return 0.80
    if mnav <= 2.00: return 0.65
    return 0.50


@dataclass
class TrendV5Params:
    ma_fast: int = MA_FAST
    ma_slow: int = MA_SLOW
    msty_max: float = MSTY_MAX
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


def make_macro_trend_v5(params: TrendV5Params | None = None):
    p = params or TrendV5Params()

    def strat(state, idx):
        if not state.has("MSTR", idx, "close"):
            return state.last_weights or {"MSTR": 1.0}
        mstr_price = state.price("MSTR", idx)
        ma_slow = _safe(state, "MSTR", idx, f"MA{p.ma_slow}")
        ma_fast = _safe(state, "MSTR", idx, f"MA{p.ma_fast}")
        if ma_slow is None or ma_fast is None:
            return {"MSTR": 1.0}

        gap_slow = (mstr_price - ma_slow) / ma_slow if ma_slow > 0 else 0.0
        downtrend = mstr_price < ma_fast < ma_slow

        mnav = state.macro("mnav", idx)
        vrp = state.macro("btc_vrp", idx)
        btc_rv = state.macro("btc_rv20", idx)
        btc_iv = state.macro("btc_iv30", idx)

        mstr_w = _mstr_base(mnav)

        # MSTY narrow harvest
        msty_w = 0.0
        if (
            vrp is not None and btc_rv is not None and btc_iv is not None
            and vrp > p.msty_vrp_floor
            and btc_rv < p.msty_rv_ceil
            and btc_iv > p.msty_iv_floor
            and abs(gap_slow) <= p.msty_band
        ):
            msty_w = p.msty_max

        # HEDGE → halve MSTR + MSTY, rest = cash
        if downtrend and vrp is not None and vrp <= p.hedge_vrp:
            mstr_w *= p.hedge_mstr_scale
            msty_w *= p.hedge_mstr_scale

        weights: dict[str, float] = {}
        if mstr_w > 0: weights["MSTR"] = mstr_w
        if msty_w > 0: weights["MSTY"] = msty_w
        return weights

    return strat


def make_macro_trend_v5_with_breaker(
    params: TrendV5Params | None = None,
    dd_panic_trigger: float = -0.15,   # v5b: was -0.20; sweep showed -0.15 catches
                                        # multi-cycle drawdowns sooner (+5 pp EXT, +0.7 pp LIVE).
    dd_panic_exit: float = -0.10,
    panic_scale: float = 0.50,
    trend_exit_ma_fast: int = 50,
    trend_exit_momentum_window: int = 20,
    trend_exit_momentum_floor: float = 0.10,
):
    """v5_breaker with dual-condition panic exit.

    The original single-condition exit (book DD ≥ -10 %) anchored on
    *all-time* peak.  After a year-long bear the peak is ancient history
    and a real recovery rally can take MSTR up 50 %+ from its low while
    the book DD is still around -25 % — i.e. the strategy stays half-
    sized through the entire bounce.  This was observed live on
    2026-05-04: MSTR +44 % over 20 days from the local low, MSTR > MA50
    by 28 %, but the book had peaked in 2025-07 and was still -31 %
    underwater, so panic refused to release.

    Fix: leave panic when **either**
      • book DD has recovered to the −10 % band (original rule), **OR**
      • a fresh uptrend has confirmed (MSTR > MA50 AND 20d MSTR return
        > +10 %).

    Whipsaw concern: if a relief rally fails, momentum will fall back
    and DD will deepen, re-tripping the entry condition.  Re-entering
    panic on the next dip is the right behaviour, not a bug — we want
    to ride confirmed recoveries and re-de-risk on confirmed continuations.
    """
    base = make_macro_trend_v5(params)
    holder = {"panic": False, "trend_streak": 0}
    TREND_CONFIRM_DAYS = 5

    def _trend_recovery_signal(state, idx) -> bool:
        if not state.has("MSTR", idx, "close"):
            return False
        try:
            price = state.price("MSTR", idx)
            ma_fast = state.indicator("MSTR", idx, f"MA{trend_exit_ma_fast}")
        except Exception:
            return False
        if ma_fast is None or price <= ma_fast:
            return False
        if idx < trend_exit_momentum_window:
            return False
        if not state.has("MSTR", idx - trend_exit_momentum_window, "close"):
            return False
        past = state.price("MSTR", idx - trend_exit_momentum_window)
        if past <= 0:
            return False
        momentum = price / past - 1.0
        return momentum > trend_exit_momentum_floor

    def strat(state, idx):
        dd = state.drawdown
        trend_ok = _trend_recovery_signal(state, idx)
        # Streak counter: how many consecutive days has trend been recovering?
        if trend_ok:
            holder["trend_streak"] += 1
        else:
            holder["trend_streak"] = 0
        trend_confirmed = holder["trend_streak"] >= TREND_CONFIRM_DAYS

        if not holder["panic"]:
            # Enter panic on drawdown only if trend is NOT confirmed.
            # A confirmed uptrend overrides drawdown anchor (avoids the
            # whipsaw where dd is anchored to a year-old peak).
            if dd <= dd_panic_trigger and not trend_confirmed:
                holder["panic"] = True
        else:
            dd_ok = dd >= dd_panic_exit
            if dd_ok or trend_confirmed:
                holder["panic"] = False

        weights = base(state, idx) or {}
        if holder["panic"]:
            return {t: w * panic_scale for t, w in weights.items()}
        return weights

    return strat
