"""MACRO regime classifier — pure indicator-driven, no MA architecture.

Tightened rules after observing that the first-pass thresholds entered
MSTU/MSTY at the wrong times (mNAV discount + β_IV high happens during
declines too, not only during uptrends).

Decision tree (first match wins):

  panic_compression   VRP < -3 % AND β_IV > 3 AND BTC IV > 0.50
                                                          → MSTZ
  income_harvest      VRP > +3 % AND BTC RV < 0.50 AND
                      0.95 ≤ MSTR/MA200 ≤ 1.10            → MSTY
  vol_leveraged_bull  mNAV ≤ 1.30 AND β_IV > 2.5 AND
                      MSTR > MA50 > MA200                 → MSTU
  premium_bull        mNAV ≥ 1.50 AND MSTR > MA200        → MSTR
  neutral             default                              → MSTR
  fallback            any required indicator missing       → MSTR

The narrow MSTY band is the most important fix: vol-seller alpha only
exists when (a) absolute IV high enough to matter, (b) RV stays calm so
options expire OTM, and (c) the underlying is genuinely sideways so
neither the upside cap nor the downside drag kicks in. A simple
"VRP > 5 %" rule (the obvious first attempt) catches plenty of bear
days where MSTY's underlying still bleeds — that asymmetry is what
sank the previous version.
"""
from __future__ import annotations


# Tightened thresholds (one-line rationale per number)
PANIC_VRP = -0.03            # RV exceeds IV by 3 % p.a. → genuine panic move
PANIC_BETA = 3.0             # MSTR breathing 3× BTC vol → leveraged unwind
PANIC_BTC_IV_FLOOR = 0.50    # absolute IV must be high too
HARVEST_VRP = 0.03           # IV exceeds RV by 3 % → vol seller has edge
HARVEST_RV_CEIL = 0.50       # but RV stays calm → options expire OTM
HARVEST_BAND_LOW = 0.95      # MSTR within ±5/10 % of MA200 → no trend drag
HARVEST_BAND_HIGH = 1.10
LEV_BULL_MNAV_CEIL = 1.30    # not yet at peak premium
LEV_BULL_BETA_FLOOR = 2.5    # MSTR meaningfully leveraged on BTC vol
PREMIUM_MNAV_FLOOR = 1.50    # 50 %+ premium → revert to baseline MSTR


REGIME_TO_ALLOC = {
    "panic_compression":  {"MSTZ": 1.0},
    "income_harvest":     {"MSTY": 1.0},
    "vol_leveraged_bull": {"MSTU": 1.0},
    "premium_bull":       {"MSTR": 1.0},
    "neutral":            {"MSTR": 1.0},
    "fallback":           {"MSTR": 1.0},
}


def classify(*, mnav, vrp, beta, eqp, btc_rv, btc_iv,
             mstr_price, mstr_ma50, mstr_ma200) -> str:
    if any(v is None for v in (mnav, vrp, beta, btc_rv, btc_iv,
                               mstr_price, mstr_ma50, mstr_ma200)):
        return "fallback"

    # Panic — strict 3-condition AND
    if vrp < PANIC_VRP and beta > PANIC_BETA and btc_iv > PANIC_BTC_IV_FLOOR:
        return "panic_compression"

    # Harvest — strict 4-condition AND, narrow band on MSTR/MA200
    sideways_band = HARVEST_BAND_LOW * mstr_ma200 <= mstr_price <= HARVEST_BAND_HIGH * mstr_ma200
    if vrp > HARVEST_VRP and btc_rv < HARVEST_RV_CEIL and sideways_band:
        return "income_harvest"

    # Leveraged-bull — discount + high beta + uptrend structure
    if mnav <= LEV_BULL_MNAV_CEIL and beta > LEV_BULL_BETA_FLOOR \
            and mstr_price > mstr_ma50 > mstr_ma200:
        return "vol_leveraged_bull"

    # Premium-bull — rich premium but still in uptrend
    if mnav >= PREMIUM_MNAV_FLOOR and mstr_price > mstr_ma200:
        return "premium_bull"

    return "neutral"


def make_macro_regime_strategy():
    def strat(state, idx):
        try:
            mstr_price = state.price("MSTR", idx)
            mstr_ma50 = state.indicator("MSTR", idx, "MA50")
            mstr_ma200 = state.indicator("MSTR", idx, "MA200")
        except Exception:
            return {"MSTR": 1.0}
        regime = classify(
            mnav=state.macro("mnav", idx),
            vrp=state.macro("btc_vrp", idx),
            beta=state.macro("beta_iv", idx),
            eqp=state.macro("equity_premium", idx),
            btc_rv=state.macro("btc_rv20", idx),
            btc_iv=state.macro("btc_iv30", idx),
            mstr_price=mstr_price,
            mstr_ma50=mstr_ma50,
            mstr_ma200=mstr_ma200,
        )
        return dict(REGIME_TO_ALLOC[regime])
    return strat
