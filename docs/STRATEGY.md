# MACRO Strategy — Formal Logic Specification

> **Implementation reference for the production allocator.**
> Code: [`services/app/src/quant/backtesting/strategies/macro_trend.py`](../services/app/src/quant/backtesting/strategies/macro_trend.py)
> Last validated: Phase 2 D5 (commit `fd49bfa`).

This document specifies the deployed strategy's behaviour, parameters,
inputs, and validation results. It is the source of truth for what the
system actually does — the [README](../README.md) is the elevator pitch.

---

## 1. Universe

Four MSTR-family instruments, all retail-tradable in Korean accounts:

| Ticker | Payoff | Role |
|---|---|---|
| **MSTR** | Long BTC-leveraged equity (β ≈ 1.5–3 vs BTC) | Core directional exposure |
| **MSTU** | 2× daily long MSTR | Leverage in confirmed bull |
| **MSTY** | Synthetic covered-call MSTR + weekly distributions | Vol-seller premium harvest |
| **MSTZ** | 2× daily inverse MSTR | Asymmetric downside hedge |
| Cash | — | Risk-off, no edge available |

---

## 2. State Machine

Four states. Daily decision; transitions guarded by 2-day hysteresis on
risk-on / risk-off flips to suppress whipsaws.

```
                     ┌────────────────┐
                     │   ACCUMULATE   │  long MSTR/MSTU, vol-target sized
                     │   (default)    │
                     └───┬─────────┬──┘
        ┌────────────────┘         └────────────────┐
        ▼                                           ▼
┌────────────────┐  IV high + RV low      ┌────────────────┐
│    HARVEST     │  + sideways MSTR       │     HEDGE      │  MA downtrend
│   100% MSTY    │                        │   100% MSTZ    │  + VRP ≤ 0
└───────┬────────┘                        └───────┬────────┘
        │                                         │
        └────────────────┐    ┌───────────────────┘
                         ▼    ▼
                    ┌──────────┐
                    │   WAIT   │   below MA200, no panic
                    │  (cash)  │
                    └──────────┘
```

### 2.1 State definitions (priority order)

| Priority | State | Trigger (all conditions AND'd) |
|---|---|---|
| 1 | **HARVEST** | `BTC_IV30 > 40%` AND `VRP > +3%` AND `BTC_RV20 < 50%` AND `\|MSTR / MA200 − 1\| ≤ 10%` |
| 2 | **HEDGE** | `MSTR < MA50 < MA200` AND `VRP ≤ 0%` |
| 3 | **ACCUMULATE** | `MSTR > MA50 > MA200` (uptrend) OR `MSTR / MA200 > -10%` (shallow pullback) |
| 4 | **WAIT** | else (below MA200, not panic) |

The first matching condition wins. HARVEST sits at priority 1 because it
is genuinely additive only inside a narrow vol-seller window — outside
that window MSTY's covered-call cap on a high-vol underlying is a
liability, not an asset.

### 2.2 Hysteresis

| Transition | Confirmation |
|---|---|
| → ACCUMULATE | 2 trading days |
| → HEDGE | 2 trading days |
| → HARVEST | instant (de-risking from leverage) |
| → WAIT | instant (de-risking from leverage) |

---

## 3. Per-State Allocation

### 3.1 ACCUMULATE — vol-targeted MSTR/MSTU spread

**Base leverage** (Hurst-Ooi-Pedersen 2017, vol-targeting):

```
target_leverage = clamp( vol_target / MSTR_RV20 , 0.5 , 2.0 )
                  vol_target = 0.50  (D5-tuned)
```

**mNAV mean-reversion overlay** (additive bias):

| mNAV band | Adjustment |
|---|---|
| ≤ 1.05 (discount) | +0.3× leverage (lever into discount) |
| 1.05 – 1.50 (fair) | no change |
| ≥ 1.50 (premium) | cap leverage at 1.0× (no MSTU) |

**Overheat de-risk** (price gap above MA200):

| Gap vs MA200 | Cap |
|---|---|
| ≥ +20 % (extreme) | leverage ≤ 0.5× |
| ≥ +10 % (overheat) | leverage ≤ 1.0× |

**Final split** — express target leverage as a MSTR(1×) + MSTU(2×) mix:

```
L ≤ 1: MSTR weight = L
L > 1: MSTR weight = 2 − L,   MSTU weight = L − 1
```

### 3.2 HARVEST — `MSTY 100%`
### 3.3 HEDGE — `MSTZ 100%`
### 3.4 WAIT — `Cash 100%`

---

## 4. Inputs

### 4.1 Technical (computed at backtest time)

| Field | Window | Source |
|---|---|---|
| `MA50`, `MA200` | rolling mean | `equity_ohlcv.adj_close` |
| `MSTR / MA200` gap | — | derived |

### 4.2 Macro indicators (`indicators_daily`)

| Field | Computation | Used by |
|---|---|---|
| `mstr_rv20` | annualised stdev of log-returns, 20d, 252-trading-day basis | ACCUMULATE sizing |
| `btc_rv20` | same, 365-calendar-day basis | HARVEST gate |
| `btc_iv30` | last DVOL of UTC day / 100 | HARVEST gate |
| `btc_vrp` | `btc_iv30 − btc_rv20` | HARVEST + HEDGE gates |
| `mnav` | `MSTR mcap / (BTC qty × BTC close)` | ACCUMULATE overlay |
| `beta_iv` | rolling OLS β: `MSTR_IV ≈ β · BTC_IV + ε`, 60d window, lag 0 | (informational; not in trade logic yet) |
| `equity_premium` | `MSTR_IV − β · BTC_IV` | (informational; not in trade logic yet) |

### 4.3 Total-return correctness

We use `adj_close` (split + dividend-adjusted) so MSTY's ~ 70 % annual
distribution is reflected as compounded total return. Using raw close
mis-states MSTY's economics by 60+ pp CAGR — an issue the D4 fix
addressed.

---

## 5. Parameters

Single source of truth: `TrendParams` dataclass in
[`macro_trend.py`](../services/app/src/quant/backtesting/strategies/macro_trend.py).

| Parameter | Default | Rationale |
|---|---|---|
| `ma_fast` | 50 | Faber 2007 GTAA medium-term filter |
| `ma_slow` | 200 | Faber 2007 GTAA tactical line (10-month) |
| `flip_days` | 2 | Whipsaw filter; sweep showed 2 is the sweet spot |
| `mnav_discount` | 1.05 | "near or below NAV" → bias to leverage |
| `mnav_premium` | 1.50 | 50 % premium → cap leverage |
| `harvest_iv_floor` | 0.40 | absolute IV must be high to make premium meaningful |
| `harvest_vrp_floor` | 0.03 | IV exceeds RV by ≥ 3 % p.a. |
| `harvest_rv_ceil` | 0.50 | RV stays calm → calls expire OTM |
| `harvest_band` | 0.10 | MSTR within ±10 % of MA200 → genuinely sideways |
| `hedge_vrp` | **0.00** | D5-tuned: any non-positive VRP + MA downtrend triggers hedge |
| `vol_target` | **0.70** | EXTENDED-validated: 70 % p.a. vol target (≈ 1.08× typical leverage). Earlier D5 picked 0.50 on a bear-only TEST window; the 5-year EXTENDED sweep showed that was over-fit. |

**EXTENDED-validated** = sweep covers 2021-03 → today (full bull/bear cycle).

---

## 6. Validation

Three reporting windows, each addressing a different question.
Cost setting throughout: 10 bps per turnover unit (realistic spread).

### 6.1 Three-window backtest

| Window | Period | What it measures |
|---|---|---|
| LONG | 2017-01 → today | Stress test only — BTC IV / VRP / RV indicators don't exist pre-2021-03 (Deribit DVOL launch), so HARVEST and HEDGE are dormant for the first 4 years. macro_trend degrades to vol-targeted MSTR/MSTU and underperforms BH MSTR. **Not a fair-alpha estimate.** |
| EXTENDED | 2021-03 → today | All MACRO indicators present, MSTU/MSTY/MSTZ synthetic pre-launch. The longest window where the strategy can fully express itself. |
| LIVE | 2024-05 → today | Real ETFs + dense indicators. The cleanest test, but only 24 months — happens to be bear-heavy. |

| Strategy | LONG (9y) | EXTENDED (5y) | LIVE (2y) |
|---|---:|---:|---:|
| **macro_trend** | +7.18 % / -81.3 % | **+18.94 % / -78.96 %** | **+42.53 % / -36.74 %** |
| BH MSTR | +26.98 % / -89.3 % | +23.67 % / -84.11 % | +7.85 % / -77.42 % |
| BH MSTU (synth) | -16.28 % / -99.9 % | -41.43 % / -99.56 % | -57.27 % / -98.58 % |
| BH MSTY (synth) | +115.62 % / -71.8 % | +90.60 % / -71.79 % | +4.42 % / -71.79 % |
| Mix 50 % MSTR/MSTY | +73.20 % / -74.3 % | +62.66 % / -74.25 % | +6.82 % / -74.25 % |
| macro_regime | +23.99 % / -89.3 % | +18.40 % / -84.11 % | -3.71 % / -79.25 % |

Honest reading:

- **LIVE is the showcase**. macro_trend's +34.7 pp CAGR alpha and -41 pp
  MDD reduction over BH MSTR is real, but the LIVE window is bear-heavy
  (MSTR -57 % from May 2024 to May 2026), so the alpha is largely
  *defensive alpha*, not return enhancement.
- **EXTENDED is the honest cycle test**. macro_trend trails BH MSTR by
  ~5 pp CAGR but reduces MDD by ~5 pp; Calmar is roughly tied. Net over
  a full bull/bear cycle, the strategy is not a CAGR enhancer — it is
  a drawdown reducer.
- **LONG is a sanity check** with caveats: BTC indicators don't exist
  pre-2021, so HARVEST/HEDGE never trigger and the strategy is degraded
  to a vol-targeted MSTR/MSTU mix. Underperformance is expected.

The right framing: **macro_trend is a defensive overlay that trades
bull-market upside for bear-market protection.** It is most useful for
holders who would otherwise be 100 % MSTR and want the worst-case
drawdown halved; it is less useful for someone optimising for raw CAGR.

BH MSTY (synth) showing +90 % CAGR over EXTENDED is the synthetic
proxy's flattering assumption (annual yield ~80 %, calibrated on
2024 peaks); real MSTY's flat ~+4 % CAGR over LIVE is the honest
number for that ETF as a buy-and-hold.

### 6.2 Walk-forward TRAIN/TEST split

Within the 24-month LIVE window, splitting in half gave a roaring-bull
TRAIN (MSTR +153 %) and a deep-bear TEST (MSTR -57 %). Absolute drift
is uninformative; alpha vs MSTR is the right test:

| Window | macro_trend | BH MSTR | Alpha vs MSTR |
|---|---:|---:|---:|
| TRAIN (bull) | +76.19 % | +153.12 % | **-77 pp** (vol-target caps the rocket) |
| TEST (bear) | +1.96 % | -57.08 % | **+59 pp** (HEDGE / WAIT save capital) |

This is the same defensive-overlay pattern at finer granularity.

### 6.3 Parameter robustness — important correction

The earlier D5 sweep was run on TEST only (bear 12 months) and picked
`vol_target = 0.50`. Re-running on EXTENDED (5y full cycle) shows that
was bear-fit:

| `vol_target` | EXTENDED CAGR | EXTENDED Calmar | LIVE CAGR | LIVE Calmar |
|---:|---:|---:|---:|---:|
| 0.50 (D5-tuned) | +12.87 % | 0.17 | +36.07 % | 1.09 |
| 0.60 (Hurst-Ooi-Pedersen) | +15.25 % | 0.19 | +38.28 % | 1.09 |
| **0.70 (current)** | **+18.94 %** | **0.24** | **+42.53 %** | **1.16** |
| 0.80 | +22.61 % | 0.29 | +48.18 % | 1.30 |

Higher vol_target dominates monotonically on both windows — meaning
0.50 was clearly cherry-picked from the bear sample. Reverting to 0.70
gives most of the upside-capture without running into the higher-leverage
drawdown territory of 0.80. `hedge_vrp = 0.00` survives the EXTENDED
re-check and stays in.

We deliberately do NOT retune `ma_fast` / `ma_slow` despite the
12-month sweep showing other values: 50 / 200 are the published
Faber-2007 defaults, and the EXTENDED window is still only 5 years —
optimising those windows would be classic OOS-fitting.

---

## 7. Operational

| Aspect | Detail |
|---|---|
| Decision frequency | Daily, once per US-equity close |
| Compute trigger | Celery beat — `compute_indicators_daily` 22:30 UTC, `compute_mstr_iv30_daily` 22:45, `compute_iv_decomposition` 23:00 |
| Briefing dispatch | Telegram, 00:00 UTC = 09:00 KST (`send_daily_briefing`) |
| Idempotency | UPSERT on `indicators_daily.date`; safe to re-run |
| Audit | Every ingest + compute logged in `ingestion_runs` |

---

## 8. Known Limitations

| # | Limitation | Mitigation |
|---|---|---|
| 1 | **24-month real ETF history** for MSTU/MSTY/MSTZ | Synthetic 2× / -2× pre-launch, MSTY covered-call proxy. Live numbers are the meaningful test. |
| 2 | **mNAV uses snapshot shares-out** | Documented in `mnav.py`; Phase 2.5 SEC 10-Q scraper will fix. |
| 3 | **Polygon Basic 2y window** | Per-month spot-anchored historical backfill (commit `f363ccf`) closed the IV30 gap. |
| 4 | **β / EquityPremium informational only** | The decomposition is computed and stored but not yet wired into trade logic — Phase 2.5 candidate. |
| 5 | **HARVEST narrow rule rarely fires** | By design — vol-seller alpha is concentrated. Live monitoring will tell us how rare it really is. |
| 6 | **No tax modelling** | ROC distributions reduce cost basis (Korean tax rules differ); backtest is pre-tax. |
| 7 | **`macro_regime` (D3) classifier is not used** for trades | Underperforms BH MSTR. Kept for diagnostic comparison; K-means upgrade is a Phase 2.5 candidate. |

---

## 9. Files

| File | Purpose |
|---|---|
| `quant/backtesting/strategies/macro_trend.py` | This spec, executable |
| `quant/backtesting/data.py` | Panel assembly (real + synthetic, adj_close) |
| `quant/backtesting/engine.py` | Long-only weight backtester |
| `quant/indicators/realized_vol.py` | RV computation |
| `quant/indicators/btc_vrp.py` | BTC VRP |
| `quant/indicators/mnav.py` | mNAV (with snapshot caveat) |
| `quant/indicators/mstr_iv.py` | ATM IV30 from Polygon options |
| `quant/indicators/iv_decomposition.py` | β + EquityPremium |
| `scripts/run_backtest.py` | Multi-strategy comparison runner |
| `scripts/walk_forward_validation.py` | TRAIN/TEST split + sensitivity sweep |
| `Makefile` | `make backtest`, `make backtest-stress`, `make walk-forward` |

---

## 10. Change log

| Commit | Phase | Change |
|---|---|---|
| `2a38e2e` | D4 | Initial backtest engine + macro_trend / macro_regime strategies |
| `eafe646` | D4 | Switch to `adj_close` (MSTY-critical total-return fix) |
| `dafbb67` | D4 | Cost-stress targets in Makefile |
| `fd49bfa` | D5 | Walk-forward validation + tuned `hedge_vrp` / `vol_target` defaults |
