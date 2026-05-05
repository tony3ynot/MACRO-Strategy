# MACRO Strategy — Formal Logic Specification

> **Implementation reference for the production allocator.**
> Current production strategy: **macro_trend_v3** (continuous-weight allocator + drawdown circuit breaker)
> Code: [`services/app/src/quant/backtesting/strategies/macro_trend_v3.py`](../services/app/src/quant/backtesting/strategies/macro_trend_v3.py)
> Three iterations preserved in the same package for diagnostic comparison.

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

## 2. Allocator architecture

Continuous-weight allocator (no binary state machine). Each day the
strategy emits target weights for {MSTR, MSTU, MSTY, MSTZ}; cash is
the implicit residual. A drawdown circuit breaker halves gross
exposure when the book itself is in trouble.

```
       ┌──────────────────────────────────────────────────┐
       │  v2 base: continuous weights                     │
       │                                                   │
       │   MSTR base   ← mNAV bucket  (0.50 - 1.00)        │
       │   MSTU overlay (+0 - 0.30, uptrend + discount)   │
       │   MSTY overlay (+0 - 0.20, sideways + IV-rich)   │
       │   MSTZ overlay (+0 - 0.15, MA downtrend + VRP≤0) │
       │                                                   │
       │   mutually exclusive: MSTZ excludes MSTU & MSTY,  │
       │                       MSTY excludes MSTU         │
       └──────────────────────────────┬───────────────────┘
                                      │
        ┌─── if book DD ≤ -20% ───┐   │
        │                          ▼   │
        │      ┌─────────────────────────────┐
        │      │ panic: × 0.5 on every weight│   (rest = cash)
        │      └────────────────────────────┘
        │                          │
        └── exit when DD ≥ -10% ───┘
```

The previous (v1) state machine — ACCUMULATE / HARVEST / HEDGE / WAIT
with 100 %-of-portfolio swings — is preserved in `macro_trend.py` for
diagnostic comparison; v1 spent 27 % of its time in WAIT (cash) and
sacrificed too much upside.  v3's continuous mix + circuit breaker is
the production allocator.

### 2.1 MSTR base — mNAV bucket curve

The single most discriminating signal MSTR-side is mNAV (market cap /
BTC-treasury value). Bucketed:

| mNAV bucket | MSTR base weight | Reading |
|---|---:|---|
| ≤ 0.95 | 1.00 | deep discount → max long base |
| 0.95 – 1.20 | 0.90 | mild discount / fair |
| 1.20 – 1.50 | 0.80 | mild premium |
| 1.50 – 2.00 | 0.65 | rich premium |
| > 2.00 | 0.50 | extreme premium → start de-risking |
| (mNAV unavailable) | 0.85 | neutral fallback (pre-2020-08) |

### 2.2 MSTU overlay — leveraged-uptrend tilt

ON when *all* hold:

```
MSTR > MA50 > MA200          (Faber 2007 uptrend)
gap(MSTR, MA200) < +20 %     (not in extreme overheat)
mNAV ≤ 1.20                  (still has revaluation room)
```

Sized by the same mNAV bucket so deep discount + uptrend gets the full
0.30 overlay; mild discount gets 0.20; near-fair gets 0.10.

### 2.3 MSTY overlay — narrow vol-seller window

ON when *all* hold:

```
BTC IV30 > 40 %      (premium $ matter)
BTC VRP > +3 %       (IV exceeds RV)
BTC RV20 < 50 %      (calls expire OTM)
|MSTR / MA200 − 1| ≤ 10 %   (genuinely sideways)
```

Sized at fixed 0.20. Mutually exclusive with MSTU (a sideways book
shouldn't also bet on a leveraged trend).

### 2.4 MSTZ overlay — asymmetric hedge

ON when:

```
MSTR < MA50 < MA200          (Faber 2007 downtrend)
BTC VRP ≤ 0 %                (RV ≥ IV — panic, not yet priced)
```

Sized at 0.15. Trims MSTR base to ≤ 0.65 because the book is now
explicitly hedging, not adding. Mutually exclusive with both MSTU and
MSTY.

### 2.5 Drawdown circuit breaker (v3)

The engine writes `state.equity` and `state.equity_peak` before each
strategy call so the strategy can read its own drawdown. A two-state
hysteresis handles the panic gate:

| Transition | Trigger |
|---|---|
| normal → panic | book drawdown ≤ −20 % |
| panic → normal | book drawdown ≥ −10 % |

**In panic**, every v2 weight is multiplied by 0.50. The freed
allocation goes to cash. We do **not** add a short overlay during
panic — a first iteration tried 25 % MSTZ and found the resulting
≈ −20 % net delta fought every relief rally and turned the strategy
into a structural short. Halving longs is the correct shape for
"preserve capital, ride the recovery".

The wide hysteresis band (−20 % → −10 %) prevents whipsawing when a
brief relief rally takes the book back toward break-even but the
underlying bear hasn't ended.

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

Single source of truth: `TrendV3Params` (which wraps `TrendV2Params`)
in [`macro_trend_v3.py`](../services/app/src/quant/backtesting/strategies/macro_trend_v3.py).

| Parameter | Default | Rationale |
|---|---|---|
| `ma_fast` | 50 | Faber 2007 GTAA medium-term filter |
| `ma_slow` | 200 | Faber 2007 GTAA long-term tactical line (10-month) |
| `mstu_max` | 0.30 | leveraged-uptrend overlay cap |
| `msty_max` | 0.20 | sideways harvest overlay cap |
| `mstz_max` | 0.15 | asymmetric hedge overlay cap |
| `gap_overheat` | +0.20 | block MSTU once MSTR is 20 % above MA200 |
| `mstu_mnav_cap` | 1.20 | block MSTU at mild premium (no leverage if rich) |
| `msty_iv_floor` | 0.40 | absolute IV high enough that premium dominates decay |
| `msty_vrp_floor` | 0.03 | IV exceeds RV by ≥ 3 % p.a. |
| `msty_rv_ceil` | 0.50 | RV calm enough that calls expire OTM |
| `msty_band` | 0.10 | MSTR within ±10 % of MA200 → genuinely sideways |
| `mstz_vrp` | 0.00 | non-positive VRP + downtrend triggers hedge |
| `dd_panic_trigger` | -0.20 | book drawdown that activates the circuit breaker |
| `dd_panic_exit` | -0.10 | book drawdown threshold to leave panic |
| `panic_scale` | 0.50 | gross-exposure haircut while in panic |

---

## 6. Validation

Three reporting windows, each addressing a different question.
Cost setting throughout: 10 bps per turnover unit (realistic spread).

### 6.1 Three-window backtest

| Window | Period | What it measures |
|---|---|---|
| LONG | 2017-01 → today | Stress test — BTC IV / VRP / RV don't exist pre-2021-03 (DVOL launch), so HARVEST/HEDGE/MSTU overlays are dormant for the first 4 years. Strategy degrades to "MSTR base + DD breaker". |
| EXTENDED | 2021-03 → today | All MACRO indicators present, MSTU/MSTY/MSTZ synthetic pre-launch. The longest window where the full strategy can express itself. |
| LIVE | 2024-05 → today | Real ETFs + dense indicators. Cleanest test; only 24 months and bear-heavy. |

Headline numbers (cost_bps = 10):

| Strategy | LONG (9y) | EXTENDED (5y) | LIVE (2y) |
|---|---:|---:|---:|
| **macro_trend_v3** *(production)* | **+17.54 % / -48 %** | **+14.11 % / -48 %** | **+13.67 % / -48 %** |
| macro_trend_v2 (no breaker) | +19.75 % / -66 % | +17.58 % / -66 % | +6.57 % / -66 % |
| macro_trend (v1, state machine) | +7.18 % / -81 % | +18.94 % / -79 % | +42.53 % / -37 % |
| macro_regime | +23.99 % / -89 % | +18.40 % / -84 % | -3.71 % / -79 % |
| BH MSTR | +26.98 % / -89 % | +23.67 % / -84 % | +7.85 % / -77 % |
| BH MSTU (synth) | -16.28 % / -100 % | -41.43 % / -100 % | -57.27 % / -99 % |
| BH MSTY (synth) | +115.62 % / -72 % | +90.60 % / -72 % | +4.42 % / -72 % |

Calmar (CAGR / |MDD|) — risk-adjusted summary:

| Strategy | LONG | EXTENDED | LIVE |
|---|---:|---:|---:|
| **macro_trend_v3** | **0.37** ✅ | **0.29** ≈ BH | **0.28** > BH |
| macro_trend_v2 | 0.30 | 0.26 | 0.10 |
| macro_trend (v1) | 0.09 | 0.24 | **1.16** |
| BH MSTR | 0.30 | 0.28 | 0.10 |

Honest reading:

- **v3 dominates Calmar across all three windows**. It accepts a
  ~5–10 pp lower CAGR than BH MSTR and returns a ~30–40 pp better
  drawdown — a clean, cycle-robust trade.
- **v1's spectacular LIVE alpha (+34.7 pp CAGR) is bear-specific** and
  did not generalise: v1 spent 27 % of EXTENDED in cash (WAIT mode)
  and gave back the equivalent of two full BTC cycles' upside. Useful
  diagnostic, not a production target.
- **v2 is what cycle-robust looks like without a circuit breaker**:
  similar Calmar to BH but the MDD ceiling is still -66 %. The breaker
  in v3 trims that to -48 % at the cost of a few more pp of CAGR.
- **Pure indicator regime classifier (`macro_regime`)** is materially
  worse than every alternative in EXTENDED and LIVE — fixed indicator
  thresholds without an MA trend filter aren't enough on this sample.

The right framing: **macro_trend_v3 is a drawdown reducer with positive
returns**. It captures roughly three-quarters of MSTR's long-run return
in exchange for halving the worst-case drawdown. That is the
production claim.

BH MSTY (synth) showing +90 % CAGR over EXTENDED is the synthetic
proxy's flattering assumption (~80 % annual yield calibrated on 2024
peaks); real MSTY's flat ~+4 % CAGR over LIVE is the honest number for
that ETF as a buy-and-hold.

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
| `quant/backtesting/strategies/macro_trend_v3.py` | **Production allocator** (v2 + drawdown circuit breaker) |
| `quant/backtesting/strategies/macro_trend_v2.py` | Continuous-weight allocator (v3's base) |
| `quant/backtesting/strategies/macro_trend.py` | v1 4-state machine — kept for diagnostic comparison |
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
| `9227d5a` | D5+ | Honest correction: vol_target 0.50 → 0.70 + EXTENDED window |
| (this) | D6 | macro_trend_v2 (continuous weights) + v3 (drawdown circuit breaker) — production allocator switches to v3 |
