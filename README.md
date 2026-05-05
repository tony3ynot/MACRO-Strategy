# MACRO Strategy

[![한국어](https://img.shields.io/badge/%ED%95%9C%EA%B5%AD%EC%96%B4-2962FF?style=for-the-badge)](README.md)
[![English](https://img.shields.io/badge/English-757575?style=for-the-badge)](README.en.md)

### Market-Adaptive Covered-call Regime Optimizer

![Status](https://img.shields.io/badge/status-Phase_2_Validated-brightgreen)
![Stack](https://img.shields.io/badge/stack-FastAPI%20%7C%20TimescaleDB%20%7C%20Celery-blue)
![Data](https://img.shields.io/badge/data-yfinance%20%7C%20Polygon%20%7C%20Deribit%20%7C%20Hyperliquid-success)
![Cost](https://img.shields.io/badge/monthly_cost-%240-brightgreen)

> **Rotating between MSTR delta exposure and MSTY premium harvest based on volatility regime — quant alpha without options, leverage abuse, or institutional infrastructure.**

대부분의 MSTR/MSTY 투자자는 한 자산만을 장기적으로 보유합니다.
본 시스템은 **변동성 국면(Regime)을 정량 분류**해서, 각 국면이 보상하는 자산으로 자동 회전시킵니다.
알파의 원천은 자산 선택이 아니라 **회전 타이밍**.

---

## The Edge

| Market Regime | Action | Alpha |
|---|---|---|
| 횡보 + IV 높음 | **MSTY**로 옵션 프리미엄 수확 | 변동성 거품을 배당으로 환원 |
| 추세 돌파 | **MSTR (+ MSTU 옵션)**로 상방 캡 제거 | MSTY의 콜 행사 손실 회피 |
| MSTR이 NAV 대비 저평가 | **MSTR 100%** 평균 회귀 베팅 | mNAV mean-reversion |
| 진성 risk-off | **Cash + MSTZ 단기 hedge** | 하방 델타 회피 + 비대칭 페이오프 |

---

## Strategy Architecture — MSTR + MSTY + Cash + Drawdown Circuit Breaker

배포 strategy = **macro_trend_v5_breaker**: 두 leveraged ETF (MSTU, MSTZ) 모두 제거.
attribution 결과 둘 다 net-negative EV로 검증됨 (MSTU −12pp, MSTZ −5pp). 정식 사양: [`docs/STRATEGY.md`](docs/STRATEGY.md).

```
base   : MSTR    0.50 - 1.00   ← mNAV bucket curve (deep discount → 1.0, extreme premium → 0.5)
overlay: MSTY    0   - 0.35   ← narrow vol-seller window
                                (IV>40% + VRP>3% + RV<50% + MSTR이 MA200 ±15%)
hedge  : cash                  ← MSTR < MA50 < MA200 + VRP ≤ 0 → MSTR/MSTY 절반, 나머지 cash
breaker: book DD ≤ -15%        → 모든 weight × 0.50 (cash 증가, 숏 안 함)
         book DD ≥ -10% 회복   → normal mode
```

### Validation — EXTENDED + LIVE (LONG 생략 — pre-2021엔 BTC 지표 없어 MSTY harvest 작동 불가)

| Window | macro_trend_v5_breaker | BH MSTR | Calmar v5 / BH |
|---|---:|---:|---:|
| **EXTENDED** 2021→ (5y full cycle) | **+17.24% / -46.5%** | +23.67% / -84% | **0.37 / 0.28** ✅ |
| **LIVE** 2024-05→ (2y bear) | **+15.78% / -46.5%** | +7.85% / -77% | **0.34 / 0.10** ✅ |

**Walk-forward sanity** (EXTENDED 반으로 split):
| Sub-period | v5_breaker | BH MSTR |
|---|---:|---:|
| 2021-04 ~ 2023-12 (bear/recovery) | +1.6% / -42% / Cal **0.04** | -3.9% / -84% / Cal -0.05 |
| 2024-01 ~ 2026-05 (bull→bear) | +33.0% / -46% / Cal **0.71** | +52.6% / -77% / Cal 0.68 |

**진짜 framing — drawdown reducer with cycle-robust risk-adjusted alpha**:
- 모든 cycle에서 **BH MSTR Calmar 압도** (EXTENDED 0.37 vs 0.28, LIVE 0.34 vs 0.10)
- 강세장에선 leverage 사용 안 해서 absolute return 양보 (~20pp on 2024 rally)
- 약세장/회복엔 +5-8pp CAGR alpha + MDD 절반
- MSTU, MSTZ 둘 다 5년 attribution에서 net-negative → 의도적으로 제거
- v1-v4는 진단용으로 보존: [`docs/STRATEGY.md §6.1`](docs/STRATEGY.md#61-three-window-backtest)

---

## Signal Architecture — IV Decomposition

순수 MSTR IV 만으론 신호가 오염됩니다 (전환사채 발행 우려, 숏 스퀴즈, mNAV 거품).
BTC IV(Deribit)를 **24/7 leading indicator + denoising baseline**으로 사용해 분해:

```
MSTR_IV(t) ≈ β · BTC_IV(t − Δ) + EquityPremium(t)
              ─────────────────   ────────────────
              Crypto-side          Equity-side
              (24/7 leading)       (residual signal)
```

| EquityPremium 상태 | 해석 | 시스템 액션 |
|---|---|---|
| ≈ 0 | BTC vol이 MSTR vol을 설명 | Core regime 룰 그대로 |
| ↑↑ (+2σ) | 주식판 거품 (squeeze, NAV 과열) | Risk-off 게이트, MSTY exit |
| ↓ (음수) | MSTR이 BTC vol 저평가 | Long 시그널 (advanced) |
| ↑ & BTC_IV ↑ | 진성 + 거품 동조 | Risk-off + downside overlay |

**β estimation**: 90-day rolling regression vs Kalman state-space adaptive — Phase 2에서 walk-forward A/B 후 우월 모델 채택.

---

## Data Stack

| Tier | Source | Coverage | Role |
|---|---|---|---|
| **1. Crypto-Native** *(24/7 leading)* | Deribit (옵션, DVOL), Coinbase·Binance (현물), Hyperliquid·Bybit (펀딩, OI, 청산) | 5+ years | 선행 시그널, denoising baseline |
| **2. Equity** *(US hours, primary)* | yfinance (MSTR/MSTU/MSTY/MSTZ OHLCV + 분배), Polygon Options Basic (MSTR 옵션 chain 2y EOD) | 2 - 25+ years | 거래 가능한 자산의 실제 수익률, MSTR-specific IV |
| **3. Fundamental** | SEC 8-K scrape (MSTR BTC holdings, capital structure), YieldMax IR (MSTY 분배 발표) | 2020-08+ | mNAV(EV-adjusted), 분배 timing |
| **4. Macro** | FRED (DGS10, DXY, MOVE) | Decades | 시장 컨텍스트 |

---

## Tech Stack

| Layer | Tools |
|---|---|
| **Backend** | Python 3.12, FastAPI, SQLAlchemy 2.0 (async), Celery |
| **Database** | PostgreSQL 16 + **TimescaleDB** (hypertables, continuous aggregates, compression) |
| **Cache / Queue** | Redis 7 |
| **Quant** | pandas, numpy, scipy, filterpy (Kalman), vectorbt-style backtester |
| **Frontend** *(Phase 5)* | Next.js 14, Tailwind, TanStack Query, Orval (OpenAPI typed client) |
| **Infrastructure** | Docker Compose, Oracle Cloud Free Tier (Ampere ARM A1, 4c/24GB), Cloudflare Tunnel |
| **Notifications** | Telegram Bot (regime transitions + 9AM daily briefing) |

---

## System Architecture

```mermaid
graph LR
  subgraph S["Data Sources"]
    direction TB
    D[Deribit<br/>BTC Options]
    Y[yfinance<br/>Equities]
    P[Polygon<br/>MSTR Options]
    H[Hyperliquid<br/>Funding/OI]
    F[8-K + IR<br/>Scrapers]
  end

  subgraph I["Ingestion"]
    direction TB
    R[Rate-Limited<br/>Connectors]
    AU[Audit Log]
  end

  subgraph DB["TimescaleDB"]
    direction TB
    OHL[Equity OHLCV]
    OPT[Options Chain]
    BTC[BTC 1m + Options]
    FUN[Fundamentals]
  end

  subgraph Q["Quant Core"]
    direction TB
    IND[VRP, mNAV, RV]
    DEC[IV Decomposition<br/>β·BTC + Residual]
    KAL[Kalman Filter]
    REG[4-State Classifier]
  end

  subgraph O["Output"]
    direction TB
    TG[Telegram Bot]
    DASH[Next.js Dashboard]
    API[FastAPI]
  end

  S --> I --> DB --> Q --> O
```

---

## Project Structure

```
MACRO-Strategy/
├── docker-compose.yml          # core: postgres, redis, app, worker
├── docker-compose.jupyter.yml  # opt-in research env (jupyter only)
├── Makefile                    # ops shortcuts (verify-*, backtest, walk-forward, …)
├── docs/
│   └── STRATEGY.md             # formal allocator spec (priority over README)
├── services/
│   ├── postgres/init/          # extensions + bootstrap SQL
│   └── app/
│       ├── Dockerfile
│       ├── requirements.txt
│       ├── migrations/         # alembic 005: initial → indicators_daily
│       └── src/
│           ├── api/            # FastAPI (health endpoint)
│           ├── core/           # config, db, ingestor base, rate limiter, telegram
│           ├── connectors/     # yfinance / deribit / coinbase / polygon /
│           │                   # binance / hyperliquid / sec_edgar / yieldmax
│           ├── workers/        # celery tasks + beat schedule
│           ├── quant/
│           │   ├── indicators/   # realized_vol, btc_vrp, mnav, mstr_iv,
│           │   │                 # iv_decomposition
│           │   ├── backtesting/  # engine, data, strategies (macro_trend,
│           │   │                 # macro_regime, benchmarks)
│           │   ├── blackscholes.py    # BS pricer + IV inversion
│           │   └── risk_free.py       # FRED DGS1MO fetch
│           └── scripts/        # backfill_*, compute_*, run_backtest,
│                               # walk_forward_validation
└── research/notebooks/         # jupyter (read-only DB role)
```

---

## Roadmap

| Phase | Status | Deliverable |
|---|---|---|
| **1. Foundation** | ✅ Done | Docker stack, schema, 7 ingestors backfilled (MSTR/MSTU/MSTY/MSTZ, BTC daily/DVOL, MSTR holdings, Binance/Hyperliquid funding, YieldMax ROC, Polygon options 2y) |
| **2. Quant Core** | ✅ Done | indicators_daily (RV/IV/VRP/mNAV/β/EquityPremium), per-month historical Polygon backfill, 4-state allocator, walk-forward OOS validation, parameter robustness |
| **3. Live Signal** | ▶ Next | Telegram daily briefing populated with current state + recommended allocation, regime-transition push |
| **4. Dashboard** | ☐ Planned | Next.js mobile-first read-only dashboard |
| **5. Deployment** | ☐ Planned | Oracle Cloud ARM (free tier), Cloudflare Tunnel, daily DB snapshot |
| **2.5 backlog** | ☐ Planned | SEC 10-Q shares-out scraper (mNAV), Kalman β smoother, K-means regime classifier upgrade |

---

## Quick Start

```bash
git clone <repo> && cd MACRO-Strategy
make up                  # 첫 실행: 5-8분 (이미지 pull + Python 패키지 설치)
make verify-health       # → {"status":"ok","db":true,"timescaledb":"2.x.x","redis":true}
make help                # 모든 타깃 리스트
```

| Command | Effect |
|---|---|
| `make up` / `make down` | core 4 서비스 시작 / 정지 (데이터 보존) |
| `make jupyter-up` | 추가로 Jupyter (:8888) — research env |
| `make logs` / `make shell-pg` | 로그 tail / psql REPL |
| `make verify-indicators` / `verify-mstr-iv` / `verify-iv-decomp` | indicator 커버리지 점검 |
| `make backtest` / `backtest-stress` / `walk-forward` | Phase 2 검증 재실행 |
| `make clean` | 정지 + volume 삭제 (확인 프롬프트 — DB 데이터 영구 삭제) |

---

## Risk Management

자유 재량 금지 — 모든 진입·청산은 사전 정의된 게이트와 vol-target 사이징으로 결정.

| 메커니즘 | 작동 방식 |
|---|---|
| **Vol targeting (Hurst-Ooi-Pedersen 2017)** | ACCUMULATE leverage = clamp(0.50 / MSTR_RV20, 0.5×, 2.0×) — RV가 높을수록 leverage 자동 축소 |
| **Overheat de-risk** | MSTR이 MA200 대비 +10% 이상 → leverage ≤ 1.0×, +20% 이상 → ≤ 0.5× |
| **mNAV cap** | mNAV ≥ 1.5 (50%+ premium) → MSTU 비중 0, MSTR-only |
| **Hysteresis** | risk-on(ACCUMULATE/HEDGE)으로 들어갈 땐 2일 confirmation; de-risking은 즉시 |
| **HARVEST narrow gate** | 4중 AND 조건 — IV>40% **AND** VRP>3% **AND** RV<50% **AND** MSTR이 MA200 ±10% — 좁은 vol-seller window만 진입 |

각 게이트의 임계값과 D5 walk-forward 검증 결과는 [`docs/STRATEGY.md §3-§6`](docs/STRATEGY.md#3-per-state-allocation) 참조.

---

## Disclaimer

본 시스템은 **시그널 전용 (Signal-only)**입니다.
자동 주문 실행을 수행하지 않으며, 한국 자본시장법상 투자자문업이 아닙니다.
모든 매매 결정과 결과의 책임은 사용자 본인에게 있습니다.
과거 데이터를 기반으로 한 수리 모델이며 미래 수익을 보장하지 않습니다.
