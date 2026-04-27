# MACRO Strategy: Market-Adaptive Covered-call Regime Optimizer
## 프로젝트 개요 및 개발 로드맵 (Project Overview & Development Roadmap)

**MACRO Strategy**는 마이크로스트래티지(MSTR)와 그 파생 상품인 MSTY(YieldMax MSTR Option Income Strategy ETF)를 활용하여, 시장의 변동성 국면(Regime)에 따라 자산 배분을 최적화하는 알고리즘 트레이딩 및 대시보드 시스템입니다. 본 문서는 시스템의 아키텍처, 퀀트 로직, 그리고 단계별 개발 계획을 상술합니다.

---

## 1. 프로젝트 비전 (Vision)
- **Micro to Macro:** 마이클 세일러의 'Micro'한 기업 운영 전략을 넘어, 시장의 'Macro'한 변동성 지표를 활용해 초과 수익(Alpha)을 창출합니다.
- **수확과 도망:** 변동성 거품(IV-RV Spread)이 낄 때는 배당을 수확하고, 추세가 터질 때는 상방이 열린 본주로 전환하여 기회비용을 최소화합니다.
- **개인형 인프라:** 복잡한 퀀트 이론을 직관적인 UI/UX로 풀어내어 가족 및 지인들이 쉽게 의사결정을 내릴 수 있도록 돕습니다.

---

## 2. 시스템 아키텍처 (System Architecture)

### 2.1 기술 스택 (Tech Stack)
- **Backend:** Python 3.10+, FastAPI (Asynchronous API)
- **Frontend:** Next.js 14, Tailwind CSS, TanStack Query (Orval 연동)
- **Database:** PostgreSQL (Time-series data), Redis (Caching)
- **Infra:** Docker & Docker Compose, WSL2 (Development), AWS EC2 (Production)
- **Library:** - `yfinance`, `ccxt` (Data Fetching)
  - `pandas`, `numpy`, `scipy` (Quantitative Analysis)
  - `filterpy` (Kalman Filter implementation)

### 2.2 데이터 파이프라인 (Data Pipeline)
1. **Source:**
   - **Deribit API:** BTC DVOL (Implied Volatility Index) 수집
   - **Binance/Upbit API:** BTC Real-time Price & Historical Data (Realized Volatility 계산)
   - **Yahoo Finance:** MSTR, MSTY, MSTU 가격 및 배당 데이터
2. **Processing:**
   - **VRP 계산:** `VRP = IV (DVOL) - RV (Rolling 20d)`
   - **NAV 분석:** MSTR 시총 / (보유 BTC 수량 * 현재가) 비율 산출
   - **Denoising:** 칼만 필터(Kalman Filter)를 통한 지표 노이즈 제거

---

## 3. 핵심 퀀트 로직 (Core Quantitative Logic)

### 3.1 국면 전환 모델 (Regime Switching Model)
시스템은 현재 시장을 4가지 국면으로 판정합니다.

| 국면 (Regime) | 판정 조건 | 권장 자산 배분 | 전략적 근거 |
| :--- | :--- | :--- | :--- |
| **수확 (Harvest)** | IV-RV Spread > 15% & 횡보장 | **MSTY 70% + 현금 30%** | 높은 변동성 프리미엄 수확 |
| **추세 (Trend)** | BTC 돌파 시그널 & NAV 안정 | **MSTR 100% (or MSTU)** | 상방 캡 제거 및 델타 노출 최대화 |
| **저평가 (Value)** | MSTR NAV Ratio < 1.0 | **MSTR 100%** | 기초자산 대비 저평가 해소 베팅 |
| **위험 (Risk-Off)** | BTC 지지선 이탈 & IV 급증 | **현금 100% (or Hedge)** | 하방 델타 리스크 완전 회피 |

---

## 4. UI/UX 및 알림 시스템 (Interface & Alert)

- **Main Dashboard:** - 현재 시장 국면 신호등 (Green/Yellow/Red)
  - VRP 히스토리 및 MSTR NAV 프리미엄 차트
  - 포트폴리오 리밸런싱 제안 가이드
- **Push Notification (Telegram/Discord):**
  - 국면 전환 발생 시 즉시 알림
  - 매일 아침 9시 '오늘의 시장 날씨' 브리핑

---

## 5. 단계별 개발 로드맵 (Roadmap)

### Phase 1: MVP & Core Engine (Week 1)
- [ ] WSL2 환경에 Docker/PostgreSQL 인프라 구축
- [ ] Deribit 및 Yahoo Finance 데이터 수집 스크립트 작성
- [ ] VRP 및 NAV Ratio 계산 모듈 개발
- [ ] 텔레그램 봇 기본 연동 (알림 기능)

### Phase 2: Analytics & Backtesting (Week 2-3)
- [ ] 과거 데이터를 활용한 전략 시뮬레이션 환경 구축
- [ ] 칼만 필터 기반의 지표 스무딩 로직 적용
- [ ] 국면 판정 알고리즘 고도화 및 파라미터 최적화

### Phase 3: Dashboard & Deployment (Week 4)
- [ ] FastAPI 기반 API 서버 구축 및 Swagger 문서화
- [ ] Orval을 활용한 프론트엔드 API 클라이언트 자동 생성
- [ ] Next.js 기반 반응형 웹 대시보드 제작
- [ ] AWS EC2 및 Docker Compose를 통한 최종 배포

---

## 6. 리스크 관리 및 면책 조항 (Disclaimer)
- 본 시스템은 과거 데이터를 기반으로 한 수리적 모델이며, 미래의 수익을 보장하지 않습니다.
- 커버드콜(MSTY)은 급격한 상승장에서 수익이 제한되고 하락장에서 원금이 손실될 수 있는 구조적 위험이 있습니다.
- 모든 투자의 최종 결정과 책임은 투자자 본인에게 있습니다.

---