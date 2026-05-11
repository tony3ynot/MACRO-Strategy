"""User-facing briefing message builders.

Three notification surfaces, each rendered by its own builder so the
voice stays consistent:

  1. `build_change_alert`   — fires when the strategy target actually
                              changes.  Action-first ("매도하세요"), with
                              share-count math if the user has set their
                              balance.
  2. `build_status_message` — on-demand /today reply.  Neutral status —
                              no urgent CTA — for users who just want to
                              check what they should be holding.
  3. `build_initial_message`— first message after /setbalance.  Treats
                              the user as new and frames the target as
                              "start here" instructions.

Plus `/detail`, `/history`, `/help`, `/pnl` builders.  All kept in this
file because they all share the same number-formatting and Korean
date helpers.
"""
from __future__ import annotations

import logging
from datetime import date, datetime
from typing import Any

import pandas as pd

logger = logging.getLogger(__name__)


WEEKDAYS_KO = ["월", "화", "수", "목", "금", "토", "일"]


def _ko_date(dt: pd.Timestamp | datetime | date) -> str:
    if isinstance(dt, pd.Timestamp):
        dt = dt.to_pydatetime()
    return f"{dt.month}월 {dt.day}일 ({WEEKDAYS_KO[dt.weekday()]})"


def _pct(x: float, signed: bool = False) -> str:
    fmt = "{:+.1f}%" if signed else "{:.1f}%"
    return fmt.format(x * 100)


TICKERS = ("MSTR", "MSTU", "MSTY", "MSTZ")


# ─── Render fragments ─────────────────────────────────────────────────


def _render_target_block(
    today_w: dict[str, float],
    balance: float | None = None,
    mstr_price: float | None = None,
) -> list[str]:
    """🎯 권장 비중 block.  Includes dollar amounts when balance is set."""
    out: list[str] = ["🎯 권장 비중", "━━━━━━━━━━━━━━━━"]
    cash_w = max(0.0, 1.0 - sum(today_w.values()))
    rows: list[tuple[str, float]] = [(t, today_w.get(t, 0.0)) for t in TICKERS if today_w.get(t, 0.0) >= 0.005]
    if cash_w >= 0.005 or not rows:
        rows.append(("현금", cash_w))

    for name, w in rows:
        line = f"  {name:<5}  {_pct(w):>6}"
        if balance is not None:
            dollar = balance * w
            line += f"   (${dollar:,.0f})"
            if name == "MSTR" and mstr_price is not None and mstr_price > 0:
                shares = int(round(dollar / mstr_price))
                line += f"  ≈ {shares}주"
        out.append(line)
    out.append("━━━━━━━━━━━━━━━━")
    return out


def _render_action_block(
    today_w: dict[str, float],
    prev_w: dict[str, float],
    balance: float | None = None,
    mstr_price: float | None = None,
) -> list[str]:
    """📋 매매 신호 block — deltas vs prev_w, with share counts."""
    out: list[str] = ["📋 매매 신호", "━━━━━━━━━━━━━━━━"]
    actions: list[str] = []

    def _qty_suffix(ticker: str, dollar_change: float) -> str:
        if balance is None or mstr_price is None or mstr_price <= 0:
            return ""
        if ticker == "MSTR":
            shares = int(round(abs(dollar_change) / mstr_price))
            return f"  ≈ {shares}주"
        return ""

    for t in TICKERS:
        w = today_w.get(t, 0.0)
        prev = prev_w.get(t, 0.0)
        dw = w - prev
        if abs(dw) < 0.005:
            continue
        if balance is not None:
            dollar = balance * dw
            qty = _qty_suffix(t, dollar)
            money = f"  (${abs(dollar):,.0f}{qty})"
        else:
            money = ""
        if w == 0 and prev > 0:
            actions.append(f"🔴 {t} 전량 매도  ({_pct(prev)} → 0%){money}")
        elif prev == 0 and w > 0:
            actions.append(f"🟢 {t} 신규 매수  (0% → {_pct(w)}){money}")
        elif dw > 0:
            actions.append(f"🟢 {t} 추가 매수  ({_pct(prev)} → {_pct(w)}){money}")
        else:
            actions.append(f"🟡 {t} 일부 매도  ({_pct(prev)} → {_pct(w)}){money}")

    cash_today = max(0.0, 1.0 - sum(today_w.values()))
    cash_prev = max(0.0, 1.0 - sum(prev_w.values()))
    if abs(cash_today - cash_prev) >= 0.005:
        arrow = "↑" if cash_today > cash_prev else "↓"
        actions.append(f"💵 현금 비중 {arrow}  ({_pct(cash_prev)} → {_pct(cash_today)})")

    if not actions:
        actions.append("✅ 변동 없음")

    out.extend(actions)
    out.append("━━━━━━━━━━━━━━━━")
    return out


def _render_market_prices(mstr_price: float | None, btc_price: float | None) -> list[str]:
    if mstr_price is None and btc_price is None:
        return []
    bits = []
    if mstr_price is not None:
        bits.append(f"MSTR ${mstr_price:.2f}")
    if btc_price is not None:
        bits.append(f"BTC ${btc_price:,.0f}")
    return ["📍 시장가: " + " · ".join(bits)]


def _render_msty_note(today_w: dict[str, float]) -> list[str]:
    """Show MSTY dividend timing reminder if MSTY is in the target.

    Ex-div is Thursday US (Friday morning KST) — must own through
    Wednesday US close to receive that week's distribution paid Friday.
    The cash deposits into the brokerage account and is *not*
    auto-reinvested; user must redeploy to maintain the target weight.
    """
    msty_w = today_w.get("MSTY", 0.0)
    if msty_w < 0.005:
        return []
    return [
        "💰 MSTY 배당 안내",
        "  • 매주 목요일(US) ex-div · 금요일(KST 오후) 입금",
        "  • 배당 받으려면 수요일 US 종가까지 MSTY 보유 필수",
        "  • 입금된 현금은 자동 재투자 X — 다음 주 초 MSTY 비중 복원 권장",
    ]


def _date_header(alert_date: date, signal_date: pd.Timestamp | date) -> str:
    """Show today's calendar date AND the data freshness date.

    Why both: users see the alert in their phone clock (today's KST date)
    but the signal is based on the last available US equity close, which
    may be 1-3 days behind on Mondays / post-holiday.  Showing both
    avoids the "왜 5/8 이 떠?" confusion when today is 5/11.
    """
    if isinstance(signal_date, pd.Timestamp):
        signal_date = signal_date.date()
    if signal_date == alert_date:
        return f"{_ko_date(alert_date)} 종가 기준"
    return f"{_ko_date(alert_date)} (시그널: {_ko_date(signal_date)} 종가 기준)"


# ─── Public builders ──────────────────────────────────────────────────


def build_change_alert(
    today_w: dict[str, float],
    prev_w: dict[str, float],
    alert_date: date,
    signal_date: pd.Timestamp | date,
    de_risked: bool,
    balance: float | None = None,
    mstr_price: float | None = None,
    btc_price: float | None = None,
) -> str:
    lines: list[str] = [
        f"🚨 매매 신호 — {_date_header(alert_date, signal_date)}",
        "",
    ]
    lines += _render_action_block(today_w, prev_w, balance=balance, mstr_price=mstr_price)
    lines.append("")
    if de_risked:
        lines.append("⚠️  위험 회피 모드 진입 (전략 보호장치 작동)")
        lines.append("")
    lines += _render_target_block(today_w, balance=balance, mstr_price=mstr_price)
    lines.append("")
    lines += _render_market_prices(mstr_price, btc_price)
    msty_note = _render_msty_note(today_w)
    if msty_note:
        lines.append("")
        lines += msty_note
    if balance is None:
        lines += [
            "",
            "💡 매매 수량을 자동 계산하려면:",
            "  /setbalance 10000  ← 본인 투자 자금 입력 (USD)",
        ]
    lines += [
        "",
        "아래 버튼으로 상세 정보 👇",
    ]
    return "\n".join(lines)


def build_status_message(
    today_w: dict[str, float],
    alert_date: date,
    signal_date: pd.Timestamp | date,
    last_signal_at: date | None = None,
    balance: float | None = None,
    mstr_price: float | None = None,
    btc_price: float | None = None,
) -> str:
    lines: list[str] = [
        f"📈 MACRO 전략 — {_date_header(alert_date, signal_date)}",
        "",
    ]
    lines += _render_target_block(today_w, balance=balance, mstr_price=mstr_price)
    lines.append("")
    if last_signal_at is not None:
        days = (alert_date - last_signal_at).days
        if days == 0:
            lines.append("🆕 오늘 매매 신호가 발생했어요. 위 비중대로 거래하세요.")
        else:
            day_str = "오늘" if days == 0 else f"{days}일 전"
            lines.append(f"✅ 마지막 신호: {_ko_date(last_signal_at)} ({day_str}) — 추가 매매 불필요")
        lines.append("")
    lines += _render_market_prices(mstr_price, btc_price)
    msty_note = _render_msty_note(today_w)
    if msty_note:
        lines.append("")
        lines += msty_note
    if balance is None:
        lines += [
            "",
            "💡 매매 수량을 자동 계산하려면:",
            "  /setbalance 10000  ← 본인 투자 자금 입력 (USD)",
        ]
    return "\n".join(lines)


def build_initial_message(
    today_w: dict[str, float],
    alert_date: date,
    signal_date: pd.Timestamp | date,
    balance: float,
    mstr_price: float | None = None,
    btc_price: float | None = None,
) -> str:
    lines: list[str] = [
        f"🎉 MACRO 전략 등록 완료 — {_ko_date(alert_date)}",
        "",
        f"📌 초기 자금: ${balance:,.0f}",
        f"📌 시그널 기준일: {_ko_date(signal_date)} 종가",
        "",
        "아래 권장 비중대로 *처음 매수*를 진행하세요:",
        "",
    ]
    lines += _render_target_block(today_w, balance=balance, mstr_price=mstr_price)
    lines.append("")
    lines += _render_market_prices(mstr_price, btc_price)
    msty_note = _render_msty_note(today_w)
    if msty_note:
        lines.append("")
        lines += msty_note
    lines += [
        "",
        "📋 앞으로의 안내",
        "  • 비중이 바뀌면 자동 알림 (/today 로 언제든 확인 가능)",
        "  • /pnl 로 배포 이후 수익률 추적",
        "  • /help 로 전략 설명",
    ]
    return "\n".join(lines)


SIGNAL_KEYBOARD: list[list[dict[str, str]]] = [
    [
        {"text": "📊 상세 지표", "callback_data": "show_detail"},
        {"text": "📈 누적 수익률", "callback_data": "show_history"},
    ],
    [
        {"text": "💼 내 수익률", "callback_data": "show_pnl"},
        {"text": "ℹ️ 전략 설명", "callback_data": "show_help"},
    ],
]


# ─── /detail handler ──────────────────────────────────────────────────


def build_detail_message(
    signal_date: pd.Timestamp,
    today_ind: pd.Series,
    today_w: dict[str, float],
    book_dd: float,
    trades_per_year: float,
) -> str:
    def _ind(label: str, value: Any, fmt: str = "{:.4f}", scale: float = 1.0) -> str:
        if value is None or pd.isna(value):
            return f"  {label:<10} —"
        return f"  {label:<10} {fmt.format(float(value) * scale)}"

    lines: list[str] = [
        f"📊 상세 지표 — {_ko_date(signal_date)} 종가 기준",
        "",
        "📍 가격 / 펀더멘털",
        _ind("MSTR",     today_ind.get("mstr_close"), "${:.2f}"),
        _ind("BTC",      today_ind.get("btc_close"),  "${:,.0f}"),
        _ind("mNAV",     today_ind.get("mnav"),       "{:.3f}"),
        "",
        "🔥 변동성 / VRP",
        _ind("MSTR RV20", today_ind.get("mstr_rv20"), "{:.1f}%", 100),
        _ind("MSTR IV30", today_ind.get("mstr_iv30"), "{:.1f}%", 100),
        _ind("BTC IV30",  today_ind.get("btc_iv30"),  "{:.1f}%", 100),
        _ind("BTC VRP",   today_ind.get("btc_vrp"),   "{:+.2f}%", 100),
        "",
        "📐 회귀 (BTC → MSTR)",
        _ind("β (IV)",         today_ind.get("beta_iv"),        "{:.2f}"),
        _ind("EquityPremium",  today_ind.get("equity_premium"), "{:+.2f}"),
        "",
        "💼 현재 권장 포지션",
    ]
    cash = max(0.0, 1.0 - sum(today_w.values()))
    for t in TICKERS:
        w = float(today_w.get(t, 0.0))
        if w >= 0.005:
            lines.append(f"  {t:<5} {_pct(w)}")
    if cash >= 0.005:
        lines.append(f"  현금   {_pct(cash)}")

    lines += [
        "",
        "📉 백테스트 누적 손실",
        f"  현재: {_pct(book_dd, signed=True)} (peak 대비)",
        "  ※ 2017년부터의 시뮬레이션 누적값입니다.",
        "    오늘 처음 시작한 분에게는 해당되지 않아요.",
        "",
        f"🔁 연간 거래 횟수: {trades_per_year:.0f}회 (5년 평균)",
    ]
    return "\n".join(lines)


# ─── /history handler ─────────────────────────────────────────────────


def _fmt_diff(strat: float, bh: float) -> str:
    diff = strat - bh
    return f" (BH {_pct(bh, signed=True)}, 차이 {diff*100:+.1f}pp)"


def build_history_message(report: dict) -> str:
    life = report["lifetime_strategy"]
    bh = report["lifetime_bh"]
    start = report["start_date"]
    end = report["end_date"]
    cost = report["cost_bps"]

    lines: list[str] = [
        f"📈 누적 수익률 (백테스트)",
        f"기간: {start} → {end}",
        f"슬리피지: {cost:.0f}bps/거래 (보수적 추정)",
        "",
        "━━━━━━━━━━━━━━━━",
        "🏆 전체 성과",
        "━━━━━━━━━━━━━━━━",
        f"  CAGR  : {_pct(life['cagr'], signed=True):>8}   (BH: {_pct(bh['cagr'], signed=True)})",
        f"  MDD   : {_pct(life['mdd'], signed=True):>8}   (BH: {_pct(bh['mdd'], signed=True)})",
        f"  Calmar: {life['calmar']:>+7.2f}    (BH: {bh['calmar']:+.2f})",
        f"  총수익: {_pct(life['total'], signed=True):>8}   (BH: {_pct(bh['total'], signed=True)})",
        "",
        "━━━━━━━━━━━━━━━━",
        "📅 연도별",
        "━━━━━━━━━━━━━━━━",
    ]
    for row in reversed(report["yearly"]):
        diff = row.strategy_return - row.bh_return
        lines.append(
            f"  {row.label:<10} {_pct(row.strategy_return, signed=True):>8}"
            f"   (BH {_pct(row.bh_return, signed=True)}, "
            f"{diff*100:+.1f}pp)"
        )
    lines += [
        "",
        "━━━━━━━━━━━━━━━━",
        "📆 최근 12개월",
        "━━━━━━━━━━━━━━━━",
    ]
    for row in report["monthly"]:
        diff = row.strategy_return - row.bh_return
        lines.append(
            f"  {row.label:<10} {_pct(row.strategy_return, signed=True):>8}"
            f"   (BH {_pct(row.bh_return, signed=True)}, "
            f"{diff*100:+.1f}pp)"
        )
    lines += [
        "",
        "※ BH = MSTR 단순 보유 (Buy-and-Hold) 비교군",
        "※ 모든 수치는 거래비용 차감 후 (net of fees)",
    ]
    return "\n".join(lines)


# ─── /pnl handler ─────────────────────────────────────────────────────


def build_pnl_message(
    deploy_date: date,
    today: date,
    initial_balance: float,
    strategy_value: float,
    bh_value: float,
) -> str:
    days = (today - deploy_date).days
    strat_ret = (strategy_value / initial_balance) - 1.0 if initial_balance > 0 else 0.0
    bh_ret = (bh_value / initial_balance) - 1.0 if initial_balance > 0 else 0.0
    edge = strat_ret - bh_ret
    return "\n".join([
        "💼 내 수익률 (배포 이후)",
        "",
        f"배포일: {deploy_date.isoformat()}",
        f"오늘:   {today.isoformat()} ({days}일 경과)",
        f"초기 자금: ${initial_balance:,.0f}",
        "",
        "━━━━━━━━━━━━━━━━",
        f"🏆 전략 가치: ${strategy_value:,.0f}  ({_pct(strat_ret, signed=True)})",
        f"🔵 BH MSTR:  ${bh_value:,.0f}  ({_pct(bh_ret, signed=True)})",
        "━━━━━━━━━━━━━━━━",
        f"차이: {edge*100:+.2f}pp" + ("  (전략 우위)" if edge > 0 else "  (BH 우위)" if edge < 0 else ""),
        "",
        "※ 모든 신호를 100% 이행했다고 가정한 수치입니다.",
        "※ 실제 거래시 슬리피지·세금에 의해 차이 있을 수 있어요.",
    ])


# ─── /help handler ────────────────────────────────────────────────────


HELP_MESSAGE = """ℹ️  MACRO 전략이란?

MicroStrategy(MSTR) 주식의 *mNAV (시가총액 / 보유 BTC 가치)* 를
중심으로 비중을 동적으로 조절하는 전략입니다.

📌 작동 원리 (간단히)
━━━━━━━━━━━━━━━━
• MSTR 가격이 보유 BTC 가치보다 저평가 (mNAV 낮음)
   → MSTR 비중 ↑
• 고평가 (mNAV 높음)
   → MSTR 비중 ↓ + 현금 보유
• BTC 옵션 시장의 위험 프리미엄(VRP)이 충분히 높을 때
   → MSTY (커버드콜 ETF) 추가
• 누적 손실이 -15% 이하로 깊어지면
   → 보호 장치(브레이커) 작동: 모든 비중 절반으로

📌 사용 명령어
━━━━━━━━━━━━━━━━
/setbalance 10000    — 내 투자 자금 등록 (USD, 매매 수량 자동 계산용)
/today               — 현재 권장 비중 확인
/detail              — 상세 지표 보기
/history             — 백테스트 누적 수익률
/pnl                 — 배포 이후 내 실현 수익률
/reset               — 등록 정보 초기화
/help                — 이 도움말

📌 처음 시작하는 분에게
━━━━━━━━━━━━━━━━
1. /setbalance 으로 본인 투자 자금 입력
2. 봇이 보내준 첫 매수 신호대로 증권사 앱에서 매수
3. 그 후엔 비중이 바뀔 때만 자동으로 알림이 옴
4. 매일 09:00 KST 기준으로 시그널 점검 (변동 없으면 침묵)

📌 주의사항
━━━━━━━━━━━━━━━━
• 백테스트 결과 ≠ 미래 수익 보장
• MDD(최대 손실)는 -46% 이상 발생 가능
• 본인 책임 하에 사용
• MSTR/MSTU/MSTY는 미국 주식 — 해외주식 계좌 필요

문의 / 버그 신고: jyp
"""


def build_help_message() -> str:
    return HELP_MESSAGE
