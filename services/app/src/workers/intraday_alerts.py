"""Intraday alert engine — runs every few minutes during US hours.

Three alert types, each with its own dedup window:

  1. Big move (MSTR ±5% from session open)
     - Dedup: once per day per direction (up/down)
     - Fires anytime intraday move crosses threshold

  2. mNAV bucket crossing (1.00, 1.20, 1.50, 2.00)
     - Dedup: once per day per bucket boundary
     - These are the boundaries inside `_mstr_base` in macro_trend_v5;
       crossing them changes the target weight at next daily close.

  3. Pre-close panic warning (live book DD ≤ -15%)
     - Dedup: once per day
     - Fires only if the strategy would enter panic if today closed
       at the current intraday price.  Gives the user a heads-up
       before the daily compute formalises the panic state.

Dedup state is in Redis with keys `alert:{type}:{date}:{key}` and
24h TTL.  Sending alerts goes through TelegramClient.send_with_keyboard
so users get the standard buttons for follow-up.
"""
from __future__ import annotations

import logging
import os
from dataclasses import dataclass
from datetime import datetime, timezone

import redis
from sqlalchemy.engine import Engine

from core import user_state
from core.db import make_sync_engine
from core.notifications.briefing import SIGNAL_KEYBOARD
from core.notifications.telegram import TelegramClient
from quant.intraday import (
    latest_tick,
    live_mnav,
    session_move_pct,
)

logger = logging.getLogger(__name__)


# ─── Thresholds ───────────────────────────────────────────────────────


BIG_MOVE_THRESHOLD = 0.05         # ±5 % vs session open
MNAV_BUCKETS = (0.95, 1.20, 1.50, 2.00)
DEDUP_TTL_SECONDS = 24 * 3600


def _r() -> redis.Redis:
    return redis.from_url(os.environ["REDIS_URL"], decode_responses=True)


def _today_utc() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d")


def _already_fired(key: str) -> bool:
    return _r().exists(key) > 0


def _mark_fired(key: str) -> None:
    _r().set(key, "1", ex=DEDUP_TTL_SECONDS)


# ─── Alert builders ───────────────────────────────────────────────────


@dataclass
class Alert:
    severity: str          # "info" | "warn" | "danger"
    title: str
    body: str

    def render(self) -> str:
        emoji = {"info": "🔔", "warn": "⚠️", "danger": "🚨"}.get(self.severity, "🔔")
        return f"{emoji} {self.title}\n\n{self.body}"


def check_big_move(engine: Engine) -> Alert | None:
    move = session_move_pct(engine, "MSTR")
    if move is None or abs(move) < BIG_MOVE_THRESHOLD:
        return None
    direction = "up" if move > 0 else "down"
    key = f"alert:bigmove:{_today_utc()}:MSTR:{direction}"
    if _already_fired(key):
        return None

    tick = latest_tick(engine, "MSTR")
    arrow = "📈" if move > 0 else "📉"
    title = f"MSTR 장중 {move*100:+.1f}% — 큰 움직임 감지"
    body_lines = [
        f"{arrow} 현재가: ${tick.close:.2f}" if tick else "",
        f"세션 시작 대비: {move*100:+.2f}%",
        "",
        "원인 가능성:",
        "  • BTC 큰 움직임",
        "  • 회사 뉴스 (10-Q, 채권 발행 등)",
        "  • 매크로 이벤트",
        "",
        "전략 의사결정은 종가 기준이라 즉시 매매는 안 해도 됨.",
        "단, MA50/mNAV 경계 근처면 종가에 비중 바뀔 수 있음.",
    ]
    _mark_fired(key)
    return Alert(
        severity="warn" if abs(move) < 0.10 else "danger",
        title=title,
        body="\n".join(line for line in body_lines if line is not None and line != ""),
    )


def check_mnav_bucket(engine: Engine) -> Alert | None:
    """Fire when live mNAV crosses one of the strategy's bucket boundaries.

    Bucket boundaries (from _mstr_base in macro_trend_v5):
      ≤0.95  → MSTR 100%
       0.95-1.20 → MSTR 90%
       1.20-1.50 → MSTR 80%
       1.50-2.00 → MSTR 65%
       >2.00   → MSTR 50%

    We compare *live* mNAV to the last-recorded *daily* mNAV; if they
    sit on different sides of a bucket boundary, that's a crossing.
    """
    live = live_mnav(engine)
    if live is None:
        return None
    # Daily reference
    from sqlalchemy import text
    with engine.connect() as conn:
        row = conn.execute(text("""
            SELECT mnav FROM indicators_daily
            WHERE mnav IS NOT NULL
            ORDER BY date DESC LIMIT 1
        """)).fetchone()
    if row is None:
        return None
    daily = float(row.mnav)

    crossed: tuple[float, str] | None = None
    for b in MNAV_BUCKETS:
        if (daily < b <= live):
            crossed = (b, "up")
            break
        if (daily > b >= live):
            crossed = (b, "down")
            break
    if crossed is None:
        return None
    bucket, direction = crossed

    key = f"alert:mnav:{_today_utc()}:{bucket:.2f}:{direction}"
    if _already_fired(key):
        return None
    _mark_fired(key)

    dir_word = "상향" if direction == "up" else "하향"
    return Alert(
        severity="info",
        title=f"mNAV {bucket:.2f} {dir_word} 돌파",
        body=(
            f"오늘 일중 mNAV: {live:.3f}\n"
            f"전일 종가 mNAV: {daily:.3f}\n\n"
            f"전략 비중 경계({bucket:.2f})를 {dir_word}했어요.\n"
            "오늘 종가 확정 시 권장 MSTR 비중이 한 단계 "
            f"{'줄어들' if direction == 'up' else '늘어날'} 가능성이 있습니다.\n"
            "(종가 데이터가 들어와야 정식 시그널이 나옵니다)"
        ),
    )


def compute_live_dd(engine: Engine) -> tuple[float, float, dict] | None:
    """Approximate book DD using today's intraday MSTR move.

    Returns (live_dd, mstr_intraday_move, today_w_dict) or None if
    insufficient data.

    Logic:
      live_eq  = backtest_final_eq × (1 + mstr_weight × intraday_move)
      live_dd  = live_eq / peak_eq − 1

    This is a first-order approximation — it doesn't recompute the
    full equity curve with intraday data, just adjusts today's
    marked-to-market PnL by the MSTR move scaled by the current
    MSTR target weight.  For panic-trigger purposes that's enough.
    """
    from quant.backtesting.data import assemble_full_panel
    from quant.backtesting.engine import run_backtest
    from quant.backtesting.strategies.macro_trend_v5 import (
        make_macro_trend_v5_with_breaker,
    )

    panel, indicators = assemble_full_panel(engine)
    res = run_backtest(
        name="live_dd",
        panel=panel, indicators=indicators,
        strategy=make_macro_trend_v5_with_breaker(),
        cost_bps=10.0,
    )
    today_w = {k: float(v) for k, v in res.weights.iloc[-1].items()}
    mstr_w = today_w.get("MSTR", 0.0)
    move = session_move_pct(engine, "MSTR") or 0.0
    final_eq = float(res.equity.iloc[-1])
    peak_eq = float(res.equity.cummax().iloc[-1])
    live_eq = final_eq * (1.0 + mstr_w * move)
    live_dd = (live_eq / peak_eq - 1.0) if peak_eq > 0 else 0.0
    return live_dd, move, today_w


def evaluate_live_panic(engine: Engine) -> tuple[bool, Alert | None]:
    """Run the live-panic state machine for one observation.

    Returns (did_just_activate, optional_alert).  If state transitioned
    inactive → active, returns an action alert; otherwise returns None.

    Daily strategy might already be in panic for today; in that case
    we don't fire (nothing to fast-forward).
    """
    from quant import live_decision

    out = compute_live_dd(engine)
    if out is None:
        return False, None
    live_dd, move, today_w = out

    # Sanity check: skip if the daily strategy has already cut weights
    # below its own pre-panic baseline.  Heuristic: total weight < 0.7
    # means we're either already panic'd or normally underweight; either
    # way, the live trigger adds nothing.
    if sum(today_w.values()) < 0.70:
        return False, None

    activated = live_decision.observe_dd(live_dd)
    if not activated:
        return False, None

    # Build action alert with panic-applied weights
    panic_w = live_decision.apply_live_panic(today_w)
    lines = ["🚨🚨 장중 즉시 매도 신호 — 보호장치 작동", ""]
    lines.append(
        f"장중 책 DD가 -15 % 임계를 15분 이상 지속해서 보호장치가 발동했습니다."
    )
    lines.append(f"  현재 추정 book DD: {live_dd*100:+.1f} %")
    lines.append(f"  MSTR 장중: {move*100:+.2f} %")
    lines.append("")
    lines.append("📋 즉시 조정 권장 비중 (모든 포지션 × 0.5)")
    lines.append("━━━━━━━━━━━━━━━━")
    for t in ("MSTR", "MSTU", "MSTY", "MSTZ"):
        if panic_w.get(t, 0.0) >= 0.005:
            lines.append(f"  {t:<5}  {panic_w[t]*100:5.1f} %")
    cash = max(0.0, 1.0 - sum(panic_w.values()))
    if cash >= 0.005:
        lines.append(f"  현금   {cash*100:5.1f} %")
    lines.append("━━━━━━━━━━━━━━━━")
    lines.append("")
    lines.append("• 본 신호는 일봉 종가 전에 발동된 *예방적* 행동입니다.")
    lines.append("• 내일 아침 일봉 시그널이 확인 → 비중 유지")
    lines.append("• 내일 일봉 시그널이 반대 → false alarm으로 자동 복귀 신호")

    return True, Alert(severity="danger", title="LIVE PANIC", body="\n".join(lines))


# ─── Broadcast ────────────────────────────────────────────────────────


def fire_alerts() -> dict[str, int]:
    """Run all alert checks; broadcast to every configured user.

    Order matters:
      1. evaluate_live_panic — the *action* trigger; if it transitions
         to active, the panic state is set in Redis and we follow up
         with a `broadcast_change_alert` so every user gets the new
         target weights (not just an informational warning).
      2. Informational alerts (big-move, mNAV bucket) follow as usual.

    Live-panic warning ≠ live-panic action.  We no longer fire the old
    "장중 패닉 임박" pre-close warning — it's been superseded by
    evaluate_live_panic which actually changes the recommended target.
    """
    client = TelegramClient()
    if not client.is_configured:
        return {"alerts": 0, "users": 0, "panic_activated": 0}

    engine = make_sync_engine()
    panic_activated = False
    panic_alert: Alert | None = None
    try:
        panic_activated, panic_alert = evaluate_live_panic(engine)
    except Exception:
        logger.exception("evaluate_live_panic failed")

    alerts: list[Alert] = []
    for check in (check_big_move, check_mnav_bucket):
        try:
            a = check(engine)
        except Exception:
            logger.exception("alert check failed: %s", check.__name__)
            continue
        if a is not None:
            alerts.append(a)
    if panic_alert is not None:
        # Surface the panic alert at the TOP regardless of dedup.
        alerts.insert(0, panic_alert)

    chat_ids = [c for c in user_state.all_chat_ids()
                if user_state.load(c).is_configured]
    fired = 0
    for cid in chat_ids:
        for a in alerts:
            try:
                client.send_with_keyboard(a.render(), SIGNAL_KEYBOARD, chat_id=cid)
                fired += 1
            except Exception:
                logger.exception("alert send failed: chat=%s", cid)

    # If live panic just activated, immediately broadcast a *change-alert*
    # so each user sees the new target weights (with panic_scale applied).
    if panic_activated:
        from workers.telegram_handlers import broadcast_change_alert
        try:
            broadcast_change_alert()
        except Exception:
            logger.exception("post-panic broadcast failed")

    return {
        "alerts": len(alerts),
        "users": len(chat_ids),
        "sent": fired,
        "panic_activated": int(panic_activated),
    }
