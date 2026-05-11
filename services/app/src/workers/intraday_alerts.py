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
PANIC_DD_THRESHOLD = -0.15        # live book DD ≤ -15 %
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


def check_pre_close_panic(engine: Engine) -> Alert | None:
    """Live book DD warning — uses today's session move + yesterday's equity.

    The proper panic decision is made by the daily strategy after the
    market close.  But if MSTR is down a lot intraday and the strategy
    is already underwater, the user benefits from knowing the panic
    state is *likely* to flip today, before the formal compute runs
    overnight.

    We approximate live book DD by:
      • Re-running today's backtest with intraday MSTR substituted for
        the latest daily close.  Cheap because we already cache the
        result.

    Simpler MVP heuristic used here: take the most recent backtest
    equity, scale it by today's intraday MSTR move weighted by the
    current target's MSTR position, and check the resulting DD.
    """
    from quant.backtesting.data import assemble_full_panel
    from quant.backtesting.engine import run_backtest
    from quant.backtesting.strategies.macro_trend_v5 import (
        make_macro_trend_v5_with_breaker,
    )

    panel, indicators = assemble_full_panel(engine)
    res = run_backtest(
        name="live_panic_check",
        panel=panel, indicators=indicators,
        strategy=make_macro_trend_v5_with_breaker(),
        cost_bps=10.0,
    )
    today_w = res.weights.iloc[-1]
    mstr_w = float(today_w.get("MSTR", 0.0))
    if mstr_w <= 0.005:
        return None

    move = session_move_pct(engine, "MSTR") or 0.0
    final_eq = float(res.equity.iloc[-1])
    peak_eq = float(res.equity.cummax().iloc[-1])
    # Approx live equity: today's close PnL = mstr_w * intraday move
    live_eq = final_eq * (1.0 + mstr_w * move)
    live_dd = (live_eq / peak_eq - 1.0) if peak_eq > 0 else 0.0

    if live_dd > PANIC_DD_THRESHOLD:
        return None
    key = f"alert:preclose_panic:{_today_utc()}"
    if _already_fired(key):
        return None
    _mark_fired(key)

    return Alert(
        severity="danger",
        title="장중 패닉 임박 — 보호장치 발동 가능",
        body=(
            f"현재 book DD (장중 추정): {live_dd*100:+.1f}%\n"
            f"MSTR 장중: {move*100:+.2f}%\n\n"
            "오늘 종가에 -15% 임계를 넘으면 전략 보호장치가 작동해\n"
            "내일 아침 시그널에서 비중이 절반으로 줄어들 수 있어요.\n\n"
            "지금 당장 매매를 권장하는 건 아니지만,\n"
            "장 마감 전 가격 동향을 한 번 더 확인하세요."
        ),
    )


# ─── Broadcast ────────────────────────────────────────────────────────


def fire_alerts() -> dict[str, int]:
    """Run all alert checks; broadcast to every configured user."""
    client = TelegramClient()
    if not client.is_configured:
        return {"alerts": 0, "users": 0}

    engine = make_sync_engine()
    alerts: list[Alert] = []
    for check in (check_big_move, check_mnav_bucket, check_pre_close_panic):
        try:
            a = check(engine)
        except Exception:
            logger.exception("alert check failed: %s", check.__name__)
            continue
        if a is not None:
            alerts.append(a)

    if not alerts:
        return {"alerts": 0, "users": 0}

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

    logger.info("intraday alerts: %s fired across %s users", fired, len(chat_ids))
    return {"alerts": len(alerts), "users": len(chat_ids), "sent": fired}
