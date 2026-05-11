"""Telegram update routing — callback buttons and text commands.

Architecture:
  - Celery beat fires `poll_telegram_updates` every ~15s
  - Each fire calls getUpdates(offset = redis['telegram:last_update_id']+1)
  - Updates are routed via this module
  - Offset is advanced in Redis after each processed update
  - `broadcast_change_alert` is called by the daily compute pipeline
    when the strategy target changes; only users with a deploy_date get
    a push.

Reasons for polling over webhook:
  - Works behind NAT (dev box / WSL2)
  - No public TLS endpoint needed
  - Easy to switch off (just remove from beat schedule)
"""
from __future__ import annotations

import logging
import os
from datetime import date
from typing import Any

import pandas as pd
import redis

from core import user_state
from core.notifications.briefing import (
    SIGNAL_KEYBOARD,
    build_change_alert,
    build_detail_message,
    build_help_message,
    build_history_message,
    build_initial_message,
    build_pnl_message,
    build_status_message,
)
from core.notifications.telegram import TelegramClient

logger = logging.getLogger(__name__)

REDIS_OFFSET_KEY = "telegram:last_update_id"
ALLOWED_UPDATES = ["message", "callback_query"]


def _redis() -> redis.Redis:
    return redis.from_url(os.environ["REDIS_URL"])


def _load_offset(r: redis.Redis) -> int:
    raw = r.get(REDIS_OFFSET_KEY)
    return int(raw) if raw else 0


def _save_offset(r: redis.Redis, update_id: int) -> None:
    r.set(REDIS_OFFSET_KEY, update_id)


# ─── Shared backtest fetch ────────────────────────────────────────────


def _fetch_today_view() -> dict[str, Any]:
    """Single backtest run; everything else slices the result.

    Returned dict has:
      • today_w, prev_w        — target weight dicts
      • signal_date            — last index of indicators (US-close date)
      • alert_date             — today's KST date (caller may override)
      • mstr_price, btc_price  — floats or None
      • de_risked              — bool (5d exposure drop heuristic)
      • book_dd                — float (backtest equity peak)
      • indicators_row         — pd.Series for /detail
      • trades_per_year        — float
    """
    from core.db import make_sync_engine
    from quant.backtesting.data import assemble_full_panel
    from quant.backtesting.engine import run_backtest
    from quant.backtesting.strategies.macro_trend_v5 import (
        make_macro_trend_v5_with_breaker,
    )

    engine = make_sync_engine()
    panel, indicators = assemble_full_panel(engine)
    res = run_backtest(
        name="view",
        panel=panel,
        indicators=indicators,
        strategy=make_macro_trend_v5_with_breaker(),
        cost_bps=10.0,
    )

    today_w = {k: float(v) for k, v in res.weights.iloc[-1].items()}
    prev_w = (
        {k: float(v) for k, v in res.weights.iloc[-2].items()}
        if len(res.weights) > 1
        else today_w
    )

    signal_idx = indicators.index[-1]
    today_ind = indicators.loc[signal_idx]

    final_eq = float(res.equity.iloc[-1])
    peak_eq = float(res.equity.cummax().iloc[-1])
    book_dd = (final_eq / peak_eq - 1.0) if peak_eq > 0 else 0.0

    recent_total = res.weights.tail(5).sum(axis=1).mean()
    today_total = float(sum(today_w.values()))
    de_risked = today_total < recent_total - 0.10

    def _scalar(v: Any) -> float | None:
        if v is None or pd.isna(v):
            return None
        return float(v)

    return {
        "today_w": today_w,
        "prev_w": prev_w,
        "signal_date": signal_idx,
        "alert_date": date.today(),
        "mstr_price": _scalar(today_ind.get("mstr_close")),
        "btc_price": _scalar(today_ind.get("btc_close")),
        "de_risked": de_risked,
        "book_dd": book_dd,
        "indicators_row": today_ind,
        "trades_per_year": res.metrics["trades"] / 5.1,
    }


# ─── Message builders for callbacks / commands ────────────────────────


def _build_status_for(chat_id: int) -> tuple[str, list[list[dict[str, str]]]]:
    view = _fetch_today_view()
    state = user_state.load(chat_id)
    text = build_status_message(
        today_w=view["today_w"],
        alert_date=view["alert_date"],
        signal_date=view["signal_date"],
        last_signal_at=state.last_signal_at,
        balance=state.balance,
        mstr_price=view["mstr_price"],
        btc_price=view["btc_price"],
    )
    return text, SIGNAL_KEYBOARD


def _build_detail_for(_chat_id: int) -> str:
    view = _fetch_today_view()
    return build_detail_message(
        signal_date=view["signal_date"],
        today_ind=view["indicators_row"],
        today_w=view["today_w"],
        book_dd=view["book_dd"],
        trades_per_year=view["trades_per_year"],
    )


def _build_history(_chat_id: int) -> str:
    from core.db import make_sync_engine
    from quant.backtesting.data import assemble_full_panel
    from quant.backtesting.reports import build_period_report

    engine = make_sync_engine()
    panel, indicators = assemble_full_panel(engine)
    report = build_period_report(panel, indicators, cost_bps=15.0)
    return build_history_message(report)


def _build_pnl_for(chat_id: int) -> str:
    state = user_state.load(chat_id)
    if not state.is_configured:
        return (
            "💡 아직 자금을 등록하지 않으셨어요.\n"
            "/setbalance 10000  ← 본인 투자 자금 입력 (USD)\n"
            "후에 /pnl 이 활성화됩니다."
        )

    from core.db import make_sync_engine
    from quant.backtesting.data import assemble_full_panel
    from quant.backtesting.engine import run_backtest
    from quant.backtesting.strategies.benchmarks import buy_and_hold
    from quant.backtesting.strategies.macro_trend_v5 import (
        make_macro_trend_v5_with_breaker,
    )

    engine = make_sync_engine()
    panel, indicators = assemble_full_panel(engine)
    deploy = state.deploy_date
    if deploy is None:
        return "💡 배포일이 없어 수익률을 계산할 수 없어요. /setbalance 를 다시 한 번 실행해 주세요."

    last_available = panel.index.max().date()
    if deploy > last_available:
        days_until = (deploy - last_available).days
        return (
            f"💡 배포일({deploy.isoformat()})이 최신 시장 데이터({last_available.isoformat()}) 이후입니다.\n"
            f"수익률은 거래일 데이터가 들어온 뒤부터 측정돼요. (약 {days_until}일 후)"
        )
    days_in = (last_available - deploy).days
    if days_in < 1:
        return (
            f"💡 배포 첫날입니다 ({deploy.isoformat()}).\n"
            "수익률은 다음 거래일 종가가 들어와야 측정됩니다.\n"
            "내일 다시 /pnl 을 확인해 주세요."
        )

    strat_res = run_backtest(
        name="pnl_strategy",
        panel=panel, indicators=indicators,
        strategy=make_macro_trend_v5_with_breaker(),
        cost_bps=15.0,
        start_date=deploy,
        seed=state.balance,
    )
    bh_res = run_backtest(
        name="pnl_bh",
        panel=panel, indicators=indicators,
        strategy=buy_and_hold("MSTR"),
        cost_bps=15.0,
        start_date=deploy,
        seed=state.balance,
    )
    today = strat_res.equity.index[-1].date()
    return build_pnl_message(
        deploy_date=deploy,
        today=today,
        initial_balance=state.balance,
        strategy_value=float(strat_res.equity.iloc[-1]),
        bh_value=float(bh_res.equity.iloc[-1]),
    )


# ─── Update routing ───────────────────────────────────────────────────


CALLBACK_HANDLERS = {
    "show_detail":  _build_detail_for,
    "show_history": _build_history,
    "show_pnl":     _build_pnl_for,
    "show_help":    lambda _cid: build_help_message(),
}


def _handle_callback_query(client: TelegramClient, query: dict) -> None:
    cb_id = query["id"]
    data = query.get("data", "")
    chat_id = query["message"]["chat"]["id"]
    user_state.remember_chat(chat_id)

    client.answer_callback_query(cb_id, text="처리 중...")

    handler = CALLBACK_HANDLERS.get(data)
    if handler is None:
        client.send_plain(f"알 수 없는 동작: {data}", chat_id=chat_id)
        return
    try:
        text = handler(chat_id)
    except Exception as exc:
        logger.exception("callback handler crashed: %s", data)
        text = f"⚠️ 오류: {exc}"
    client.send_plain(text, chat_id=chat_id)


# ─── Slash commands ───────────────────────────────────────────────────


def _cmd_today(client: TelegramClient, chat_id: int, _args: list[str]) -> None:
    text, kb = _build_status_for(chat_id)
    client.send_with_keyboard(text, kb, chat_id=chat_id)


def _cmd_detail(client: TelegramClient, chat_id: int, _args: list[str]) -> None:
    client.send_plain(_build_detail_for(chat_id), chat_id=chat_id)


def _cmd_history(client: TelegramClient, chat_id: int, _args: list[str]) -> None:
    client.send_plain(_build_history(chat_id), chat_id=chat_id)


def _cmd_pnl(client: TelegramClient, chat_id: int, _args: list[str]) -> None:
    client.send_plain(_build_pnl_for(chat_id), chat_id=chat_id)


def _cmd_help(client: TelegramClient, chat_id: int, _args: list[str]) -> None:
    client.send_plain(build_help_message(), chat_id=chat_id)


def _parse_balance(args: list[str]) -> float | None:
    if not args:
        return None
    raw = args[0].replace(",", "").replace("_", "").strip().lstrip("$")
    try:
        v = float(raw)
        return v if v > 0 else None
    except ValueError:
        return None


def _cmd_setbalance(client: TelegramClient, chat_id: int, args: list[str]) -> None:
    balance = _parse_balance(args)
    if balance is None:
        client.send_plain(
            "사용법: /setbalance <USD>\n"
            "예: /setbalance 10000",
            chat_id=chat_id,
        )
        return

    user_state.set_balance(chat_id, balance)
    view = _fetch_today_view()
    state = user_state.load(chat_id)
    user_state.record_signal(chat_id, view["today_w"], view["signal_date"].date())

    text = build_initial_message(
        today_w=view["today_w"],
        alert_date=view["alert_date"],
        signal_date=view["signal_date"],
        balance=balance,
        mstr_price=view["mstr_price"],
        btc_price=view["btc_price"],
    )
    client.send_with_keyboard(text, SIGNAL_KEYBOARD, chat_id=chat_id)


def _cmd_reset(client: TelegramClient, chat_id: int, _args: list[str]) -> None:
    user_state.reset_deploy(chat_id)
    client.send_plain(
        "✅ 등록 정보를 초기화했어요.\n"
        "/setbalance 으로 다시 시작할 수 있어요.",
        chat_id=chat_id,
    )


COMMANDS = {
    "/start":      _cmd_today,
    "/today":      _cmd_today,
    "/signal":     _cmd_today,
    "/detail":     _cmd_detail,
    "/history":    _cmd_history,
    "/pnl":        _cmd_pnl,
    "/help":       _cmd_help,
    "/setbalance": _cmd_setbalance,
    "/reset":      _cmd_reset,
}


def _handle_command(client: TelegramClient, msg: dict) -> None:
    text = msg.get("text", "").strip()
    chat_id = msg["chat"]["id"]
    user_state.remember_chat(chat_id)

    parts = text.split()
    if not parts:
        return
    cmd = parts[0].split("@")[0].lower()  # strip @botname
    args = parts[1:]

    handler = COMMANDS.get(cmd)
    if handler is None:
        client.send_plain(
            "사용 가능한 명령:\n"
            "  /today        — 오늘의 권장 비중\n"
            "  /detail       — 상세 지표\n"
            "  /history      — 백테스트 누적 수익률\n"
            "  /pnl          — 내 실현 수익률\n"
            "  /setbalance N — 투자 자금 등록 (USD)\n"
            "  /reset        — 초기화\n"
            "  /help         — 도움말",
            chat_id=chat_id,
        )
        return
    try:
        handler(client, chat_id, args)
    except Exception as exc:
        logger.exception("command crashed: %s", cmd)
        client.send_plain(f"⚠️ 오류: {exc}", chat_id=chat_id)


def process_pending_updates() -> int:
    """Drain Telegram's pending updates queue.  Returns count processed."""
    client = TelegramClient()
    if not client.is_configured:
        return 0

    r = _redis()
    offset = _load_offset(r)
    updates = client.get_updates(
        offset=offset + 1 if offset else 0,
        timeout=0,
        allowed_updates=ALLOWED_UPDATES,
    )
    if not updates:
        return 0

    max_id = offset
    processed = 0
    for upd in updates:
        upd_id: int = upd["update_id"]
        try:
            if "callback_query" in upd:
                _handle_callback_query(client, upd["callback_query"])
            elif "message" in upd:
                msg = upd["message"]
                if "text" in msg and msg["text"].startswith("/"):
                    _handle_command(client, msg)
        except Exception:
            logger.exception("update %s handler crashed; advancing anyway", upd_id)
        max_id = max(max_id, upd_id)
        processed += 1

    _save_offset(r, max_id)
    return processed


# ─── Change-triggered daily broadcast ─────────────────────────────────


def broadcast_change_alert() -> dict[str, int]:
    """Run the strategy, broadcast to every user whose target changed.

    Skips users without a deploy_date (they haven't /setbalance'd yet).
    Updates each user's `last_target` so we don't re-fire the same
    signal on the next run.

    Returns a small dict of counters for logging / smoke-testing.
    """
    client = TelegramClient()
    if not client.is_configured:
        return {"users": 0, "fired": 0, "skipped_no_state": 0, "skipped_no_change": 0}

    view = _fetch_today_view()
    chat_ids = user_state.all_chat_ids()
    stats = {"users": len(chat_ids), "fired": 0, "skipped_no_state": 0, "skipped_no_change": 0}

    for cid in chat_ids:
        state = user_state.load(cid)
        if not state.is_configured:
            stats["skipped_no_state"] += 1
            continue
        if not user_state.is_new_target(cid, view["today_w"]):
            stats["skipped_no_change"] += 1
            continue

        text = build_change_alert(
            today_w=view["today_w"],
            prev_w=state.positions or view["prev_w"],
            alert_date=view["alert_date"],
            signal_date=view["signal_date"],
            de_risked=view["de_risked"],
            balance=state.balance,
            mstr_price=view["mstr_price"],
            btc_price=view["btc_price"],
        )
        try:
            client.send_with_keyboard(text, SIGNAL_KEYBOARD, chat_id=cid)
            user_state.record_signal(cid, view["today_w"], view["signal_date"].date())
            stats["fired"] += 1
        except Exception:
            logger.exception("alert send failed for chat=%s", cid)

    logger.info("broadcast_change_alert: %s", stats)
    return stats
