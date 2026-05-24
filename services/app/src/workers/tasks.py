"""Celery task wrappers around the synchronous ingestors.

Each task uses a small lookback window (`mode="daily"`) so the daily
incremental run is fast and idempotent. Backfill mode is reserved for
explicit CLI invocations (scripts/backfill_*).
"""
from __future__ import annotations

import logging
from datetime import date, timedelta

from connectors.binance_perp import BinanceFundingIngestor
from connectors.coinbase_btc import CoinbaseBTCDailyIngestor
from connectors.deribit_dvol import DeribitDVOLIngestor
from connectors.hyperliquid_perp import HyperliquidFundingIngestor
from connectors.sec_edgar import FIRST_BTC_PURCHASE, MSTRBTCHoldingsIngestor
from connectors.yfinance_equity import YFinanceEquityIngestor
from connectors.yieldmax import YieldMaxMSTYIngestor

from .celery_app import celery_app

logger = logging.getLogger(__name__)

DAILY_LOOKBACK = timedelta(days=5)


# ─── Equity / fundamentals ───────────────────────────────────────────────

@celery_app.task(name="workers.tasks.ingest_equity_daily")
def ingest_equity_daily() -> int:
    end = date.today()
    return YFinanceEquityIngestor().run(end - DAILY_LOOKBACK, end, mode="daily").rows


@celery_app.task(name="workers.tasks.ingest_mstr_holdings_daily")
def ingest_mstr_holdings_daily() -> int:
    end = date.today()
    # SEC EDGAR — full window, ingestor's UPSERT keeps it idempotent
    return MSTRBTCHoldingsIngestor().run(FIRST_BTC_PURCHASE, end, mode="daily").rows


@celery_app.task(name="workers.tasks.ingest_yieldmax_msty_weekly")
def ingest_yieldmax_msty_weekly() -> int:
    end = date.today()
    return YieldMaxMSTYIngestor().run(date(2024, 1, 1), end, mode="daily").rows


# ─── Crypto ──────────────────────────────────────────────────────────────

@celery_app.task(name="workers.tasks.ingest_btc_daily")
def ingest_btc_daily() -> int:
    end = date.today()
    return CoinbaseBTCDailyIngestor().run(end - DAILY_LOOKBACK, end, mode="daily").rows


@celery_app.task(name="workers.tasks.ingest_btc_dvol")
def ingest_btc_dvol() -> int:
    end = date.today()
    return DeribitDVOLIngestor().run(end - DAILY_LOOKBACK, end, mode="daily").rows


@celery_app.task(name="workers.tasks.ingest_binance_funding")
def ingest_binance_funding() -> int:
    end = date.today()
    return BinanceFundingIngestor().run(end - DAILY_LOOKBACK, end, mode="daily").rows


@celery_app.task(name="workers.tasks.ingest_hyperliquid_funding")
def ingest_hyperliquid_funding() -> int:
    end = date.today()
    return HyperliquidFundingIngestor().run(end - DAILY_LOOKBACK, end, mode="daily").rows


# ─── Quant indicators (Phase 2 D1) ───────────────────────────────────────

@celery_app.task(name="workers.tasks.compute_indicators_daily")
def compute_indicators_daily() -> int:
    """Recompute the trailing window of indicators_daily.

    30-day lookback covers the longest rolling window we use (20d) plus
    10 days of slack so any late-arriving data (DVOL backfill,
    distributions ROC) gets reflected.
    """
    from scripts.compute_indicators import (
        compute_indicators,
        fetch_mstr_shares_outstanding,
        load_base_data,
        upsert_indicators,
    )
    from core.db import make_sync_engine

    engine = make_sync_engine()
    start = date.today() - timedelta(days=30)
    data = load_base_data(engine, start - timedelta(days=40))
    shares_out = fetch_mstr_shares_outstanding()
    df = compute_indicators(data, shares_out)
    df = df.loc[df.index >= start]
    return upsert_indicators(engine, df)


@celery_app.task(name="workers.tasks.compute_mstr_iv30_daily")
def compute_mstr_iv30_daily() -> int:
    """Recompute the trailing 30 days of MSTR IV30 from Polygon options."""
    from scripts.compute_mstr_iv import (
        load_mstr_close,
        load_options_close,
        upsert_mstr_iv30,
    )
    from quant.indicators.mstr_iv import compute_mstr_iv30_daily as compute
    from quant.risk_free import fetch_dgs1mo_series
    from core.db import make_sync_engine

    engine = make_sync_engine()
    start = date.today() - timedelta(days=30)
    options = load_options_close(engine, start)
    mstr_close = load_mstr_close(engine, start)
    rates = fetch_dgs1mo_series()
    iv30 = compute(options, mstr_close, rates)
    return upsert_mstr_iv30(engine, iv30)


@celery_app.task(name="workers.tasks.compute_iv_decomposition")
def compute_iv_decomposition() -> int:
    """Refit β(t) and EquityPremium(t) from the latest MSTR / BTC IV.

    Cheap (closed-form rolling OLS over <500 rows), so we recompute the
    full series each tick — there's no incremental gain in scoping it
    to a lookback window."""
    from scripts.compute_iv_decomposition import (
        load_paired_iv,
        upsert_decomposition,
    )
    from quant.indicators.iv_decomposition import (
        DEFAULT_WINDOW,
        best_lag,
        compute_decomposition,
    )
    from core.db import make_sync_engine

    engine = make_sync_engine()
    mstr_iv, btc_iv = load_paired_iv(engine)
    chosen_lag, _ = best_lag(mstr_iv, btc_iv)
    decomp = compute_decomposition(mstr_iv, btc_iv, lag=chosen_lag, window=DEFAULT_WINDOW)
    valid = decomp.dropna(subset=["beta_iv", "equity_premium"], how="all")
    return upsert_decomposition(engine, valid)


# ─── Briefing & Telegram poller ─────────────────────────────────────────


@celery_app.task(name="workers.tasks.send_daily_briefing")
def send_daily_briefing() -> dict[str, int]:
    """Change-triggered broadcast at 09:00 KST.

    Replaces the old "always-fire daily briefing".  Now silent on days
    when the target weights don't change.  Users who want an explicit
    daily status can run /today on demand.

    Also resets the live-panic state machine.  The daily strategy is
    authoritative each morning — if it confirms panic, the next
    intraday cycle (~15 min after market open) will re-enter live
    panic.  If it doesn't, the false alarm is cleared and the
    change-trigger broadcast naturally pushes a "return to normal"
    signal because the target weights differ from the last sent ones.

    Returns broadcast stats (users seen / fired / skipped) for logging.
    """
    from quant import live_decision
    from workers.telegram_handlers import broadcast_change_alert

    try:
        live_decision.reset_all()
    except Exception:
        logger.exception("live_panic reset failed (continuing)")

    try:
        result = broadcast_change_alert()
    except Exception:
        logger.exception("send_daily_briefing: broadcast crashed")
        return {"error": 1}

    # Heartbeat: if no signals fired, send a silent "alive" line so users
    # can tell the difference between "no change today" and "the bot died".
    # Telegram `disable_notification` shows the line in history without
    # ringing — quiet days don't wake people up but a missing line for 2
    # consecutive days does.
    if result.get("fired", 0) == 0:
        from datetime import date

        from core import user_state
        from core.notifications.briefing import _ko_date
        from core.notifications.telegram import TelegramClient

        try:
            client = TelegramClient()
            heartbeat = (
                f"✅ MACRO 봇 가동 중 — {_ko_date(date.today())}\n"
                "오늘 매매 신호 없음 — 기존 비중 유지\n"
                "(매일 09시에 이 메시지가 안 오면 봇 점검 필요)"
            )
            sent = 0
            for cid in user_state.all_chat_ids():
                if not user_state.load(cid).is_configured:
                    continue
                client.send_message(
                    text=heartbeat,
                    parse_mode="",
                    chat_id=cid,
                    disable_notification=True,  # silent — chat history only
                )
                sent += 1
            result["heartbeats_sent"] = sent
        except Exception:
            logger.exception("heartbeat send failed")

    return result


@celery_app.task(name="workers.tasks.poll_telegram_updates")
def poll_telegram_updates() -> int:
    """Drain pending Telegram updates (button taps + slash commands).

    Runs frequently (~15s) via celery beat.  Each call is short — it
    calls getUpdates with timeout=0 (non-blocking) and only processes
    what's already queued at Telegram's side.
    """
    from workers.telegram_handlers import process_pending_updates

    try:
        return process_pending_updates()
    except Exception:
        logger.exception("poll_telegram_updates: top-level crash")
        return 0


# ─── Intraday (Phase 2a/2b) ────────────────────────────────────────────


def _us_market_open_now() -> bool:
    """Cheap heuristic: weekday + within 13:30-20:00 UTC window.

    13:30 UTC = 09:30 EDT (= 08:30 EST during winter — we accept the
    +1h drift since the alert engine is dedup'd per day).
    """
    from datetime import datetime, timezone
    now = datetime.now(timezone.utc)
    if now.weekday() >= 5:
        return False
    return 13 <= now.hour < 21


@celery_app.task(name="workers.tasks.ingest_intraday_prices")
def ingest_intraday_prices() -> dict[str, int]:
    """Pull latest tick for MSTR family (yfinance) + BTC (Coinbase).

    BTC always runs (24/7).  Equities only during US market hours so
    we don't burn requests on stale data.
    """
    from connectors.intraday_prices import (
        BTCIntradayIngestor,
        EquityIntradayIngestor,
    )
    from core.db import make_sync_engine

    engine = make_sync_engine()
    out: dict[str, int] = {}
    try:
        out["BTC"] = BTCIntradayIngestor(engine).run()
    except Exception:
        logger.exception("BTC intraday fetch failed")
        out["BTC"] = 0

    if _us_market_open_now():
        try:
            eq_out = EquityIntradayIngestor(engine).run()
            out.update(eq_out)
        except Exception:
            logger.exception("equity intraday fetch failed")

    return out


@celery_app.task(name="workers.tasks.check_intraday_alerts")
def check_intraday_alerts() -> dict[str, int]:
    """Run alert checks and broadcast to all configured users."""
    if not _us_market_open_now():
        return {"alerts": 0, "skipped": 1}
    from workers.intraday_alerts import fire_alerts
    try:
        return fire_alerts()
    except Exception:
        logger.exception("check_intraday_alerts: top-level crash")
        return {"error": 1}
