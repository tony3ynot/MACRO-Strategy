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


# ─── Briefing (Phase 1: stub; Phase 2: indicator-driven) ─────────────────

@celery_app.task(name="workers.tasks.send_daily_briefing")
def send_daily_briefing() -> str:
    """Build and send the 09:00 KST briefing.

    Phase 1: minimal stub that proves the pipeline (ingestors → DB → bot).
    Phase 2: replaces `_build_briefing_body` with indicator-driven output
    (regime, VRP, mNAV, EquityPremium, allocation diff vs current).

    The briefing FORMAT is intentionally not fixed yet — we'll refine
    template + content with the user before locking in.
    """
    from core.notifications.telegram import TelegramClient

    body = _build_briefing_body()
    client = TelegramClient()
    if not client.is_configured:
        logger.info("Telegram not configured — briefing built but not sent:\n%s", body)
        return body
    client.send_plain(body)
    return body


def _build_briefing_body() -> str:
    """STUB. Real content TBD with user. Keep this function easy to swap."""
    return (
        "MACRO Strategy — Daily Briefing\n"
        f"({date.today().isoformat()})\n"
        "\n"
        "Phase 1 infrastructure online. Regime indicators arrive in Phase 2.\n"
        "(briefing template under review — content will be replaced)"
    )
