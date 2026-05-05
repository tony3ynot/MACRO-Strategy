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
    """Build the daily briefing from the production allocator (v5_breaker).

    Runs the full backtest end-to-end so the panic-mode flag and book
    drawdown are computed from the same equity curve a live user would
    have experienced. Reads today's recommended weights, deltas vs
    yesterday, latest indicators, and the strategy's own drawdown.
    """
    import pandas as pd

    from core.db import make_sync_engine
    from quant.backtesting.data import assemble_full_panel
    from quant.backtesting.engine import run_backtest
    from quant.backtesting.strategies.macro_trend_v5 import (
        make_macro_trend_v5_with_breaker,
    )

    engine = make_sync_engine()
    try:
        panel, indicators = assemble_full_panel(engine)
    except Exception as exc:
        logger.exception("briefing: panel assembly failed")
        return f"MACRO Strategy briefing failed: {exc}"

    res = run_backtest(
        name="briefing",
        panel=panel,
        indicators=indicators,
        strategy=make_macro_trend_v5_with_breaker(),
        cost_bps=10.0,
    )

    today_w = res.weights.iloc[-1]
    prev_w = res.weights.iloc[-2] if len(res.weights) > 1 else today_w
    cash_today = max(0.0, 1.0 - today_w.sum())
    cash_prev = max(0.0, 1.0 - prev_w.sum())

    today_idx = indicators.index[-1]
    today_ind = indicators.loc[today_idx]

    final_eq = float(res.equity.iloc[-1])
    peak_eq = float(res.equity.cummax().iloc[-1])
    book_dd = (final_eq / peak_eq - 1.0) if peak_eq > 0 else 0.0

    # Compare today's weights to a 5-day moving average — large drops signal
    # the breaker / hedge has fired regardless of which gate triggered.
    recent_total = res.weights.tail(5).sum(axis=1).mean()
    today_total = float(today_w.sum())
    de_risked = today_total < recent_total - 0.10  # ≥10pp gross-exposure drop

    def _fmt_w(name: str, w: float, prev: float) -> str:
        diff = w - prev
        marker = "↑" if diff > 0.005 else "↓" if diff < -0.005 else "·"
        return f"  {name:<5} {w*100:5.1f}%  {marker}"

    lines: list[str] = [
        f"📊 MACRO Strategy — {today_idx.date().isoformat()}",
        "",
        "Recommended allocation:",
    ]
    for ticker in ("MSTR", "MSTU", "MSTY", "MSTZ"):
        w = float(today_w.get(ticker, 0.0))
        if w > 0.005:
            lines.append(_fmt_w(ticker, w, float(prev_w.get(ticker, 0.0))))
    if cash_today > 0.005:
        lines.append(_fmt_w("Cash", cash_today, cash_prev))
    if de_risked:
        lines.append("  ⚠ de-risked (gross exposure dropped from recent avg)")

    def _ind(label: str, value, fmt: str = "{:.4f}", scale: float = 1.0) -> str:
        if pd.isna(value):
            return f"  {label:<10} —"
        return f"  {label:<10} {fmt.format(float(value) * scale)}"

    lines += [
        "",
        "Indicators:",
        _ind("MSTR",     today_ind.get("mstr_close"), "${:.2f}"),
        _ind("BTC",      today_ind.get("btc_close"),  "${:,.0f}"),
        _ind("mNAV",     today_ind.get("mnav"),       "{:.3f}"),
        _ind("MSTR RV20", today_ind.get("mstr_rv20"), "{:.1f}%", 100),
        _ind("BTC IV30", today_ind.get("btc_iv30"),   "{:.1f}%", 100),
        _ind("BTC VRP",  today_ind.get("btc_vrp"),    "{:+.2f}%", 100),
        "",
        f"Strategy book DD: {book_dd*100:+.1f}% from peak",
        f"Trades / yr (5y): {res.metrics['trades'] / 5.1:.0f}",
    ]
    return "\n".join(lines)
