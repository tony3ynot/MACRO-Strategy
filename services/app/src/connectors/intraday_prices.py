"""Intraday price ingestion (sub-daily) for the alert engine.

Two sources, one table (`intraday_prices`):

  • EquityIntradayIngestor  — yfinance, last 1-min bar per ticker
    Fired during US market hours (handler decides; this class doesn't
    police time).  Each call writes one row per ticker with the
    timestamp of the latest available 1-min bar's close.

  • BTCIntradayIngestor     — Coinbase REST product ticker
    Fired continuously (BTC 24/7).  One row per call.

Both are cheap (single REST call each), idempotent (UPSERT on
ticker+ts), and tolerant of empty responses (returns 0 rows).
"""
from __future__ import annotations

import logging
from datetime import datetime, timezone

import httpx
import yfinance as yf
from sqlalchemy import text
from sqlalchemy.engine import Engine
from tenacity import retry, stop_after_attempt, wait_exponential

logger = logging.getLogger(__name__)


EQUITY_TICKERS = ("MSTR", "MSTU", "MSTY", "MSTZ")


def _upsert_one(engine: Engine, ticker: str, ts: datetime, close: float, source: str) -> int:
    with engine.begin() as conn:
        conn.execute(
            text("""
                INSERT INTO intraday_prices (ticker, ts, close, source)
                VALUES (:ticker, :ts, :close, :source)
                ON CONFLICT (ticker, ts) DO UPDATE SET
                    close = EXCLUDED.close,
                    source = EXCLUDED.source,
                    ingested_at = now()
            """),
            {"ticker": ticker, "ts": ts, "close": float(close), "source": source},
        )
    return 1


class EquityIntradayIngestor:
    source = "yfinance"

    def __init__(self, engine: Engine):
        self.engine = engine

    @retry(stop=stop_after_attempt(2), wait=wait_exponential(multiplier=1, min=1, max=5))
    def fetch_latest(self, ticker: str) -> tuple[datetime, float] | None:
        """Return (ts, close) for the most recent 1m bar, or None if no data.

        yfinance returns timezone-aware timestamps in US/Eastern; we
        normalise to UTC for storage so the hypertable indexes line up
        with the BTC stream (Coinbase already UTC).
        """
        hist = yf.Ticker(ticker).history(period="1d", interval="1m", auto_adjust=False)
        if hist.empty:
            return None
        last = hist.iloc[-1]
        ts = hist.index[-1].to_pydatetime().astimezone(timezone.utc)
        return ts, float(last["Close"])

    def run(self, tickers: tuple[str, ...] = EQUITY_TICKERS) -> dict[str, int]:
        result: dict[str, int] = {}
        for t in tickers:
            try:
                got = self.fetch_latest(t)
            except Exception:
                logger.exception("yfinance fetch failed: %s", t)
                result[t] = 0
                continue
            if got is None:
                result[t] = 0
                continue
            ts, close = got
            result[t] = _upsert_one(self.engine, t, ts, close, self.source)
        return result


class BTCIntradayIngestor:
    """Coinbase REST product ticker — `/products/BTC-USD/ticker`.

    Each call returns {price, time, ...} for the latest trade.  Cheap
    enough to call every minute; no auth needed for public ticker.
    """
    source = "coinbase"
    URL = "https://api.exchange.coinbase.com/products/BTC-USD/ticker"

    def __init__(self, engine: Engine):
        self.engine = engine

    @retry(stop=stop_after_attempt(2), wait=wait_exponential(multiplier=1, min=1, max=5))
    def fetch_latest(self) -> tuple[datetime, float] | None:
        try:
            r = httpx.get(self.URL, timeout=5)
            r.raise_for_status()
        except Exception:
            logger.exception("coinbase ticker fetch failed")
            return None
        data = r.json()
        price = data.get("price")
        ts_str = data.get("time")
        if not price or not ts_str:
            return None
        ts = datetime.fromisoformat(ts_str.replace("Z", "+00:00"))
        return ts, float(price)

    def run(self) -> int:
        got = self.fetch_latest()
        if got is None:
            return 0
        ts, price = got
        return _upsert_one(self.engine, "BTC", ts, price, self.source)
