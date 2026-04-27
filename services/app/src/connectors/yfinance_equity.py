"""yfinance ingestor for MSTR/MSTU/MSTY/MSTZ.

Pulls daily OHLCV (open/high/low/close/adj_close/volume), dividends, and splits.
Idempotent: ON CONFLICT updates OHLCV but preserves manually-set
distribution.classification (so YieldMax IR enrichment isn't overwritten).
"""
from __future__ import annotations

import logging
from datetime import date

import pandas as pd
import yfinance as yf
from sqlalchemy import text
from tenacity import retry, stop_after_attempt, wait_exponential

from core.ingestor import Ingestor

logger = logging.getLogger(__name__)

TICKERS: tuple[str, ...] = ("MSTR", "MSTU", "MSTY", "MSTZ")


class YFinanceEquityIngestor(Ingestor):
    source = "yfinance"
    tickers: tuple[str, ...] = TICKERS

    def _execute(self, start: date, end: date) -> int:
        total = 0
        for ticker in self.tickers:
            try:
                rows = self._ingest_ticker(ticker, start, end)
                logger.info("%s: %s rows", ticker, rows)
                total += rows
            except Exception:
                logger.exception("ticker failed: %s", ticker)
                # Continue with other tickers; failed run still records via base class.
                # Re-raise only if we want hard fail. For backfill, soft-fail per-ticker.
                continue
        return total

    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=2, min=2, max=15))
    def _ingest_ticker(self, ticker: str, start: date, end: date) -> int:
        history = yf.Ticker(ticker).history(
            start=start.isoformat(),
            end=end.isoformat(),
            auto_adjust=False,
            actions=True,
        )
        if history.empty:
            return 0

        ohlcv_count = self._upsert_ohlcv(ticker, history)
        dist_count = self._upsert_distributions(ticker, history)
        return ohlcv_count + dist_count

    def _upsert_ohlcv(self, ticker: str, history: pd.DataFrame) -> int:
        records: list[dict] = []
        for ts, row in history.iterrows():
            ts_date = ts.date() if hasattr(ts, "date") else ts
            try:
                records.append({
                    "ticker": ticker,
                    "ts": ts_date,
                    "open": float(row["Open"]),
                    "high": float(row["High"]),
                    "low": float(row["Low"]),
                    "close": float(row["Close"]),
                    "adj_close": float(row["Adj Close"]),
                    "volume": int(row["Volume"]) if pd.notna(row["Volume"]) else 0,
                    "source": self.source,
                })
            except (ValueError, TypeError):
                # NaN row — skip
                continue

        if not records:
            return 0

        with self.engine.begin() as conn:
            conn.execute(
                text("""
                    INSERT INTO equity_ohlcv
                        (ticker, ts, open, high, low, close, adj_close, volume, source)
                    VALUES
                        (:ticker, :ts, :open, :high, :low, :close, :adj_close, :volume, :source)
                    ON CONFLICT (ticker, ts) DO UPDATE SET
                        open        = EXCLUDED.open,
                        high        = EXCLUDED.high,
                        low         = EXCLUDED.low,
                        close       = EXCLUDED.close,
                        adj_close   = EXCLUDED.adj_close,
                        volume      = EXCLUDED.volume,
                        source      = EXCLUDED.source,
                        ingested_at = now()
                """),
                records,
            )
        return len(records)

    def _upsert_distributions(self, ticker: str, history: pd.DataFrame) -> int:
        records: list[dict] = []
        for ts, row in history.iterrows():
            ex_date = ts.date() if hasattr(ts, "date") else ts

            div = row.get("Dividends", 0.0)
            if pd.notna(div) and float(div) > 0:
                records.append({
                    "ticker": ticker,
                    "ex_date": ex_date,
                    "pay_date": None,
                    "amount": float(div),
                    "type": "dividend",
                    "classification": None,
                    "source": self.source,
                })

            split = row.get("Stock Splits", 0.0)
            if pd.notna(split) and float(split) > 0:
                records.append({
                    "ticker": ticker,
                    "ex_date": ex_date,
                    "pay_date": None,
                    "amount": float(split),
                    "type": "split",
                    "classification": None,
                    "source": self.source,
                })

        if not records:
            return 0

        with self.engine.begin() as conn:
            conn.execute(
                text("""
                    INSERT INTO distributions
                        (ticker, ex_date, pay_date, amount, type, classification, source)
                    VALUES
                        (:ticker, :ex_date, :pay_date, :amount, :type, :classification, :source)
                    ON CONFLICT (ticker, ex_date, type) DO UPDATE SET
                        amount         = EXCLUDED.amount,
                        pay_date       = EXCLUDED.pay_date,
                        classification = COALESCE(distributions.classification, EXCLUDED.classification),
                        ingested_at    = now()
                """),
                records,
            )
        return len(records)
