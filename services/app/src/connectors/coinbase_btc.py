"""Coinbase BTC-USD daily OHLCV ingestor.

Endpoint: GET https://api.exchange.coinbase.com/products/BTC-USD/candles
Granularity: 86400 (daily). Hard cap of 300 candles per call.
Public endpoint, no auth required, ~10 req/sec rate limit.
"""
from __future__ import annotations

import logging
from datetime import date, datetime, timedelta, timezone

import httpx
from sqlalchemy import text
from tenacity import retry, stop_after_attempt, wait_exponential

from core.ingestor import Ingestor

logger = logging.getLogger(__name__)

COINBASE_BASE = "https://api.exchange.coinbase.com"
PRODUCT = "BTC-USD"
GRANULARITY_DAILY = 86_400  # seconds
CHUNK_DAYS = 290  # margin under the 300-row API cap


class CoinbaseBTCDailyIngestor(Ingestor):
    source = "coinbase"

    def _execute(self, start: date, end: date) -> int:
        rows = self._fetch_chunked(start, end)
        if not rows:
            logger.warning("coinbase returned no candles for %s → %s", start, end)
            return 0
        return self._upsert(rows)

    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=2, min=2, max=15))
    def _fetch_chunked(self, start: date, end: date) -> list[dict]:
        all_records: list[dict] = []
        seen_dates: set[date] = set()

        cur_start = start
        with httpx.Client(timeout=30) as client:
            chunk_idx = 0
            while cur_start <= end:
                cur_end = min(cur_start + timedelta(days=CHUNK_DAYS), end)
                response = client.get(
                    f"{COINBASE_BASE}/products/{PRODUCT}/candles",
                    params={
                        "start": cur_start.isoformat(),
                        "end": cur_end.isoformat(),
                        "granularity": GRANULARITY_DAILY,
                    },
                )
                response.raise_for_status()
                data = response.json()  # [[time, low, high, open, close, volume], ...]

                added = 0
                for candle in data:
                    ts, lo, hi, op_, cl, vol = candle
                    candle_date = datetime.fromtimestamp(ts, tz=timezone.utc).date()
                    if candle_date in seen_dates:
                        continue
                    seen_dates.add(candle_date)
                    all_records.append({
                        "date": candle_date,
                        "source": self.source,
                        "open": float(op_),
                        "high": float(hi),
                        "low": float(lo),
                        "close": float(cl),
                        "volume": float(vol),
                    })
                    added += 1

                logger.info(
                    "chunk %s [%s → %s]: %s rows from API, %s new",
                    chunk_idx, cur_start, cur_end, len(data), added,
                )

                cur_start = cur_end + timedelta(days=1)
                chunk_idx += 1

        return all_records

    def _upsert(self, rows: list[dict]) -> int:
        with self.engine.begin() as conn:
            conn.execute(
                text("""
                    INSERT INTO btc_ohlcv_daily
                        (date, source, open, high, low, close, volume)
                    VALUES
                        (:date, :source, :open, :high, :low, :close, :volume)
                    ON CONFLICT (source, date) DO UPDATE SET
                        open        = EXCLUDED.open,
                        high        = EXCLUDED.high,
                        low         = EXCLUDED.low,
                        close       = EXCLUDED.close,
                        volume      = EXCLUDED.volume,
                        ingested_at = now()
                """),
                rows,
            )
        return len(rows)
