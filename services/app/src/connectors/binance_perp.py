"""Binance Futures BTC perpetual funding-rate ingestor.

Endpoint: GET https://fapi.binance.com/fapi/v1/fundingRate
Public, no auth, no key required.
Funding paid every 8 hours (00:00, 08:00, 16:00 UTC).
1000-row cap per call → paginate by startTime.
"""
from __future__ import annotations

import logging
from datetime import date, datetime, timezone

import httpx
from sqlalchemy import text
from tenacity import retry, stop_after_attempt, wait_exponential

from core.ingestor import Ingestor

logger = logging.getLogger(__name__)

BINANCE_FAPI = "https://fapi.binance.com"
SYMBOL = "BTCUSDT"


class BinanceFundingIngestor(Ingestor):
    source = "binance_funding"

    def _execute(self, start: date, end: date) -> int:
        rows = self._fetch_paginated(start, end)
        if not rows:
            logger.warning("binance returned no funding for %s → %s", start, end)
            return 0
        return self._upsert(rows)

    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=2, min=2, max=15))
    def _fetch_paginated(self, start: date, end: date) -> list[dict]:
        start_ms = _date_to_ms(start)
        end_ms = _date_to_ms(end)

        all_records: list[dict] = []
        cur_start = start_ms
        with httpx.Client(timeout=30) as client:
            chunk_idx = 0
            while cur_start < end_ms:
                response = client.get(
                    f"{BINANCE_FAPI}/fapi/v1/fundingRate",
                    params={
                        "symbol": SYMBOL,
                        "startTime": cur_start,
                        "endTime": end_ms,
                        "limit": 1000,
                    },
                )
                response.raise_for_status()
                data = response.json()
                if not data:
                    break

                for item in data:
                    funding_time = int(item["fundingTime"])
                    # markPrice was added later — early records (pre-2020) ship "".
                    mark_raw = item.get("markPrice") or ""
                    mark_price = float(mark_raw) if mark_raw else None
                    all_records.append({
                        "venue": "binance",
                        "symbol": SYMBOL,
                        "ts": datetime.fromtimestamp(funding_time / 1000, tz=timezone.utc),
                        "funding_rate": float(item["fundingRate"]),
                        "mark_price": mark_price,
                        "oi_usd": None,
                    })

                last_ts = int(data[-1]["fundingTime"])
                logger.info(
                    "chunk %s: %s rows (from %s)",
                    chunk_idx, len(data),
                    datetime.fromtimestamp(last_ts / 1000, tz=timezone.utc).date(),
                )
                if len(data) < 1000:
                    break
                cur_start = last_ts + 1
                chunk_idx += 1

        return all_records

    def _upsert(self, rows: list[dict]) -> int:
        with self.engine.begin() as conn:
            conn.execute(
                text("""
                    INSERT INTO crypto_perp_funding
                        (venue, symbol, ts, funding_rate, mark_price, oi_usd)
                    VALUES
                        (:venue, :symbol, :ts, :funding_rate, :mark_price, :oi_usd)
                    ON CONFLICT (venue, symbol, ts) DO UPDATE SET
                        funding_rate = EXCLUDED.funding_rate,
                        mark_price   = COALESCE(EXCLUDED.mark_price, crypto_perp_funding.mark_price)
                """),
                rows,
            )
        return len(rows)


def _date_to_ms(d: date) -> int:
    return int(datetime(d.year, d.month, d.day, tzinfo=timezone.utc).timestamp() * 1000)
