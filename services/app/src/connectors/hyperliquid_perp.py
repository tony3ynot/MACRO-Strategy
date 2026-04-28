"""Hyperliquid BTC perpetual funding-rate ingestor.

Endpoint: POST https://api.hyperliquid.xyz/info
Body: {"type": "fundingHistory", "coin": "BTC", "startTime": ms, "endTime": ms}
Public, no auth.
Funding paid HOURLY (vs. Binance's 8h) — 8x more events per day.
500-row cap per call → paginate by startTime.
"""
from __future__ import annotations

import logging
from datetime import date, datetime, timezone

import httpx
from sqlalchemy import text
from tenacity import retry, stop_after_attempt, wait_exponential

from core.ingestor import Ingestor

logger = logging.getLogger(__name__)

HYPERLIQUID_INFO = "https://api.hyperliquid.xyz/info"
COIN = "BTC"
SYMBOL = "BTC-PERP"


class HyperliquidFundingIngestor(Ingestor):
    source = "hyperliquid_funding"

    def _execute(self, start: date, end: date) -> int:
        rows = self._fetch_paginated(start, end)
        if not rows:
            logger.warning("hyperliquid returned no funding for %s → %s", start, end)
            return 0
        return self._upsert(rows)

    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=2, min=2, max=15))
    def _fetch_paginated(self, start: date, end: date) -> list[dict]:
        start_ms = _date_to_ms(start)
        end_ms = _date_to_ms(end)

        all_records: list[dict] = []
        seen_ts: set[int] = set()
        cur_start = start_ms

        with httpx.Client(timeout=30) as client:
            chunk_idx = 0
            while cur_start < end_ms:
                response = client.post(
                    HYPERLIQUID_INFO,
                    json={
                        "type": "fundingHistory",
                        "coin": COIN,
                        "startTime": cur_start,
                        "endTime": end_ms,
                    },
                )
                response.raise_for_status()
                data = response.json()
                if not data:
                    break

                added = 0
                for item in data:
                    ts_ms = int(item["time"])
                    if ts_ms in seen_ts:
                        continue
                    seen_ts.add(ts_ms)
                    all_records.append({
                        "venue": "hyperliquid",
                        "symbol": SYMBOL,
                        "ts": datetime.fromtimestamp(ts_ms / 1000, tz=timezone.utc),
                        "funding_rate": float(item["fundingRate"]),
                        # 'premium' = (mark - index) / index, not the mark price itself
                        "mark_price": None,
                        "oi_usd": None,
                    })
                    added += 1

                last_ts = max(int(it["time"]) for it in data)
                logger.info(
                    "chunk %s: %s rows from API, %s new (last %s)",
                    chunk_idx, len(data), added,
                    datetime.fromtimestamp(last_ts / 1000, tz=timezone.utc).date(),
                )

                # Hyperliquid pagination: advance cur_start past last received ts
                if added == 0 or last_ts <= cur_start:
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
                        funding_rate = EXCLUDED.funding_rate
                """),
                rows,
            )
        return len(rows)


def _date_to_ms(d: date) -> int:
    return int(datetime(d.year, d.month, d.day, tzinfo=timezone.utc).timestamp() * 1000)
