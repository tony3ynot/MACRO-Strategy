"""Deribit BTC DVOL (volatility index) ingestor.

Endpoint: GET /api/v2/public/get_volatility_index_data
DVOL launched ~2021-03. Earlier dates return empty data.
Single 1D-resolution call typically covers ~5 years (≤ 5000-row response cap).
"""
from __future__ import annotations

import logging
from datetime import date, datetime, timezone

import httpx
from sqlalchemy import text
from tenacity import retry, stop_after_attempt, wait_exponential

from core.ingestor import Ingestor

logger = logging.getLogger(__name__)

DERIBIT_BASE = "https://www.deribit.com/api/v2"


class DeribitDVOLIngestor(Ingestor):
    source = "deribit_dvol"

    def _execute(self, start: date, end: date) -> int:
        start_ms = _date_to_ms(start)
        end_ms = _date_to_ms(end)

        rows = self._fetch_paginated(start_ms, end_ms)
        if not rows:
            logger.warning("deribit returned no DVOL data for %s → %s", start, end)
            return 0
        return self._upsert(rows)

    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=2, min=2, max=15))
    def _fetch_paginated(self, start_ms: int, end_ms: int) -> list[dict]:
        """Chunk the range into ~900-day windows. Deribit returns max ~1000 rows
        per call at 1D resolution, so 900 days per chunk leaves margin and avoids
        relying on Deribit's `continuation` token semantics.
        """
        all_records: list[dict] = []
        seen_ts: set[int] = set()
        chunk_ms = 900 * 86_400_000

        cur_start = start_ms
        with httpx.Client(timeout=30) as client:
            chunk_idx = 0
            while cur_start < end_ms:
                cur_end = min(cur_start + chunk_ms, end_ms)
                response = client.get(
                    f"{DERIBIT_BASE}/public/get_volatility_index_data",
                    params={
                        "currency": "BTC",
                        "start_timestamp": cur_start,
                        "end_timestamp": cur_end,
                        "resolution": "1D",
                    },
                )
                response.raise_for_status()
                data = response.json().get("result", {}).get("data", [])

                added = 0
                for row in data:
                    ts_ms, _, _, _, close_ = row
                    if ts_ms in seen_ts:
                        continue
                    seen_ts.add(ts_ms)
                    all_records.append({
                        "ts": datetime.fromtimestamp(ts_ms / 1000, tz=timezone.utc),
                        "dvol": float(close_),
                    })
                    added += 1

                logger.info(
                    "chunk %s [%s → %s]: %s rows from API, %s new",
                    chunk_idx,
                    datetime.fromtimestamp(cur_start / 1000, tz=timezone.utc).date(),
                    datetime.fromtimestamp(cur_end / 1000, tz=timezone.utc).date(),
                    len(data), added,
                )

                cur_start = cur_end
                chunk_idx += 1

        return all_records

    def _upsert(self, rows: list[dict]) -> int:
        with self.engine.begin() as conn:
            conn.execute(
                text("""
                    INSERT INTO btc_dvol (ts, dvol)
                    VALUES (:ts, :dvol)
                    ON CONFLICT (ts) DO UPDATE SET dvol = EXCLUDED.dvol
                """),
                rows,
            )
        return len(rows)


def _date_to_ms(d: date) -> int:
    """Convert UTC midnight of given date to Unix milliseconds."""
    return int(datetime(d.year, d.month, d.day, tzinfo=timezone.utc).timestamp() * 1000)
