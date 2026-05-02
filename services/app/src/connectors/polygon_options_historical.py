"""Per-month spot-anchored Polygon options enumeration.

Why this exists
---------------
PolygonOptionsIngestor anchors the strike band on TODAY's MSTR spot ±N %.
Contracts whose strikes were ATM in the past at very different spot
levels (MSTR went from $130 to $543 to $150 inside the 2-year window)
fall outside today's band and never enter our DB. The IV30 indicator
ends up with month-long gaps wherever historical spot drifted off the
current band.

This subclass walks the equity_ohlcv MSTR series month by month, anchors
a spot-relative strike band on each month's *median* close, and unions
the contracts returned per period via Polygon's `as_of` listing
parameter. Everything else (rate-limiter, per-contract fetcher, UPSERT,
resume-safe dedup) is inherited unchanged so the result lands in
options_chain alongside the existing rows.
"""
from __future__ import annotations

import logging
from datetime import date, timedelta

import httpx
from sqlalchemy import text

from connectors.polygon_options import (
    HTTP_TIMEOUT,
    POLYGON_BASE,
    UNDERLYING,
    PolygonOptionsIngestor,
)

logger = logging.getLogger(__name__)


class PolygonOptionsHistoricalIngestor(PolygonOptionsIngestor):
    # Same source tag as the original — both flows write to options_chain
    # and audit/log lines should treat them as one logical pipeline.
    source = "polygon"

    def __init__(
        self,
        engine=None,
        strike_pct_band: float = 0.20,
        max_dte_days: int = 60,
    ):
        """strike_pct_band defaults wider than the live ingestor (0.20 vs
        0.15) because monthly anchors are coarse — using the median puts
        intra-month spot moves at the edges of a tighter band.

        max_dte_days is also wider (60 vs 45) so we have at least two
        expiries straddling 30 DTE for term-structure interpolation."""
        super().__init__(
            engine=engine,
            strike_pct_band=strike_pct_band,
            max_dte_days=max_dte_days,
        )

    def _execute(self, start: date, end: date) -> int:
        anchors = self._monthly_anchors(start, end)
        if not anchors:
            logger.warning("no monthly anchors found in %s..%s", start, end)
            return 0

        logger.info("monthly anchors (%d):", len(anchors))
        for d, spot in anchors:
            lo, hi = self._strike_band(spot)
            logger.info("  %s  spot=%7.2f  band=[%6.0f, %6.0f]", d, spot, lo, hi)

        contracts = self._enumerate_contracts_per_period(anchors)
        logger.info("found %d unique contracts across %d windows",
                    len(contracts), len(anchors))

        already_done = self._contracts_already_ingested(contracts, start, end)
        remaining = [c for c in contracts if c["ticker"] not in already_done]
        logger.info(
            "resume: %s/%s already in DB, %s new to fetch",
            len(already_done), len(contracts), len(remaining),
        )

        total_rows = 0
        with httpx.Client(timeout=HTTP_TIMEOUT) as client:
            for i, c in enumerate(remaining, start=1):
                try:
                    rows = self._ingest_contract(client, c, start, end)
                    total_rows += rows
                    if rows > 0 or i % 50 == 0:
                        logger.info(
                            "[%s/%s] %s: +%s rows (cum %s)",
                            i, len(remaining), c["ticker"], rows, total_rows,
                        )
                except Exception:
                    logger.exception("contract failed: %s", c["ticker"])
        return total_rows

    # ──── Anchor + enumeration ──────────────────────────────────────────

    def _monthly_anchors(self, start: date, end: date) -> list[tuple[date, float]]:
        """Median MSTR close per calendar month → mid-month as_of date."""
        with self.engine.connect() as conn:
            rows = conn.execute(
                text("""
                    SELECT
                        date_trunc('month', ts)::date AS month_start,
                        percentile_cont(0.5) WITHIN GROUP (ORDER BY close) AS median_close
                    FROM equity_ohlcv
                    WHERE ticker='MSTR' AND ts >= :s AND ts <= :e
                    GROUP BY 1
                    ORDER BY 1
                """),
                {"s": start, "e": end},
            ).fetchall()
        return [(r.month_start + timedelta(days=14), float(r.median_close)) for r in rows]

    def _enumerate_contracts_per_period(
        self, anchors: list[tuple[date, float]]
    ) -> list[dict]:
        """For each (as_of, spot), query Polygon listing with as_of-anchored
        strike band, expiries from as_of to as_of+max_dte_days.

        Polygon's `expired` flag is interpreted relative to *today*, not
        the `as_of` date — so a contract that expired in 2024-06 is
        `expired=true` today even when we're asking for its 2024-05 state.
        We therefore query both flags and dedup on ticker (same pattern
        as PolygonOptionsIngestor)."""
        all_contracts: dict[str, dict] = {}
        with httpx.Client(timeout=HTTP_TIMEOUT) as client:
            for as_of_date, spot in anchors:
                lo, hi = self._strike_band(spot)
                period_added = 0
                page_count = 0
                for expired_flag in ("true", "false"):
                    params = {
                        "underlying_ticker": UNDERLYING,
                        "as_of": as_of_date.isoformat(),
                        "expired": expired_flag,
                        "expiration_date.gte": as_of_date.isoformat(),
                        "expiration_date.lte": (
                            as_of_date + timedelta(days=self.max_dte_days)
                        ).isoformat(),
                        "strike_price.gte": str(lo),
                        "strike_price.lte": str(hi),
                        "limit": 1000,
                        "apiKey": self.api_key,
                    }
                    next_url: str | None = None
                    for _ in range(20):
                        self.limiter.acquire()
                        response = (
                            client.get(next_url, params={"apiKey": self.api_key})
                            if next_url
                            else client.get(
                                f"{POLYGON_BASE}/v3/reference/options/contracts",
                                params=params,
                            )
                        )
                        response.raise_for_status()
                        payload = response.json()
                        results = payload.get("results", [])
                        for c in results:
                            if c["ticker"] not in all_contracts:
                                period_added += 1
                            all_contracts[c["ticker"]] = c
                        page_count += 1
                        next_url = payload.get("next_url")
                        if not next_url:
                            break
                logger.info(
                    "as_of=%s pages=%d new=%d (cumulative %d)",
                    as_of_date, page_count, period_added, len(all_contracts),
                )
        return list(all_contracts.values())
