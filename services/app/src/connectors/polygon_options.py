"""Polygon Options Basic ingestor for MSTR options chain.

Pulls daily aggregates for MSTR options contracts (active + expired) within
Polygon Basic's 2-year window.

Constraints (Basic tier):
- 5 API calls / minute (hard cap, enforced via token bucket)
- 2 years historical
- End-of-day data only
- Snapshot and grouped-daily endpoints are blocked (403/400)

Stored fields per (underlying, ts, expiry, strike, type):
  open, high, low, last(=close), volume, vwap, transactions → from Polygon
  iv, delta, gamma, vega, theta, bid, ask, oi → NULL (computed in Phase 2
  via Black-Scholes inversion using equity_ohlcv close + this close)
"""
from __future__ import annotations

import logging
from datetime import date, datetime, timedelta, timezone

import httpx
from sqlalchemy import text
from tenacity import retry, retry_if_exception_type, stop_after_attempt, wait_exponential

from core.config import get_settings
from core.ingestor import Ingestor
from core.rate_limiter import TokenBucketRateLimiter

logger = logging.getLogger(__name__)

POLYGON_BASE = "https://api.polygon.io"
UNDERLYING = "MSTR"
RATE_LIMIT_PER_MIN = 5  # Polygon Basic hard cap
HTTP_TIMEOUT = 30


class PolygonRateLimitError(Exception):
    """Raised on 429 from Polygon — retried with backoff."""


class PolygonOptionsIngestor(Ingestor):
    source = "polygon"

    def __init__(
        self,
        engine=None,
        strike_pct_band: float = 0.15,
        max_dte_days: int = 45,
    ):
        """strike_pct_band: include strikes within ±this fraction of recent
        MSTR close. Default 0.15 (±15%) covers liquid ATM zone for VRP/IV30;
        widen to 0.30+ for skew, narrow to 0.05 for fastest backfill.

        max_dte_days: how far past `end` to include expiries. Default 45 is
        enough for IV30 (need ~25-35 DTE on each date in window). Raise to
        120 for full term-structure (up to 90 DTE) or skew analysis."""
        super().__init__(engine=engine)
        self.api_key = get_settings().polygon_api_key
        if not self.api_key:
            raise RuntimeError("POLYGON_API_KEY required")
        self.limiter = TokenBucketRateLimiter(RATE_LIMIT_PER_MIN, name="polygon")
        self.strike_pct_band = strike_pct_band
        self.max_dte_days = max_dte_days

    # ──── Ingestor entry ────────────────────────────────────────────────

    def _execute(self, start: date, end: date) -> int:
        anchor_close = self._most_recent_mstr_close()
        strike_lo, strike_hi = self._strike_band(anchor_close)
        logger.info(
            "MSTR anchor close=%.2f, strike band=[%.2f, %.2f]",
            anchor_close, strike_lo, strike_hi,
        )

        contracts = self._enumerate_contracts(start, end, strike_lo, strike_hi)
        logger.info("found %s contracts in window %s → %s", len(contracts), start, end)

        already_done = self._contracts_already_ingested(contracts, start, end)
        remaining = [c for c in contracts if c["ticker"] not in already_done]
        logger.info(
            "resume: %s/%s contracts already in DB, %s to fetch",
            len(already_done), len(contracts), len(remaining),
        )

        total_rows = 0
        with httpx.Client(timeout=HTTP_TIMEOUT) as client:
            for i, c in enumerate(remaining, start=1):
                try:
                    rows = self._ingest_contract(client, c, start, end)
                    total_rows += rows
                    if rows > 0 or i % 50 == 0:
                        logger.info("[%s/%s] %s: +%s rows (cum %s)",
                                    i, len(remaining), c["ticker"], rows, total_rows)
                except Exception:
                    logger.exception("contract failed: %s", c["ticker"])
        return total_rows

    # ──── Enumeration ────────────────────────────────────────────────────

    def _enumerate_contracts(
        self, start: date, end: date, strike_lo: float, strike_hi: float
    ) -> list[dict]:
        """Polygon's expired flag is exclusive, so we query active and
        expired separately and merge. Dedup on ticker."""
        all_contracts: dict[str, dict] = {}
        with httpx.Client(timeout=HTTP_TIMEOUT) as client:
            for expired_flag in ("true", "false"):
                next_url: str | None = None
                params = {
                    "underlying_ticker": UNDERLYING,
                    "expired": expired_flag,
                    "expiration_date.gte": start.isoformat(),
                    "expiration_date.lte": (end + timedelta(days=self.max_dte_days)).isoformat(),
                    "strike_price.gte": str(strike_lo),
                    "strike_price.lte": str(strike_hi),
                    "limit": 1000,
                    "apiKey": self.api_key,
                }
                for page in range(50):
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
                        all_contracts[c["ticker"]] = c
                    logger.info(
                        "list expired=%s page=%s: +%s contracts (running total %s)",
                        expired_flag, page, len(results), len(all_contracts),
                    )
                    next_url = payload.get("next_url")
                    if not next_url:
                        break
        return list(all_contracts.values())

    def _contracts_already_ingested(
        self, contracts: list[dict], start: date, end: date
    ) -> set[str]:
        """Determine which contracts already have ANY row in options_chain
        within the requested window. Skips them on resume."""
        if not contracts:
            return set()
        with self.engine.connect() as conn:
            result = conn.execute(
                text("""
                    SELECT DISTINCT expiry, strike, type
                    FROM options_chain
                    WHERE underlying = :u AND ts BETWEEN :s AND :e
                """),
                {"u": UNDERLYING, "s": start, "e": end},
            )
            done_keys = {(row.expiry, float(row.strike), row.type) for row in result}

        done_tickers: set[str] = set()
        for c in contracts:
            key = (
                date.fromisoformat(c["expiration_date"]),
                float(c["strike_price"]),
                "C" if c["contract_type"] == "call" else "P",
            )
            if key in done_keys:
                done_tickers.add(c["ticker"])
        return done_tickers

    # ──── Per-contract fetch ────────────────────────────────────────────

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=2, min=2, max=30),
        retry=retry_if_exception_type((PolygonRateLimitError, httpx.HTTPError)),
    )
    def _ingest_contract(
        self, client: httpx.Client, contract: dict, start: date, end: date
    ) -> int:
        ticker = contract["ticker"]
        expiry = date.fromisoformat(contract["expiration_date"])
        strike = float(contract["strike_price"])
        opt_type = "C" if contract["contract_type"] == "call" else "P"

        # Fetch the contract's tradable lifetime intersected with our window
        agg_start = max(start, expiry - timedelta(days=730))
        agg_end = min(end, expiry)
        if agg_start > agg_end:
            return 0

        self.limiter.acquire()
        response = client.get(
            f"{POLYGON_BASE}/v2/aggs/ticker/{ticker}/range/1/day/"
            f"{agg_start.isoformat()}/{agg_end.isoformat()}",
            params={"apiKey": self.api_key, "sort": "asc", "limit": 5000},
        )
        if response.status_code == 429:
            raise PolygonRateLimitError("Polygon 429")
        response.raise_for_status()

        results = response.json().get("results", [])
        if not results:
            return 0

        records: list[dict] = []
        for bar in results:
            ts_date = datetime.fromtimestamp(bar["t"] / 1000, tz=timezone.utc).date()
            records.append({
                "underlying": UNDERLYING,
                "ts": ts_date,
                "expiry": expiry,
                "strike": strike,
                "type": opt_type,
                "bid": None, "ask": None,
                "open": float(bar["o"]) if bar.get("o") is not None else None,
                "high": float(bar["h"]) if bar.get("h") is not None else None,
                "low":  float(bar["l"]) if bar.get("l") is not None else None,
                "last": float(bar["c"]),
                "vwap": float(bar["vw"]) if bar.get("vw") is not None else None,
                "iv": None,
                "delta": None, "gamma": None, "vega": None, "theta": None,
                "oi": None,
                "volume": int(bar["v"]) if bar.get("v") is not None else 0,
                "transactions": int(bar["n"]) if bar.get("n") is not None else None,
                "source": self.source,
            })

        with self.engine.begin() as conn:
            conn.execute(
                text("""
                    INSERT INTO options_chain
                        (underlying, ts, expiry, strike, type,
                         bid, ask, open, high, low, last, vwap,
                         iv, delta, gamma, vega, theta,
                         oi, volume, transactions, source)
                    VALUES
                        (:underlying, :ts, :expiry, :strike, :type,
                         :bid, :ask, :open, :high, :low, :last, :vwap,
                         :iv, :delta, :gamma, :vega, :theta,
                         :oi, :volume, :transactions, :source)
                    ON CONFLICT (underlying, ts, expiry, strike, type) DO UPDATE SET
                        open         = EXCLUDED.open,
                        high         = EXCLUDED.high,
                        low          = EXCLUDED.low,
                        last         = EXCLUDED.last,
                        vwap         = EXCLUDED.vwap,
                        volume       = EXCLUDED.volume,
                        transactions = EXCLUDED.transactions,
                        source       = EXCLUDED.source,
                        ingested_at  = now()
                """),
                records,
            )
        return len(records)

    # ──── Helpers ────────────────────────────────────────────────────────

    def _most_recent_mstr_close(self) -> float:
        with self.engine.connect() as conn:
            row = conn.execute(
                text("""
                    SELECT close FROM equity_ohlcv
                    WHERE ticker = 'MSTR' ORDER BY ts DESC LIMIT 1
                """)
            ).fetchone()
        if row is None:
            raise RuntimeError("equity_ohlcv has no MSTR — run yfinance backfill first")
        return float(row.close)

    def _strike_band(self, anchor: float) -> tuple[float, float]:
        return (
            round(anchor * (1 - self.strike_pct_band), 2),
            round(anchor * (1 + self.strike_pct_band), 2),
        )
