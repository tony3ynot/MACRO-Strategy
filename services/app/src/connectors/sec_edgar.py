"""SEC EDGAR scraper for MSTR Bitcoin holdings (mstr_btc_holdings).

Pulls 8-K filings from MicroStrategy (CIK 1050446) since the first BTC
purchase (2020-08-11), parses the primary document for cumulative
bitcoin holdings statements, and upserts into mstr_btc_holdings.

SEC compliance: rate-limit ≤10 req/sec, declarative User-Agent required.
"""
from __future__ import annotations

import logging
import re
import time
from datetime import date
from typing import Iterator

import httpx
from sqlalchemy import text
from tenacity import retry, stop_after_attempt, wait_exponential

from core.config import get_settings
from core.ingestor import Ingestor

logger = logging.getLogger(__name__)

CIK_RAW = 1050446
CIK_PADDED = f"{CIK_RAW:010d}"  # "0001050446"
FIRST_BTC_PURCHASE = date(2020, 8, 11)

EDGAR_SUBMISSIONS = f"https://data.sec.gov/submissions/CIK{CIK_PADDED}.json"
EDGAR_ARCHIVES = f"https://www.sec.gov/Archives/edgar/data/{CIK_RAW}"

# Cumulative-holdings patterns. Two phrasing eras:
#   2020-2022 prose ("MicroStrategy"): "holds an aggregate of X bitcoins"
#   2024+ tabular  ("Strategy"):       row sandwiches cumulative between $-amounts
# Strategy: collect ALL candidates, return MAX (cumulative ≥ period acquisition).
PROSE_PATTERNS = [
    # "held / holds / holding (an aggregate of) (approximately) X bitcoins"
    re.compile(
        r"(?:hold(?:s|ing)?|held)\s+(?:an\s+aggregate\s+of\s+)?(?:approximately\s+)?"
        r"(\d{1,3}(?:,\d{3})+|\d{4,})\s+bitcoins?",
        re.IGNORECASE,
    ),
    re.compile(
        r"aggregate\s+(?:holdings?|purchases?)\s+of\s+(?:approximately\s+)?"
        r"(\d{1,3}(?:,\d{3})+|\d{4,})\s+bitcoins?",
        re.IGNORECASE,
    ),
    re.compile(
        r"approximately\s+(\d{1,3}(?:,\d{3})+|\d{4,})\s+bitcoins?\s+for\s+(?:an\s+)?aggregate",
        re.IGNORECASE,
    ),
]

# Tabular: cumulative BTC always sits between "$ <avg_price>" and "$ <cum_cost>"
# in the data row, regardless of period acquisition magnitude.
TABLE_PATTERN = re.compile(
    r"\$\s*[\d,.]+\s+(\d{2,3}(?:,\d{3})+)\s*\$"
)

# Aggregate-cost pattern (USD)
COST_PATTERN = re.compile(
    r"(?:aggregate|total)\s+purchase\s+price\s+of\s+(?:approximately\s+)?\$"
    r"([\d,]+(?:\.\d+)?)\s*(billion|million)?",
    re.IGNORECASE,
)

# Sanity bounds
MIN_BTC = 10_000   # MSTR's first 8-K reported ~21k BTC
MAX_BTC = 5_000_000  # sanity cap

REQUEST_DELAY_SECONDS = 0.15  # ~6.6 req/sec, polite under SEC's 10/sec cap


class MSTRBTCHoldingsIngestor(Ingestor):
    source = "sec_edgar_mstr"

    def __init__(self, engine=None):
        super().__init__(engine=engine)
        self.headers = {
            "User-Agent": get_settings().sec_user_agent,
            "Accept-Encoding": "gzip, deflate",
        }

    def _execute(self, start: date, end: date) -> int:
        if start < FIRST_BTC_PURCHASE:
            start = FIRST_BTC_PURCHASE

        with httpx.Client(headers=self.headers, timeout=30) as client:
            filings = list(self._list_8k_filings(client, start, end))
            logger.info("found %s 8-K filings in range %s → %s", len(filings), start, end)

            records: list[dict] = []
            for f in filings:
                try:
                    rec = self._parse_filing(client, f)
                    if rec:
                        records.append(rec)
                except Exception:
                    logger.exception("parse failed: accession=%s", f["accession"])
                time.sleep(REQUEST_DELAY_SECONDS)

        if not records:
            logger.warning("no parseable holdings statements found")
            return 0

        # If multiple 8-Ks land on the same date, keep the largest btc_qty
        # (later filings refine the number).
        dedup: dict[date, dict] = {}
        for r in records:
            existing = dedup.get(r["date"])
            if existing is None or r["btc_qty"] >= existing["btc_qty"]:
                dedup[r["date"]] = r
        return self._upsert(list(dedup.values()))

    def _list_8k_filings(
        self, client: httpx.Client, start: date, end: date
    ) -> Iterator[dict]:
        response = client.get(EDGAR_SUBMISSIONS)
        response.raise_for_status()
        recent = response.json().get("filings", {}).get("recent", {})

        forms = recent.get("form", [])
        dates = recent.get("filingDate", [])
        accessions = recent.get("accessionNumber", [])
        primary_docs = recent.get("primaryDocument", [])

        for form, fdate, acc, doc in zip(forms, dates, accessions, primary_docs):
            if form != "8-K":
                continue
            filing_date = date.fromisoformat(fdate)
            if not (start <= filing_date <= end):
                continue
            yield {
                "accession": acc,
                "date": filing_date,
                "primary_doc": doc,
                "url": self._build_url(acc, doc),
            }

    def _build_url(self, accession: str, primary_doc: str) -> str:
        folder = accession.replace("-", "")
        return f"{EDGAR_ARCHIVES}/{folder}/{primary_doc}"

    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=1, max=8))
    def _parse_filing(self, client: httpx.Client, filing: dict) -> dict | None:
        response = client.get(filing["url"])
        response.raise_for_status()
        clean = self._strip_html(response.text)

        # Quick filter: skip filings that don't even mention bitcoin
        if "bitcoin" not in clean.lower():
            return None

        cumulative_btc = self._extract_cumulative_btc(clean)
        if cumulative_btc is None:
            logger.debug("no holdings pattern in %s", filing["accession"])
            return None

        cumulative_cost = self._extract_cost(clean)

        return {
            "date": filing["date"],
            "btc_qty": cumulative_btc,
            "cumulative_cost": cumulative_cost,
            "last_purchase_date": filing["date"],
            "source_filing": filing["accession"],
        }

    @staticmethod
    def _strip_html(html: str) -> str:
        no_tags = re.sub(r"<[^>]+>", " ", html)
        no_entities = (
            no_tags.replace("&nbsp;", " ")
                   .replace("&amp;", "&")
                   .replace("&#160;", " ")
                   .replace("&#8217;", "'")
        )
        return re.sub(r"\s+", " ", no_entities).strip()

    @staticmethod
    def _extract_cumulative_btc(text: str) -> int | None:
        candidates: set[int] = set()

        for pat in PROSE_PATTERNS:
            for m in pat.finditer(text):
                qty = int(m.group(1).replace(",", ""))
                if MIN_BTC <= qty <= MAX_BTC:
                    candidates.add(qty)

        for m in TABLE_PATTERN.finditer(text):
            qty = int(m.group(1).replace(",", ""))
            if MIN_BTC <= qty <= MAX_BTC:
                candidates.add(qty)

        # Cumulative is always ≥ period acquisition, so the max candidate
        # corresponds to the cumulative figure we want.
        return max(candidates) if candidates else None

    @staticmethod
    def _extract_cost(text: str) -> float | None:
        m = COST_PATTERN.search(text)
        if not m:
            return None
        amount = float(m.group(1).replace(",", ""))
        unit = (m.group(2) or "").lower()
        if unit == "billion":
            amount *= 1_000_000_000
        elif unit == "million":
            amount *= 1_000_000
        return amount

    def _upsert(self, records: list[dict]) -> int:
        with self.engine.begin() as conn:
            conn.execute(
                text("""
                    INSERT INTO mstr_btc_holdings
                        (date, btc_qty, cumulative_cost, last_purchase_date, source_filing)
                    VALUES
                        (:date, :btc_qty, :cumulative_cost, :last_purchase_date, :source_filing)
                    ON CONFLICT (date) DO UPDATE SET
                        btc_qty            = EXCLUDED.btc_qty,
                        cumulative_cost    = COALESCE(EXCLUDED.cumulative_cost, mstr_btc_holdings.cumulative_cost),
                        last_purchase_date = EXCLUDED.last_purchase_date,
                        source_filing      = EXCLUDED.source_filing,
                        ingested_at        = now()
                """),
                records,
            )
        return len(records)
