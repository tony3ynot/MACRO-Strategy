"""YieldMax MSTY distribution classification scraper.

Pulls the public distributions table from yieldmaxetfs.com which lists
ROC percentage per distribution. Enriches the existing yfinance-sourced
rows in `distributions` with:
  - pay_date (yfinance leaves NULL)
  - roc_pct (new from YieldMax)
  - classification: 'ROC' if roc_pct >= 50%, else 'ordinary' (dominant label)

YieldMax publishes Section 19(a) estimates in this table. Year-end
1099-DIV may reclassify; for Phase 2 backtests, treat roc_pct as
estimate, refine annually if precision matters.
"""
from __future__ import annotations

import logging
import re
from datetime import date, datetime

import httpx
from sqlalchemy import text
from tenacity import retry, stop_after_attempt, wait_exponential

from core.ingestor import Ingestor

logger = logging.getLogger(__name__)

YIELDMAX_URL = "https://www.yieldmaxetfs.com/our-etfs/msty/"
TICKER = "MSTY"
USER_AGENT = "Mozilla/5.0 MACRO-Strategy/0.1"

# Match each distribution table row.
# Column order: amount, declared, ex_date, record, payable, ROC%
ROW_RE = re.compile(
    r"<tr>"
    r"<td>\$([\d.]+)</td>"
    r"<td>(\d{2}/\d{2}/\d{4})</td>"
    r"<td>(\d{2}/\d{2}/\d{4})</td>"
    r"<td>(\d{2}/\d{2}/\d{4})</td>"
    r"<td>(\d{2}/\d{2}/\d{4})</td>"
    r"<td>([\d.]+)%?</td>"
    r"</tr>",
    re.IGNORECASE,
)


class YieldMaxMSTYIngestor(Ingestor):
    source = "yieldmax_msty"

    def _execute(self, start: date, end: date) -> int:
        rows = self._fetch_distributions()
        if not rows:
            logger.warning("YieldMax returned no parseable rows")
            return 0

        rows = [r for r in rows if start <= r["ex_date"] <= end]
        if not rows:
            return 0
        return self._update(rows)

    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=2, min=2, max=15))
    def _fetch_distributions(self) -> list[dict]:
        response = httpx.get(
            YIELDMAX_URL,
            headers={"User-Agent": USER_AGENT},
            timeout=30,
            follow_redirects=True,
        )
        response.raise_for_status()
        html = response.text

        # Restrict regex to the dedicated table to avoid matching unrelated tables.
        m = re.search(
            r'<table class="distributions-table">(.*?)</table>',
            html,
            re.DOTALL | re.IGNORECASE,
        )
        if not m:
            logger.error("distributions-table not found in YieldMax HTML")
            return []
        table_html = m.group(1)

        records: list[dict] = []
        for match in ROW_RE.finditer(table_html):
            amount, declared_str, ex_str, record_str, pay_str, roc_pct_str = match.groups()
            try:
                ex_date = _parse_us_date(ex_str)
                pay_date = _parse_us_date(pay_str)
                amt = float(amount)
                roc_pct = float(roc_pct_str)
            except ValueError:
                logger.warning("could not parse row: %s", match.group(0)[:100])
                continue

            classification = "ROC" if roc_pct >= 50.0 else "ordinary"
            records.append({
                "ex_date": ex_date,
                "pay_date": pay_date,
                "amount": amt,
                "roc_pct": roc_pct,
                "classification": classification,
            })

        logger.info("parsed %s YieldMax distribution rows", len(records))
        return records

    def _update(self, rows: list[dict]) -> int:
        """Update existing yfinance-sourced 'dividend' rows in-place."""
        with self.engine.begin() as conn:
            result = conn.execute(
                text("""
                    UPDATE distributions
                    SET pay_date       = :pay_date,
                        roc_pct        = :roc_pct,
                        classification = :classification,
                        ingested_at    = now()
                    WHERE ticker = 'MSTY'
                      AND ex_date = :ex_date
                      AND type = 'dividend'
                """),
                rows,
            )
            return result.rowcount


def _parse_us_date(s: str) -> date:
    return datetime.strptime(s, "%m/%d/%Y").date()
