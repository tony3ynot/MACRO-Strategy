"""FRED DGS1MO — daily 1-month US Treasury yield, used as r in BS pricing.

FRED publishes free CSVs without auth at fred.stlouisfed.org. We fetch
once per process and cache in-memory; callers convert to a date-indexed
pandas Series and ffill across weekends/holidays (rates don't move
between trading sessions).

The 1-month tenor matches a 30-DTE option's discount horizon better than
the 3-month T-bill, and FRED's `DGS1MO` series goes back to 2001-07.
"""
from __future__ import annotations

import io
import logging
from functools import lru_cache

import httpx
import pandas as pd

logger = logging.getLogger(__name__)

FRED_DGS1MO_URL = (
    "https://fred.stlouisfed.org/graph/fredgraph.csv?id=DGS1MO"
)


@lru_cache(maxsize=1)
def _fetch_csv() -> str:
    logger.info("fetching FRED DGS1MO CSV")
    r = httpx.get(FRED_DGS1MO_URL, timeout=30.0, follow_redirects=True)
    r.raise_for_status()
    return r.text


def fetch_dgs1mo_series() -> pd.Series:
    """Return date-indexed (date object) series of decimal rates.

    FRED publishes percent (e.g. 4.32 = 4.32 %); we convert to decimal
    here. Missing observations (`.`) are dropped, then forward-filled
    against a daily index so non-trading days resolve to the previous
    publication.
    """
    csv_text = _fetch_csv()
    df = pd.read_csv(io.StringIO(csv_text))
    df.columns = [c.strip().lower() for c in df.columns]
    # FRED CSV format: observation_date, DGS1MO  (older format may use DATE)
    date_col = "observation_date" if "observation_date" in df.columns else "date"
    val_col = "dgs1mo"
    df[date_col] = pd.to_datetime(df[date_col]).dt.date
    df[val_col] = pd.to_numeric(df[val_col], errors="coerce")
    df = df.dropna(subset=[val_col]).set_index(date_col).sort_index()
    series = (df[val_col] / 100.0).rename("dgs1mo")

    # Reindex to daily and ffill so weekends/holidays inherit Friday's rate
    full_idx = pd.date_range(series.index.min(), series.index.max(), freq="1D").date
    return series.reindex(full_idx).ffill()
