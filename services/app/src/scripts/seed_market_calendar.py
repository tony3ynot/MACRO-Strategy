"""Seed market_calendar with NYSE schedule + 24/7 crypto, 2017-01-01 to 2030-12-31.

Idempotent: re-running does nothing once rows exist for given (date, market).
"""
from __future__ import annotations

import sys
from datetime import date, timedelta

import pandas as pd
import pandas_market_calendars as mcal
from sqlalchemy import text

from core.db import make_sync_engine

START = date(2017, 1, 1)
END = date(2030, 12, 31)


def build_nyse_rows() -> list[dict]:
    nyse = mcal.get_calendar("NYSE")
    schedule = nyse.schedule(start_date=START.isoformat(), end_date=END.isoformat())
    open_days = {d.date(): (o, c) for d, (o, c) in schedule[["market_open", "market_close"]].iterrows()}

    rows: list[dict] = []
    cur = START
    while cur <= END:
        if cur in open_days:
            o, c = open_days[cur]
            rows.append({
                "date": cur,
                "market": "NYSE",
                "is_open": True,
                "open_utc": o.to_pydatetime(),
                "close_utc": c.to_pydatetime(),
            })
        else:
            rows.append({
                "date": cur,
                "market": "NYSE",
                "is_open": False,
                "open_utc": None,
                "close_utc": None,
            })
        cur += timedelta(days=1)
    return rows


def build_crypto_rows() -> list[dict]:
    rows: list[dict] = []
    cur = START
    while cur <= END:
        rows.append({
            "date": cur,
            "market": "CRYPTO_24_7",
            "is_open": True,
            "open_utc": pd.Timestamp(cur, tz="UTC").to_pydatetime(),
            "close_utc": pd.Timestamp(cur + timedelta(days=1), tz="UTC").to_pydatetime(),
        })
        cur += timedelta(days=1)
    return rows


def main() -> int:
    rows = build_nyse_rows() + build_crypto_rows()
    engine = make_sync_engine()

    insert_sql = text("""
        INSERT INTO market_calendar (date, market, is_open, open_utc, close_utc)
        VALUES (:date, :market, :is_open, :open_utc, :close_utc)
        ON CONFLICT (date, market) DO NOTHING
    """)

    with engine.begin() as conn:
        result = conn.execute(insert_sql, rows)
        inserted = result.rowcount

    total = len(rows)
    print(f"market_calendar: {inserted} new rows inserted (out of {total} candidate rows)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
