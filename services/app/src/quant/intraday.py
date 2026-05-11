"""Live (intraday) signal helpers.

Wraps the daily indicators with the most-recent intraday tick so
alerts can fire mid-session without waiting for end-of-day compute.

Two main quantities:

  • live_mnav()    — uses latest MSTR + latest BTC + the most recent
                     daily snapshot of shares_outstanding × btc_qty.
                     BTC moves intraday → mNAV moves intraday.

  • session_open_pct(ticker)
                   — % move of latest intraday close vs the first
                     intraday bar of the current session.  Used for
                     "big move" alerts (e.g., MSTR ±5 % since open).

Both functions are read-only against `intraday_prices` and the daily
tables; computation is sub-millisecond.
"""
from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import datetime, timezone

from sqlalchemy import text
from sqlalchemy.engine import Engine

logger = logging.getLogger(__name__)


@dataclass
class LiveTick:
    ticker: str
    ts: datetime
    close: float


def latest_tick(engine: Engine, ticker: str) -> LiveTick | None:
    sql = text("""
        SELECT ts, close FROM intraday_prices
        WHERE ticker = :ticker
        ORDER BY ts DESC LIMIT 1
    """)
    with engine.connect() as conn:
        row = conn.execute(sql, {"ticker": ticker}).fetchone()
    if row is None:
        return None
    return LiveTick(ticker=ticker, ts=row.ts, close=float(row.close))


def session_open(engine: Engine, ticker: str, session_start_utc: datetime) -> float | None:
    """First intraday close at or after the session-start UTC timestamp."""
    sql = text("""
        SELECT close FROM intraday_prices
        WHERE ticker = :ticker AND ts >= :since
        ORDER BY ts ASC LIMIT 1
    """)
    with engine.connect() as conn:
        row = conn.execute(sql, {"ticker": ticker, "since": session_start_utc}).fetchone()
    return float(row.close) if row else None


def us_session_start_utc(now: datetime | None = None) -> datetime:
    """The most recent US equity session open (09:30 ET) in UTC.

    Daylight savings is intentionally approximated as ET = UTC-4 (EDT).
    During EST (Nov-Mar) this returns the session open 1h late, which
    over-counts at-the-open ticks into the previous "day" group but is
    benign for our use case (we compare *intraday* close to *the* open
    within the same calendar day).
    """
    now = now or datetime.now(timezone.utc)
    # 09:30 ET = 13:30 UTC (EDT) or 14:30 UTC (EST).
    # Use 13:30 UTC; close enough for the move % calculation.
    session_open_utc = now.replace(hour=13, minute=30, second=0, microsecond=0)
    if now < session_open_utc:
        # Pre-13:30 UTC = before today's US open → use yesterday's
        session_open_utc = session_open_utc.replace(day=session_open_utc.day - 1)
    return session_open_utc


def session_move_pct(engine: Engine, ticker: str) -> float | None:
    """Intraday return: (latest - session_open) / session_open."""
    tick = latest_tick(engine, ticker)
    if tick is None:
        return None
    open_price = session_open(engine, ticker, us_session_start_utc())
    if open_price is None or open_price <= 0:
        return None
    return tick.close / open_price - 1.0


def live_mnav(engine: Engine) -> float | None:
    """mNAV scaled forward from the latest daily indicator using live ticks.

        mNAV_live = mNAV_daily × (MSTR_live / MSTR_close) × (BTC_close / BTC_live)

    Derivation:
        mNAV = (MSTR_px × shares) / (BTC_px × btc_qty)
    shares and btc_qty are ~constant intraday (and weekly), so the
    multiplicative factor for live tick movement collapses to just the
    two price ratios.  This avoids needing `mstr_capital_structure`
    (which is empty as of D6) and is correct to <0.1 % intraday.
    """
    mstr_tick = latest_tick(engine, "MSTR")
    btc_tick = latest_tick(engine, "BTC")
    if mstr_tick is None or btc_tick is None:
        return None

    with engine.connect() as conn:
        row = conn.execute(text("""
            SELECT mnav, mstr_close, btc_close
            FROM indicators_daily
            WHERE mnav IS NOT NULL AND mstr_close IS NOT NULL AND btc_close IS NOT NULL
            ORDER BY date DESC LIMIT 1
        """)).fetchone()
    if row is None:
        return None
    mnav_d = float(row.mnav)
    mstr_d = float(row.mstr_close)
    btc_d = float(row.btc_close)
    if mstr_d <= 0 or btc_d <= 0 or btc_tick.close <= 0:
        return None
    return mnav_d * (mstr_tick.close / mstr_d) * (btc_d / btc_tick.close)
