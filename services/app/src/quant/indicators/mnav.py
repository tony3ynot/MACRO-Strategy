"""mNAV — MSTR market cap relative to underlying BTC treasury value.

    mNAV = (mstr_close * shares_outstanding) / (btc_qty * btc_close)

A value of 1.0 means MSTR trades at fair NAV; 1.5 means investors are
paying a 50 % premium over the BTC the company holds.

LIMITATION (acknowledged for Phase 2 D1):
yfinance only ships *current* shares-outstanding, so historical mNAV is
approximated by holding the latest share count constant backward. This
under-states early-2020 mNAV (MSTR has roughly 4× more shares now after
ATM issuance) but the *direction* and *recent* values are accurate.

Phase 2.5 will replace this with a SEC 10-Q / proxy scraper that gives
true point-in-time shares_out and stores it in `mstr_capital_structure`.
The compute layer reads from `mstr_capital_structure` first; this module
exposes a fallback that uses the snapshot.
"""
from __future__ import annotations

import pandas as pd


def mnav_simple(
    mstr_close: pd.Series,
    btc_close: pd.Series,
    btc_qty: pd.Series,
    shares_outstanding: float,
) -> pd.Series:
    """All series date-indexed; shares_outstanding is a single scalar.

    `btc_qty` is forward-filled so that mNAV is defined on every day
    between MSTR's first-BTC-purchase and today (holdings change
    sporadically — usually 5-15 events per year).
    """
    df = pd.concat(
        [
            mstr_close.rename("mstr_close"),
            btc_close.rename("btc_close"),
            btc_qty.rename("btc_qty"),
        ],
        axis=1,
    ).sort_index()
    df["btc_qty"] = df["btc_qty"].ffill()
    df = df.dropna(subset=["mstr_close", "btc_close", "btc_qty"])
    mcap = df["mstr_close"] * shares_outstanding
    treasury = df["btc_qty"] * df["btc_close"]
    return mcap / treasury
