"""Backfill Hyperliquid BTC perp funding history.

Default range: 2023-06-01 → today (Hyperliquid mainnet launched mid-2023).
Funding events HOURLY (24/day). ~3 years ≈ 25,000 events.

Usage:
    python -m scripts.backfill_hyperliquid_funding
    python -m scripts.backfill_hyperliquid_funding --start 2024-01-01
"""
from __future__ import annotations

import argparse
import logging
import sys
from datetime import date

from connectors.hyperliquid_perp import HyperliquidFundingIngestor

DEFAULT_START = date(2023, 6, 1)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--start", type=date.fromisoformat, default=DEFAULT_START)
    parser.add_argument("--end", type=date.fromisoformat, default=date.today())
    parser.add_argument("--mode", choices=["backfill", "daily"], default="backfill")
    parser.add_argument("--log-level", default="INFO")
    args = parser.parse_args()

    logging.basicConfig(
        level=getattr(logging, args.log_level.upper()),
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    print(f"Hyperliquid funding backfill: {args.start} → {args.end} (mode={args.mode})")

    ingestor = HyperliquidFundingIngestor()
    result = ingestor.run(args.start, args.end, mode=args.mode)
    print(f"  done: {result.rows} rows in {result.duration_seconds:.1f}s")
    return 0


if __name__ == "__main__":
    sys.exit(main())
