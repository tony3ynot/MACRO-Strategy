"""Backfill mstr_btc_holdings via SEC EDGAR 8-K parsing.

Default range: 2020-08-11 (MSTR's first BTC purchase) → today.

Usage:
    python -m scripts.backfill_mstr_holdings
    python -m scripts.backfill_mstr_holdings --start 2024-01-01 --log-level DEBUG
"""
from __future__ import annotations

import argparse
import logging
import sys
from datetime import date

from connectors.sec_edgar import FIRST_BTC_PURCHASE, MSTRBTCHoldingsIngestor


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--start", type=date.fromisoformat, default=FIRST_BTC_PURCHASE)
    parser.add_argument("--end", type=date.fromisoformat, default=date.today())
    parser.add_argument("--mode", choices=["backfill", "daily"], default="backfill")
    parser.add_argument("--log-level", default="INFO")
    args = parser.parse_args()

    logging.basicConfig(
        level=getattr(logging, args.log_level.upper()),
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    print(f"SEC EDGAR MSTR holdings backfill: {args.start} → {args.end} (mode={args.mode})")

    ingestor = MSTRBTCHoldingsIngestor()
    result = ingestor.run(args.start, args.end, mode=args.mode)

    print(
        f"  done: {result.rows} rows in {result.duration_seconds:.1f}s "
        f"({result.source}, {result.mode})"
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
