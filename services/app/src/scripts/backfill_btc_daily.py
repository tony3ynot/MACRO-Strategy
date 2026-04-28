"""Backfill BTC-USD daily OHLCV via Coinbase Exchange public API.

Default range: 2017-01-01 → today. ~12 API calls for full backfill.

Usage:
    python -m scripts.backfill_btc_daily
    python -m scripts.backfill_btc_daily --start 2024-01-01
"""
from __future__ import annotations

import argparse
import logging
import sys
from datetime import date

from connectors.coinbase_btc import CoinbaseBTCDailyIngestor

DEFAULT_START = date(2017, 1, 1)


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

    print(f"coinbase BTC daily backfill: {args.start} → {args.end} (mode={args.mode})")

    ingestor = CoinbaseBTCDailyIngestor()
    result = ingestor.run(args.start, args.end, mode=args.mode)

    print(
        f"  done: {result.rows} rows in {result.duration_seconds:.1f}s "
        f"({result.source}, {result.mode})"
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
