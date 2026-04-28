"""Backfill btc_dvol via Deribit public API.

DVOL data starts ~2021-03 — earlier dates return empty. Default range covers
the full available history: 2020-01-01 → today (Deribit handles clamping).

Usage:
    python -m scripts.backfill_btc_dvol
    python -m scripts.backfill_btc_dvol --start 2024-01-01
"""
from __future__ import annotations

import argparse
import logging
import sys
from datetime import date

from connectors.deribit_dvol import DeribitDVOLIngestor

DEFAULT_START = date(2020, 1, 1)


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

    print(f"deribit DVOL backfill: {args.start} → {args.end} (mode={args.mode})")

    ingestor = DeribitDVOLIngestor()
    result = ingestor.run(args.start, args.end, mode=args.mode)

    print(
        f"  done: {result.rows} rows in {result.duration_seconds:.1f}s "
        f"({result.source}, {result.mode})"
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
