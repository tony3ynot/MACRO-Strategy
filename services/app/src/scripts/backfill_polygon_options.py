"""Backfill MSTR options chain via Polygon Options Basic.

Default: last 7 days, ±50% strike band around recent MSTR close (small
verification slice). Full 2-year backfill takes ~16h at 5 req/min — set
--start to the date 2 years ago and run overnight.

Resume-safe: skips contracts that already have rows in options_chain
within the requested window.

Usage:
    # Tiny verification (~30 contracts, ~6 min):
    python -m scripts.backfill_polygon_options --start 2026-04-21 --end 2026-04-28

    # Full 2-year backfill (overnight, ~16h):
    python -m scripts.backfill_polygon_options --start 2024-04-28 --end 2026-04-28
"""
from __future__ import annotations

import argparse
import logging
import sys
from datetime import date, timedelta

from connectors.polygon_options import PolygonOptionsIngestor


def main() -> int:
    today = date.today()
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--start", type=date.fromisoformat, default=today - timedelta(days=7))
    parser.add_argument("--end", type=date.fromisoformat, default=today)
    parser.add_argument("--mode", choices=["backfill", "daily"], default="backfill")
    parser.add_argument("--strike-band", type=float, default=0.5,
                        help="Include strikes within ±this fraction of MSTR close (default 0.5)")
    parser.add_argument("--log-level", default="INFO")
    args = parser.parse_args()

    logging.basicConfig(
        level=getattr(logging, args.log_level.upper()),
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    print(f"Polygon Options backfill: {args.start} → {args.end} "
          f"(mode={args.mode}, strike±{args.strike_band*100:.0f}%)")

    ingestor = PolygonOptionsIngestor(strike_pct_band=args.strike_band)
    result = ingestor.run(args.start, args.end, mode=args.mode)

    print(
        f"  done: {result.rows} rows in {result.duration_seconds:.1f}s "
        f"({result.source}, {result.mode})"
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
