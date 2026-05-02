"""Per-month spot-anchored Polygon options backfill.

Solves the strike-band drift problem: PolygonOptionsIngestor anchors on
TODAY's MSTR spot, so historical periods when MSTR was at a very
different price miss their ATM contracts. This script walks each month
in the requested range, anchors a new strike band on that month's
median MSTR close, and unions the contracts via Polygon's `as_of`
parameter.

Idempotent — already-ingested contracts (same expiry/strike/type) are
skipped via the inherited resume detector.

Usage:
    # Default: full Polygon Basic 2-year window
    python -m scripts.backfill_polygon_options_historical

    # Custom range
    python -m scripts.backfill_polygon_options_historical \\
        --start 2024-04-29 --end 2026-04-29
"""
from __future__ import annotations

import argparse
import logging
import sys
from datetime import date, timedelta

from connectors.polygon_options_historical import PolygonOptionsHistoricalIngestor


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--start", type=date.fromisoformat,
        default=date.today() - timedelta(days=730),
    )
    parser.add_argument(
        "--end", type=date.fromisoformat, default=date.today(),
    )
    parser.add_argument("--strike-band", type=float, default=0.20)
    parser.add_argument("--max-dte-days", type=int, default=60)
    parser.add_argument("--log-level", default="INFO")
    args = parser.parse_args()

    logging.basicConfig(
        level=getattr(logging, args.log_level.upper()),
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    print(
        f"polygon historical backfill: {args.start} → {args.end} "
        f"(±{args.strike_band:.0%} per-month, max_dte={args.max_dte_days})"
    )

    ingestor = PolygonOptionsHistoricalIngestor(
        strike_pct_band=args.strike_band,
        max_dte_days=args.max_dte_days,
    )
    result = ingestor.run(args.start, args.end, mode="backfill")
    print(f"  done: {result.rows} rows in {result.duration_seconds:.1f}s")
    return 0


if __name__ == "__main__":
    sys.exit(main())
