"""Enrich MSTY distributions with YieldMax ROC classification breakdown.

Default: 2024-01-01 → today (covers MSTY's 2024-02 inception forward).

Idempotent: UPDATE on existing rows; running again refreshes pay_date /
roc_pct / classification with current YieldMax-published values
(they may revise once year-end 1099-DIV lands).

Usage:
    python -m scripts.backfill_yieldmax_msty
"""
from __future__ import annotations

import argparse
import logging
import sys
from datetime import date

from connectors.yieldmax import YieldMaxMSTYIngestor

DEFAULT_START = date(2024, 1, 1)


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

    print(f"YieldMax MSTY classification: {args.start} → {args.end} (mode={args.mode})")

    ingestor = YieldMaxMSTYIngestor()
    result = ingestor.run(args.start, args.end, mode=args.mode)
    print(f"  done: {result.rows} rows updated in {result.duration_seconds:.1f}s")
    return 0


if __name__ == "__main__":
    sys.exit(main())
