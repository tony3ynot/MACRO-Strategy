"""Run the multi-tier MACRO Strategy backtest comparison.

Three reporting windows:
  Long      : 2017-01-03 → today.  Synthetic MSTU/MSTY/MSTZ pre-2024 + no
              BTC IV / VRP / RV pre-2021-03 (Deribit DVOL launch). HARVEST
              and HEDGE states are dormant throughout the BTC-IV gap, so
              the strategy degrades to vol-targeted MSTR/MSTU.  Useful as
              a stress test, NOT as a fair-alpha estimate.
  Extended  : 2021-03-25 → today.  All MACRO indicators present; MSTU/
              MSTY/MSTZ still synthetic pre-launch.  This is the longest
              window where the strategy can fully express itself.
  Live      : 2024-05-17 → today.  Real ETFs + dense MACRO indicators.

Strategies compared:
  - Buy-and-hold MSTR / MSTU / MSTY
  - Fixed mixes (50/50 MSTR-MSTY, 70/30 MSTR-MSTU)
  - macro_trend            : 4-state allocator (ACCUMULATE / HARVEST /
                             HEDGE / WAIT) with vol-targeting and mNAV
                             overlay
  - macro_regime           : pure indicator-driven regime classifier

Outputs a printed comparison table per window and a CSV at
/tmp/macro_backtest_results.csv.
"""
from __future__ import annotations

import argparse
import logging
import sys
from datetime import date

import pandas as pd

from core.db import make_sync_engine
from quant.backtesting.data import assemble_full_panel
from quant.backtesting.engine import compare, run_backtest
from quant.backtesting.strategies.benchmarks import buy_and_hold, fixed_mix
from quant.backtesting.strategies.macro_regime import make_macro_regime_strategy
from quant.backtesting.strategies.macro_trend import make_macro_trend
from quant.backtesting.strategies.macro_trend_v2 import make_macro_trend_v2
from quant.backtesting.strategies.macro_trend_v3 import make_macro_trend_v3

logger = logging.getLogger(__name__)


def all_strategies():
    return [
        ("BH MSTR",                       buy_and_hold("MSTR")),
        ("BH MSTU (synth+real)",          buy_and_hold("MSTU")),
        ("BH MSTY (synth+real)",          buy_and_hold("MSTY")),
        ("Mix 50% MSTR / 50% MSTY",       fixed_mix({"MSTR": 0.5, "MSTY": 0.5})),
        ("Mix 70% MSTR / 30% MSTU",       fixed_mix({"MSTR": 0.7, "MSTU": 0.3})),
        ("macro_trend",                   make_macro_trend()),
        ("macro_trend_v2",                make_macro_trend_v2()),
        ("macro_trend_v3",                make_macro_trend_v3()),
        ("macro_regime",                  make_macro_regime_strategy()),
    ]


def run_window(panel, indicators, start, end, label, cost_bps):
    print(f"\n=== {label}: {start} → {end} ===")
    results = []
    for name, factory in all_strategies():
        res = run_backtest(
            name=name,
            panel=panel,
            indicators=indicators,
            strategy=factory,
            start_date=start,
            end_date=end,
            cost_bps=cost_bps,
        )
        results.append(res)
    table = compare(results).sort_values("cagr", ascending=False)
    pd.set_option(
        "display.float_format",
        lambda v: f"{v:>+8.2%}" if -10 < v < 10 else f"{v:>10.2f}",
    )
    print(table.to_string())
    pd.reset_option("display.float_format")
    return table


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--cost-bps", type=float, default=2.0,
                        help="Per-trade cost in bps of turnover (default 2)")
    parser.add_argument("--long-start", type=date.fromisoformat,
                        default=date(2017, 1, 3))
    parser.add_argument("--extended-start", type=date.fromisoformat,
                        default=date(2021, 3, 25),
                        help="Earliest date with all MACRO indicators "
                             "available (Deribit DVOL launch).")
    parser.add_argument("--live-start", type=date.fromisoformat,
                        default=date(2024, 5, 17))
    parser.add_argument("--end", type=date.fromisoformat, default=None)
    parser.add_argument("--out", default="/tmp/macro_backtest_results.csv")
    parser.add_argument("--log-level", default="INFO")
    args = parser.parse_args()

    logging.basicConfig(
        level=getattr(logging, args.log_level.upper()),
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    engine = make_sync_engine()
    logger.info("loading panel + indicators")
    panel, indicators = assemble_full_panel(engine)
    end = args.end or panel.index.max().date()
    logger.info(
        "panel: %s rows, %s tickers; indicators: %s rows",
        len(panel), len({t for (t, _) in panel.columns}), len(indicators),
    )

    long_table = run_window(panel, indicators, args.long_start, end,
                            "LONG (synthetic+real, pre-2021 indicators absent)",
                            args.cost_bps)
    extended_table = run_window(panel, indicators, args.extended_start, end,
                                "EXTENDED (all MACRO indicators present, synth ETFs pre-2024)",
                                args.cost_bps)
    live_table = run_window(panel, indicators, args.live_start, end,
                            "LIVE (real ETFs + dense indicators)", args.cost_bps)

    long_table["window"] = "long"
    extended_table["window"] = "extended"
    live_table["window"] = "live"
    out = pd.concat([long_table, extended_table, live_table])
    out.to_csv(args.out)
    print(f"\n  saved → {args.out}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
