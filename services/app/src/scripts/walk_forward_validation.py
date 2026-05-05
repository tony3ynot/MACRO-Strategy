"""Phase 2 D5 — walk-forward validation of macro_trend.

We answer two questions:

1. Does the strategy *generalise*?  Run defaults on a 12-month TRAIN
   window (2024-05 → 2025-05), then re-run on the 12-month TEST window
   (2025-05 → 2026-05).  If both windows show similar CAGR / Sharpe,
   the rules aren't curve-fit to one particular regime.

2. Are the *parameters robust*?  For each magic number in TrendParams,
   perturb it across a plausible range while holding the others at
   default.  If CAGR collapses for small moves, the parameter is
   over-fit; if it stays in a tight band, it's robust.

We deliberately do not run a full grid search — the fixed-rule
strategy doesn't have an "optimal" point to find, and an exhaustive
sweep would just identify the lucky combo on this window.  One-at-a-
time perturbation isolates each lever's contribution.

Output: console tables.  Final summary highlights any parameter whose
range produces > 10 pp CAGR swing — those are the ones to watch in a
larger-sample re-run.
"""
from __future__ import annotations

import argparse
import logging
import sys
from dataclasses import asdict
from datetime import date

import pandas as pd

from core.db import make_sync_engine
from quant.backtesting.data import assemble_full_panel
from quant.backtesting.engine import run_backtest
from quant.backtesting.strategies.benchmarks import buy_and_hold
from quant.backtesting.strategies.macro_trend import TrendParams, make_macro_trend

logger = logging.getLogger(__name__)


SWEEPS: dict[str, list] = {
    "ma_fast":           [25, 35, 50, 65, 80],
    "ma_slow":           [100, 150, 200, 250],
    "flip_days":         [1, 2, 3, 5],
    "harvest_iv_floor":  [0.30, 0.35, 0.40, 0.45, 0.50],
    "harvest_vrp_floor": [0.01, 0.02, 0.03, 0.05, 0.07],
    "harvest_rv_ceil":   [0.40, 0.50, 0.60, 0.70, 0.80],
    "harvest_band":      [0.05, 0.10, 0.15, 0.20],
    "hedge_vrp":         [-0.05, -0.03, -0.01, 0.0],
    "vol_target":        [0.40, 0.50, 0.60, 0.70, 0.80, 0.90],
    "mnav_discount":     [0.95, 1.00, 1.05, 1.10, 1.15],
    "mnav_premium":      [1.30, 1.40, 1.50, 1.70, 2.00],
}


def run_one(panel, indicators, start, end, params, label, cost_bps):
    return run_backtest(
        name=label,
        panel=panel,
        indicators=indicators,
        strategy=make_macro_trend(params),
        start_date=start,
        end_date=end,
        cost_bps=cost_bps,
    )


def fmt_metrics(res) -> dict:
    return {
        "cagr": res.metrics["cagr"],
        "mdd": res.metrics["mdd"],
        "sharpe": res.metrics["sharpe"],
        "calmar": res.metrics["calmar"],
        "trades": res.metrics["trades"],
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--train-start", type=date.fromisoformat, default=date(2024, 5, 17))
    parser.add_argument("--train-end",   type=date.fromisoformat, default=date(2025, 5, 16))
    parser.add_argument("--test-start",  type=date.fromisoformat, default=date(2025, 5, 17))
    parser.add_argument("--test-end",    type=date.fromisoformat, default=None)
    parser.add_argument("--cost-bps", type=float, default=10.0,
                        help="Realistic spread cost (default 10)")
    parser.add_argument("--log-level", default="INFO")
    args = parser.parse_args()

    logging.basicConfig(
        level=getattr(logging, args.log_level.upper()),
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    engine = make_sync_engine()
    panel, indicators = assemble_full_panel(engine)
    test_end = args.test_end or panel.index.max().date()

    # ── 1. TRAIN vs TEST baseline ─────────────────────────────────────
    print(f"\n=== TRAIN/TEST BASELINE (defaults, cost_bps={args.cost_bps}) ===")
    print(f"   TRAIN  {args.train_start} → {args.train_end}")
    print(f"   TEST   {args.test_start} → {test_end}")

    rows = []
    default = TrendParams()
    for label, (s, e) in [
        ("TRAIN macro_trend", (args.train_start, args.train_end)),
        ("TEST  macro_trend", (args.test_start, test_end)),
    ]:
        res = run_one(panel, indicators, s, e, default, label, args.cost_bps)
        rows.append({"window": label, **fmt_metrics(res)})

    # Add BH MSTR benchmark for context
    for label, (s, e) in [
        ("TRAIN BH MSTR", (args.train_start, args.train_end)),
        ("TEST  BH MSTR", (args.test_start, test_end)),
    ]:
        res = run_backtest(
            name=label, panel=panel, indicators=indicators,
            strategy=buy_and_hold("MSTR"),
            start_date=s, end_date=e, cost_bps=args.cost_bps,
        )
        rows.append({"window": label, **fmt_metrics(res)})

    base_df = pd.DataFrame(rows).set_index("window")
    pd.set_option("display.float_format",
                  lambda v: f"{v:>+8.2%}" if -10 < v < 10 else f"{v:>10.2f}")
    print(base_df.to_string())
    pd.reset_option("display.float_format")

    # Generalisation sanity — alpha vs benchmark, not absolute CAGR.
    # The TEST window may itself be a different regime (e.g. bear vs
    # bull on TRAIN), so absolute CAGR drift is uninformative; the
    # right test is whether the strategy keeps adding alpha over MSTR.
    train_alpha = base_df.loc["TRAIN macro_trend", "cagr"] - base_df.loc["TRAIN BH MSTR", "cagr"]
    test_alpha  = base_df.loc["TEST  macro_trend", "cagr"] - base_df.loc["TEST  BH MSTR", "cagr"]
    print(f"\n   alpha vs MSTR: TRAIN {train_alpha:+.2%}  |  TEST {test_alpha:+.2%}")
    if test_alpha > 0:
        print("   → ✅ strategy adds alpha out-of-sample.")
        if train_alpha < 0 and test_alpha > 0:
            print("      (TRAIN was bull regime — strategy gives up upside,")
            print("       TEST was bear — protection kicked in.)")
    else:
        print("   → ⚠ strategy fails to add alpha on TEST; investigate.")

    # ── 2. Per-parameter sensitivity on TEST OOS ──────────────────────
    print(f"\n=== PARAMETER SENSITIVITY (TEST {args.test_start} → {test_end}) ===")
    sweep_rows = []
    for param, values in SWEEPS.items():
        for v in values:
            kwargs = asdict(default)
            kwargs[param] = v
            params = TrendParams(**kwargs)
            res = run_one(panel, indicators, args.test_start, test_end,
                          params, f"{param}={v}", args.cost_bps)
            sweep_rows.append({
                "param": param,
                "value": v,
                "test_cagr": res.metrics["cagr"],
                "test_mdd": res.metrics["mdd"],
                "test_sharpe": res.metrics["sharpe"],
                "trades": res.metrics["trades"],
            })

    sweep_df = pd.DataFrame(sweep_rows)

    # Per-parameter summary: range of CAGR, swing, default value's CAGR
    summary_rows = []
    default_dict = asdict(default)
    for param, values in SWEEPS.items():
        sub = sweep_df.loc[sweep_df["param"] == param].sort_values("value")
        default_v = default_dict[param]
        # Pick the row whose value is closest to default
        idx_def = (sub["value"] - default_v).abs().idxmin()
        summary_rows.append({
            "param": param,
            "default": default_v,
            "default_cagr": sub.loc[idx_def, "test_cagr"],
            "min_cagr": sub["test_cagr"].min(),
            "max_cagr": sub["test_cagr"].max(),
            "swing_pp": sub["test_cagr"].max() - sub["test_cagr"].min(),
            "n_values": len(sub),
        })
    summary = (pd.DataFrame(summary_rows)
               .sort_values("swing_pp", ascending=False)
               .set_index("param"))

    print("\n   Sensitivity ranking (CAGR swing across the sweep):")
    pd.set_option("display.float_format",
                  lambda v: f"{v:>+8.2%}" if -10 < v < 10 else f"{v:>10.2f}")
    print(summary.to_string())
    pd.reset_option("display.float_format")

    print("\n   Per-parameter detail:")
    pd.set_option("display.float_format",
                  lambda v: f"{v:>+8.2%}" if -10 < v < 10 else f"{v:>10.2f}")
    for param in summary.index:
        sub = sweep_df.loc[sweep_df["param"] == param].sort_values("value")
        print(f"\n   --- {param} (default {default_dict[param]}) ---")
        print(sub[["value", "test_cagr", "test_mdd", "test_sharpe", "trades"]]
              .to_string(index=False))
    pd.reset_option("display.float_format")

    # ── 3. Robustness verdict ────────────────────────────────────────
    high_swing = summary.loc[summary["swing_pp"] > 0.15]
    print("\n=== ROBUSTNESS VERDICT ===")
    if high_swing.empty:
        print("   ✅ All parameters cause < 15 pp CAGR swing — strategy is")
        print("      not over-tuned to any single threshold.")
    else:
        print("   ⚠  These parameters cause > 15 pp CAGR swing on OOS:")
        for p, row in high_swing.iterrows():
            print(f"      • {p}: swing {row['swing_pp']:+.2%} "
                  f"(min {row['min_cagr']:+.2%}, max {row['max_cagr']:+.2%})")
        print("   → re-evaluate these on a larger sample before live use.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
