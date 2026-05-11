"""Period-level performance reports from a backtest equity curve.

Used by the Telegram briefing bot's `/history` view.  Reuses the same
production allocator the live signals use, run with a slightly more
conservative slippage assumption (15 bps vs the engine default of 10).
"""
from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import date

import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)


@dataclass
class PeriodReturn:
    label: str
    strategy_return: float
    bh_return: float
    days: int


def _period_return(equity: pd.Series) -> float:
    if len(equity) < 2:
        return 0.0
    start = float(equity.iloc[0])
    end = float(equity.iloc[-1])
    if start <= 0:
        return 0.0
    return end / start - 1.0


def yearly_returns(strategy_equity: pd.Series, bh_equity: pd.Series) -> list[PeriodReturn]:
    """One row per calendar year present in the series."""
    out: list[PeriodReturn] = []
    for year, group in strategy_equity.groupby(strategy_equity.index.year):
        bh_group = bh_equity.loc[group.index[0]:group.index[-1]]
        days = (group.index[-1] - group.index[0]).days + 1
        label = str(year) if days > 60 else f"{year} (부분)"
        out.append(PeriodReturn(
            label=label,
            strategy_return=_period_return(group),
            bh_return=_period_return(bh_group),
            days=days,
        ))
    return out


def monthly_returns(strategy_equity: pd.Series, bh_equity: pd.Series, n_recent: int = 12) -> list[PeriodReturn]:
    """Last N calendar months (most recent first)."""
    out: list[PeriodReturn] = []
    grouped = list(strategy_equity.groupby(strategy_equity.index.to_period("M")))
    for period, group in grouped[-n_recent:]:
        bh_group = bh_equity.loc[group.index[0]:group.index[-1]]
        out.append(PeriodReturn(
            label=str(period),
            strategy_return=_period_return(group),
            bh_return=_period_return(bh_group),
            days=(group.index[-1] - group.index[0]).days + 1,
        ))
    return list(reversed(out))


def lifetime_metrics(equity: pd.Series) -> dict[str, float]:
    if len(equity) < 2:
        return {"cagr": 0.0, "mdd": 0.0, "calmar": 0.0, "total": 0.0}
    total = float(equity.iloc[-1] / equity.iloc[0])
    days = (equity.index[-1] - equity.index[0]).days
    cagr = total ** (365.25 / max(days, 1)) - 1.0 if days > 0 else 0.0
    peak = equity.cummax()
    mdd = float((equity / peak - 1.0).min())
    calmar = cagr / abs(mdd) if mdd < 0 else 0.0
    return {"cagr": cagr, "mdd": mdd, "calmar": calmar, "total": total - 1.0}


def build_period_report(
    panel: pd.DataFrame,
    indicators: pd.DataFrame,
    cost_bps: float = 15.0,
) -> dict:
    """Run production strategy + BH MSTR, return year/month/lifetime metrics."""
    from quant.backtesting.engine import run_backtest
    from quant.backtesting.strategies.benchmarks import buy_and_hold
    from quant.backtesting.strategies.macro_trend_v5 import make_macro_trend_v5_with_breaker

    strat_res = run_backtest(
        name="strategy",
        panel=panel,
        indicators=indicators,
        strategy=make_macro_trend_v5_with_breaker(),
        cost_bps=cost_bps,
    )
    bh_res = run_backtest(
        name="bh_mstr",
        panel=panel,
        indicators=indicators,
        strategy=buy_and_hold("MSTR"),
        cost_bps=cost_bps,
    )
    return {
        "lifetime_strategy": lifetime_metrics(strat_res.equity),
        "lifetime_bh": lifetime_metrics(bh_res.equity),
        "yearly": yearly_returns(strat_res.equity, bh_res.equity),
        "monthly": monthly_returns(strat_res.equity, bh_res.equity, n_recent=12),
        "start_date": strat_res.equity.index[0].date(),
        "end_date": strat_res.equity.index[-1].date(),
        "cost_bps": cost_bps,
    }
