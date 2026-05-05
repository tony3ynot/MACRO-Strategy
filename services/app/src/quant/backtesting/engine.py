"""Minimal backtest engine for daily-bar long-only weight strategies.

Contract
--------
A strategy is a callable that receives the current state and returns
the *target portfolio weights* dict for the next bar:

    weights = strategy(state, idx) → {ticker: weight}

State exposes price series, indicator series, and the prior weights
through `state.get(ticker, idx, indicator=None)`. Weights sum to the
intended gross exposure (1.0 = 100 %, 2.0 = 2× leverage). Cash is the
implicit residual (1 - sum(weights)).

The engine assumes:
- Trades execute at the same bar's close at which the signal was made.
- No slippage / commissions by default; pass `cost_bps` to deduct a flat
  per-trade cost in basis points of turnover.
- Leveraged synthetic series should already be returns-aware (handled
  by the data layer, not the engine).

Outputs
-------
BacktestResult: dataclass with equity curve, weights history, trade log
                and computed metrics.
"""
from __future__ import annotations

import logging
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import date

import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)

EPS = 1e-9


@dataclass
class StrategyState:
    """Lightweight handle passed to strategy callables.

    Strategies access prices/indicators through `state.get` so we can
    transparently swap in different storage backends (DataFrame, dict,
    DB-loaded panel)."""
    panel: pd.DataFrame              # MultiIndex (ticker, indicator) → date columns OR
                                     # date-indexed wide DataFrame; see _flatten_panel.
    dates: pd.DatetimeIndex
    indicators: pd.DataFrame         # date-indexed wide DataFrame with all indicator
                                     # columns (mnav, btc_vrp, beta_iv, etc.)
    last_weights: dict[str, float] = field(default_factory=dict)

    def price(self, ticker: str, idx: int) -> float:
        return float(self.panel[(ticker, "close")].iloc[idx])

    def indicator(self, ticker: str, idx: int, name: str) -> float:
        return float(self.panel[(ticker, name)].iloc[idx])

    def has(self, ticker: str, idx: int, name: str = "close") -> bool:
        try:
            v = self.panel[(ticker, name)].iloc[idx]
            return pd.notna(v)
        except (KeyError, IndexError):
            return False

    def macro(self, name: str, idx: int) -> float | None:
        if name not in self.indicators.columns:
            return None
        v = self.indicators[name].iloc[idx]
        return None if pd.isna(v) else float(v)


@dataclass
class BacktestResult:
    name: str
    equity: pd.Series
    weights: pd.DataFrame            # rows = dates, cols = tickers
    trades: pd.DataFrame             # date, ticker, delta_weight
    metrics: dict[str, float]


def _annualised_return(equity: pd.Series) -> float:
    if len(equity) < 2:
        return 0.0
    total = equity.iloc[-1] / equity.iloc[0]
    days = (equity.index[-1] - equity.index[0]).days
    if days <= 0:
        return 0.0
    return float(total ** (365.25 / days) - 1.0)


def _max_drawdown(equity: pd.Series) -> float:
    peak = equity.cummax()
    dd = equity / peak - 1.0
    return float(dd.min())


def _sharpe(equity: pd.Series, periods_per_year: int = 252) -> float:
    rets = equity.pct_change().dropna()
    if rets.std(ddof=0) < EPS:
        return 0.0
    return float(np.sqrt(periods_per_year) * rets.mean() / rets.std(ddof=0))


def _calmar(equity: pd.Series) -> float:
    cagr = _annualised_return(equity)
    mdd = _max_drawdown(equity)
    return float(cagr / abs(mdd)) if mdd < 0 else 0.0


def _compute_metrics(equity: pd.Series, trades: pd.DataFrame) -> dict[str, float]:
    return {
        "cagr": _annualised_return(equity),
        "mdd": _max_drawdown(equity),
        "sharpe": _sharpe(equity),
        "calmar": _calmar(equity),
        "final_x": float(equity.iloc[-1] / equity.iloc[0]),
        "trades": int(len(trades)),
    }


def run_backtest(
    name: str,
    panel: pd.DataFrame,                  # MultiIndex columns (ticker, field)
    indicators: pd.DataFrame,             # date-indexed indicator panel
    strategy: Callable[[StrategyState, int], dict[str, float]],
    seed: float = 1.0,
    cost_bps: float = 0.0,
    start_date: date | None = None,
    end_date: date | None = None,
) -> BacktestResult:
    """Run a single strategy and return the backtest result."""
    panel = panel.sort_index()
    if start_date is not None:
        panel = panel.loc[panel.index >= pd.Timestamp(start_date)]
    if end_date is not None:
        panel = panel.loc[panel.index <= pd.Timestamp(end_date)]
    indicators = indicators.reindex(panel.index)
    if panel.empty:
        raise ValueError("empty panel after date filtering")

    dates = panel.index
    state = StrategyState(panel=panel, dates=dates, indicators=indicators)

    tickers = sorted({t for (t, _f) in panel.columns})
    n_days = len(dates)
    equity = np.zeros(n_days)
    equity[0] = seed

    weights_history = np.zeros((n_days, len(tickers)))
    weight_idx = {t: j for j, t in enumerate(tickers)}

    last_weights = {t: 0.0 for t in tickers}
    trade_log: list[tuple] = []
    cost_rate = cost_bps / 1e4

    for i in range(n_days):
        if i > 0:
            # Daily P&L from the *previous bar's* weights × today's return
            ret = 0.0
            for t, w in last_weights.items():
                if w == 0.0 or not state.has(t, i, "close") or not state.has(t, i - 1, "close"):
                    continue
                p_now = state.price(t, i)
                p_prev = state.price(t, i - 1)
                if p_prev > 0:
                    ret += w * (p_now / p_prev - 1.0)
            equity[i] = equity[i - 1] * (1.0 + ret)

        # Compute target weights for next-bar exposure
        try:
            target = strategy(state, i) or {}
        except Exception as exc:
            logger.exception("strategy raised on idx=%s: %s", i, exc)
            target = last_weights

        # Sanitize: drop tickers without price today, normalise NaN weights
        clean: dict[str, float] = {}
        for t, w in target.items():
            if t not in weight_idx or not state.has(t, i, "close"):
                continue
            if w is None or pd.isna(w):
                continue
            clean[t] = float(w)

        # Compute turnover for cost
        if cost_rate > 0:
            turnover = 0.0
            all_t = set(last_weights) | set(clean)
            for t in all_t:
                turnover += abs(clean.get(t, 0.0) - last_weights.get(t, 0.0))
            equity[i] *= max(0.0, 1.0 - turnover * cost_rate)

        # Log trades (any change)
        for t in set(last_weights) | set(clean):
            dw = clean.get(t, 0.0) - last_weights.get(t, 0.0)
            if abs(dw) > EPS:
                trade_log.append((dates[i], t, dw, clean.get(t, 0.0)))

        # Persist weights
        for t in tickers:
            weights_history[i, weight_idx[t]] = clean.get(t, 0.0)
        last_weights = {t: clean.get(t, 0.0) for t in tickers}
        state.last_weights = last_weights

    equity_series = pd.Series(equity, index=dates, name="equity")
    weights_df = pd.DataFrame(weights_history, index=dates, columns=tickers)
    trades_df = pd.DataFrame(trade_log, columns=["date", "ticker", "delta_weight", "new_weight"])
    metrics = _compute_metrics(equity_series, trades_df)
    return BacktestResult(
        name=name,
        equity=equity_series,
        weights=weights_df,
        trades=trades_df,
        metrics=metrics,
    )


def compare(results: list[BacktestResult]) -> pd.DataFrame:
    """Return a tidy comparison frame of metrics across strategies."""
    rows = [{"strategy": r.name, **r.metrics} for r in results]
    df = pd.DataFrame(rows).set_index("strategy")
    # Format key columns for display
    return df[["cagr", "mdd", "sharpe", "calmar", "final_x", "trades"]]
