"""Buy-and-hold and naive-rebalance benchmarks."""
from __future__ import annotations


def buy_and_hold(ticker: str):
    def strat(state, idx):
        return {ticker: 1.0} if state.has(ticker, idx, "close") else {}
    return strat


def fixed_mix(weights: dict[str, float], rebalance_every: int = 21):
    """Rebalance to fixed weights every N bars (default monthly)."""
    def strat(state, idx):
        if idx == 0 or idx % rebalance_every == 0:
            return dict(weights)
        return state.last_weights
    return strat
