"""Lightweight strategy ensemble.

Takes a list of `(weight_factor, strategy_callable)` tuples and emits
the convex combination of their target weights each day. Each member
strategy operates on the same `StrategyState` so they all see the
same prices, indicators, and book drawdown.

Use cases:
- v3 + v5 to blend cycle-robust circuit breaker (v3) with the
  no-leveraged-ETF discipline (v5).
- v1 + v3 to mix the deep-bear specialist (v1, lots of WAIT) with
  the cycle-robust core (v3).

The ensemble does not coordinate state between members; each member
keeps its own holder-state via the closure pattern. That means a
50/50 blend of two stateful strategies gets the average of both
states, which is fine for our drawdown-circuit-breaker semantics
because a single breaker dominates the blend at panic time anyway.
"""
from __future__ import annotations

from collections.abc import Callable, Iterable


def make_ensemble(members: Iterable[tuple[float, Callable]]):
    members = list(members)
    if not members:
        raise ValueError("ensemble must have at least one member")
    total = sum(w for w, _ in members)
    if total <= 0:
        raise ValueError("ensemble weights must sum to a positive number")
    members = [(w / total, fn) for w, fn in members]

    def strat(state, idx):
        agg: dict[str, float] = {}
        for weight, fn in members:
            sub = fn(state, idx) or {}
            for t, w in sub.items():
                agg[t] = agg.get(t, 0.0) + weight * w
        # Drop near-zero noise so the engine doesn't log meaningless trades
        return {t: w for t, w in agg.items() if abs(w) > 1e-4}

    return strat
