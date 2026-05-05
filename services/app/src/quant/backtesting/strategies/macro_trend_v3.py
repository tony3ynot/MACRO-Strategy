"""macro_trend v3 — v2 + drawdown circuit breaker.

v2's "always-on MSTR base" architecture is cycle-robust but bled along
with MSTR in the LIVE deep-bear window because nothing dialled exposure
back as the equity curve broke. v3 bolts on a single capital-preservation
gate: when *the strategy's own drawdown* gets bad enough, halve gross
exposure. The freed allocation goes to cash — *not* to a short hedge.
A first cut tried 25 % MSTZ in panic mode and lost money during
recoveries (MSTZ is daily-2× inverse, so 25 % MSTZ = -50 % delta on top
of any small MSTR position; the book ended up modestly net-short and
fought every relief rally).

Two-state hysteresis:
  normal  : run v2 logic at full size
  panic   : multiply every v2 weight by `panic_scale` (default 0.5);
            no short overlay; remainder is implicit cash.

Trigger: book DD ≤ DD_PANIC_TRIGGER → enter panic
Reset  : book DD ≥ DD_PANIC_EXIT    → back to normal

Engine support: the runner now writes `state.equity` and
`state.equity_peak` before each strategy call so this strategy can read
its own drawdown via `state.drawdown`.
"""
from __future__ import annotations

from dataclasses import dataclass, field

from .macro_trend_v2 import TrendV2Params, make_macro_trend_v2


# Halve gross exposure when book is down 20 %; resume full size after
# the curve has recovered to within -10 % of peak.  Both numbers can be
# moved by the parameter sweep without changing the algorithm.
DD_PANIC_TRIGGER = -0.20
DD_PANIC_EXIT = -0.10
PANIC_SCALE = 0.50


@dataclass
class TrendV3Params:
    v2: TrendV2Params = field(default_factory=TrendV2Params)
    dd_panic_trigger: float = DD_PANIC_TRIGGER
    dd_panic_exit: float = DD_PANIC_EXIT
    panic_scale: float = PANIC_SCALE


def make_macro_trend_v3(params: TrendV3Params | None = None):
    p = params or TrendV3Params()
    v2_strat = make_macro_trend_v2(p.v2)
    holder = {"panic": False}

    def strat(state, idx):
        dd = state.drawdown
        if not holder["panic"] and dd <= p.dd_panic_trigger:
            holder["panic"] = True
        elif holder["panic"] and dd >= p.dd_panic_exit:
            holder["panic"] = False

        weights = v2_strat(state, idx) or {}
        if holder["panic"]:
            return {t: w * p.panic_scale for t, w in weights.items()}
        return weights

    return strat
