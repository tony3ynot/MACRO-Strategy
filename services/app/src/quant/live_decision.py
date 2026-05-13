"""Live (intraday) decision state machine.

Single concern: turn the Phase 2b "pre-close panic warning" into an
authoritative action signal when an intraday drawdown is sustained.

State machine (all keys in Redis with 24h TTL):
  live:panic:streak           int — consecutive intraday-check failures
  live:panic:active           "1" — set when streak ≥ SUSTAIN_CHECKS
  live:panic:entered_at       ISO datetime when active was set

Rules:
  • Each intraday check (every 3 min) computes live book DD.
  • DD ≤ PANIC_DD_THRESHOLD → streak += 1; else streak = 0.
  • streak ≥ SUSTAIN_CHECKS (= 15 min @ 3-min cadence) → enter active.
  • Already active → stays active until daily reset.
  • If daily strategy doesn't enter panic on its own → daily reset
    auto-clears live state ("false alarm" handled by re-broadcast).

The actual *trade decision* (apply panic_scale × 0.5 to weights) is
applied inside `_fetch_today_view` so every surface — daily briefing,
/today, change-trigger broadcaster — sees the same target.
"""
from __future__ import annotations

import logging
import os
from datetime import datetime, timezone

import redis

logger = logging.getLogger(__name__)


PANIC_DD_THRESHOLD = -0.15            # same as daily strategy's dd_panic_trigger
SUSTAIN_CHECKS = 5                    # 5 × 3-min cadence = 15 min
PANIC_SCALE = 0.50                    # same as daily strategy's panic_scale
STATE_TTL_SECONDS = 24 * 3600


KEY_STREAK = "live:panic:streak"
KEY_ACTIVE = "live:panic:active"
KEY_ENTERED_AT = "live:panic:entered_at"


def _r() -> redis.Redis:
    return redis.from_url(os.environ["REDIS_URL"], decode_responses=True)


# ─── State accessors ──────────────────────────────────────────────────


def is_active() -> bool:
    return _r().exists(KEY_ACTIVE) > 0


def entered_at() -> datetime | None:
    raw = _r().get(KEY_ENTERED_AT)
    return datetime.fromisoformat(raw) if raw else None


def _streak() -> int:
    raw = _r().get(KEY_STREAK)
    return int(raw) if raw else 0


# ─── State transitions ────────────────────────────────────────────────


def observe_dd(live_dd: float) -> bool:
    """Feed one intraday DD observation; return True iff state transitioned
    from inactive → active *on this call*.

    Idempotent w.r.t. staying-active or staying-inactive.
    """
    r = _r()
    if live_dd <= PANIC_DD_THRESHOLD:
        new_streak = _streak() + 1
        r.set(KEY_STREAK, new_streak, ex=STATE_TTL_SECONDS)
        if new_streak >= SUSTAIN_CHECKS and not r.exists(KEY_ACTIVE):
            now = datetime.now(timezone.utc).isoformat()
            r.set(KEY_ACTIVE, "1", ex=STATE_TTL_SECONDS)
            r.set(KEY_ENTERED_AT, now, ex=STATE_TTL_SECONDS)
            logger.warning("live panic ENTERED — DD=%s streak=%s", live_dd, new_streak)
            return True
        return False
    else:
        # DD recovered → reset streak but DON'T clear active (only daily compute does)
        r.delete(KEY_STREAK)
        return False


def reset_all() -> None:
    """Called by the daily compute task — wipes the entire live state.

    The daily strategy is authoritative every morning.  If it confirms
    panic, the next intraday cycle will re-enter live_panic in 15 min
    on its own.  If it doesn't, the false alarm is automatically
    cleared and the change-broadcaster will fire a "restore" signal.
    """
    r = _r()
    cleared = 0
    for k in (KEY_STREAK, KEY_ACTIVE, KEY_ENTERED_AT):
        cleared += r.delete(k)
    if cleared:
        logger.info("live panic state cleared by daily reset")


# ─── Weight modifier ──────────────────────────────────────────────────


def apply_live_panic(weights: dict[str, float]) -> dict[str, float]:
    """If live_panic is active, scale all weights by PANIC_SCALE.

    Identical math to the daily breaker's panic branch, so the live
    weights match what the daily strategy will produce tomorrow if it
    agrees with us.
    """
    if not is_active():
        return weights
    return {t: w * PANIC_SCALE for t, w in weights.items()}
