"""Per-chat user state for the Telegram bot.

Stored in Redis (single-user setup; trivially scales to multi-user by
keying on chat_id).  Five things we track:

  • balance         — total invested capital, set via /setbalance
  • deploy_date     — first day the user committed to following the strategy
                      (anchor for realized-P&L tracking)
  • positions       — current target weights the user is *supposed* to hold
                      (mirror of the last signal we sent them)
  • last_target     — last target weights the bot signalled (hash) — used
                      to decide whether today's signal is new
  • last_signal_at  — date of the most recent change-triggered signal

We use Redis rather than a DB because (a) we already run Redis for
celery, (b) state is small + non-relational, (c) atomic ops are
convenient for the read-compare-write pattern in change-triggered
notification.
"""
from __future__ import annotations

import json
import logging
import os
from dataclasses import dataclass
from datetime import date

import redis

logger = logging.getLogger(__name__)


def _r() -> redis.Redis:
    return redis.from_url(os.environ["REDIS_URL"], decode_responses=True)


def _k(chat_id: int | str, field: str) -> str:
    return f"user:{chat_id}:{field}"


# ─── Balance + deploy date ────────────────────────────────────────────


def set_balance(chat_id: int | str, balance_usd: float, today: date | None = None) -> None:
    """Set initial capital and anchor the deploy date to today."""
    r = _r()
    r.set(_k(chat_id, "balance"), balance_usd)
    if not r.exists(_k(chat_id, "deploy_date")):
        d = today or date.today()
        r.set(_k(chat_id, "deploy_date"), d.isoformat())


def get_balance(chat_id: int | str) -> float | None:
    raw = _r().get(_k(chat_id, "balance"))
    return float(raw) if raw else None


def get_deploy_date(chat_id: int | str) -> date | None:
    raw = _r().get(_k(chat_id, "deploy_date"))
    return date.fromisoformat(raw) if raw else None


def reset_deploy(chat_id: int | str) -> None:
    """Drop deploy_date + positions — used when user wants a clean restart."""
    r = _r()
    r.delete(_k(chat_id, "deploy_date"))
    r.delete(_k(chat_id, "positions"))
    r.delete(_k(chat_id, "last_target"))
    r.delete(_k(chat_id, "last_signal_at"))


# ─── Position mirror + signal de-dupe ─────────────────────────────────


def get_positions(chat_id: int | str) -> dict[str, float]:
    raw = _r().get(_k(chat_id, "positions"))
    return json.loads(raw) if raw else {}


def set_positions(chat_id: int | str, weights: dict[str, float]) -> None:
    cleaned = {t: round(float(w), 6) for t, w in weights.items() if float(w) > 0.0001}
    _r().set(_k(chat_id, "positions"), json.dumps(cleaned, sort_keys=True))


def _target_fingerprint(weights: dict[str, float]) -> str:
    """Hashable fingerprint so we can detect a change cheaply."""
    cleaned = {t: round(float(w), 4) for t, w in weights.items() if float(w) > 0.0001}
    return json.dumps(cleaned, sort_keys=True)


def is_new_target(chat_id: int | str, weights: dict[str, float]) -> bool:
    """True iff the target weights differ from the last one we signalled.

    Threshold: 1 bp rounding in `_target_fingerprint`.  Below that we
    treat the target as unchanged and suppress the notification.
    """
    new = _target_fingerprint(weights)
    last = _r().get(_k(chat_id, "last_target"))
    return new != last


def record_signal(chat_id: int | str, weights: dict[str, float], signal_date: date) -> None:
    r = _r()
    r.set(_k(chat_id, "last_target"), _target_fingerprint(weights))
    r.set(_k(chat_id, "last_signal_at"), signal_date.isoformat())
    set_positions(chat_id, weights)


def get_last_signal_at(chat_id: int | str) -> date | None:
    raw = _r().get(_k(chat_id, "last_signal_at"))
    return date.fromisoformat(raw) if raw else None


# ─── Convenience accessor ─────────────────────────────────────────────


@dataclass
class UserState:
    chat_id: int
    balance: float | None
    deploy_date: date | None
    positions: dict[str, float]
    last_signal_at: date | None

    @property
    def is_configured(self) -> bool:
        return self.balance is not None and self.deploy_date is not None


def load(chat_id: int | str) -> UserState:
    return UserState(
        chat_id=int(chat_id),
        balance=get_balance(chat_id),
        deploy_date=get_deploy_date(chat_id),
        positions=get_positions(chat_id),
        last_signal_at=get_last_signal_at(chat_id),
    )


# ─── Known chats (so the broadcaster knows who to message) ────────────
# In multi-user mode this would come from a separate registration flow.
# For now we maintain a single SET of chat_ids that have ever interacted.


def remember_chat(chat_id: int | str) -> None:
    _r().sadd("user:chat_ids", str(chat_id))


def all_chat_ids() -> list[int]:
    raw = _r().smembers("user:chat_ids")
    return sorted(int(c) for c in raw) if raw else []
