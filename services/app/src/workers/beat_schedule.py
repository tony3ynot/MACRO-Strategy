"""Celery beat schedule.

All times in UTC. Cadence rationale:
- Equity update at 21:30 UTC (~17:30 ET) catches Yahoo's post-close refresh.
- BTC daily after midnight UTC for previous-day catchup.
- Funding rates align with their natural cadences (Binance 8h, others denser).
- Briefing at 00:00 UTC = 09:00 KST.
"""
from __future__ import annotations

from celery.schedules import crontab

beat_schedule = {
    # ── Equity & fundamentals ──────────────────────────────────────────
    "ingest-equity-eod": {
        "task": "workers.tasks.ingest_equity_daily",
        "schedule": crontab(hour=21, minute=30),  # ~17:30 ET, post-US-close
    },
    "ingest-mstr-holdings-daily": {
        "task": "workers.tasks.ingest_mstr_holdings_daily",
        "schedule": crontab(hour=2, minute=30),
    },
    "ingest-yieldmax-msty-weekly": {
        "task": "workers.tasks.ingest_yieldmax_msty_weekly",
        "schedule": crontab(hour=3, minute=0, day_of_week=1),  # Monday
    },

    # ── Crypto ─────────────────────────────────────────────────────────
    "ingest-btc-daily": {
        "task": "workers.tasks.ingest_btc_daily",
        "schedule": crontab(hour=2, minute=0),
    },
    "ingest-btc-dvol": {
        "task": "workers.tasks.ingest_btc_dvol",
        "schedule": crontab(hour="*/6", minute=10),  # every 6h
    },
    "ingest-binance-funding": {
        "task": "workers.tasks.ingest_binance_funding",
        "schedule": crontab(hour="*/8", minute=15),  # every 8h, post-funding
    },
    "ingest-hyperliquid-funding": {
        "task": "workers.tasks.ingest_hyperliquid_funding",
        "schedule": crontab(hour="*/6", minute=20),
    },

    # ── Output ─────────────────────────────────────────────────────────
    "send-daily-briefing": {
        "task": "workers.tasks.send_daily_briefing",
        "schedule": crontab(hour=0, minute=0),  # 00:00 UTC = 09:00 KST
    },
}
