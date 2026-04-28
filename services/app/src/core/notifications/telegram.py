"""Telegram bot client for sending alerts and briefings.

Uses Telegram Bot API directly (no python-telegram-bot dep needed for one-way
sending). Requires TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID in env.
"""
from __future__ import annotations

import logging

import httpx

from core.config import get_settings

logger = logging.getLogger(__name__)

API_BASE = "https://api.telegram.org"


class TelegramNotConfigured(RuntimeError):
    """Raised when bot token or chat id is missing."""


class TelegramClient:
    def __init__(self):
        settings = get_settings()
        self.token = settings.telegram_bot_token
        self.chat_id = settings.telegram_chat_id

    @property
    def is_configured(self) -> bool:
        return bool(self.token) and bool(self.chat_id)

    def send_message(
        self,
        text: str,
        parse_mode: str = "MarkdownV2",
        disable_notification: bool = False,
    ) -> dict | None:
        if not self.is_configured:
            logger.warning("Telegram not configured — message dropped")
            return None
        url = f"{API_BASE}/bot{self.token}/sendMessage"
        response = httpx.post(
            url,
            json={
                "chat_id": self.chat_id,
                "text": text,
                "parse_mode": parse_mode,
                "disable_notification": disable_notification,
            },
            timeout=10,
        )
        if response.status_code >= 400:
            logger.error(
                "Telegram send failed: status=%s body=%s",
                response.status_code, response.text[:300],
            )
        response.raise_for_status()
        return response.json()

    def send_plain(self, text: str) -> dict | None:
        """Plain text — no Markdown escaping concerns."""
        return self.send_message(text, parse_mode="")
