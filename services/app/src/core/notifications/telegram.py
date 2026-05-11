"""Telegram bot client for sending alerts and briefings.

Uses Telegram Bot API directly (no python-telegram-bot dep needed for
this scope).  Supports:

  • One-way send (`send_plain`, `send_message`)
  • Inline keyboard send (`send_with_keyboard`)
  • Callback acknowledgement (`answer_callback_query`)
  • Update polling (`get_updates`)

Pull-based polling (rather than webhooks) keeps the bot working locally
with no inbound exposure.  When we deploy to Oracle Cloud later, the
polling task can stay or be swapped for a webhook — same client.

Requires TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID in env.
"""
from __future__ import annotations

import logging
from typing import Any

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

    def _url(self, method: str) -> str:
        return f"{API_BASE}/bot{self.token}/{method}"

    def send_message(
        self,
        text: str,
        parse_mode: str = "MarkdownV2",
        disable_notification: bool = False,
        chat_id: int | str | None = None,
        reply_markup: dict | None = None,
    ) -> dict | None:
        if not self.is_configured:
            logger.warning("Telegram not configured — message dropped")
            return None
        body: dict[str, Any] = {
            "chat_id": chat_id or self.chat_id,
            "text": text,
            "parse_mode": parse_mode,
            "disable_notification": disable_notification,
        }
        if reply_markup is not None:
            body["reply_markup"] = reply_markup
        response = httpx.post(self._url("sendMessage"), json=body, timeout=10)
        if response.status_code >= 400:
            logger.error(
                "Telegram send failed: status=%s body=%s",
                response.status_code, response.text[:300],
            )
        response.raise_for_status()
        return response.json()

    def send_plain(
        self,
        text: str,
        chat_id: int | str | None = None,
        reply_markup: dict | None = None,
    ) -> dict | None:
        """Plain text — no Markdown escaping concerns."""
        return self.send_message(text, parse_mode="", chat_id=chat_id, reply_markup=reply_markup)

    def send_with_keyboard(
        self,
        text: str,
        keyboard: list[list[dict[str, str]]],
        chat_id: int | str | None = None,
    ) -> dict | None:
        """Plain text + inline keyboard.

        `keyboard` is a list of rows; each row is a list of buttons.
        Each button is `{"text": "Label", "callback_data": "action_id"}`.
        """
        return self.send_plain(
            text,
            chat_id=chat_id,
            reply_markup={"inline_keyboard": keyboard},
        )

    def answer_callback_query(self, callback_id: str, text: str = "") -> dict | None:
        """Stop the spinning loader on a tapped button."""
        if not self.is_configured:
            return None
        response = httpx.post(
            self._url("answerCallbackQuery"),
            json={"callback_query_id": callback_id, "text": text},
            timeout=10,
        )
        if response.status_code >= 400:
            logger.warning(
                "Telegram answerCallbackQuery failed: %s %s",
                response.status_code, response.text[:200],
            )
        return response.json() if response.status_code < 400 else None

    def get_updates(
        self,
        offset: int = 0,
        timeout: int = 0,
        allowed_updates: list[str] | None = None,
    ) -> list[dict]:
        """Long-poll the Bot API for incoming updates.

        Pass `offset = last_seen_update_id + 1` to acknowledge / advance
        past prior messages.  Telegram retains updates for 24h.
        """
        if not self.is_configured:
            return []
        body: dict[str, Any] = {"offset": offset, "timeout": timeout}
        if allowed_updates is not None:
            body["allowed_updates"] = allowed_updates
        try:
            response = httpx.post(
                self._url("getUpdates"),
                json=body,
                timeout=timeout + 10,
            )
            response.raise_for_status()
        except Exception:
            logger.exception("Telegram getUpdates failed")
            return []
        data = response.json()
        return data.get("result", []) if data.get("ok") else []
