from __future__ import annotations

import os
from typing import Any

import requests

from alert_delivery_policy import resolve_telegram_alert_target


def send_telegram_message(
    text: str,
    *,
    chat_id: str | None = None,
    bot_token: str | None = None,
    parse_mode: str = "Markdown",
    disable_web_page_preview: bool = True,
    timeout: float = 10.0,
) -> dict[str, Any]:
    token = str(bot_token or os.getenv("TELEGRAM_BOT_TOKEN") or "").strip()
    chat = resolve_telegram_alert_target(chat_id)
    if not token:
        raise RuntimeError("Missing TELEGRAM_BOT_TOKEN")

    url = f"https://api.telegram.org/bot{token}/sendMessage"
    payload = {
        "chat_id": chat,
        "text": str(text or ""),
        "parse_mode": parse_mode,
        "disable_web_page_preview": disable_web_page_preview,
    }
    response = requests.post(url, json=payload, timeout=timeout)
    response.raise_for_status()
    return response.json()
