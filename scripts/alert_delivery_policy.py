from __future__ import annotations

import os


def resolve_telegram_alert_target(explicit_target: str | None = None) -> str:
    target = str(
        explicit_target
        or os.getenv("TELEGRAM_DIRECT_CHAT_ID")
        or os.getenv("TELEGRAM_ALERT_CHAT_ID")
        or os.getenv("TELEGRAM_CHAT_ID")
        or ""
    ).strip()
    if not target:
        raise RuntimeError("Missing TELEGRAM_DIRECT_CHAT_ID/TELEGRAM_ALERT_CHAT_ID/TELEGRAM_CHAT_ID")
    return target
