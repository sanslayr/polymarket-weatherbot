from __future__ import annotations

import os


def _split_targets(raw: str) -> list[str]:
    return [item.strip() for item in str(raw or "").split(",") if item.strip()]


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


def resolve_telegram_alert_targets(explicit_targets: list[str] | None = None) -> list[str]:
    if explicit_targets:
        targets = [str(item).strip() for item in explicit_targets if str(item).strip()]
        if targets:
            return list(dict.fromkeys(targets))

    multi = _split_targets(os.getenv("TELEGRAM_ALERT_TARGETS") or "")
    if multi:
        return list(dict.fromkeys(multi))

    return [resolve_telegram_alert_target()]
