from __future__ import annotations

import os
import re
import subprocess
from pathlib import Path
from typing import Any

import requests

from alert_delivery_policy import resolve_telegram_alert_target, resolve_telegram_alert_targets


def _resolve_bot_token(account: str = "weatherbot") -> str:
    env_token = str(os.getenv("TELEGRAM_BOT_TOKEN") or "").strip()
    if env_token:
        return env_token
    config_path = Path.home() / ".openclaw" / "openclaw.json"
    try:
        text = config_path.read_text(encoding="utf-8")
    except Exception:
        return ""
    account_pattern = re.compile(
        rf'["\']{re.escape(account)}["\']\s*:\s*\{{.*?["\']botToken["\']\s*:\s*["\']([^"\']+)["\']',
        re.S,
    )
    match = account_pattern.search(text)
    if match:
        return str(match.group(1) or "").strip()
    default_pattern = re.compile(r'["\']default["\']\s*:\s*\{.*?["\']botToken["\']\s*:\s*["\']([^"\']+)["\']', re.S)
    match = default_pattern.search(text)
    if match:
        return str(match.group(1) or "").strip()
    return ""


def send_telegram_message(
    text: str,
    *,
    chat_id: str | None = None,
    bot_token: str | None = None,
    account: str = "weatherbot",
    parse_mode: str | None = "Markdown",
    disable_web_page_preview: bool = True,
    reply_to_message_id: int | None = None,
    message_thread_id: int | None = None,
    timeout: float = 10.0,
) -> dict[str, Any]:
    token = str(bot_token or _resolve_bot_token(account)).strip()
    chat = resolve_telegram_alert_target(chat_id)
    if not token:
        raise RuntimeError("Missing TELEGRAM_BOT_TOKEN")

    url = f"https://api.telegram.org/bot{token}/sendMessage"
    payload = {
        "chat_id": chat,
        "text": str(text or ""),
        "disable_web_page_preview": disable_web_page_preview,
    }
    if parse_mode:
        payload["parse_mode"] = parse_mode
    if reply_to_message_id is not None:
        payload["reply_to_message_id"] = int(reply_to_message_id)
    if message_thread_id is not None:
        payload["message_thread_id"] = int(message_thread_id)
    response = requests.post(url, json=payload, timeout=timeout)
    response.raise_for_status()
    return response.json()


def send_telegram_messages(
    text: str,
    *,
    chat_ids: list[str] | None = None,
    account: str = "weatherbot",
    parse_mode: str = "Markdown",
    disable_web_page_preview: bool = True,
    timeout: float = 10.0,
) -> list[dict[str, Any]]:
    results: list[dict[str, Any]] = []
    errors: list[str] = []
    for chat in resolve_telegram_alert_targets(chat_ids):
        try:
            results.append(
                send_telegram_message(
                    text,
                    chat_id=chat,
                    account=account,
                    parse_mode=parse_mode,
                    disable_web_page_preview=disable_web_page_preview,
                    timeout=timeout,
                )
            )
        except Exception as exc:
            errors.append(f"{chat}: {exc}")
    if results:
        return results
    if errors:
        raise RuntimeError("All Telegram deliveries failed: " + "; ".join(errors))
    return results


def send_telegram_messages_report(
    text: str,
    *,
    chat_ids: list[str] | None = None,
    account: str = "weatherbot",
    parse_mode: str = "Markdown",
    disable_web_page_preview: bool = True,
    timeout: float = 10.0,
) -> dict[str, Any]:
    targets = resolve_telegram_alert_targets(chat_ids)
    successes: list[dict[str, Any]] = []
    errors: list[dict[str, str]] = []
    for chat in targets:
        try:
            response = send_telegram_message(
                text,
                chat_id=chat,
                account=account,
                parse_mode=parse_mode,
                disable_web_page_preview=disable_web_page_preview,
                timeout=timeout,
            )
            successes.append({"chat_id": chat, "response": response})
        except Exception as exc:
            errors.append({"chat_id": chat, "error": str(exc)})
    if not successes and errors:
        raise RuntimeError("All Telegram deliveries failed: " + "; ".join(f"{item['chat_id']}: {item['error']}" for item in errors))
    return {
        "account": account,
        "targets": targets,
        "successes": successes,
        "errors": errors,
    }


def send_telegram_message_openclaw(
    text: str,
    *,
    chat_id: str | None = None,
    account: str = "weatherbot",
    timeout: float = 15.0,
) -> dict[str, Any]:
    chat = resolve_telegram_alert_target(chat_id)
    proc = subprocess.run(
        [
            "openclaw",
            "message",
            "send",
            "--channel",
            "telegram",
            "--account",
            str(account),
            "--target",
            str(chat),
            "--message",
            str(text or ""),
            "--json",
        ],
        capture_output=True,
        text=True,
        timeout=timeout,
    )
    if proc.returncode != 0:
        raise RuntimeError(proc.stderr.strip() or proc.stdout.strip() or "openclaw telegram send failed")
    import json
    return json.loads(proc.stdout)


def send_telegram_messages_openclaw(
    text: str,
    *,
    chat_ids: list[str] | None = None,
    account: str = "weatherbot",
    timeout: float = 15.0,
) -> list[dict[str, Any]]:
    results: list[dict[str, Any]] = []
    errors: list[str] = []
    for chat in resolve_telegram_alert_targets(chat_ids):
        try:
            results.append(
                send_telegram_message_openclaw(
                    text,
                    chat_id=chat,
                    account=account,
                    timeout=timeout,
                )
            )
        except Exception as exc:
            errors.append(f"{chat}: {exc}")
    if results:
        return results
    if errors:
        raise RuntimeError("All Telegram deliveries failed: " + "; ".join(errors))
    return results
