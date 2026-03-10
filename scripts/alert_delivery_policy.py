from __future__ import annotations

import json
import os
from pathlib import Path


OPENCLAW_CONFIG_PATH = Path.home() / ".openclaw" / "openclaw.json"


def _split_targets(raw: str) -> list[str]:
    return [item.strip() for item in str(raw or "").split(",") if item.strip()]


def _binding_targets_from_openclaw(
    *,
    config_path: Path = OPENCLAW_CONFIG_PATH,
    account_id: str = "weatherbot",
    agent_id: str = "weathernerd",
) -> tuple[str | None, list[str]]:
    try:
        payload = json.loads(config_path.read_text(encoding="utf-8"))
    except Exception:
        return None, []

    direct_target: str | None = None
    multi_targets: list[str] = []
    for binding in payload.get("bindings") or []:
        if not isinstance(binding, dict):
            continue
        if str(binding.get("agentId") or "").strip() != agent_id:
            continue
        match = binding.get("match") or {}
        if not isinstance(match, dict):
            continue
        if str(match.get("channel") or "").strip() != "telegram":
            continue
        if str(match.get("accountId") or "").strip() != account_id:
            continue
        peer = match.get("peer") or {}
        if not isinstance(peer, dict):
            continue
        target = str(peer.get("id") or "").strip()
        if not target:
            continue
        multi_targets.append(target)
        if str(peer.get("kind") or "").strip() == "direct" and not direct_target:
            direct_target = target
    return direct_target, list(dict.fromkeys(multi_targets))


def resolve_telegram_alert_target(explicit_target: str | None = None) -> str:
    binding_direct, _binding_targets = _binding_targets_from_openclaw()
    target = str(
        explicit_target
        or os.getenv("TELEGRAM_DIRECT_CHAT_ID")
        or os.getenv("TELEGRAM_ALERT_CHAT_ID")
        or os.getenv("TELEGRAM_CHAT_ID")
        or binding_direct
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

    binding_direct, binding_targets = _binding_targets_from_openclaw()
    if binding_targets:
        ordered = [binding_direct] if binding_direct else []
        ordered.extend(target for target in binding_targets if target and target != binding_direct)
        return list(dict.fromkeys(ordered))

    return [resolve_telegram_alert_target()]
