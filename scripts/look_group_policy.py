#!/usr/bin/env python3
"""Load per-group /look runtime policy with safe defaults."""

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parent.parent
CONFIG_PATH = ROOT / "config" / "look_group_policy.json"

DEFAULT_POLICY_DOC: dict[str, Any] = {
    "defaults": {
        "rate_limit": {
            "enabled": True,
            "apply_in_direct": False,
            "user_cooldown": {
                "mode": "adaptive",
                "scope": "sender-per-group",
                "fixed_sec": 60,
                "base_sec": 15,
                "step_sec": 15,
                "max_sec": 90,
                "window_sec": 180,
                "burst_soft_limit": 1,
            },
            "result_scope": "group-only",
            "inflight_wait_sec": 3,
            "inflight_stale_sec": 120,
        },
    },
    "groups": {},
}


@dataclass(frozen=True)
class UserCooldownPolicy:
    mode: str
    scope: str
    fixed_sec: int
    base_sec: int
    step_sec: int
    max_sec: int
    window_sec: int
    burst_soft_limit: int


@dataclass(frozen=True)
class RateLimitPolicy:
    enabled: bool
    apply_in_direct: bool
    user_cooldown: UserCooldownPolicy
    result_scope: str
    inflight_wait_sec: int
    inflight_stale_sec: int


@dataclass(frozen=True)
class LookGroupPolicy:
    policy_id: str
    rate_limit: RateLimitPolicy
    raw: dict[str, Any]


def resolve_look_group_policy(peer_id: str | None) -> LookGroupPolicy:
    doc = _load_policy_doc()
    defaults = dict(doc.get("defaults") or {})
    group_overrides = ((doc.get("groups") or {}).get(str(peer_id or "")) if peer_id else None) or {}
    merged = _deep_merge(defaults, group_overrides)
    rate_limit_raw = dict(merged.get("rate_limit") or {})
    user_cooldown_raw = _resolve_user_cooldown_raw(rate_limit_raw)
    return LookGroupPolicy(
        policy_id=str(peer_id or "defaults"),
        rate_limit=RateLimitPolicy(
            enabled=bool(rate_limit_raw.get("enabled", True)),
            apply_in_direct=bool(rate_limit_raw.get("apply_in_direct", False)),
            user_cooldown=UserCooldownPolicy(
                mode=_normalize_choice(
                    user_cooldown_raw.get("mode"),
                    allowed={"adaptive", "fixed"},
                    fallback="adaptive",
                ),
                scope=_normalize_choice(
                    user_cooldown_raw.get("scope"),
                    allowed={"sender-global", "sender-per-group"},
                    fallback="sender-per-group",
                ),
                fixed_sec=max(0, _to_int(user_cooldown_raw.get("fixed_sec"), 60)),
                base_sec=max(0, _to_int(user_cooldown_raw.get("base_sec"), 15)),
                step_sec=max(0, _to_int(user_cooldown_raw.get("step_sec"), 15)),
                max_sec=max(0, _to_int(user_cooldown_raw.get("max_sec"), 90)),
                window_sec=max(1, _to_int(user_cooldown_raw.get("window_sec"), 180)),
                burst_soft_limit=max(0, _to_int(user_cooldown_raw.get("burst_soft_limit"), 1)),
            ),
            result_scope=_normalize_choice(
                rate_limit_raw.get("result_scope"),
                allowed={"telegram-groups-shared", "group-only"},
                fallback="group-only",
            ),
            inflight_wait_sec=max(0, _to_int(rate_limit_raw.get("inflight_wait_sec"), 3)),
            inflight_stale_sec=max(1, _to_int(rate_limit_raw.get("inflight_stale_sec"), 120)),
        ),
        raw=merged,
    )


def _load_policy_doc() -> dict[str, Any]:
    try:
        with CONFIG_PATH.open("r", encoding="utf-8") as fh:
            loaded = json.load(fh)
        if isinstance(loaded, dict):
            return _deep_merge(DEFAULT_POLICY_DOC, loaded)
    except Exception:
        pass
    return dict(DEFAULT_POLICY_DOC)


def _resolve_user_cooldown_raw(rate_limit_raw: dict[str, Any]) -> dict[str, Any]:
    nested = dict(rate_limit_raw.get("user_cooldown") or {})
    legacy_scope = rate_limit_raw.get("user_cooldown_scope")
    legacy_fixed_sec = rate_limit_raw.get("user_cooldown_sec")

    if legacy_scope is not None:
        nested["scope"] = legacy_scope

    if legacy_fixed_sec is not None:
        nested["fixed_sec"] = legacy_fixed_sec
        nested["mode"] = "fixed"

    return nested


def _deep_merge(base: dict[str, Any], override: dict[str, Any]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key in set(base) | set(override):
        base_value = base.get(key)
        override_value = override.get(key)
        if isinstance(base_value, dict) and isinstance(override_value, dict):
            result[key] = _deep_merge(base_value, override_value)
        elif key in override:
            result[key] = override_value
        else:
            result[key] = base_value
    return result


def _to_int(value: Any, fallback: int) -> int:
    try:
        return int(value)
    except Exception:
        return fallback


def _normalize_choice(value: Any, *, allowed: set[str], fallback: str) -> str:
    text = str(value or "").strip().lower()
    return text if text in allowed else fallback
