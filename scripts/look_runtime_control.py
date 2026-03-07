#!/usr/bin/env python3
"""Ephemeral runtime controls for /look rate limiting and dedupe."""

from __future__ import annotations

import hashlib
import json
import os
import re
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from look_group_policy import LookGroupPolicy, resolve_look_group_policy

STATE_DIR = Path(os.getenv("LOOK_RUNTIME_STATE_DIR") or "/tmp/polymarket-weatherbot-look-control")
POLL_INTERVAL_SECONDS = 0.5
RESULT_SCHEMA_VERSION = "look-runtime-v2"


@dataclass(frozen=True)
class LookRuntimeContext:
    channel: str | None = None
    peer_kind: str | None = None
    peer_id: str | None = None
    sender_id: str | None = None
    session_key: str | None = None

    @property
    def is_group(self) -> bool:
        return str(self.channel or "").lower() == "telegram" and str(self.peer_kind or "").lower() == "group"

    @classmethod
    def from_runtime(
        cls,
        *,
        channel: str | None = None,
        peer_kind: str | None = None,
        peer_id: str | None = None,
        sender_id: str | None = None,
        session_key: str | None = None,
    ) -> "LookRuntimeContext":
        env = os.environ
        session_key_value = _pick_first(
            session_key,
            env.get("OPENCLAW_SESSION_KEY"),
            env.get("CLAW_SESSION_KEY"),
            env.get("SESSION_KEY"),
        )
        inferred_channel, inferred_kind, inferred_peer = _parse_session_key(session_key_value)
        return cls(
            channel=_pick_first(channel, env.get("OPENCLAW_CHANNEL"), env.get("CHANNEL"), inferred_channel),
            peer_kind=_pick_first(peer_kind, env.get("OPENCLAW_PEER_KIND"), env.get("PEER_KIND"), inferred_kind),
            peer_id=_pick_first(
                peer_id,
                env.get("OPENCLAW_PEER_ID"),
                env.get("OPENCLAW_CHAT_ID"),
                env.get("TELEGRAM_CHAT_ID"),
                env.get("CHAT_ID"),
                inferred_peer,
            ),
            sender_id=_pick_first(
                sender_id,
                env.get("OPENCLAW_SENDER_ID"),
                env.get("REQUESTER_SENDER_ID"),
                env.get("TELEGRAM_SENDER_ID"),
                env.get("TELEGRAM_USER_ID"),
                env.get("SENDER_ID"),
                env.get("USER_ID"),
            ),
            session_key=session_key_value,
        )


@dataclass(frozen=True)
class PreflightDecision:
    proceed: bool
    text: str | None = None


class LookRuntimeController:
    def __init__(self, *, context: LookRuntimeContext, compute_key: str) -> None:
        self.context = context
        self.compute_key = compute_key
        self.now = time.time()
        self._claimed = False
        self.policy = resolve_look_group_policy(context.peer_id if context.is_group else None)
        STATE_DIR.mkdir(parents=True, exist_ok=True)

    def preflight(self) -> PreflightDecision:
        if not self._rate_limit_active():
            return PreflightDecision(True, None)

        shared_result = self._read_shared_result_text()
        if shared_result:
            return PreflightDecision(False, f"♻️ 复用 2 分钟内已生成的相同查询结果。\n\n{shared_result}")

        cooldown_left = self._user_cooldown_remaining()
        if cooldown_left > 0:
            return PreflightDecision(
                False,
                f"⏳ 请求过快，用户级冷却剩余 {cooldown_left} 秒。请稍后再试。",
            )

        inflight_result = self._wait_for_inflight()
        if inflight_result is not None:
            return PreflightDecision(False, inflight_result)

        try:
            self._claim_inflight()
        except RuntimeError:
            inflight_result = self._wait_for_inflight()
            if inflight_result is not None:
                return PreflightDecision(False, inflight_result)
            return PreflightDecision(False, "⏳ 同一查询正在生成中，请稍后查看上一条结果。")

        self._touch_user_state()
        return PreflightDecision(True, None)

    def success(self, text: str) -> None:
        if self._rate_limit_active():
            self._write_shared_result(text)
        self._release_inflight()

    def failure(self) -> None:
        self._release_inflight()

    def _rate_limit_active(self) -> bool:
        if not self.policy.rate_limit.enabled:
            return False
        if self.context.is_group:
            return True
        return bool(self.policy.rate_limit.apply_in_direct)

    def _user_cooldown_remaining(self) -> int:
        if not self.context.sender_id or self.policy.rate_limit.user_cooldown_sec <= 0:
            return 0
        payload = _read_json(_user_state_path(self._user_scope_key()))
        if not payload:
            return 0
        last_started = _safe_float(payload.get("last_started_at"))
        if last_started is None:
            return 0
        remaining = int(self.policy.rate_limit.user_cooldown_sec - (self.now - last_started))
        return remaining if remaining > 0 else 0

    def _touch_user_state(self) -> None:
        if not self.context.sender_id:
            return
        _write_json_atomic(
            _user_state_path(self._user_scope_key()),
            {
                "sender_scope": self._user_scope_key(),
                "last_started_at": self.now,
                "compute_key": self.compute_key,
                "policy_id": self.policy.policy_id,
            },
        )

    def _wait_for_inflight(self) -> str | None:
        start = time.time()
        inflight_path = _inflight_path(self._shared_scope_key())
        while True:
            payload = _read_json(inflight_path)
            if not payload:
                result_text = self._read_shared_result_text()
                return f"♻️ 复用刚完成的相同查询结果。\n\n{result_text}" if result_text else None
            started_at = _safe_float(payload.get("started_at"))
            if started_at is None or (time.time() - started_at) > self.policy.rate_limit.inflight_stale_sec:
                _unlink_if_exists(inflight_path)
                return None
            result_text = self._read_shared_result_text()
            if result_text:
                return f"♻️ 复用正在生成完成的相同查询结果。\n\n{result_text}"
            if (time.time() - start) >= self.policy.rate_limit.inflight_wait_sec:
                return "⏳ 同一查询正在生成中，请稍后查看上一条结果。"
            time.sleep(POLL_INTERVAL_SECONDS)

    def _claim_inflight(self) -> None:
        path = _inflight_path(self._shared_scope_key())
        payload = {
            "scope_key": self._shared_scope_key(),
            "compute_key": self.compute_key,
            "started_at": self.now,
            "pid": os.getpid(),
            "policy_id": self.policy.policy_id,
        }
        try:
            fd = os.open(path, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o644)
        except FileExistsError:
            raise RuntimeError("inflight request already exists")
        try:
            os.write(fd, json.dumps(payload, ensure_ascii=True).encode("utf-8"))
        finally:
            os.close(fd)
        self._claimed = True

    def _write_shared_result(self, text: str) -> None:
        ttl_seconds = self.policy.rate_limit.shared_result_ttl_sec
        if ttl_seconds <= 0:
            return
        _write_json_atomic(
            _shared_result_path(self._shared_scope_key()),
            {
                "scope_key": self._shared_scope_key(),
                "compute_key": self.compute_key,
                "expires_at": time.time() + ttl_seconds,
                "text": text,
                "policy_id": self.policy.policy_id,
                "source_peer_id": self.context.peer_id,
            },
        )

    def _read_shared_result_text(self) -> str | None:
        payload = _read_json(_shared_result_path(self._shared_scope_key()))
        if not payload:
            return None
        expires_at = _safe_float(payload.get("expires_at"))
        if expires_at is None or expires_at < time.time():
            return None
        text = str(payload.get("text") or "").strip()
        return text or None

    def _release_inflight(self) -> None:
        if self._claimed:
            _unlink_if_exists(_inflight_path(self._shared_scope_key()))
            self._claimed = False

    def _shared_scope_key(self) -> str:
        scope = self.policy.rate_limit.shared_result_scope
        if scope == "group-only" and self.context.peer_id:
            return f"{RESULT_SCHEMA_VERSION}|group|{self.context.peer_id}|{self.compute_key}"
        return f"{RESULT_SCHEMA_VERSION}|telegram-groups-shared|{self.compute_key}"

    def _user_scope_key(self) -> str:
        scope = self.policy.rate_limit.user_cooldown_scope
        sender = str(self.context.sender_id or "").strip()
        if scope == "sender-per-group" and self.context.peer_id:
            return f"{sender}|{self.context.peer_id}"
        return sender


def build_request_key(*, station_icao: str, target_date: str, command_name: str = "look") -> str:
    normalized = f"{RESULT_SCHEMA_VERSION}|{command_name.lower()}|{station_icao.upper()}|{target_date}"
    return hashlib.sha256(normalized.encode("utf-8")).hexdigest()


def _pick_first(*values: Any) -> str | None:
    for value in values:
        text = str(value or "").strip()
        if text:
            return text
    return None


def _parse_session_key(session_key: str | None) -> tuple[str | None, str | None, str | None]:
    text = str(session_key or "").strip()
    if not text:
        return None, None, None
    match = re.match(r"^agent:[^:]+:(?P<channel>[^:]+):(?P<kind>direct|group|channel):(?P<peer>.+)$", text)
    if not match:
        return None, None, None
    return match.group("channel"), match.group("kind"), match.group("peer")


def _user_state_path(scope_key: str) -> Path:
    return STATE_DIR / f"user-{_short_hash(scope_key)}.json"


def _shared_result_path(scope_key: str) -> Path:
    return STATE_DIR / f"shared-result-{_short_hash(scope_key)}.json"


def _inflight_path(scope_key: str) -> Path:
    return STATE_DIR / f"inflight-{_short_hash(scope_key)}.json"


def _short_hash(value: str) -> str:
    return hashlib.sha256(str(value).encode("utf-8")).hexdigest()[:24]


def _read_json(path: Path) -> dict[str, Any] | None:
    try:
        with path.open("r", encoding="utf-8") as fh:
            return json.load(fh)
    except Exception:
        return None


def _write_json_atomic(path: Path, payload: dict[str, Any]) -> None:
    tmp = path.with_suffix(path.suffix + f".tmp-{os.getpid()}")
    with tmp.open("w", encoding="utf-8") as fh:
        json.dump(payload, fh, ensure_ascii=True)
    os.replace(tmp, path)


def _unlink_if_exists(path: Path) -> None:
    try:
        path.unlink()
    except FileNotFoundError:
        return
    except Exception:
        return


def _safe_float(value: Any) -> float | None:
    try:
        if value in (None, ""):
            return None
        return float(value)
    except Exception:
        return None
