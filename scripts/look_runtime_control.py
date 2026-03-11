#!/usr/bin/env python3
"""Runtime controls for /look adaptive cooldown, inflight dedupe, and delivery markers."""

from __future__ import annotations

import hashlib
import json
import math
import os
import re
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from look_group_policy import LookGroupPolicy, UserCooldownPolicy, resolve_look_group_policy

STATE_DIR = Path(os.getenv("LOOK_RUNTIME_STATE_DIR") or "/tmp/polymarket-weatherbot-look-control")
PENDING_DELIVERY_DIR = STATE_DIR / "pending-deliveries"
REPORT_REF_DIR = STATE_DIR / "report-refs"
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
        group_id = _pick_first(env.get("OPENCLAW_GROUP_ID"), env.get("GROUP_ID"))
        current_channel_id = _pick_first(
            env.get("OPENCLAW_CURRENT_CHANNEL_ID"),
            env.get("OPENCLAW_CHAT_ID"),
            env.get("TELEGRAM_CHAT_ID"),
            env.get("CHAT_ID"),
            env.get("CHANNEL_ID"),
        )
        inferred_peer_from_channel = _parse_peerish_value(current_channel_id)
        inferred_kind_from_channel = _infer_peer_kind_from_value(current_channel_id)
        return cls(
            channel=_pick_first(
                channel,
                env.get("OPENCLAW_CHANNEL"),
                env.get("OPENCLAW_MESSAGE_PROVIDER"),
                env.get("CHANNEL"),
                inferred_channel,
            ),
            peer_kind=_pick_first(
                peer_kind,
                env.get("OPENCLAW_PEER_KIND"),
                "group" if group_id else None,
                inferred_kind_from_channel,
                env.get("PEER_KIND"),
                inferred_kind,
            ),
            peer_id=_pick_first(
                peer_id,
                env.get("OPENCLAW_PEER_ID"),
                group_id,
                inferred_peer_from_channel,
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


@dataclass(frozen=True)
class UserCooldownStatus:
    remaining_sec: int
    required_gap_sec: int
    recent_count: int
    mode: str


class LookRuntimeController:
    def __init__(self, *, context: LookRuntimeContext, compute_key: str, query_label: str | None = None) -> None:
        self.context = context
        self.compute_key = compute_key
        self.query_label = str(query_label or "").strip()
        self.now = time.time()
        self._claimed = False
        self.policy = resolve_look_group_policy(context.peer_id if context.is_group else None)
        STATE_DIR.mkdir(parents=True, exist_ok=True)
        PENDING_DELIVERY_DIR.mkdir(parents=True, exist_ok=True)
        REPORT_REF_DIR.mkdir(parents=True, exist_ok=True)

    def preflight(self) -> PreflightDecision:
        if not self._rate_limit_active():
            return PreflightDecision(True, None)

        inflight_result = self._wait_for_inflight()
        if inflight_result is not None:
            return PreflightDecision(False, inflight_result)

        cooldown = self._user_cooldown_status()
        if cooldown.remaining_sec > 0:
            return PreflightDecision(False, self._format_cooldown_block_message(cooldown))

        try:
            self._claim_inflight()
        except RuntimeError:
            inflight_result = self._wait_for_inflight()
            if inflight_result is not None:
                return PreflightDecision(False, inflight_result)
            return PreflightDecision(False, "⏳ 同一查询生成中，请稍后查看本群最近对应报告。")

        self._touch_user_state()
        return PreflightDecision(True, None)

    def success(self, text: str, *, result_meta: dict[str, Any] | None = None) -> None:
        updated_at = time.time()
        self._write_query_snapshot(text, result_meta=result_meta, updated_at=updated_at)
        self._mark_delivery_for_current_conversation(updated_at=updated_at)
        if self._rate_limit_active():
            self._write_scoped_result(text, result_meta=result_meta, updated_at=updated_at)
        self._write_pending_delivery(updated_at=updated_at)
        self._release_inflight()

    def failure(self) -> None:
        self._release_inflight()

    def _rate_limit_active(self) -> bool:
        if not self.policy.rate_limit.enabled:
            return False
        if self.context.is_group:
            return True
        return bool(self.policy.rate_limit.apply_in_direct)

    def _user_cooldown_status(self) -> UserCooldownStatus:
        cooldown_policy = self.policy.rate_limit.user_cooldown
        if not self.context.sender_id:
            return UserCooldownStatus(remaining_sec=0, required_gap_sec=0, recent_count=0, mode=cooldown_policy.mode)

        starts = self._load_recent_user_starts(cooldown_policy)
        if not starts:
            return UserCooldownStatus(remaining_sec=0, required_gap_sec=0, recent_count=0, mode=cooldown_policy.mode)

        last_started = starts[-1]
        elapsed_since_last = max(0.0, self.now - last_started)
        required_gap = self._required_user_gap_sec(cooldown_policy, recent_count=len(starts))
        if required_gap <= 0:
            return UserCooldownStatus(remaining_sec=0, required_gap_sec=0, recent_count=len(starts), mode=cooldown_policy.mode)
        remaining = int(math.ceil(required_gap - elapsed_since_last))
        return UserCooldownStatus(
            remaining_sec=remaining if remaining > 0 else 0,
            required_gap_sec=required_gap,
            recent_count=len(starts),
            mode=cooldown_policy.mode,
        )

    def _touch_user_state(self) -> None:
        if not self.context.sender_id:
            return
        cooldown_policy = self.policy.rate_limit.user_cooldown
        starts = self._load_recent_user_starts(cooldown_policy)
        starts.append(self.now)
        starts = self._prune_recent_user_starts(starts, cooldown_policy, now_ts=self.now)
        _write_json_atomic(
            _user_state_path(self._user_scope_key()),
            {
                "sender_scope": self._user_scope_key(),
                "last_started_at": self.now,
                "recent_started_at": starts[-12:],
                "compute_key": self.compute_key,
                "policy_id": self.policy.policy_id,
            },
        )

    def _wait_for_inflight(self) -> str | None:
        start = time.time()
        inflight_path = _inflight_path(self._result_scope_key())
        saw_inflight = False
        while True:
            payload = _read_json(inflight_path)
            if not payload:
                if not saw_inflight:
                    return None
                result_payload = self._read_scoped_result_payload()
                return self._deliver_or_notice_from_payload(
                    result_payload,
                    fallback_notice="♻️ 相同查询刚完成，请查看本群最近对应报告。",
                ) if result_payload else None
            saw_inflight = True
            started_at = _safe_float(payload.get("started_at"))
            if started_at is None or (time.time() - started_at) > self.policy.rate_limit.inflight_stale_sec:
                _unlink_if_exists(inflight_path)
                return None
            result_payload = self._read_scoped_result_payload()
            if result_payload:
                return self._deliver_or_notice_from_payload(
                    result_payload,
                    fallback_notice="♻️ 相同查询已在本轮完成，请查看本群最近对应报告。",
                )
            if (time.time() - start) >= self.policy.rate_limit.inflight_wait_sec:
                return "⏳ 同一查询生成中，请稍后查看本群最近对应报告。"
            time.sleep(POLL_INTERVAL_SECONDS)

    def _claim_inflight(self) -> None:
        path = _inflight_path(self._result_scope_key())
        payload = {
            "scope_key": self._result_scope_key(),
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

    def peek_cached_result_payload(self) -> dict[str, Any] | None:
        if self._rate_limit_active():
            payload = self._read_scoped_result_payload()
            if payload:
                return payload
            if self._delivery_scope_key():
                return None
        return self._read_query_snapshot_payload()

    def deliver_cached_or_notice(self, payload: dict[str, Any] | None, *, notice: str, fallback_text: str | None = None) -> str:
        normalized = self._normalize_payload(payload, fallback_text=fallback_text)
        if not normalized:
            return notice
        if self._current_chat_already_received_payload(normalized):
            return notice
        self._mark_delivery_for_current_chat(normalized)
        return str(normalized.get("text") or "")

    def deliver_unchanged_notice(self, payload: dict[str, Any] | None, *, notice: str, fallback_text: str | None = None) -> str:
        return self.deliver_cached_or_notice(payload, notice=notice, fallback_text=fallback_text)

    def should_emit_unchanged_notice(self, payload: dict[str, Any] | None, *, fallback_text: str | None = None) -> bool:
        normalized = self._normalize_payload(payload, fallback_text=fallback_text)
        if not normalized:
            return False
        return self._current_chat_already_received_payload(normalized)

    def _write_scoped_result(self, text: str, *, result_meta: dict[str, Any] | None = None, updated_at: float | None = None) -> None:
        stamp = float(updated_at if updated_at is not None else time.time())
        _write_json_atomic(
            _scoped_result_path(self._result_scope_key()),
            {
                "scope_key": self._result_scope_key(),
                "compute_key": self.compute_key,
                "updated_at": stamp,
                "text": text,
                "query_label": self.query_label,
                "result_meta": result_meta if isinstance(result_meta, dict) else {},
                "policy_id": self.policy.policy_id,
                "source_peer_id": self.context.peer_id,
            },
        )

    def _write_query_snapshot(self, text: str, *, result_meta: dict[str, Any] | None = None, updated_at: float | None = None) -> None:
        stamp = float(updated_at if updated_at is not None else time.time())
        _write_json_atomic(
            _query_snapshot_path(self.compute_key),
            {
                "compute_key": self.compute_key,
                "updated_at": stamp,
                "text": text,
                "query_label": self.query_label,
                "result_meta": result_meta if isinstance(result_meta, dict) else {},
                "policy_id": self.policy.policy_id,
                "source_peer_id": self.context.peer_id,
            },
        )

    def _write_pending_delivery(self, *, updated_at: float) -> None:
        peer_id = str(self.context.peer_id or "").strip()
        if str(self.context.channel or "").lower() != "telegram" or not peer_id:
            return
        payload = {
            "schema": "look-pending-delivery-v1",
            "channel": str(self.context.channel or "").strip().lower() or "telegram",
            "peer_id": peer_id,
            "compute_key": self.compute_key,
            "query_label": self.query_label,
            "updated_at": float(updated_at),
        }
        _write_json_atomic(_pending_delivery_path(peer_id, self.compute_key), payload)

    def _latest_report_message_id(self) -> str | None:
        payload = _read_json(_report_ref_path(str(self.context.peer_id or "").strip(), self.compute_key))
        if not payload:
            return None
        if str(payload.get("channel") or "").strip().lower() != "telegram":
            return None
        message_id = payload.get("report_message_id")
        if isinstance(message_id, int):
            return str(message_id)
        text = str(message_id or "").strip()
        return text or None

    def _read_scoped_result_payload(self) -> dict[str, Any] | None:
        payload = _read_json(_scoped_result_path(self._result_scope_key()))
        if not payload:
            return None
        text = str(payload.get("text") or "").strip()
        if not text:
            return None
        payload["text"] = text
        return payload

    def _read_query_snapshot_payload(self) -> dict[str, Any] | None:
        payload = _read_json(_query_snapshot_path(self.compute_key))
        if not payload:
            return None
        text = str(payload.get("text") or "").strip()
        if not text:
            return None
        payload["text"] = text
        return payload

    def _release_inflight(self) -> None:
        if self._claimed:
            _unlink_if_exists(_inflight_path(self._result_scope_key()))
            self._claimed = False

    def _current_chat_already_received_payload(self, payload: dict[str, Any]) -> bool:
        scope_key = self._delivery_scope_key()
        if not scope_key:
            return False
        if self.context.is_group:
            if self._has_pending_group_delivery(payload):
                return False
            if not self._has_confirmed_group_delivery(payload):
                return False
        marker = _read_json(_delivery_marker_path(scope_key, self.compute_key))
        if not marker:
            return False
        marker_updated_at = _safe_float(marker.get("payload_updated_at"))
        payload_updated_at = _safe_float(payload.get("updated_at"))
        if marker_updated_at is None or payload_updated_at is None:
            return False
        return abs(marker_updated_at - payload_updated_at) < 1e-6

    def _has_pending_group_delivery(self, payload: dict[str, Any]) -> bool:
        peer_id = str(self.context.peer_id or "").strip()
        if str(self.context.channel or "").strip().lower() != "telegram" or not peer_id:
            return False
        if self._has_confirmed_group_delivery(payload):
            return False
        pending = _read_json(_pending_delivery_path(peer_id, self.compute_key))
        if not pending:
            return False
        payload_updated_at = _safe_float(payload.get("updated_at"))
        pending_updated_at = _safe_float(pending.get("updated_at"))
        if payload_updated_at is None or pending_updated_at is None:
            return False
        return abs(pending_updated_at - payload_updated_at) < 1e-6

    def _has_confirmed_group_delivery(self, payload: dict[str, Any]) -> bool:
        peer_id = str(self.context.peer_id or "").strip()
        if str(self.context.channel or "").strip().lower() != "telegram" or not peer_id:
            return False
        report_ref = _read_json(_report_ref_path(peer_id, self.compute_key))
        if not report_ref:
            return False
        if str(report_ref.get("channel") or "").strip().lower() != "telegram":
            return False
        if not self._latest_report_message_id():
            return False
        payload_updated_at = _safe_float(payload.get("updated_at"))
        report_updated_at = _safe_float(report_ref.get("payload_updated_at"))
        if payload_updated_at is None or report_updated_at is None:
            return False
        return abs(report_updated_at - payload_updated_at) < 1e-6

    def _mark_delivery_for_current_chat(self, payload: dict[str, Any]) -> None:
        payload_updated_at = _safe_float(payload.get("updated_at"))
        if payload_updated_at is None:
            return
        self._mark_delivery_for_current_conversation(updated_at=payload_updated_at)

    def _mark_delivery_for_current_conversation(self, *, updated_at: float) -> None:
        scope_key = self._delivery_scope_key()
        if not scope_key:
            return
        _write_json_atomic(
            _delivery_marker_path(scope_key, self.compute_key),
            {
                "delivery_scope_key": scope_key,
                "channel": self.context.channel,
                "peer_kind": self.context.peer_kind,
                "peer_id": self.context.peer_id,
                "session_key": self.context.session_key,
                "compute_key": self.compute_key,
                "payload_updated_at": float(updated_at),
                "query_label": self.query_label,
            },
        )

    def _delivery_scope_key(self) -> str | None:
        session_key = str(self.context.session_key or "").strip()
        if session_key:
            return f"session:{session_key}"
        channel = str(self.context.channel or "").strip().lower()
        peer_kind = str(self.context.peer_kind or "").strip().lower()
        peer_id = str(self.context.peer_id or "").strip()
        if channel and peer_kind and peer_id:
            return f"peer:{channel}:{peer_kind}:{peer_id}"
        if channel and peer_kind:
            return f"peer-kind:{channel}:{peer_kind}"
        if channel and peer_id:
            return f"peerish:{channel}:{peer_id}"
        if peer_kind and peer_id:
            return f"peerish:{peer_kind}:{peer_id}"
        if peer_id:
            return f"peer:{peer_id}"
        if channel:
            return f"channel:{channel}"
        if peer_kind:
            return f"peer-kind:{peer_kind}"
        # Some live weatherbot invocations still arrive without explicit runtime
        # context. Fall back to a per-query delivery scope so repeated unchanged
        # requests do not keep re-posting the full report in the same conversation.
        return f"unknown:{self.compute_key}"

    def _result_scope_key(self) -> str:
        scope = self.policy.rate_limit.result_scope
        if scope == "group-only" and self.context.peer_id:
            return f"{RESULT_SCHEMA_VERSION}|group|{self.context.peer_id}|{self.compute_key}"
        return f"{RESULT_SCHEMA_VERSION}|telegram-groups-shared|{self.compute_key}"

    def _user_scope_key(self) -> str:
        scope = self.policy.rate_limit.user_cooldown.scope
        sender = str(self.context.sender_id or "").strip()
        if scope == "sender-per-group" and self.context.peer_id:
            return f"{sender}|{self.context.peer_id}"
        return sender

    def _format_cooldown_block_message(self, cooldown: UserCooldownStatus) -> str:
        if cooldown.mode == "adaptive":
            return (
                f"⏳ 请求过快，当前动态冷却剩余 {cooldown.remaining_sec} 秒。"
                f"当前作用域近窗内已触发 {cooldown.recent_count} 次 /look，当前冷却档位 {cooldown.required_gap_sec} 秒。"
            )
        return f"⏳ 请求过快，用户级冷却剩余 {cooldown.remaining_sec} 秒。请稍后再试。"

    def _load_recent_user_starts(self, cooldown_policy: UserCooldownPolicy) -> list[float]:
        payload = _read_json(_user_state_path(self._user_scope_key())) or {}
        history: list[float] = []
        raw_history = payload.get("recent_started_at")
        if isinstance(raw_history, list):
            for item in raw_history:
                ts = _safe_float(item)
                if ts is not None:
                    history.append(ts)
        if not history:
            last_started = _safe_float(payload.get("last_started_at"))
            if last_started is not None:
                history.append(last_started)
        return self._prune_recent_user_starts(history, cooldown_policy, now_ts=self.now)

    def _prune_recent_user_starts(
        self,
        starts: list[float],
        cooldown_policy: UserCooldownPolicy,
        *,
        now_ts: float,
    ) -> list[float]:
        window_sec = self._history_window_sec(cooldown_policy)
        if window_sec > 0:
            starts = [ts for ts in starts if ts >= 0 and (now_ts - ts) <= window_sec]
        starts.sort()
        return starts[-12:]

    def _history_window_sec(self, cooldown_policy: UserCooldownPolicy) -> int:
        if cooldown_policy.mode == "adaptive":
            return max(cooldown_policy.window_sec, cooldown_policy.max_sec, cooldown_policy.base_sec)
        return max(cooldown_policy.fixed_sec, 0)

    def _required_user_gap_sec(self, cooldown_policy: UserCooldownPolicy, *, recent_count: int) -> int:
        if cooldown_policy.mode == "fixed":
            return max(0, cooldown_policy.fixed_sec)
        max_sec = max(cooldown_policy.base_sec, cooldown_policy.max_sec)
        penalty_steps = max(0, recent_count - cooldown_policy.burst_soft_limit)
        required = cooldown_policy.base_sec + penalty_steps * cooldown_policy.step_sec
        return min(max_sec, max(0, required))

    def _format_cached_result_notice(self, payload: dict[str, Any] | None, *, fallback: str) -> str:
        label = str((payload or {}).get("query_label") or self.query_label or "").strip()
        if not label:
            return fallback
        return f"♻️ 已查询过 {label}，请查看本群同站同日最近报告。"

    def _deliver_or_notice_from_payload(self, payload: dict[str, Any] | None, *, fallback_notice: str) -> str:
        if not payload:
            return fallback_notice
        return self.deliver_cached_or_notice(
            payload,
            notice=self._format_cached_result_notice(payload, fallback=fallback_notice),
        )

    def _normalize_payload(self, payload: dict[str, Any] | None, *, fallback_text: str | None = None) -> dict[str, Any] | None:
        normalized = payload if isinstance(payload, dict) else None
        if not normalized:
            return None
        text = str(normalized.get("text") or fallback_text or "").strip()
        if not text:
            return None
        normalized["text"] = text
        return normalized


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


def _parse_peerish_value(value: str | None) -> str | None:
    text = str(value or "").strip()
    if not text:
        return None
    match = re.match(r"^(?:group|channel|direct|dm|peer|chat|telegram):(?P<peer>-?\d+)$", text, flags=re.IGNORECASE)
    if match:
        return match.group("peer")
    if re.fullmatch(r"-?\d+", text):
        return text
    return None


def _infer_peer_kind_from_value(value: str | None) -> str | None:
    text = str(value or "").strip().lower()
    if not text:
        return None
    if text.startswith(("group:", "channel:")):
        return "group"
    if text.startswith(("direct:", "dm:", "peer:", "chat:", "telegram:")):
        return "direct"
    return None


def _user_state_path(scope_key: str) -> Path:
    return STATE_DIR / f"user-{_short_hash(scope_key)}.json"


def _scoped_result_path(scope_key: str) -> Path:
    return STATE_DIR / f"scoped-result-{_short_hash(scope_key)}.json"


def _query_snapshot_path(compute_key: str) -> Path:
    return STATE_DIR / f"query-snapshot-{_short_hash(compute_key)}.json"


def _inflight_path(scope_key: str) -> Path:
    return STATE_DIR / f"inflight-{_short_hash(scope_key)}.json"


def _delivery_marker_path(scope_key: str, compute_key: str) -> Path:
    return STATE_DIR / f"delivery-{_short_hash(scope_key + '|' + compute_key)}.json"


def _pending_delivery_path(peer_id: str, compute_key: str) -> Path:
    return PENDING_DELIVERY_DIR / f"pending-{_short_hash(str(peer_id) + '|' + str(compute_key))}.json"


def _report_ref_path(peer_id: str, compute_key: str) -> Path:
    return REPORT_REF_DIR / f"report-ref-{_short_hash(str(peer_id) + '|' + str(compute_key))}.json"


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
