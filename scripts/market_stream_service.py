from __future__ import annotations

import importlib
import json
import time
from typing import Any

from market_state_store import MarketStateStore


MARKET_WS_URL = "wss://ws-subscriptions-clob.polymarket.com/ws/market"


def _normalize_messages(raw_message: str) -> list[dict[str, Any]]:
    try:
        payload = json.loads(raw_message)
    except Exception:
        return []
    if isinstance(payload, list):
        return [item for item in payload if isinstance(item, dict)]
    if isinstance(payload, dict):
        return [payload]
    return []


def _websocket_module():
    try:
        return importlib.import_module("websocket")
    except ModuleNotFoundError as exc:
        raise RuntimeError("Missing websocket-client package for market stream runtime") from exc


def _close_ws(ws: Any) -> None:
    if ws is None:
        return
    try:
        ws.close()
    except Exception:
        pass


def stream_market_state(
    *,
    asset_ids: list[str],
    duration_seconds: float = 8.0,
    ping_interval_seconds: float = 10.0,
    custom_feature_enabled: bool = True,
    reconnect_delay_seconds: float = 0.75,
) -> dict[str, Any]:
    websocket = _websocket_module()
    store = MarketStateStore()
    messages: list[dict[str, Any]] = []
    started = time.time()
    deadline = started + float(duration_seconds)
    last_ping = started
    ws = None

    subscribe_payload = {
        "type": "market",
        "assets_ids": [str(item) for item in asset_ids if str(item).strip()],
        "custom_feature_enabled": bool(custom_feature_enabled),
    }

    try:
        while time.time() < deadline:
            if ws is None:
                try:
                    ws = websocket.create_connection(MARKET_WS_URL, timeout=5)
                    ws.send(json.dumps(subscribe_payload))
                    last_ping = time.time()
                except Exception:
                    _close_ws(ws)
                    ws = None
                    if time.time() >= deadline:
                        break
                    time.sleep(min(float(reconnect_delay_seconds), max(0.1, deadline - time.time())))
                    continue
            now = time.time()
            if now - last_ping >= float(ping_interval_seconds):
                try:
                    ws.send("PING")
                except Exception:
                    _close_ws(ws)
                    ws = None
                    continue
                last_ping = now
            try:
                raw = ws.recv()
            except websocket.WebSocketTimeoutException:
                continue
            except Exception:
                _close_ws(ws)
                ws = None
                continue
            for msg in _normalize_messages(raw):
                messages.append(msg)
                store.apply_message(msg)
    finally:
        _close_ws(ws)

    return {
        "subscribed_asset_ids": list(subscribe_payload["assets_ids"]),
        "messages": messages,
        "state": store.snapshot(),
    }


def monitor_market_state(
    *,
    asset_ids: list[str],
    duration_seconds: float = 120.0,
    baseline_seconds: float = 2.0,
    ping_interval_seconds: float = 10.0,
    custom_feature_enabled: bool = True,
    on_update: Any | None = None,
    reconnect_delay_seconds: float = 0.75,
) -> dict[str, Any]:
    websocket = _websocket_module()
    store = MarketStateStore()
    messages: list[dict[str, Any]] = []
    started = time.time()
    deadline = started + float(duration_seconds)
    last_ping = started
    baseline_state: dict[str, dict[str, Any]] | None = None
    triggered_payload: dict[str, Any] | None = None
    ws = None

    subscribe_payload = {
        "type": "market",
        "assets_ids": [str(item) for item in asset_ids if str(item).strip()],
        "custom_feature_enabled": bool(custom_feature_enabled),
    }

    try:
        while time.time() < deadline:
            if ws is None:
                try:
                    ws = websocket.create_connection(MARKET_WS_URL, timeout=1.0)
                    ws.send(json.dumps(subscribe_payload))
                    last_ping = time.time()
                except Exception:
                    _close_ws(ws)
                    ws = None
                    if time.time() >= deadline:
                        break
                    time.sleep(min(float(reconnect_delay_seconds), max(0.1, deadline - time.time())))
                    continue
            now = time.time()
            if now - last_ping >= float(ping_interval_seconds):
                try:
                    ws.send("PING")
                except Exception:
                    _close_ws(ws)
                    ws = None
                    continue
                last_ping = now
            try:
                raw = ws.recv()
            except websocket.WebSocketTimeoutException:
                raw = None
            except Exception:
                _close_ws(ws)
                ws = None
                continue
            if raw is not None:
                for msg in _normalize_messages(raw):
                    messages.append(msg)
                    store.apply_message(msg)
            elapsed = time.time() - started
            if baseline_state is None and elapsed >= float(baseline_seconds):
                baseline_state = store.snapshot()
            if baseline_state is not None and on_update is not None:
                observed_at_utc = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
                maybe_payload = on_update(
                    {
                        "baseline_state": baseline_state,
                        "current_state": store.snapshot(),
                        "messages": messages,
                        "observed_at_utc": observed_at_utc,
                        "elapsed_seconds": elapsed,
                    }
                )
                if maybe_payload:
                    triggered_payload = dict(maybe_payload)
                    break
    finally:
        _close_ws(ws)

    return {
        "subscribed_asset_ids": list(subscribe_payload["assets_ids"]),
        "messages": messages,
        "state": store.snapshot(),
        "baseline_state": baseline_state or {},
        "triggered_payload": triggered_payload,
    }
