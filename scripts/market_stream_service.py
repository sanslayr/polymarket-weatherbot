from __future__ import annotations

import json
import time
from typing import Any

import websocket

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


def stream_market_state(
    *,
    asset_ids: list[str],
    duration_seconds: float = 8.0,
    ping_interval_seconds: float = 10.0,
    custom_feature_enabled: bool = True,
) -> dict[str, Any]:
    ws = websocket.create_connection(MARKET_WS_URL, timeout=5)
    store = MarketStateStore()
    messages: list[dict[str, Any]] = []
    started = time.time()
    last_ping = started

    subscribe_payload = {
        "type": "market",
        "assets_ids": [str(item) for item in asset_ids if str(item).strip()],
        "custom_feature_enabled": bool(custom_feature_enabled),
    }
    ws.send(json.dumps(subscribe_payload))

    try:
        while time.time() - started < float(duration_seconds):
            now = time.time()
            if now - last_ping >= float(ping_interval_seconds):
                ws.send("PING")
                last_ping = now
            try:
                raw = ws.recv()
            except websocket.WebSocketTimeoutException:
                continue
            for msg in _normalize_messages(raw):
                messages.append(msg)
                store.apply_message(msg)
    finally:
        try:
            ws.close()
        except Exception:
            pass

    return {
        "subscribed_asset_ids": list(subscribe_payload["assets_ids"]),
        "messages": messages,
        "state": store.snapshot(),
    }
