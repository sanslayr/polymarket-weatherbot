from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any


def _to_float(value: Any) -> float | None:
    try:
        if value in (None, ""):
            return None
        return float(value)
    except Exception:
        return None


def _to_timestamp_seconds(value: Any) -> float | None:
    if value in (None, ""):
        return None
    try:
        if isinstance(value, (int, float)):
            ts = float(value)
            return ts / 1000.0 if ts > 10_000_000_000 else ts
        text = str(value).strip()
        if not text:
            return None
        if text.isdigit():
            ts = float(text)
            return ts / 1000.0 if ts > 10_000_000_000 else ts
        dt = datetime.fromisoformat(text.replace("Z", "+00:00"))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc).timestamp()
    except Exception:
        return None


def _trim_recent(items: list[dict[str, Any]], *, now_ts: float | None, window_seconds: float = 180.0, max_items: int = 128) -> list[dict[str, Any]]:
    if not items:
        return []
    if now_ts is None:
        return items[-max_items:]
    cutoff = float(now_ts) - float(window_seconds)
    trimmed = [item for item in items if _to_timestamp_seconds(item.get("timestamp")) is not None and float(_to_timestamp_seconds(item.get("timestamp"))) >= cutoff]
    return trimmed[-max_items:]


def _level_rows(levels: list[Any], limit: int = 3) -> list[dict[str, float]]:
    out: list[dict[str, float]] = []
    for item in levels[:limit]:
        if not isinstance(item, dict):
            continue
        price = _to_float(item.get("price"))
        size = _to_float(item.get("size"))
        if price is None:
            continue
        out.append({"price": price, "size": size or 0.0})
    return out


def _sum_level_size(levels: list[dict[str, float]]) -> float | None:
    if not levels:
        return None
    return float(sum(float(item.get("size") or 0.0) for item in levels))


def _recompute_book_metrics(state: dict[str, Any]) -> None:
    best_bid = _to_float(state.get("best_bid"))
    best_ask = _to_float(state.get("best_ask"))
    if best_bid is not None and best_ask is not None:
        state["mid_price"] = round((best_bid + best_ask) / 2.0, 6)
        state["spread"] = round(best_ask - best_bid, 6)
        state["spread_bps"] = round((best_ask - best_bid) * 10000.0, 3)
    else:
        state["mid_price"] = None
        state["spread"] = None
        state["spread_bps"] = None
    top_bid_size = _to_float(state.get("top_bid_size"))
    top_ask_size = _to_float(state.get("top_ask_size"))
    if top_bid_size is not None and top_ask_size is not None and (top_bid_size + top_ask_size) > 0:
        state["book_imbalance"] = round(top_bid_size / (top_bid_size + top_ask_size), 4)
    else:
        state["book_imbalance"] = None


def _record_trade(state: dict[str, Any], *, price: Any, size: Any, timestamp: Any, side: Any = None) -> None:
    ts = _to_timestamp_seconds(timestamp)
    if ts is None:
        return
    trades = list(state.get("_recent_trades") or [])
    trades.append(
        {
            "timestamp": ts,
            "price": _to_float(price),
            "size": _to_float(size),
            "side": str(side or "").strip().lower() or None,
        }
    )
    trades = _trim_recent(trades, now_ts=ts)
    state["_recent_trades"] = trades
    state["trade_count_3m"] = len(trades)
    state["trade_volume_3m"] = round(sum(float(item.get("size") or 0.0) for item in trades), 6)


def _record_update(state: dict[str, Any], *, timestamp: Any) -> None:
    ts = _to_timestamp_seconds(timestamp)
    if ts is None:
        return
    updates = list(state.get("_recent_updates") or [])
    updates.append({"timestamp": ts})
    updates = _trim_recent(updates, now_ts=ts)
    state["_recent_updates"] = updates
    state["book_update_count_3m"] = len(updates)
    state["last_update_timestamp"] = ts


@dataclass
class MarketStateStore:
    assets: dict[str, dict[str, Any]] = field(default_factory=dict)

    def apply_message(self, payload: dict[str, Any]) -> None:
        if not isinstance(payload, dict):
            return
        if isinstance(payload.get("price_changes"), list):
            for item in payload.get("price_changes") or []:
                if isinstance(item, dict):
                    self.apply_message(
                        {
                            **item,
                            "event_type": "price_change",
                            "timestamp": payload.get("timestamp") or payload.get("timestampMs"),
                        }
                    )
            return
        event_type = str(payload.get("event_type") or payload.get("type") or "").strip().lower()
        asset_id = str(payload.get("asset_id") or payload.get("assetId") or "").strip()
        if not asset_id:
            return
        state = self.assets.setdefault(asset_id, {"asset_id": asset_id})

        if event_type == "book":
            bids = payload.get("bids") if isinstance(payload.get("bids"), list) else []
            asks = payload.get("asks") if isinstance(payload.get("asks"), list) else []
            best_bid = _to_float(bids[0].get("price")) if bids and isinstance(bids[0], dict) else None
            best_ask = _to_float(asks[0].get("price")) if asks and isinstance(asks[0], dict) else None
            top_bid_levels = _level_rows(bids)
            top_ask_levels = _level_rows(asks)
            book_timestamp = payload.get("timestamp") or payload.get("timestampMs")
            state.update(
                {
                    "best_bid": best_bid,
                    "best_ask": best_ask,
                    "top_bid_size": _to_float(bids[0].get("size")) if bids and isinstance(bids[0], dict) else None,
                    "top_ask_size": _to_float(asks[0].get("size")) if asks and isinstance(asks[0], dict) else None,
                    "top_bid_levels": top_bid_levels,
                    "top_ask_levels": top_ask_levels,
                    "bid_depth_3": _sum_level_size(top_bid_levels),
                    "ask_depth_3": _sum_level_size(top_ask_levels),
                    "book_timestamp": book_timestamp,
                    "event_type": "book",
                }
            )
            _record_update(state, timestamp=book_timestamp)
            _recompute_book_metrics(state)
            return

        if event_type == "price_change":
            side = str(payload.get("side") or "").lower()
            price = _to_float(payload.get("price"))
            size = _to_float(payload.get("size"))
            timestamp = payload.get("timestamp") or payload.get("timestampMs")
            state["event_type"] = "price_change"
            state["last_price_change"] = {
                "side": side,
                "price": price,
                "size": size,
                "timestamp": timestamp,
            }
            best_bid = _to_float(payload.get("best_bid"))
            best_ask = _to_float(payload.get("best_ask"))
            if best_bid is not None:
                state["best_bid"] = best_bid if best_bid > 0 else None
            if best_ask is not None:
                state["best_ask"] = best_ask if best_ask > 0 else None
            if side == "buy" and size == 0 and state.get("best_bid") == price:
                state["best_bid"] = None
            if side == "sell" and size == 0 and state.get("best_ask") == price:
                state["best_ask"] = None
            _record_update(state, timestamp=timestamp)
            _recompute_book_metrics(state)
            return

        if event_type == "last_trade_price":
            timestamp = payload.get("timestamp") or payload.get("timestampMs")
            state.update(
                {
                    "event_type": "last_trade_price",
                    "last_trade_price": _to_float(payload.get("price")),
                    "last_trade_side": payload.get("side"),
                    "last_trade_timestamp": timestamp,
                }
            )
            _record_trade(
                state,
                price=payload.get("price"),
                size=payload.get("size") or payload.get("amount") or payload.get("quantity"),
                timestamp=timestamp,
                side=payload.get("side"),
            )
            _record_update(state, timestamp=timestamp)
            return

        state["last_message"] = payload

    def snapshot(self) -> dict[str, dict[str, Any]]:
        now_ts = datetime.now(timezone.utc).timestamp()
        snapshot: dict[str, dict[str, Any]] = {}
        for asset_id, raw_state in self.assets.items():
            state = {key: value for key, value in dict(raw_state).items() if not str(key).startswith("_")}
            latest_ts = max(
                [
                    ts
                    for ts in (
                        _to_timestamp_seconds(state.get("book_timestamp")),
                        _to_timestamp_seconds(state.get("last_trade_timestamp")),
                        _to_timestamp_seconds(state.get("last_update_timestamp")),
                    )
                    if ts is not None
                ],
                default=None,
            )
            state["staleness_ms"] = int(max(0.0, now_ts - latest_ts) * 1000.0) if latest_ts is not None else None
            _recompute_book_metrics(state)
            snapshot[asset_id] = state
        return snapshot
