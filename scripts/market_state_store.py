from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


def _to_float(value: Any) -> float | None:
    try:
        if value in (None, ""):
            return None
        return float(value)
    except Exception:
        return None


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
            state.update(
                {
                    "best_bid": best_bid,
                    "best_ask": best_ask,
                    "top_bid_size": _to_float(bids[0].get("size")) if bids and isinstance(bids[0], dict) else None,
                    "top_ask_size": _to_float(asks[0].get("size")) if asks and isinstance(asks[0], dict) else None,
                    "book_timestamp": payload.get("timestamp") or payload.get("timestampMs"),
                    "event_type": "book",
                }
            )
            return

        if event_type == "price_change":
            side = str(payload.get("side") or "").lower()
            price = _to_float(payload.get("price"))
            size = _to_float(payload.get("size"))
            state["event_type"] = "price_change"
            state["last_price_change"] = {
                "side": side,
                "price": price,
                "size": size,
                "timestamp": payload.get("timestamp") or payload.get("timestampMs"),
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
            return

        if event_type == "last_trade_price":
            state.update(
                {
                    "event_type": "last_trade_price",
                    "last_trade_price": _to_float(payload.get("price")),
                    "last_trade_side": payload.get("side"),
                    "last_trade_timestamp": payload.get("timestamp") or payload.get("timestampMs"),
                }
            )
            return

        state["last_message"] = payload

    def snapshot(self) -> dict[str, dict[str, Any]]:
        return {asset_id: dict(state) for asset_id, state in self.assets.items()}
