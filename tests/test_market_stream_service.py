import json
import sys
import unittest
from pathlib import Path
from unittest.mock import patch


ROOT = Path(__file__).resolve().parents[1]
SCRIPTS = ROOT / "scripts"
if str(SCRIPTS) not in sys.path:
    sys.path.insert(0, str(SCRIPTS))

from market_stream_service import monitor_market_state  # noqa: E402


class _FakeWebSocketTimeout(Exception):
    pass


class _FakeConnection:
    def __init__(self, responses: list[str]) -> None:
        self._responses = list(responses)
        self.sent_payloads: list[str] = []
        self.closed = False

    def send(self, payload: str) -> None:
        self.sent_payloads.append(payload)

    def recv(self) -> str:
        if not self._responses:
            raise _FakeWebSocketTimeout()
        return self._responses.pop(0)

    def close(self) -> None:
        self.closed = True


class _FakeWebSocketModule:
    WebSocketTimeoutException = _FakeWebSocketTimeout

    def __init__(self, responses: list[str]) -> None:
        self._connection = _FakeConnection(responses)

    def create_connection(self, *_args, **_kwargs) -> _FakeConnection:
        return self._connection


class MarketStreamServiceTest(unittest.TestCase):
    def test_monitor_market_state_evaluates_each_price_change_in_batch(self) -> None:
        fake_ws = _FakeWebSocketModule(
            [
                json.dumps(
                    {
                        "event_type": "book",
                        "asset_id": "a",
                        "bids": [{"price": "0.38", "size": "5"}],
                        "asks": [{"price": "0.52", "size": "5"}],
                        "timestamp": "2026-03-13T19:00:00Z",
                    }
                ),
                json.dumps(
                    {
                        "timestamp": "2026-03-13T19:00:01Z",
                        "price_changes": [
                            {
                                "asset_id": "a",
                                "side": "BUY",
                                "price": "0.38",
                                "size": 0,
                                "best_bid": "0",
                                "best_ask": "0.001",
                            },
                            {
                                "asset_id": "a",
                                "side": "BUY",
                                "price": "0.37",
                                "size": 5,
                                "best_bid": "0.37",
                                "best_ask": "0.50",
                            },
                        ],
                    }
                ),
            ]
        )

        def on_update(payload: dict[str, object]) -> dict[str, str] | None:
            current_state = dict(payload.get("current_state") or {})
            bucket = dict(current_state.get("a") or {})
            if bucket.get("best_bid") is None and bucket.get("best_ask") == 0.001:
                return {"trigger": "hit"}
            return None

        with patch("market_stream_service._websocket_module", return_value=fake_ws):
            result = monitor_market_state(
                asset_ids=["a"],
                duration_seconds=0.2,
                baseline_seconds=0.0,
                on_update=on_update,
            )

        self.assertEqual(result["triggered_payload"], {"trigger": "hit"})
        self.assertEqual(result["state"]["a"]["best_bid"], None)
        self.assertEqual(result["state"]["a"]["best_ask"], 0.001)
        self.assertEqual(len(result["messages"]), 2)


if __name__ == "__main__":
    unittest.main()
