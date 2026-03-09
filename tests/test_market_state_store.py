import sys
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
SCRIPTS = ROOT / "scripts"
if str(SCRIPTS) not in sys.path:
    sys.path.insert(0, str(SCRIPTS))

from market_state_store import MarketStateStore  # noqa: E402


class MarketStateStoreTest(unittest.TestCase):
    def test_apply_book_message_updates_top_of_book(self) -> None:
        store = MarketStateStore()
        store.apply_message(
            {
                "event_type": "book",
                "asset_id": "abc",
                "bids": [{"price": "0.03", "size": "120"}],
                "asks": [{"price": "0.05", "size": "90"}],
                "timestamp": "2026-03-09T09:30:00Z",
            }
        )
        snap = store.snapshot()["abc"]
        self.assertEqual(snap["best_bid"], 0.03)
        self.assertEqual(snap["best_ask"], 0.05)

    def test_apply_price_change_can_clear_best_bid(self) -> None:
        store = MarketStateStore()
        store.apply_message(
            {
                "event_type": "book",
                "asset_id": "abc",
                "bids": [{"price": "0.03", "size": "120"}],
                "asks": [{"price": "0.05", "size": "90"}],
            }
        )
        store.apply_message(
            {
                "event_type": "price_change",
                "asset_id": "abc",
                "side": "buy",
                "price": "0.03",
                "size": 0,
            }
        )
        snap = store.snapshot()["abc"]
        self.assertIsNone(snap["best_bid"])

    def test_apply_batched_price_changes_updates_best_levels(self) -> None:
        store = MarketStateStore()
        store.apply_message(
            {
                "event_type": "price_change",
                "timestamp": "2026-03-09T09:30:00Z",
                "price_changes": [
                    {
                        "asset_id": "abc",
                        "side": "SELL",
                        "price": "0.95",
                        "size": "20000",
                        "best_bid": "0",
                        "best_ask": "0.001",
                    }
                ],
            }
        )
        snap = store.snapshot()["abc"]
        self.assertIsNone(snap["best_bid"])
        self.assertEqual(snap["best_ask"], 0.001)


if __name__ == "__main__":
    unittest.main()
