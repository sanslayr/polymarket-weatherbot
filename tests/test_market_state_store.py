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
                "bids": [{"price": "0.03", "size": "120"}, {"price": "0.02", "size": "80"}],
                "asks": [{"price": "0.05", "size": "90"}, {"price": "0.06", "size": "70"}],
                "timestamp": "2026-03-09T09:30:00Z",
            }
        )
        snap = store.snapshot()["abc"]
        self.assertEqual(snap["best_bid"], 0.03)
        self.assertEqual(snap["best_ask"], 0.05)
        self.assertEqual(snap["bid_depth_3"], 200.0)
        self.assertEqual(snap["ask_depth_3"], 160.0)
        self.assertEqual(snap["mid_price"], 0.04)
        self.assertEqual(snap["spread"], 0.02)

    def test_apply_book_message_sorts_bids_desc_and_asks_asc(self) -> None:
        store = MarketStateStore()
        store.apply_message(
            {
                "event_type": "book",
                "asset_id": "abc",
                "bids": [{"price": "0.001", "size": "10"}, {"price": "0.002", "size": "20"}],
                "asks": [{"price": "0.999", "size": "30"}, {"price": "0.005", "size": "40"}],
                "timestamp": "2026-03-10T22:20:30Z",
            }
        )
        snap = store.snapshot()["abc"]
        self.assertEqual(snap["best_bid"], 0.002)
        self.assertEqual(snap["best_ask"], 0.005)
        self.assertEqual(snap["top_bid_size"], 20.0)
        self.assertEqual(snap["top_ask_size"], 40.0)
        self.assertEqual(snap["top_bid_levels"][0]["price"], 0.002)
        self.assertEqual(snap["top_ask_levels"][0]["price"], 0.005)

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

    def test_last_trade_price_tracks_trade_counts_and_volume(self) -> None:
        store = MarketStateStore()
        store.apply_message(
            {
                "event_type": "last_trade_price",
                "asset_id": "abc",
                "price": "0.04",
                "size": "15",
                "side": "buy",
                "timestamp": "2026-03-09T09:30:00Z",
            }
        )
        store.apply_message(
            {
                "event_type": "last_trade_price",
                "asset_id": "abc",
                "price": "0.03",
                "size": "5",
                "side": "sell",
                "timestamp": "2026-03-09T09:31:00Z",
            }
        )
        snap = store.snapshot()["abc"]
        self.assertEqual(snap["trade_count_3m"], 2)
        self.assertEqual(snap["trade_volume_3m"], 20.0)
        self.assertEqual(snap["last_trade_price"], 0.03)


if __name__ == "__main__":
    unittest.main()
