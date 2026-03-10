import sys
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
SCRIPTS = ROOT / "scripts"
if str(SCRIPTS) not in sys.path:
    sys.path.insert(0, str(SCRIPTS))

from market_monitor_service import build_bucket_snapshots  # noqa: E402


class MarketMonitorServiceTest(unittest.TestCase):
    def test_build_bucket_snapshots_carries_prev_and_current_book(self) -> None:
        snapshots = build_bucket_snapshots(
            market_catalog_snapshot={
                "markets": [
                    {
                        "bucket_label": "6°C",
                        "bucket_kind": "exact",
                        "temperature_unit": "C",
                        "threshold_native": 6,
                        "threshold_c": 6,
                        "lower_bound_c": 5.5,
                        "upper_bound_c": 6.49,
                        "yes_token_id": "a",
                    }
                ]
            },
            current_state={"a": {"best_bid": 0.01, "best_ask": 0.03, "trade_count_3m": 2}},
            previous_state={"a": {"best_bid": 0.05, "best_ask": 0.06}},
        )
        self.assertEqual(len(snapshots), 1)
        self.assertEqual(snapshots[0]["prev_best_bid"], 0.05)
        self.assertEqual(snapshots[0]["best_ask"], 0.03)
        self.assertEqual(snapshots[0]["temperature_unit"], "C")
        self.assertEqual(snapshots[0]["trade_count_3m"], 2)


if __name__ == "__main__":
    unittest.main()
