import sys
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
SCRIPTS = ROOT / "scripts"
if str(SCRIPTS) not in sys.path:
    sys.path.insert(0, str(SCRIPTS))

from market_subscription_policy import build_market_subscription_plan  # noqa: E402


class MarketSubscriptionPolicyCoreOnlyTest(unittest.TestCase):
    def test_core_only_drops_upside_scan(self) -> None:
        plan = build_market_subscription_plan(
            market_catalog_snapshot={
                "markets": [
                    {"bucket_kind": "exact", "threshold_c": 7, "yes_token_id": "a", "active": True, "closed": False},
                    {"bucket_kind": "exact", "threshold_c": 8, "yes_token_id": "b", "active": True, "closed": False},
                ]
            },
            observed_max_temp_c=7,
            report_window_active=True,
            daily_peak_state="open",
            core_only=True,
        )
        self.assertEqual(plan["monitor_mode"], "core")
        self.assertEqual(plan["core_watch_asset_ids"], ["a"])
        self.assertEqual(plan["upside_scan_asset_ids"], [])


if __name__ == "__main__":
    unittest.main()
