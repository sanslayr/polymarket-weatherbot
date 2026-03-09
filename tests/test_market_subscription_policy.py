import sys
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
SCRIPTS = ROOT / "scripts"
if str(SCRIPTS) not in sys.path:
    sys.path.insert(0, str(SCRIPTS))

from market_subscription_policy import build_market_subscription_plan  # noqa: E402


class MarketSubscriptionPolicyTest(unittest.TestCase):
    def test_builds_core_and_upside_plan(self) -> None:
        plan = build_market_subscription_plan(
            market_catalog_snapshot={
                "markets": [
                    {"bucket_kind": "exact", "threshold_c": 6, "yes_token_id": "a", "active": True, "closed": False},
                    {"bucket_kind": "exact", "threshold_c": 7, "yes_token_id": "b", "active": True, "closed": False},
                    {"bucket_kind": "exact", "threshold_c": 8, "yes_token_id": "c", "active": True, "closed": False},
                    {"bucket_kind": "at_or_above", "threshold_c": 9, "yes_token_id": "d", "active": True, "closed": False},
                ]
            },
            observed_max_temp_c=6.0,
            report_window_active=False,
        )

        self.assertEqual(plan["monitor_mode"], "core_plus_upside")
        self.assertEqual(plan["core_watch_asset_ids"], ["a"])
        self.assertEqual(plan["upside_scan_asset_ids"], ["b", "c", "d"])

    def test_idles_when_top_or_higher_already_reached(self) -> None:
        plan = build_market_subscription_plan(
            market_catalog_snapshot={
                "markets": [
                    {"bucket_kind": "exact", "threshold_c": 10, "yes_token_id": "a", "active": True, "closed": False},
                    {"bucket_kind": "at_or_above", "threshold_c": 11, "yes_token_id": "b", "active": True, "closed": False},
                ]
            },
            observed_max_temp_c=11.0,
            report_window_active=False,
        )

        self.assertEqual(plan["monitor_mode"], "idle")
        self.assertIn("top_or_higher_already_reached", plan["reason_codes"])


if __name__ == "__main__":
    unittest.main()
