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

    def test_selects_top_or_higher_bucket_when_observed_has_already_reached_it(self) -> None:
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

        self.assertEqual(plan["monitor_mode"], "core")
        self.assertEqual(plan["core_watch_asset_ids"], ["b"])
        self.assertIn("top_or_higher_reference_selected", plan["reason_codes"])

    def test_falls_back_to_full_market_when_observed_reference_missing(self) -> None:
        plan = build_market_subscription_plan(
            market_catalog_snapshot={
                "markets": [
                    {"bucket_kind": "exact", "threshold_c": 10, "yes_token_id": "a", "active": True, "closed": False},
                    {"bucket_kind": "exact", "threshold_c": 11, "yes_token_id": "b", "active": True, "closed": False},
                    {"bucket_kind": "at_or_above", "threshold_c": 12, "yes_token_id": "c", "active": True, "closed": False},
                ]
            },
            observed_max_temp_c=None,
            report_window_active=True,
        )

        self.assertEqual(plan["monitor_mode"], "full_market")
        self.assertEqual(plan["core_watch_asset_ids"], ["a", "b", "c"])
        self.assertEqual(plan["upside_scan_asset_ids"], [])
        self.assertIn("observed_reference_missing_all_live", plan["reason_codes"])

    def test_selects_range_bucket_when_observed_falls_inside_fahrenheit_market(self) -> None:
        plan = build_market_subscription_plan(
            market_catalog_snapshot={
                "markets": [
                    {
                        "bucket_kind": "at_or_below",
                        "bucket_label": "55°F or below",
                        "temperature_unit": "F",
                        "threshold_native": 55,
                        "threshold_c": 12.7778,
                        "upper_bound_c": 13.05,
                        "yes_token_id": "a",
                        "active": True,
                        "closed": False,
                    },
                    {
                        "bucket_kind": "range",
                        "bucket_label": "60–61°F",
                        "temperature_unit": "F",
                        "lower_bound_c": 15.2778,
                        "upper_bound_c": 16.3833,
                        "yes_token_id": "b",
                        "active": True,
                        "closed": False,
                    },
                    {
                        "bucket_kind": "range",
                        "bucket_label": "62–63°F",
                        "temperature_unit": "F",
                        "lower_bound_c": 16.3889,
                        "upper_bound_c": 17.5,
                        "yes_token_id": "c",
                        "active": True,
                        "closed": False,
                    },
                    {
                        "bucket_kind": "at_or_above",
                        "bucket_label": "70°F or higher",
                        "temperature_unit": "F",
                        "threshold_native": 70,
                        "threshold_c": 21.1111,
                        "lower_bound_c": 20.8333,
                        "yes_token_id": "d",
                        "active": True,
                        "closed": False,
                    },
                ]
            },
            observed_max_temp_c=16.1,
            report_window_active=False,
        )

        self.assertEqual(plan["core_watch_asset_ids"], ["b"])
        self.assertEqual(plan["upside_scan_asset_ids"], ["c", "d"])


if __name__ == "__main__":
    unittest.main()
