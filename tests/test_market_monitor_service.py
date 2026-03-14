import sys
import unittest
from pathlib import Path
import os
from unittest.mock import patch


ROOT = Path(__file__).resolve().parents[1]
SCRIPTS = ROOT / "scripts"
if str(SCRIPTS) not in sys.path:
    sys.path.insert(0, str(SCRIPTS))

from market_monitor_service import build_bucket_snapshots, run_market_monitor_event_window  # noqa: E402


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

    def test_event_window_uses_pre_report_reference_state_for_first_post_report_move(self) -> None:
        catalog = {
            "markets": [
                {
                    "bucket_label": "10°C",
                    "bucket_kind": "exact",
                    "temperature_unit": "C",
                    "threshold_native": 10,
                    "threshold_c": 10,
                    "lower_bound_c": 9.5,
                    "upper_bound_c": 10.49,
                    "yes_token_id": "a",
                    "active": True,
                    "closed": False,
                },
                {
                    "bucket_label": "11°C",
                    "bucket_kind": "exact",
                    "temperature_unit": "C",
                    "threshold_native": 11,
                    "threshold_c": 11,
                    "lower_bound_c": 10.5,
                    "upper_bound_c": 11.49,
                    "yes_token_id": "b",
                    "active": True,
                    "closed": False,
                },
            ]
        }

        def fake_monitor_market_state(*, asset_ids, duration_seconds, baseline_seconds, on_update, **_kwargs):
            self.assertEqual(asset_ids, ["a", "b"])
            first = on_update(
                {
                    "baseline_state": {},
                    "current_state": {
                        "a": {"best_bid": 0.05, "best_ask": 0.06},
                        "b": {"best_bid": 0.03, "best_ask": 0.05},
                    },
                    "messages": [],
                    "observed_at_utc": "2026-03-09T09:29:50Z",
                    "elapsed_seconds": 5.0,
                }
            )
            self.assertIsNone(first)
            triggered = on_update(
                {
                    "baseline_state": {
                        "a": {"best_bid": 0.0, "best_ask": 0.01},
                        "b": {"best_bid": 0.03, "best_ask": 0.05},
                    },
                    "current_state": {
                        "a": {"best_bid": 0.0, "best_ask": 0.01, "last_trade_price": 0.01},
                        "b": {"best_bid": 0.04, "best_ask": 0.05, "last_trade_price": 0.045},
                    },
                    "messages": [],
                    "observed_at_utc": "2026-03-09T09:30:40Z",
                    "elapsed_seconds": 55.0,
                }
            )
            return {
                "messages": [],
                "state": {
                    "a": {"best_bid": 0.0, "best_ask": 0.01, "last_trade_price": 0.01},
                    "b": {"best_bid": 0.04, "best_ask": 0.05, "last_trade_price": 0.045},
                },
                "baseline_state": {
                    "a": {"best_bid": 0.0, "best_ask": 0.01},
                    "b": {"best_bid": 0.03, "best_ask": 0.05},
                },
                "triggered_payload": triggered,
            }

        with patch("market_monitor_service._load_catalog", return_value=catalog), patch(
            "market_monitor_service.monitor_market_state",
            side_effect=fake_monitor_market_state,
        ):
            result = run_market_monitor_event_window(
                polymarket_event_url="https://example.com/event",
                observed_max_temp_c=10.0,
                scheduled_report_utc="2026-03-09T09:30:00Z",
                stream_seconds=60.0,
                baseline_seconds=2.0,
                core_only=False,
            )

        self.assertTrue(result["signal"]["triggered"])
        self.assertEqual(result["signal"]["signal_type"], "report_temp_scan_floor_stop")
        self.assertEqual(result["signal"]["implied_report_temp_lower_bound_c"], 11.0)
        self.assertEqual(result["reference_state"]["a"]["best_bid"], 0.05)

    def test_event_window_reads_price_floor_from_env(self) -> None:
        catalog = {
            "markets": [
                {
                    "bucket_label": "10°C",
                    "bucket_kind": "exact",
                    "temperature_unit": "C",
                    "threshold_native": 10,
                    "threshold_c": 10,
                    "lower_bound_c": 9.5,
                    "upper_bound_c": 10.49,
                    "yes_token_id": "a",
                    "active": True,
                    "closed": False,
                },
                {
                    "bucket_label": "11°C",
                    "bucket_kind": "exact",
                    "temperature_unit": "C",
                    "threshold_native": 11,
                    "threshold_c": 11,
                    "lower_bound_c": 10.5,
                    "upper_bound_c": 11.49,
                    "yes_token_id": "b",
                    "active": True,
                    "closed": False,
                },
            ]
        }

        def fake_monitor_market_state(*, on_update, **_kwargs):
            triggered = on_update(
                {
                    "baseline_state": {
                        "a": {"best_bid": 0.012, "best_ask": 0.02},
                        "b": {"best_bid": 0.015, "best_ask": 0.03},
                    },
                    "current_state": {
                        "a": {"best_bid": 0.0, "best_ask": 0.01},
                        "b": {"best_bid": 0.015, "best_ask": 0.03},
                    },
                    "messages": [],
                    "observed_at_utc": "2026-03-09T09:30:40Z",
                    "elapsed_seconds": 40.0,
                }
            )
            return {
                "messages": [],
                "state": {
                    "a": {"best_bid": 0.0, "best_ask": 0.01},
                    "b": {"best_bid": 0.015, "best_ask": 0.03},
                },
                "baseline_state": {
                    "a": {"best_bid": 0.012, "best_ask": 0.02},
                    "b": {"best_bid": 0.015, "best_ask": 0.03},
                },
                "triggered_payload": triggered,
            }

        with patch.dict(os.environ, {"MARKET_SIGNAL_PRICE_FLOOR": "0.01"}, clear=False), patch(
            "market_monitor_service._load_catalog",
            return_value=catalog,
        ), patch(
            "market_monitor_service.monitor_market_state",
            side_effect=fake_monitor_market_state,
        ):
            result = run_market_monitor_event_window(
                polymarket_event_url="https://example.com/event",
                observed_max_temp_c=10.0,
                scheduled_report_utc="2026-03-09T09:30:00Z",
                stream_seconds=60.0,
                baseline_seconds=2.0,
                core_only=False,
            )

        self.assertTrue(result["signal"]["triggered"])
        self.assertEqual(result["signal"]["implied_report_temp_lower_bound_c"], 11.0)

    def test_event_window_subscribes_all_live_buckets_when_observed_reference_missing(self) -> None:
        catalog = {
            "markets": [
                {
                    "bucket_label": "10°C",
                    "bucket_kind": "exact",
                    "threshold_c": 10,
                    "yes_token_id": "a",
                    "active": True,
                    "closed": False,
                },
                {
                    "bucket_label": "11°C",
                    "bucket_kind": "exact",
                    "threshold_c": 11,
                    "yes_token_id": "b",
                    "active": True,
                    "closed": False,
                },
                {
                    "bucket_label": "12°C or higher",
                    "bucket_kind": "at_or_above",
                    "threshold_c": 12,
                    "yes_token_id": "c",
                    "active": True,
                    "closed": False,
                },
            ]
        }

        def fake_monitor_market_state(*, asset_ids, **_kwargs):
            self.assertEqual(asset_ids, ["a", "b", "c"])
            return {
                "messages": [],
                "state": {"a": {"best_bid": 0.03, "best_ask": 0.04}},
                "baseline_state": {"a": {"best_bid": 0.03, "best_ask": 0.04}},
                "triggered_payload": None,
            }

        with patch("market_monitor_service._load_catalog", return_value=catalog), patch(
            "market_monitor_service.monitor_market_state",
            side_effect=fake_monitor_market_state,
        ):
            result = run_market_monitor_event_window(
                polymarket_event_url="https://example.com/event",
                observed_max_temp_c=None,
                scheduled_report_utc="2026-03-09T09:30:00Z",
                stream_seconds=60.0,
                baseline_seconds=2.0,
                core_only=False,
            )

        self.assertTrue(result["monitor_ok"])
        self.assertEqual(result["monitor_status"], "ok")

    def test_event_window_marks_no_market_data_as_monitor_failure(self) -> None:
        catalog = {
            "markets": [
                {
                    "bucket_label": "10°C",
                    "bucket_kind": "exact",
                    "threshold_c": 10,
                    "yes_token_id": "a",
                    "active": True,
                    "closed": False,
                },
                {
                    "bucket_label": "11°C",
                    "bucket_kind": "exact",
                    "threshold_c": 11,
                    "yes_token_id": "b",
                    "active": True,
                    "closed": False,
                },
            ]
        }

        with patch("market_monitor_service._load_catalog", return_value=catalog), patch(
            "market_monitor_service.monitor_market_state",
            return_value={"messages": [], "state": {}, "baseline_state": {}, "triggered_payload": None},
        ):
            result = run_market_monitor_event_window(
                polymarket_event_url="https://example.com/event",
                observed_max_temp_c=10.0,
                scheduled_report_utc="2026-03-09T09:30:00Z",
                stream_seconds=60.0,
                baseline_seconds=2.0,
                core_only=False,
            )

        self.assertFalse(result["monitor_ok"])
        self.assertEqual(result["monitor_status"], "no_market_data")
        self.assertEqual(result["monitor_diagnostics"]["subscribed_asset_count"], 2)
        self.assertFalse(result["signal"]["triggered"])

    def test_event_window_continuous_mode_uses_previous_state_outside_report_window(self) -> None:
        catalog = {
            "markets": [
                {
                    "bucket_label": "19°C",
                    "bucket_kind": "exact",
                    "temperature_unit": "C",
                    "threshold_native": 19,
                    "threshold_c": 19,
                    "lower_bound_c": 18.5,
                    "upper_bound_c": 19.49,
                    "yes_token_id": "a",
                    "active": True,
                    "closed": False,
                },
                {
                    "bucket_label": "20°C",
                    "bucket_kind": "exact",
                    "temperature_unit": "C",
                    "threshold_native": 20,
                    "threshold_c": 20,
                    "lower_bound_c": 19.5,
                    "upper_bound_c": 20.49,
                    "yes_token_id": "b",
                    "active": True,
                    "closed": False,
                },
            ]
        }

        def fake_monitor_market_state(*, on_update, **_kwargs):
            triggered = on_update(
                {
                    "baseline_state": {},
                    "current_state": {
                        "a": {"best_bid": 0.0, "best_ask": 0.01, "last_trade_price": 0.01},
                        "b": {"best_bid": 0.07, "best_ask": 0.09, "last_trade_price": 0.08},
                    },
                    "messages": [],
                    "observed_at_utc": "2026-03-09T09:50:20Z",
                    "elapsed_seconds": 20.0,
                }
            )
            return {
                "messages": [],
                "state": {
                    "a": {"best_bid": 0.0, "best_ask": 0.01, "last_trade_price": 0.01},
                    "b": {"best_bid": 0.07, "best_ask": 0.09, "last_trade_price": 0.08},
                },
                "baseline_state": {},
                "triggered_payload": triggered,
            }

        with patch("market_monitor_service._load_catalog", return_value=catalog), patch(
            "market_monitor_service.monitor_market_state",
            side_effect=fake_monitor_market_state,
        ):
            result = run_market_monitor_event_window(
                polymarket_event_url="https://example.com/event",
                observed_max_temp_c=19.0,
                scheduled_report_utc="2026-03-09T09:30:00Z",
                stream_seconds=60.0,
                baseline_seconds=2.0,
                core_only=False,
                previous_state={
                    "a": {"best_bid": 0.06, "best_ask": 0.07, "last_trade_price": 0.065},
                    "b": {"best_bid": 0.05, "best_ask": 0.07, "last_trade_price": 0.06},
                },
                continuous_mode=True,
            )

        self.assertTrue(result["signal"]["triggered"])
        self.assertEqual(result["signal"]["signal_type"], "report_temp_scan_floor_stop")
        self.assertEqual(result["signal"]["implied_report_temp_lower_bound_c"], 20.0)
        self.assertEqual(result["reference_state"]["a"]["best_bid"], 0.06)
        self.assertEqual(result["final_state"]["b"]["best_bid"], 0.07)


if __name__ == "__main__":
    unittest.main()
