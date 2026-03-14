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
        self.assertEqual(result["signal"]["signal_type"], "observed_temp_floor_sweep")
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
        self.assertEqual(result["signal"]["signal_type"], "observed_temp_floor_sweep")
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

    def test_event_window_returns_quote_trace_for_debug_review(self) -> None:
        catalog = {
            "markets": [
                {
                    "bucket_label": "27°C",
                    "bucket_kind": "exact",
                    "threshold_c": 27,
                    "yes_token_id": "a",
                    "active": True,
                    "closed": False,
                },
                {
                    "bucket_label": "28°C",
                    "bucket_kind": "exact",
                    "threshold_c": 28,
                    "yes_token_id": "b",
                    "active": True,
                    "closed": False,
                },
            ]
        }

        with patch("market_monitor_service._load_catalog", return_value=catalog), patch(
            "market_monitor_service.monitor_market_state",
            return_value={
                "messages": [],
                "state": {
                    "a": {"best_bid": None, "best_ask": 0.001},
                    "b": {"best_bid": 0.07, "best_ask": 0.09},
                },
                "quote_trace": {
                    "a": [
                        {"timestamp": 1.0, "event_type": "book", "best_bid": 0.06, "best_ask": 0.08},
                        {"timestamp": 2.0, "event_type": "best_bid_ask", "best_bid": None, "best_ask": 0.001},
                    ]
                },
                "baseline_state": {
                    "a": {"best_bid": 0.06, "best_ask": 0.08},
                    "b": {"best_bid": 0.05, "best_ask": 0.07},
                },
                "triggered_payload": None,
            },
        ):
            result = run_market_monitor_event_window(
                polymarket_event_url="https://example.com/event",
                observed_max_temp_c=27.0,
                scheduled_report_utc="2026-03-09T09:30:00Z",
                stream_seconds=60.0,
                baseline_seconds=2.0,
                core_only=False,
            )

        self.assertIn("a", result["quote_trace"])
        self.assertEqual(result["quote_trace"]["a"][-1]["event_type"], "best_bid_ask")
        self.assertIsNone(result["quote_trace"]["a"][-1]["best_bid"])
        self.assertEqual(result["quote_trace"]["a"][-1]["best_ask"], 0.001)

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
        self.assertEqual(result["signal"]["signal_type"], "observed_temp_floor_sweep")
        self.assertEqual(result["signal"]["implied_report_temp_lower_bound_c"], 20.0)
        self.assertEqual(result["reference_state"]["a"]["best_bid"], 0.06)
        self.assertEqual(result["final_state"]["b"]["best_bid"], 0.07)

    def test_event_window_floor_sweep_treats_high_baseline_bid_at_one_cent_as_dead(self) -> None:
        catalog = {
            "markets": [
                {
                    "bucket_label": "27°C",
                    "bucket_kind": "exact",
                    "temperature_unit": "C",
                    "threshold_native": 27,
                    "threshold_c": 27,
                    "lower_bound_c": 26.5,
                    "upper_bound_c": 27.49,
                    "yes_token_id": "a",
                    "active": True,
                    "closed": False,
                },
                {
                    "bucket_label": "28°C",
                    "bucket_kind": "exact",
                    "temperature_unit": "C",
                    "threshold_native": 28,
                    "threshold_c": 28,
                    "lower_bound_c": 27.5,
                    "upper_bound_c": 28.49,
                    "yes_token_id": "b",
                    "active": True,
                    "closed": False,
                },
            ]
        }

        def fake_monitor_market_state(*, on_update, **_kwargs):
            on_update(
                {
                    "baseline_state": {},
                    "current_state": {
                        "a": {"best_bid": 0.009, "best_ask": 0.17},
                        "b": {"best_bid": 0.43, "best_ask": 0.48},
                    },
                    "messages": [],
                    "observed_at_utc": "2026-03-09T09:50:20Z",
                    "elapsed_seconds": 20.0,
                }
            )
            return {
                "messages": [],
                "state": {
                    "a": {"best_bid": 0.009, "best_ask": 0.17},
                    "b": {"best_bid": 0.43, "best_ask": 0.48},
                },
                "baseline_state": {},
                "triggered_payload": None,
            }

        with patch("market_monitor_service._load_catalog", return_value=catalog), patch(
            "market_monitor_service.monitor_market_state",
            side_effect=fake_monitor_market_state,
        ):
            result = run_market_monitor_event_window(
                polymarket_event_url="https://example.com/event",
                observed_max_temp_c=27.0,
                scheduled_report_utc="2026-03-09T09:30:00Z",
                stream_seconds=60.0,
                baseline_seconds=2.0,
                core_only=False,
                previous_state={
                    "a": {"best_bid": 0.12, "best_ask": 0.19, "last_trade_price": 0.16},
                    "b": {"best_bid": 0.18, "best_ask": 0.23, "last_trade_price": 0.20},
                },
                continuous_mode=True,
            )

        self.assertEqual(len(result["signals"]), 1)
        self.assertEqual(result["signals"][0]["signal_type"], "observed_temp_floor_sweep")
        self.assertEqual(result["signals"][0]["evidence"]["dead_bucket_label"], "27°C")
        self.assertEqual(result["signals"][0]["target_bucket_label"], "28°C")

    def test_event_window_emits_two_floor_sweep_alerts_when_adjacent_buckets_die_in_sequence(self) -> None:
        catalog = {
            "markets": [
                {
                    "bucket_label": "27°C",
                    "bucket_kind": "exact",
                    "temperature_unit": "C",
                    "threshold_native": 27,
                    "threshold_c": 27,
                    "lower_bound_c": 26.5,
                    "upper_bound_c": 27.49,
                    "yes_token_id": "a",
                    "active": True,
                    "closed": False,
                },
                {
                    "bucket_label": "28°C",
                    "bucket_kind": "exact",
                    "temperature_unit": "C",
                    "threshold_native": 28,
                    "threshold_c": 28,
                    "lower_bound_c": 27.5,
                    "upper_bound_c": 28.49,
                    "yes_token_id": "b",
                    "active": True,
                    "closed": False,
                },
                {
                    "bucket_label": "29°C",
                    "bucket_kind": "exact",
                    "temperature_unit": "C",
                    "threshold_native": 29,
                    "threshold_c": 29,
                    "lower_bound_c": 28.5,
                    "upper_bound_c": 29.49,
                    "yes_token_id": "c",
                    "active": True,
                    "closed": False,
                },
            ]
        }

        def fake_monitor_market_state(*, asset_ids, on_update, **_kwargs):
            self.assertEqual(asset_ids, ["a", "b", "c"])
            on_update(
                {
                    "baseline_state": {},
                    "current_state": {
                        "a": {"best_bid": 0.06, "best_ask": 0.08},
                        "b": {"best_bid": 0.05, "best_ask": 0.07},
                        "c": {"best_bid": 0.04, "best_ask": 0.06},
                    },
                    "messages": [],
                    "observed_at_utc": "2026-03-09T09:29:55Z",
                    "elapsed_seconds": 5.0,
                }
            )
            on_update(
                {
                    "baseline_state": {},
                    "current_state": {
                        "a": {"best_bid": 0.0, "best_ask": 0.001, "last_trade_price": 0.001},
                        "b": {"best_bid": 0.07, "best_ask": 0.09},
                        "c": {"best_bid": 0.04, "best_ask": 0.06},
                    },
                    "messages": [],
                    "observed_at_utc": "2026-03-09T09:30:40Z",
                    "elapsed_seconds": 50.0,
                }
            )
            on_update(
                {
                    "baseline_state": {},
                    "current_state": {
                        "a": {"best_bid": 0.0, "best_ask": 0.001, "last_trade_price": 0.001},
                        "b": {"best_bid": 0.0, "best_ask": 0.001, "last_trade_price": 0.001},
                        "c": {"best_bid": 0.08, "best_ask": 0.10},
                    },
                    "messages": [],
                    "observed_at_utc": "2026-03-09T09:31:35Z",
                    "elapsed_seconds": 105.0,
                }
            )
            return {
                "messages": [],
                "state": {
                    "a": {"best_bid": 0.0, "best_ask": 0.001, "last_trade_price": 0.001},
                    "b": {"best_bid": 0.0, "best_ask": 0.001, "last_trade_price": 0.001},
                    "c": {"best_bid": 0.08, "best_ask": 0.10},
                },
                "baseline_state": {},
                "triggered_payload": None,
            }

        with patch("market_monitor_service._load_catalog", return_value=catalog), patch(
            "market_monitor_service.monitor_market_state",
            side_effect=fake_monitor_market_state,
        ):
            result = run_market_monitor_event_window(
                polymarket_event_url="https://example.com/event",
                observed_max_temp_c=27.0,
                scheduled_report_utc="2026-03-09T09:30:00Z",
                stream_seconds=120.0,
                baseline_seconds=2.0,
                core_only=False,
            )

        self.assertEqual(len(result["signals"]), 2)
        self.assertEqual(result["signals"][0]["signal_type"], "observed_temp_floor_sweep")
        self.assertEqual(result["signals"][0]["evidence"]["dead_bucket_label"], "27°C")
        self.assertEqual(result["signals"][0]["target_bucket_label"], "28°C")
        self.assertEqual(result["signals"][1]["evidence"]["dead_bucket_label"], "28°C")
        self.assertEqual(result["signals"][1]["target_bucket_label"], "29°C")
        self.assertEqual(result["signal"]["target_bucket_label"], "29°C")

    def test_event_window_continuous_mode_keeps_emitting_floor_sweep_alerts_for_resident_monitoring(self) -> None:
        catalog = {
            "markets": [
                {
                    "bucket_label": "27°C",
                    "bucket_kind": "exact",
                    "temperature_unit": "C",
                    "threshold_native": 27,
                    "threshold_c": 27,
                    "lower_bound_c": 26.5,
                    "upper_bound_c": 27.49,
                    "yes_token_id": "a",
                    "active": True,
                    "closed": False,
                },
                {
                    "bucket_label": "28°C",
                    "bucket_kind": "exact",
                    "temperature_unit": "C",
                    "threshold_native": 28,
                    "threshold_c": 28,
                    "lower_bound_c": 27.5,
                    "upper_bound_c": 28.49,
                    "yes_token_id": "b",
                    "active": True,
                    "closed": False,
                },
                {
                    "bucket_label": "29°C",
                    "bucket_kind": "exact",
                    "temperature_unit": "C",
                    "threshold_native": 29,
                    "threshold_c": 29,
                    "lower_bound_c": 28.5,
                    "upper_bound_c": 29.49,
                    "yes_token_id": "c",
                    "active": True,
                    "closed": False,
                },
            ]
        }

        def fake_monitor_market_state(*, on_update, **_kwargs):
            on_update(
                {
                    "baseline_state": {},
                    "current_state": {
                        "a": {"best_bid": 0.0, "best_ask": 0.001, "last_trade_price": 0.001},
                        "b": {"best_bid": 0.06, "best_ask": 0.08},
                        "c": {"best_bid": 0.05, "best_ask": 0.07},
                    },
                    "messages": [],
                    "observed_at_utc": "2026-03-09T09:50:20Z",
                    "elapsed_seconds": 20.0,
                }
            )
            on_update(
                {
                    "baseline_state": {},
                    "current_state": {
                        "a": {"best_bid": 0.0, "best_ask": 0.001, "last_trade_price": 0.001},
                        "b": {"best_bid": 0.0, "best_ask": 0.001, "last_trade_price": 0.001},
                        "c": {"best_bid": 0.07, "best_ask": 0.09},
                    },
                    "messages": [],
                    "observed_at_utc": "2026-03-09T09:51:05Z",
                    "elapsed_seconds": 65.0,
                }
            )
            return {
                "messages": [],
                "state": {
                    "a": {"best_bid": 0.0, "best_ask": 0.001, "last_trade_price": 0.001},
                    "b": {"best_bid": 0.0, "best_ask": 0.001, "last_trade_price": 0.001},
                    "c": {"best_bid": 0.07, "best_ask": 0.09},
                },
                "baseline_state": {},
                "triggered_payload": None,
            }

        with patch("market_monitor_service._load_catalog", return_value=catalog), patch(
            "market_monitor_service.monitor_market_state",
            side_effect=fake_monitor_market_state,
        ):
            result = run_market_monitor_event_window(
                polymarket_event_url="https://example.com/event",
                observed_max_temp_c=27.0,
                scheduled_report_utc="2026-03-09T09:30:00Z",
                stream_seconds=120.0,
                baseline_seconds=2.0,
                core_only=False,
                previous_state={
                    "a": {"best_bid": 0.06, "best_ask": 0.08, "last_trade_price": 0.07},
                    "b": {"best_bid": 0.05, "best_ask": 0.07, "last_trade_price": 0.06},
                    "c": {"best_bid": 0.04, "best_ask": 0.06, "last_trade_price": 0.05},
                },
                continuous_mode=True,
            )

        self.assertEqual(len(result["signals"]), 2)
        self.assertEqual(result["signals"][0]["evidence"]["dead_bucket_label"], "27°C")
        self.assertEqual(result["signals"][0]["target_bucket_label"], "28°C")
        self.assertEqual(result["signals"][1]["evidence"]["dead_bucket_label"], "28°C")
        self.assertEqual(result["signals"][1]["target_bucket_label"], "29°C")

    def test_event_window_continuous_mode_restores_floor_watch_state_across_resident_blocks(self) -> None:
        catalog = {
            "markets": [
                {
                    "bucket_label": "27°C",
                    "bucket_kind": "exact",
                    "temperature_unit": "C",
                    "threshold_native": 27,
                    "threshold_c": 27,
                    "lower_bound_c": 26.5,
                    "upper_bound_c": 27.49,
                    "yes_token_id": "a",
                    "active": True,
                    "closed": False,
                },
                {
                    "bucket_label": "28°C",
                    "bucket_kind": "exact",
                    "temperature_unit": "C",
                    "threshold_native": 28,
                    "threshold_c": 28,
                    "lower_bound_c": 27.5,
                    "upper_bound_c": 28.49,
                    "yes_token_id": "b",
                    "active": True,
                    "closed": False,
                },
                {
                    "bucket_label": "29°C",
                    "bucket_kind": "exact",
                    "temperature_unit": "C",
                    "threshold_native": 29,
                    "threshold_c": 29,
                    "lower_bound_c": 28.5,
                    "upper_bound_c": 29.49,
                    "yes_token_id": "c",
                    "active": True,
                    "closed": False,
                },
            ]
        }

        def first_block_monitor(*, on_update, **_kwargs):
            on_update(
                {
                    "baseline_state": {},
                    "current_state": {
                        "a": {"best_bid": 0.0, "best_ask": 0.001, "last_trade_price": 0.001},
                        "b": {"best_bid": 0.06, "best_ask": 0.08},
                        "c": {"best_bid": 0.05, "best_ask": 0.07},
                    },
                    "messages": [],
                    "observed_at_utc": "2026-03-09T09:50:20Z",
                    "elapsed_seconds": 20.0,
                }
            )
            return {
                "messages": [],
                "state": {
                    "a": {"best_bid": 0.0, "best_ask": 0.001, "last_trade_price": 0.001},
                    "b": {"best_bid": 0.06, "best_ask": 0.08},
                    "c": {"best_bid": 0.05, "best_ask": 0.07},
                },
                "baseline_state": {},
                "triggered_payload": None,
            }

        with patch("market_monitor_service._load_catalog", return_value=catalog), patch(
            "market_monitor_service.monitor_market_state",
            side_effect=first_block_monitor,
        ):
            first_result = run_market_monitor_event_window(
                polymarket_event_url="https://example.com/event",
                observed_max_temp_c=27.0,
                scheduled_report_utc="2026-03-09T09:30:00Z",
                stream_seconds=60.0,
                baseline_seconds=2.0,
                core_only=False,
                previous_state={
                    "a": {"best_bid": 0.06, "best_ask": 0.08, "last_trade_price": 0.07},
                    "b": {"best_bid": 0.05, "best_ask": 0.07, "last_trade_price": 0.06},
                    "c": {"best_bid": 0.04, "best_ask": 0.06, "last_trade_price": 0.05},
                },
                continuous_mode=True,
            )

        self.assertEqual(first_result["signals"][0]["evidence"]["dead_bucket_label"], "27°C")
        self.assertEqual(first_result["signals"][0]["target_bucket_label"], "28°C")

        def second_block_monitor(*, on_update, **_kwargs):
            on_update(
                {
                    "baseline_state": {},
                    "current_state": {
                        "a": {"best_bid": 0.0, "best_ask": 0.001, "last_trade_price": 0.001},
                        "b": {"best_bid": 0.0, "best_ask": 0.001, "last_trade_price": 0.001},
                        "c": {"best_bid": 0.07, "best_ask": 0.09},
                    },
                    "messages": [],
                    "observed_at_utc": "2026-03-09T09:54:15Z",
                    "elapsed_seconds": 15.0,
                }
            )
            return {
                "messages": [],
                "state": {
                    "a": {"best_bid": 0.0, "best_ask": 0.001, "last_trade_price": 0.001},
                    "b": {"best_bid": 0.0, "best_ask": 0.001, "last_trade_price": 0.001},
                    "c": {"best_bid": 0.07, "best_ask": 0.09},
                },
                "baseline_state": {},
                "triggered_payload": None,
            }

        with patch("market_monitor_service._load_catalog", return_value=catalog), patch(
            "market_monitor_service.monitor_market_state",
            side_effect=second_block_monitor,
        ):
            second_result = run_market_monitor_event_window(
                polymarket_event_url="https://example.com/event",
                observed_max_temp_c=27.0,
                scheduled_report_utc="2026-03-09T09:34:00Z",
                stream_seconds=60.0,
                baseline_seconds=2.0,
                core_only=False,
                previous_state={
                    "a": {"best_bid": 0.0, "best_ask": 0.001, "last_trade_price": 0.001},
                    "b": {"best_bid": 0.06, "best_ask": 0.08, "last_trade_price": 0.07},
                    "c": {"best_bid": 0.05, "best_ask": 0.07, "last_trade_price": 0.06},
                },
                continuous_mode=True,
                floor_watch_state=first_result["floor_watch_state"],
            )

        self.assertEqual(len(second_result["signals"]), 1)
        self.assertEqual(second_result["signals"][0]["evidence"]["dead_bucket_label"], "28°C")
        self.assertEqual(second_result["signals"][0]["target_bucket_label"], "29°C")


if __name__ == "__main__":
    unittest.main()
