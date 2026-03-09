import sys
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
SCRIPTS = ROOT / "scripts"
if str(SCRIPTS) not in sys.path:
    sys.path.insert(0, str(SCRIPTS))

from market_implied_weather_signal import infer_market_implied_report_signal  # noqa: E402


class MarketImpliedWeatherSignalTest(unittest.TestCase):
    def test_detects_lower_bound_jump_from_dead_lower_bucket(self) -> None:
        signal = infer_market_implied_report_signal(
            bucket_snapshots=[
                {
                    "bucket_label": "6°C or below",
                    "bucket_kind": "at_or_below",
                    "threshold_c": 6,
                    "prev_best_bid": 0.05,
                    "best_bid": 0.0,
                    "best_ask": 0.01,
                    "last_trade_price": 0.01,
                    "trade_count_3m": 2,
                }
            ],
            scheduled_report_utc="2026-03-09T09:30:00Z",
            now_utc="2026-03-09T09:31:20Z",
            latest_observed_temp_c=6.0,
        )

        self.assertTrue(signal["triggered"])
        self.assertEqual(signal["implied_report_temp_lower_bound_c"], 7.0)
        self.assertEqual(signal["confidence"], "high")
        self.assertIn("> 6", signal["message"])

    def test_ignores_bucket_if_it_was_already_too_cheap(self) -> None:
        signal = infer_market_implied_report_signal(
            bucket_snapshots=[
                {
                    "bucket_label": "6°C or below",
                    "bucket_kind": "at_or_below",
                    "threshold_c": 6,
                    "prev_best_bid": 0.01,
                    "best_bid": 0.0,
                    "best_ask": 0.01,
                }
            ],
            scheduled_report_utc="2026-03-09T09:30:00Z",
            now_utc="2026-03-09T09:31:20Z",
        )

        self.assertFalse(signal["triggered"])
        self.assertIsNone(signal["implied_report_temp_lower_bound_c"])

    def test_ask_collapse_alone_can_trigger_signal(self) -> None:
        signal = infer_market_implied_report_signal(
            bucket_snapshots=[
                {
                    "bucket_label": "6°C or below",
                    "bucket_kind": "at_or_below",
                    "threshold_c": 6,
                    "prev_best_bid": 0.04,
                    "best_bid": 0.005,
                    "best_ask": 0.01,
                }
            ],
            scheduled_report_utc="2026-03-09T09:30:00Z",
            now_utc="2026-03-09T09:32:10Z",
        )

        self.assertTrue(signal["triggered"])
        self.assertEqual(signal["implied_report_temp_lower_bound_c"], 7.0)

    def test_does_not_trigger_before_post_report_window(self) -> None:
        signal = infer_market_implied_report_signal(
            bucket_snapshots=[
                {
                    "bucket_label": "6°C or below",
                    "bucket_kind": "at_or_below",
                    "threshold_c": 6,
                    "prev_best_bid": 0.05,
                    "best_bid": 0.0,
                    "best_ask": 0.01,
                }
            ],
            scheduled_report_utc="2026-03-09T09:30:00Z",
            now_utc="2026-03-09T09:30:20Z",
        )

        self.assertFalse(signal["triggered"])

    def test_top_or_higher_lock_in_triggers_when_all_lower_buckets_die(self) -> None:
        signal = infer_market_implied_report_signal(
            bucket_snapshots=[
                {
                    "bucket_label": "10°C",
                    "bucket_kind": "exact",
                    "threshold_c": 10,
                    "prev_best_bid": 0.04,
                    "best_bid": 0.0,
                    "best_ask": 0.01,
                },
                {
                    "bucket_label": "11°C",
                    "bucket_kind": "exact",
                    "threshold_c": 11,
                    "prev_best_bid": 0.05,
                    "best_bid": 0.0,
                    "best_ask": 0.01,
                },
                {
                    "bucket_label": "12°C or higher",
                    "bucket_kind": "at_or_above",
                    "threshold_c": 12,
                    "prev_best_bid": 0.2,
                    "best_bid": 0.18,
                    "best_ask": 0.23,
                },
            ],
            scheduled_report_utc="2026-03-09T09:30:00Z",
            now_utc="2026-03-09T09:31:40Z",
        )

        self.assertTrue(signal["triggered"])
        self.assertEqual(signal["signal_type"], "report_temp_top_bucket_lock_in")
        self.assertEqual(signal["implied_report_temp_lower_bound_c"], 12.0)

    def test_ascending_scan_stops_at_first_live_bucket(self) -> None:
        signal = infer_market_implied_report_signal(
            bucket_snapshots=[
                {
                    "bucket_label": "6°C",
                    "bucket_kind": "exact",
                    "threshold_c": 6,
                    "prev_best_bid": 0.05,
                    "best_bid": 0.0,
                    "best_ask": 0.01,
                },
                {
                    "bucket_label": "7°C",
                    "bucket_kind": "exact",
                    "threshold_c": 7,
                    "prev_best_bid": 0.07,
                    "best_bid": 0.04,
                    "best_ask": 0.09,
                },
                {
                    "bucket_label": "8°C",
                    "bucket_kind": "exact",
                    "threshold_c": 8,
                    "prev_best_bid": 0.11,
                    "best_bid": 0.10,
                    "best_ask": 0.13,
                },
            ],
            scheduled_report_utc="2026-03-09T09:30:00Z",
            now_utc="2026-03-09T09:31:30Z",
            latest_observed_temp_c=6.0,
        )

        self.assertTrue(signal["triggered"])
        self.assertEqual(signal["signal_type"], "report_temp_scan_floor_stop")
        self.assertEqual(signal["implied_report_temp_lower_bound_c"], 7.0)
        self.assertIn("7°C", signal["message"])

    def test_ascending_scan_can_stop_at_top_or_higher(self) -> None:
        signal = infer_market_implied_report_signal(
            bucket_snapshots=[
                {
                    "bucket_label": "10°C",
                    "bucket_kind": "exact",
                    "threshold_c": 10,
                    "prev_best_bid": 0.04,
                    "best_bid": 0.0,
                    "best_ask": 0.01,
                },
                {
                    "bucket_label": "11°C",
                    "bucket_kind": "exact",
                    "threshold_c": 11,
                    "prev_best_bid": 0.05,
                    "best_bid": 0.0,
                    "best_ask": 0.01,
                },
                {
                    "bucket_label": "12°C or higher",
                    "bucket_kind": "at_or_above",
                    "threshold_c": 12,
                    "prev_best_bid": 0.2,
                    "best_bid": 0.18,
                    "best_ask": 0.23,
                },
            ],
            scheduled_report_utc="2026-03-09T09:30:00Z",
            now_utc="2026-03-09T09:31:40Z",
            latest_observed_temp_c=10.0,
        )

        self.assertTrue(signal["triggered"])
        self.assertEqual(signal["signal_type"], "report_temp_top_bucket_lock_in")
        self.assertEqual(signal["implied_report_temp_lower_bound_c"], 12.0)


if __name__ == "__main__":
    unittest.main()
