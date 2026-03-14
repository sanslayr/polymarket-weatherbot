import sys
import unittest
from pathlib import Path
from unittest.mock import patch


ROOT = Path(__file__).resolve().parents[1]
SCRIPTS = ROOT / "scripts"
if str(SCRIPTS) not in sys.path:
    sys.path.insert(0, str(SCRIPTS))

from market_alert_worker import _station_task  # noqa: E402


class MarketAlertWorkerTest(unittest.TestCase):
    def test_station_task_emits_live_alerts_for_each_floor_sweep_signal(self) -> None:
        row = {
            "city": "Buenos Aires",
            "icao": "SABE",
            "lat": "-34.56",
            "lon": "-58.42",
            "polymarket_event_url_format": "https://example.com/event",
            "polymarket_city_slug": "buenos-aires",
        }
        metar_ctx = {
            "observed_max_temp_c": 27.0,
            "observed_max_temp_quantized": True,
            "observed_max_time_local": "2026-03-13T16:00:00-03:00",
            "resident_reason": "recent_speci_2h",
        }
        signal_1 = {
            "signal_type": "observed_temp_floor_sweep",
            "triggered": True,
            "target_bucket_label": "28°C",
            "observed_at_utc": "2026-03-13T19:01:00Z",
            "evidence": {
                "dead_bucket_label": "27°C",
                "first_live_bucket_label": "28°C",
                "first_live_bucket_bid": 0.41,
                "first_live_bucket_ask": 0.45,
                "collapsed_prefix_labels": ["27°C"],
                "collapsed_prefix_prev_bids": {"27°C": 0.08},
                "collapsed_prefix_current_bids": {"27°C": 0.0},
                "collapsed_prefix_current_asks": {"27°C": 0.001},
            },
        }
        signal_2 = {
            "signal_type": "observed_temp_floor_sweep",
            "triggered": True,
            "target_bucket_label": "29°C",
            "observed_at_utc": "2026-03-13T19:03:00Z",
            "evidence": {
                "dead_bucket_label": "28°C",
                "first_live_bucket_label": "29°C",
                "first_live_bucket_bid": 0.52,
                "first_live_bucket_ask": 0.58,
                "collapsed_prefix_labels": ["28°C"],
                "collapsed_prefix_prev_bids": {"28°C": 0.11},
                "collapsed_prefix_current_bids": {"28°C": 0.0},
                "collapsed_prefix_current_asks": {"28°C": 0.001},
            },
        }
        live_alerts: list[dict] = []

        def fake_run_market_monitor_event_window(*, on_signal=None, **_kwargs):
            on_signal({"signal": signal_1, "bucket_snapshots": [], "current_state": {}})
            on_signal({"signal": signal_2, "bucket_snapshots": [], "current_state": {}})
            return {
                "signal": signal_2,
                "signals": [signal_1, signal_2],
                "monitor_ok": True,
                "monitor_status": "ok",
                "monitor_diagnostics": {"triggered_signal_count": 2},
                "final_state": {},
                "catalog": {"event_found": True, "markets": []},
            }

        def fake_live_alert_callback(payload: dict) -> dict:
            live_alerts.append(dict(payload))
            return {
                "sent": True,
                "cooldown_skipped": False,
                "delivery": {"targets": ["tg"], "success_count": 1, "error_count": 0},
            }

        with patch("market_monitor_service.run_market_monitor_event_window", side_effect=fake_run_market_monitor_event_window), patch(
            "market_alert_worker.polymarket_event_url",
            return_value="https://example.com/event",
        ), patch("market_alert_worker.station_timezone_name", return_value="UTC"):
            payload = _station_task(
                row,
                metar_ctx,
                "2026-03-13T19:00:00Z",
                stream_seconds=120.0,
                continuous_mode=True,
                live_alert_callback=fake_live_alert_callback,
            )

        self.assertEqual(len(live_alerts), 2)
        self.assertEqual(live_alerts[0]["signal"]["evidence"]["dead_bucket_label"], "27°C")
        self.assertEqual(live_alerts[1]["signal"]["evidence"]["dead_bucket_label"], "28°C")
        self.assertEqual(payload["signal"]["target_bucket_label"], "29°C")
        self.assertEqual(len(payload["signals"]), 2)
        self.assertEqual(len(payload["emitted_alerts"]), 2)
        self.assertIn("27°C 归零", live_alerts[0]["text"])
        self.assertIn("28°C 归零", live_alerts[1]["text"])


if __name__ == "__main__":
    unittest.main()
