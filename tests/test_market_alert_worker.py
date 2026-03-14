import sys
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
SCRIPTS = ROOT / "scripts"
if str(SCRIPTS) not in sys.path:
    sys.path.insert(0, str(SCRIPTS))

from unittest.mock import patch

from market_alert_worker import _station_has_active_task, _station_task  # noqa: E402
from market_alert_worker import _is_day_disabled_for_event, _should_disable_station_for_event, _update_day_disabled_event  # noqa: E402
from station_catalog import Station  # noqa: E402


class MarketAlertWorkerTest(unittest.TestCase):
    def test_station_has_active_task_matches_same_station_across_different_windows(self) -> None:
        active_tasks = {
            "NZWN|2026-03-12T09:48:00Z": object(),
            "LFPG|2026-03-12T10:00:00Z": object(),
        }
        self.assertTrue(_station_has_active_task(active_tasks, "NZWN"))
        self.assertFalse(_station_has_active_task(active_tasks, "KORD"))

    def test_should_disable_station_for_event_on_no_subscriptions_without_live_market(self) -> None:
        payload = {
            "monitor_status": "no_subscriptions",
            "tradable_live_market_count": 0,
        }
        self.assertTrue(_should_disable_station_for_event(payload))

    def test_update_day_disabled_event_marks_and_matches_same_event(self) -> None:
        state = {"day_disabled_events": {}}
        payload = {
            "station": Station(city="Hong Kong", icao="VHHH", lat=22.3089, lon=113.9146),
            "event_url": "https://polymarket.com/event/highest-temperature-in-hong-kong-on-march-12-2026",
            "monitor_status": "no_subscriptions",
            "tradable_live_market_count": 0,
        }
        _update_day_disabled_event(state, payload)
        self.assertTrue(
            _is_day_disabled_for_event(
                state,
                "VHHH",
                "https://polymarket.com/event/highest-temperature-in-hong-kong-on-march-12-2026",
            )
        )
        self.assertFalse(
            _is_day_disabled_for_event(
                state,
                "VHHH",
                "https://polymarket.com/event/highest-temperature-in-hong-kong-on-march-13-2026",
            )
        )

    def test_station_task_exposes_detector_observed_temp_for_window_logging(self) -> None:
        row = {"city": "Paris", "icao": "LFPG", "lat": "49.0097", "lon": "2.5479", "country": "France"}
        metar_ctx = {
            "observed_max_temp_c": 10.0,
            "observed_max_temp_quantized": True,
            "observed_max_time_local": "2026-03-14T14:20:00+01:00",
        }
        result = {
            "signal": {"triggered": False},
            "monitor_ok": True,
            "monitor_status": "ok",
            "monitor_diagnostics": {},
            "final_state": {},
            "catalog": {},
        }
        with patch("market_monitor_service.run_market_monitor_event_window", return_value=result):
            payload = _station_task(
                row,
                metar_ctx,
                "2026-03-14T13:20:00Z",
                stream_seconds=60.0,
            )

        self.assertEqual(payload["detector_observed_temp_c"], 10.0)
        self.assertTrue(payload["detector_observed_temp_quantized"])
        self.assertEqual(payload["detector_observed_time_local"], "2026-03-14T14:20:00+01:00")


if __name__ == "__main__":
    unittest.main()
