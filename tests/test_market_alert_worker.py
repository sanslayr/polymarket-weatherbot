import sys
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
SCRIPTS = ROOT / "scripts"
if str(SCRIPTS) not in sys.path:
    sys.path.insert(0, str(SCRIPTS))

from market_alert_worker import _station_has_active_task  # noqa: E402
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


if __name__ == "__main__":
    unittest.main()
