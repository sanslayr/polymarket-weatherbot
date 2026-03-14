import sys
import unittest
from pathlib import Path
from unittest.mock import patch


ROOT = Path(__file__).resolve().parents[1]
SCRIPTS = ROOT / "scripts"
if str(SCRIPTS) not in sys.path:
    sys.path.insert(0, str(SCRIPTS))

from market_alert_delivery_service import alert_key, deliver_alert_payload  # noqa: E402
from station_catalog import Station  # noqa: E402


class MarketAlertDeliveryServiceTest(unittest.TestCase):
    def test_alert_key_ignores_scheduled_report_when_event_and_bucket_match(self) -> None:
        signal_a = {
            "signal_type": "report_temp_scan_floor_stop",
            "scheduled_report_utc": "2026-03-11T13:00:00Z",
            "target_bucket_label": "20°C",
            "evidence": {"first_live_bucket_label": "20°C"},
        }
        signal_b = {
            "signal_type": "report_temp_scan_floor_stop",
            "scheduled_report_utc": "2026-03-11T13:04:00Z",
            "target_bucket_label": "20°C",
            "evidence": {"first_live_bucket_label": "20°C"},
        }

        self.assertEqual(
            alert_key("NZWN", signal_a, event_url="https://example.com/event"),
            alert_key("NZWN", signal_b, event_url="https://example.com/event"),
        )

    def test_alert_key_changes_when_target_bucket_changes(self) -> None:
        signal_a = {
            "signal_type": "observed_temp_floor_sweep",
            "target_bucket_label": "20°C",
            "evidence": {"first_live_bucket_label": "20°C", "dead_bucket_label": "19°C"},
        }
        signal_b = {
            "signal_type": "observed_temp_floor_sweep",
            "target_bucket_label": "21°C",
            "evidence": {"first_live_bucket_label": "21°C", "dead_bucket_label": "20°C"},
        }

        self.assertNotEqual(
            alert_key("NZWN", signal_a, event_url="https://example.com/event"),
            alert_key("NZWN", signal_b, event_url="https://example.com/event"),
        )

    def test_deliver_alert_payload_allows_sequential_floor_sweep_alerts_with_different_dead_buckets(self) -> None:
        state = {"last_alerts": {}}
        station = Station(city="Buenos Aires", icao="SABE", lat=-34.56, lon=-58.42)
        first_payload = {
            "station": station,
            "event_url": "https://example.com/event",
            "signal": {
                "signal_type": "observed_temp_floor_sweep",
                "target_bucket_label": "28°C",
                "evidence": {"first_live_bucket_label": "28°C", "dead_bucket_label": "27°C"},
            },
            "text": "first",
        }
        second_payload = {
            "station": station,
            "event_url": "https://example.com/event",
            "signal": {
                "signal_type": "observed_temp_floor_sweep",
                "target_bucket_label": "29°C",
                "evidence": {"first_live_bucket_label": "29°C", "dead_bucket_label": "28°C"},
            },
            "text": "second",
        }

        with patch(
            "market_alert_delivery_service.send_telegram_messages_report",
            side_effect=[
                {"targets": ["tg"], "successes": ["tg"], "errors": []},
                {"targets": ["tg"], "successes": ["tg"], "errors": []},
            ],
        ) as mock_send:
            first = deliver_alert_payload(
                payload=first_payload,
                state=state,
                cooldown_seconds=900,
                alert_account="weatherbot",
            )
            second = deliver_alert_payload(
                payload=second_payload,
                state=state,
                cooldown_seconds=900,
                alert_account="weatherbot",
            )

        self.assertTrue(first["sent"])
        self.assertTrue(second["sent"])
        self.assertEqual(mock_send.call_count, 2)

    def test_deliver_alert_payload_skips_duplicate_floor_sweep_alert_inside_cooldown(self) -> None:
        state = {"last_alerts": {}}
        station = Station(city="Buenos Aires", icao="SABE", lat=-34.56, lon=-58.42)
        payload = {
            "station": station,
            "event_url": "https://example.com/event",
            "signal": {
                "signal_type": "observed_temp_floor_sweep",
                "target_bucket_label": "28°C",
                "evidence": {"first_live_bucket_label": "28°C", "dead_bucket_label": "27°C"},
            },
            "text": "duplicate",
        }

        with patch(
            "market_alert_delivery_service.send_telegram_messages_report",
            return_value={"targets": ["tg"], "successes": ["tg"], "errors": []},
        ) as mock_send:
            first = deliver_alert_payload(
                payload=dict(payload),
                state=state,
                cooldown_seconds=900,
                alert_account="weatherbot",
            )
            second = deliver_alert_payload(
                payload=dict(payload),
                state=state,
                cooldown_seconds=900,
                alert_account="weatherbot",
            )

        self.assertTrue(first["sent"])
        self.assertFalse(second["sent"])
        self.assertTrue(second["cooldown_skipped"])
        self.assertEqual(mock_send.call_count, 1)


if __name__ == "__main__":
    unittest.main()
