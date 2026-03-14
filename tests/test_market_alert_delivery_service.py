import sys
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
SCRIPTS = ROOT / "scripts"
if str(SCRIPTS) not in sys.path:
    sys.path.insert(0, str(SCRIPTS))

from market_alert_delivery_service import alert_key  # noqa: E402


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
            "signal_type": "report_temp_scan_floor_stop",
            "target_bucket_label": "20°C",
            "evidence": {"first_live_bucket_label": "20°C"},
        }
        signal_b = {
            "signal_type": "report_temp_scan_floor_stop",
            "target_bucket_label": "21°C",
            "evidence": {"first_live_bucket_label": "21°C"},
        }

        self.assertNotEqual(
            alert_key("NZWN", signal_a, event_url="https://example.com/event"),
            alert_key("NZWN", signal_b, event_url="https://example.com/event"),
        )


if __name__ == "__main__":
    unittest.main()
