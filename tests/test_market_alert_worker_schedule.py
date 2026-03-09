import sys
import unittest
from datetime import datetime, timezone
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
SCRIPTS = ROOT / "scripts"
if str(SCRIPTS) not in sys.path:
    sys.path.insert(0, str(SCRIPTS))

from market_alert_worker import _current_or_next_window, _estimate_routine_cadence_minutes  # noqa: E402


class MarketAlertWorkerScheduleTest(unittest.TestCase):
    def test_estimate_routine_cadence_minutes_uses_recent_metar_spacing(self) -> None:
        rows = [
            {"reportTime": "2026-03-09T08:20:00Z", "rawOb": "TEST 090820Z"},
            {"reportTime": "2026-03-09T08:50:00Z", "rawOb": "TEST 090850Z"},
            {"reportTime": "2026-03-09T09:20:00Z", "rawOb": "TEST 090920Z"},
            {"reportTime": "2026-03-09T09:50:00Z", "rawOb": "TEST 090950Z"},
        ]
        self.assertEqual(_estimate_routine_cadence_minutes(rows), 30.0)

    def test_current_or_next_window_returns_current_window_when_inside_post_report_band(self) -> None:
        ctx = {
            "latest_report_utc": datetime(2026, 3, 9, 9, 20, tzinfo=timezone.utc),
            "routine_cadence_min": 30.0,
        }
        start, end, scheduled = _current_or_next_window(ctx, datetime(2026, 3, 9, 9, 21, 30, tzinfo=timezone.utc))
        self.assertEqual(start, datetime(2026, 3, 9, 9, 21, tzinfo=timezone.utc))
        self.assertEqual(end, datetime(2026, 3, 9, 9, 23, tzinfo=timezone.utc))
        self.assertEqual(scheduled, "2026-03-09T09:20:00Z")

    def test_current_or_next_window_rolls_forward_to_next_report(self) -> None:
        ctx = {
            "latest_report_utc": datetime(2026, 3, 9, 9, 20, tzinfo=timezone.utc),
            "routine_cadence_min": 30.0,
        }
        start, end, scheduled = _current_or_next_window(ctx, datetime(2026, 3, 9, 9, 40, tzinfo=timezone.utc))
        self.assertEqual(start, datetime(2026, 3, 9, 9, 51, tzinfo=timezone.utc))
        self.assertEqual(end, datetime(2026, 3, 9, 9, 53, tzinfo=timezone.utc))
        self.assertEqual(scheduled, "2026-03-09T09:50:00Z")


if __name__ == "__main__":
    unittest.main()
