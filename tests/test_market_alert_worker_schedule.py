import sys
import unittest
from datetime import datetime, timezone
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
SCRIPTS = ROOT / "scripts"
if str(SCRIPTS) not in sys.path:
    sys.path.insert(0, str(SCRIPTS))

from market_alert_worker import (  # noqa: E402
    _configured_schedule_for_station,
    _current_or_next_window,
    _detect_schedule_drift,
    _estimate_routine_cadence_minutes,
    _infer_routine_minute_slots,
    _json_safe,
    _latest_metar_context,
    _next_scheduled_report_utc_from_slots,
    _polymarket_event_url,
)
from station_catalog import Station  # noqa: E402


class MarketAlertWorkerScheduleTest(unittest.TestCase):
    def test_estimate_routine_cadence_minutes_uses_recent_metar_spacing(self) -> None:
        rows = [
            {"reportTime": "2026-03-09T08:00:00Z", "rawOb": "METAR TEST 090820Z"},
            {"reportTime": "2026-03-09T08:20:00Z", "rawOb": "SPECI TEST 090835Z"},
            {"reportTime": "2026-03-09T08:30:00Z", "rawOb": "METAR TEST 090850Z"},
            {"reportTime": "2026-03-09T09:00:00Z", "rawOb": "METAR TEST 090920Z"},
            {"reportTime": "2026-03-09T09:30:00Z", "rawOb": "METAR TEST 090950Z"},
        ]
        self.assertEqual(_estimate_routine_cadence_minutes(rows), 30.0)

    def test_current_or_next_window_returns_current_window_when_inside_post_report_band(self) -> None:
        ctx = {
            "latest_report_utc": datetime(2026, 3, 9, 9, 20, tzinfo=timezone.utc),
            "routine_cadence_min": 30.0,
            "routine_minute_slots": [20, 50],
        }
        start, end, scheduled = _current_or_next_window(ctx, datetime(2026, 3, 9, 9, 21, 30, tzinfo=timezone.utc))
        self.assertEqual(start, datetime(2026, 3, 9, 9, 20, 30, tzinfo=timezone.utc))
        self.assertEqual(end, datetime(2026, 3, 9, 9, 25, tzinfo=timezone.utc))
        self.assertEqual(scheduled, "2026-03-09T09:20:00Z")

    def test_current_or_next_window_keeps_current_report_when_inside_pre_window_gap(self) -> None:
        ctx = {
            "latest_report_utc": datetime(2026, 3, 9, 9, 20, tzinfo=timezone.utc),
            "routine_cadence_min": 30.0,
            "routine_minute_slots": [20, 50],
        }
        start, end, scheduled = _current_or_next_window(ctx, datetime(2026, 3, 9, 9, 20, 10, tzinfo=timezone.utc))
        self.assertEqual(start, datetime(2026, 3, 9, 9, 20, 30, tzinfo=timezone.utc))
        self.assertEqual(end, datetime(2026, 3, 9, 9, 25, tzinfo=timezone.utc))
        self.assertEqual(scheduled, "2026-03-09T09:20:00Z")

    def test_current_or_next_window_rolls_forward_to_next_report(self) -> None:
        ctx = {
            "latest_report_utc": datetime(2026, 3, 9, 9, 20, tzinfo=timezone.utc),
            "routine_cadence_min": 30.0,
            "routine_minute_slots": [20, 50],
        }
        start, end, scheduled = _current_or_next_window(ctx, datetime(2026, 3, 9, 9, 40, tzinfo=timezone.utc))
        self.assertEqual(start, datetime(2026, 3, 9, 9, 50, 30, tzinfo=timezone.utc))
        self.assertEqual(end, datetime(2026, 3, 9, 9, 55, tzinfo=timezone.utc))
        self.assertEqual(scheduled, "2026-03-09T09:50:00Z")

    def test_json_safe_serializes_station_and_datetime(self) -> None:
        payload = {
            "station": Station(city="Chicago", icao="KORD", lat=41.97, lon=-87.90),
            "signal": {"observed_at_utc": datetime(2026, 3, 9, 9, 20, tzinfo=timezone.utc)},
        }
        safe = _json_safe(payload)
        self.assertEqual(safe["station"]["icao"], "KORD")
        self.assertEqual(safe["signal"]["observed_at_utc"], "2026-03-09T09:20:00+00:00")

    def test_latest_metar_context_uses_latest_routine_metar_but_keeps_all_obs_for_max(self) -> None:
        fake_rows = [
            {"reportTime": "2026-03-09T08:00:00Z", "rawOb": "METAR TEST 090820Z", "temp": "7"},
            {"reportTime": "2026-03-09T08:20:00Z", "rawOb": "SPECI TEST 090835Z", "temp": "9"},
            {"reportTime": "2026-03-09T08:30:00Z", "rawOb": "METAR TEST 090850Z", "temp": "8"},
            {"reportTime": "2026-03-09T09:00:00Z", "rawOb": "METAR TEST 090920Z", "temp": "8"},
        ]
        station = Station(city="Chicago", icao="KORD", lat=41.97, lon=-87.90)
        from unittest.mock import patch

        with patch("market_alert_worker.fetch_metar_24h", return_value=fake_rows), patch(
            "market_alert_worker._utc_now",
            return_value=datetime(2026, 3, 9, 10, 0, tzinfo=timezone.utc),
        ):
            ctx = _latest_metar_context(station)

        self.assertIsNotNone(ctx)
        self.assertEqual(ctx["latest_report_utc"], datetime(2026, 3, 9, 9, 20, tzinfo=timezone.utc))
        self.assertEqual(ctx["observed_max_temp_c"], 9.0)
        self.assertEqual(ctx["routine_minute_slots"], [51])
        self.assertEqual(ctx["schedule_source"], "config")

    def test_configured_schedule_for_station_reads_constant_slots(self) -> None:
        schedule = _configured_schedule_for_station("LTAC")
        self.assertEqual(schedule["cadence_min"], 30.0)
        self.assertEqual(schedule["minute_slots"], [20, 50])

    def test_infer_routine_minute_slots_extracts_fixed_half_hour_pattern(self) -> None:
        rows = [
            {"reportTime": "2026-03-09T08:00:00Z", "rawOb": "METAR TEST 090820Z"},
            {"reportTime": "2026-03-09T08:30:00Z", "rawOb": "METAR TEST 090850Z"},
            {"reportTime": "2026-03-09T09:00:00Z", "rawOb": "METAR TEST 090920Z"},
            {"reportTime": "2026-03-09T09:30:00Z", "rawOb": "METAR TEST 090950Z"},
        ]
        self.assertEqual(_infer_routine_minute_slots(rows, 30.0), [20, 50])

    def test_next_scheduled_report_from_slots_finds_next_hour_slot(self) -> None:
        self.assertEqual(
            _next_scheduled_report_utc_from_slots(
                datetime(2026, 3, 9, 9, 52, tzinfo=timezone.utc),
                [20, 50],
            ),
            datetime(2026, 3, 9, 10, 20, tzinfo=timezone.utc),
        )

    def test_detect_schedule_drift_when_recent_phase_shifts(self) -> None:
        drift = _detect_schedule_drift(
            configured_cadence_min=60.0,
            configured_minute_slots=[51],
            inferred_cadence_min=60.0,
            inferred_minute_slots=[0],
            minute_counts={0: 6},
        )
        self.assertIsNotNone(drift)
        self.assertEqual(drift["inferred_minute_slots"], [0])

    def test_polymarket_event_url_uses_station_local_date_for_report_window(self) -> None:
        row = {
            "polymarket_city_slug": "seoul",
            "polymarket_event_url_format": "https://polymarket.com/event/highest-temperature-in-{city_slug}-on-{date_slug}",
        }
        station = Station(city="Seoul", icao="RKSI", lat=37.46, lon=126.44)
        event_url = _polymarket_event_url(
            row,
            station,
            scheduled_report_utc="2026-03-10T15:00:00Z",
            now_utc=datetime(2026, 3, 10, 14, 55, tzinfo=timezone.utc),
        )
        self.assertTrue(event_url.endswith("highest-temperature-in-seoul-on-march-11-2026"))


if __name__ == "__main__":
    unittest.main()
