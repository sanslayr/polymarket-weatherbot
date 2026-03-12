import sys
import unittest
from datetime import datetime, timezone
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
SCRIPTS = ROOT / "scripts"
if str(SCRIPTS) not in sys.path:
    sys.path.insert(0, str(SCRIPTS))

from market_alert_runtime_state import json_safe  # noqa: E402
from market_alert_scheduler import (  # noqa: E402
    configured_schedule_for_station,
    current_or_next_window,
    detect_schedule_drift,
    estimate_routine_cadence_minutes,
    infer_routine_minute_slots,
    latest_metar_context,
    loop_sleep_seconds,
    next_scheduled_report_utc_from_slots,
    polymarket_event_url,
    scheduler_metar_context,
    utc_now,
    window_stream_seconds_remaining,
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
        self.assertEqual(estimate_routine_cadence_minutes(rows), 30.0)

    def test_current_or_next_window_returns_current_window_when_inside_post_report_band(self) -> None:
        ctx = {
            "latest_report_utc": datetime(2026, 3, 9, 9, 20, tzinfo=timezone.utc),
            "routine_cadence_min": 30.0,
            "routine_minute_slots": [20, 50],
        }
        start, end, scheduled = current_or_next_window(ctx, datetime(2026, 3, 9, 9, 21, 30, tzinfo=timezone.utc))
        self.assertEqual(start, datetime(2026, 3, 9, 9, 20, tzinfo=timezone.utc))
        self.assertEqual(end, datetime(2026, 3, 9, 9, 24, tzinfo=timezone.utc))
        self.assertEqual(scheduled, "2026-03-09T09:20:00Z")

    def test_current_or_next_window_keeps_current_report_when_inside_pre_window_gap(self) -> None:
        ctx = {
            "latest_report_utc": datetime(2026, 3, 9, 9, 20, tzinfo=timezone.utc),
            "routine_cadence_min": 30.0,
            "routine_minute_slots": [20, 50],
        }
        start, end, scheduled = current_or_next_window(ctx, datetime(2026, 3, 9, 9, 20, 10, tzinfo=timezone.utc))
        self.assertEqual(start, datetime(2026, 3, 9, 9, 20, tzinfo=timezone.utc))
        self.assertEqual(end, datetime(2026, 3, 9, 9, 24, tzinfo=timezone.utc))
        self.assertEqual(scheduled, "2026-03-09T09:20:00Z")

    def test_current_or_next_window_rolls_forward_to_next_report(self) -> None:
        ctx = {
            "latest_report_utc": datetime(2026, 3, 9, 9, 20, tzinfo=timezone.utc),
            "routine_cadence_min": 30.0,
            "routine_minute_slots": [20, 50],
        }
        start, end, scheduled = current_or_next_window(ctx, datetime(2026, 3, 9, 9, 40, tzinfo=timezone.utc))
        self.assertEqual(start, datetime(2026, 3, 9, 9, 50, tzinfo=timezone.utc))
        self.assertEqual(end, datetime(2026, 3, 9, 9, 54, tzinfo=timezone.utc))
        self.assertEqual(scheduled, "2026-03-09T09:50:00Z")

    def test_current_or_next_window_returns_resident_block_outside_routine_window(self) -> None:
        ctx = {
            "latest_report_utc": datetime(2026, 3, 9, 9, 20, tzinfo=timezone.utc),
            "routine_cadence_min": 30.0,
            "routine_minute_slots": [20, 50],
            "resident_mode": True,
            "resident_until_utc": "2026-03-09T11:20:00Z",
        }
        start, end, scheduled = current_or_next_window(ctx, datetime(2026, 3, 9, 9, 40, 15, tzinfo=timezone.utc))
        self.assertEqual(start, datetime(2026, 3, 9, 9, 40, tzinfo=timezone.utc))
        self.assertEqual(end, datetime(2026, 3, 9, 9, 44, tzinfo=timezone.utc))
        self.assertEqual(scheduled, "2026-03-09T09:40:00Z")

    def test_current_or_next_window_clips_resident_block_before_next_routine_window(self) -> None:
        ctx = {
            "latest_report_utc": datetime(2026, 3, 9, 9, 20, tzinfo=timezone.utc),
            "routine_cadence_min": 30.0,
            "routine_minute_slots": [20, 50],
            "resident_mode": True,
            "resident_until_utc": "2026-03-09T11:20:00Z",
        }
        start, end, scheduled = current_or_next_window(ctx, datetime(2026, 3, 9, 9, 48, 15, tzinfo=timezone.utc))
        self.assertEqual(start, datetime(2026, 3, 9, 9, 48, tzinfo=timezone.utc))
        self.assertEqual(end, datetime(2026, 3, 9, 9, 50, tzinfo=timezone.utc))
        self.assertEqual(scheduled, "2026-03-09T09:48:00Z")

    def test_window_stream_seconds_remaining_clips_to_window_end(self) -> None:
        self.assertEqual(
            window_stream_seconds_remaining(
                datetime(2026, 3, 9, 9, 24, tzinfo=timezone.utc),
                datetime(2026, 3, 9, 9, 23, tzinfo=timezone.utc),
            ),
            60.0,
        )
        self.assertIsNone(
            window_stream_seconds_remaining(
                datetime(2026, 3, 9, 9, 24, tzinfo=timezone.utc),
                datetime(2026, 3, 9, 9, 24, tzinfo=timezone.utc),
            )
        )

    def test_loop_sleep_seconds_wakes_early_for_pending_window_even_with_active_tasks(self) -> None:
        self.assertEqual(
            loop_sleep_seconds(
                next_wake=datetime(2026, 3, 9, 9, 20, 2, tzinfo=timezone.utc),
                now_utc=datetime(2026, 3, 9, 9, 20, 0, tzinfo=timezone.utc),
                has_active_tasks=True,
            ),
            2.0,
        )

    def test_json_safe_serializes_station_and_datetime(self) -> None:
        payload = {
            "station": Station(city="Chicago", icao="KORD", lat=41.97, lon=-87.90),
            "signal": {"observed_at_utc": datetime(2026, 3, 9, 9, 20, tzinfo=timezone.utc)},
        }
        safe = json_safe(payload)
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

        with patch("market_alert_scheduler.fetch_metar_24h", return_value=fake_rows), patch(
            "market_alert_scheduler.utc_now",
            return_value=datetime(2026, 3, 9, 10, 0, tzinfo=timezone.utc),
        ):
            ctx = latest_metar_context(station)

        self.assertIsNotNone(ctx)
        self.assertEqual(ctx["latest_report_utc"], datetime(2026, 3, 9, 9, 20, tzinfo=timezone.utc))
        self.assertEqual(ctx["observed_max_temp_c"], 9.0)
        self.assertEqual(ctx["routine_minute_slots"], [51])
        self.assertEqual(ctx["schedule_source"], "config")

    def test_configured_schedule_for_station_reads_constant_slots(self) -> None:
        schedule = configured_schedule_for_station("LTAC")
        self.assertEqual(schedule["cadence_min"], 30.0)
        self.assertEqual(schedule["minute_slots"], [20, 50])

    def test_latest_metar_context_falls_back_to_config_schedule_when_metar_missing(self) -> None:
        station = Station(city="London", icao="EGLC", lat=51.50, lon=0.05)
        from unittest.mock import patch

        with patch("market_alert_scheduler.fetch_metar_24h", return_value=[]), patch(
            "market_alert_scheduler.utc_now",
            return_value=datetime(2026, 3, 10, 13, 21, tzinfo=timezone.utc),
        ):
            ctx = latest_metar_context(station)

        self.assertIsNotNone(ctx)
        self.assertEqual(ctx["routine_cadence_min"], 30.0)
        self.assertEqual(ctx["routine_minute_slots"], [20, 50])
        self.assertEqual(ctx["latest_report_utc"], datetime(2026, 3, 10, 13, 20, tzinfo=timezone.utc))
        self.assertEqual(ctx["schedule_source"], "config")

    def test_scheduler_metar_context_uses_schedule_slots_even_when_cached_metar_lags(self) -> None:
        station = Station(city="Wellington", icao="NZWN", lat=-41.32, lon=174.80)
        stale_rows = [
            {"reportTime": "2026-03-11T00:30:00Z", "rawOb": "METAR TEST 110030Z", "temp": "17"},
        ]
        from unittest.mock import patch

        with patch("market_alert_scheduler.stale_cached_metar_rows", return_value=stale_rows), patch(
            "market_alert_scheduler.utc_now",
            return_value=datetime(2026, 3, 11, 1, 0, 30, tzinfo=timezone.utc),
        ):
            ctx = scheduler_metar_context(station)

        self.assertIsNotNone(ctx)
        self.assertEqual(ctx["latest_report_utc"], datetime(2026, 3, 11, 1, 0, tzinfo=timezone.utc))
        self.assertEqual(ctx["observed_max_temp_c"], 17.0)

    def test_scheduler_metar_context_enters_resident_mode_for_recent_speci(self) -> None:
        station = Station(city="Wellington", icao="NZWN", lat=-41.32, lon=174.80)
        rows = [
            {"reportTime": "2026-03-11T00:00:00Z", "rawOb": "METAR TEST 110000Z", "temp": "16"},
            {"reportTime": "2026-03-11T00:20:00Z", "rawOb": "SPECI TEST 110020Z", "temp": "18"},
            {"reportTime": "2026-03-11T00:30:00Z", "rawOb": "METAR TEST 110030Z", "temp": "17"},
        ]
        from unittest.mock import patch

        with patch("market_alert_scheduler.stale_cached_metar_rows", return_value=rows), patch(
            "market_alert_scheduler.utc_now",
            return_value=datetime(2026, 3, 11, 1, 0, tzinfo=timezone.utc),
        ):
            ctx = scheduler_metar_context(station)

        self.assertIsNotNone(ctx)
        self.assertTrue(ctx["recent_speci_2h"])
        self.assertTrue(ctx["resident_mode"])
        self.assertEqual(ctx["resident_reason"], "recent_speci_2h")


    def test_infer_routine_minute_slots_extracts_fixed_half_hour_pattern(self) -> None:
        rows = [
            {"reportTime": "2026-03-09T08:00:00Z", "rawOb": "METAR TEST 090820Z"},
            {"reportTime": "2026-03-09T08:30:00Z", "rawOb": "METAR TEST 090850Z"},
            {"reportTime": "2026-03-09T09:00:00Z", "rawOb": "METAR TEST 090920Z"},
            {"reportTime": "2026-03-09T09:30:00Z", "rawOb": "METAR TEST 090950Z"},
        ]
        self.assertEqual(infer_routine_minute_slots(rows, 30.0), [20, 50])

    def test_next_scheduled_report_from_slots_finds_next_hour_slot(self) -> None:
        self.assertEqual(
            next_scheduled_report_utc_from_slots(
                datetime(2026, 3, 9, 9, 52, tzinfo=timezone.utc),
                [20, 50],
            ),
            datetime(2026, 3, 9, 10, 20, tzinfo=timezone.utc),
        )

    def test_detect_schedule_drift_when_recent_phase_shifts(self) -> None:
        drift = detect_schedule_drift(
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
        event_url = polymarket_event_url(
            row,
            station,
            scheduled_report_utc="2026-03-10T15:00:00Z",
            now_utc=datetime(2026, 3, 10, 14, 55, tzinfo=timezone.utc),
        )
        self.assertTrue(event_url.endswith("highest-temperature-in-seoul-on-march-11-2026"))


if __name__ == "__main__":
    unittest.main()
