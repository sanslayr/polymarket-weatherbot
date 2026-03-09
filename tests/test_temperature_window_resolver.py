import sys
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
SCRIPTS = ROOT / "scripts"
if str(SCRIPTS) not in sys.path:
    sys.path.insert(0, str(SCRIPTS))

from temperature_window_resolver import resolve_temperature_window  # noqa: E402
from temperature_shape_analysis import analyze_temperature_shape  # noqa: E402


class TemperatureWindowResolverTest(unittest.TestCase):
    def test_seoul_flat_plateau_reanchors_late_tail(self) -> None:
        primary_window = {
            "start_local": "2026-03-08T16:00",
            "end_local": "2026-03-08T23:00",
            "peak_local": "2026-03-08T22:00",
            "peak_temp_c": 4.0,
        }
        hourly_day = {
            "time": [
                "2026-03-08T14:00",
                "2026-03-08T15:00",
                "2026-03-08T16:00",
                "2026-03-08T17:00",
                "2026-03-08T18:00",
                "2026-03-08T19:00",
                "2026-03-08T20:00",
                "2026-03-08T21:00",
                "2026-03-08T22:00",
                "2026-03-08T23:00",
            ],
            "temperature_2m": [2.1, 2.4, 2.6, 2.8, 2.9, 3.1, 3.3, 3.7, 4.0, 4.0],
        }
        metar_diag = {
            "station_icao": "RKSI",
            "latest_report_local": "2026-03-08T15:30:00+09:00",
            "observed_max_time_local": "2026-03-08T15:30:00+09:00",
            "observed_max_temp_c": 4.0,
            "latest_temp": 4.0,
            "temp_trend_1step_c": 0.0,
            "temp_trend_smooth_c": 0.0,
            "latest_cloud_code": "CAVOK",
            "cloud_trend": "云层级别稳定（CAVOK）",
            "latest_precip_state": "none",
            "precip_trend": "none",
        }

        resolved = resolve_temperature_window(
            primary_window,
            hourly_day,
            metar_diag,
            station_icao="RKSI",
        )

        self.assertTrue(resolved["override_active"])
        self.assertEqual(resolved["mode"], "obs_plateau_reanchor")
        self.assertEqual(resolved["resolved_window"]["peak_local"], "2026-03-08T15:30")
        self.assertEqual(resolved["resolved_window"]["start_local"], "2026-03-08T14:30")
        self.assertEqual(resolved["resolved_window"]["end_local"], "2026-03-08T16:30")
        self.assertLessEqual(float(resolved["resolved_window"]["peak_temp_c"]), 4.25)

    def test_obs_peak_reanchor_when_obs_far_above_model(self) -> None:
        primary_window = {
            "start_local": "2026-03-08T12:00",
            "end_local": "2026-03-08T15:00",
            "peak_local": "2026-03-08T14:00",
            "peak_temp_c": 6.0,
        }
        hourly_day = {
            "time": [
                "2026-03-08T13:00",
                "2026-03-08T14:00",
                "2026-03-08T15:00",
                "2026-03-08T16:00",
            ],
            "temperature_2m": [5.5, 6.0, 6.2, 6.3],
        }
        metar_diag = {
            "station_icao": "TEST",
            "latest_report_local": "2026-03-08T15:30:00+00:00",
            "observed_max_time_local": "2026-03-08T15:30:00+00:00",
            "observed_max_temp_c": 8.0,
            "latest_temp": 8.0,
            "temp_trend_1step_c": 0.6,
            "temp_trend_smooth_c": 0.5,
            "latest_cloud_code": "CLR",
            "cloud_trend": "稳定",
            "latest_precip_state": "none",
            "precip_trend": "none",
        }

        resolved = resolve_temperature_window(
            primary_window,
            hourly_day,
            metar_diag,
            station_icao="TEST",
        )

        self.assertTrue(resolved["override_active"])
        self.assertEqual(resolved["mode"], "obs_peak_reanchor")
        self.assertEqual(resolved["resolved_window"]["peak_local"], "2026-03-08T15:30")
        self.assertEqual(resolved["resolved_window"]["start_local"], "2026-03-08T14:30")
        self.assertEqual(resolved["resolved_window"]["end_local"], "2026-03-08T17:30")
        self.assertEqual(float(resolved["resolved_window"]["peak_temp_c"]), 8.0)

    def test_sustained_near_peak_hold_can_reanchor_late_tail(self) -> None:
        primary_window = {
            "start_local": "2026-03-08T18:00",
            "end_local": "2026-03-08T22:00",
            "peak_local": "2026-03-08T21:00",
            "peak_temp_c": 4.4,
        }
        hourly_day = {
            "time": [
                "2026-03-08T13:00",
                "2026-03-08T14:00",
                "2026-03-08T15:00",
                "2026-03-08T16:00",
                "2026-03-08T17:00",
                "2026-03-08T18:00",
                "2026-03-08T19:00",
                "2026-03-08T20:00",
                "2026-03-08T21:00",
                "2026-03-08T22:00",
            ],
            "temperature_2m": [2.8, 3.5, 3.9, 4.1, 4.1, 4.2, 4.3, 4.3, 4.4, 4.4],
            "temperature_850hPa": [0.5] * 10,
            "wind_speed_850hPa": [15.0] * 10,
            "wind_direction_850hPa": [240.0] * 10,
            "cloud_cover_low": [0.0] * 10,
            "pressure_msl": [1018.0] * 10,
        }
        metar_diag = {
            "station_icao": "RKSI",
            "latest_report_local": "2026-03-08T16:30:00+09:00",
            "observed_max_time_local": "2026-03-08T14:30:00+09:00",
            "observed_max_temp_c": 4.0,
            "latest_temp": 3.9,
            "temp_trend_1step_c": 0.0,
            "temp_trend_smooth_c": 0.0,
            "latest_cloud_code": "CAVOK",
            "cloud_trend": "云层级别稳定（CAVOK）",
            "latest_precip_state": "none",
            "precip_trend": "none",
        }
        temp_shape_analysis = analyze_temperature_shape(
            hourly_day,
            metar_diag=metar_diag,
            station_icao="RKSI",
        )

        resolved = resolve_temperature_window(
            primary_window,
            hourly_day,
            metar_diag,
            station_icao="RKSI",
            temp_shape_analysis=temp_shape_analysis,
        )

        self.assertTrue(resolved["override_active"])
        self.assertEqual(resolved["mode"], "obs_plateau_reanchor")
        self.assertIn("obs_hold_near_peak", resolved["reason_codes"])
        self.assertEqual(resolved["resolved_window"]["peak_local"], "2026-03-08T14:30")
        self.assertEqual(resolved["resolved_window"]["start_local"], "2026-03-08T14:00")
        self.assertEqual(resolved["resolved_window"]["end_local"], "2026-03-08T17:30")


if __name__ == "__main__":
    unittest.main()
