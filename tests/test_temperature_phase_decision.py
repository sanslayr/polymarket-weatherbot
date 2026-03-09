import sys
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
SCRIPTS = ROOT / "scripts"
if str(SCRIPTS) not in sys.path:
    sys.path.insert(0, str(SCRIPTS))

from realtime_pipeline import select_realtime_triggers  # noqa: E402
from temperature_phase_decision import build_temperature_phase_decision  # noqa: E402
from temperature_shape_analysis import analyze_temperature_shape  # noqa: E402


class TemperaturePhaseDecisionTest(unittest.TestCase):
    def test_ksea_early_peak_stays_open_for_daily_lock(self) -> None:
        primary_window = {
            "start_local": "2026-03-08T07:00",
            "peak_local": "2026-03-08T08:00",
            "end_local": "2026-03-08T09:00",
            "peak_temp_c": 10.0,
        }
        hourly_day = {
            "time": [
                "2026-03-08T07:00",
                "2026-03-08T08:00",
                "2026-03-08T09:00",
                "2026-03-08T10:00",
                "2026-03-08T11:00",
                "2026-03-08T12:00",
                "2026-03-08T13:00",
            ],
            "temperature_2m": [9.5, 10.0, 9.2, 9.0, 9.6, 9.9, 9.3],
            "temperature_850hPa": [2.0] * 7,
            "wind_speed_850hPa": [16.0] * 7,
            "wind_direction_850hPa": [220.0] * 7,
            "cloud_cover_low": [50.0, 65.0, 80.0, 70.0, 30.0, 20.0, 45.0],
            "pressure_msl": [1015.0] * 7,
        }
        metar_diag = {
            "station_icao": "KSEA",
            "latest_report_local": "2026-03-08T08:53",
            "observed_max_time_local": "2026-03-08T07:53",
            "observed_max_temp_c": 10.0,
            "latest_temp": 9.4,
            "latest_cloud_code": "OVC",
            "latest_precip_state": "light",
            "precip_trend": "steady",
            "cloud_trend": "回补",
            "temp_trend_1step_c": -0.6,
            "temp_trend_smooth_c": -0.5,
            "temp_bias_c": -0.1,
            "peak_lock_confirmed": True,
        }
        temp_shape_analysis = analyze_temperature_shape(
            hourly_day,
            metar_diag=metar_diag,
            station_icao="KSEA",
        )

        decision = build_temperature_phase_decision(
            primary_window,
            metar_diag,
            line850="冷平流窗口期内（0.72，09:00 Local）",
            temp_shape_analysis=temp_shape_analysis,
        )

        self.assertEqual(decision["phase"], "post")
        self.assertEqual(decision["daily_peak_state"], "open")
        self.assertEqual(decision["display_phase"], "early_peak_watch")
        self.assertTrue(decision["should_use_early_peak_wording"])
        self.assertIn(decision["second_peak_potential"], {"weak", "moderate", "high"})
        self.assertEqual(decision["rebound_mode"], "second_peak")
        self.assertTrue(decision["should_discuss_second_peak"])
        self.assertTrue(decision["should_discuss_multi_peak"])

        lines = select_realtime_triggers(
            primary_window,
            metar_diag,
            temp_unit="F",
            temp_phase_decision=decision,
        )
        joined = "\n".join(lines)
        self.assertNotIn("高点基本定局", joined)
        self.assertNotIn("高点大概率已定", joined)
        self.assertTrue(("早段高点已出现" in joined) or ("早峰后整理" in joined))
        self.assertIn("弱二峰", joined)

    def test_true_post_window_case_can_lock(self) -> None:
        primary_window = {
            "start_local": "2026-03-08T15:00",
            "peak_local": "2026-03-08T17:00",
            "end_local": "2026-03-08T18:00",
            "peak_temp_c": 24.0,
        }
        metar_diag = {
            "latest_report_local": "2026-03-08T18:40",
            "observed_max_time_local": "2026-03-08T17:20",
            "observed_max_temp_c": 24.2,
            "latest_temp": 23.2,
            "latest_cloud_code": "CLR",
            "latest_precip_state": "none",
            "precip_trend": "none",
            "cloud_trend": "稳定",
            "temp_trend_1step_c": -0.5,
            "temp_trend_smooth_c": -0.4,
            "temp_bias_c": -0.2,
            "peak_lock_confirmed": True,
        }

        decision = build_temperature_phase_decision(primary_window, metar_diag)

        self.assertEqual(decision["phase"], "post")
        self.assertEqual(decision["daily_peak_state"], "locked")
        self.assertFalse(decision["should_use_early_peak_wording"])

        lines = select_realtime_triggers(
            primary_window,
            metar_diag,
            temp_phase_decision=decision,
        )
        joined = "\n".join(lines)
        self.assertIn("高点大概率已定", joined)

    def test_sustained_near_peak_hold_prefers_plateau_wording(self) -> None:
        primary_window = {
            "start_local": "2026-03-08T14:00",
            "peak_local": "2026-03-08T15:00",
            "end_local": "2026-03-08T17:00",
            "peak_temp_c": 12.5,
        }
        hourly_day = {
            "time": [
                "2026-03-08T11:00",
                "2026-03-08T12:00",
                "2026-03-08T13:00",
                "2026-03-08T14:00",
                "2026-03-08T15:00",
                "2026-03-08T16:00",
                "2026-03-08T17:00",
            ],
            "temperature_2m": [8.8, 10.4, 11.7, 12.5, 12.4, 12.5, 12.4],
            "temperature_850hPa": [1.0] * 7,
            "wind_speed_850hPa": [12.0] * 7,
            "wind_direction_850hPa": [180.0] * 7,
            "cloud_cover_low": [10.0] * 7,
            "pressure_msl": [1015.0] * 7,
        }
        metar_diag = {
            "latest_report_local": "2026-03-08T16:30:00+00:00",
            "observed_max_time_local": "2026-03-08T14:30:00+00:00",
            "observed_max_temp_c": 12.5,
            "latest_temp": 12.4,
            "latest_cloud_code": "CLR",
            "latest_precip_state": "none",
            "precip_trend": "none",
            "cloud_trend": "稳定",
            "temp_trend_1step_c": 0.0,
            "temp_trend_smooth_c": 0.0,
            "temp_bias_c": 0.0,
            "peak_lock_confirmed": False,
        }
        temp_shape_analysis = analyze_temperature_shape(
            hourly_day,
            metar_diag=metar_diag,
            station_icao="TEST",
        )

        decision = build_temperature_phase_decision(
            primary_window,
            metar_diag,
            temp_shape_analysis=temp_shape_analysis,
        )

        self.assertIn(decision["daily_peak_state"], {"lean_locked", "locked"})
        self.assertEqual(decision["plateau_hold_state"], "sustained")
        self.assertEqual(decision["rebound_mode"], "none")
        self.assertEqual(decision["dominant_shape"], "peak_plateau")
        self.assertFalse(decision["should_discuss_second_peak"])
        self.assertTrue(decision["should_prefer_plateau_wording"])

        lines = select_realtime_triggers(
            primary_window,
            metar_diag,
            temp_phase_decision=decision,
        )
        joined = "\n".join(lines)
        self.assertNotIn("独立二峰", joined)


if __name__ == "__main__":
    unittest.main()
