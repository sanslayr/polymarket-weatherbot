import sys
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
SCRIPTS = ROOT / "scripts"
if str(SCRIPTS) not in sys.path:
    sys.path.insert(0, str(SCRIPTS))

from temperature_shape_analysis import analyze_temperature_shape  # noqa: E402


class TemperatureShapeAnalysisTest(unittest.TestCase):
    def test_detects_separated_double_peak(self) -> None:
        hourly_day = {
            "time": [
                "2026-03-08T08:00",
                "2026-03-08T09:00",
                "2026-03-08T10:00",
                "2026-03-08T11:00",
                "2026-03-08T12:00",
                "2026-03-08T13:00",
                "2026-03-08T14:00",
                "2026-03-08T15:00",
                "2026-03-08T16:00",
                "2026-03-08T17:00",
            ],
            "temperature_2m": [7.0, 8.8, 10.4, 11.2, 10.0, 9.4, 10.8, 11.4, 10.6, 9.8],
            "temperature_850hPa": [0.0] * 10,
            "wind_speed_850hPa": [18.0] * 10,
            "wind_direction_850hPa": [220.0] * 10,
            "cloud_cover_low": [25.0] * 10,
            "pressure_msl": [1012.0] * 10,
        }
        metar_diag = {
            "latest_report_local": "2026-03-08T12:30:00+00:00",
            "observed_max_time_local": "2026-03-08T11:30:00+00:00",
            "observed_max_temp_c": 11.2,
            "latest_temp": 10.0,
            "temp_trend_smooth_c": -0.4,
        }

        shape = analyze_temperature_shape(hourly_day, metar_diag=metar_diag, station_icao="TEST")

        self.assertEqual(shape["forecast"]["shape_type"], "multi_peak")
        self.assertEqual(shape["forecast"]["multi_peak_state"], "likely")
        self.assertEqual(len(shape["forecast"]["candidates"]), 2)
        self.assertEqual(shape["forecast"]["future_candidate"]["peak_local"], "2026-03-08T15:00")

    def test_detects_broad_plateau_and_sustained_hold(self) -> None:
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
            "temp_trend_smooth_c": 0.0,
        }

        shape = analyze_temperature_shape(hourly_day, metar_diag=metar_diag, station_icao="TEST")

        self.assertEqual(shape["forecast"]["shape_type"], "broad_plateau")
        self.assertEqual(shape["forecast"]["plateau_state"], "broad")
        self.assertEqual(shape["forecast"]["multi_peak_state"], "none")
        self.assertEqual(shape["observed"]["plateau_state"], "sustained")


if __name__ == "__main__":
    unittest.main()
