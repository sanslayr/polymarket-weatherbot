import sys
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
SCRIPTS = ROOT / "scripts"
if str(SCRIPTS) not in sys.path:
    sys.path.insert(0, str(SCRIPTS))

from regime_detector import detect_station_regimes  # noqa: E402


class RegimeDetectorTest(unittest.TestCase):
    def test_ltac_sunny_highland_dry_mix_activates_on_clear_dry_weak_wind_day(self) -> None:
        raw_state = {
            "observations": {
                "latest_report_local": "2026-03-09T11:30:00+00:00",
                "latest_temp_c": 19.6,
                "latest_dewpoint_c": 6.2,
                "latest_rh": 41.0,
                "latest_wspd_kt": 3.0,
                "latest_cloud_code": "FEW",
                "precip_state": "none",
                "radiation_eff": 0.86,
                "temp_trend_effective_c": 0.24,
            },
            "forecast": {"meta": {"station": "LTAC"}},
            "window": {
                "primary": {"peak_local": "2026-03-09T14:00"},
                "calc": {"peak_local": "2026-03-09T14:00"},
            },
        }

        result = detect_station_regimes(raw_state)

        self.assertEqual(result["station"], "LTAC")
        self.assertEqual(result["active_regimes"][0]["id"], "sunny_highland_dry_mix")
        self.assertEqual(result["active_regimes"][0]["confidence"], "high")
        self.assertGreater(result["active_regimes"][0]["posterior_effect"]["median_shift_c"], 0.0)

    def test_ltac_regime_stays_inactive_when_cloudier_windier_and_precipitating(self) -> None:
        raw_state = {
            "observations": {
                "latest_report_local": "2026-03-09T11:30:00+00:00",
                "latest_temp_c": 16.4,
                "latest_dewpoint_c": 11.8,
                "latest_rh": 76.0,
                "latest_wspd_kt": 12.0,
                "latest_cloud_code": "BKN",
                "precip_state": "light",
                "radiation_eff": 0.42,
                "temp_trend_effective_c": -0.06,
            },
            "forecast": {"meta": {"station": "LTAC"}},
            "window": {
                "primary": {"peak_local": "2026-03-09T14:00"},
                "calc": {"peak_local": "2026-03-09T14:00"},
            },
        }

        result = detect_station_regimes(raw_state)

        self.assertEqual(result["active_regimes"], [])


if __name__ == "__main__":
    unittest.main()
