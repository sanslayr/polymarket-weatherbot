import sys
import unittest
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import Mock, patch


ROOT = Path(__file__).resolve().parents[1]
SCRIPTS = ROOT / "scripts"
if str(SCRIPTS) not in sys.path:
    sys.path.insert(0, str(SCRIPTS))

from metar_analysis_service import metar_observation_block  # noqa: E402
from metar_utils import extract_observed_max_for_local_day, fetch_metar_24h, is_routine_metar_report, metar_obs_time_utc, metar_raw_ob_time_utc  # noqa: E402


class MetarObservationServiceTest(unittest.TestCase):
    def test_prefers_raw_ob_issue_time_over_report_time(self) -> None:
        metar = {
            "rawOb": "METAR LTAC 260720Z VRB03KT 9999 SCT035 02/M07 Q1016 NOSIG",
            "reportTime": "2026-02-26T07:00:00.000Z",
        }
        self.assertEqual(
            metar_obs_time_utc(metar),
            datetime(2026, 2, 26, 7, 20, tzinfo=timezone.utc),
        )

    def test_handles_month_rollover_using_nearest_reference_month(self) -> None:
        metar = {
            "rawOb": "METAR KJFK 312350Z 28011KT 10SM FEW030 06/M01 A3010 RMK AO2",
            "reportTime": "2026-02-01T00:00:00.000Z",
        }
        self.assertEqual(
            metar_raw_ob_time_utc(metar),
            datetime(2026, 1, 31, 23, 50, tzinfo=timezone.utc),
        )

    def test_detects_metar_vs_speci_from_raw_ob(self) -> None:
        self.assertTrue(is_routine_metar_report({"rawOb": "METAR LTAC 260720Z VRB03KT 9999 SCT035"}))
        self.assertFalse(is_routine_metar_report({"rawOb": "SPECI LTAC 260706Z VRB03KT 9999 SCT035"}))

    def test_fetch_metar_uses_30h_window(self) -> None:
        response = Mock()
        response.json.return_value = []
        response.raise_for_status.return_value = None
        with patch("metar_utils.runtime_cache_enabled", return_value=False), patch("metar_utils.requests.get", return_value=response) as mock_get:
            fetch_metar_24h("NZWN", force_refresh=True)
        self.assertIn("hours=30", mock_get.call_args.args[0])

    def test_extracts_observed_max_for_current_local_day(self) -> None:
        rows = [
            {"reportTime": "2026-03-10T10:30:00Z", "rawOb": "METAR TEST 101030Z", "temp": "16"},
            {"reportTime": "2026-03-10T11:00:00Z", "rawOb": "METAR TEST 101100Z", "temp": "18"},
            {"reportTime": "2026-03-10T11:30:00Z", "rawOb": "METAR TEST 101130Z", "temp": "18"},
            {"reportTime": "2026-03-09T23:30:00Z", "rawOb": "METAR TEST 092330Z", "temp": "25"},
        ]
        extracted = extract_observed_max_for_local_day(
            rows,
            "Pacific/Auckland",
            now_utc=datetime(2026, 3, 10, 12, 0, tzinfo=timezone.utc),
        )
        self.assertEqual(extracted["observed_max_temp_c"], 18.0)
        self.assertEqual(extracted["observed_max_time_local"], "2026-03-11T00:30:00+13:00")
        self.assertTrue(extracted["observed_max_temp_quantized"])

    def test_weather_phenomenon_vcsh_renders_as_vicinity_showers(self) -> None:
        metar24 = [
            {
                "reportTime": "2026-03-11T12:00:00Z",
                "rawOb": "METAR TEST 111200Z 18008KT 9999 VCSH SCT025 18/12 Q1012",
                "temp": "18",
                "dewp": "12",
                "altim": "1012",
                "wdir": 180,
                "wspd": 8,
                "wxString": "VCSH",
                "clouds": [{"cover": "SCT", "base": 2500}],
            }
        ]
        hourly_local = {
            "time": ["2026-03-11T12:00"],
            "temperature_2m": [18.0],
            "pressure_msl": [1012.0],
        }

        block, _diag = metar_observation_block(metar24, hourly_local, "UTC")

        self.assertIn("VCSH（附近阵雨）", block)

    def test_latest_obs_header_includes_local_date_when_not_current_local_day(self) -> None:
        metar24 = [
            {
                "reportTime": "2000-01-01T12:00:00Z",
                "rawOb": "METAR TEST 011200Z 18008KT 9999 SCT025 18/12 Q1012",
                "temp": "18",
                "dewp": "12",
                "altim": "1012",
                "wdir": 180,
                "wspd": 8,
                "clouds": [{"cover": "SCT", "base": 2500}],
            }
        ]
        hourly_local = {
            "time": ["2000-01-01T20:00"],
            "temperature_2m": [18.0],
            "pressure_msl": [1012.0],
        }

        block, _diag = metar_observation_block(metar24, hourly_local, "Asia/Shanghai")

        self.assertIn("**最新报：2000/01/01 20:00 Local**", block)

    def test_short_interval_reports_damp_trend_and_disable_two_step_accel(self) -> None:
        metar24 = [
            {
                "reportTime": "2026-03-11T10:20:00Z",
                "rawOb": "METAR TEST 111020Z 18008KT 9999 SCT025 9/4 Q1012",
                "temp": "9",
                "dewp": "4",
                "altim": "1012",
                "wdir": 180,
                "wspd": 8,
                "clouds": [{"cover": "SCT", "base": 2500}],
            },
            {
                "reportTime": "2026-03-11T10:50:00Z",
                "rawOb": "METAR TEST 111050Z 18008KT 9999 SCT025 10/4 Q1012",
                "temp": "10",
                "dewp": "4",
                "altim": "1012",
                "wdir": 180,
                "wspd": 8,
                "clouds": [{"cover": "SCT", "base": 2500}],
            },
            {
                "reportTime": "2026-03-11T11:00:00Z",
                "rawOb": "SPECI TEST 111100Z 18008KT 9999 SCT025 11/4 Q1012",
                "temp": "11",
                "dewp": "4",
                "altim": "1012",
                "wdir": 180,
                "wspd": 8,
                "clouds": [{"cover": "SCT", "base": 2500}],
            },
        ]
        hourly_local = {
            "time": ["2026-03-11T10:00", "2026-03-11T11:00"],
            "temperature_2m": [9.0, 11.0],
            "pressure_msl": [1012.0, 1012.0],
        }

        _block, diag = metar_observation_block(metar24, hourly_local, "UTC")

        self.assertEqual(diag["metar_recent_interval_min"], 10.0)
        self.assertEqual(diag["metar_prev_interval_min"], 30.0)
        self.assertTrue(diag["metar_speci_active"])
        self.assertIsNone(diag["temp_accel_effective_c"])
        self.assertIsNotNone(diag["temp_trend_smooth_c"])
        self.assertIsNotNone(diag["temp_trend_effective_c"])
        self.assertLess(abs(diag["temp_trend_effective_c"]), abs(diag["temp_trend_smooth_c"]))

    def test_cloud_appearance_from_cavok_is_described_as_cloud_appearance(self) -> None:
        metar24 = [
            {
                "reportTime": "2026-03-11T12:20:00Z",
                "rawOb": "METAR TEST 111220Z VRB03KT 9999 CAVOK 12/M04 Q1011",
                "temp": "12",
                "dewp": "-4",
                "altim": "1011",
                "wdir": None,
                "wspd": 3,
            },
            {
                "reportTime": "2026-03-11T12:50:00Z",
                "rawOb": "METAR TEST 111250Z VRB04KT 9999 SCT040 BKN200 12/M06 Q1012",
                "temp": "12",
                "dewp": "-6",
                "altim": "1012",
                "wdir": None,
                "wspd": 4,
                "clouds": [{"cover": "SCT", "base": 4000}, {"cover": "BKN", "base": 20000}],
            },
        ]
        hourly_local = {
            "time": ["2026-03-11T12:00"],
            "temperature_2m": [12.0],
            "pressure_msl": [1012.0],
        }

        block, _diag = metar_observation_block(metar24, hourly_local, "UTC")

        self.assertIn("云层出现", block)
        self.assertNotIn("云层重排", block)
        self.assertNotIn("上一报CAVOK", block)
        self.assertIn("较上一报 +1 hPa", block)


if __name__ == "__main__":
    unittest.main()
