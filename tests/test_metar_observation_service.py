import sys
import unittest
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import Mock, patch


ROOT = Path(__file__).resolve().parents[1]
SCRIPTS = ROOT / "scripts"
if str(SCRIPTS) not in sys.path:
    sys.path.insert(0, str(SCRIPTS))

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


if __name__ == "__main__":
    unittest.main()
