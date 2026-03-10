import sys
import unittest
from datetime import datetime, timezone
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
SCRIPTS = ROOT / "scripts"
if str(SCRIPTS) not in sys.path:
    sys.path.insert(0, str(SCRIPTS))

from metar_utils import is_routine_metar_report, metar_obs_time_utc, metar_raw_ob_time_utc  # noqa: E402


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


if __name__ == "__main__":
    unittest.main()
