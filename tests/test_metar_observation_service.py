import sys
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
SCRIPTS = ROOT / "scripts"
if str(SCRIPTS) not in sys.path:
    sys.path.insert(0, str(SCRIPTS))

from metar_analysis_service import metar_observation_block  # noqa: E402


class MetarObservationServiceTest(unittest.TestCase):
    def test_stable_conditions_do_not_emit_realtime_reminder(self) -> None:
        hourly_local = {
            "time": ["2026-03-09T08:00", "2026-03-09T09:00", "2026-03-09T10:00"],
            "temperature_2m": [7.0, 7.0, 7.2],
            "pressure_msl": [1020.0, 1020.0, 1019.8],
        }
        metar24 = [
            {
                "reportTime": "2026-03-09T07:50:00Z",
                "rawOb": "EGLC 090750Z 19003KT OVC001 07/07 Q1020",
                "temp": 7.0,
                "dewp": 7.0,
                "altim": 1020.0,
                "wdir": 190,
                "wspd": 3,
                "wxString": "",
                "clouds": [{"cover": "OVC", "base": 100}],
            },
            {
                "reportTime": "2026-03-09T08:20:00Z",
                "rawOb": "EGLC 090820Z 19003KT OVC001 07/07 Q1020",
                "temp": 7.0,
                "dewp": 7.0,
                "altim": 1020.0,
                "wdir": 190,
                "wspd": 3,
                "wxString": "",
                "clouds": [{"cover": "OVC", "base": 100}],
            },
            {
                "reportTime": "2026-03-09T08:50:00Z",
                "rawOb": "EGLC 090850Z VRB03KT OVC001 07/07 Q1020",
                "temp": 7.0,
                "dewp": 7.0,
                "altim": 1020.0,
                "wdir": "VRB",
                "wspd": 3,
                "wxString": "",
                "clouds": [{"cover": "OVC", "base": 100}],
            },
        ]

        block, _diag = metar_observation_block(metar24, hourly_local, "Etc/UTC", target_date="2026-03-09")
        self.assertNotIn("最近两小时实况趋势", block)
        self.assertNotIn("实况提醒", block)

    def test_meaningful_shift_emits_realtime_reminder(self) -> None:
        hourly_local = {
            "time": ["2026-03-09T08:00", "2026-03-09T09:00", "2026-03-09T10:00"],
            "temperature_2m": [7.0, 7.5, 8.2],
            "pressure_msl": [1020.0, 1019.2, 1018.8],
        }
        metar24 = [
            {
                "reportTime": "2026-03-09T07:50:00Z",
                "rawOb": "EGLC 090750Z 18004KT OVC004 07/06 Q1021",
                "temp": 7.0,
                "dewp": 6.0,
                "altim": 1021.0,
                "wdir": 180,
                "wspd": 4,
                "wxString": "",
                "clouds": [{"cover": "OVC", "base": 400}],
            },
            {
                "reportTime": "2026-03-09T08:20:00Z",
                "rawOb": "EGLC 090820Z 20005KT BKN003 08/06 Q1020",
                "temp": 8.0,
                "dewp": 6.0,
                "altim": 1020.0,
                "wdir": 200,
                "wspd": 5,
                "wxString": "",
                "clouds": [{"cover": "BKN", "base": 300}],
            },
            {
                "reportTime": "2026-03-09T08:50:00Z",
                "rawOb": "EGLC 090850Z 24007KT SCT020 09/06 Q1019",
                "temp": 9.0,
                "dewp": 6.0,
                "altim": 1019.0,
                "wdir": 240,
                "wspd": 7,
                "wxString": "",
                "clouds": [{"cover": "SCT", "base": 2000}],
            },
        ]

        block, _diag = metar_observation_block(metar24, hourly_local, "Etc/UTC", target_date="2026-03-09")
        self.assertIn("实况提醒", block)
        self.assertTrue(("短时升温仍在延续" in block) or ("云量在转疏" in block) or ("风向正在重排" in block))


if __name__ == "__main__":
    unittest.main()
