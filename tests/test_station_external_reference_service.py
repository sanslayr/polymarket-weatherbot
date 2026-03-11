import sys
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
SCRIPTS = ROOT / "scripts"
if str(SCRIPTS) not in sys.path:
    sys.path.insert(0, str(SCRIPTS))

from station_catalog import Station  # noqa: E402
from station_external_reference_service import (  # noqa: E402
    fetch_station_external_reference,
    render_station_external_reference_line,
)


class StationExternalReferenceServiceTest(unittest.TestCase):
    def test_non_ltac_station_has_no_external_reference(self) -> None:
        station = Station(city="Tokyo", icao="RJTT", lat=35.55, lon=139.78)
        self.assertIsNone(fetch_station_external_reference(station))

    def test_render_mgm_reference_line_in_celsius(self) -> None:
        line = render_station_external_reference_line(
            {
                "source": "mgm",
                "veri_zamani": "2026-03-11T12:20:00+03:00",
                "temp_c": 12.4,
                "rh": 61,
                "wind_dir": 225,
                "wind_kmh": 17.2,
            },
            "C",
        )
        self.assertIn("- MGM参考（12:20 Local）", line)
        self.assertIn("气温=12.4°C", line)
        self.assertIn("湿度=61%", line)
        self.assertIn("风向=西南风（225°）", line)
        self.assertIn("风速=17.2km/h", line)

    def test_render_mgm_reference_line_in_fahrenheit(self) -> None:
        line = render_station_external_reference_line(
            {
                "source": "mgm",
                "veri_zamani": "2026-03-11T12:20:00+03:00",
                "temp_c": 12.0,
            },
            "F",
        )
        self.assertIn("气温=53.6°F", line)

    def test_render_unknown_reference_source_returns_empty_line(self) -> None:
        self.assertEqual(render_station_external_reference_line({"source": "other"}, "C"), "")


if __name__ == "__main__":
    unittest.main()
