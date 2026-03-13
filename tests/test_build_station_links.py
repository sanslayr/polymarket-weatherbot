import sys
import unittest
from datetime import datetime, timezone
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
SCRIPTS = ROOT / "scripts"
if str(SCRIPTS) not in sys.path:
    sys.path.insert(0, str(SCRIPTS))

import build_station_links as BSL  # noqa: E402


class BuildStationLinksTest(unittest.TestCase):
    def test_build_links_routes_seattle_to_wpc_northwest_chart(self) -> None:
        row = BSL.load_station(ROOT / "station_links.csv", "KSEA")

        payload = BSL.build_links(
            row=row,
            model="ecmwf",
            now_utc=datetime(2026, 3, 11, 12, 0, tzinfo=timezone.utc),
            target_valid_utc=datetime(2026, 3, 11, 18, 0, tzinfo=timezone.utc),
            target_date_utc=datetime(2026, 3, 11, 0, 0, tzinfo=timezone.utc),
        )

        self.assertEqual(payload["links"]["weather_map_label"], "WPC West North")
        self.assertEqual(
            payload["links"]["weather_map"],
            "https://www.wpc.ncep.noaa.gov/sfc/namnwsfcwbg.gif",
        )

    def test_build_links_routes_hong_kong_to_hko_weather_chart(self) -> None:
        row = BSL.load_station(ROOT / "station_links.csv", "VHHH")

        payload = BSL.build_links(
            row=row,
            model="ecmwf",
            now_utc=datetime(2026, 3, 11, 12, 0, tzinfo=timezone.utc),
            target_valid_utc=datetime(2026, 3, 11, 18, 0, tzinfo=timezone.utc),
            target_date_utc=datetime(2026, 3, 11, 0, 0, tzinfo=timezone.utc),
        )

        self.assertEqual(payload["links"]["weather_map_label"], "HKO")
        self.assertEqual(
            payload["links"]["weather_map"],
            "https://www.weather.gov.hk/en/wxinfo/currwx/wxcht.htm",
        )

    def test_build_links_routes_shanghai_to_nmc_weather_map(self) -> None:
        row = BSL.load_station(ROOT / "station_links.csv", "ZSPD")

        payload = BSL.build_links(
            row=row,
            model="ecmwf",
            now_utc=datetime(2026, 3, 11, 12, 0, tzinfo=timezone.utc),
            target_valid_utc=datetime(2026, 3, 11, 18, 0, tzinfo=timezone.utc),
            target_date_utc=datetime(2026, 3, 11, 0, 0, tzinfo=timezone.utc),
        )

        self.assertEqual(payload["links"]["weather_map_label"], "NMC")
        self.assertEqual(
            payload["links"]["weather_map"],
            "http://nmc.cn/publish/observations/china/dm/weatherchart-h000.htm",
        )


if __name__ == "__main__":
    unittest.main()
