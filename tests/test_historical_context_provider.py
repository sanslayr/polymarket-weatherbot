import csv
import gzip
import os
import sys
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import patch


ROOT = Path(__file__).resolve().parents[1]
SCRIPTS = ROOT / "scripts"
if str(SCRIPTS) not in sys.path:
    sys.path.insert(0, str(SCRIPTS))

import historical_context_provider  # noqa: E402


class HistoricalContextProviderTests(unittest.TestCase):
    def test_build_historical_context_defaults_to_profile_only_mode(self) -> None:
        with patch.dict(os.environ, {"LOOK_ENABLE_HISTORICAL_HOURLY_MATCHING": "0"}):
            context = historical_context_provider.build_historical_context(
                "RJTT",
                "2026-03-11",
                {
                    "latest_temp": 7.0,
                    "latest_dewpoint": -3.0,
                    "latest_rh_pct": 49.0,
                    "latest_wdir": 40.0,
                    "latest_wspd": 4.0,
                    "latest_report_local": "2026-03-11T11:00:00+09:00",
                    "latest_cloud_code": "BKN",
                },
                forecast_decision={"decision": {"main_path": "云量压制路径"}},
                factor_summary="东南侧临水·主城南侧(近郊)",
                site_tag="湾岸填海机场",
            )

        self.assertTrue(context.get("available"))
        self.assertEqual(context.get("mode"), "profile_only")
        self.assertEqual(context.get("analogs"), [])
        self.assertIsNone(context.get("weighted_reference"))
        summary_lines = [str(item) for item in (context.get("summary_lines") or [])]
        self.assertTrue(any("站点背景摘要：" in line for line in summary_lines))

    def test_station_hourly_index_uses_disk_cache_across_cache_clear(self) -> None:
        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            raw_root = root / "raw"
            cache_root = root / "cache"
            station_dir = raw_root / "RJTT"
            station_dir.mkdir(parents=True, exist_ok=True)
            sample_path = station_dir / "202603.csv.gz"
            fieldnames = [
                "DATE",
                "TMP",
                "DEW",
                "WND",
                "VIS",
                "CIG",
                "GA1",
                "GA2",
                "GA3",
                "AA1",
                "AA2",
                "AA3",
                "AA4",
            ]
            with gzip.open(sample_path, "wt", encoding="utf-8", newline="") as handle:
                writer = csv.DictWriter(handle, fieldnames=fieldnames)
                writer.writeheader()
                writer.writerow(
                    {
                        "DATE": "2026-03-11T00:00:00+00:00",
                        "TMP": "+0123,1",
                        "DEW": "+0045,1",
                        "WND": "090,1,N,0050,1",
                        "VIS": "16000,1,N,1",
                        "CIG": "01000,1,N",
                        "GA1": "02,1,01000,1,1,1",
                        "GA2": "",
                        "GA3": "",
                        "AA1": "",
                        "AA2": "",
                        "AA3": "",
                        "AA4": "",
                    }
                )

            historical_context_provider._load_station_hourly_index.cache_clear()
            with patch.object(historical_context_provider, "STATION_HOURLY_INDEX_CACHE_DIR", cache_root), \
                patch.object(historical_context_provider, "archive_raw_dir", return_value=raw_root), \
                patch.object(historical_context_provider, "_station_timezone", return_value="UTC"):
                first = historical_context_provider._load_station_hourly_index("RJTT")
                self.assertTrue(first)
                self.assertTrue((cache_root / "RJTT.pkl").exists())

                historical_context_provider._load_station_hourly_index.cache_clear()
                with patch.object(historical_context_provider.gzip, "open", side_effect=RuntimeError("should not read raw gzip")):
                    second = historical_context_provider._load_station_hourly_index("RJTT")

            self.assertEqual(first, second)


if __name__ == "__main__":
    unittest.main()
