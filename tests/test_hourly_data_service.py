import os
import sys
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import patch


ROOT = Path(__file__).resolve().parents[1]
SCRIPTS = ROOT / "scripts"
if str(SCRIPTS) not in sys.path:
    sys.path.insert(0, str(SCRIPTS))

import hourly_data_service  # noqa: E402


class HourlyDataServiceTests(unittest.TestCase):
    def test_prune_runtime_cache_purges_old_ecmwf_open_data_files(self) -> None:
        with TemporaryDirectory() as tmp:
            cache_dir = Path(tmp)
            ecmwf_dir = cache_dir / "ecmwf_open_data"
            ecmwf_dir.mkdir(parents=True, exist_ok=True)
            old_file = ecmwf_dir / "old.grib2"
            fresh_file = ecmwf_dir / "fresh.grib2"
            old_file.write_bytes(b"old")
            fresh_file.write_bytes(b"fresh")
            old_ts = (datetime.now(timezone.utc) - timedelta(hours=30)).timestamp()
            fresh_ts = datetime.now(timezone.utc).timestamp()
            os.utime(old_file, (old_ts, old_ts))
            os.utime(fresh_file, (fresh_ts, fresh_ts))

            with patch("hourly_data_service.CACHE_DIR", cache_dir), \
                patch("hourly_data_service.PRUNE_STAMP_FILE", cache_dir / ".runtime_prune_stamp"), \
                patch("hourly_data_service.runtime_cache_enabled", return_value=True), \
                patch.dict(os.environ, {"ECMWF_OPEN_DATA_CACHE_HOURS": "24"}, clear=False):
                hourly_data_service.prune_runtime_cache(max_age_hours=24)

            self.assertFalse(old_file.exists())
            self.assertTrue(fresh_file.exists())
            self.assertTrue((cache_dir / ".runtime_prune_stamp").exists())

    def test_prune_runtime_cache_skips_when_recently_pruned(self) -> None:
        with TemporaryDirectory() as tmp:
            cache_dir = Path(tmp)
            stamp = cache_dir / ".runtime_prune_stamp"
            ecmwf_dir = cache_dir / "ecmwf_open_data"
            ecmwf_dir.mkdir(parents=True, exist_ok=True)
            old_file = ecmwf_dir / "old.grib2"
            old_file.write_bytes(b"old")
            old_ts = (datetime.now(timezone.utc) - timedelta(hours=30)).timestamp()
            os.utime(old_file, (old_ts, old_ts))
            stamp.write_text(datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"), encoding="utf-8")

            with patch("hourly_data_service.CACHE_DIR", cache_dir), \
                patch("hourly_data_service.PRUNE_STAMP_FILE", stamp), \
                patch("hourly_data_service.runtime_cache_enabled", return_value=True), \
                patch.dict(os.environ, {"ECMWF_OPEN_DATA_CACHE_HOURS": "24"}, clear=False):
                hourly_data_service.prune_runtime_cache(max_age_hours=24)

            self.assertTrue(old_file.exists())


if __name__ == "__main__":
    unittest.main()
