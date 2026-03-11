import importlib
import os
import sys
import unittest
from datetime import datetime, timezone
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
SCRIPTS = ROOT / "scripts"
if str(SCRIPTS) not in sys.path:
    sys.path.insert(0, str(SCRIPTS))


class LookChangeGuardTests(unittest.TestCase):
    def setUp(self) -> None:
        self._saved = {
            "LOOK_FORCE_LIVE_METAR": os.environ.get("LOOK_FORCE_LIVE_METAR"),
            "LOOK_FORCE_LIVE_POLYMARKET": os.environ.get("LOOK_FORCE_LIVE_POLYMARKET"),
        }

    def tearDown(self) -> None:
        for key, value in self._saved.items():
            if value is None:
                os.environ.pop(key, None)
            else:
                os.environ[key] = value

    def _reload_module(self, *, live_metar: str, live_polymarket: str):
        os.environ["LOOK_FORCE_LIVE_METAR"] = live_metar
        os.environ["LOOK_FORCE_LIVE_POLYMARKET"] = live_polymarket
        import look_change_guard

        return importlib.reload(look_change_guard)

    def _cached_payload(self, module, *, age_seconds: int = 30) -> dict:
        return {
            "updated_at": datetime.now(timezone.utc).timestamp() - age_seconds,
            "text": "cached report",
            "result_meta": {
                "report_version": module.REPORT_RESULT_VERSION,
                "forecast_signature": module._forecast_signature("ecmwf"),
                "metar_signature": {
                    "raw_ob": "LTAC 110850Z VRB02KT CAVOK 10/M11 Q1025",
                    "obs_time_utc": "2026-03-11T08:50:00Z",
                },
            },
        }

    def test_build_unchanged_notice_is_disabled_when_live_refresh_is_forced(self) -> None:
        module = self._reload_module(live_metar="1", live_polymarket="1")

        notice = module.build_unchanged_notice(
            query_label="Ankara(LTAC)-20260311",
            icao="LTAC",
            model="ecmwf",
            cached_payload=self._cached_payload(module),
        )

        self.assertIsNone(notice)

    def test_build_unchanged_notice_still_uses_reuse_window_when_live_refresh_is_disabled(self) -> None:
        module = self._reload_module(live_metar="0", live_polymarket="0")

        notice = module.build_unchanged_notice(
            query_label="Ankara(LTAC)-20260311",
            icao="LTAC",
            model="ecmwf",
            cached_payload=self._cached_payload(module),
        )

        self.assertIsNotNone(notice)
        self.assertIn("复用窗口", str(notice))


if __name__ == "__main__":
    unittest.main()
