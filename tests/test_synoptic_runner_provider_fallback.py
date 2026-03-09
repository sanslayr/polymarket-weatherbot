import sys
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch


ROOT = Path(__file__).resolve().parents[1]
SCRIPTS = ROOT / "scripts"
if str(SCRIPTS) not in sys.path:
    sys.path.insert(0, str(SCRIPTS))

import synoptic_runner  # noqa: E402


class SynopticRunnerProviderFallbackTest(unittest.TestCase):
    def test_runner_falls_back_from_ecmwf_to_gfs(self) -> None:
        station = SimpleNamespace(icao="TEST", lat=10.0, lon=20.0)

        def fake_build(provider, **kwargs):
            if provider == "ecmwf-open-data":
                raise RuntimeError("ecmwf-opendata package missing")
            return {
                "analysis_time_utc": "2026-03-09T12:00:00Z",
                "analysis_time_local": "2026-03-09T12:00",
                "station": {"icao": "TEST", "lat": 10.0, "lon": 20.0},
                "lat": [9.0, 10.0, 11.0],
                "lon": [19.0, 20.0, 21.0],
                "fields": {
                    "mslp_hpa": [[1010.0] * 3] * 3,
                    "z500_gpm": [[5880.0] * 3] * 3,
                    "t850_c": [[15.0] * 3] * 3,
                    "u850_ms": [[5.0] * 3] * 3,
                    "v850_ms": [[1.0] * 3] * 3,
                },
                "previous_fields": {
                    "mslp_hpa": [[1011.0] * 3] * 3,
                    "z500_gpm": [[5870.0] * 3] * 3,
                },
            }

        def fake_analyze(payload, mode="full"):
            return {
                "analysis_time_utc": payload["analysis_time_utc"],
                "analysis_time_local": payload["analysis_time_local"],
                "scale_summary": {
                    "synoptic": {
                        "systems": [
                            {
                                "level": "850",
                                "system_type": "warm_advection_band",
                                "center_lat": 12.0,
                                "center_lon": 20.0,
                                "distance_to_station_km": 220.0,
                            }
                        ]
                    }
                },
            }

        with tempfile.TemporaryDirectory() as tmp:
            cache_dir = Path(tmp) / "cache"
            scripts_dir = ROOT / "scripts"
            with patch.object(synoptic_runner, "build_synoptic_grid_payload", side_effect=fake_build), patch.object(synoptic_runner, "analyze_synoptic_2d", side_effect=fake_analyze):
                payload = synoptic_runner.run_synoptic_section(
                    st=station,
                    target_date="2026-03-09",
                    peak_local="2026-03-09T12:00",
                    tz_name="UTC",
                    model="ecmwf",
                    runtime_tag="2026030918Z",
                    scripts_dir=scripts_dir,
                    cache_dir=cache_dir,
                    provider="ecmwf-open-data",
                    pass_mode="inner_only",
                )

        self.assertEqual(payload["_provider_requested"], "ecmwf-open-data")
        self.assertEqual(payload["_provider_used"], "gfs-grib2")
        telemetry = dict(payload.get("_telemetry") or {})
        self.assertTrue(telemetry.get("provider_fallback"))
        self.assertEqual(telemetry.get("provider_used"), "gfs-grib2")


if __name__ == "__main__":
    unittest.main()
