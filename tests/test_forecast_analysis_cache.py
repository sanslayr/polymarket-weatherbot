import json
import sys
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import patch


ROOT = Path(__file__).resolve().parents[1]
SCRIPTS = ROOT / "scripts"
if str(SCRIPTS) not in sys.path:
    sys.path.insert(0, str(SCRIPTS))

import forecast_analysis_cache  # noqa: E402


class ForecastAnalysisCacheTests(unittest.TestCase):
    def test_read_cached_forecast_analysis_reuses_ensemble_when_live_snapshot_is_stale(self) -> None:
        payload = {
            "schema_version": forecast_analysis_cache.SCHEMA_VERSION,
            "station": "ZSPD",
            "target_date": "2026-03-14",
            "model": "ecmwf",
            "synoptic_provider": "gfs-grib2",
            "runtime_tag": "2026031300Z",
            "latest_report_local": "2026-03-13T08:00:00+08:00",
            "analysis_peak_local": "2026-03-14T14:00",
            "ensemble_factor": {"summary": {"dominant_path": "neutral_stable"}},
            "analysis_snapshot": {"schema_version": "snapshot.v1"},
        }
        with TemporaryDirectory() as tmp:
            cache_dir = Path(tmp)
            with patch("forecast_analysis_cache.CACHE_DIR", cache_dir), patch(
                "forecast_analysis_cache.runtime_cache_enabled",
                return_value=True,
            ):
                forecast_analysis_cache.write_cached_forecast_analysis(
                    payload,
                    station_icao="ZSPD",
                    target_date="2026-03-14",
                    model="ecmwf",
                    synoptic_provider="gfs-grib2",
                    runtime_tag="2026031300Z",
                )
                cached = forecast_analysis_cache.read_cached_forecast_analysis(
                    station_icao="ZSPD",
                    target_date="2026-03-14",
                    model="ecmwf",
                    synoptic_provider="gfs-grib2",
                    runtime_tag="2026031300Z",
                    latest_report_local="2026-03-13T08:30:00+08:00",
                    analysis_peak_local="2026-03-14T14:00",
                )

        self.assertIsNotNone(cached)
        self.assertEqual((cached or {}).get("ensemble_factor", {}).get("summary", {}).get("dominant_path"), "neutral_stable")
        self.assertFalse(bool((cached or {}).get("analysis_snapshot_fresh")))
        self.assertEqual((cached or {}).get("analysis_snapshot"), {})

    def test_build_and_cache_forecast_analysis_preserves_member_level_ensemble_payload(self) -> None:
        ensemble_factor = {
            "schema_version": "ecmwf-ensemble-factor.v2",
            "member_count": 2,
            "summary": {"dominant_path": "warm_support"},
            "probabilities": {"warm_support": 0.7, "transition": 0.3},
            "detail_probabilities": {"warm_support": 0.7},
            "diagnostics": {"delta_t850_p50_c": 0.9},
            "source": {"runtime_used": "2026031300Z"},
            "selection": {"station": "ZSPD"},
            "members": [{"number": 0, "t850_c": 6.2}, {"number": 1, "t850_c": 6.8}],
        }
        analysis_snapshot = {
            "schema_version": "snapshot.v1",
            "ensemble_factor": ensemble_factor,
            "canonical_raw_state": {
                "forecast": {
                    "ensemble_factor": ensemble_factor,
                }
            },
        }

        with TemporaryDirectory() as tmp:
            cache_dir = Path(tmp)
            with patch("forecast_analysis_cache.CACHE_DIR", cache_dir), patch(
                "forecast_analysis_cache.runtime_cache_enabled",
                return_value=True,
            ), patch(
                "forecast_analysis_cache.build_ecmwf_ensemble_factor",
                return_value=ensemble_factor,
            ), patch(
                "forecast_analysis_cache.build_analysis_snapshot",
                return_value=analysis_snapshot,
            ):
                payload = forecast_analysis_cache.build_and_cache_forecast_analysis(
                    station_icao="ZSPD",
                    station_lat=31.14,
                    station_lon=121.80,
                    target_date="2026-03-14",
                    model="ecmwf",
                    synoptic_provider="gfs-grib2",
                    runtime_tag="2026031300Z",
                    primary_window={"peak_local": "2026-03-14T14:00"},
                    synoptic_window={"peak_local": "2026-03-14T14:00"},
                    metar_diag={"latest_report_local": "2026-03-13T08:00:00+08:00"},
                    forecast_decision={"meta": {"runtime": "2026031300Z"}},
                    temp_shape_analysis=None,
                    temp_unit="C",
                    tz_name="Asia/Shanghai",
                )

                cache_files = list(cache_dir.glob("forecast_analysis_*.json"))
                self.assertEqual(len(cache_files), 1)
                stored = json.loads(cache_files[0].read_text(encoding="utf-8"))

        self.assertEqual(len(payload["ensemble_factor"]["members"]), 2)
        self.assertEqual(len(payload["analysis_snapshot"]["ensemble_factor"]["members"]), 2)
        self.assertEqual(
            len(payload["analysis_snapshot"]["canonical_raw_state"]["forecast"]["ensemble_factor"]["members"]),
            2,
        )
        stored_payload = stored["payload"]
        self.assertEqual(len(stored_payload["ensemble_factor"]["members"]), 2)
        self.assertEqual(len(stored_payload["analysis_snapshot"]["ensemble_factor"]["members"]), 2)

    def test_read_cached_forecast_analysis_rebuilds_when_snapshot_schema_is_outdated(self) -> None:
        payload = {
            "schema_version": forecast_analysis_cache.SCHEMA_VERSION,
            "station": "LTAC",
            "target_date": "2026-03-14",
            "model": "ecmwf",
            "synoptic_provider": "ecmwf-open-data",
            "runtime_tag": "2026031400Z",
            "latest_report_local": "2026-03-14T11:50:00+03:00",
            "analysis_peak_local": "2026-03-14T16:00",
            "ensemble_factor": {"summary": {"dominant_path": "warm_support"}},
            "analysis_snapshot": {"schema_version": "analysis-snapshot.v7"},
        }
        with TemporaryDirectory() as tmp:
            cache_dir = Path(tmp)
            with patch("forecast_analysis_cache.CACHE_DIR", cache_dir), patch(
                "forecast_analysis_cache.runtime_cache_enabled",
                return_value=True,
            ):
                forecast_analysis_cache.write_cached_forecast_analysis(
                    payload,
                    station_icao="LTAC",
                    target_date="2026-03-14",
                    model="ecmwf",
                    synoptic_provider="ecmwf-open-data",
                    runtime_tag="2026031400Z",
                )
                cached = forecast_analysis_cache.read_cached_forecast_analysis(
                    station_icao="LTAC",
                    target_date="2026-03-14",
                    model="ecmwf",
                    synoptic_provider="ecmwf-open-data",
                    runtime_tag="2026031400Z",
                    latest_report_local="2026-03-14T11:50:00+03:00",
                    analysis_peak_local="2026-03-14T16:00",
                )

        self.assertIsNotNone(cached)
        self.assertFalse(bool((cached or {}).get("analysis_snapshot_fresh")))
        self.assertEqual((cached or {}).get("analysis_snapshot"), {})


if __name__ == "__main__":
    unittest.main()
