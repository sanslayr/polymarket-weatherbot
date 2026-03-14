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


def _surface_ready_ensemble(dominant_path: str = "warm_support", number: int = 0) -> dict[str, object]:
    return {
        "schema_version": "ecmwf-ensemble-factor.v6",
        "summary": {"dominant_path": dominant_path},
        "source": {"detail_level": "surface_anchor"},
        "members": [{"number": number, "path_label": dominant_path, "t2m_c": 13.2, "msl_hpa": 1009.4}],
    }


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
            "ensemble_factor": _surface_ready_ensemble("transition"),
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
        self.assertEqual((cached or {}).get("ensemble_factor", {}).get("summary", {}).get("dominant_path"), "transition")
        self.assertFalse(bool((cached or {}).get("analysis_snapshot_fresh")))
        self.assertEqual((cached or {}).get("analysis_snapshot"), {})

    def test_build_and_cache_forecast_analysis_preserves_member_level_ensemble_payload(self) -> None:
        ensemble_factor = {
            "schema_version": "ecmwf-ensemble-factor.v5",
            "member_count": 2,
            "summary": {"dominant_path": "warm_support"},
            "probabilities": {"warm_support": 0.7, "transition": 0.3},
            "detail_probabilities": {"warm_support": 0.7},
            "diagnostics": {"delta_t850_p50_c": 0.9},
            "member_trajectory": {"members": [{"number": 0, "next3h_t2m_delta_c": 0.4, "t2m_current_c": 13.1}]},
            "source": {"runtime_used": "2026031300Z", "detail_level": "surface_trajectory"},
            "selection": {"station": "ZSPD"},
            "members": [{"number": 0, "t2m_c": 13.2}, {"number": 1, "t2m_c": 13.8}],
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
        self.assertEqual(payload["ensemble_factor"]["member_trajectory"]["members"][0]["next3h_t2m_delta_c"], 0.4)
        self.assertEqual(len(payload["analysis_snapshot"]["ensemble_factor"]["members"]), 2)
        self.assertEqual(
            len(payload["analysis_snapshot"]["canonical_raw_state"]["forecast"]["ensemble_factor"]["members"]),
            2,
        )
        stored_payload = stored["payload"]
        self.assertEqual(len(stored_payload["ensemble_factor"]["members"]), 2)
        self.assertEqual(len(stored_payload["analysis_snapshot"]["ensemble_factor"]["members"]), 2)
        self.assertEqual(stored_payload["ensemble_factor"]["member_trajectory"]["members"][0]["next3h_t2m_delta_c"], 0.4)

    def test_build_and_cache_forecast_analysis_reuses_cached_ensemble_when_gate_is_closed(self) -> None:
        cached_payload = {
            "schema_version": forecast_analysis_cache.SCHEMA_VERSION,
            "station": "LTAC",
            "target_date": "2026-03-14",
            "model": "ecmwf",
            "synoptic_provider": "ecmwf-open-data",
            "runtime_tag": "2026031400Z",
            "latest_report_local": "2026-03-14T13:50:00+03:00",
            "analysis_peak_local": "2026-03-14T16:00",
            "ensemble_factor": {
                **_surface_ready_ensemble("warm_support", 1),
            },
            "analysis_snapshot": {"schema_version": forecast_analysis_cache.ANALYSIS_SNAPSHOT_SCHEMA_VERSION},
        }

        with TemporaryDirectory() as tmp:
            cache_dir = Path(tmp)
            with patch("forecast_analysis_cache.CACHE_DIR", cache_dir), patch(
                "forecast_analysis_cache.runtime_cache_enabled",
                return_value=True,
            ), patch(
                "forecast_analysis_cache.should_build_ecmwf_ensemble_factor",
                return_value=False,
            ), patch(
                "forecast_analysis_cache.build_analysis_snapshot",
                return_value={"schema_version": forecast_analysis_cache.ANALYSIS_SNAPSHOT_SCHEMA_VERSION},
            ):
                forecast_analysis_cache.write_cached_forecast_analysis(
                    cached_payload,
                    station_icao="LTAC",
                    target_date="2026-03-14",
                    model="ecmwf",
                    synoptic_provider="ecmwf-open-data",
                    runtime_tag="2026031400Z",
                )
                payload = forecast_analysis_cache.build_and_cache_forecast_analysis(
                    station_icao="LTAC",
                    station_lat=40.12,
                    station_lon=32.99,
                    target_date="2026-03-14",
                    model="ecmwf",
                    synoptic_provider="ecmwf-open-data",
                    runtime_tag="2026031400Z",
                    primary_window={"peak_local": "2026-03-14T16:00"},
                    synoptic_window={"peak_local": "2026-03-14T15:00"},
                    metar_diag={"latest_report_local": "2026-03-14T14:50:00+03:00"},
                    forecast_decision={"meta": {"runtime": "2026031400Z"}},
                    temp_shape_analysis=None,
                    temp_unit="C",
                    tz_name="Europe/Istanbul",
                )

        self.assertEqual(payload["ensemble_factor"]["summary"]["dominant_path"], "warm_support")
        self.assertEqual(payload["ensemble_factor"]["members"][0]["number"], 1)

    def test_read_cached_forecast_analysis_can_backfill_ensemble_from_older_runtime(self) -> None:
        empty_current_payload = {
            "schema_version": forecast_analysis_cache.SCHEMA_VERSION,
            "station": "LTAC",
            "target_date": "2026-03-14",
            "model": "ecmwf",
            "synoptic_provider": "ecmwf-open-data",
            "runtime_tag": "2026031406Z",
            "latest_report_local": "2026-03-14T14:50:00+03:00",
            "analysis_peak_local": "2026-03-14T15:00",
            "ensemble_factor": {},
            "analysis_snapshot": {"schema_version": forecast_analysis_cache.ANALYSIS_SNAPSHOT_SCHEMA_VERSION},
        }
        older_payload = {
            "schema_version": forecast_analysis_cache.SCHEMA_VERSION,
            "station": "LTAC",
            "target_date": "2026-03-14",
            "model": "ecmwf",
            "synoptic_provider": "ecmwf-open-data",
            "runtime_tag": "2026031318Z",
            "latest_report_local": "2026-03-14T13:20:00+03:00",
            "analysis_peak_local": "2026-03-14T15:00",
            "ensemble_factor": {
                **_surface_ready_ensemble("warm_support", 11),
            },
            "analysis_snapshot": {"schema_version": forecast_analysis_cache.ANALYSIS_SNAPSHOT_SCHEMA_VERSION},
        }
        with TemporaryDirectory() as tmp:
            cache_dir = Path(tmp)
            with patch("forecast_analysis_cache.CACHE_DIR", cache_dir), patch(
                "forecast_analysis_cache.runtime_cache_enabled",
                return_value=True,
            ):
                forecast_analysis_cache.write_cached_forecast_analysis(
                    empty_current_payload,
                    station_icao="LTAC",
                    target_date="2026-03-14",
                    model="ecmwf",
                    synoptic_provider="ecmwf-open-data",
                    runtime_tag="2026031406Z",
                )
                forecast_analysis_cache.write_cached_forecast_analysis(
                    older_payload,
                    station_icao="LTAC",
                    target_date="2026-03-14",
                    model="ecmwf",
                    synoptic_provider="ecmwf-open-data",
                    runtime_tag="2026031318Z",
                )
                cached = forecast_analysis_cache.read_cached_forecast_analysis(
                    station_icao="LTAC",
                    target_date="2026-03-14",
                    model="ecmwf",
                    synoptic_provider="ecmwf-open-data",
                    runtime_tag="2026031406Z",
                    latest_report_local="2026-03-14T14:50:00+03:00",
                    analysis_peak_local="2026-03-14T15:00",
                )

        self.assertIsNotNone(cached)
        self.assertEqual((cached or {}).get("ensemble_runtime_fallback"), "2026031318Z")
        self.assertEqual((cached or {}).get("ensemble_factor", {}).get("members", [])[0]["number"], 11)
        self.assertFalse(bool((cached or {}).get("analysis_snapshot_fresh")))

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
            "ensemble_factor": _surface_ready_ensemble("warm_support"),
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

    def test_read_cached_forecast_analysis_can_reuse_cached_ensemble_when_peak_moves(self) -> None:
        payload = {
            "schema_version": forecast_analysis_cache.SCHEMA_VERSION,
            "station": "LTAC",
            "target_date": "2026-03-14",
            "model": "ecmwf",
            "synoptic_provider": "ecmwf-open-data",
            "runtime_tag": "2026031400Z",
            "latest_report_local": "2026-03-14T11:50:00+03:00",
            "analysis_peak_local": "2026-03-14T16:00",
            "ensemble_factor": _surface_ready_ensemble("warm_support"),
            "analysis_snapshot": {"schema_version": forecast_analysis_cache.ANALYSIS_SNAPSHOT_SCHEMA_VERSION},
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
                    latest_report_local="2026-03-14T12:20:00+03:00",
                    analysis_peak_local="2026-03-14T15:00",
                    allow_peak_mismatch_reuse=True,
                )

        self.assertIsNotNone(cached)
        self.assertFalse(bool((cached or {}).get("analysis_snapshot_fresh")))
        self.assertEqual((cached or {}).get("ensemble_factor", {}).get("summary", {}).get("dominant_path"), "warm_support")
        self.assertEqual((cached or {}).get("analysis_snapshot"), {})

    def test_refresh_cached_forecast_analysis_snapshot_writes_latest_metar_view(self) -> None:
        payload = {
            "schema_version": forecast_analysis_cache.SCHEMA_VERSION,
            "station": "LTAC",
            "target_date": "2026-03-14",
            "model": "ecmwf",
            "synoptic_provider": "ecmwf-open-data",
            "runtime_tag": "2026031400Z",
            "latest_report_local": "2026-03-14T11:50:00+03:00",
            "analysis_peak_local": "2026-03-14T16:00",
            "ensemble_factor": _surface_ready_ensemble("warm_support"),
            "analysis_snapshot": {"schema_version": "analysis-snapshot.v14"},
        }
        refreshed_snapshot = {"schema_version": forecast_analysis_cache.ANALYSIS_SNAPSHOT_SCHEMA_VERSION, "peak_data": {"summary": {}}}

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
                stored = forecast_analysis_cache.refresh_cached_forecast_analysis_snapshot(
                    cached_payload=payload,
                    station_icao="LTAC",
                    target_date="2026-03-14",
                    model="ecmwf",
                    synoptic_provider="ecmwf-open-data",
                    runtime_tag="2026031400Z",
                    latest_report_local="2026-03-14T12:20:00+03:00",
                    analysis_peak_local="2026-03-14T15:00",
                    analysis_snapshot=refreshed_snapshot,
                    ensemble_factor=_surface_ready_ensemble("warm_support"),
                )
                cache_files = list(cache_dir.glob("forecast_analysis_*.json"))
                self.assertEqual(len(cache_files), 1)
                disk_payload = json.loads(cache_files[0].read_text(encoding="utf-8"))["payload"]

        self.assertEqual(stored["latest_report_local"], "2026-03-14T12:20:00+03:00")
        self.assertEqual(stored["analysis_peak_local"], "2026-03-14T15:00")
        self.assertEqual(stored["analysis_snapshot"]["schema_version"], forecast_analysis_cache.ANALYSIS_SNAPSHOT_SCHEMA_VERSION)
        self.assertEqual(disk_payload["latest_report_local"], "2026-03-14T12:20:00+03:00")
        self.assertEqual(disk_payload["analysis_peak_local"], "2026-03-14T15:00")


if __name__ == "__main__":
    unittest.main()
