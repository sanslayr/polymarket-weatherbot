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

from forecast_cache_worker import (  # noqa: E402
    _active_probe_cycle_tag,
    _cycle_tag_is_not_older,
    _ecmwf_cycle_runtime_tag,
    _extract_synoptic_runtime_state,
    _prewarm_station_target,
    _purge_old_forecast_cache,
    _purge_stale_forecast_cache,
    _should_probe_cycle,
    _should_run,
    _target_cycle_satisfied,
    _target_dates_for_station,
)
from station_catalog import Station  # noqa: E402


class ForecastCacheWorkerTests(unittest.TestCase):
    def test_target_dates_follow_station_local_date(self) -> None:
        station = Station(city="Seoul", icao="RKSI", lat=37.46, lon=126.44)
        dates = _target_dates_for_station(
            station,
            now_utc=datetime(2026, 3, 10, 23, 30, tzinfo=timezone.utc),
            days_ahead=1,
        )
        self.assertEqual(dates, ["2026-03-11", "2026-03-12"])

    def test_should_run_when_missing_state(self) -> None:
        self.assertTrue(_should_run({}, "RKSI|2026-03-10", 900))

    def test_ecmwf_cycle_runtime_tag_uses_six_hour_blocks(self) -> None:
        self.assertEqual(
            _ecmwf_cycle_runtime_tag(datetime(2026, 3, 10, 11, 30, tzinfo=timezone.utc)),
            "2026031006Z",
        )

    def test_active_probe_cycle_tag_waits_for_probe_start(self) -> None:
        self.assertEqual(
            _active_probe_cycle_tag(datetime(2026, 3, 10, 5, 30, tzinfo=timezone.utc), 3, 6),
            "2026031000Z",
        )
        self.assertEqual(
            _active_probe_cycle_tag(datetime(2026, 3, 10, 9, 0, tzinfo=timezone.utc), 3, 6),
            "2026031006Z",
        )
        self.assertIsNone(
            _active_probe_cycle_tag(datetime(2026, 3, 10, 2, 59, tzinfo=timezone.utc), 3, 6)
        )
        self.assertIsNone(
            _active_probe_cycle_tag(datetime(2026, 3, 10, 6, 0, tzinfo=timezone.utc), 3, 6)
        )

    def test_should_probe_cycle_respects_poll_interval(self) -> None:
        state = {
            "probe_state": {
                "2026031006Z": {
                    "last_probe_at_utc": "2026-03-10T09:10:00Z",
                }
            }
        }
        self.assertFalse(_should_probe_cycle(state, "2026031006Z", datetime(2026, 3, 10, 9, 35, tzinfo=timezone.utc), 30))
        self.assertTrue(_should_probe_cycle(state, "2026031006Z", datetime(2026, 3, 10, 9, 40, tzinfo=timezone.utc), 30))

    def test_cycle_tag_is_not_older(self) -> None:
        self.assertTrue(_cycle_tag_is_not_older("2026031006Z", "2026031000Z"))
        self.assertTrue(_cycle_tag_is_not_older("2026031006Z", "2026031006Z"))
        self.assertFalse(_cycle_tag_is_not_older("2026031000Z", "2026031006Z"))

    def test_purge_stale_forecast_cache_removes_entries_older_than_max_age(self) -> None:
        now = datetime(2026, 3, 10, 12, 0, tzinfo=timezone.utc)
        with TemporaryDirectory() as tmp:
            cache_dir = Path(tmp)
            old_decision = cache_dir / "forecast_decision_old.json"
            fresh_bundle = cache_dir / "forecast_3d_bundle_fresh.json"
            old_analysis = cache_dir / "forecast_analysis_old.json"
            old_decision.write_text("{}", encoding="utf-8")
            fresh_bundle.write_text("{}", encoding="utf-8")
            old_analysis.write_text("{}", encoding="utf-8")
            old_ts = (now - timedelta(hours=25)).timestamp()
            fresh_ts = (now - timedelta(hours=2)).timestamp()
            os.utime(old_decision, (old_ts, old_ts))
            os.utime(fresh_bundle, (fresh_ts, fresh_ts))
            os.utime(old_analysis, (old_ts, old_ts))

            with patch("forecast_cache_worker.CACHE_DIR", cache_dir), patch("forecast_cache_worker._utc_now", return_value=now):
                removed = _purge_stale_forecast_cache(max_age_hours=24)

            self.assertEqual(removed, {"forecast_decision": 1, "forecast_3d_bundle": 0, "forecast_analysis": 1})
            self.assertFalse(old_decision.exists())
            self.assertTrue(fresh_bundle.exists())
            self.assertFalse(old_analysis.exists())

    def test_purge_old_forecast_cache_removes_prior_analysis_runtime(self) -> None:
        with TemporaryDirectory() as tmp:
            cache_dir = Path(tmp)
            old_analysis = cache_dir / "forecast_analysis_old.json"
            keep_analysis = cache_dir / "forecast_analysis_keep.json"
            old_analysis.write_text(
                '{"payload":{"station":"RKSI","target_date":"2026-03-11","runtime_tag":"2026031000Z"}}',
                encoding="utf-8",
            )
            keep_analysis.write_text(
                '{"payload":{"station":"RKSI","target_date":"2026-03-11","runtime_tag":"2026031006Z"}}',
                encoding="utf-8",
            )

            with patch("forecast_cache_worker.CACHE_DIR", cache_dir):
                removed = _purge_old_forecast_cache(
                    station_icao="RKSI",
                    target_date="2026-03-11",
                    keep_runtime_tag="2026031006Z",
                )

            self.assertEqual(removed["forecast_analysis"], 1)
            self.assertFalse(old_analysis.exists())
            self.assertTrue(keep_analysis.exists())

    def test_prewarm_station_target_builds_forecast_analysis_cache(self) -> None:
        station = Station(city="Seoul", icao="RKSI", lat=37.46, lon=126.44)
        with patch(
            "forecast_cache_worker._build_analysis_window",
            return_value=(
                {},
                {"time": ["2026-03-11T00:00"]},
                {"observed_max_temp_c": 11.2},
                {"peak_local": "2026-03-11T15:00"},
                {"peak_local": "2026-03-11T15:00"},
                {"forecast_curve": []},
                "C",
                "ecmwf",
            ),
        ), patch(
            "forecast_cache_worker._utc_now",
            return_value=datetime(2026, 3, 10, 9, 0, tzinfo=timezone.utc),
        ), patch(
            "forecast_cache_worker._attach_historical_context",
            return_value={"available": True},
        ), patch(
            "forecast_cache_worker.build_and_cache_forecast_analysis",
            return_value={"ensemble_factor": {"summary": {"dominant_path": "neutral_stable"}}, "analysis_snapshot": {"schema_version": "snapshot.v1"}},
        ), patch(
            "forecast_pipeline.load_or_build_forecast_decision",
            return_value=(
                {
                    "quality": {"synoptic_provider_used": "gfs-grib2", "source_state": "fresh"},
                    "meta": {"runtime": "2026031006Z", "runtime_requested": "2026031006Z"},
                },
                {"analysis_runtime_used": "2026031006Z"},
                "",
            ),
        ):
            payload = _prewarm_station_target(station, "2026-03-11")

        self.assertTrue(payload["forecast_analysis_cached"])
        self.assertTrue(payload["analysis_snapshot_cached"])
        self.assertTrue(payload["ensemble_factor_cached"])
        self.assertEqual(payload["analysis_cache_runtime"], "2026031006Z")

    def test_extract_synoptic_runtime_state_prefers_actual_analysis_runtime(self) -> None:
        state = _extract_synoptic_runtime_state(
            forecast_decision={
                "quality": {
                    "source_state": "fresh",
                    "missing_layers": [],
                    "synoptic_analysis_runtime_used": "2026031006Z",
                }
            },
            synoptic={
                "analysis_runtime_used": "2026031006Z",
                "previous_runtime_used": "2026031000Z",
            },
        )
        self.assertEqual(state["actual_runtime_tag"], "2026031006Z")
        self.assertEqual(state["previous_runtime_tag"], "2026031000Z")
        self.assertTrue(state["synoptic_complete"])
        self.assertFalse(state["runtime_mixed"])

    def test_target_cycle_satisfied_rejects_degraded_or_mixed_runtime(self) -> None:
        self.assertFalse(
            _target_cycle_satisfied(
                {
                    "actual_runtime_tag": "2026031006Z",
                    "synoptic_complete": False,
                },
                "2026031006Z",
            )
        )
        self.assertFalse(
            _target_cycle_satisfied(
                {
                    "actual_runtime_tag": "2026031000Z",
                    "synoptic_complete": True,
                },
                "2026031006Z",
            )
        )
        self.assertTrue(
            _target_cycle_satisfied(
                {
                    "actual_runtime_tag": "2026031006Z",
                    "synoptic_complete": True,
                },
                "2026031006Z",
            )
        )


if __name__ == "__main__":
    unittest.main()
