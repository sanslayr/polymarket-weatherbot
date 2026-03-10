import json
import sys
import unittest
from datetime import datetime, timezone
from pathlib import Path
from tempfile import TemporaryDirectory
from types import SimpleNamespace
from unittest.mock import patch


ROOT = Path(__file__).resolve().parents[1]
SCRIPTS = ROOT / "scripts"
if str(SCRIPTS) not in sys.path:
    sys.path.insert(0, str(SCRIPTS))

import forecast_pipeline  # noqa: E402
import hourly_data_service  # noqa: E402


class _FixedHourlyDateTime(datetime):
    @classmethod
    def now(cls, tz=None):
        dt = cls(2026, 3, 10, 11, 0, tzinfo=timezone.utc)
        if tz is None:
            return dt.replace(tzinfo=None)
        return dt.astimezone(tz)


class _FixedForecastDateTime(datetime):
    @classmethod
    def now(cls, tz=None):
        dt = cls(2026, 3, 10, 11, 30, tzinfo=timezone.utc)
        if tz is None:
            return dt.replace(tzinfo=None)
        return dt.astimezone(tz)


class ForecastCacheReuseTests(unittest.TestCase):
    def test_fetch_hourly_router_reuses_previous_cycle_cache_before_network(self) -> None:
        station = SimpleNamespace(city="Munich", icao="EDDM", lat=48.35, lon=11.79)
        target_date = "2026-03-10"
        prev_tag = hourly_data_service.model_cycle_tag("ecmwf", _FixedHourlyDateTime.now(timezone.utc).replace(hour=5))
        payload = {
            "hourly": {
                "time": ["2026-03-10T00:00"],
                "temperature_2m": [6.0],
            },
            "timezone": "Europe/Berlin",
        }

        with TemporaryDirectory() as tmp:
            cache_dir = Path(tmp)
            with patch("hourly_data_service.CACHE_DIR", cache_dir), \
                patch("hourly_data_service.runtime_cache_enabled", return_value=True), \
                patch("hourly_data_service.datetime", _FixedHourlyDateTime):
                hourly_data_service._write_cache("hourly", payload, station.icao, target_date, "ecmwf", prev_tag)

                with patch("hourly_data_service.requests.get", side_effect=AssertionError("network should not be used")):
                    cached, provider = hourly_data_service.fetch_hourly_router(
                        station,
                        target_date,
                        "ecmwf",
                        provider="auto",
                        prefer_cached_sources=True,
                    )

        self.assertEqual(provider, "openmeteo-prev-cache")
        self.assertEqual(cached, payload)

    def test_load_or_build_forecast_decision_reuses_cached_runtime_bundle(self) -> None:
        now_utc = _FixedForecastDateTime.now(timezone.utc)
        station = SimpleNamespace(city="Munich", icao="EDDM", lat=48.35, lon=11.79)
        target_date = "2026-03-10"
        runtime = forecast_pipeline._runtime_tag("ecmwf", now_utc)
        bundle = {
            "schema_version": forecast_pipeline.FORECAST_3D_BUNDLE_SCHEMA_VERSION,
            "station": "EDDM",
            "date": target_date,
            "model": "ecmwf",
            "synoptic_provider": "ecmwf-open-data",
            "synoptic_provider_used": "ecmwf-open-data",
            "synoptic_pass_strategy": "split_outer500",
            "runtime": runtime,
            "anchors_local": ["2026-03-10T14:00"],
            "outer500_anchors_local": ["2026-03-10T12:00"],
            "slices": [
                {
                    "analysis_time_utc": "2026-03-10T12:00:00Z",
                    "analysis_time_local": "2026-03-10T13:00:00+01:00",
                    "analysis_runtime_used": "2026031000Z",
                    "analysis_stream_used": "oper",
                    "scale_summary": {"synoptic": {"systems": []}},
                }
            ],
        }

        def _fake_build_forecast_decision(**kwargs):
            return {
                "schema_version": forecast_pipeline.SCHEMA_VERSION,
                "meta": {
                    "station": str(kwargs["station"].icao),
                    "date": kwargs["target_date"],
                    "model": kwargs["model"],
                    "synoptic_provider": kwargs["synoptic_provider"],
                    "runtime": runtime,
                },
                "quality": {
                    "source_state": "fresh",
                    "missing_layers": [],
                },
            }

        with TemporaryDirectory() as tmp:
            cache_dir = Path(tmp)
            bundle_path = cache_dir / "forecast_3d_bundle_cached.json"
            bundle_path.write_text(json.dumps(bundle, ensure_ascii=False), encoding="utf-8")

            with patch("forecast_pipeline.CACHE_DIR", cache_dir), \
                patch("forecast_pipeline.runtime_cache_enabled", return_value=True), \
                patch("forecast_pipeline.datetime", _FixedForecastDateTime), \
                patch("forecast_pipeline.build_forecast_decision", side_effect=_fake_build_forecast_decision):
                decision, synoptic, error = forecast_pipeline.load_or_build_forecast_decision(
                    station=station,
                    target_date=target_date,
                    model="ecmwf",
                    synoptic_provider="ecmwf-open-data",
                    now_utc=now_utc,
                    now_local=now_utc,
                    station_lat=station.lat,
                    station_lon=station.lon,
                    primary_window={"peak_local": "2026-03-10T14:00"},
                    tz_name="Europe/Berlin",
                    run_synoptic_fn=lambda *args, **kwargs: (_ for _ in ()).throw(AssertionError("runner should not execute")),
                    prefer_cached_synoptic=True,
                )

        self.assertIsNone(error)
        self.assertEqual(decision["quality"]["source_state"], "bundle-cache-hit")
        self.assertEqual(decision["quality"]["synoptic_analysis_runtime_used"], "2026031000Z")
        self.assertEqual(decision["meta"]["runtime"], "2026031000Z")
        self.assertEqual(decision["meta"]["runtime_requested"], runtime)
        self.assertEqual(synoptic.get("_provider_used"), "ecmwf-open-data")


if __name__ == "__main__":
    unittest.main()
