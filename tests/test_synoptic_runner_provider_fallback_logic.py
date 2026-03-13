import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch


ROOT = Path(__file__).resolve().parents[1]
SCRIPTS = ROOT / "scripts"
if str(SCRIPTS) not in sys.path:
    sys.path.insert(0, str(SCRIPTS))

from synoptic_runner import (  # noqa: E402
    _read_provider_failure_memo,
    _shared_request_bbox,
    _should_continue_to_next_provider,
    _write_provider_failure_memo,
    analyze_synoptic_2d,
)


class SynopticRunnerProviderFallbackLogicTest(unittest.TestCase):
    def test_rate_limit_continues_to_next_provider_when_available(self) -> None:
        self.assertTrue(
            _should_continue_to_next_provider(
                candidate_index=0,
                total_candidates=2,
                exc=RuntimeError("429 Too Many Requests"),
            )
        )

    def test_rate_limit_stops_when_no_more_candidates(self) -> None:
        self.assertFalse(
            _should_continue_to_next_provider(
                candidate_index=1,
                total_candidates=2,
                exc=RuntimeError("429 Too Many Requests"),
            )
        )

    def test_provider_failure_memo_roundtrip(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            cache_dir = Path(td)
            _write_provider_failure_memo(
                cache_dir,
                provider="ecmwf-open-data",
                scope="global",
                error_type="rate_limit_429",
                error="429 Too Many Requests",
            )
            doc = _read_provider_failure_memo(cache_dir, "ecmwf-open-data", "global")
            self.assertIsNotNone(doc)
            self.assertEqual(doc["error_type"], "rate_limit_429")

    def test_shared_request_bbox_reuses_same_grid_for_nearby_stations(self) -> None:
        cfg = {"lat_span": 6.0, "lon_span": 8.0, "step": 1.0}
        station_a = type("Station", (), {"lat": 40.6413, "lon": -73.7781})()
        station_b = type("Station", (), {"lat": 40.7769, "lon": -73.8740})()

        with patch("synoptic_runner.os.getenv", side_effect=lambda key, default=None: {"FORECAST_SHARED_GRID_ENABLED": "1", "FORECAST_SHARED_GRID_QUANTIZE_RATIO": "0.5"}.get(key, default)):
            bbox_a = _shared_request_bbox(station_a, cfg)
            bbox_b = _shared_request_bbox(station_b, cfg)

        self.assertEqual(bbox_a, bbox_b)

    def test_analyze_synoptic_2d_surfaces_missing_dependency_clearly(self) -> None:
        with patch("synoptic_runner.importlib.import_module", side_effect=ModuleNotFoundError("No module named 'numpy'")):
            with self.assertRaisesRegex(RuntimeError, "dependency missing"):
                analyze_synoptic_2d({}, mode="full")


if __name__ == "__main__":
    unittest.main()
