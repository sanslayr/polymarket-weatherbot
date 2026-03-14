import sys
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory


ROOT = Path(__file__).resolve().parents[1]
SCRIPTS = ROOT / "scripts"
if str(SCRIPTS) not in sys.path:
    sys.path.insert(0, str(SCRIPTS))

from posterior_learning_sample_service import build_posterior_learning_sample  # noqa: E402
from posterior_training_log_service import append_posterior_learning_sample, read_posterior_learning_log  # noqa: E402


def _sample_snapshot() -> dict:
    return {
        "schema_version": "analysis-snapshot.v10",
        "unit": "C",
        "canonical_raw_state": {
            "schema_version": "canonical-raw-state.v3",
            "observations": {"latest_report_local": "2026-03-14T12:20:00+03:00"},
            "forecast": {"meta": {"station": "LTAC", "date": "2026-03-14", "model": "ecmwf", "synoptic_provider": "ecmwf-open-data", "runtime": "2026031400"}},
        },
        "posterior_feature_vector": {"schema_version": "posterior-feature-vector.v4", "time_phase": {"phase": "near_window", "display_phase": "near_window"}},
        "quality_snapshot": {"schema_version": "quality-snapshot.v2"},
        "weather_posterior": {
            "schema_version": "weather-posterior.v1",
            "core": {"schema_version": "weather-posterior-core.v4"},
            "quantiles": {"p50_c": 13.1},
            "event_probs": {"new_high_next_60m": 0.79, "lock_by_window_end": 0.32},
        },
        "peak_data": {"summary": {"ranges": {"display": {"lo": 12.3, "hi": 13.9}, "core": {"lo": 12.6, "hi": 13.6}}, "range_truth_source": "weather_posterior"}},
    }


class PosteriorTrainingLogServiceTests(unittest.TestCase):
    def test_append_and_read_training_log(self) -> None:
        sample = build_posterior_learning_sample(
            analysis_snapshot=_sample_snapshot(),
            sampling_reason="unit_test_checkpoint",
        )
        with TemporaryDirectory() as tmp:
            path = append_posterior_learning_sample(sample, root=Path(tmp))
            rows = read_posterior_learning_log(
                station_icao="LTAC",
                target_date_local="2026-03-14",
                root=Path(tmp),
            )

        self.assertTrue(path.name.endswith("LTAC.jsonl"))
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["sample"]["sample_id"], sample["sample_id"])
        self.assertEqual(rows[0]["case_index"]["sampling_reason"], "unit_test_checkpoint")


if __name__ == "__main__":
    unittest.main()
