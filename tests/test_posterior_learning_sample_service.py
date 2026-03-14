import sys
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
SCRIPTS = ROOT / "scripts"
if str(SCRIPTS) not in sys.path:
    sys.path.insert(0, str(SCRIPTS))

from posterior_case_index_service import build_posterior_case_index  # noqa: E402
from posterior_learning_sample_service import build_posterior_learning_sample  # noqa: E402


def _sample_snapshot() -> dict:
    return {
        "schema_version": "analysis-snapshot.v10",
        "unit": "C",
        "canonical_raw_state": {
            "schema_version": "canonical-raw-state.v3",
            "unit": "C",
            "observations": {
                "latest_report_local": "2026-03-14T12:20:00+03:00",
            },
            "forecast": {
                "meta": {
                    "station": "LTAC",
                    "date": "2026-03-14",
                    "model": "ecmwf",
                    "synoptic_provider": "ecmwf-open-data",
                    "runtime": "2026031400",
                }
            },
            "window": {
                "calc": {
                    "start_local": "2026-03-14T13:00",
                    "peak_local": "2026-03-14T15:00",
                    "end_local": "2026-03-14T16:00",
                }
            },
        },
        "posterior_feature_vector": {
            "schema_version": "posterior-feature-vector.v4",
            "time_phase": {"phase": "near_window", "display_phase": "near_window"},
            "observation_state": {"latest_temp_c": 12.0},
            "peak_phase_state": {"daily_peak_state": "open", "second_peak_potential": "none"},
            "ensemble_path_state": {
                "dominant_path": "warm_support",
                "observed_path": "warm_support",
            },
            "matched_branch_outlook_state": {
                "branch_family": "warm_landing_watch",
                "branch_stage_now": "pending",
                "next_transition_gate": "low_level_coupling",
                "branch_volatility": "low",
            },
            "quality_state": {"source_state": "fresh"},
        },
        "quality_snapshot": {
            "schema_version": "quality-snapshot.v2",
            "scores": {"confidence_label": "high"},
        },
        "weather_posterior": {
            "schema_version": "weather-posterior.v1",
            "quantiles": {"p10_c": 12.3, "p25_c": 12.6, "p50_c": 13.1, "p75_c": 13.6, "p90_c": 13.9},
            "event_probs": {"new_high_next_60m": 0.79, "lock_by_window_end": 0.32},
            "calibration": {"progress_spread_multiplier": 0.84, "upper_tail_cap_c": 13.95},
            "range_hint": {
                "display": {"lo_c": 12.3, "hi_c": 13.9},
                "core": {"lo_c": 12.6, "hi_c": 13.6},
            },
            "range_hint_meta": {"source": "posterior_quantiles_progress_capped"},
            "peak_time": {"best_time_local": "2026-03-14T15:00:00+03:00"},
            "reason_codes": ["ensemble_aligned", "warm_landing_pending"],
            "core": {
                "schema_version": "weather-posterior-core.v4",
                "anchor": {"posterior_median_c": 13.1},
                "progress": {"analysis_window_mode": "forecast_primary"},
                "path_context": {
                    "significant_forecast_detail_text": "当前匹配的是暖输送待接地这支",
                },
            },
        },
        "peak_data": {
            "summary": {
                "range_truth_source": "weather_posterior",
                "ranges": {
                    "source": "posterior_quantiles_progress_capped",
                    "display": {"lo": 12.3, "hi": 13.9},
                    "core": {"lo": 12.6, "hi": 13.6},
                },
                "observed": {"max_temp_c": 12.0},
            }
        },
        "boundary_layer_regime": {"regime_key": "synoptic"},
        "synoptic_summary": {"summary": {"pathway": "暖输送待落地"}},
        "condition_state": {"quality": {"source_state": "fresh"}},
    }


class PosteriorLearningSampleServiceTests(unittest.TestCase):
    def test_build_learning_sample_keeps_branch_and_display_outputs(self) -> None:
        sample = build_posterior_learning_sample(
            analysis_snapshot=_sample_snapshot(),
            sampling_reason="near_resolution_checkpoint",
            source_context={"entrypoint": "unit_test"},
        )

        self.assertEqual(sample["station_icao"], "LTAC")
        self.assertEqual(sample["phase"], "near_window")
        self.assertEqual(sample["feature_blocks"]["matched_branch_outlook_state"]["branch_family"], "warm_landing_watch")
        self.assertEqual(sample["posterior_context"]["path_context"]["significant_forecast_detail_text"], "当前匹配的是暖输送待接地这支")
        self.assertEqual(sample["display_output"]["display_range"]["hi_c"], 13.9)
        self.assertEqual(sample["lineage"]["range_truth_source"], "weather_posterior")
        self.assertEqual(sample["source_context"]["entrypoint"], "unit_test")
        self.assertTrue(sample["sample_id"])

    def test_case_index_summarizes_learning_sample_for_case_review(self) -> None:
        sample = build_posterior_learning_sample(
            analysis_snapshot=_sample_snapshot(),
            sampling_reason="near_resolution_checkpoint",
        )
        case_index = build_posterior_case_index(sample)

        self.assertEqual(case_index["sample_id"], sample["sample_id"])
        self.assertEqual(case_index["branch_family"], "warm_landing_watch")
        self.assertEqual(case_index["dominant_path"], "warm_support")
        self.assertAlmostEqual(case_index["display_width_c"], 1.6)


if __name__ == "__main__":
    unittest.main()
