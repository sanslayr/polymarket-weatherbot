import sys
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
SCRIPTS = ROOT / "scripts"
if str(SCRIPTS) not in sys.path:
    sys.path.insert(0, str(SCRIPTS))

from canonical_raw_state_service import build_canonical_raw_state  # noqa: E402
from posterior_feature_service import build_posterior_feature_vector  # noqa: E402


class PosteriorFeatureServiceTest(unittest.TestCase):
    def test_builds_quantitative_contracts_without_presentation_text(self) -> None:
        primary_window = {
            "start_local": "2026-03-09T11:00",
            "peak_local": "2026-03-09T14:00",
            "end_local": "2026-03-09T16:00",
            "peak_temp_c": 24.5,
            "low_cloud_pct": 22.0,
            "w850_kmh": 25.0,
        }
        metar_diag = {
            "latest_report_local": "2026-03-09T11:30:00+00:00",
            "latest_temp": 18.8,
            "latest_dewpoint": 8.2,
            "latest_rh": 52.0,
            "latest_wspd": 7.0,
            "latest_wdir": 170.0,
            "wind_dir_change_deg": 14.0,
            "latest_cloud_code": "FEW",
            "latest_cloud_lowest_base_ft": 4200,
            "latest_wx": "",
            "cloud_effective_cover_smooth": 0.22,
            "radiation_eff_smooth": 0.83,
            "cloud_trend": "cloud slowly thinning",
            "latest_precip_state": "none",
            "precip_trend": "none",
            "temp_trend_smooth_c": 0.28,
            "temp_bias_smooth_c": 0.18,
            "temp_accel_2step_c": -0.04,
            "observed_max_temp_c": 18.8,
            "observed_max_time_local": "2026-03-09T11:30:00+00:00",
            "observed_max_interval_lo_c": 18.5,
            "observed_max_interval_hi_c": 19.5,
            "metar_temp_quantized": False,
            "metar_routine_cadence_min": 30,
            "metar_recent_interval_min": 30,
        }
        forecast_decision = {
            "meta": {
                "station": "LTAC",
                "date": "2026-03-09",
                "model": "gfs",
                "synoptic_provider": "ecmwf-open-data",
                "runtime": "2026030900Z",
                "window": dict(primary_window),
            },
            "quality": {
                "source_state": "fresh",
                "missing_layers": [],
                "synoptic_coverage": 1.0,
                "synoptic_provider_requested": "ecmwf-open-data",
                "synoptic_provider_used": "ecmwf-open-data",
            },
            "features": {
                "objects_3d": {
                    "main_object": {
                        "track_id": "track_1",
                        "type": "advection_3d",
                        "evolution": "approaching",
                        "intensity_trend": "steady",
                        "distance_km_min": 220.0,
                        "closest_approach_distance_km": 180.0,
                        "closest_approach_time_local": "2026-03-09T14:00",
                        "anchors_count": 3,
                        "confidence": "medium",
                    },
                    "main_track": {},
                    "count": 1,
                    "anchors_count": 3,
                    "candidates": [],
                    "tracks": [],
                },
                "h500": {"regime_label": "副热带高压边缘"},
                "h850": {
                    "review": {
                        "thermal_advection_state": "probable",
                        "transport_state": "warm",
                        "surface_coupling_state": "partial",
                        "surface_role": "influence",
                        "surface_bias": "warm",
                        "surface_effect_weight": 0.42,
                    }
                },
                "h700": {
                    "summary": "700hPa 干层特征偏明显",
                    "source": "synoptic-700",
                    "dry_intrusion_scope": "near",
                    "dry_intrusion_nearest_km": 180.0,
                    "dry_intrusion_strength": 10.8,
                },
                "h925": {
                    "summary": "925层耦合偏强",
                    "coupling_state": "strong",
                    "landing_signal": "warm_tilt",
                    "coupling_score": 0.78,
                },
                "sounding": {
                    "thermo": {
                        "profile_source": "model_proxy",
                        "sounding_confidence": "medium",
                        "coverage": {"density_class": "moderate"},
                        "rh925_pct": 66.0,
                        "rh850_pct": 48.0,
                        "rh700_pct": 32.0,
                        "midlevel_rh_pct": 36.0,
                        "t925_t850_c": 2.4,
                        "low_level_cap_score": 0.22,
                        "midlevel_dry_score": 0.58,
                        "midlevel_moist_score": 0.10,
                        "mixing_support_score": 0.66,
                        "wind_profile_mix_score": 0.44,
                        "layer_relationships": {
                            "thermal_structure": "well_mixed",
                            "moisture_layering": "low_moist_mid_dry",
                            "wind_turning_state": "veering_with_height",
                            "coupling_chain_state": "partial",
                        },
                    }
                },
            },
            "decision": {
                "background": {
                    "line_500": "高空暖脊背景仍在。",
                    "line_850": "850暖平流可部分落地。",
                    "extra": "700干层有利日照",
                }
            },
        }

        canonical = build_canonical_raw_state(
            primary_window=primary_window,
            metar_diag=metar_diag,
            forecast_decision=forecast_decision,
            temp_unit="C",
            temp_shape_analysis={
                "forecast": {
                    "shape_type": "single_peak",
                    "multi_peak_state": "none",
                    "plateau_state": "narrow",
                },
                "observed": {
                    "plateau_state": "holding",
                    "hold_duration_hours": 0.5,
                },
            },
        )
        features = build_posterior_feature_vector(
            canonical_raw_state=canonical,
            boundary_layer_regime={
                "regime_key": "mixing_depth",
                "dominant_mechanism": "混合加深",
                "confidence": "medium",
                "advection_role": "influence",
                "headline": "这不应进入 posterior feature",
            },
            temp_phase_decision={
                "phase": "near_window",
                "display_phase": "near_window",
                "short_term_state": "reaccelerating",
                "daily_peak_state": "open",
                "second_peak_potential": "weak",
                "rebound_mode": "retest",
                "dominant_shape": "peak_plateau",
                "plateau_hold_state": "holding",
            },
        )

        self.assertEqual(canonical["schema_version"], "canonical-raw-state.v2")
        self.assertEqual(features["schema_version"], "posterior-feature-vector.v2")
        self.assertEqual(features["meta"]["station"], "LTAC")
        self.assertEqual(features["transport_state"]["thermal_advection_state"], "probable")
        self.assertEqual(features["vertical_structure_state"]["coverage_density"], "moderate")
        self.assertEqual(features["vertical_structure_state"]["h925_coupling_state"], "strong")
        self.assertEqual(features["forecast_shape_state"]["observed_plateau_state"], "holding")
        self.assertEqual(features["track_state"]["main_track_evolution"], "approaching")
        self.assertEqual(features["regime_state"]["dominant_mechanism"], "混合加深")
        self.assertNotIn("legacy_text", canonical["forecast"])
        self.assertNotIn("headline", features["regime_state"])


if __name__ == "__main__":
    unittest.main()
