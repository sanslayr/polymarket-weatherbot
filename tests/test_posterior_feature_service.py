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

        self.assertEqual(canonical["schema_version"], "canonical-raw-state.v3")
        self.assertEqual(features["schema_version"], "posterior-feature-vector.v4")
        self.assertEqual(features["meta"]["station"], "LTAC")
        self.assertEqual(features["transport_state"]["thermal_advection_state"], "probable")
        self.assertEqual(features["vertical_structure_state"]["coverage_density"], "moderate")
        self.assertEqual(features["vertical_structure_state"]["h925_coupling_state"], "strong")
        self.assertEqual(features["forecast_shape_state"]["observed_plateau_state"], "holding")
        self.assertEqual(features["track_state"]["main_track_evolution"], "approaching")
        self.assertEqual(features["regime_state"]["dominant_mechanism"], "混合加深")
        self.assertEqual(features["ensemble_path_state"]["dominant_path"], "")
        self.assertAlmostEqual(features["observation_state"]["modeled_headroom_c"], 5.7, places=2)
        self.assertEqual(features["observation_state"]["reports_since_observed_peak"], 0)
        self.assertEqual(features["time_phase"]["analysis_window_mode"], "")
        self.assertNotIn("legacy_text", canonical["forecast"])
        self.assertNotIn("headline", features["regime_state"])

    def test_detects_live_alignment_with_dominant_ensemble_path(self) -> None:
        primary_window = {
            "start_local": "2026-03-09T12:00",
            "peak_local": "2026-03-09T16:00",
            "end_local": "2026-03-09T18:00",
            "peak_temp_c": 25.2,
            "low_cloud_pct": 16.0,
            "w850_kmh": 28.0,
        }
        metar_diag = {
            "latest_report_local": "2026-03-09T12:00:00+00:00",
            "latest_temp": 20.2,
            "latest_dewpoint": 7.8,
            "latest_rh": 44.0,
            "latest_wspd": 8.0,
            "latest_wdir": 180.0,
            "latest_cloud_code": "FEW",
            "latest_cloud_lowest_base_ft": 4800,
            "latest_wx": "",
            "cloud_effective_cover_smooth": 0.18,
            "radiation_eff_smooth": 0.86,
            "cloud_trend": "cloud thinning",
            "latest_precip_state": "none",
            "precip_trend": "none",
            "temp_trend_smooth_c": 0.32,
            "temp_bias_smooth_c": 0.24,
            "temp_accel_2step_c": 0.02,
            "observed_max_temp_c": 20.2,
            "observed_max_time_local": "2026-03-09T12:00:00+00:00",
            "observed_max_interval_lo_c": 20.0,
            "observed_max_interval_hi_c": 20.5,
            "metar_temp_quantized": False,
            "metar_routine_cadence_min": 30,
            "metar_recent_interval_min": 30,
        }
        forecast_decision = {
            "meta": {
                "station": "KATL",
                "date": "2026-03-09",
                "model": "ecmwf",
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
                "objects_3d": {"tracks": [], "count": 0, "anchors_count": 0},
                "h850": {
                    "review": {
                        "thermal_advection_state": "probable",
                        "transport_state": "warm",
                        "surface_coupling_state": "partial",
                        "surface_bias": "warm",
                    }
                },
                "sounding": {"thermo": {"coverage": {"density_class": "moderate"}}},
            },
        }

        canonical = build_canonical_raw_state(
            primary_window=primary_window,
            metar_diag=metar_diag,
            forecast_decision=forecast_decision,
            ensemble_factor={
                "summary": {
                    "dominant_path": "warm_support",
                    "dominant_path_detail": "warm_support",
                    "dominant_prob": 0.74,
                    "dominant_detail_prob": 0.74,
                    "dominant_margin_prob": 0.29,
                    "split_state": "clustered",
                },
                "probabilities": {
                    "warm_support": 0.74,
                    "transition": 0.20,
                    "cold_suppression": 0.06,
                },
                "diagnostics": {
                    "delta_t850_p10_c": 0.4,
                    "delta_t850_p50_c": 0.8,
                    "delta_t850_p90_c": 1.2,
                    "wind850_p50_kmh": 25.0,
                    "neutral_stable_prob": 0.08,
                    "weak_warm_transition_prob": 0.12,
                    "weak_cold_transition_prob": 0.0,
                },
                "source": {"provider": "ecmwf-ens-open-data"},
            },
            temp_unit="C",
        )
        features = build_posterior_feature_vector(
            canonical_raw_state=canonical,
            temp_phase_decision={
                "phase": "near_window",
                "display_phase": "near_window",
                "short_term_state": "holding",
                "daily_peak_state": "open",
                "second_peak_potential": "none",
                "rebound_mode": "none",
                "dominant_shape": "single_peak",
                "plateau_hold_state": "none",
            },
        )

        ensemble_state = features["ensemble_path_state"]
        self.assertEqual(ensemble_state["dominant_path_detail"], "warm_support")
        self.assertEqual(ensemble_state["observed_path"], "warm_support")
        self.assertEqual(ensemble_state["observed_alignment_match_state"], "exact")
        self.assertEqual(ensemble_state["observed_alignment_confidence"], "high")
        self.assertTrue(ensemble_state["observed_path_locked"])
        self.assertGreater(ensemble_state["observed_alignment_score"], 0.75)
        self.assertGreater(ensemble_state["observed_warm_signal"], ensemble_state["observed_cold_signal"])

    def test_builds_observation_matched_subset_even_when_full_dominant_path_is_wrong(self) -> None:
        primary_window = {
            "start_local": "2026-03-09T12:00",
            "peak_local": "2026-03-09T15:30",
            "end_local": "2026-03-09T17:30",
            "peak_temp_c": 24.8,
            "low_cloud_pct": 18.0,
            "w850_kmh": 24.0,
        }
        metar_diag = {
            "latest_report_local": "2026-03-09T12:30:00+00:00",
            "latest_temp": 20.5,
            "latest_dewpoint": 7.8,
            "latest_rh": 43.0,
            "latest_wspd": 8.0,
            "latest_wdir": 185.0,
            "latest_cloud_code": "FEW",
            "latest_cloud_lowest_base_ft": 4800,
            "latest_wx": "",
            "cloud_effective_cover_smooth": 0.16,
            "radiation_eff_smooth": 0.86,
            "cloud_trend": "cloud thinning",
            "latest_precip_state": "none",
            "precip_trend": "none",
            "temp_trend_smooth_c": 0.34,
            "temp_bias_smooth_c": 0.28,
            "temp_accel_2step_c": 0.04,
            "observed_max_temp_c": 20.5,
            "observed_max_time_local": "2026-03-09T12:30:00+00:00",
            "observed_max_interval_lo_c": 20.3,
            "observed_max_interval_hi_c": 20.8,
            "metar_temp_quantized": False,
            "metar_routine_cadence_min": 30,
            "metar_recent_interval_min": 30,
        }
        forecast_decision = {
            "meta": {
                "station": "LTAC",
                "date": "2026-03-09",
                "model": "ecmwf",
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
                "objects_3d": {"tracks": [], "count": 0, "anchors_count": 0},
                "h850": {
                    "review": {
                        "thermal_advection_state": "probable",
                        "transport_state": "warm",
                        "surface_coupling_state": "partial",
                        "surface_bias": "warm",
                    }
                },
                "sounding": {"thermo": {"coverage": {"density_class": "moderate"}}},
            },
        }

        ensemble_members = [
            {"number": 0, "path_label": "cold_suppression", "path_detail": "cold_suppression", "delta_t850_c": -1.1, "wind_speed_850_kmh": 18.0},
            {"number": 1, "path_label": "cold_suppression", "path_detail": "cold_suppression", "delta_t850_c": -0.9, "wind_speed_850_kmh": 16.0},
            {"number": 2, "path_label": "cold_suppression", "path_detail": "cold_suppression", "delta_t850_c": -0.8, "wind_speed_850_kmh": 17.0},
            {"number": 3, "path_label": "cold_suppression", "path_detail": "cold_suppression", "delta_t850_c": -0.7, "wind_speed_850_kmh": 19.0},
            {"number": 4, "path_label": "cold_suppression", "path_detail": "cold_suppression", "delta_t850_c": -0.6, "wind_speed_850_kmh": 18.0},
            {"number": 5, "path_label": "warm_support", "path_detail": "warm_support", "delta_t850_c": 0.7, "wind_speed_850_kmh": 24.0},
            {"number": 6, "path_label": "warm_support", "path_detail": "warm_support", "delta_t850_c": 0.9, "wind_speed_850_kmh": 25.0},
            {"number": 7, "path_label": "warm_support", "path_detail": "warm_support", "delta_t850_c": 1.0, "wind_speed_850_kmh": 27.0},
            {"number": 8, "path_label": "warm_support", "path_detail": "warm_support", "delta_t850_c": 1.1, "wind_speed_850_kmh": 26.0},
            {"number": 9, "path_label": "transition", "path_detail": "weak_warm_transition", "delta_t850_c": 0.3, "wind_speed_850_kmh": 21.0},
        ]

        canonical = build_canonical_raw_state(
            primary_window=primary_window,
            metar_diag=metar_diag,
            forecast_decision=forecast_decision,
            ensemble_factor={
                "summary": {
                    "dominant_path": "cold_suppression",
                    "dominant_path_detail": "cold_suppression",
                    "dominant_prob": 0.50,
                    "dominant_detail_prob": 0.50,
                    "dominant_margin_prob": 0.10,
                    "split_state": "mixed",
                    "transition_detail": "weak_warm_transition",
                    "transition_detail_prob": 0.10,
                },
                "probabilities": {
                    "warm_support": 0.40,
                    "transition": 0.10,
                    "cold_suppression": 0.50,
                },
                "diagnostics": {
                    "delta_t850_p10_c": -1.0,
                    "delta_t850_p50_c": -0.15,
                    "delta_t850_p90_c": 1.0,
                    "wind850_p50_kmh": 20.0,
                    "neutral_stable_prob": 0.0,
                    "weak_warm_transition_prob": 0.10,
                    "weak_cold_transition_prob": 0.0,
                },
                "members": ensemble_members,
                "source": {"provider": "ecmwf-ens-open-data"},
            },
            temp_unit="C",
        )
        features = build_posterior_feature_vector(
            canonical_raw_state=canonical,
            temp_phase_decision={
                "phase": "near_window",
                "display_phase": "near_window",
                "short_term_state": "holding",
                "daily_peak_state": "open",
                "second_peak_potential": "none",
                "rebound_mode": "none",
                "dominant_shape": "single_peak",
                "plateau_hold_state": "none",
            },
        )

        ensemble_state = features["ensemble_path_state"]
        self.assertEqual(ensemble_state["observed_path"], "warm_support")
        self.assertTrue(ensemble_state["matched_subset_active"])
        self.assertEqual(ensemble_state["matched_subset_reason"], "observed_path_override")
        self.assertEqual(ensemble_state["matched_dominant_path"], "warm_support")
        self.assertEqual(ensemble_state["matched_member_count"], 4)
        self.assertAlmostEqual(ensemble_state["matched_member_share"], 0.4, places=3)
        self.assertEqual(ensemble_state["rejected_dominant_path"], "cold_suppression")
        branch_outlook = features["matched_branch_outlook_state"]
        self.assertEqual(branch_outlook["branch_family"], "warm_landing_watch")
        self.assertEqual(branch_outlook["next_transition_gate"], "low_level_coupling")
        self.assertEqual(branch_outlook["expected_next_family"], "warm_support_track")
        self.assertEqual(branch_outlook["fallback_family"], "neutral_plateau")
        self.assertTrue(branch_outlook["warm_landing_pending"])

    def test_high_volatility_matched_branch_is_preserved_as_volatile_split(self) -> None:
        primary_window = {
            "start_local": "2026-03-09T13:00",
            "peak_local": "2026-03-09T16:00",
            "end_local": "2026-03-09T18:00",
            "peak_temp_c": 22.8,
            "low_cloud_pct": 42.0,
            "w850_kmh": 18.0,
        }
        metar_diag = {
            "latest_report_local": "2026-03-09T13:20:00+00:00",
            "latest_temp": 18.0,
            "latest_dewpoint": 8.4,
            "latest_rh": 52.0,
            "latest_wspd": 7.0,
            "latest_wdir": 150.0,
            "latest_cloud_code": "SCT",
            "latest_cloud_lowest_base_ft": 3200,
            "latest_wx": "",
            "cloud_effective_cover_smooth": 0.46,
            "radiation_eff_smooth": 0.64,
            "cloud_trend": "steady",
            "latest_precip_state": "none",
            "precip_trend": "none",
            "temp_trend_smooth_c": 0.10,
            "temp_bias_smooth_c": 0.02,
            "temp_accel_2step_c": 0.00,
            "observed_max_temp_c": 18.0,
            "observed_max_time_local": "2026-03-09T13:20:00+00:00",
            "observed_max_interval_lo_c": 17.8,
            "observed_max_interval_hi_c": 18.3,
            "metar_temp_quantized": False,
            "metar_routine_cadence_min": 30,
            "metar_recent_interval_min": 30,
        }
        forecast_decision = {
            "meta": {
                "station": "RJTT",
                "date": "2026-03-09",
                "model": "ecmwf",
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
                "objects_3d": {"tracks": [], "count": 0, "anchors_count": 0},
                "h850": {"review": {"thermal_advection_state": "none", "transport_state": "neutral"}},
                "sounding": {"thermo": {"coverage": {"density_class": "moderate"}}},
            },
        }

        canonical = build_canonical_raw_state(
            primary_window=primary_window,
            metar_diag=metar_diag,
            forecast_decision=forecast_decision,
            ensemble_factor={
                "summary": {
                    "dominant_path": "transition",
                    "dominant_path_detail": "neutral_stable",
                    "dominant_prob": 0.46,
                    "dominant_detail_prob": 0.30,
                    "dominant_margin_prob": 0.06,
                    "split_state": "split",
                    "transition_detail": "neutral_stable",
                    "transition_detail_prob": 0.30,
                    "signal_dispersion_c": 2.9,
                },
                "probabilities": {
                    "warm_support": 0.29,
                    "transition": 0.46,
                    "cold_suppression": 0.25,
                },
                "diagnostics": {
                    "delta_t850_p10_c": -1.2,
                    "delta_t850_p50_c": 0.0,
                    "delta_t850_p90_c": 1.7,
                    "wind850_p50_kmh": 17.0,
                    "neutral_stable_prob": 0.30,
                    "weak_warm_transition_prob": 0.10,
                    "weak_cold_transition_prob": 0.06,
                },
                "source": {"provider": "ecmwf-ens-open-data"},
            },
            temp_unit="C",
            temp_shape_analysis={
                "forecast": {
                    "shape_type": "broad_plateau",
                    "multi_peak_state": "possible",
                    "plateau_state": "broad",
                },
                "observed": {
                    "plateau_state": "holding",
                    "hold_duration_hours": 0.5,
                },
            },
        )
        features = build_posterior_feature_vector(
            canonical_raw_state=canonical,
            temp_phase_decision={
                "phase": "near_window",
                "display_phase": "near_window",
                "short_term_state": "holding",
                "daily_peak_state": "open",
                "second_peak_potential": "none",
                "rebound_mode": "none",
                "dominant_shape": "broad_plateau",
                "plateau_hold_state": "holding",
            },
        )

        branch_outlook = features["matched_branch_outlook_state"]
        self.assertEqual(branch_outlook["branch_family"], "volatile_split")
        self.assertEqual(branch_outlook["branch_volatility"], "high")
        self.assertEqual(branch_outlook["next_transition_gate"], "branch_resolution")


if __name__ == "__main__":
    unittest.main()
