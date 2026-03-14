import sys
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
SCRIPTS = ROOT / "scripts"
if str(SCRIPTS) not in sys.path:
    sys.path.insert(0, str(SCRIPTS))

from canonical_raw_state_service import build_canonical_raw_state  # noqa: E402
from posterior_feature_service import build_posterior_feature_vector  # noqa: E402
from quality_snapshot_service import build_quality_snapshot  # noqa: E402
from weather_posterior_core import _build_path_context  # noqa: E402
from weather_posterior_service import build_weather_posterior  # noqa: E402


class WeatherPosteriorServiceTest(unittest.TestCase):
    def test_observed_path_branch_context_stays_reportable_without_subset_cap(self) -> None:
        path_context = _build_path_context(
            branch_outlook_state={
                "branch_source": "observed_path",
                "branch_family": "warm_landing_watch",
                "branch_stage_now": "pending",
                "branch_path": "warm_support",
                "branch_path_detail": "warm_support",
                "branch_side": "warm",
                "branch_dominant_prob": 0.94,
                "branch_volatility": "low",
                "next_transition_gate": "low_level_coupling",
                "expected_next_family": "warm_support_track",
                "expected_next_stage": "building",
                "expected_follow_through_prob": 0.64,
                "fallback_family": "neutral_plateau",
                "fallback_stage": "stalling",
                "fallback_prob": 0.36,
                "matched_subset_active": False,
            },
        )

        self.assertGreaterEqual(path_context["significant_forecast_detail_score"], 0.78)
        self.assertIn("暖输送待接地", path_context["significant_forecast_detail_text"])
        self.assertIn("下一步主看低层耦合", path_context["significant_forecast_detail_text"])

    def test_builds_quantiles_and_event_probs_from_structured_features(self) -> None:
        primary_window = {
            "start_local": "2026-03-09T11:00",
            "peak_local": "2026-03-09T14:00",
            "end_local": "2026-03-09T16:00",
            "peak_temp_c": 24.0,
            "low_cloud_pct": 20.0,
            "w850_kmh": 32.0,
        }
        metar_diag = {
            "latest_report_local": "2026-03-09T11:30:00+00:00",
            "latest_temp": 19.6,
            "latest_dewpoint": 7.4,
            "latest_rh": 46.0,
            "latest_wspd": 8.0,
            "latest_wdir": 175.0,
            "latest_cloud_code": "FEW",
            "latest_cloud_lowest_base_ft": 4500,
            "latest_wx": "",
            "cloud_effective_cover_smooth": 0.18,
            "radiation_eff_smooth": 0.86,
            "cloud_trend": "cloud thinning",
            "latest_precip_state": "none",
            "precip_trend": "none",
            "temp_trend_smooth_c": 0.34,
            "temp_bias_smooth_c": 0.32,
            "temp_accel_2step_c": 0.05,
            "observed_max_temp_c": 19.6,
            "observed_max_time_local": "2026-03-09T11:30:00+00:00",
            "observed_max_interval_lo_c": 19.4,
            "observed_max_interval_hi_c": 20.0,
            "metar_temp_quantized": False,
            "metar_routine_cadence_min": 30,
            "metar_recent_interval_min": 30,
        }
        forecast_decision = {
            "meta": {
                "station": "LTAC",
                "date": "2026-03-09",
                "model": "ifs",
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
                "synoptic_provider_fallback": False,
            },
            "features": {
                "objects_3d": {
                    "main_object": {
                        "track_id": "track_1",
                        "type": "advection_3d",
                        "evolution": "approaching",
                        "intensity_trend": "strengthening",
                        "distance_km_min": 180.0,
                        "closest_approach_distance_km": 120.0,
                        "closest_approach_time_local": "2026-03-09T14:00",
                        "anchors_count": 3,
                        "confidence": "high",
                    },
                    "tracks": [],
                    "count": 1,
                    "anchors_count": 3,
                },
                "h500": {"regime_label": "高压脊"},
                "h850": {
                    "review": {
                        "thermal_advection_state": "probable",
                        "transport_state": "warm",
                        "surface_coupling_state": "partial",
                        "surface_role": "influence",
                        "surface_bias": "warm",
                        "surface_effect_weight": 0.42,
                        "timing_score": 0.82,
                        "reach_score": 0.64,
                        "distance_km": 180.0,
                    }
                },
                "h700": {
                    "summary": "700hPa 干层信号近站（约160km）",
                    "source": "synoptic-700",
                    "dry_intrusion_scope": "near",
                    "dry_intrusion_nearest_km": 160.0,
                    "dry_intrusion_strength": 12.2,
                },
                "h925": {
                    "summary": "低层耦合偏强（925-850 传输更易落地）",
                    "coupling_state": "strong",
                    "landing_signal": "neutral",
                    "coupling_score": 0.82,
                },
                "sounding": {
                    "thermo": {
                        "profile_source": "model_proxy",
                        "sounding_confidence": "medium",
                        "coverage": {"density_class": "moderate"},
                        "rh925_pct": 58.0,
                        "rh850_pct": 42.0,
                        "rh700_pct": 30.0,
                        "midlevel_rh_pct": 34.0,
                        "low_level_cap_score": 0.16,
                        "layer_relationships": {
                            "thermal_structure": "well_mixed",
                            "coupling_chain_state": "partial",
                        },
                    }
                },
            },
            "decision": {
                "main_path": "实况主导",
                "trigger": "关注下一报温度斜率",
                "override_risk": "low",
                "context": {
                    "code": "midlevel_dry_support",
                    "polarity": "supportive",
                    "source": "h700",
                },
                "background": {
                    "line_500": "高压脊控制。",
                    "line_850": "暖平流窗口期内。",
                    "extra": "700hPa 干层偏近站",
                },
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
                    "plateau_state": "none",
                    "hold_duration_hours": 0.0,
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
            },
            temp_phase_decision={
                "phase": "near_window",
                "display_phase": "near_window",
                "short_term_state": "reaccelerating",
                "daily_peak_state": "open",
                "second_peak_potential": "weak",
                "rebound_mode": "none",
                "dominant_shape": "single_peak",
                "plateau_hold_state": "none",
            },
        )
        posterior = build_weather_posterior(
            canonical_raw_state=canonical,
            posterior_feature_vector=features,
            quality_snapshot=build_quality_snapshot(
                canonical_raw_state=canonical,
                posterior_feature_vector=features,
            ),
        )

        quantiles = posterior["quantiles"]
        self.assertEqual(posterior["schema_version"], "weather-posterior.v1")
        self.assertTrue(posterior["calibration"]["applied"])
        self.assertLessEqual(quantiles["p10_c"], quantiles["p25_c"])
        self.assertLessEqual(quantiles["p25_c"], quantiles["p50_c"])
        self.assertLessEqual(quantiles["p50_c"], quantiles["p75_c"])
        self.assertLessEqual(quantiles["p75_c"], quantiles["p90_c"])
        self.assertGreaterEqual(quantiles["p10_c"], 19.4)
        self.assertGreater(posterior["event_probs"]["new_high_next_60m"], 0.4)
        self.assertGreater(posterior["event_probs"]["exceed_modeled_peak"], 0.05)
        self.assertEqual(posterior["peak_time"]["timing_source"], "track_eta")
        self.assertIn("modeled_peak_anchor", posterior["reason_codes"])

    def test_far_window_uses_ensemble_path_as_uncertainty_factor_not_temperature_anchor(self) -> None:
        primary_window = {
            "start_local": "2026-03-10T11:00",
            "peak_local": "2026-03-10T14:00",
            "end_local": "2026-03-10T16:00",
            "peak_temp_c": 24.0,
            "low_cloud_pct": 55.0,
            "w850_kmh": 20.0,
        }
        metar_diag = {
            "latest_report_local": "2026-03-09T18:00:00+00:00",
            "latest_temp": 17.2,
            "latest_dewpoint": 10.0,
            "latest_rh": 68.0,
            "latest_wspd": 6.0,
            "latest_wdir": 40.0,
            "latest_cloud_code": "BKN",
            "latest_cloud_lowest_base_ft": 2200,
            "latest_wx": "",
            "cloud_effective_cover_smooth": 0.72,
            "radiation_eff_smooth": 0.45,
            "cloud_trend": "steady",
            "latest_precip_state": "none",
            "precip_trend": "none",
            "temp_trend_smooth_c": 0.02,
            "temp_bias_smooth_c": -0.10,
            "temp_accel_2step_c": 0.0,
            "observed_max_temp_c": 17.2,
            "observed_max_time_local": "2026-03-09T18:00:00+00:00",
            "observed_max_interval_lo_c": 17.0,
            "observed_max_interval_hi_c": 17.5,
            "metar_temp_quantized": False,
            "metar_routine_cadence_min": 30,
            "metar_recent_interval_min": 30,
        }
        forecast_decision = {
            "meta": {
                "station": "ZSPD",
                "date": "2026-03-10",
                "model": "ecmwf",
                "synoptic_provider": "ecmwf-open-data",
                "runtime": "2026030912Z",
                "window": dict(primary_window),
            },
            "quality": {
                "source_state": "fresh",
                "missing_layers": [],
                "synoptic_coverage": 1.0,
                "synoptic_provider_requested": "ecmwf-open-data",
                "synoptic_provider_used": "ecmwf-open-data",
                "synoptic_provider_fallback": False,
            },
            "features": {
                "objects_3d": {"tracks": [], "count": 0, "anchors_count": 0},
                "h850": {"review": {"thermal_advection_state": "probable", "transport_state": "neutral"}},
                "sounding": {
                    "thermo": {
                        "coverage": {"density_class": "moderate"},
                        "low_level_cap_score": 0.48,
                    }
                },
            },
        }

        canonical = build_canonical_raw_state(
            primary_window=primary_window,
            metar_diag=metar_diag,
            forecast_decision=forecast_decision,
            ensemble_factor={
                "summary": {
                    "dominant_path": "transition",
                    "dominant_prob": 0.41,
                    "split_state": "split",
                },
                "probabilities": {
                    "warm_support": 0.34,
                    "transition": 0.41,
                    "cold_suppression": 0.25,
                },
                "diagnostics": {
                    "delta_t850_p10_c": -1.4,
                    "delta_t850_p50_c": 0.0,
                    "delta_t850_p90_c": 1.2,
                },
                "source": {"provider": "ecmwf-ens-open-data"},
            },
            temp_unit="C",
        )
        features = build_posterior_feature_vector(
            canonical_raw_state=canonical,
            temp_phase_decision={
                "phase": "far",
                "display_phase": "far",
                "short_term_state": "holding",
                "daily_peak_state": "open",
                "second_peak_potential": "none",
                "rebound_mode": "none",
                "dominant_shape": "single_peak",
                "plateau_hold_state": "none",
            },
        )
        posterior = build_weather_posterior(
            canonical_raw_state=canonical,
            posterior_feature_vector=features,
            quality_snapshot=build_quality_snapshot(
                canonical_raw_state=canonical,
                posterior_feature_vector=features,
            ),
        )

        self.assertIn("far_modeled_peak_soft_anchor", posterior["reason_codes"])
        self.assertIn("ensemble_path_split", posterior["reason_codes"])
        self.assertGreaterEqual(posterior["anchor"]["spread_c"], 1.2)

    def test_near_window_prefers_observations_over_model_peak_anchor(self) -> None:
        primary_window = {
            "start_local": "2026-03-09T13:00",
            "peak_local": "2026-03-09T14:00",
            "end_local": "2026-03-09T16:00",
            "peak_temp_c": 24.0,
            "low_cloud_pct": 18.0,
            "w850_kmh": 22.0,
        }
        metar_diag = {
            "latest_report_local": "2026-03-09T12:30:00+00:00",
            "latest_temp": 21.0,
            "latest_dewpoint": 8.0,
            "latest_rh": 44.0,
            "latest_wspd": 8.0,
            "latest_wdir": 180.0,
            "latest_cloud_code": "FEW",
            "latest_cloud_lowest_base_ft": 5000,
            "latest_wx": "",
            "cloud_effective_cover_smooth": 0.16,
            "radiation_eff_smooth": 0.88,
            "cloud_trend": "cloud thinning",
            "latest_precip_state": "none",
            "precip_trend": "none",
            "temp_trend_smooth_c": 0.26,
            "temp_bias_smooth_c": 0.20,
            "temp_accel_2step_c": 0.03,
            "observed_max_temp_c": 21.0,
            "observed_max_time_local": "2026-03-09T12:30:00+00:00",
            "observed_max_interval_lo_c": 20.8,
            "observed_max_interval_hi_c": 21.4,
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
                "h850": {"review": {"thermal_advection_state": "probable", "transport_state": "warm"}},
                "sounding": {"thermo": {"coverage": {"density_class": "moderate"}}},
            },
        }

        canonical = build_canonical_raw_state(
            primary_window=primary_window,
            metar_diag=metar_diag,
            forecast_decision=forecast_decision,
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
        posterior = build_weather_posterior(
            canonical_raw_state=canonical,
            posterior_feature_vector=features,
            quality_snapshot=build_quality_snapshot(
                canonical_raw_state=canonical,
                posterior_feature_vector=features,
            ),
        )

        self.assertIn("near_peak_obs_anchor", posterior["reason_codes"])
        self.assertIn("regime_sunny_highland_dry_mix", posterior["reason_codes"])
        self.assertGreater(posterior["anchor"]["regime_median_shift_c"], 0.0)
        self.assertLess(posterior["anchor"]["posterior_median_c"], 23.5)
        self.assertEqual(
            posterior["regimes"]["active_regimes"][0]["id"],
            "sunny_highland_dry_mix",
        )

    def test_same_day_live_ensemble_alignment_narrows_spread_without_moving_center(self) -> None:
        primary_window = {
            "start_local": "2026-03-09T13:00",
            "peak_local": "2026-03-09T16:00",
            "end_local": "2026-03-09T18:00",
            "peak_temp_c": 24.8,
            "low_cloud_pct": 18.0,
            "w850_kmh": 24.0,
        }
        metar_diag = {
            "latest_report_local": "2026-03-09T12:00:00+00:00",
            "latest_temp": 20.4,
            "latest_dewpoint": 8.0,
            "latest_rh": 46.0,
            "latest_wspd": 7.0,
            "latest_wdir": 185.0,
            "latest_cloud_code": "FEW",
            "latest_cloud_lowest_base_ft": 4800,
            "latest_wx": "",
            "cloud_effective_cover_smooth": 0.20,
            "radiation_eff_smooth": 0.85,
            "cloud_trend": "cloud thinning",
            "latest_precip_state": "none",
            "precip_trend": "none",
            "temp_trend_smooth_c": 0.30,
            "temp_bias_smooth_c": 0.22,
            "temp_accel_2step_c": 0.02,
            "observed_max_temp_c": 20.4,
            "observed_max_time_local": "2026-03-09T12:00:00+00:00",
            "observed_max_interval_lo_c": 20.2,
            "observed_max_interval_hi_c": 20.7,
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
                "synoptic_provider_fallback": False,
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

        aligned_canonical = build_canonical_raw_state(
            primary_window=primary_window,
            metar_diag=metar_diag,
            forecast_decision=forecast_decision,
            ensemble_factor={
                "summary": {
                    "dominant_path": "warm_support",
                    "dominant_path_detail": "warm_support",
                    "dominant_prob": 0.74,
                    "dominant_detail_prob": 0.74,
                    "dominant_margin_prob": 0.28,
                    "split_state": "clustered",
                },
                "probabilities": {
                    "warm_support": 0.74,
                    "transition": 0.18,
                    "cold_suppression": 0.08,
                },
                "diagnostics": {
                    "delta_t850_p10_c": 0.4,
                    "delta_t850_p50_c": 0.9,
                    "delta_t850_p90_c": 1.3,
                    "wind850_p50_kmh": 24.0,
                    "neutral_stable_prob": 0.08,
                    "weak_warm_transition_prob": 0.10,
                    "weak_cold_transition_prob": 0.0,
                },
                "source": {"provider": "ecmwf-ens-open-data"},
            },
            temp_unit="C",
        )
        conflicting_canonical = build_canonical_raw_state(
            primary_window=primary_window,
            metar_diag=metar_diag,
            forecast_decision=forecast_decision,
            ensemble_factor={
                "summary": {
                    "dominant_path": "transition",
                    "dominant_path_detail": "neutral_stable",
                    "dominant_prob": 0.74,
                    "dominant_detail_prob": 0.62,
                    "dominant_margin_prob": 0.28,
                    "transition_detail": "neutral_stable",
                    "transition_detail_prob": 0.62,
                    "split_state": "clustered",
                },
                "probabilities": {
                    "warm_support": 0.12,
                    "transition": 0.74,
                    "cold_suppression": 0.14,
                },
                "diagnostics": {
                    "delta_t850_p10_c": -0.2,
                    "delta_t850_p50_c": 0.1,
                    "delta_t850_p90_c": 0.4,
                    "wind850_p50_kmh": 18.0,
                    "neutral_stable_prob": 0.62,
                    "weak_warm_transition_prob": 0.08,
                    "weak_cold_transition_prob": 0.04,
                },
                "source": {"provider": "ecmwf-ens-open-data"},
            },
            temp_unit="C",
        )

        aligned_features = build_posterior_feature_vector(
            canonical_raw_state=aligned_canonical,
            temp_phase_decision={
                "phase": "same_day",
                "display_phase": "same_day",
                "short_term_state": "holding",
                "daily_peak_state": "open",
                "second_peak_potential": "none",
                "rebound_mode": "none",
                "dominant_shape": "single_peak",
                "plateau_hold_state": "none",
            },
        )
        conflicting_features = build_posterior_feature_vector(
            canonical_raw_state=conflicting_canonical,
            temp_phase_decision={
                "phase": "same_day",
                "display_phase": "same_day",
                "short_term_state": "holding",
                "daily_peak_state": "open",
                "second_peak_potential": "none",
                "rebound_mode": "none",
                "dominant_shape": "single_peak",
                "plateau_hold_state": "none",
            },
        )
        aligned_posterior = build_weather_posterior(
            canonical_raw_state=aligned_canonical,
            posterior_feature_vector=aligned_features,
            quality_snapshot=build_quality_snapshot(
                canonical_raw_state=aligned_canonical,
                posterior_feature_vector=aligned_features,
            ),
        )
        conflicting_posterior = build_weather_posterior(
            canonical_raw_state=conflicting_canonical,
            posterior_feature_vector=conflicting_features,
            quality_snapshot=build_quality_snapshot(
                canonical_raw_state=conflicting_canonical,
                posterior_feature_vector=conflicting_features,
            ),
        )

        self.assertIn("ensemble_path_alignment_locked", aligned_posterior["reason_codes"])
        self.assertNotIn("ensemble_path_alignment_locked", conflicting_posterior["reason_codes"])
        self.assertAlmostEqual(
            aligned_posterior["anchor"]["posterior_median_c"],
            conflicting_posterior["anchor"]["posterior_median_c"],
            places=2,
        )
        self.assertLess(
            aligned_posterior["anchor"]["spread_c"],
            conflicting_posterior["anchor"]["spread_c"],
        )

    def test_progress_aware_calibration_narrows_flat_near_end_case(self) -> None:
        primary_window = {
            "start_local": "2026-03-09T13:00",
            "peak_local": "2026-03-09T14:00",
            "end_local": "2026-03-09T16:00",
            "peak_temp_c": 24.0,
            "low_cloud_pct": 18.0,
            "w850_kmh": 22.0,
        }
        metar_diag = {
            "latest_report_local": "2026-03-09T15:40:00+00:00",
            "latest_temp": 23.4,
            "latest_dewpoint": 8.0,
            "latest_rh": 44.0,
            "latest_wspd": 8.0,
            "latest_wdir": 180.0,
            "latest_cloud_code": "FEW",
            "latest_cloud_lowest_base_ft": 5000,
            "latest_wx": "",
            "cloud_effective_cover_smooth": 0.16,
            "radiation_eff_smooth": 0.70,
            "cloud_trend": "stable",
            "latest_precip_state": "none",
            "precip_trend": "none",
            "temp_trend_smooth_c": -0.05,
            "temp_bias_smooth_c": -0.10,
            "temp_accel_2step_c": -0.02,
            "observed_max_temp_c": 23.6,
            "observed_max_time_local": "2026-03-09T15:10:00+00:00",
            "observed_max_interval_lo_c": 23.4,
            "observed_max_interval_hi_c": 23.8,
            "metar_temp_quantized": False,
            "metar_routine_cadence_min": 30,
            "metar_recent_interval_min": 30,
            "analysis_window_mode": "obs_plateau_reanchor",
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
                "h850": {"review": {"thermal_advection_state": "probable", "transport_state": "neutral"}},
                "sounding": {"thermo": {"coverage": {"density_class": "moderate"}}},
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
            temp_phase_decision={
                "phase": "in_window",
                "display_phase": "in_window",
                "short_term_state": "holding",
                "daily_peak_state": "lean_locked",
                "second_peak_potential": "none",
                "rebound_mode": "retest",
                "dominant_shape": "peak_plateau",
                "plateau_hold_state": "holding",
            },
        )
        posterior = build_weather_posterior(
            canonical_raw_state=canonical,
            posterior_feature_vector=features,
            quality_snapshot=build_quality_snapshot(
                canonical_raw_state=canonical,
                posterior_feature_vector=features,
            ),
        )

        self.assertLess(posterior["calibration"]["progress_spread_multiplier"], 1.0)
        self.assertLessEqual(posterior["quantiles"]["p75_c"], 23.9)
        self.assertLessEqual(posterior["quantiles"]["p90_c"], 23.95)
        self.assertEqual(posterior["range_hint_meta"]["source"], "posterior_quantiles_progress_capped")
        self.assertLessEqual(posterior["range_hint"]["display"]["hi_c"], 23.8)
        self.assertGreaterEqual(posterior["range_hint"]["display"]["lo_c"], 23.4)
        self.assertEqual(posterior["core"]["progress"]["analysis_window_mode"], "obs_plateau_reanchor")

    def test_post_window_lock_caps_upper_tail_near_observed_high(self) -> None:
        primary_window = {
            "start_local": "2026-03-08T15:00",
            "peak_local": "2026-03-08T17:00",
            "end_local": "2026-03-08T18:00",
            "peak_temp_c": 24.0,
            "low_cloud_pct": 10.0,
            "w850_kmh": 18.0,
        }
        metar_diag = {
            "latest_report_local": "2026-03-08T18:40:00+00:00",
            "latest_temp": 23.2,
            "latest_dewpoint": 7.0,
            "latest_rh": 35.0,
            "latest_wspd": 4.0,
            "latest_wdir": 200.0,
            "latest_cloud_code": "CLR",
            "latest_cloud_lowest_base_ft": 8000,
            "latest_wx": "",
            "cloud_effective_cover_smooth": 0.05,
            "radiation_eff_smooth": 0.55,
            "cloud_trend": "稳定",
            "latest_precip_state": "none",
            "precip_trend": "none",
            "temp_trend_1step_c": -0.5,
            "temp_trend_smooth_c": -0.4,
            "temp_bias_c": -0.2,
            "temp_accel_2step_c": -0.08,
            "observed_max_temp_c": 24.2,
            "observed_max_time_local": "2026-03-08T17:20:00+00:00",
            "observed_max_interval_lo_c": 24.0,
            "observed_max_interval_hi_c": 24.5,
            "metar_temp_quantized": False,
            "metar_routine_cadence_min": 30,
            "metar_recent_interval_min": 30,
            "peak_lock_confirmed": True,
        }
        forecast_decision = {
            "meta": {
                "station": "LTAC",
                "date": "2026-03-08",
                "model": "ecmwf",
                "synoptic_provider": "ecmwf-open-data",
                "runtime": "2026030800Z",
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
            temp_unit="C",
            temp_shape_analysis={
                "forecast": {
                    "shape_type": "single_peak",
                    "multi_peak_state": "none",
                    "plateau_state": "narrow",
                },
                "observed": {
                    "plateau_state": "none",
                    "hold_duration_hours": 0.0,
                },
            },
        )
        features = build_posterior_feature_vector(
            canonical_raw_state=canonical,
            temp_phase_decision={
                "phase": "post",
                "display_phase": "post",
                "short_term_state": "fading",
                "daily_peak_state": "locked",
                "second_peak_potential": "none",
                "rebound_mode": "none",
                "dominant_shape": "single_peak_tail",
                "plateau_hold_state": "none",
            },
        )
        posterior = build_weather_posterior(
            canonical_raw_state=canonical,
            posterior_feature_vector=features,
            quality_snapshot=build_quality_snapshot(
                canonical_raw_state=canonical,
                posterior_feature_vector=features,
            ),
        )

        self.assertLessEqual(posterior["quantiles"]["p90_c"], 24.45)
        self.assertLessEqual(posterior["quantiles"]["p75_c"], 24.35)
        self.assertLessEqual(posterior["calibration"]["upper_tail_cap_c"], 24.45)
        self.assertEqual(posterior["range_hint_meta"]["source"], "posterior_quantiles_progress_capped")
        self.assertLessEqual(posterior["range_hint"]["display"]["hi_c"], 24.5)
        self.assertGreaterEqual(posterior["range_hint"]["display"]["lo_c"], 24.0)

    def test_second_peak_watch_keeps_display_hint_from_overcapping(self) -> None:
        primary_window = {
            "start_local": "2026-03-09T13:00",
            "peak_local": "2026-03-09T14:00",
            "end_local": "2026-03-09T16:00",
            "peak_temp_c": 24.0,
            "low_cloud_pct": 18.0,
            "w850_kmh": 22.0,
        }
        metar_diag = {
            "latest_report_local": "2026-03-09T15:40:00+00:00",
            "latest_temp": 23.4,
            "latest_dewpoint": 8.0,
            "latest_rh": 44.0,
            "latest_wspd": 8.0,
            "latest_wdir": 180.0,
            "latest_cloud_code": "FEW",
            "latest_cloud_lowest_base_ft": 5000,
            "latest_wx": "",
            "cloud_effective_cover_smooth": 0.16,
            "radiation_eff_smooth": 0.70,
            "cloud_trend": "stable",
            "latest_precip_state": "none",
            "precip_trend": "none",
            "temp_trend_smooth_c": -0.05,
            "temp_bias_smooth_c": -0.10,
            "temp_accel_2step_c": -0.02,
            "observed_max_temp_c": 23.6,
            "observed_max_time_local": "2026-03-09T15:10:00+00:00",
            "observed_max_interval_lo_c": 23.4,
            "observed_max_interval_hi_c": 23.8,
            "metar_temp_quantized": False,
            "metar_routine_cadence_min": 30,
            "metar_recent_interval_min": 30,
            "analysis_window_mode": "obs_plateau_reanchor",
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
                "h850": {"review": {"thermal_advection_state": "probable", "transport_state": "neutral"}},
                "sounding": {"thermo": {"coverage": {"density_class": "moderate"}}},
            },
        }

        canonical = build_canonical_raw_state(
            primary_window=primary_window,
            metar_diag=metar_diag,
            forecast_decision=forecast_decision,
            temp_unit="C",
            temp_shape_analysis={
                "forecast": {
                    "shape_type": "double_peak",
                    "multi_peak_state": "possible",
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
            temp_phase_decision={
                "phase": "in_window",
                "display_phase": "in_window",
                "short_term_state": "holding",
                "daily_peak_state": "open",
                "second_peak_potential": "moderate",
                "rebound_mode": "second_peak",
                "dominant_shape": "double_peak",
                "plateau_hold_state": "holding",
            },
        )
        posterior = build_weather_posterior(
            canonical_raw_state=canonical,
            posterior_feature_vector=features,
            quality_snapshot=build_quality_snapshot(
                canonical_raw_state=canonical,
                posterior_feature_vector=features,
            ),
        )

        self.assertEqual(posterior["range_hint_meta"]["source"], "posterior_quantiles")

    def test_obs_matched_ensemble_subset_becomes_active_posterior_source(self) -> None:
        primary_window = {
            "start_local": "2026-03-09T12:00",
            "peak_local": "2026-03-09T15:30",
            "end_local": "2026-03-09T17:30",
            "peak_temp_c": 24.4,
            "low_cloud_pct": 18.0,
            "w850_kmh": 24.0,
        }
        metar_diag = {
            "latest_report_local": "2026-03-09T12:30:00+00:00",
            "latest_temp": 20.4,
            "latest_dewpoint": 7.6,
            "latest_rh": 43.0,
            "latest_wspd": 8.0,
            "latest_wdir": 180.0,
            "latest_cloud_code": "FEW",
            "latest_cloud_lowest_base_ft": 4800,
            "latest_wx": "",
            "cloud_effective_cover_smooth": 0.16,
            "radiation_eff_smooth": 0.85,
            "cloud_trend": "cloud thinning",
            "latest_precip_state": "none",
            "precip_trend": "none",
            "temp_trend_smooth_c": 0.32,
            "temp_bias_smooth_c": 0.22,
            "temp_accel_2step_c": 0.03,
            "observed_max_temp_c": 20.4,
            "observed_max_time_local": "2026-03-09T12:30:00+00:00",
            "observed_max_interval_lo_c": 20.2,
            "observed_max_interval_hi_c": 20.7,
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
                "synoptic_provider_fallback": False,
            },
            "features": {
                "objects_3d": {"tracks": [], "count": 0, "anchors_count": 0},
                "h850": {
                    "review": {
                        "thermal_advection_state": "probable",
                        "transport_state": "warm",
                        "surface_coupling_state": "partial",
                        "surface_role": "background",
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
            temp_shape_analysis={
                "forecast": {
                    "shape_type": "single_peak",
                    "multi_peak_state": "none",
                    "plateau_state": "narrow",
                },
                "observed": {
                    "plateau_state": "none",
                    "hold_duration_hours": 0.0,
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
                "dominant_shape": "single_peak",
                "plateau_hold_state": "none",
            },
        )
        posterior = build_weather_posterior(
            canonical_raw_state=canonical,
            posterior_feature_vector=features,
            quality_snapshot=build_quality_snapshot(
                canonical_raw_state=canonical,
                posterior_feature_vector=features,
            ),
        )

        self.assertEqual(posterior["core"]["anchor"]["ensemble_active_source"], "matched_subset")
        self.assertTrue(posterior["core"]["anchor"]["ensemble_matched_subset_active"])
        self.assertEqual(posterior["core"]["anchor"]["ensemble_dominant_path"], "warm_support")
        self.assertEqual(posterior["core"]["anchor"]["ensemble_full_dominant_path"], "cold_suppression")
        self.assertIn("ensemble_obs_matched_subset", posterior["reason_codes"])
        self.assertLess(posterior["core"]["path_context"]["upper_tail_allowance_adjust_c"], 0.0)
        self.assertIn("接地", posterior["core"]["path_context"]["significant_forecast_detail_text"])
        self.assertLessEqual(posterior["calibration"]["cold_tail_allowance_c"], 0.0)

    def test_cold_path_significant_detail_expands_cold_tail_allowance(self) -> None:
        primary_window = {
            "start_local": "2026-03-09T14:00",
            "peak_local": "2026-03-09T16:00",
            "end_local": "2026-03-09T17:30",
            "peak_temp_c": 24.8,
            "low_cloud_pct": 82.0,
            "w850_kmh": 16.0,
        }
        metar_diag = {
            "latest_report_local": "2026-03-09T14:40:00+00:00",
            "latest_temp": 24.0,
            "latest_dewpoint": 19.0,
            "latest_rh": 74.0,
            "latest_wspd": 7.0,
            "latest_wdir": 70.0,
            "latest_cloud_code": "OVC",
            "latest_cloud_lowest_base_ft": 1400,
            "latest_wx": "TSRA",
            "cloud_effective_cover_smooth": 0.86,
            "radiation_eff_smooth": 0.36,
            "cloud_trend": "overcast holding",
            "latest_precip_state": "convective",
            "precip_trend": "steady",
            "temp_trend_smooth_c": -0.28,
            "temp_bias_smooth_c": -0.24,
            "temp_accel_2step_c": -0.08,
            "observed_max_temp_c": 24.2,
            "observed_max_time_local": "2026-03-09T14:10:00+00:00",
            "observed_max_interval_lo_c": 24.0,
            "observed_max_interval_hi_c": 24.5,
            "metar_temp_quantized": False,
            "metar_routine_cadence_min": 30,
            "metar_recent_interval_min": 30,
        }
        forecast_decision = {
            "meta": {
                "station": "ZSPD",
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
                "synoptic_provider_fallback": False,
            },
            "features": {
                "objects_3d": {"tracks": [], "count": 0, "anchors_count": 0},
                "h850": {
                    "review": {
                        "thermal_advection_state": "confirmed",
                        "transport_state": "cold",
                        "surface_coupling_state": "strong",
                        "surface_role": "influence",
                        "surface_bias": "cold",
                    }
                },
                "h925": {
                    "coupling_state": "strong",
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
                    "dominant_path": "cold_suppression",
                    "dominant_path_detail": "cold_suppression",
                    "dominant_prob": 0.72,
                    "dominant_detail_prob": 0.72,
                    "dominant_margin_prob": 0.44,
                    "split_state": "clustered",
                    "transition_detail": "weak_cold_transition",
                    "transition_detail_prob": 0.12,
                },
                "probabilities": {
                    "warm_support": 0.10,
                    "transition": 0.18,
                    "cold_suppression": 0.72,
                },
                "diagnostics": {
                    "delta_t850_p10_c": -1.2,
                    "delta_t850_p50_c": -0.8,
                    "delta_t850_p90_c": -0.2,
                    "wind850_p50_kmh": 18.0,
                    "neutral_stable_prob": 0.06,
                    "weak_warm_transition_prob": 0.0,
                    "weak_cold_transition_prob": 0.12,
                },
                "source": {"provider": "ecmwf-ens-open-data"},
            },
            temp_unit="C",
            temp_shape_analysis={
                "forecast": {
                    "shape_type": "single_peak",
                    "multi_peak_state": "none",
                    "plateau_state": "narrow",
                },
                "observed": {
                    "plateau_state": "none",
                    "hold_duration_hours": 0.0,
                },
            },
        )
        features = build_posterior_feature_vector(
            canonical_raw_state=canonical,
            temp_phase_decision={
                "phase": "in_window",
                "display_phase": "in_window",
                "short_term_state": "fading",
                "daily_peak_state": "open",
                "second_peak_potential": "none",
                "rebound_mode": "none",
                "dominant_shape": "single_peak",
                "plateau_hold_state": "none",
            },
        )
        posterior = build_weather_posterior(
            canonical_raw_state=canonical,
            posterior_feature_vector=features,
            quality_snapshot=build_quality_snapshot(
                canonical_raw_state=canonical,
                posterior_feature_vector=features,
            ),
        )

        self.assertGreater(posterior["core"]["path_context"]["cold_tail_allowance_c"], 0.0)
        self.assertIn("对流压温", posterior["core"]["path_context"]["significant_forecast_detail_text"])
        self.assertGreater(posterior["calibration"]["cold_tail_allowance_c"], 0.12)


if __name__ == "__main__":
    unittest.main()
