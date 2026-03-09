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
from weather_posterior_service import build_weather_posterior  # noqa: E402


class WeatherPosteriorServiceTest(unittest.TestCase):
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


if __name__ == "__main__":
    unittest.main()
