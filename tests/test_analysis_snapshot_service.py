import sys
import unittest
from pathlib import Path
from unittest.mock import patch


ROOT = Path(__file__).resolve().parents[1]
SCRIPTS = ROOT / "scripts"
if str(SCRIPTS) not in sys.path:
    sys.path.insert(0, str(SCRIPTS))

from analysis_snapshot_service import build_analysis_snapshot  # noqa: E402
from report_render_service import choose_section_text  # noqa: E402


class AnalysisSnapshotServiceTest(unittest.TestCase):
    def _sample_inputs(self):
        primary_window = {
            "start_local": "2026-03-09T11:00",
            "peak_local": "2026-03-09T14:00",
            "end_local": "2026-03-09T16:00",
            "peak_temp_c": 24.0,
            "low_cloud_pct": 18.0,
            "w850_kmh": 22.0,
        }
        metar_diag = {
            "latest_report_local": "2026-03-09T11:30:00+00:00",
            "latest_temp": 19.4,
            "observed_max_temp_c": 19.4,
            "observed_max_time_local": "2026-03-09T11:30:00+00:00",
            "latest_cloud_code": "SCT",
            "latest_precip_state": "none",
            "precip_trend": "none",
            "latest_wspd": 6.0,
            "latest_wdir": 180.0,
            "latest_rh": 42.0,
            "latest_dewpoint": 6.0,
            "latest_wx": "",
            "cloud_trend": "cloud slowly thinning",
            "temp_trend_smooth_c": 0.3,
            "temp_bias_smooth_c": 0.2,
            "cloud_effective_cover_smooth": 0.25,
            "radiation_eff_smooth": 0.82,
            "wind_dir_change_deg": 10.0,
        }
        forecast_decision = {
            "meta": {"window": dict(primary_window)},
            "quality": {
                "source_state": "fresh",
                "missing_layers": [],
                "synoptic_coverage": 1.0,
            },
            "features": {
                "objects_3d": {"candidates": []},
                "h500": {
                    "regime_label": "副高边缘",
                    "impact_weight": "medium",
                    "tmax_weight_score": 0.32,
                },
                "h850": {
                    "review": {
                        "has_signal": True,
                        "surface_role": "influence",
                        "surface_bias": "warm",
                        "surface_effect_weight": 0.4,
                    }
                },
                "h700": {"summary": "700hPa 干层特征偏明显"},
                "h925": {"summary": "925层耦合偏强"},
                "sounding": {
                    "thermo": {
                        "profile_source": "model_proxy",
                        "sounding_confidence": "medium",
                        "low_level_cap_score": 0.22,
                        "mixing_support_score": 0.68,
                        "midlevel_dry_score": 0.42,
                        "midlevel_moist_score": 0.08,
                        "wind_profile_mix_score": 0.45,
                        "layer_findings": ["925–850混合较顺畅。"],
                    }
                },
            },
            "decision": {
                "object_3d_main": {
                    "type": "advection_3d",
                    "confidence": "high",
                    "impact_scope": "station_relevant",
                    "vertical_coherence_score": 0.8,
                    "surface_coupling_score": 0.72,
                    "distance_km_min": 120.0,
                    "evolution": "approaching",
                    "rank_score": 3.4,
                    "evidence": {"support": ["系统贴近站点"], "conflict": []},
                },
                "background": {
                    "line_500": "高空暖脊背景仍在。",
                    "line_850": "850暖平流可部分落地。",
                    "extra": "700干层有利日照",
                },
            },
        }
        return primary_window, metar_diag, forecast_decision

    def test_snapshot_drives_render_headline(self) -> None:
        primary_window, metar_diag, forecast_decision = self._sample_inputs()

        snapshot = build_analysis_snapshot(
            primary_window=primary_window,
            metar_diag=metar_diag,
            forecast_decision=forecast_decision,
            temp_unit="C",
        )

        self.assertIn("condition_state", snapshot)
        self.assertIn("boundary_layer_regime", snapshot)
        self.assertIn("temp_phase_decision", snapshot)
        self.assertIn("peak_data", snapshot)
        self.assertIn("synoptic_summary", snapshot)
        self.assertIn("canonical_raw_state", snapshot)
        self.assertIn("posterior_feature_vector", snapshot)
        self.assertIn("quality_snapshot", snapshot)
        self.assertIn("weather_posterior", snapshot)
        self.assertIn("ensemble_factor", snapshot)
        self.assertIn("summary", snapshot["peak_data"])
        self.assertIn("block", snapshot["peak_data"])
        self.assertTrue(any("相关链路" in line for line in snapshot["synoptic_summary"]["lines"]))
        self.assertEqual(snapshot["schema_version"], "analysis-snapshot.v7")
        self.assertEqual(snapshot["canonical_raw_state"]["schema_version"], "canonical-raw-state.v3")
        self.assertEqual(snapshot["posterior_feature_vector"]["schema_version"], "posterior-feature-vector.v3")
        self.assertEqual(snapshot["quality_snapshot"]["schema_version"], "quality-snapshot.v2")
        self.assertEqual(snapshot["weather_posterior"]["schema_version"], "weather-posterior.v1")
        self.assertNotIn("headline", snapshot["posterior_feature_vector"].get("regime_state", {}))
        self.assertIn("observation_state", snapshot["posterior_feature_vector"])
        self.assertIn("quantiles", snapshot["weather_posterior"])

        snapshot["boundary_layer_regime"]["headline"] = "测试主导机制"
        snapshot["posterior_feature_vector"]["time_phase"]["hours_to_peak"] = 18.0
        rendered = choose_section_text(
            primary_window,
            "样例 METAR 文本",
            metar_diag,
            "",
            forecast_decision=forecast_decision,
            analysis_snapshot=snapshot,
        )

        self.assertIn("🧭 形势与路径：", rendered)

    def test_market_range_hint_uses_peak_block_ranges(self) -> None:
        primary_window, metar_diag, forecast_decision = self._sample_inputs()
        snapshot = build_analysis_snapshot(
            primary_window=primary_window,
            metar_diag=metar_diag,
            forecast_decision=forecast_decision,
            temp_unit="C",
        )
        snapshot["peak_data"]["summary"]["ranges"]["display"]["lo"] = 7.0
        snapshot["peak_data"]["summary"]["ranges"]["display"]["hi"] = 7.2
        snapshot["peak_data"]["summary"]["ranges"]["core"]["lo"] = 7.05
        snapshot["peak_data"]["summary"]["ranges"]["core"]["hi"] = 7.2
        snapshot["weather_posterior"]["range_hint"] = {
            "display": {"lo_c": 7.8, "hi_c": 9.0},
            "core": {"lo_c": 8.0, "hi_c": 8.8},
        }

        with patch("report_render_service._build_polymarket_section", return_value="") as mock_poly:
            choose_section_text(
                primary_window,
                "样例 METAR 文本",
                metar_diag,
                "https://polymarket.com/event/test",
                forecast_decision=forecast_decision,
                analysis_snapshot=snapshot,
            )

        kwargs = mock_poly.call_args.kwargs
        self.assertEqual(
            kwargs["range_hint"],
            {
                "display_lo": 7.0,
                "display_hi": 7.2,
                "core_lo": 7.05,
                "core_hi": 7.2,
            },
        )
        self.assertEqual(kwargs["weather_posterior"], snapshot["weather_posterior"])

    def test_peak_ranges_are_sourced_from_weather_posterior_quantiles(self) -> None:
        primary_window, metar_diag, forecast_decision = self._sample_inputs()

        snapshot = build_analysis_snapshot(
            primary_window=primary_window,
            metar_diag=metar_diag,
            forecast_decision=forecast_decision,
            temp_unit="C",
        )

        peak_ranges = snapshot["peak_data"]["summary"]["ranges"]
        posterior_hint = snapshot["weather_posterior"]["range_hint"]
        self.assertEqual(snapshot["peak_data"]["summary"]["range_truth_source"], "weather_posterior")
        self.assertEqual(peak_ranges["source"], "posterior_quantiles")
        self.assertAlmostEqual(peak_ranges["core"]["lo"], posterior_hint["core"]["lo_c"], places=2)
        self.assertAlmostEqual(peak_ranges["core"]["hi"], posterior_hint["core"]["hi_c"], places=2)
        self.assertGreaterEqual(peak_ranges["display"]["lo"], posterior_hint["display"]["lo_c"])
        self.assertLessEqual(peak_ranges["display"]["hi"], posterior_hint["display"]["hi_c"])
        self.assertAlmostEqual(peak_ranges["display"]["lo"], peak_ranges["core"]["lo"], places=2)
        self.assertAlmostEqual(peak_ranges["display"]["hi"], peak_ranges["core"]["hi"], places=2)
        self.assertGreaterEqual(peak_ranges["posterior_tail_weight"], 0.0)
        self.assertLessEqual(peak_ranges["posterior_tail_weight"], 1.0)

    def test_post_window_snapshot_keeps_settled_obs_anchor_range(self) -> None:
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
            "observed_max_time_local": "2026-03-08T17:20:00+00:00",
            "observed_max_temp_c": 24.2,
            "observed_max_interval_lo_c": 24.0,
            "observed_max_interval_hi_c": 24.5,
            "latest_temp": 23.2,
            "latest_cloud_code": "CLR",
            "latest_precip_state": "none",
            "precip_trend": "none",
            "cloud_trend": "稳定",
            "temp_trend_1step_c": -0.5,
            "temp_trend_smooth_c": -0.4,
            "temp_bias_c": -0.2,
            "peak_lock_confirmed": True,
            "latest_wspd": 4.0,
            "latest_wdir": 200.0,
            "latest_rh": 35.0,
            "latest_dewpoint": 7.0,
            "latest_wx": "",
            "cloud_effective_cover_smooth": 0.05,
            "radiation_eff_smooth": 0.55,
            "metar_routine_cadence_min": 30,
            "metar_recent_interval_min": 30,
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
                "synoptic_provider_fallback": False,
            },
            "features": {
                "objects_3d": {"tracks": [], "count": 0, "anchors_count": 0},
                "h850": {"review": {"thermal_advection_state": "none", "transport_state": "neutral"}},
                "sounding": {"thermo": {"coverage": {"density_class": "moderate"}}},
            },
        }

        snapshot = build_analysis_snapshot(
            primary_window=primary_window,
            metar_diag=metar_diag,
            forecast_decision=forecast_decision,
            temp_unit="C",
        )

        settled = snapshot["peak_data"]["summary"]["ranges"]["settled"]
        self.assertTrue(settled["active"])
        self.assertLessEqual(settled["hi"], 24.5)
        self.assertGreaterEqual(settled["lo"], 24.0)

    def test_near_end_flat_path_caps_posterior_rewidening(self) -> None:
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
            "observed_max_time_local": "2026-03-09T15:10:00+00:00",
            "observed_max_temp_c": 23.6,
            "observed_max_interval_lo_c": 23.4,
            "observed_max_interval_hi_c": 23.8,
            "latest_temp": 23.4,
            "latest_cloud_code": "FEW",
            "latest_precip_state": "none",
            "precip_trend": "none",
            "cloud_trend": "stable",
            "temp_trend_smooth_c": -0.05,
            "temp_bias_smooth_c": -0.10,
            "temp_accel_2step_c": -0.02,
            "peak_lock_confirmed": False,
            "latest_wspd": 8.0,
            "latest_wdir": 180.0,
            "latest_rh": 44.0,
            "latest_dewpoint": 8.0,
            "latest_wx": "",
            "latest_cloud_lowest_base_ft": 5000,
            "cloud_effective_cover_smooth": 0.16,
            "radiation_eff_smooth": 0.70,
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
                "h850": {"review": {"thermal_advection_state": "probable", "transport_state": "neutral"}},
                "sounding": {"thermo": {"coverage": {"density_class": "moderate"}}},
            },
        }

        snapshot = build_analysis_snapshot(
            primary_window=primary_window,
            metar_diag=metar_diag,
            forecast_decision=forecast_decision,
            temp_unit="C",
        )

        peak_ranges = snapshot["peak_data"]["summary"]["ranges"]
        posterior_hint = snapshot["weather_posterior"]["range_hint"]
        self.assertIn(
            peak_ranges["source"],
            {"posterior_quantiles", "posterior_quantiles_path_capped"},
        )
        self.assertLess(snapshot["weather_posterior"]["calibration"]["progress_spread_multiplier"], 1.0)
        self.assertLessEqual(peak_ranges["display"]["hi"], posterior_hint["core"]["hi_c"])
        self.assertLessEqual(peak_ranges["display"]["hi"], 23.8)
        self.assertGreaterEqual(peak_ranges["display"]["lo"], 23.4)


if __name__ == "__main__":
    unittest.main()
