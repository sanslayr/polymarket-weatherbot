import sys
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
SCRIPTS = ROOT / "scripts"
if str(SCRIPTS) not in sys.path:
    sys.path.insert(0, str(SCRIPTS))

from analysis_snapshot_service import build_analysis_snapshot  # noqa: E402
from report_render_service import choose_section_text  # noqa: E402


class AnalysisSnapshotServiceTest(unittest.TestCase):
    def test_snapshot_drives_render_headline(self) -> None:
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
        self.assertIn("summary", snapshot["peak_data"])
        self.assertIn("block", snapshot["peak_data"])
        self.assertTrue(any("主导系统" in line for line in snapshot["synoptic_summary"]["lines"]))

        snapshot["boundary_layer_regime"]["headline"] = "测试主导机制"
        rendered = choose_section_text(
            primary_window,
            "样例 METAR 文本",
            metar_diag,
            "",
            forecast_decision=forecast_decision,
            analysis_snapshot=snapshot,
        )

        self.assertIn("测试主导机制", rendered)


if __name__ == "__main__":
    unittest.main()
