import sys
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
SCRIPTS = ROOT / "scripts"
if str(SCRIPTS) not in sys.path:
    sys.path.insert(0, str(SCRIPTS))

from report_focus_service import build_report_focus_bundle  # noqa: E402
from synoptic_summary_service import build_synoptic_summary  # noqa: E402


class ReportFocusAndSynopticSummaryTest(unittest.TestCase):
    def test_synoptic_summary_prefers_boundary_layer_process_wording(self) -> None:
        summary = build_synoptic_summary(
            primary_window={},
            metar_diag={"latest_wdir": None, "latest_wspd": 3.0},
            syn_w={"start_local": "2026-03-09T14:00", "end_local": "2026-03-09T17:00"},
            calc_window={"low_cloud_pct": 90.0},
            obj={},
            candidates=[],
            cov=1.0,
            line500="",
            h500_feature={"regime_label": "近区槽脊过渡", "impact_weight": "medium", "tmax_weight_score": 0.25},
            line850="",
            advection_review={},
            extra="低云与湿层维持",
            h700_summary="700hPa 湿层约束偏强，云量维持能力高",
            h925_summary="",
            snd_thermo={"profile_source": "model_proxy"},
            cloud_code_now="OVC",
            boundary_layer_regime={"regime_key": "boundary_layer_clearing"},
        )

        joined = "\n".join(summary["lines"])
        self.assertIn("主导机制", joined)
        self.assertIn("相关链路", joined)
        self.assertIn("低云雾何时抬升和破碎更关键", joined)
        self.assertNotIn("高空动力触发（低层受封盖约束", joined)
        self.assertNotIn("关键证据", joined)
        self.assertNotIn("峰值窗口", joined)
        self.assertLessEqual(len(summary["lines"]), 4)

    def test_report_focus_does_not_frame_overnight_high_as_new_high_watch(self) -> None:
        bundle = build_report_focus_bundle(
            primary_window={"peak_local": "2026-03-09T14:00"},
            metar_diag={"metar_routine_cadence_min": 30},
            analysis_snapshot={
                "temp_phase_decision": {
                    "display_phase": "far",
                    "daily_peak_state": "open",
                    "short_term_state": "holding",
                    "timing": {
                        "before_typical_peak": True,
                        "overnight_carryover_high": True,
                    },
                    "shape": {
                        "future_candidate_role": "primary_remaining_peak",
                    },
                },
                "weather_posterior": {
                    "event_probs": {
                        "new_high_next_60m": 0.82,
                        "lock_by_window_end": 0.15,
                    }
                },
                "quality_snapshot": {"scores": {"confidence_label": "medium"}},
                "peak_data": {"summary": {"consistency": {}, "confidence": {}, "phase_now": "far"}},
                "boundary_layer_regime": {"tracking_line": "优先看低云底是否抬升。", "regime_key": "boundary_layer_clearing", "thermo": {"vertical_regime": "low_cloud_clearing"}},
                "condition_state": {},
            },
        )

        joined = "\n".join(bundle["vars_block"])
        self.assertIn("白天主峰仍未到来", joined)
        self.assertNotIn("再创新高空间", joined)
        self.assertEqual(len(bundle["vars_block"]), 3)

    def test_report_focus_avoids_low_cloud_wording_outside_low_cloud_regime(self) -> None:
        bundle = build_report_focus_bundle(
            primary_window={"peak_local": "2026-03-09T14:00"},
            metar_diag={"metar_routine_cadence_min": 30},
            analysis_snapshot={
                "temp_phase_decision": {
                    "display_phase": "far",
                    "daily_peak_state": "open",
                    "short_term_state": "holding",
                    "timing": {"before_typical_peak": True, "overnight_carryover_high": True},
                    "shape": {"future_candidate_role": "primary_remaining_peak"},
                },
                "weather_posterior": {"event_probs": {"new_high_next_60m": 0.72}},
                "quality_snapshot": {"scores": {"confidence_label": "medium"}},
                "peak_data": {"summary": {"consistency": {}, "confidence": {}, "phase_now": "far"}},
                "boundary_layer_regime": {
                    "tracking_line": "优先看近地风场和温度斜率是否继续配合。",
                    "regime_key": "synoptic",
                    "thermo": {"vertical_regime": "dry_clear_mixed"},
                },
                "condition_state": {},
            },
        )

        joined = "\n".join(bundle["vars_block"])
        self.assertIn("白天主峰仍未到来，先看后段升温何时真正展开", joined)
        self.assertNotIn("低云/雾层何时真正松动", joined)

    def test_report_focus_prefers_directional_fallback_over_cadence_note(self) -> None:
        bundle = build_report_focus_bundle(
            primary_window={"peak_local": "2026-03-09T14:00"},
            metar_diag={"metar_routine_cadence_min": 30},
            analysis_snapshot={
                "temp_phase_decision": {
                    "display_phase": "far",
                    "daily_peak_state": "open",
                    "short_term_state": "holding",
                    "timing": {},
                    "shape": {},
                },
                "weather_posterior": {"event_probs": {}},
                "quality_snapshot": {"scores": {"confidence_label": "medium"}},
                "peak_data": {"summary": {"consistency": {}, "confidence": {}, "phase_now": "far"}},
                "boundary_layer_regime": {"tracking_line": ""},
                "condition_state": {},
            },
        )

        joined = "\n".join(bundle["vars_block"])
        self.assertIn("优先看下一报温度斜率、风向节奏和云量是否继续支持当前路径", joined)
        self.assertNotIn("该站常规约每30分钟一报", joined)
        self.assertEqual(len(bundle["vars_block"]), 2)

    def test_synoptic_summary_skips_generic_front_wording_in_weak_background_case(self) -> None:
        summary = build_synoptic_summary(
            primary_window={},
            metar_diag={"latest_wdir": 70.0, "latest_wspd": 14.0},
            syn_w={"start_local": "2026-03-09T15:00", "end_local": "2026-03-09T17:00"},
            calc_window={"low_cloud_pct": 12.0},
            obj={},
            candidates=[],
            cov=1.0,
            line500="",
            h500_feature={"regime_label": "高空弱信号背景", "impact_weight": "low", "tmax_weight_score": 0.08},
            line850="",
            advection_review={"thermal_advection_state": "weak", "transport_state": "cold", "surface_role": "background"},
            extra="弱斜压过渡",
            h700_summary="",
            h925_summary="",
            snd_thermo={"profile_source": "model_proxy"},
            cloud_code_now="FEW",
            boundary_layer_regime={
                "regime_key": "synoptic",
                "headline": "今天没有特别单一的主导因素，先看午后升温效率能否继续维持，再看低层风场能否继续带动升温；这会一起决定午后还能不能继续升温。",
                "layer_summary": "低层空气不太容易完全混匀，午后升温更要看升温势头能否维持。",
            },
        )

        joined = "\n".join(summary["lines"])
        self.assertNotIn("锋性过渡", joined)
        self.assertNotIn("相关链路", joined)
        self.assertNotIn("今天没有特别单一的主导因素", joined)
        self.assertIn("后段更要看午后升温效率能否继续维持，以及低层风场能否继续带动升温", joined)
        self.assertIn("上沿有一点受压风险", joined)


if __name__ == "__main__":
    unittest.main()
