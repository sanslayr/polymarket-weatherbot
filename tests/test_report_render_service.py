import sys
import unittest
from pathlib import Path
from unittest.mock import patch


ROOT = Path(__file__).resolve().parents[1]
SCRIPTS = ROOT / "scripts"
if str(SCRIPTS) not in sys.path:
    sys.path.insert(0, str(SCRIPTS))

from report_render_service import _background_compact_clause, _format_local_clock, _natural_flow_chain_line, choose_section_text  # noqa: E402


def _snapshot_template() -> dict:
    return {
        "synoptic_summary": {
            "lines": [
                "🧭 **环流形势对最高温影响**",
                "- **主导机制**：低层混合仍在加深。",
                "- 午后低层偏南风还能继续托住升温。",
                "- 云量约束不强，但后段上冲空间有限。",
            ],
            "summary": {
                "pathway": "低层偏暖输送（850-925hPa）更明确，若云量放开，升温会更顺。",
                "impact": "更可能比原先预报略高；影响会直接落在峰值窗。",
            },
        },
        "boundary_layer_regime": {
            "headline": "低层混合仍在加深，后段更要看偏南风能否继续稳住。",
            "tracking_line": "优先看温度斜率和低层风向是否继续配合。",
            "regime_key": "synoptic",
            "thermo": {"vertical_regime": "dry_clear_mixed"},
        },
        "temp_phase_decision": {"display_phase": "near_window"},
        "peak_data": {
            "summary": {
                "phase_now": "near_window",
                "ranges": {
                    "display": {"lo": 29.0, "hi": 31.0},
                    "core": {"lo": 29.5, "hi": 30.5},
                },
            },
            "block": [
                "📈 **峰值窗口判断**",
                "- 最高温大致落在 29-31°C。",
            ],
        },
        "weather_posterior": {"event_probs": {}},
        "posterior_feature_vector": {"time_phase": {"hours_to_peak": 2.5}},
        "quality_snapshot": {"scores": {"confidence_label": "medium"}},
        "condition_state": {},
    }


class ReportRenderServiceTest(unittest.TestCase):
    def test_format_local_clock_includes_date_for_non_current_local_day(self) -> None:
        self.assertEqual(
            _format_local_clock("2000-01-01T20:00:00+08:00"),
            "2000/01/01 20:00 Local",
        )

    def test_same_day_near_window_puts_background_first_and_keeps_original_metar_block(self) -> None:
        text = choose_section_text(
            primary_window={
                "peak_local": "2026-03-09T14:00",
                "start_local": "2026-03-09T13:00",
                "end_local": "2026-03-09T15:00",
                "peak_temp_c": 30.0,
            },
            metar_text="• **🌡️ 气温**：28.4°C（较上一报 +0.6°C）",
            metar_diag={
                "station_icao": "NZWN",
                "latest_report_local": "2026-03-09T11:30:00+00:00",
                "latest_temp": 28.4,
                "observed_max_temp_c": 28.4,
                "observed_max_time_local": "2026-03-09T11:30:00+00:00",
                "latest_wdir": 180,
                "latest_wspd": 8,
                "latest_cloud_tokens": ["SCT025"],
                "temp_trend_1step_c": 0.6,
            },
            polymarket_event_url="",
            compact_synoptic=True,
            analysis_snapshot=_snapshot_template(),
        )

        first_block = text.split("\n\n", 1)[0]
        self.assertTrue(first_block.startswith("🧭 背景："))
        self.assertNotIn("Wellington", first_block)
        self.assertNotIn("当前主要关注", first_block)
        self.assertIn("📡 **最新实况分析（METAR）**", text)
        self.assertIn("较上一报 +0.6°C", text)
        self.assertNotIn("⚠️ 关注", text)
        self.assertNotIn("**环流形势对最高温影响**", text)
        self.assertNotIn("**主导机制**", text)

    def test_far_window_prioritizes_synoptic_and_pushes_obs_to_reference(self) -> None:
        snapshot = _snapshot_template()
        snapshot["posterior_feature_vector"]["time_phase"]["hours_to_peak"] = 18.0
        snapshot["temp_phase_decision"] = {"display_phase": "far"}
        snapshot["peak_data"]["summary"]["phase_now"] = "far"
        snapshot["boundary_layer_regime"]["layer_summary"] = "低层空气不太容易完全混匀，午后升温更要看升温势头能否维持。"
        snapshot["synoptic_summary"]["summary"]["pathway"] = "暂未识别到单独可追踪的近站系统。"
        snapshot["synoptic_summary"]["summary"]["impact"] = "最高温倾向略偏下沿。"

        text = choose_section_text(
            primary_window={
                "peak_local": "2026-03-10T14:00",
                "start_local": "2026-03-10T13:00",
                "end_local": "2026-03-10T15:00",
                "peak_temp_c": 30.0,
            },
            metar_text="- 当前实况平稳。",
            metar_diag={
                "station_icao": "RJTT",
                "latest_report_local": "2026-03-09T20:00:00+00:00",
                "latest_temp": 21.0,
                "observed_max_temp_c": 22.0,
                "observed_max_time_local": "2026-03-09T15:40:00+00:00",
                "latest_wdir": 180,
                "latest_wspd": 9,
                "latest_cloud_tokens": ["SCT025"],
                "historical_context": {
                    "summary_lines": [
                        "站点背景摘要：峰值偏早，后段常见的是锁温或回落，而不是持续上冲。",
                    ]
                },
            },
            polymarket_event_url="",
            compact_synoptic=False,
            analysis_snapshot=snapshot,
        )

        first_block = text.split("\n\n", 1)[0]
        self.assertTrue(first_block.startswith("🧭 形势与路径："))
        self.assertNotIn("Tokyo当前", first_block)
        self.assertNotIn("当前主看", first_block)
        self.assertTrue(
            "低层混合仍在加深" in first_block
            or "风向切换时点" in first_block
        )
        self.assertIn("数值预报峰值约 30.0°C", first_block)
        self.assertNotIn("暂未识别到单独可追踪的近站系统", first_block)
        self.assertNotIn("当前看", first_block)
        self.assertIn("📡 当前实况：", text)
        self.assertIn("21°C", text)
        self.assertNotIn("21.0°C", text)
        self.assertIn("今日已观测最高温：22°C（2026/03/09 15:40 Local）", text)
        self.assertNotIn("升温转弱", text)
        self.assertLess(text.index("📈 **峰值窗口判断**"), text.index("📡 当前实况："))

    def test_cross_day_far_synoptic_downgrades_obs_and_uses_forecast_rationale(self) -> None:
        snapshot = _snapshot_template()
        snapshot["posterior_feature_vector"]["time_phase"]["hours_to_peak"] = 12.0
        snapshot["canonical_raw_state"] = {
            "forecast": {
                "meta": {
                    "model": "ecmwf",
                    "runtime": "2026031112Z",
                },
                "context": {
                    "bottleneck_text": "925–850混合偏弱，低云破碎时点将决定上沿空间。",
                },
                "ensemble_factor": {
                    "summary": {
                        "dominant_path": "transition",
                        "dominant_path_detail": "neutral_stable",
                        "dominant_prob": 0.43,
                        "transition_detail": "neutral_stable",
                        "split_state": "split",
                        "signal_dispersion_c": 2.1,
                    },
                    "probabilities": {
                        "warm_support": 0.35,
                        "transition": 0.43,
                        "cold_suppression": 0.22,
                    },
                    "source": {
                        "runtime_used": "2026031112Z",
                    },
                },
                "h500": {
                    "thermal_role": "cold_high_suppression",
                    "tmax_bias_label": "明显压温支持",
                },
                "h850_review": {
                    "advection_type": "cold",
                    "surface_role": "background",
                },
                "sounding": {
                    "thermo": {
                        "layer_findings": [
                            "925–850混合偏弱，升温更依赖低云何时真正破碎。",
                        ],
                    }
                },
            },
            "window": {
                "calc": {
                    "start_local": "2026-03-10T13:00",
                    "end_local": "2026-03-10T15:00",
                    "peak_local": "2026-03-10T14:00",
                    "peak_temp_c": 30.0,
                }
            },
            "shape": {
                "forecast": {
                    "shape_type": "broad_plateau",
                    "plateau_state": "broad",
                    "day_range_c": 2.8,
                    "global_peak_temp_c": 30.0,
                }
            },
        }

        with patch(
            "report_render_service._build_polymarket_section",
            return_value=(
                "📈 **Polymarket 盘口与博弈**\n"
                "**博弈区间**\n"
                "  • **29°C（👍最有可能）：Bid 19¢ | Ask 20¢**"
            ),
        ):
            text = choose_section_text(
                primary_window={
                    "peak_local": "2026-03-10T14:00",
                    "start_local": "2026-03-10T13:00",
                    "end_local": "2026-03-10T15:00",
                    "peak_temp_c": 30.0,
                },
                metar_text="• **🌡️ 气温**：28.4°C（较上一报 +0.6°C）",
                metar_diag={
                    "station_icao": "NZWN",
                    "latest_report_local": "2026-03-09T11:30:00+00:00",
                    "latest_temp": 28.4,
                    "observed_max_temp_c": 28.4,
                    "observed_max_time_local": "2026-03-09T11:30:00+00:00",
                    "latest_wdir": 180,
                    "latest_wspd": 8,
                    "latest_cloud_tokens": ["SCT025"],
                    "temp_trend_1step_c": 0.6,
                },
                polymarket_event_url="https://polymarket.com/event/test",
                compact_synoptic=True,
                analysis_snapshot=snapshot,
            )

        first_block = text.split("\n\n", 1)[0]
        self.assertTrue(first_block.startswith("🧭 形势与路径（ECMWF 2026031112Z）："))
        self.assertIn("📡 当前实况：", text)
        self.assertNotIn("📡 **最新实况分析（METAR）**", text)
        self.assertNotIn("较上一报 +0.6°C", text)
        self.assertNotIn("🔀 系集路径：", text)
        self.assertNotIn("📈 单跑参考：", text)
        self.assertIn("ECMWF ENS 仍有分歧", text)
        self.assertIn("静稳维持", text)
        self.assertIn("ECMWF 数值预报峰值约 30.0°C", text)
        self.assertIn("今日已观测最高温：28.4°C（2026/03/09 11:30 Local）", text)
        self.assertNotIn("目标峰值窗仍远，区间先按模式峰值和环流修正理解", text)
        self.assertNotIn("最新报已经到", text)
        self.assertNotIn("**判断依据**", text)

    def test_near_window_drops_generic_background_when_no_clear_signal(self) -> None:
        snapshot = _snapshot_template()
        snapshot["boundary_layer_regime"]["headline"] = "当前更像低层风场和午后升温效率共同作用，先看后段升温能否继续维持。"
        snapshot["synoptic_summary"]["summary"]["pathway"] = "暂未识别到单独可追踪的近站系统。"
        snapshot["synoptic_summary"]["summary"]["impact"] = "暂时看不出明显偏高或偏低；短时改写幅度有限。"

        text = choose_section_text(
            primary_window={
                "peak_local": "2026-03-09T14:00",
                "start_local": "2026-03-09T13:00",
                "end_local": "2026-03-09T15:00",
                "peak_temp_c": 30.0,
            },
            metar_text="- 这里是完整 METAR 逐项分析。",
            metar_diag={
                "station_icao": "NZWN",
                "latest_report_local": "2026-03-09T11:30:00+00:00",
                "latest_temp": 21.0,
                "observed_max_temp_c": 21.0,
                "observed_max_time_local": "2026-03-09T11:30:00+00:00",
                "latest_wdir": 180,
                "latest_wspd": 9,
                "latest_cloud_tokens": ["SCT025"],
            },
            polymarket_event_url="",
            compact_synoptic=False,
            analysis_snapshot=snapshot,
        )

        self.assertTrue(text.startswith("📡 **最新实况分析（METAR）**"))
        self.assertNotIn("🧭 背景：", text)

    def test_near_window_drops_background_when_only_generic_mechanism_exists(self) -> None:
        snapshot = _snapshot_template()
        snapshot["boundary_layer_regime"]["headline"] = "当前更像低层混合仍在加深，后面主要看混合层还能不能继续做深。"
        snapshot["synoptic_summary"]["summary"]["pathway"] = "低层混合仍在加深。"
        snapshot["synoptic_summary"]["summary"]["impact"] = "短时改写幅度有限。"

        text = choose_section_text(
            primary_window={
                "peak_local": "2026-03-09T14:00",
                "start_local": "2026-03-09T13:00",
                "end_local": "2026-03-09T15:00",
                "peak_temp_c": 30.0,
            },
            metar_text="- 这里是完整 METAR 逐项分析。",
            metar_diag={
                "station_icao": "NZWN",
                "latest_report_local": "2026-03-09T11:30:00+00:00",
                "latest_temp": 21.0,
                "observed_max_temp_c": 21.0,
                "observed_max_time_local": "2026-03-09T11:30:00+00:00",
                "latest_wdir": 180,
                "latest_wspd": 9,
                "latest_cloud_tokens": ["SCT025"],
            },
            polymarket_event_url="",
            compact_synoptic=False,
            analysis_snapshot=snapshot,
        )

        self.assertTrue(text.startswith("📡 **最新实况分析（METAR）**"))
        self.assertNotIn("🧭 背景：", text)

    def test_natural_flow_chain_line_avoids_mixing_attention_wrapper_with_full_mechanism_clause(self) -> None:
        line = _natural_flow_chain_line("Munich", "云量演变仍是关键变量", "午后上沿更容易被压住")
        self.assertEqual(line, "• Munich当前云量演变仍是关键变量。")
        self.assertNotIn("Munich当前主要关注云量演变仍是关键变量", line)

    def test_background_compact_clause_keeps_only_mechanism_and_direction(self) -> None:
        clause = _background_compact_clause("暖空气输送仍在建立", "若云量继续放开，上沿仍有小幅上修空间")
        self.assertEqual(clause, "暖空气输送仍在建立；若云量继续放开，上沿仍有小幅上修空间")

    def test_range_rationale_is_inserted_before_polymarket_block(self) -> None:
        snapshot = _snapshot_template()
        snapshot["synoptic_summary"]["summary"]["pathway"] = "锋后偏南气流仍在。"
        snapshot["synoptic_summary"]["summary"]["impact"] = "午后上冲空间偏受限。"
        snapshot["peak_data"]["summary"]["ranges"]["display"] = {"lo": 16.4, "hi": 18.4}
        snapshot["peak_data"]["summary"]["ranges"]["core"] = {"lo": 17.0, "hi": 18.4}
        with unittest.mock.patch(
            "report_render_service._build_polymarket_section",
            return_value=(
                "📈 **Polymarket 盘口与博弈**\n"
                "**博弈区间**\n"
                "  • **17°C（😇潜在Alpha）：Bid 4.8¢ | Ask 6.8¢**\n"
                "  • **18°C（👍最有可能）：Bid 19¢ | Ask 20¢**"
            ),
        ):
            text = choose_section_text(
                primary_window={
                    "peak_local": "2026-03-09T14:00",
                    "start_local": "2026-03-09T13:00",
                    "end_local": "2026-03-09T15:00",
                    "peak_temp_c": 30.0,
                },
                metar_text="• **🌡️ 气温**：28.4°C（较上一报 +0.6°C）",
                metar_diag={
                    "station_icao": "NZWN",
                    "latest_report_local": "2026-03-09T11:30:00+00:00",
                    "latest_temp": 16.0,
                    "observed_max_temp_c": 28.4,
                    "observed_max_time_local": "2026-03-09T11:30:00+00:00",
                    "temp_trend_1step_c": 0.0,
                },
                polymarket_event_url="https://polymarket.com/event/test",
                analysis_snapshot=snapshot,
            )

        self.assertIn("**判断依据**", text)
        self.assertNotIn("最新报还在 16°C 一带横着走", text)
        self.assertIn("区间先放在 17.0~18.4°C，下沿留给偏冷回摆", text)
        self.assertIn("在锋后偏南气流持续维持的前提下", text)
        self.assertLess(text.index("**判断依据**"), text.index("📈 **Polymarket 盘口与博弈**"))

    def test_transition_window_can_promote_to_near_obs_on_live_signal(self) -> None:
        snapshot = _snapshot_template()
        snapshot["posterior_feature_vector"]["time_phase"]["hours_to_peak"] = 8.0
        snapshot["peak_data"]["summary"]["ranges"]["core"] = {"lo": 17.5, "hi": 18.3}
        snapshot["synoptic_summary"]["summary"]["pathway"] = "锋后偏南气流仍在。"

        text = choose_section_text(
            primary_window={
                "peak_local": "2026-03-09T18:00",
                "start_local": "2026-03-09T16:00",
                "end_local": "2026-03-09T20:00",
                "peak_temp_c": 18.0,
            },
            metar_text="**最新报：14:00 Local**（上一报 13:30）",
            metar_diag={
                "station_icao": "NZWN",
                "latest_report_local": "2026-03-09T14:00:00+00:00",
                "latest_temp": 17.0,
                "observed_max_temp_c": 17.0,
                "observed_max_time_local": "2026-03-09T14:00:00+00:00",
                "temp_trend_1step_c": 0.6,
            },
            polymarket_event_url="",
            analysis_snapshot=snapshot,
        )

        self.assertIn("📡 **最新实况分析（METAR）**", text)
        self.assertNotIn("📡 当前实况：", text)

    def test_transition_rationale_prefers_targeted_synoptic_basis_when_peak_is_still_far(self) -> None:
        snapshot = _snapshot_template()
        snapshot["posterior_feature_vector"]["time_phase"]["hours_to_peak"] = 7.0
        snapshot["synoptic_summary"]["summary"]["impact"] = "午后上沿更容易受压。"
        snapshot["canonical_raw_state"] = {
            "forecast": {
                "h500": {
                    "thermal_role": "cold_high_suppression",
                    "tmax_bias_label": "明显压温支持",
                },
                "h850_review": {
                    "advection_type": "cold",
                    "surface_role": "background",
                },
                "sounding": {
                    "thermo": {
                        "layer_findings": [
                            "925–850混合偏弱，升温更依赖低云何时真正破碎。",
                        ],
                    },
                },
            },
        }
        with patch(
            "report_render_service._build_polymarket_section",
            return_value=(
                "📈 **Polymarket 盘口与博弈**\n"
                "**博弈区间**\n"
                "  • **11°C（👍最有可能）：Bid 19¢ | Ask 20¢**"
            ),
        ):
            text = choose_section_text(
                primary_window={
                    "peak_local": "2026-03-09T18:00",
                    "start_local": "2026-03-09T16:00",
                    "end_local": "2026-03-09T20:00",
                    "peak_temp_c": 12.0,
                },
                metar_text="• **🌡️ 气温**：7°C",
                metar_diag={
                    "station_icao": "RJTT",
                    "latest_report_local": "2026-03-09T11:00:00+09:00",
                    "latest_temp": 7.0,
                    "temp_trend_1step_c": 0.0,
                },
                polymarket_event_url="https://polymarket.com/event/test",
                analysis_snapshot=snapshot,
            )

        self.assertIn("**判断依据**", text)
        self.assertNotIn("最新报还在 7°C 一带横着走", text)
        self.assertTrue(
            "若冷空气压制尚未解除" in text
            or "混合层加深幅度" in text
        )

    def test_transition_rationale_can_reference_observed_path_vs_ensemble(self) -> None:
        snapshot = _snapshot_template()
        snapshot["posterior_feature_vector"] = {
            "time_phase": {"hours_to_peak": 4.5},
            "ensemble_path_state": {
                "dominant_path": "transition",
                "dominant_path_detail": "weak_warm_transition",
                "dominant_prob": 0.68,
                "transition_detail": "weak_warm_transition",
                "observed_path": "transition",
                "observed_path_detail": "weak_warm_transition",
                "observed_alignment_match_state": "exact",
                "observed_alignment_confidence": "high",
                "observed_path_locked": True,
            },
        }
        snapshot["canonical_raw_state"] = {
            "forecast": {
                "h850_review": {
                    "advection_type": "warm",
                    "surface_role": "background",
                },
            },
        }
        with patch(
            "report_render_service._build_polymarket_section",
            return_value=(
                "📈 **Polymarket 盘口与博弈**\n"
                "**博弈区间**\n"
                "  • **11°C（👍最有可能）：Bid 19¢ | Ask 20¢**"
            ),
        ):
            text = choose_section_text(
                primary_window={
                    "peak_local": "2026-03-09T16:30",
                    "start_local": "2026-03-09T15:00",
                    "end_local": "2026-03-09T18:00",
                    "peak_temp_c": 12.0,
                },
                metar_text="• **🌡️ 气温**：10°C",
                metar_diag={
                    "station_icao": "RJTT",
                    "latest_report_local": "2026-03-09T12:00:00+09:00",
                    "latest_temp": 10.0,
                    "temp_trend_1step_c": 0.1,
                },
                polymarket_event_url="https://polymarket.com/event/test",
                analysis_snapshot=snapshot,
            )

        self.assertIn("**判断依据**", text)
        self.assertIn("当前实况高度贴合系集主路径，正沿暖侧试探演进", text)
        self.assertIn("主路径约 68%", text)

    def test_transition_rationale_can_fallback_to_obs_vs_model_pace_hint(self) -> None:
        snapshot = _snapshot_template()
        snapshot["posterior_feature_vector"] = {
            "unit": "C",
            "time_phase": {"hours_to_peak": 4.0},
            "observation_state": {
                "temp_trend_c": 0.22,
                "temp_accel_2step_c": 0.12,
            },
            "cloud_radiation_state": {
                "radiation_eff": 0.71,
                "cloud_trend": "clearing",
            },
            "transport_state": {
                "transport_state": "warm",
                "thermal_advection_state": "confirmed",
                "surface_bias": "warm",
            },
            "quality_state": {
                "metar_recent_interval_min": 30.0,
                "metar_routine_cadence_min": 30.0,
                "metar_speci_active": False,
                "metar_speci_likely": False,
            },
        }
        with patch(
            "report_render_service._build_polymarket_section",
            return_value=(
                "📈 **Polymarket 盘口与博弈**\n"
                "**博弈区间**\n"
                "  • **12°C（👍最有可能）：Bid 19¢ | Ask 20¢**"
            ),
        ):
            text = choose_section_text(
                primary_window={
                    "peak_local": "2026-03-09T16:00",
                    "start_local": "2026-03-09T14:00",
                    "end_local": "2026-03-09T18:00",
                    "peak_temp_c": 12.0,
                },
                metar_text="• **🌡️ 气温**：10°C",
                metar_diag={
                    "station_icao": "ZSPD",
                    "latest_report_local": "2026-03-09T12:00:00+08:00",
                    "latest_temp": 10.0,
                    "temp_trend_1step_c": 0.1,
                },
                polymarket_event_url="https://polymarket.com/event/test",
                analysis_snapshot=snapshot,
            )

        self.assertIn("当前升温节奏和辐射条件更像暖侧路径在提前落地", text)
        self.assertNotIn("最新报已经到", text)
        self.assertNotIn("最新报还在", text)

    def test_transition_rationale_does_not_fire_on_single_point_normal_variation(self) -> None:
        snapshot = _snapshot_template()
        snapshot["posterior_feature_vector"] = {
            "unit": "C",
            "time_phase": {"hours_to_peak": 4.0},
            "observation_state": {
                "temp_trend_c": 0.18,
                "temp_accel_2step_c": 0.0,
            },
            "cloud_radiation_state": {
                "radiation_eff": 0.56,
                "cloud_trend": "steady",
            },
            "transport_state": {
                "transport_state": "warm",
                "thermal_advection_state": "probable",
                "surface_bias": "warm",
            },
            "quality_state": {
                "metar_recent_interval_min": 30.0,
                "metar_routine_cadence_min": 30.0,
                "metar_speci_active": False,
                "metar_speci_likely": False,
            },
        }
        with patch(
            "report_render_service._build_polymarket_section",
            return_value=(
                "📈 **Polymarket 盘口与博弈**\n"
                "**博弈区间**\n"
                "  • **12°C（👍最有可能）：Bid 19¢ | Ask 20¢**"
            ),
        ):
            text = choose_section_text(
                primary_window={
                    "peak_local": "2026-03-09T16:00",
                    "start_local": "2026-03-09T14:00",
                    "end_local": "2026-03-09T18:00",
                    "peak_temp_c": 12.0,
                },
                metar_text="• **🌡️ 气温**：10°C",
                metar_diag={
                    "station_icao": "ZSPD",
                    "latest_report_local": "2026-03-09T12:00:00+08:00",
                    "latest_temp": 10.0,
                    "temp_trend_1step_c": 0.1,
                },
                polymarket_event_url="https://polymarket.com/event/test",
                analysis_snapshot=snapshot,
            )

        self.assertNotIn("更像暖侧路径在提前落地", text)
        self.assertNotIn("比模式主路径略快", text)

    def test_transition_rationale_ignores_two_step_signal_when_report_cadence_is_short(self) -> None:
        snapshot = _snapshot_template()
        snapshot["posterior_feature_vector"] = {
            "unit": "C",
            "time_phase": {"hours_to_peak": 4.0},
            "observation_state": {
                "temp_trend_c": 0.22,
                "temp_accel_2step_c": 0.18,
            },
            "cloud_radiation_state": {
                "radiation_eff": 0.74,
                "cloud_trend": "clearing",
            },
            "transport_state": {
                "transport_state": "warm",
                "thermal_advection_state": "confirmed",
                "surface_bias": "warm",
            },
            "quality_state": {
                "metar_recent_interval_min": 10.0,
                "metar_routine_cadence_min": 30.0,
                "metar_speci_active": True,
                "metar_speci_likely": False,
            },
        }
        with patch(
            "report_render_service._build_polymarket_section",
            return_value=(
                "📈 **Polymarket 盘口与博弈**\n"
                "**博弈区间**\n"
                "  • **12°C（👍最有可能）：Bid 19¢ | Ask 20¢**"
            ),
        ):
            text = choose_section_text(
                primary_window={
                    "peak_local": "2026-03-09T16:00",
                    "start_local": "2026-03-09T14:00",
                    "end_local": "2026-03-09T18:00",
                    "peak_temp_c": 12.0,
                },
                metar_text="• **🌡️ 气温**：10°C",
                metar_diag={
                    "station_icao": "ZSPD",
                    "latest_report_local": "2026-03-09T12:00:00+08:00",
                    "latest_temp": 10.0,
                    "temp_trend_1step_c": 0.1,
                },
                polymarket_event_url="https://polymarket.com/event/test",
                analysis_snapshot=snapshot,
            )

        self.assertNotIn("更像暖侧路径在提前落地", text)
        self.assertNotIn("比模式主路径略快", text)

    def test_compact_spacing_keeps_reminder_and_focus_tight(self) -> None:
        snapshot = _snapshot_template()
        snapshot["peak_data"]["block"] = [
            "🌡️ **可能最高温区间（仅供参考）**",
            "• **17.5~18.8°C**（主看 17.5~18.3°C；峰值窗 16:00~20:00 Local）",
        ]
        with patch(
            "report_render_service.build_report_focus_bundle",
            return_value={
                "vars_block": ["⚠️ **关注变量**（接近窗口）", "未来20-40分钟仍有再创新高空间，优先看温度斜率是否继续维持正值"],
                "metar_analysis_lines": ["• 实况提醒：短时升温仍在延续。"],
                "market_label_policy": {},
            },
        ):
            text = choose_section_text(
                primary_window={
                    "peak_local": "2026-03-09T18:00",
                    "start_local": "2026-03-09T16:00",
                    "end_local": "2026-03-09T20:00",
                    "peak_temp_c": 18.0,
                },
                metar_text="**最新报：14:00 Local**（上一报 13:30）",
                metar_diag={
                    "station_icao": "NZWN",
                    "latest_report_local": "2026-03-09T14:00:00+00:00",
                    "latest_temp": 18.0,
                    "observed_max_temp_c": 18.0,
                    "observed_max_time_local": "2026-03-09T14:00:00+00:00",
                    "temp_trend_1step_c": 0.6,
                },
                polymarket_event_url="",
                analysis_snapshot=snapshot,
            )

        self.assertNotIn("\n\n• 实况提醒：", text)
        self.assertNotIn("Local）\n\n⚠️ 关注：", text)


if __name__ == "__main__":
    unittest.main()
