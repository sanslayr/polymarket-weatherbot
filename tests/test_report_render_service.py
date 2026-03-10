import sys
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
SCRIPTS = ROOT / "scripts"
if str(SCRIPTS) not in sys.path:
    sys.path.insert(0, str(SCRIPTS))

from report_render_service import choose_section_text  # noqa: E402


def _snapshot_template() -> dict:
    return {
        "synoptic_summary": {
            "lines": [
                "🧭 **环流形势对最高温影响**",
                "- **主导机制**：低层混合仍在加深。",
                "- 午后低层偏南风还能继续托住升温。",
                "- 云量约束不强，但后段上冲空间有限。",
            ]
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
        "quality_snapshot": {"scores": {"confidence_label": "medium"}},
        "condition_state": {},
    }


class ReportRenderServiceTest(unittest.TestCase):
    def test_compact_synoptic_becomes_single_sentence_near_window(self) -> None:
        text = choose_section_text(
            primary_window={
                "peak_local": "2026-03-09T14:00",
                "start_local": "2026-03-09T13:00",
                "end_local": "2026-03-09T15:00",
                "peak_temp_c": 30.0,
            },
            metar_text="- 当前实况平稳。",
            metar_diag={},
            polymarket_event_url="",
            compact_synoptic=True,
            analysis_snapshot=_snapshot_template(),
        )

        synoptic_block = text.split("\n\n", 1)[0]
        self.assertTrue(synoptic_block.startswith("🧭 环流形势："))
        self.assertNotIn("\n", synoptic_block)
        self.assertNotIn("**环流形势对最高温影响**", synoptic_block)
        self.assertNotIn("**主导机制**", synoptic_block)
        self.assertIn("低层混合仍在加深，后段更要看偏南风能否继续稳住", synoptic_block)

    def test_non_compact_synoptic_keeps_structured_block(self) -> None:
        text = choose_section_text(
            primary_window={
                "peak_local": "2026-03-09T14:00",
                "start_local": "2026-03-09T13:00",
                "end_local": "2026-03-09T15:00",
                "peak_temp_c": 30.0,
            },
            metar_text="- 当前实况平稳。",
            metar_diag={},
            polymarket_event_url="",
            compact_synoptic=False,
            analysis_snapshot=_snapshot_template(),
        )

        synoptic_block = text.split("\n\n", 1)[0]
        self.assertIn("🧭 **环流形势对最高温影响**", synoptic_block)
        self.assertIn("**主导机制**", synoptic_block)

    def test_far_window_downgrades_metar_and_moves_it_after_forecast(self) -> None:
        snapshot = _snapshot_template()
        snapshot["temp_phase_decision"] = {"display_phase": "far"}
        snapshot["peak_data"]["summary"]["phase_now"] = "far"

        text = choose_section_text(
            primary_window={
                "peak_local": "2026-03-11T14:00",
                "start_local": "2026-03-11T13:00",
                "end_local": "2026-03-11T15:00",
                "peak_temp_c": 30.0,
            },
            metar_text="- 这里是完整 METAR 逐项分析。",
            metar_diag={
                "latest_report_local": "2026-03-09T09:30:00+00:00",
                "latest_temp": 21.0,
                "latest_wdir": 180,
                "latest_wspd": 9,
                "latest_cloud_layers": "SCT025(疏云2500ft/762m)",
            },
            polymarket_event_url="",
            compact_synoptic=False,
            analysis_snapshot=snapshot,
        )

        self.assertIn("📡 **当前实况参考（降级）**", text)
        self.assertNotIn("📡 **最新实况分析（METAR）**", text)
        self.assertNotIn("**实况分析**", text)
        self.assertNotIn("下一报", text)
        self.assertNotIn("未来20-40分钟", text)
        self.assertLess(text.index("📈 **峰值窗口判断**"), text.index("📡 **当前实况参考（降级）**"))


if __name__ == "__main__":
    unittest.main()
