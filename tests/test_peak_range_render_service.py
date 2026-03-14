import sys
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
SCRIPTS = ROOT / "scripts"
if str(SCRIPTS) not in sys.path:
    sys.path.insert(0, str(SCRIPTS))

from peak_range_render_service import render_peak_range_block  # noqa: E402


class PeakRangeRenderServiceTest(unittest.TestCase):
    def test_render_hides_historical_reference_from_report_block(self) -> None:
        block = render_peak_range_block(
            {
                "ranges": {
                    "display": {"lo": 13.4, "hi": 15.2},
                    "core": {"lo": 14.0, "hi": 15.2},
                    "window": {"label": "峰值窗", "text": "15:00~17:00 Local"},
                    "skew_bucket": "neutral",
                },
                "annotations": ["- 注释A"],
                "historical_reference": {
                    "title": "- 历史参考：",
                    "lines": ["- 2022/03/09 & 2024/03/11"],
                    "shift_text": "历史上修提示",
                },
            },
            unit="C",
            fmt_range_fn=lambda lo, hi: f"{lo:.1f}~{hi:.1f}°C",
        )

        joined = "\n".join(block)
        self.assertIn("🌡️ **可能最高温区间", joined)
        self.assertIn("- 注释A", joined)
        self.assertNotIn("历史参考", joined)
        self.assertNotIn("2022/03/09", joined)
        self.assertNotIn("历史上修提示", joined)

    def test_render_hides_core_range_when_only_small_upper_tail_diff(self) -> None:
        block = render_peak_range_block(
            {
                "ranges": {
                    "display": {"lo": 10.8, "hi": 12.2},
                    "core": {"lo": 10.8, "hi": 11.8},
                    "window": {"label": "峰值窗", "text": "12:00~15:00 Local"},
                },
            },
            unit="C",
            fmt_range_fn=lambda lo, hi: f"{lo:.1f}~{hi:.1f}°C",
        )

        self.assertEqual(
            block[1],
            "• **10.8~12.2°C**（峰值窗 12:00~15:00 Local）",
        )

    def test_render_keeps_core_range_when_gap_is_material(self) -> None:
        block = render_peak_range_block(
            {
                "ranges": {
                    "display": {"lo": 17.5, "hi": 18.8},
                    "core": {"lo": 17.5, "hi": 18.3},
                    "window": {"label": "峰值窗", "text": "16:00~20:00 Local"},
                },
            },
            unit="C",
            fmt_range_fn=lambda lo, hi: f"{lo:.1f}~{hi:.1f}°C",
        )

        self.assertEqual(
            block[1],
            "• **17.5~18.8°C**（峰值窗 16:00~20:00 Local）",
        )

    def test_render_includes_pxx_band_for_posterior_driven_range(self) -> None:
        block = render_peak_range_block(
            {
                "ranges": {
                    "display": {"lo": 17.5, "hi": 18.8},
                    "core": {"lo": 17.5, "hi": 18.3},
                    "window": {"label": "峰值窗", "text": "16:00~20:00 Local"},
                    "source": "posterior_quantiles",
                    "posterior_tail_weight": 0.35,
                },
            },
            unit="C",
            fmt_range_fn=lambda lo, hi: f"{lo:.1f}~{hi:.1f}°C",
        )

        self.assertEqual(
            block[1],
            "• **17.5~18.8°C**（峰值窗 16:00~20:00 Local）",
        )

    def test_render_marks_path_capped_posterior_band(self) -> None:
        block = render_peak_range_block(
            {
                "ranges": {
                    "display": {"lo": 23.4, "hi": 23.8},
                    "core": {"lo": 23.4, "hi": 23.8},
                    "window": {"label": "峰值窗", "text": "14:00~16:00 Local"},
                    "source": "posterior_quantiles_path_capped",
                    "posterior_tail_weight": 0.0,
                },
            },
            unit="C",
            fmt_range_fn=lambda lo, hi: f"{lo:.1f}~{hi:.1f}°C",
        )

        self.assertEqual(
            block[1],
            "• **23.4~23.8°C**（已按实况路径收紧；峰值窗 14:00~16:00 Local）",
        )


if __name__ == "__main__":
    unittest.main()
