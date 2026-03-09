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


if __name__ == "__main__":
    unittest.main()
