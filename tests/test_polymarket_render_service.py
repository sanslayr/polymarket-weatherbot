import sys
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
SCRIPTS = ROOT / "scripts"
if str(SCRIPTS) not in sys.path:
    sys.path.insert(0, str(SCRIPTS))

from polymarket_render_service import _build_polymarket_section  # noqa: E402


class PolymarketRenderServiceTest(unittest.TestCase):
    def test_formats_bid_ask_per_row_precision(self) -> None:
        text = _build_polymarket_section(
            "https://polymarket.com/event/highest-temperature-in-ankara-on-march-9-2026",
            {"peak_temp_c": 7.4},
            weather_anchor={"observed_max_temp_c": 6.0},
            prefetched_event=(
                True,
                [
                    {"slug": "highest-temperature-in-ankara-on-march-9-2026-6c", "bestBid": "0.06", "bestAsk": "0.07"},
                    {"slug": "highest-temperature-in-ankara-on-march-9-2026-7c", "bestBid": "0.28", "bestAsk": "0.29"},
                    {"slug": "highest-temperature-in-ankara-on-march-9-2026-8c", "bestBid": "0.381", "bestAsk": "0.392"},
                ],
            ),
        )
        self.assertIn("7°C（👍最有可能）：Bid 28¢ | Ask 29¢", text)
        self.assertIn("8°C：Bid 38.1¢ | Ask 39.2¢", text)

    def test_formats_bid_ask_as_integer_cents_for_cent_market(self) -> None:
        text = _build_polymarket_section(
            "https://polymarket.com/event/highest-temperature-in-ankara-on-march-9-2026",
            {"peak_temp_c": 7.4},
            weather_anchor={"observed_max_temp_c": 6.0},
            prefetched_event=(
                True,
                [
                    {"slug": "highest-temperature-in-ankara-on-march-9-2026-6c", "bestBid": "0.06", "bestAsk": "0.07"},
                    {"slug": "highest-temperature-in-ankara-on-march-9-2026-7c", "bestBid": "0.28", "bestAsk": "0.29"},
                ],
            ),
        )
        self.assertIn("6°C：Bid 6¢ | Ask 7¢", text)
        self.assertIn("7°C（👍最有可能）：Bid 28¢ | Ask 29¢", text)

    def test_polymarket_section_uses_compact_subheaders_and_unindented_rows(self) -> None:
        text = _build_polymarket_section(
            "https://polymarket.com/event/highest-temperature-in-ankara-on-march-9-2026",
            {"peak_temp_c": 7.4},
            weather_anchor={"observed_max_temp_c": 6.0},
            prefetched_event=(
                True,
                [
                    {"slug": "highest-temperature-in-ankara-on-march-9-2026-6c", "bestBid": "0.06", "bestAsk": "0.07"},
                    {"slug": "highest-temperature-in-ankara-on-march-9-2026-7c", "bestBid": "0.28", "bestAsk": "0.29"},
                    {"slug": "highest-temperature-in-ankara-on-march-9-2026-8c", "bestBid": "0.381", "bestAsk": "0.392"},
                ],
            ),
        )

        self.assertIn("**市场定价**", text)
        self.assertNotIn("市场定价期望", text)
        self.assertIn("**博弈区间**", text)
        self.assertIn("↳", text)
        self.assertIn("\n• **7°C（👍最有可能）：Bid 28¢ | Ask 29¢**", text)


if __name__ == "__main__":
    unittest.main()
