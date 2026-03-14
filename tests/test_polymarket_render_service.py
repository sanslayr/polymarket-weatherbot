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
            label_policy={"best_weather_min": 0.30, "best_lead_min": 0.05, "min_display_rows": 3},
            prefetched_event=(
                True,
                [
                    {"slug": "highest-temperature-in-ankara-on-march-9-2026-6c", "bestBid": "0.06", "bestAsk": "0.07"},
                    {"slug": "highest-temperature-in-ankara-on-march-9-2026-7c", "bestBid": "0.28", "bestAsk": "0.29"},
                    {"slug": "highest-temperature-in-ankara-on-march-9-2026-8c", "bestBid": "0.381", "bestAsk": "0.392"},
                ],
            ),
        )
        self.assertIn("7°C（👍最可能）：Bid 28¢ | Ask 29¢", text)
        self.assertIn("8°C：Bid 38.1¢ | Ask 39.2¢", text)

    def test_formats_bid_ask_as_integer_cents_for_cent_market(self) -> None:
        text = _build_polymarket_section(
            "https://polymarket.com/event/highest-temperature-in-ankara-on-march-9-2026",
            {"peak_temp_c": 7.4},
            weather_anchor={"observed_max_temp_c": 6.0},
            label_policy={"best_weather_min": 0.30, "best_lead_min": 0.05, "min_display_rows": 3},
            prefetched_event=(
                True,
                [
                    {"slug": "highest-temperature-in-ankara-on-march-9-2026-6c", "bestBid": "0.06", "bestAsk": "0.07"},
                    {"slug": "highest-temperature-in-ankara-on-march-9-2026-7c", "bestBid": "0.28", "bestAsk": "0.29"},
                ],
            ),
        )
        self.assertIn("6°C：Bid 6¢ | Ask 7¢", text)
        self.assertIn("7°C（👍最可能）：Bid 28¢ | Ask 29¢", text)

    def test_polymarket_section_uses_compact_subheaders_and_unindented_rows(self) -> None:
        text = _build_polymarket_section(
            "https://polymarket.com/event/highest-temperature-in-ankara-on-march-9-2026",
            {"peak_temp_c": 7.4},
            weather_anchor={"observed_max_temp_c": 6.0},
            label_policy={"best_weather_min": 0.30, "best_lead_min": 0.05, "min_display_rows": 3},
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
        self.assertIn("\n• **7°C（👍最可能）：Bid 28¢ | Ask 29¢**", text)

    def test_alpha_label_requires_significant_probability_edge(self) -> None:
        text = _build_polymarket_section(
            "https://polymarket.com/event/highest-temperature-in-ankara-on-march-9-2026",
            {"peak_temp_c": 7.2},
            weather_anchor={"observed_max_temp_c": 6.0},
            weather_posterior={
                "quantiles": {
                    "p10_c": 6.8,
                    "p25_c": 7.0,
                    "p50_c": 7.2,
                    "p75_c": 7.4,
                    "p90_c": 7.6,
                }
            },
            prefetched_event=(
                True,
                [
                    {"slug": "highest-temperature-in-ankara-on-march-9-2026-6c", "bestBid": "0.24", "bestAsk": "0.25"},
                    {"slug": "highest-temperature-in-ankara-on-march-9-2026-7c", "bestBid": "0.33", "bestAsk": "0.34"},
                    {"slug": "highest-temperature-in-ankara-on-march-9-2026-8c", "bestBid": "0.13", "bestAsk": "0.14"},
                ],
            ),
            label_policy={"phase_now": "same_day", "min_display_rows": 3},
        )

        self.assertNotIn("😇潜在Alpha", text)

    def test_alpha_label_uses_posterior_probability_edge(self) -> None:
        text = _build_polymarket_section(
            "https://polymarket.com/event/highest-temperature-in-ankara-on-march-9-2026",
            {"peak_temp_c": 8.0},
            weather_anchor={"observed_max_temp_c": 7.0},
            weather_posterior={
                "quantiles": {
                    "p10_c": 7.6,
                    "p25_c": 7.8,
                    "p50_c": 8.0,
                    "p75_c": 8.2,
                    "p90_c": 8.4,
                }
            },
            allow_best_label=False,
            prefetched_event=(
                True,
                [
                    {"slug": "highest-temperature-in-ankara-on-march-9-2026-7c", "bestBid": "0.10", "bestAsk": "0.11"},
                    {"slug": "highest-temperature-in-ankara-on-march-9-2026-8c", "bestBid": "0.10", "bestAsk": "0.11"},
                    {"slug": "highest-temperature-in-ankara-on-march-9-2026-9c", "bestBid": "0.02", "bestAsk": "0.03"},
                ],
            ),
            label_policy={"phase_now": "same_day", "min_display_rows": 3},
        )

        self.assertIn("8°C（😇潜在Alpha）", text)

    def test_far_phase_keeps_multiple_bins_and_avoids_single_best_bin(self) -> None:
        text = _build_polymarket_section(
            "https://polymarket.com/event/highest-temperature-in-ankara-on-march-9-2026",
            {"peak_temp_c": 8.5},
            weather_anchor={"observed_max_temp_c": 6.0},
            weather_posterior={
                "quantiles": {
                    "p10_c": 6.8,
                    "p25_c": 7.6,
                    "p50_c": 8.5,
                    "p75_c": 9.4,
                    "p90_c": 10.2,
                }
            },
            prefetched_event=(
                True,
                [
                    {"slug": "highest-temperature-in-ankara-on-march-9-2026-7c", "bestBid": "0.15", "bestAsk": "0.16"},
                    {"slug": "highest-temperature-in-ankara-on-march-9-2026-8c", "bestBid": "0.20", "bestAsk": "0.21"},
                    {"slug": "highest-temperature-in-ankara-on-march-9-2026-9c", "bestBid": "0.19", "bestAsk": "0.20"},
                    {"slug": "highest-temperature-in-ankara-on-march-9-2026-10c", "bestBid": "0.13", "bestAsk": "0.14"},
                ],
            ),
            label_policy={"phase_now": "far", "min_display_rows": 3},
        )

        ladder_rows = [line for line in text.splitlines() if line.startswith("• ")]
        self.assertGreaterEqual(len(ladder_rows), 3)
        self.assertNotIn("👍最可能", text)

    def test_far_phase_broad_distribution_avoids_best_and_alpha_tags_under_strict_policy(self) -> None:
        text = _build_polymarket_section(
            "https://polymarket.com/event/highest-temperature-in-ankara-on-march-9-2026",
            {"peak_temp_c": 13.0},
            weather_anchor={"latest_temp_c": 10.0, "observed_max_temp_c": 10.0},
            weather_posterior={
                "quantiles": {
                    "p10_c": 12.14,
                    "p25_c": 12.65,
                    "p50_c": 13.01,
                    "p75_c": 13.9,
                    "p90_c": 14.5,
                }
            },
            prefetched_event=(
                True,
                [
                    {"slug": "highest-temperature-in-ankara-on-march-9-2026-12c", "bestBid": "0.053", "bestAsk": "0.059"},
                    {"slug": "highest-temperature-in-ankara-on-march-9-2026-13c", "bestBid": "0.33", "bestAsk": "0.36"},
                    {"slug": "highest-temperature-in-ankara-on-march-9-2026-14c", "bestBid": "0.44", "bestAsk": "0.48"},
                    {"slug": "highest-temperature-in-ankara-on-march-9-2026-15c", "bestBid": "0.10", "bestAsk": "0.13"},
                    {"slug": "highest-temperature-in-ankara-on-march-9-2026-16corhigher", "bestBid": "0.012", "bestAsk": "0.013"},
                ],
            ),
            label_policy={
                "phase_now": "far",
                "best_weather_min": 0.54,
                "best_lead_min": 0.30,
                "alpha_cheap_weather_min": 0.22,
                "alpha_cheap_score_min": 0.28,
                "alpha_cheap_edge_min": 0.20,
                "alpha_mid_weather_min": 0.48,
                "alpha_mid_score_min": 0.36,
                "alpha_mid_edge_min": 0.23,
                "min_display_rows": 3,
            },
        )

        self.assertNotIn("👍最可能", text)
        self.assertNotIn("😇潜在Alpha", text)


if __name__ == "__main__":
    unittest.main()
