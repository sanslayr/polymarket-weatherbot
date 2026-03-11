import sys
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
SCRIPTS = ROOT / "scripts"
if str(SCRIPTS) not in sys.path:
    sys.path.insert(0, str(SCRIPTS))

from market_signal_alert_service import format_market_signal_alert  # noqa: E402


class MarketSignalAlertServiceTest(unittest.TestCase):
    def test_formats_short_alert_with_time_and_link(self) -> None:
        text = format_market_signal_alert(
            city="Ankara",
            station_icao="LTAC",
            signal={
                "signal_type": "report_temp_scan_floor_stop",
                "implied_report_temp_lower_bound_c": 7,
                "target_bucket_threshold_c": 7,
                "observed_at_utc": "2026-03-09T08:31:15Z",
                "evidence": {
                    "first_live_bucket_label": "7°C",
                    "first_live_bucket_bid": 0.14,
                    "collapsed_prefix_labels": ["6°C"],
                    "collapsed_prefix_prev_bids": {"6°C": 0.06},
                },
            },
            observed_max_temp_c=6.7,
            observed_max_temp_quantized=False,
            observed_max_time_local="2026-03-09T14:20:00+03:00",
            scheduled_report_label="2026/03/09",
            observed_at_local="2026-03-09T11:31:15+03:00",
            local_tz_label="Local",
            polymarket_event_url="https://polymarket.com/event/highest-temperature-in-ankara-on-march-9-2026",
        )
        self.assertIn("⚠️ *盘口归零异动 | Ankara @ 2026/03/09*", text)
        self.assertIn("• *推测最新报最高温：7°C*", text)
        self.assertIn("• 已记录METAR最高温：6.7°C @ 14:20 Local", text)
        self.assertIn("• 盘口观察：6°C Yes 由 6¢ 短时间跌至接近归零。", text)
        self.assertIn("2026/03/09 08:31:15 UTC | 11:31:15 Local", text)
        self.assertIn("\n\n*当前市场盘口价格：*", text)
        self.assertIn("• 7°C：Bid 14¢ | Ask N/A", text)
        self.assertIn("（基于盘口异动推测，不代表官方实况）", text)
        self.assertIn("🔗 [Polymarket 市场](https://polymarket.com/event/highest-temperature-in-ankara-on-march-9-2026)", text)

    def test_top_bucket_message_is_preserved(self) -> None:
        text = format_market_signal_alert(
            city="Ankara",
            signal={
                "signal_type": "report_temp_top_bucket_lock_in",
                "message": "市场大概率已按最新报进入 12°C or higher 交易。",
                "observed_at_utc": "2026-03-09T08:31:15Z",
                "evidence": {"first_live_bucket_label": "12°C or higher"},
            },
        )
        self.assertIn("• *推测最新报最高温：12°C or higher*", text)

    def test_lower_bound_jump_mentions_observed_bucket(self) -> None:
        text = format_market_signal_alert(
            city="Ankara",
            signal={
                "signal_type": "report_temp_lower_bound_jump",
                "implied_report_temp_lower_bound_c": 7,
                "observed_at_utc": "2026-03-09T08:31:15Z",
                "evidence": {"bucket_label": "6°C", "best_bid": 0.0, "best_ask": 0.009},
            },
        )
        self.assertIn("• 市场隐含最新报下界：>= 7°C", text)
        self.assertIn("• 盘口观察：6°C 买盘接近归零，卖盘压到 1¢ 或更低。", text)

    def test_omits_local_when_same_as_utc(self) -> None:
        text = format_market_signal_alert(
            city="London",
            signal={
                "signal_type": "report_temp_scan_floor_stop",
                "target_bucket_threshold_c": 12,
                "observed_at_utc": "2026-03-09T09:22:00Z",
            },
            observed_at_local="2026-03-09T09:22:00+00:00",
            local_tz_label="Local",
        )
        self.assertIn("2026/03/09 09:22:00 UTC", text)
        self.assertNotIn("2026/03/09 09:22:00 UTC | ", text)

    def test_formats_thousandth_precision_as_tenth_cent(self) -> None:
        text = format_market_signal_alert(
            city="Ankara",
            signal={
                "signal_type": "report_temp_scan_floor_stop",
                "target_bucket_threshold_c": 7,
                "observed_at_utc": "2026-03-09T08:31:15Z",
                "evidence": {
                    "first_live_bucket_label": "7°C",
                    "first_live_bucket_bid": "0.009",
                    "collapsed_prefix_labels": ["6°C"],
                    "collapsed_prefix_prev_bids": {"6°C": "0.021"},
                },
            },
        )
        self.assertIn("• 盘口观察：6°C Yes 由 2.1¢ 短时间跌至接近归零。", text)
        self.assertIn("• 7°C：Bid 0.9¢ | Ask N/A", text)

    def test_scan_floor_stop_avoids_from_na_wording_and_marks_weak_bid(self) -> None:
        text = format_market_signal_alert(
            city="Ankara",
            signal={
                "signal_type": "report_temp_scan_floor_stop",
                "target_bucket_threshold_c": 8,
                "observed_at_utc": "2026-03-10T22:20:30Z",
                "evidence": {
                    "first_live_bucket_label": "8°C",
                    "first_live_bucket_bid": 0.001,
                    "collapsed_prefix_labels": ["7°C"],
                    "collapsed_prefix_prev_bids": {"7°C": None},
                    "collapsed_prefix_current_bids": {"7°C": None},
                    "collapsed_prefix_current_asks": {"7°C": 0.001},
                    "price_floor": 0.02,
                },
            },
        )
        self.assertIn("• 盘口观察：7°C Yes 盘口短时归零（卖盘压到 1¢ 或更低）。", text)
        self.assertIn("• 8°C：Bid 0.1¢ | Ask N/A", text)
        self.assertNotIn("由 N/A 跌至接近归零", text)

    def test_scan_floor_stop_lists_all_collapsed_prefix_labels_in_order(self) -> None:
        text = format_market_signal_alert(
            city="Tel Aviv",
            station_icao="LLBG",
            signal={
                "signal_type": "report_temp_scan_floor_stop",
                "target_bucket_threshold_c": 20,
                "scheduled_report_utc": "2026-03-10T22:50:00Z",
                "observed_at_utc": "2026-03-10T22:50:30Z",
                "evidence": {
                    "first_live_bucket_label": "20°C",
                    "first_live_bucket_bid": 0.05,
                    "collapsed_prefix_labels": ["17°C", "18°C", "19°C"],
                    "collapsed_prefix_prev_bids": {"17°C": 0.003, "18°C": 0.003, "19°C": 0.003},
                    "collapsed_prefix_current_bids": {"17°C": 0.003, "18°C": 0.003, "19°C": 0.003},
                    "collapsed_prefix_current_asks": {"17°C": 0.007, "18°C": 0.006, "19°C": 0.005},
                    "price_floor": 0.02,
                },
            },
            scheduled_report_label="2026/03/11",
            observed_at_local="2026-03-11T00:50:30+02:00",
            local_tz_label="Local",
            observed_max_temp_c=10.0,
            observed_max_temp_quantized=True,
            observed_max_time_local="2026-03-11T14:20:00+02:00",
            polymarket_event_url="https://polymarket.com/event/highest-temperature-in-tel-aviv-on-march-11-2026",
        )
        self.assertIn("⚠️ *盘口归零异动 | Tel Aviv @ 2026/03/11*", text)
        self.assertIn("• *推测最新报最高温：20°C*", text)
        self.assertIn("• 已记录METAR最高温：10°C @ 14:20 Local", text)
        self.assertIn(
            "• 盘口观察：17°C Yes 盘口短时归零（卖盘压到 1¢ 或更低）；18°C Yes 盘口短时归零（卖盘压到 1¢ 或更低）；19°C Yes 盘口短时归零（卖盘压到 1¢ 或更低）。",
            text,
        )
        self.assertIn("\n*当前市场盘口价格：*", text)
        self.assertIn("• 20°C：Bid 5¢ | Ask N/A", text)

    def test_observed_max_temp_uses_integer_c_when_metar_is_quantized(self) -> None:
        text = format_market_signal_alert(
            city="Tel Aviv",
            signal={
                "signal_type": "report_temp_scan_floor_stop",
                "target_bucket_threshold_c": 20,
                "temperature_unit": "C",
                "observed_at_utc": "2026-03-10T22:50:30Z",
            },
            observed_max_temp_c=10.0,
            observed_max_temp_quantized=True,
        )
        self.assertIn("已记录METAR最高温：10°C", text)
        self.assertNotIn("10.0°C", text)

    def test_observed_max_temp_uses_fahrenheit_for_us_markets(self) -> None:
        text = format_market_signal_alert(
            city="Dallas",
            signal={
                "signal_type": "report_temp_scan_floor_stop",
                "target_bucket_threshold_native": 80,
                "temperature_unit": "F",
                "observed_at_utc": "2026-03-10T22:53:30Z",
            },
            observed_max_temp_c=10.0,
            observed_max_temp_quantized=True,
        )
        self.assertIn("已记录METAR最高温：50°F", text)
        self.assertNotIn("50.0°F", text)

    def test_scan_floor_stop_prefers_bucket_label_for_range_market_output(self) -> None:
        text = format_market_signal_alert(
            city="Dallas",
            signal={
                "signal_type": "report_temp_scan_floor_stop",
                "target_bucket_label": "50-51°F",
                "target_bucket_threshold_native": 51,
                "temperature_unit": "F",
                "observed_at_utc": "2026-03-10T22:53:30Z",
                "evidence": {},
            },
        )
        self.assertIn("• *推测最新报最高温：50-51°F*", text)
        self.assertNotIn("• *推测最新报最高温：51°F*", text)

    def test_scan_floor_stop_includes_first_live_ask_when_available(self) -> None:
        text = format_market_signal_alert(
            city="Tokyo",
            signal={
                "signal_type": "report_temp_scan_floor_stop",
                "target_bucket_threshold_c": 10,
                "observed_at_utc": "2026-03-11T05:02:15Z",
                "evidence": {
                    "first_live_bucket_label": "10°C",
                    "first_live_bucket_bid": 0.45,
                    "first_live_bucket_ask": 0.47,
                    "collapsed_prefix_labels": ["9°C"],
                    "collapsed_prefix_prev_bids": {"9°C": 0.13},
                },
            },
            observed_max_temp_c=9.0,
            observed_max_temp_quantized=True,
            observed_max_time_local="2026-03-11T13:30:00+09:00",
            scheduled_report_label="2026/03/11",
            observed_at_local="2026-03-11T14:02:15+09:00",
            local_tz_label="Local",
            polymarket_event_url="https://polymarket.com/event/highest-temperature-in-tokyo-on-march-11-2026",
        )
        self.assertIn("• *推测最新报最高温：10°C*", text)
        self.assertIn("• 已记录METAR最高温：9°C @ 13:30 Local", text)
        self.assertIn("• 盘口观察：9°C Yes 由 13¢ 短时间跌至接近归零。", text)
        self.assertIn("\n*当前市场盘口价格：*", text)
        self.assertIn("• 10°C：Bid 45¢ | Ask 47¢", text)
        self.assertIn("🔗 [Polymarket 市场](https://polymarket.com/event/highest-temperature-in-tokyo-on-march-11-2026)", text)

    def test_scan_floor_stop_lists_live_ladder_rows_from_first_live_and_above(self) -> None:
        text = format_market_signal_alert(
            city="Ankara",
            signal={
                "signal_type": "report_temp_scan_floor_stop",
                "target_bucket_threshold_c": 27,
                "observed_at_utc": "2026-03-11T12:01:15Z",
                "evidence": {
                    "first_live_bucket_label": "27°C",
                    "first_live_bucket_bid": 0.25,
                    "first_live_bucket_ask": 0.29,
                    "collapsed_prefix_labels": ["26°C"],
                    "collapsed_prefix_prev_bids": {"26°C": 0.08},
                    "live_ladder_rows": [
                        {"bucket_label": "27°C", "best_bid": 0.25, "best_ask": 0.29},
                        {"bucket_label": "28°C", "best_bid": 0.33, "best_ask": 0.40},
                        {"bucket_label": "29°C", "best_bid": 0.21, "best_ask": 0.23},
                        {"bucket_label": "30°C", "best_bid": 0.05, "best_ask": 0.09},
                    ],
                },
            },
        )
        self.assertIn("• 27°C：Bid 25¢ | Ask 29¢", text)
        self.assertIn("• 28°C：Bid 33¢ | Ask 40¢", text)
        self.assertIn("• 29°C：Bid 21¢ | Ask 23¢", text)
        self.assertIn("• 30°C：Bid 5¢ | Ask 9¢", text)

    def test_lower_bound_jump_uses_native_fahrenheit_display_when_available(self) -> None:
        text = format_market_signal_alert(
            city="Chicago",
            signal={
                "signal_type": "report_temp_lower_bound_jump",
                "implied_report_temp_lower_bound_c": 16.1,
                "implied_report_temp_lower_bound_native": 61,
                "target_bucket_threshold_c": 15.5,
                "target_bucket_threshold_native": 60,
                "temperature_unit": "F",
                "observed_at_utc": "2026-03-09T08:31:15Z",
                "evidence": {"bucket_label": "60°F or below", "best_bid": 0.0, "best_ask": 0.009},
            },
        )
        self.assertIn("• 市场隐含最新报下界：>= 61°F", text)
        self.assertIn("• 盘口观察：60°F or below 买盘接近归零，卖盘压到 1¢ 或更低。", text)


if __name__ == "__main__":
    unittest.main()
