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
            observed_at_local="2026-03-09T11:31:15+03:00",
            local_tz_label="Local",
            polymarket_event_url="https://polymarket.com/event/highest-temperature-in-ankara-on-march-9-2026",
        )
        self.assertIn("⚠️ **盘口异常提示 | Ankara**", text)
        self.assertIn("🕒 异动时间：2026/03/09 08:31:15 UTC | 11:31:15 Local", text)
        self.assertIn("🌡️ 推测最新报最高温：7°C", text)
        self.assertIn("📉 观察盘口：6°C Yes 由 6¢ 跌至接近归零，7°C Yes 仍有 14¢ bid报价。", text)
        self.assertIn("📝 提示基于盘口异动，不代表官方实况。", text)
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
        self.assertIn("🌡️ 推测最新报最高温：12°C or higher", text)

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
        self.assertIn("📈 市场隐含最新报下界：>= 7°C", text)
        self.assertIn("📉 观察盘口：6°C 买盘接近归零，卖盘压到 1¢ 或更低。", text)

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
        self.assertIn("🕒 异动时间：2026/03/09 09:22:00 UTC", text)
        self.assertNotIn("异动时间：2026/03/09 09:22:00 UTC | ", text)

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
        self.assertIn("📉 观察盘口：6°C Yes 由 2.1¢ 跌至接近归零，7°C Yes 仍有 0.9¢ bid报价。", text)

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
        self.assertIn("📈 市场隐含最新报下界：>= 61°F", text)
        self.assertIn("📉 观察盘口：60°F or below 买盘接近归零，卖盘压到 1¢ 或更低。", text)


if __name__ == "__main__":
    unittest.main()
