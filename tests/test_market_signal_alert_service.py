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
                "evidence": {"first_live_bucket_label": "7°C"},
            },
            observed_at_local="2026-03-09T11:31:15+03:00",
            local_tz_label="Local",
            polymarket_event_url="https://polymarket.com/event/highest-temperature-in-ankara-on-march-9-2026",
        )
        self.assertIn("⚠️ **盘口异常提示 | Ankara**", text)
        self.assertIn("🕒 异动时间：2026/03/09 08:31:15 UTC | 2026/03/09 11:31:15 Local", text)
        self.assertIn("🌡️ 推测最新报最高温：7°C", text)
        self.assertIn("📝 提示基于盘口异动，不代表官方实况。", text)
        self.assertIn("🔗 [查看 Polymarket 市场](", text)

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


if __name__ == "__main__":
    unittest.main()
