import os
import sys
import unittest
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace
from zoneinfo import ZoneInfo


os.environ["WEATHERBOT_SKIP_VENV_REEXEC"] = "1"

ROOT = Path(__file__).resolve().parents[1]
SCRIPTS = ROOT / "scripts"
if str(SCRIPTS) not in sys.path:
    sys.path.insert(0, str(SCRIPTS))

import telegram_report_cli  # noqa: E402


class TelegramReportCliTests(unittest.TestCase):
    def test_render_report_header_omits_model_and_source_details(self) -> None:
        station = SimpleNamespace(city="Tokyo", icao="RJTT", lat=35.5523, lon=139.7798)
        now_utc = datetime(2026, 3, 11, 5, 2, tzinfo=timezone.utc)
        bundle = SimpleNamespace(
            now_utc=now_utc,
            now_local=now_utc.astimezone(ZoneInfo("Asia/Tokyo")),
            mode="full",
            compact_synoptic=False,
            forecast_quality={
                "source_state": "cache-hit",
                "missing_layers": [],
                "synoptic_provider_requested": "ecmwf-open-data",
                "synoptic_provider_used": "ecmwf-open-data",
            },
            synoptic_provider_used="ecmwf-open-data",
            synoptic_error=None,
        )

        header = telegram_report_cli._render_report_header(station, bundle)

        self.assertIn("生成时间:", header)
        self.assertIn("2026/03/11 14:02:00 Local (UTC+09:00)", header)
        self.assertNotIn("UTC |", header)
        self.assertNotIn("分析链路:", header)
        self.assertNotIn("小时预报源", header)
        self.assertNotIn("数值预报场源", header)
        self.assertNotIn("运行时次", header)

    def test_render_report_header_uses_generic_metar_only_notice(self) -> None:
        station = SimpleNamespace(city="Tokyo", icao="RJTT", lat=35.5523, lon=139.7798)
        now_utc = datetime(2026, 3, 11, 5, 2, tzinfo=timezone.utc)
        bundle = SimpleNamespace(
            now_utc=now_utc,
            now_local=now_utc.astimezone(ZoneInfo("Asia/Tokyo")),
            mode="metar_only",
            compact_synoptic=False,
            forecast_quality={},
            synoptic_provider_used="",
            synoptic_error=None,
        )

        header = telegram_report_cli._render_report_header(station, bundle)

        self.assertIn("数据提醒: 当前按实况降级生成。", header)
        self.assertNotIn("分析链路:", header)
        self.assertNotIn("运行时次", header)


if __name__ == "__main__":
    unittest.main()
