from __future__ import annotations

import sys
import unittest
from pathlib import Path
from unittest.mock import patch


ROOT = Path(__file__).resolve().parents[1]
SCRIPTS = ROOT / "scripts"
if str(SCRIPTS) not in sys.path:
    sys.path.insert(0, str(SCRIPTS))

from telegram_notifier import send_telegram_messages, send_telegram_messages_report  # noqa: E402


class TelegramNotifierTests(unittest.TestCase):
    def test_send_messages_continues_after_first_target_failure(self) -> None:
        with patch("telegram_notifier.resolve_telegram_alert_targets", return_value=["7419505165", "-1003586303099"]), patch(
            "telegram_notifier.send_telegram_message",
            side_effect=[RuntimeError("direct failed"), {"ok": True, "result": {"chat": {"id": -1003586303099}}}],
        ) as mock_send:
            results = send_telegram_messages("test")

        self.assertEqual(len(results), 1)
        self.assertEqual(mock_send.call_count, 2)

    def test_send_messages_raises_when_all_targets_fail(self) -> None:
        with patch("telegram_notifier.resolve_telegram_alert_targets", return_value=["7419505165", "-1003586303099"]), patch(
            "telegram_notifier.send_telegram_message",
            side_effect=[RuntimeError("direct failed"), RuntimeError("group failed")],
        ):
            with self.assertRaisesRegex(RuntimeError, "All Telegram deliveries failed"):
                send_telegram_messages("test")

    def test_send_messages_report_keeps_partial_failures_visible(self) -> None:
        with patch("telegram_notifier.resolve_telegram_alert_targets", return_value=["7419505165", "-1003586303099"]), patch(
            "telegram_notifier.send_telegram_message",
            side_effect=[RuntimeError("direct failed"), {"ok": True, "result": {"chat": {"id": -1003586303099}}}],
        ):
            report = send_telegram_messages_report("test")

        self.assertEqual(report["targets"], ["7419505165", "-1003586303099"])
        self.assertEqual(len(report["successes"]), 1)
        self.assertEqual(len(report["errors"]), 1)


if __name__ == "__main__":
    unittest.main()
