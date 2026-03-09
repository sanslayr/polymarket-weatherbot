from __future__ import annotations

import os
import sys
import unittest
from pathlib import Path
from unittest.mock import patch


SCRIPT_DIR = Path(__file__).resolve().parents[1] / "scripts"
if str(SCRIPT_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPT_DIR))

from alert_delivery_policy import resolve_telegram_alert_target, resolve_telegram_alert_targets


class AlertDeliveryPolicyTests(unittest.TestCase):
    def test_prefers_direct_chat_target(self) -> None:
        with patch.dict(
            os.environ,
            {
                "TELEGRAM_DIRECT_CHAT_ID": "7419505165",
                "TELEGRAM_ALERT_CHAT_ID": "-1003586303099",
                "TELEGRAM_CHAT_ID": "-1009999999999",
            },
            clear=False,
        ):
            self.assertEqual(resolve_telegram_alert_target(), "7419505165")

    def test_explicit_target_wins(self) -> None:
        with patch.dict(os.environ, {"TELEGRAM_DIRECT_CHAT_ID": "7419505165"}, clear=False):
            self.assertEqual(resolve_telegram_alert_target("12345"), "12345")

    def test_multi_targets_prefer_explicit_list(self) -> None:
        with patch.dict(
            os.environ,
            {"TELEGRAM_ALERT_TARGETS": "7419505165,-1003586303099"},
            clear=False,
        ):
            self.assertEqual(resolve_telegram_alert_targets(["12345", "67890"]), ["12345", "67890"])

    def test_multi_targets_from_env(self) -> None:
        with patch.dict(
            os.environ,
            {"TELEGRAM_ALERT_TARGETS": "7419505165, -1003586303099,7419505165"},
            clear=False,
        ):
            self.assertEqual(resolve_telegram_alert_targets(), ["7419505165", "-1003586303099"])


if __name__ == "__main__":
    unittest.main()
