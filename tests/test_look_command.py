import sys
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
SCRIPTS = ROOT / "scripts"
if str(SCRIPTS) not in sys.path:
    sys.path.insert(0, str(SCRIPTS))

from look_command import parse_telegram_command  # noqa: E402


class LookCommandTests(unittest.TestCase):
    def test_supports_bot_qualified_slash_command(self) -> None:
        parsed = parse_telegram_command("/look@WeatherNerd_bot seoul 20260310")

        self.assertEqual(parsed["cmd"], "look")
        self.assertEqual(parsed["station"], "seoul")
        self.assertEqual(parsed["date"], "20260310")


if __name__ == "__main__":
    unittest.main()
