import sys
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
SCRIPTS = ROOT / "scripts"
if str(SCRIPTS) not in sys.path:
    sys.path.insert(0, str(SCRIPTS))

from look_command import render_look_help  # noqa: E402
from station_catalog import common_alias_examples, resolve_station, supported_station_labels  # noqa: E402


class LookCommandHelpTest(unittest.TestCase):
    def test_help_uses_current_station_catalog(self) -> None:
        help_text = render_look_help()

        self.assertNotIn("不发送预告", help_text)
        self.assertIn("支持站点", help_text)
        self.assertIn("Ankara(LTAC)", help_text)
        self.assertIn("Munich(EDDM)", help_text)
        self.assertIn("seo", help_text)

    def test_station_alias_helpers_match_supported_catalog(self) -> None:
        labels = supported_station_labels()
        aliases = common_alias_examples()

        self.assertGreaterEqual(len(labels), 16)
        self.assertIn("seo", aliases)
        self.assertIn("lko", aliases)
        self.assertEqual(resolve_station("seo").icao, "RKSI")
        self.assertEqual(resolve_station("sel").icao, "RKSI")
        self.assertEqual(resolve_station("mun").icao, "EDDM")
        self.assertEqual(resolve_station("lko").icao, "VILK")


if __name__ == "__main__":
    unittest.main()
