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
        self.assertIn("Hong Kong(VHHH)", help_text)
        self.assertIn("Munich(EDDM)", help_text)
        self.assertIn("Shanghai(ZSPD)", help_text)
        self.assertIn("Singapore(WSSS)", help_text)
        self.assertIn("Tel Aviv(LLBG)", help_text)
        self.assertIn("Tokyo(RJTT)", help_text)
        self.assertIn("/look hkg", help_text)
        self.assertIn("/look tlv", help_text)
        self.assertIn("/look tok", help_text)
        self.assertIn("/look Singapore", help_text)
        self.assertIn("hkg", help_text)
        self.assertIn("sha", help_text)
        self.assertIn("sin", help_text)
        self.assertIn("seo", help_text)
        self.assertIn("tlv", help_text)
        self.assertIn("tok", help_text)

    def test_station_alias_helpers_match_supported_catalog(self) -> None:
        labels = supported_station_labels()
        aliases = common_alias_examples()

        self.assertGreaterEqual(len(labels), 21)
        self.assertIn("hkg", aliases)
        self.assertIn("sha", aliases)
        self.assertIn("sin", aliases)
        self.assertIn("seo", aliases)
        self.assertIn("lko", aliases)
        self.assertIn("tlv", aliases)
        self.assertIn("tok", aliases)
        self.assertEqual(resolve_station("hkg").icao, "VHHH")
        self.assertEqual(resolve_station("hk").icao, "VHHH")
        self.assertEqual(resolve_station("sha").icao, "ZSPD")
        self.assertEqual(resolve_station("pvg").icao, "ZSPD")
        self.assertEqual(resolve_station("sin").icao, "WSSS")
        self.assertEqual(resolve_station("sg").icao, "WSSS")
        self.assertEqual(resolve_station("seo").icao, "RKSI")
        self.assertEqual(resolve_station("sel").icao, "RKSI")
        self.assertEqual(resolve_station("tlv").icao, "LLBG")
        self.assertEqual(resolve_station("hnd").icao, "RJTT")
        self.assertEqual(resolve_station("tok").icao, "RJTT")
        self.assertEqual(resolve_station("mun").icao, "EDDM")
        self.assertEqual(resolve_station("lko").icao, "VILK")


if __name__ == "__main__":
    unittest.main()
