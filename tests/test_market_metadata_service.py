import sys
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
SCRIPTS = ROOT / "scripts"
if str(SCRIPTS) not in sys.path:
    sys.path.insert(0, str(SCRIPTS))

from market_metadata_service import _bucket_meta_from_slug  # noqa: E402


class MarketMetadataServiceTest(unittest.TestCase):
    def test_bucket_meta_parses_or_below_slug(self) -> None:
        meta = _bucket_meta_from_slug("highest-temperature-in-ankara-on-march-9-2026-6corbelow")
        self.assertEqual(meta["bucket_kind"], "at_or_below")
        self.assertEqual(meta["threshold_c"], 6)

    def test_bucket_meta_parses_exact_slug(self) -> None:
        meta = _bucket_meta_from_slug("highest-temperature-in-ankara-on-march-9-2026-7c")
        self.assertEqual(meta["bucket_kind"], "exact")
        self.assertEqual(meta["threshold_c"], 7)


if __name__ == "__main__":
    unittest.main()
