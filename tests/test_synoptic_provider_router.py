import sys
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
SCRIPTS = ROOT / "scripts"
if str(SCRIPTS) not in sys.path:
    sys.path.insert(0, str(SCRIPTS))

from synoptic_provider_router import (  # noqa: E402
    DEFAULT_SYNOPTIC_PROVIDER,
    normalize_synoptic_provider,
    provider_candidates,
)


class SynopticProviderRouterTest(unittest.TestCase):
    def test_default_provider_is_ecmwf_open_data(self) -> None:
        self.assertEqual(DEFAULT_SYNOPTIC_PROVIDER, "ecmwf-open-data")
        self.assertEqual(normalize_synoptic_provider(None), "ecmwf-open-data")
        self.assertEqual(normalize_synoptic_provider("ecmwf"), "ecmwf-open-data")
        self.assertEqual(normalize_synoptic_provider("gfs"), "gfs-grib2")

    def test_ecmwf_chain_falls_back_to_gfs(self) -> None:
        self.assertEqual(
            provider_candidates("ecmwf-open-data"),
            ["ecmwf-open-data", "gfs-grib2"],
        )
        self.assertEqual(provider_candidates("gfs-grib2"), ["gfs-grib2"])


if __name__ == "__main__":
    unittest.main()
