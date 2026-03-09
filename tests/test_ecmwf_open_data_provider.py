import sys
import unittest
from datetime import datetime, timezone
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
SCRIPTS = ROOT / "scripts"
if str(SCRIPTS) not in sys.path:
    sys.path.insert(0, str(SCRIPTS))

from ecmwf_open_data_provider import resolve_ecmwf_runtime_for_valid_time  # noqa: E402


class EcmwfOpenDataProviderTest(unittest.TestCase):
    def test_resolve_prefers_same_cycle_when_step_valid(self) -> None:
        valid_utc = datetime(2026, 3, 10, 18, tzinfo=timezone.utc)
        runtime_tag, fh, stream = resolve_ecmwf_runtime_for_valid_time(
            valid_utc,
            "2026031018Z",
        )
        self.assertEqual(runtime_tag, "2026031018Z")
        self.assertEqual(fh, 0)
        self.assertEqual(stream, "scda")

    def test_resolve_falls_back_to_12z_oper_when_preferred_cycle_is_after_valid(self) -> None:
        valid_utc = datetime(2026, 3, 10, 12, tzinfo=timezone.utc)
        runtime_tag, fh, stream = resolve_ecmwf_runtime_for_valid_time(
            valid_utc,
            "2026031018Z",
        )
        self.assertEqual(runtime_tag, "2026031012Z")
        self.assertEqual(fh, 0)
        self.assertEqual(stream, "oper")


if __name__ == "__main__":
    unittest.main()
