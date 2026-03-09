import sys
import tempfile
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
SCRIPTS = ROOT / "scripts"
if str(SCRIPTS) not in sys.path:
    sys.path.insert(0, str(SCRIPTS))

from synoptic_runner import (  # noqa: E402
    _read_provider_failure_memo,
    _should_continue_to_next_provider,
    _write_provider_failure_memo,
)


class SynopticRunnerProviderFallbackLogicTest(unittest.TestCase):
    def test_rate_limit_continues_to_next_provider_when_available(self) -> None:
        self.assertTrue(
            _should_continue_to_next_provider(
                candidate_index=0,
                total_candidates=2,
                exc=RuntimeError("429 Too Many Requests"),
            )
        )

    def test_rate_limit_stops_when_no_more_candidates(self) -> None:
        self.assertFalse(
            _should_continue_to_next_provider(
                candidate_index=1,
                total_candidates=2,
                exc=RuntimeError("429 Too Many Requests"),
            )
        )

    def test_provider_failure_memo_roundtrip(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            cache_dir = Path(td)
            _write_provider_failure_memo(
                cache_dir,
                provider="ecmwf-open-data",
                scope="global",
                error_type="rate_limit_429",
                error="429 Too Many Requests",
            )
            doc = _read_provider_failure_memo(cache_dir, "ecmwf-open-data", "global")
            self.assertIsNotNone(doc)
            self.assertEqual(doc["error_type"], "rate_limit_429")


if __name__ == "__main__":
    unittest.main()
