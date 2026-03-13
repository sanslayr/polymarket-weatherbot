import sys
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
SCRIPTS = ROOT / "scripts"
if str(SCRIPTS) not in sys.path:
    sys.path.insert(0, str(SCRIPTS))

from ecmwf_ensemble_factor_service import summarize_member_paths  # noqa: E402


class EcmwfEnsembleFactorServiceTest(unittest.TestCase):
    def test_summarize_member_paths_builds_probabilities_and_split_state(self) -> None:
        current_payload = {
            "members": [
                {"number": 0, "t850_c": 8.2, "wind_speed_850_kmh": 24.0},
                {"number": 1, "t850_c": 8.8, "wind_speed_850_kmh": 28.0},
                {"number": 2, "t850_c": 6.9, "wind_speed_850_kmh": 18.0},
                {"number": 3, "t850_c": 7.5, "wind_speed_850_kmh": 14.0},
            ]
        }
        previous_payload = {
            "members": [
                {"number": 0, "t850_c": 7.1, "wind_speed_850_kmh": 20.0},
                {"number": 1, "t850_c": 7.7, "wind_speed_850_kmh": 22.0},
                {"number": 2, "t850_c": 7.9, "wind_speed_850_kmh": 16.0},
                {"number": 3, "t850_c": 7.4, "wind_speed_850_kmh": 10.0},
            ]
        }

        payload = summarize_member_paths(current_payload, previous_payload)

        self.assertEqual(payload["schema_version"], "ecmwf-ensemble-factor.v2")
        self.assertEqual(payload["member_count"], 4)
        self.assertEqual(payload["summary"]["dominant_path"], "warm_support")
        self.assertEqual(payload["summary"]["split_state"], "mixed")
        self.assertAlmostEqual(payload["probabilities"]["warm_support"], 0.5)
        self.assertAlmostEqual(payload["probabilities"]["cold_suppression"], 0.25)
        self.assertAlmostEqual(payload["probabilities"]["transition"], 0.25)
        self.assertAlmostEqual(payload["summary"]["dominant_margin_prob"], 0.25)
        self.assertAlmostEqual(payload["detail_probabilities"]["neutral_stable"], 0.25)

    def test_transition_bucket_tracks_stable_detail_explicitly(self) -> None:
        current_payload = {
            "members": [
                {"number": 0, "t850_c": 8.0, "wind_speed_850_kmh": 12.0},
                {"number": 1, "t850_c": 8.1, "wind_speed_850_kmh": 14.0},
                {"number": 2, "t850_c": 8.0, "wind_speed_850_kmh": 16.0},
                {"number": 3, "t850_c": 8.2, "wind_speed_850_kmh": 18.0},
            ]
        }
        previous_payload = {
            "members": [
                {"number": 0, "t850_c": 7.9, "wind_speed_850_kmh": 10.0},
                {"number": 1, "t850_c": 8.0, "wind_speed_850_kmh": 12.0},
                {"number": 2, "t850_c": 8.1, "wind_speed_850_kmh": 15.0},
                {"number": 3, "t850_c": 8.3, "wind_speed_850_kmh": 18.0},
            ]
        }

        payload = summarize_member_paths(current_payload, previous_payload)

        self.assertEqual(payload["summary"]["dominant_path"], "transition")
        self.assertEqual(payload["summary"]["dominant_path_detail"], "neutral_stable")
        self.assertEqual(payload["summary"]["transition_detail"], "neutral_stable")
        self.assertEqual(payload["summary"]["split_state"], "clustered")
        self.assertAlmostEqual(payload["probabilities"]["transition"], 1.0)
        self.assertAlmostEqual(payload["detail_probabilities"]["neutral_stable"], 1.0)


if __name__ == "__main__":
    unittest.main()
