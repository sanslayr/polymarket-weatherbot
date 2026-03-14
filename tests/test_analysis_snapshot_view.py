import sys
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
SCRIPTS = ROOT / "scripts"
if str(SCRIPTS) not in sys.path:
    sys.path.insert(0, str(SCRIPTS))

from analysis_snapshot_view import (  # noqa: E402
    snapshot_branch_outlook,
    snapshot_path_context,
    snapshot_weather_posterior_anchor,
)


class AnalysisSnapshotViewTests(unittest.TestCase):
    def test_branch_outlook_reads_single_source_from_feature_vector(self) -> None:
        snapshot = {
            "posterior_feature_vector": {
                "matched_branch_outlook_state": {
                    "branch_family": "warm_landing_watch",
                    "next_transition_gate": "low_level_coupling",
                }
            }
        }

        branch_outlook = snapshot_branch_outlook(snapshot)
        self.assertEqual(branch_outlook["branch_family"], "warm_landing_watch")
        self.assertEqual(branch_outlook["next_transition_gate"], "low_level_coupling")

    def test_weather_posterior_anchor_falls_back_to_core_anchor(self) -> None:
        snapshot = {
            "weather_posterior": {
                "core": {
                    "anchor": {
                        "posterior_median_c": 23.4,
                    },
                    "path_context": {
                        "significant_forecast_detail_text": "当前匹配的是暖输送待接地这支",
                    },
                }
            }
        }

        anchor = snapshot_weather_posterior_anchor(snapshot)
        path_context = snapshot_path_context(snapshot)
        self.assertEqual(anchor["posterior_median_c"], 23.4)
        self.assertIn("暖输送待接地", path_context["significant_forecast_detail_text"])


if __name__ == "__main__":
    unittest.main()
