import sys
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
SCRIPTS = ROOT / "scripts"
if str(SCRIPTS) not in sys.path:
    sys.path.insert(0, str(SCRIPTS))

from historical_strategy import blend_historical_range  # noqa: E402


class HistoricalStrategyTest(unittest.TestCase):
    def test_strong_supportive_reference_can_shift_range_center(self) -> None:
        metar_diag = {
            "historical": {
                "context": {
                    "station_prior": {
                        "special_features": "late-day surge risk",
                    }
                },
                "branch_assessment": {
                    "branch_mode": "converged",
                },
                "weighted_reference": {
                    "recommended_tmax_c": 26.4,
                    "recommended_tmax_p25_c": 25.9,
                    "recommended_tmax_p75_c": 26.8,
                    "analog_count": 4,
                    "reference_strength": "strong",
                    "selected_branch": "干混合增温",
                    "synoptic_alignment": "supportive",
                },
            }
        }

        core_lo, core_hi, disp_lo, disp_hi, blend = blend_historical_range(
            metar_diag=metar_diag,
            phase_now="near_window",
            compact_settled_mode=False,
            core_lo=24.6,
            core_hi=25.4,
            disp_lo=24.3,
            disp_hi=25.7,
        )

        self.assertIsNotNone(blend)
        assert blend is not None
        self.assertTrue(blend["applied"])
        self.assertFalse(blend["advisory_only"])
        self.assertGreater(blend["shift_c"], 0.0)
        self.assertGreater((core_lo + core_hi) / 2.0, 25.0)
        self.assertGreater(disp_hi, 25.7)

    def test_weak_reference_remains_advisory_only(self) -> None:
        metar_diag = {
            "historical": {
                "context": {
                    "station_prior": {
                        "special_features": "balanced baseline station",
                    }
                },
                "branch_assessment": {
                    "branch_mode": "preferred",
                },
                "weighted_reference": {
                    "recommended_tmax_c": 23.4,
                    "recommended_tmax_p25_c": 23.0,
                    "recommended_tmax_p75_c": 23.8,
                    "analog_count": 3,
                    "reference_strength": "weak",
                    "selected_branch": "过渡型",
                    "synoptic_alignment": "neutral",
                },
            }
        }

        core_lo, core_hi, disp_lo, disp_hi, blend = blend_historical_range(
            metar_diag=metar_diag,
            phase_now="far",
            compact_settled_mode=False,
            core_lo=22.8,
            core_hi=23.6,
            disp_lo=22.4,
            disp_hi=24.0,
        )

        self.assertIsNotNone(blend)
        assert blend is not None
        self.assertFalse(blend["applied"])
        self.assertTrue(blend["advisory_only"])
        self.assertEqual((core_lo, core_hi, disp_lo, disp_hi), (22.8, 23.6, 22.4, 24.0))


if __name__ == "__main__":
    unittest.main()
