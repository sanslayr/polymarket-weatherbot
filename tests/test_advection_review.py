import sys
import unittest
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo


ROOT = Path(__file__).resolve().parents[1]
SCRIPTS = ROOT / "scripts"
if str(SCRIPTS) not in sys.path:
    sys.path.insert(0, str(SCRIPTS))

from advection_review import build_850_advection_review  # noqa: E402


class AdvectionReviewTest(unittest.TestCase):
    def test_remote_weakly_coupled_cold_advection_is_downgraded(self) -> None:
        primary_window = {
            "start_local": "2026-03-09T14:00",
            "end_local": "2026-03-09T17:00",
            "w850_kmh": 22.0,
            "low_cloud_pct": 35.0,
        }
        systems = [
            {
                "system_type": "cold_advection",
                "geo_context": {
                    "distance_km": 520.0,
                    "distance_band": "300-800km",
                    "center_lat": 29.8,
                    "center_lon": 80.2,
                },
            }
        ]

        review = build_850_advection_review(
            systems,
            now_local=datetime(2026, 3, 9, 9, 0, tzinfo=ZoneInfo("Asia/Kolkata")),
            primary_window=primary_window,
            h925_summary="低层耦合偏弱（高空信号落地效率有限）",
            terrain_tag="内陆平原",
        )

        self.assertEqual(review["advection_type"], "cold")
        self.assertEqual(review["transport_state"], "cold")
        self.assertIn(review["thermal_advection_state"], {"weak", "none"})
        self.assertEqual(review["surface_coupling_state"], "weak")
        self.assertIn(review["surface_role"], {"background", "low_representativeness"})
        self.assertLess(float(review["surface_effect_weight"]), 0.2)
        self.assertNotIn("冷平流", review["summary_line"])
        self.assertNotIn("平流", review["summary_line"])

    def test_near_warm_advection_with_good_coupling_can_stay_foreground(self) -> None:
        primary_window = {
            "start_local": "2026-03-09T14:00",
            "end_local": "2026-03-09T18:00",
            "w850_kmh": 48.0,
            "low_cloud_pct": 18.0,
        }
        systems = [
            {
                "system_type": "warm_advection",
                "geo_context": {
                    "distance_km": 120.0,
                    "distance_band": "0-300km",
                    "center_lat": 48.4,
                    "center_lon": 2.8,
                },
            }
        ]

        review = build_850_advection_review(
            systems,
            now_local=datetime(2026, 3, 9, 9, 0, tzinfo=ZoneInfo("Europe/Paris")),
            primary_window=primary_window,
            h925_summary="低层耦合偏强（暖平流更易下传）",
            terrain_tag="盆地平原",
        )

        self.assertEqual(review["advection_type"], "warm")
        self.assertEqual(review["transport_state"], "warm")
        self.assertIn(review["thermal_advection_state"], {"probable", "confirmed"})
        self.assertIn(review["surface_coupling_state"], {"partial", "strong"})
        self.assertIn(review["surface_role"], {"dominant", "influence"})
        self.assertGreaterEqual(float(review["surface_effect_weight"]), 0.28)
        self.assertIn("暖平流", review["summary_line"])


if __name__ == "__main__":
    unittest.main()
