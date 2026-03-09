import sys
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
SCRIPTS = ROOT / "scripts"
if str(SCRIPTS) not in sys.path:
    sys.path.insert(0, str(SCRIPTS))

from forecast_pipeline import _merge_synoptic_payloads  # noqa: E402
from vertical_3d import build_3d_objects  # noqa: E402


class Vertical3DTrackingTest(unittest.TestCase):
    def test_anchor_tracking_preserves_time_and_approach(self) -> None:
        payloads = [
            {
                "analysis_time_utc": "2026-03-09T00:00:00Z",
                "analysis_time_local": "2026-03-09T09:00",
                "scale_summary": {
                    "synoptic": {
                        "systems": [
                            {
                                "level": "850",
                                "system_type": "warm_advection_band",
                                "center_lat": 18.0,
                                "center_lon": 20.0,
                                "distance_to_station_km": 890.0,
                                "intensity_k_per_6h": 1.8,
                            },
                            {
                                "level": "925",
                                "system_type": "warm_advection_band",
                                "center_lat": 18.2,
                                "center_lon": 20.1,
                                "distance_to_station_km": 912.0,
                                "intensity_k_per_6h": 1.6,
                            },
                        ]
                    }
                },
            },
            {
                "analysis_time_utc": "2026-03-09T00:00:00Z",
                "analysis_time_local": "2026-03-09T09:00",
                "scale_summary": {
                    "synoptic": {
                        "systems": [
                            {
                                "level": "500",
                                "system_type": "trough_axis",
                                "center_lat": 17.8,
                                "center_lon": 19.8,
                                "distance_to_station_km": 870.0,
                                "intensity_gpm": 40.0,
                            }
                        ]
                    }
                },
            },
            {
                "analysis_time_utc": "2026-03-09T06:00:00Z",
                "analysis_time_local": "2026-03-09T15:00",
                "scale_summary": {
                    "synoptic": {
                        "systems": [
                            {
                                "level": "850",
                                "system_type": "warm_advection_band",
                                "center_lat": 15.0,
                                "center_lon": 20.0,
                                "distance_to_station_km": 556.0,
                                "intensity_k_per_6h": 2.0,
                            },
                            {
                                "level": "925",
                                "system_type": "warm_advection_band",
                                "center_lat": 15.2,
                                "center_lon": 20.1,
                                "distance_to_station_km": 580.0,
                                "intensity_k_per_6h": 1.9,
                            },
                        ]
                    }
                },
            },
            {
                "analysis_time_utc": "2026-03-09T12:00:00Z",
                "analysis_time_local": "2026-03-09T21:00",
                "scale_summary": {
                    "synoptic": {
                        "systems": [
                            {
                                "level": "850",
                                "system_type": "warm_advection_band",
                                "center_lat": 12.0,
                                "center_lon": 20.0,
                                "distance_to_station_km": 223.0,
                                "intensity_k_per_6h": 2.3,
                            },
                            {
                                "level": "925",
                                "system_type": "warm_advection_band",
                                "center_lat": 12.1,
                                "center_lon": 20.0,
                                "distance_to_station_km": 234.0,
                                "intensity_k_per_6h": 2.1,
                            },
                        ]
                    }
                },
            },
        ]

        merged = _merge_synoptic_payloads(payloads)
        self.assertEqual(merged["anchor_count"], 3)

        objects = build_3d_objects(
            synoptic=merged,
            station_lat=10.0,
            station_lon=20.0,
            primary_window={
                "peak_temp_c": 29.0,
                "low_cloud_pct": 20.0,
                "w850_kmh": 26.0,
            },
            diag700={"summary": "700hPa 干层特征偏明显"},
            diag925={"summary": "925层耦合偏强"},
        )

        self.assertEqual(objects["anchors_count"], 3)
        self.assertGreaterEqual(len(objects["tracks"]), 1)

        main = objects["main_object"]
        self.assertEqual(main["anchors_count"], 3)
        self.assertEqual(main["evolution"], "approaching")
        self.assertEqual(main["closest_approach_time_utc"], "2026-03-09T12:00:00Z")
        self.assertLessEqual(float(main["distance_km_min"]), 230.0)


if __name__ == "__main__":
    unittest.main()
