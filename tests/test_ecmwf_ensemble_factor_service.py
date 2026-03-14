import sys
import unittest
from pathlib import Path
from unittest.mock import patch


ROOT = Path(__file__).resolve().parents[1]
SCRIPTS = ROOT / "scripts"
if str(SCRIPTS) not in sys.path:
    sys.path.insert(0, str(SCRIPTS))

import ecmwf_ensemble_factor_service as ens_service  # noqa: E402
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

        self.assertEqual(payload["schema_version"], "ecmwf-ensemble-factor.v7")
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

    def test_member_trajectory_captures_next_step_evolution(self) -> None:
        current_payload = {
            "members": [
                {"number": 0, "t850_c": 8.0, "wind_speed_850_kmh": 18.0},
                {"number": 1, "t850_c": 8.1, "wind_speed_850_kmh": 20.0},
            ]
        }
        previous_payload = {
            "members": [
                {"number": 0, "t850_c": 7.6, "wind_speed_850_kmh": 16.0},
                {"number": 1, "t850_c": 8.0, "wind_speed_850_kmh": 18.0},
            ]
        }
        next_payload = {
            "members": [
                {"number": 0, "t850_c": 8.7, "wind_speed_850_kmh": 21.0},
                {"number": 1, "t850_c": 7.9, "wind_speed_850_kmh": 19.0},
            ]
        }

        from ecmwf_ensemble_factor_service import _build_member_trajectory  # noqa: E402

        trajectory = _build_member_trajectory(
            previous_payload=previous_payload,
            current_payload=current_payload,
            next_payload=next_payload,
            previous_local=__import__("datetime").datetime(2026, 3, 9, 9, 0),
            current_local=__import__("datetime").datetime(2026, 3, 9, 12, 0),
            next_local=__import__("datetime").datetime(2026, 3, 9, 15, 0),
        )

        self.assertEqual(trajectory["dominant_shape"], "warming_follow_through")
        first = trajectory["members"][0]
        self.assertEqual(first["trajectory_shape"], "warming_follow_through")
        self.assertAlmostEqual(first["next3h_t850_delta_c"], 0.7)
        second = trajectory["members"][1]
        self.assertEqual(second["trajectory_shape"], "mixed_transition")

    def test_batch_builder_reuses_multi_station_surface_extracts(self) -> None:
        requests = [
            {
                "request_id": "KATL:2026-03-14",
                "station_icao": "KATL",
                "station_lat": 33.64,
                "station_lon": -84.43,
                "peak_local": "2026-03-14T15:00",
                "analysis_local": "2026-03-14T12:00",
                "tz_name": "America/New_York",
                "preferred_runtime_tag": "2026031412Z",
                "metar24": [],
            },
            {
                "request_id": "KMIA:2026-03-14",
                "station_icao": "KMIA",
                "station_lat": 25.79,
                "station_lon": -80.29,
                "peak_local": "2026-03-14T15:00",
                "analysis_local": "2026-03-14T12:00",
                "tz_name": "America/New_York",
                "preferred_runtime_tag": "2026031412Z",
                "metar24": [],
            },
        ]
        extract_calls: list[int] = []

        def fake_extract_multi(_pf, _cf, stations, _root):
            extract_calls.append(len(stations))
            out = {}
            for idx, station in enumerate(stations):
                station_id = station["id"]
                currentish = 16.0 + idx
                out[station_id] = {
                    "selected_lat": station["lat"],
                    "selected_lon": station["lon"],
                    "valid_time": "2026-03-14T12:00:00",
                    "members": [
                        {"number": 0, "t2m_c": currentish, "td2m_c": 8.0, "wind_speed_10m_kmh": 12.0, "msl_hpa": 1012.0},
                        {"number": 1, "t2m_c": currentish + 0.6, "td2m_c": 8.4, "wind_speed_10m_kmh": 14.0, "msl_hpa": 1011.4},
                    ],
                }
            return out

        with patch.object(ens_service, "_read_cache", return_value=None), patch.object(
            ens_service,
            "_fetch_ensemble_surface_files",
            return_value=(Path("pf.grib2"), Path("cf.grib2"), "2026031412Z", 0),
        ), patch.object(
            ens_service,
            "_extract_point_surface_members_multi",
            side_effect=fake_extract_multi,
        ), patch.object(
            ens_service,
            "_write_cache",
            return_value=None,
        ), patch.object(
            ens_service,
            "_history_surface_local_times",
            side_effect=lambda anchor_local, runtime_tag, max_history_hours=None: [anchor_local],
        ), patch.object(
            ens_service,
            "_local_day_surface_times",
            side_effect=lambda anchor_local: [anchor_local],
        ):
            payloads = ens_service.build_ecmwf_ensemble_factor_batch(requests=requests, detail_stage="auto", root=ROOT)

        self.assertEqual(set(payloads.keys()), {"KATL:2026-03-14", "KMIA:2026-03-14"})
        self.assertTrue(any(size == 2 for size in extract_calls))
        self.assertIn(
            payloads["KATL:2026-03-14"]["source"]["detail_level"],
            {"surface_anchor", "surface_trajectory"},
        )
        self.assertEqual(payloads["KATL:2026-03-14"]["member_count"], 2)


if __name__ == "__main__":
    unittest.main()
