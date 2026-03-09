import sys
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
SCRIPTS = ROOT / "scripts"
if str(SCRIPTS) not in sys.path:
    sys.path.insert(0, str(SCRIPTS))

from canonical_raw_state_service import build_canonical_raw_state  # noqa: E402
from posterior_feature_service import build_posterior_feature_vector  # noqa: E402
from quality_snapshot_service import build_quality_snapshot  # noqa: E402


class QualitySnapshotServiceTest(unittest.TestCase):
    def test_builds_uncertainty_controls_from_runtime_coverage(self) -> None:
        primary_window = {
            "start_local": "2026-03-09T11:00",
            "peak_local": "2026-03-09T14:00",
            "end_local": "2026-03-09T16:00",
            "peak_temp_c": 24.0,
            "low_cloud_pct": 30.0,
            "w850_kmh": 24.0,
        }
        metar_diag = {
            "latest_report_local": "2026-03-09T11:30:00+00:00",
            "latest_temp": 18.0,
            "latest_dewpoint": 7.0,
            "latest_rh": 48.0,
            "latest_wspd": 6.0,
            "latest_wdir": 180.0,
            "latest_cloud_code": "SCT",
            "latest_wx": "",
            "cloud_effective_cover_smooth": 0.35,
            "radiation_eff_smooth": 0.72,
            "cloud_trend": "steady",
            "latest_precip_state": "none",
            "precip_trend": "none",
            "temp_trend_smooth_c": 0.18,
            "temp_bias_smooth_c": -0.10,
            "observed_max_temp_c": 18.0,
            "observed_max_interval_lo_c": 17.8,
            "observed_max_interval_hi_c": 18.3,
            "metar_temp_quantized": True,
            "metar_routine_cadence_min": 60,
            "metar_recent_interval_min": 60,
        }
        forecast_decision = {
            "meta": {
                "station": "VIDP",
                "date": "2026-03-09",
                "model": "ifs",
                "synoptic_provider": "gfs-grib2",
                "runtime": "2026030900Z",
                "window": dict(primary_window),
            },
            "quality": {
                "source_state": "fresh",
                "missing_layers": ["700"],
                "synoptic_coverage": 0.58,
                "synoptic_provider_requested": "ecmwf-open-data",
                "synoptic_provider_used": "gfs-grib2",
                "synoptic_provider_fallback": True,
                "synoptic_anchors_total": 5,
                "synoptic_anchors_ok": 3,
            },
            "features": {
                "objects_3d": {
                    "main_object": {
                        "track_id": "track_1",
                        "type": "advection_3d",
                        "anchors_count": 2,
                        "confidence": "low",
                    },
                    "count": 1,
                    "anchors_count": 3,
                },
                "h850": {"review": {}},
                "h700": {"summary": ""},
                "h925": {"summary": ""},
                "sounding": {
                    "thermo": {
                        "profile_source": "model_proxy",
                        "coverage": {"density_class": "sparse"},
                    }
                },
            },
            "decision": {
                "background": {
                    "line_500": "高空背景信号有限。",
                    "line_850": "低层输送信号一般。",
                    "extra": "",
                }
            },
        }

        canonical = build_canonical_raw_state(
            primary_window=primary_window,
            metar_diag=metar_diag,
            forecast_decision=forecast_decision,
            temp_unit="C",
        )
        features = build_posterior_feature_vector(
            canonical_raw_state=canonical,
            boundary_layer_regime={"regime_key": "static_stable", "dominant_mechanism": "静稳约束"},
            temp_phase_decision={"phase": "far", "display_phase": "far"},
        )
        quality = build_quality_snapshot(
            canonical_raw_state=canonical,
            posterior_feature_vector=features,
        )

        self.assertEqual(quality["schema_version"], "quality-snapshot.v2")
        self.assertEqual(quality["source"]["synoptic_provider_used"], "gfs-grib2")
        self.assertTrue(quality["source"]["synoptic_provider_fallback"])
        self.assertNotIn("legacy_text_fallback_present", quality["source"])
        self.assertEqual(quality["coverage"]["sounding_density"], "sparse")
        self.assertGreater(quality["posterior_adjustments"]["spread_multiplier"], 1.0)
        self.assertIn("provider_fallback", quality["flags"])


if __name__ == "__main__":
    unittest.main()
