import sys
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
SCRIPTS = ROOT / "scripts"
if str(SCRIPTS) not in sys.path:
    sys.path.insert(0, str(SCRIPTS))

from boundary_layer_regime import build_boundary_layer_regime  # noqa: E402
from diagnostics_sounding import diagnose_sounding  # noqa: E402


class BoundaryLayerRegimeTest(unittest.TestCase):
    def test_diagnose_sounding_uses_model_proxy_when_obs_missing(self) -> None:
        primary_window = {
            "peak_temp_c": 9.0,
            "low_cloud_pct": 88.0,
            "w850_kmh": 10.0,
        }
        metar_diag = {
            "latest_report_local": "2026-03-08T09:00:00+01:00",
            "latest_temp": 5.8,
            "latest_dewpoint": 5.4,
            "latest_rh": 97.0,
            "latest_wspd": 3.0,
            "latest_wx": "BR",
            "latest_cloud_lowest_base_ft": 300,
        }

        sounding = diagnose_sounding(
            primary_window,
            metar_diag,
            h700_summary="700hPa 干层信号在外围（约260km）",
            h925_summary="低层耦合偏弱（高空信号落地效率有限）",
            cloud_code_now="OVC",
        )

        thermo = sounding["thermo"]
        self.assertTrue(thermo["has_profile"])
        self.assertEqual(thermo["profile_source"], "model_proxy")
        self.assertEqual(thermo["quality"], "model_proxy")
        self.assertEqual(thermo["vertical_regime"], "low_cloud_clearing")
        self.assertGreaterEqual(float(thermo["low_level_cap_score"]), 0.65)
        self.assertIn("coverage", thermo)
        self.assertEqual(thermo["coverage"]["density_class"], "sparse")
        self.assertIn("增加1000/950/900/800层以识别浅逆温与相变层", thermo["coverage"]["recommendations"])
        self.assertTrue(any("模式层结" in item for item in sounding["items"]))

    def test_boundary_layer_regime_prioritizes_clearing_under_static_stable_signal_mix(self) -> None:
        primary_window = {
            "peak_temp_c": 9.0,
            "low_cloud_pct": 88.0,
            "w850_kmh": 10.0,
        }
        metar_diag = {
            "latest_report_local": "2026-03-08T09:00:00+01:00",
            "latest_temp": 5.8,
            "latest_dewpoint": 5.4,
            "latest_rh": 97.0,
            "latest_wspd": 3.0,
            "latest_wx": "BR",
            "latest_cloud_lowest_base_ft": 300,
            "latest_cloud_code": "OVC",
            "temp_trend_smooth_c": 0.02,
        }
        sounding = diagnose_sounding(
            primary_window,
            metar_diag,
            h700_summary="700hPa 干层信号在外围（约260km）",
            h925_summary="低层耦合偏弱（高空信号落地效率有限）",
            cloud_code_now="OVC",
        )

        regime = build_boundary_layer_regime(
            primary_window=primary_window,
            metar_diag=metar_diag,
            snd_thermo=sounding["thermo"],
            advection_review={
                "has_signal": True,
                "thermal_advection_state": "weak",
                "transport_state": "cold",
                "surface_coupling_state": "weak",
                "surface_role": "background",
                "surface_bias": "cold",
                "surface_effect_weight": 0.08,
            },
            h700_summary="700hPa 干层信号在外围（约260km）",
            h925_summary="低层耦合偏弱（高空信号落地效率有限）",
            line850="850偏冷输送距站偏远或落地不完整，先按背景约束项处理。",
            extra="低云雾仍有维持风险",
            h500_regime="近区槽脊过渡",
            object_type="",
            cloud_code_now="OVC",
        )

        self.assertEqual(regime["regime_key"], "boundary_layer_clearing")
        self.assertEqual(regime["dominant_mechanism"], "低云清除")
        self.assertNotIn("dominant_question", regime)
        self.assertIn("低云和雾何时散开", regime["headline"])
        self.assertIn("低云底", regime["tracking_line"])
        self.assertTrue(
            ("近地面" in regime["layer_summary"])
            or ("低云" in regime["layer_summary"])
            or ("空气" in regime["layer_summary"])
        )

    def test_diagnose_sounding_builds_layer_relationships_from_obs_profile(self) -> None:
        sounding = diagnose_sounding(
            {
                "peak_temp_c": 23.0,
            },
            {},
            obs_context={
                "use_sounding_obs": True,
                "confidence": "H",
                "obs_age_hours": 4.0,
                "thermo": {
                    "has_profile": True,
                    "quality": "complete",
                    "profile_source": "obs",
                    "rh925_pct": 88.0,
                    "rh850_pct": 58.0,
                    "rh700_pct": 34.0,
                    "t925_t850_c": 2.8,
                    "midlevel_rh_pct": 46.0,
                    "wind925_dir_deg": 140.0,
                    "wind850_dir_deg": 182.0,
                    "wind700_dir_deg": 228.0,
                    "wind925_kt": 10.0,
                    "wind850_kt": 19.0,
                    "wind700_kt": 31.0,
                    "low_level_cap_score": 0.88,
                    "mixing_support_score": 0.34,
                },
                "layer_findings": ["925–850hPa存在稳定层（封盖信号），冲高持续性受限。"],
            },
        )

        thermo = sounding["thermo"]
        relationships = thermo["layer_relationships"]
        self.assertEqual(relationships["thermal_structure"], "capped")
        self.assertEqual(relationships["moisture_layering"], "low_moist_mid_dry")
        self.assertEqual(relationships["wind_turning_state"], "veering_with_height")
        self.assertEqual(relationships["coupling_chain_state"], "decoupled")
        self.assertTrue(any("低层湿层上接中层干层" in item for item in thermo["relationship_findings"]))
        self.assertTrue(any("探空层间关系" in item for item in sounding["items"]))

    def test_background_advection_does_not_force_advection_regime(self) -> None:
        regime = build_boundary_layer_regime(
            primary_window={
                "peak_temp_c": 28.0,
                "low_cloud_pct": 30.0,
                "w850_kmh": 18.0,
            },
            metar_diag={
                "latest_report_local": "2026-03-09T10:00:00+05:30",
                "latest_temp": 20.4,
                "latest_dewpoint": 8.0,
                "latest_rh": 48.0,
                "latest_wspd": 4.0,
                "temp_trend_smooth_c": 0.06,
            },
            snd_thermo={
                "profile_source": "model_proxy",
                "low_level_cap_score": 0.25,
                "mixing_support_score": 0.38,
                "midlevel_dry_score": 0.1,
                "midlevel_moist_score": 0.0,
                "layer_findings": ["925–850混合一般，低层仍未形成强落地链条。"],
            },
            advection_review={
                "has_signal": True,
                "thermal_advection_state": "weak",
                "transport_state": "cold",
                "surface_coupling_state": "weak",
                "surface_role": "background",
                "surface_bias": "cold",
                "surface_effect_weight": 0.09,
            },
            h925_summary="低层耦合偏弱（高空信号落地效率有限）",
            line850="850偏冷输送距站偏远或落地不完整，先按背景约束项处理。",
            h500_regime="高空弱信号背景",
            cloud_code_now="FEW",
        )

        self.assertNotEqual(regime["regime_key"], "advection")
        self.assertEqual(regime["advection_role"], "background")

    def test_generic_regime_uses_plain_dry_clear_wording(self) -> None:
        regime = build_boundary_layer_regime(
            primary_window={
                "peak_temp_c": 11.0,
                "low_cloud_pct": 12.0,
                "w850_kmh": 24.0,
            },
            metar_diag={
                "latest_report_local": "2026-03-09T14:20:00+03:00",
                "latest_temp": 8.0,
                "latest_dewpoint": -11.0,
                "latest_rh": 25.0,
                "latest_wspd": 14.0,
                "temp_trend_smooth_c": 0.18,
            },
            snd_thermo={
                "profile_source": "model_proxy",
                "low_level_cap_score": 0.18,
                "mixing_support_score": 0.28,
                "midlevel_dry_score": 0.18,
                "midlevel_moist_score": 0.0,
                "layer_findings": ["925–850混合偏弱，午后升温更要看少云能否维持。"],
            },
            advection_review={
                "has_signal": True,
                "thermal_advection_state": "weak",
                "transport_state": "cold",
                "surface_coupling_state": "weak",
                "surface_role": "background",
                "surface_bias": "cold",
                "surface_effect_weight": 0.08,
            },
            h500_regime="高空弱信号背景",
            cloud_code_now="FEW",
        )

        self.assertEqual(regime["regime_key"], "synoptic")
        self.assertIn("今天没有特别单一的主导因素", regime["headline"])
        self.assertIn("午后升温效率能否继续维持", regime["headline"])
        self.assertIn("低层风场能否继续带动升温", regime["headline"])
        self.assertEqual(regime["thermo"]["vertical_regime"], "dry_clear_mixed")
        self.assertIn("升温势头能否维持", regime["layer_summary"])


if __name__ == "__main__":
    unittest.main()
