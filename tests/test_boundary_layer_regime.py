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
        self.assertGreaterEqual(float(thermo["low_level_cap_score"]), 0.65)
        self.assertTrue(any("模式层结" in item for item in sounding["items"]))

    def test_boundary_layer_regime_marks_paris_like_case_as_clearing_problem(self) -> None:
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
            h700_summary="700hPa 干层信号在外围（约260km）",
            h925_summary="低层耦合偏弱（高空信号落地效率有限）",
            line850="低层输送信号一般",
            extra="低云雾仍有维持风险",
            h500_regime="近区槽脊过渡",
            object_type="",
            cloud_code_now="OVC",
        )

        self.assertEqual(regime["regime_key"], "boundary_layer_clearing")
        self.assertEqual(regime["dominant_question"], "散云题")
        self.assertIn("边界层清除题", regime["headline"])
        self.assertIn("低云底", regime["tracking_line"])
        self.assertTrue(("低层" in regime["layer_summary"]) or ("高湿" in regime["layer_summary"]))


if __name__ == "__main__":
    unittest.main()
