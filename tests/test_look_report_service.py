import sys
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
SCRIPTS = ROOT / "scripts"
if str(SCRIPTS) not in sys.path:
    sys.path.insert(0, str(SCRIPTS))

from look_report_service import _render_footer, _should_build_ensemble_factor  # noqa: E402


class LookReportServiceTests(unittest.TestCase):
    def test_same_day_ensemble_gate_opens_from_three_hours_to_peak(self) -> None:
        primary_window = {"peak_local": "2026-03-09T16:00"}
        metar_diag = {"latest_report_local": "2026-03-09T13:00:00+00:00"}

        self.assertTrue(_should_build_ensemble_factor(primary_window, metar_diag))

    def test_same_day_ensemble_gate_stays_closed_inside_three_hours(self) -> None:
        primary_window = {"peak_local": "2026-03-09T16:00"}
        metar_diag = {"latest_report_local": "2026-03-09T13:30:00+00:00"}

        self.assertFalse(_should_build_ensemble_factor(primary_window, metar_diag))

    def test_same_day_ensemble_gate_can_open_early_when_observations_show_strong_path_signal(self) -> None:
        primary_window = {"peak_local": "2026-03-09T16:00"}
        metar_diag = {
            "latest_report_local": "2026-03-09T14:30:00+00:00",
            "temp_trend_smooth_c": 0.24,
            "temp_bias_smooth_c": 0.28,
            "cloud_effective_cover_smooth": 0.18,
            "radiation_eff_smooth": 0.84,
            "cloud_trend": "cloud thinning",
        }

        self.assertTrue(_should_build_ensemble_factor(primary_window, metar_diag))

    def test_render_footer_uses_plain_sounding_label(self) -> None:
        footer = _render_footer(
            {
                "polymarket_event": "https://example.com/poly",
                "metar_24h": "https://example.com/metar",
                "wunderground": "https://example.com/wu",
                "weather_map": "https://example.com/map",
                "sounding_tropicaltidbits": "https://example.com/sounding",
            }
        )

        self.assertIn("[探空图](https://example.com/sounding)", footer)
        self.assertNotIn("Tropicaltidbits", footer)


if __name__ == "__main__":
    unittest.main()
