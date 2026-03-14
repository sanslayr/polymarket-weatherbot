import sys
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch


ROOT = Path(__file__).resolve().parents[1]
SCRIPTS = ROOT / "scripts"
if str(SCRIPTS) not in sys.path:
    sys.path.insert(0, str(SCRIPTS))

from ecmwf_open_data_provider import (  # noqa: E402
    _active_fetch_cooldown,
    _fetch_pair,
    _record_fetch_cooldown,
    _retrieve_grib,
    build_2d_grid_payload_ecmwf,
)


class EcmwfOpenDataProviderReuseTest(unittest.TestCase):
    def test_outer500_reuses_full_pressure_when_available(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            workspace = Path(td)
            full_pressure = workspace / "ecmwf_2026030900Z_oper_f012_full_pl.grib2"
            full_pressure.write_text("x", encoding="utf-8")
            with patch("ecmwf_open_data_provider._retrieve_grib") as mocked:
                pressure, surface = _fetch_pair(
                    workspace=workspace,
                    runtime_tag="2026030900Z",
                    fh=12,
                    stream="oper",
                    field_profile="outer500",
                    source="azure",
                    model_name="ifs",
                )
            self.assertEqual(pressure, full_pressure)
            self.assertIsNone(surface)
            mocked.assert_not_called()

    def test_full_profile_parser_receives_surface_paths(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            venv_py = root / ".venv_nwp" / "bin" / "python"
            venv_py.parent.mkdir(parents=True, exist_ok=True)
            venv_py.write_text("", encoding="utf-8")
            pressure_a = root / "a_pl.grib2"
            surface_a = root / "a_sfc.grib2"
            pressure_p = root / "p_pl.grib2"
            surface_p = root / "p_sfc.grib2"
            for p in (pressure_a, surface_a, pressure_p, surface_p):
                p.write_text("", encoding="utf-8")

            calls = []

            def fake_run(argv, **kwargs):
                calls.append(list(argv))
                return SimpleNamespace(
                    returncode=0,
                    stdout='{"lat":[0.0],"lon":[0.0],"fields":{"mslp_hpa":[[1010.0]],"z500_gpm":[[5600.0]],"t850_c":[[1.0]],"u850_ms":[[1.0]],"v850_ms":[[1.0]],"rh850_pct":[[80.0]],"t700_c":[[0.0]],"u700_ms":[[1.0]],"v700_ms":[[1.0]],"rh700_pct":[[70.0]],"t925_c":[[2.0]],"u925_ms":[[1.0]],"v925_ms":[[1.0]],"rh925_pct":[[85.0]]},"previous_fields":{"mslp_hpa":[[1011.0]],"z500_gpm":[[5590.0]]}}',
                    stderr="",
                )

            with (
                patch("ecmwf_open_data_provider._repo_root", return_value=root),
                patch(
                    "ecmwf_open_data_provider._ensure_pair_with_cycle_fallback",
                    side_effect=[
                        (pressure_a, surface_a, "2026030900Z", 12, "oper"),
                        (pressure_p, surface_p, "2026030900Z", 6, "oper"),
                    ],
                ),
                patch("ecmwf_open_data_provider.subprocess.run", side_effect=fake_run),
            ):
                build_2d_grid_payload_ecmwf(
                    station_icao="LTAC",
                    station_lat=40.0,
                    station_lon=33.0,
                    lat_min=39.0,
                    lat_max=41.0,
                    lon_min=32.0,
                    lon_max=34.0,
                    analysis_time_local="2026-03-09T15:00",
                    previous_time_local="2026-03-09T09:00",
                    tz_name="Europe/Istanbul",
                    cycle_tag="2026030900Z",
                    field_profile="full",
                    root=root,
                )

            self.assertEqual(len(calls), 1)
            self.assertEqual(calls[0][3:7], [str(pressure_a), str(surface_a), str(pressure_p), str(surface_p)])

    def test_retrieve_grib_respects_active_cooldown(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            venv_py = root / ".venv_nwp" / "bin" / "python"
            venv_py.parent.mkdir(parents=True, exist_ok=True)
            venv_py.write_text("", encoding="utf-8")
            request = {
                "stream": "enfo",
                "type": "pf",
                "levtype": "pl",
                "step": 12,
                "date": "2026-03-14",
                "time": 0,
            }
            _record_fetch_cooldown(
                root=root,
                source="azure",
                model="ifs",
                request=request,
                seconds=300,
                reason="429 test",
            )
            with (
                patch("ecmwf_open_data_provider._repo_root", return_value=root),
                patch("ecmwf_open_data_provider.repo_venv_python", return_value=venv_py),
                patch("ecmwf_open_data_provider.subprocess.run") as mocked,
            ):
                with self.assertRaises(RuntimeError) as ctx:
                    _retrieve_grib(
                        target=root / "test.grib2",
                        request=request,
                        source="azure",
                        model="ifs",
                        root=root,
                    )
            self.assertIn("cooldown active", str(ctx.exception))
            mocked.assert_not_called()

    def test_retrieve_grib_records_rate_limit_cooldown(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            venv_py = root / ".venv_nwp" / "bin" / "python"
            venv_py.parent.mkdir(parents=True, exist_ok=True)
            venv_py.write_text("", encoding="utf-8")
            request = {
                "stream": "enfo",
                "type": "pf",
                "levtype": "pl",
                "step": 12,
                "date": "2026-03-14",
                "time": 0,
            }
            with (
                patch("ecmwf_open_data_provider._repo_root", return_value=root),
                patch("ecmwf_open_data_provider.repo_venv_python", return_value=venv_py),
                patch(
                    "ecmwf_open_data_provider.subprocess.run",
                    return_value=SimpleNamespace(returncode=1, stdout="", stderr="429 Too Many Requests"),
                ),
            ):
                with self.assertRaises(RuntimeError):
                    _retrieve_grib(
                        target=root / "test.grib2",
                        request=request,
                        source="azure",
                        model="ifs",
                        root=root,
                    )
            active, reason = _active_fetch_cooldown(
                root=root,
                source="azure",
                model="ifs",
                request=request,
            )
            self.assertTrue(active)
            self.assertIn("429", reason)


if __name__ == "__main__":
    unittest.main()
