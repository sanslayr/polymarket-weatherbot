from __future__ import annotations

import json
import os
import subprocess
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from runtime_utils import runtime_dt_from_tag, runtime_tag_from_dt


def _repo_root(root: Path) -> Path:
    candidates = [
        root / "skills" / "polymarket-weatherbot",
        root,
        Path(__file__).resolve().parent.parent,
    ]
    for candidate in candidates:
        if (candidate / ".venv_gfs").exists():
            return candidate
    return Path(__file__).resolve().parent.parent


@contextmanager
def _ecmwf_workspace(root: Path, subdir: str):
    path = _repo_root(root) / "cache" / "runtime" / subdir
    path.mkdir(parents=True, exist_ok=True)
    yield path


def _ecmwf_step_valid(runtime_hour: int, fh: int) -> bool:
    if fh < 0 or fh % 3 != 0:
        return False
    if runtime_hour in {0, 12}:
        if fh <= 144:
            return True
        return fh <= 240 and fh % 6 == 0
    if runtime_hour in {6, 18}:
        return fh <= 90
    return False


def resolve_ecmwf_runtime_for_valid_time(
    valid_utc: datetime,
    preferred_runtime_tag: str,
    *,
    max_back_cycles: int = 4,
) -> tuple[str, int, str]:
    rt = runtime_dt_from_tag(preferred_runtime_tag)
    for _ in range(max_back_cycles + 1):
        fh = int(round((valid_utc - rt).total_seconds() / 3600.0))
        if _ecmwf_step_valid(rt.hour, fh):
            stream = "oper" if rt.hour in {0, 12} else "scda"
            return runtime_tag_from_dt(rt), fh, stream
        rt = rt - timedelta(hours=6)
    fh = int(round((valid_utc - rt).total_seconds() / 3600.0))
    stream = "oper" if rt.hour in {0, 12} else "scda"
    return runtime_tag_from_dt(rt), fh, stream


def _build_request(
    *,
    runtime_dt: datetime,
    step: int,
    stream: str,
    field_profile: str,
) -> tuple[dict[str, Any], dict[str, Any]]:
    base = {
        "date": runtime_dt.strftime("%Y-%m-%d"),
        "time": int(runtime_dt.strftime("%H")),
        "stream": stream,
        "type": "fc",
        "step": int(step),
    }
    profile = str(field_profile or "full").strip().lower()
    if profile == "outer500":
        pressure_request = {
            **base,
            "levtype": "pl",
            "levelist": 500,
            "param": ["gh", "u"],
        }
    else:
        pressure_request = {
            **base,
            "levtype": "pl",
            "levelist": [500, 700, 850, 925],
            "param": ["gh", "t", "u", "v", "r"],
        }
    surface_request = {
        **base,
        "levtype": "sfc",
        "param": "msl",
    }
    return pressure_request, surface_request


def _retrieve_grib(
    *,
    target: Path,
    request: dict[str, Any],
    source: str,
    model: str,
    root: Path,
) -> None:
    target.parent.mkdir(parents=True, exist_ok=True)
    py = _repo_root(root) / ".venv_gfs" / "bin" / "python"
    if not py.exists():
        raise RuntimeError("ecmwf-opendata runtime missing (.venv_gfs/bin/python)")
    code = r'''
import json, sys
from ecmwf.opendata import Client

target = sys.argv[1]
source = sys.argv[2]
model = sys.argv[3]
request = json.loads(sys.argv[4])
client = Client(
    source=source,
    model=model,
    resol="0p25",
    preserve_request_order=False,
    infer_stream_keyword=False,
)
client.retrieve(request=request, target=target)
'''
    proc = subprocess.run(
        [str(py), "-c", code, str(target), str(source), str(model), json.dumps(request)],
        capture_output=True,
        text=True,
        timeout=180,
    )
    if proc.returncode != 0:
        raise RuntimeError(f"ecmwf open data fetch failed: {proc.stderr.strip() or proc.stdout.strip()}")


def _fetch_pair(
    *,
    workspace: Path,
    runtime_tag: str,
    fh: int,
    stream: str,
    field_profile: str,
    source: str,
    model_name: str,
) -> tuple[Path, Path]:
    profile = str(field_profile or "full").strip().lower()
    pressure_target = workspace / f"ecmwf_{runtime_tag}_{stream}_f{fh:03d}_{profile}_pl.grib2"
    surface_target = workspace / f"ecmwf_{runtime_tag}_{stream}_f{fh:03d}_{profile}_sfc.grib2"
    if pressure_target.exists() and surface_target.exists():
        return pressure_target, surface_target

    runtime_dt = runtime_dt_from_tag(runtime_tag)
    pressure_request, surface_request = _build_request(
        runtime_dt=runtime_dt,
        step=fh,
        stream=stream,
        field_profile=profile,
    )
    if not pressure_target.exists():
        _retrieve_grib(
            target=pressure_target,
            request=pressure_request,
            source=source,
            model=model_name,
            root=workspace.parents[2],
        )
    if not surface_target.exists():
        _retrieve_grib(
            target=surface_target,
            request=surface_request,
            source=source,
            model=model_name,
            root=workspace.parents[2],
        )
    return pressure_target, surface_target


def _ensure_pair_with_cycle_fallback(
    *,
    workspace: Path,
    valid_utc: datetime,
    preferred_runtime_tag: str,
    field_profile: str,
    source: str,
    model_name: str,
    root: Path,
    max_back_cycles: int = 6,
) -> tuple[Path, Path, str, int, str]:
    base_runtime = runtime_dt_from_tag(preferred_runtime_tag)
    last_err: Exception | None = None
    for back in range(max_back_cycles + 1):
        runtime_try = base_runtime - timedelta(hours=6 * back)
        fh_try = int(round((valid_utc - runtime_try).total_seconds() / 3600.0))
        if not _ecmwf_step_valid(runtime_try.hour, fh_try):
            continue
        runtime_tag = runtime_tag_from_dt(runtime_try)
        stream = "oper" if runtime_try.hour in {0, 12} else "scda"
        try:
            pressure, surface = _fetch_pair(
                workspace=workspace,
                runtime_tag=runtime_tag,
                fh=fh_try,
                stream=stream,
                field_profile=field_profile,
                source=source,
                model_name=model_name,
            )
            return pressure, surface, runtime_tag, fh_try, stream
        except Exception as exc:
            last_err = exc
            continue
    raise RuntimeError(f"ecmwf cycle fallback exhausted: {last_err}")


def build_2d_grid_payload_ecmwf(
    *,
    station_icao: str,
    station_lat: float,
    station_lon: float,
    lat_min: float,
    lat_max: float,
    lon_min: float,
    lon_max: float,
    analysis_time_local: str,
    previous_time_local: str,
    tz_name: str,
    cycle_tag: str,
    field_profile: str = "full",
    root: Path,
    source: str | None = None,
    model_name: str | None = None,
) -> dict[str, Any]:
    from zoneinfo import ZoneInfo

    z = ZoneInfo(tz_name)
    a_local = datetime.strptime(analysis_time_local, "%Y-%m-%dT%H:%M").replace(tzinfo=z)
    p_local = datetime.strptime(previous_time_local, "%Y-%m-%dT%H:%M").replace(tzinfo=z)
    a_utc = a_local.astimezone(timezone.utc)
    p_utc = p_local.astimezone(timezone.utc)

    src = str(source or os.getenv("ECMWF_OPEN_DATA_SOURCE", "azure")).strip().lower() or "azure"
    mdl = str(model_name or os.getenv("ECMWF_OPEN_DATA_MODEL", "ifs")).strip().lower() or "ifs"
    profile = str(field_profile or "full").strip().lower()

    with _ecmwf_workspace(root, "ecmwf_open_data") as workspace:
        pressure_a, surface_a, rt_tag_a, fh_a, stream_a = _ensure_pair_with_cycle_fallback(
            workspace=workspace,
            valid_utc=a_utc,
            preferred_runtime_tag=cycle_tag,
            field_profile=profile,
            source=src,
            model_name=mdl,
            root=root,
        )
        pressure_p, surface_p, rt_tag_p, fh_p, stream_p = _ensure_pair_with_cycle_fallback(
            workspace=workspace,
            valid_utc=p_utc,
            preferred_runtime_tag=cycle_tag,
            field_profile=profile,
            source=src,
            model_name=mdl,
            root=root,
        )

        py = _repo_root(root) / ".venv_gfs" / "bin" / "python"
        if not py.exists():
            raise RuntimeError("grib parser venv missing (.venv_gfs)")

        if profile == "outer500":
            code = r'''
import json, sys, xarray as xr
import numpy as np
pa, sa, pp, sp = sys.argv[1], sys.argv[2], sys.argv[3], sys.argv[4]
lat_min, lat_max, lon_min, lon_max = map(float, sys.argv[5:9])

def open_ds(path):
    return xr.open_dataset(path, engine="cfgrib")

def arr2(v):
    a = np.asarray(v)
    a = np.squeeze(a)
    while a.ndim > 2:
        a = a[0]
    if a.ndim != 2:
        raise RuntimeError(f"expected 2D field, got shape={a.shape}")
    return a

def level_slice(var, level):
    if "isobaricInhPa" in var.dims:
        return arr2(var.sel(isobaricInhPa=level, method="nearest").values)
    return arr2(var.values)

def norm_lon(v):
    x = float(v)
    return ((x + 180.0) % 360.0) - 180.0

da = open_ds(pa)
dp = open_ds(pp)
lat = np.asarray(da["latitude"].values, dtype=float)
lon_raw = np.asarray(da["longitude"].values, dtype=float)
lon_norm = np.asarray([norm_lon(v) for v in lon_raw], dtype=float)

lat_lo = min(lat_min, lat_max)
lat_hi = max(lat_min, lat_max)
lon_lo = min(lon_min, lon_max)
lon_hi = max(lon_min, lon_max)

lat_idx = np.where((lat >= lat_lo) & (lat <= lat_hi))[0]
lon_idx = np.where((lon_norm >= lon_lo) & (lon_norm <= lon_hi))[0]
if len(lat_idx) == 0 or len(lon_idx) == 0:
    raise RuntimeError("ecmwf bbox crop returned empty slice")

def crop(arr):
    return arr[np.ix_(lat_idx, lon_idx)]

z_src = da["gh"] if "gh" in da else da["z"]
z_prev_src = dp["gh"] if "gh" in dp else dp["z"]
z5 = level_slice(z_src, 500.0)
z5_prev = level_slice(z_prev_src, 500.0)
u8 = level_slice(da["u"], 850.0)

out = {
  "lat": lat[lat_idx].tolist(),
  "lon": lon_norm[lon_idx].tolist(),
  "fields": {
    "z500_gpm": crop(z5).tolist(),
    "u850_ms": crop(u8).tolist(),
  },
  "previous_fields": {
    "z500_gpm": crop(z5_prev).tolist(),
  }
}
print(json.dumps(out, ensure_ascii=False))
'''
        else:
            code = r'''
import json, sys, xarray as xr
import numpy as np
pa, sa, pp, sp = sys.argv[1], sys.argv[2], sys.argv[3], sys.argv[4]
lat_min, lat_max, lon_min, lon_max = map(float, sys.argv[5:9])

def open_ds(path):
    return xr.open_dataset(path, engine="cfgrib")

def arr2(v):
    a = np.asarray(v)
    a = np.squeeze(a)
    while a.ndim > 2:
        a = a[0]
    if a.ndim != 2:
        raise RuntimeError(f"expected 2D field, got shape={a.shape}")
    return a

def level_slice(var, level):
    if "isobaricInhPa" in var.dims:
        return arr2(var.sel(isobaricInhPa=level, method="nearest").values)
    return arr2(var.values)

def norm_lon(v):
    x = float(v)
    return ((x + 180.0) % 360.0) - 180.0

da = open_ds(pa)
sa = open_ds(sa)
dp = open_ds(pp)
sp = open_ds(sp)

lat = np.asarray(da["latitude"].values, dtype=float)
lon_raw = np.asarray(da["longitude"].values, dtype=float)
lon_norm = np.asarray([norm_lon(v) for v in lon_raw], dtype=float)

lat_lo = min(lat_min, lat_max)
lat_hi = max(lat_min, lat_max)
lon_lo = min(lon_min, lon_max)
lon_hi = max(lon_min, lon_max)

lat_idx = np.where((lat >= lat_lo) & (lat <= lat_hi))[0]
lon_idx = np.where((lon_norm >= lon_lo) & (lon_norm <= lon_hi))[0]
if len(lat_idx) == 0 or len(lon_idx) == 0:
    raise RuntimeError("ecmwf bbox crop returned empty slice")

def crop(arr):
    return arr[np.ix_(lat_idx, lon_idx)]

pr = arr2(sa["msl"].values)
if np.nanmean(pr) > 2000:
    pr = pr / 100.0
prp = arr2(sp["msl"].values)
if np.nanmean(prp) > 2000:
    prp = prp / 100.0

z_src = da["gh"] if "gh" in da else da["z"]
z_prev_src = dp["gh"] if "gh" in dp else dp["z"]
z5 = level_slice(z_src, 500.0)
z5_prev = level_slice(z_prev_src, 500.0)

t8 = level_slice(da["t"], 850.0)
if np.nanmean(t8) > 170:
    t8 = t8 - 273.15
u8 = level_slice(da["u"], 850.0)
v8 = level_slice(da["v"], 850.0)

t7 = level_slice(da["t"], 700.0)
if np.nanmean(t7) > 170:
    t7 = t7 - 273.15
u7 = level_slice(da["u"], 700.0)
v7 = level_slice(da["v"], 700.0)
r7 = level_slice(da["r"], 700.0)
r8 = level_slice(da["r"], 850.0)
r925 = level_slice(da["r"], 925.0)

t925 = level_slice(da["t"], 925.0)
if np.nanmean(t925) > 170:
    t925 = t925 - 273.15
u925 = level_slice(da["u"], 925.0)
v925 = level_slice(da["v"], 925.0)

out = {
  "lat": lat[lat_idx].tolist(),
  "lon": lon_norm[lon_idx].tolist(),
  "fields": {
    "mslp_hpa": crop(pr).tolist(),
    "z500_gpm": crop(z5).tolist(),
    "t850_c": crop(t8).tolist(),
    "u850_ms": crop(u8).tolist(),
    "v850_ms": crop(v8).tolist(),
    "rh850_pct": crop(r8).tolist(),
    "t700_c": crop(t7).tolist(),
    "u700_ms": crop(u7).tolist(),
    "v700_ms": crop(v7).tolist(),
    "rh700_pct": crop(r7).tolist(),
    "t925_c": crop(t925).tolist(),
    "u925_ms": crop(u925).tolist(),
    "v925_ms": crop(v925).tolist(),
    "rh925_pct": crop(r925).tolist(),
  },
  "previous_fields": {
    "mslp_hpa": crop(prp).tolist(),
    "z500_gpm": crop(z5_prev).tolist(),
  }
}
print(json.dumps(out, ensure_ascii=False))
'''

        proc = subprocess.run(
            [
                str(py),
                "-c",
                code,
                str(pressure_a),
                str(surface_a),
                str(pressure_p),
                str(surface_p),
                str(lat_min),
                str(lat_max),
                str(lon_min),
                str(lon_max),
            ],
            capture_output=True,
            text=True,
            timeout=180,
        )
        if proc.returncode != 0:
            raise RuntimeError(f"ecmwf grid parse failed: {proc.stderr.strip() or proc.stdout.strip()}")
        parsed = json.loads(proc.stdout)

    return {
        "analysis_time_utc": a_utc.strftime("%Y-%m-%dT%H:%M:00Z"),
        "analysis_time_local": analysis_time_local,
        "station": {"icao": station_icao, "lat": station_lat, "lon": station_lon},
        "lat": parsed["lat"],
        "lon": parsed["lon"],
        "fields": parsed["fields"],
        "previous_fields": parsed["previous_fields"],
        "grid_meta": {
            "step_deg": 0.25,
            "bbox": {"lat_min": lat_min, "lat_max": lat_max, "lon_min": lon_min, "lon_max": lon_max},
            "provider": "ecmwf-open-data",
            "field_profile": profile,
            "source": src,
            "model": mdl,
            "analysis_runtime_used": rt_tag_a,
            "analysis_fh_used": int(fh_a),
            "analysis_stream_used": stream_a,
            "previous_runtime_used": rt_tag_p,
            "previous_fh_used": int(fh_p),
            "previous_stream_used": stream_p,
        },
    }
