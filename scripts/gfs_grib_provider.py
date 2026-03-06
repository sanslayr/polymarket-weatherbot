from __future__ import annotations

import json
import math
import subprocess
import tempfile
from datetime import datetime, timedelta, timezone
from contextlib import contextmanager
from pathlib import Path
from typing import Any
from urllib.parse import quote

import requests

from runtime_cache_policy import gfs_binary_cache_enabled
from runtime_utils import resolve_runtime_for_valid_time, runtime_dt_from_tag


def _nomads_url(runtime_dt: datetime, fh: int, lat: float, lon: float, span: float = 0.6) -> str:
    rdate = runtime_dt.strftime("%Y%m%d")
    rh = runtime_dt.strftime("%H")
    file = f"gfs.t{rh}z.pgrb2.0p25.f{fh:03d}"
    left = lon - span
    right = lon + span
    bottom = lat - span
    top = lat + span
    q = (
        f"file={file}"
        "&lev_2_m_above_ground=on"
        "&lev_925_mb=on"
        "&lev_850_mb=on"
        "&lev_700_mb=on"
        "&lev_mean_sea_level=on"
        "&lev_low_cloud_layer=on"
        "&var_TMP=on&var_UGRD=on&var_VGRD=on&var_PRMSL=on&var_LCDC=on"
        "&subregion="
        f"&leftlon={left:.3f}&rightlon={right:.3f}&toplat={top:.3f}&bottomlat={bottom:.3f}"
        f"&dir={quote(f'/gfs.{rdate}/{rh}/atmos')}"
    )
    return f"https://nomads.ncep.noaa.gov/cgi-bin/filter_gfs_0p25.pl?{q}"


def _download_grib(path: Path, url: str) -> None:
    r = requests.get(url, timeout=45)
    r.raise_for_status()
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(r.content)


def _repo_root(root: Path) -> Path:
    candidates = [
        root,
        root / "skills" / "polymarket-weatherbot",
        Path(__file__).resolve().parent.parent,
    ]
    for candidate in candidates:
        if (candidate / ".venv_gfs").exists():
            return candidate
    return Path(__file__).resolve().parent.parent


@contextmanager
def _grib_workspace(root: Path, subdir: str):
    repo_root = _repo_root(root)
    if gfs_binary_cache_enabled():
        path = repo_root / "cache" / "runtime" / subdir
        path.mkdir(parents=True, exist_ok=True)
        yield path
        return
    with tempfile.TemporaryDirectory(prefix=f"weatherbot-{subdir}-") as tmp:
        yield Path(tmp)


def _is_http_404(exc: Exception) -> bool:
    try:
        resp = getattr(exc, "response", None)
        return bool(resp is not None and int(getattr(resp, "status_code", 0)) == 404)
    except Exception:
        return False


def _ensure_grib_with_cycle_fallback(
    *,
    cache_root: Path,
    file_suffix: str,
    runtime_dt: datetime,
    fh: int,
    build_url,
    max_back_cycles: int = 3,
) -> tuple[Path, datetime, int]:
    last_err: Exception | None = None
    for back in range(max_back_cycles + 1):
        rt_try = runtime_dt - timedelta(hours=6 * back)
        fh_try = fh + 6 * back
        p = cache_root / f"{rt_try.strftime('%Y%m%d%H')}_f{fh_try:03d}{file_suffix}"
        if p.exists():
            return p, rt_try, fh_try
        try:
            _download_grib(p, build_url(rt_try, fh_try))
            return p, rt_try, fh_try
        except Exception as exc:
            last_err = exc
            if _is_http_404(exc):
                continue
            # non-404 network/parser transient: continue trying older cycle too
            continue
    raise RuntimeError(f"gfs grib download fallback exhausted: {last_err}")


def _extract_point_from_grib(grib_path: Path, lat: float, lon: float, root: Path) -> dict[str, Any]:
    py = _repo_root(root) / ".venv_gfs" / "bin" / "python"
    if not py.exists():
        raise RuntimeError("gfs parser venv missing (.venv_gfs)")

    code = r'''
import json, sys, math
import xarray as xr
p, lat, lon = sys.argv[1], float(sys.argv[2]), float(sys.argv[3])
ds = xr.open_dataset(p, engine="cfgrib")
dsn = ds.sel(latitude=lat, longitude=lon, method="nearest")

def val(name, d=float('nan')):
    try:
        return float(dsn[name].values)
    except Exception:
        return d

t2m = val('t2m'); t850 = val('t'); u850 = val('u'); v850 = val('v'); prmsl = val('prmsl'); lcc = val('lcc')
if t2m > 170: t2m -= 273.15
if t850 > 170: t850 -= 273.15
if prmsl > 2000: prmsl /= 100.0
wspd = math.sqrt(u850*u850 + v850*v850) * 3.6
wdir = (math.degrees(math.atan2(-u850, -v850)) + 360.0) % 360.0
vt = dsn.get('valid_time')
valid = str(vt.values)[:19] if vt is not None else ''
print(json.dumps({
  'time': valid.replace(' ','T')[:16],
  'temperature_2m': round(t2m,2),
  'temperature_850hPa': round(t850,2),
  'wind_speed_850hPa': round(wspd,2),
  'wind_direction_850hPa': round(wdir,1),
  'pressure_msl': round(prmsl,2),
  'cloud_cover_low': max(0.0,min(100.0,round(lcc,1)))
}, ensure_ascii=False))
'''
    proc = subprocess.run([str(py), "-c", code, str(grib_path), str(lat), str(lon)], capture_output=True, text=True, timeout=60)
    if proc.returncode != 0:
        raise RuntimeError(f"gfs grib parse failed: {proc.stderr.strip() or proc.stdout.strip()}")
    return json.loads(proc.stdout.strip())


def _nomads_grid_url(
    runtime_dt: datetime,
    fh: int,
    *,
    lat_min: float,
    lat_max: float,
    lon_min: float,
    lon_max: float,
    field_profile: str = "full",
) -> str:
    rdate = runtime_dt.strftime("%Y%m%d")
    rh = runtime_dt.strftime("%H")
    file = f"gfs.t{rh}z.pgrb2.0p25.f{fh:03d}"
    profile = str(field_profile or "full").strip().lower()
    if profile == "outer500":
        levels = "&lev_500_mb=on&lev_850_mb=on"
        vars_part = "&var_HGT=on&var_UGRD=on"
    else:
        levels = "&lev_500_mb=on&lev_700_mb=on&lev_850_mb=on&lev_925_mb=on&lev_mean_sea_level=on"
        vars_part = "&var_HGT=on&var_TMP=on&var_UGRD=on&var_VGRD=on&var_PRMSL=on"
    q = (
        f"file={file}"
        f"{levels}"
        f"{vars_part}"
        "&subregion="
        f"&leftlon={lon_min:.3f}&rightlon={lon_max:.3f}&toplat={lat_max:.3f}&bottomlat={lat_min:.3f}"
        f"&dir={quote(f'/gfs.{rdate}/{rh}/atmos')}"
    )
    return f"https://nomads.ncep.noaa.gov/cgi-bin/filter_gfs_0p25.pl?{q}"


def build_2d_grid_payload_gfs(
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
) -> dict[str, Any]:
    tz = datetime.now().astimezone().tzinfo
    # parse local clock as tz_name if provided
    from zoneinfo import ZoneInfo
    z = ZoneInfo(tz_name)
    a_local = datetime.strptime(analysis_time_local, "%Y-%m-%dT%H:%M").replace(tzinfo=z)
    p_local = datetime.strptime(previous_time_local, "%Y-%m-%dT%H:%M").replace(tzinfo=z)
    a_utc = a_local.astimezone(timezone.utc)
    p_utc = p_local.astimezone(timezone.utc)

    rt_tag_a, fh_a = resolve_runtime_for_valid_time(a_utc, cycle_tag, cycle_hours=6, max_back_cycles=3)
    rt_tag_p, fh_p = resolve_runtime_for_valid_time(p_utc, cycle_tag, cycle_hours=6, max_back_cycles=3)
    if fh_a < 0 or fh_p < 0:
        raise RuntimeError("gfs runtime fallback exhausted for analysis/previous times")

    rt_dt_a = runtime_dt_from_tag(rt_tag_a)
    rt_dt_p = runtime_dt_from_tag(rt_tag_p)

    profile = str(field_profile or "full").strip().lower()
    bbox_suffix = f"_{lat_min:.2f}_{lat_max:.2f}_{lon_min:.2f}_{lon_max:.2f}_{profile}.grib2"
    with _grib_workspace(root, "gfs_grids") as cache_root:
        ga, rt_dt_a_used, fh_a_used = _ensure_grib_with_cycle_fallback(
            cache_root=cache_root,
            file_suffix=bbox_suffix,
            runtime_dt=rt_dt_a,
            fh=fh_a,
            build_url=lambda rt, fhx: _nomads_grid_url(
                rt,
                fhx,
                lat_min=lat_min,
                lat_max=lat_max,
                lon_min=lon_min,
                lon_max=lon_max,
                field_profile=profile,
            ),
            max_back_cycles=3,
        )
        gp, rt_dt_p_used, fh_p_used = _ensure_grib_with_cycle_fallback(
            cache_root=cache_root,
            file_suffix=bbox_suffix,
            runtime_dt=rt_dt_p,
            fh=fh_p,
            build_url=lambda rt, fhx: _nomads_grid_url(
                rt,
                fhx,
                lat_min=lat_min,
                lat_max=lat_max,
                lon_min=lon_min,
                lon_max=lon_max,
                field_profile=profile,
            ),
            max_back_cycles=3,
        )

        py = _repo_root(root) / ".venv_gfs" / "bin" / "python"
        if not py.exists():
            raise RuntimeError("gfs parser venv missing (.venv_gfs)")
        if profile == "outer500":
            code = r'''
import json, sys, xarray as xr
import numpy as np
pa, pp = sys.argv[1], sys.argv[2]
da = xr.open_dataset(pa, engine='cfgrib')
dp = xr.open_dataset(pp, engine='cfgrib')

def arr2(v):
    a = np.asarray(v)
    a = np.squeeze(a)
    while a.ndim > 2:
        a = a[0]
    if a.ndim != 2:
        raise RuntimeError(f'expected 2D field, got shape={a.shape}')
    return a

def level_slice(var, level):
    if 'isobaricInhPa' in var.dims:
        return arr2(var.sel(isobaricInhPa=level, method='nearest').values)
    return arr2(var.values)

z_src = da['gh'] if 'gh' in da else da['z']
z5 = level_slice(z_src, 500.0)
if np.nanmean(z5) > 20000:
    z5 = z5 / 9.80665
z_src_prev = dp['gh'] if 'gh' in dp else dp['z']
z5_prev = level_slice(z_src_prev, 500.0)
if np.nanmean(z5_prev) > 20000:
    z5_prev = z5_prev / 9.80665
u8 = level_slice(da['u'], 850.0)

lat = np.asarray(da['latitude'].values).tolist()
lon = np.asarray(da['longitude'].values).tolist()
out = {
  'lat': lat,
  'lon': lon,
  'fields': {
    'z500_gpm': z5.tolist(),
    'u850_ms': u8.tolist(),
  },
  'previous_fields': {
    'z500_gpm': z5_prev.tolist(),
  }
}
print(json.dumps(out, ensure_ascii=False))
'''
        else:
            code = r'''
import json, sys, xarray as xr
import numpy as np
pa, pp = sys.argv[1], sys.argv[2]
da = xr.open_dataset(pa, engine='cfgrib')
dp = xr.open_dataset(pp, engine='cfgrib')

def arr2(v):
    a = np.asarray(v)
    a = np.squeeze(a)
    while a.ndim > 2:
        a = a[0]
    if a.ndim != 2:
        raise RuntimeError(f'expected 2D field, got shape={a.shape}')
    return a

def level_slice(var, level):
    # Explicitly select isobaric levels to avoid structural mis-pick (e.g. taking 850 for 500 fields).
    if 'isobaricInhPa' in var.dims:
        return arr2(var.sel(isobaricInhPa=level, method='nearest').values)
    return arr2(var.values)

pr = arr2(da['prmsl'].values)
if np.nanmean(pr) > 2000: pr = pr / 100.0
prp = arr2(dp['prmsl'].values)
if np.nanmean(prp) > 2000: prp = prp / 100.0

# 500hPa height (gh/z) must come from 500 level explicitly
z_src = da['gh'] if 'gh' in da else da['z']
z5 = level_slice(z_src, 500.0)
if np.nanmean(z5) > 20000:
    z5 = z5 / 9.80665
z_src_prev = dp['gh'] if 'gh' in dp else dp['z']
z5_prev = level_slice(z_src_prev, 500.0)
if np.nanmean(z5_prev) > 20000:
    z5_prev = z5_prev / 9.80665

# 850/700/925hPa fields explicitly by level
t8 = level_slice(da['t'], 850.0)
if np.nanmean(t8) > 170: t8 = t8 - 273.15
u8 = level_slice(da['u'], 850.0)
v8 = level_slice(da['v'], 850.0)

t7 = level_slice(da['t'], 700.0)
if np.nanmean(t7) > 170: t7 = t7 - 273.15
u7 = level_slice(da['u'], 700.0)
v7 = level_slice(da['v'], 700.0)

t925 = level_slice(da['t'], 925.0)
if np.nanmean(t925) > 170: t925 = t925 - 273.15
u925 = level_slice(da['u'], 925.0)
v925 = level_slice(da['v'], 925.0)

lat = np.asarray(da['latitude'].values).tolist()
lon = np.asarray(da['longitude'].values).tolist()
out = {
  'lat': lat,
  'lon': lon,
  'fields': {
    'mslp_hpa': pr.tolist(),
    'z500_gpm': z5.tolist(),
    't850_c': t8.tolist(),
    'u850_ms': u8.tolist(),
    'v850_ms': v8.tolist(),
    't700_c': t7.tolist(),
    'u700_ms': u7.tolist(),
    'v700_ms': v7.tolist(),
    't925_c': t925.tolist(),
    'u925_ms': u925.tolist(),
    'v925_ms': v925.tolist(),
  },
  'previous_fields': {
    'mslp_hpa': prp.tolist(),
    'z500_gpm': z5_prev.tolist(),
  }
}
print(json.dumps(out, ensure_ascii=False))
'''
        proc = subprocess.run([str(py), "-c", code, str(ga), str(gp)], capture_output=True, text=True, timeout=120)
        if proc.returncode != 0:
            raise RuntimeError(f"gfs grid parse failed: {proc.stderr.strip() or proc.stdout.strip()}")
        parsed = json.loads(proc.stdout)

    return {
        "analysis_time_utc": a_utc.strftime("%Y-%m-%dT%H:%M:00Z"),
        "station": {"icao": station_icao, "lat": station_lat, "lon": station_lon},
        "lat": parsed["lat"],
        "lon": parsed["lon"],
        "fields": parsed["fields"],
        "previous_fields": parsed["previous_fields"],
        "grid_meta": {
            "step_deg": 0.25,
            "bbox": {"lat_min": lat_min, "lat_max": lat_max, "lon_min": lon_min, "lon_max": lon_max},
            "provider": "gfs-grib2",
            "field_profile": profile,
            "analysis_runtime_used": rt_dt_a_used.strftime("%Y%m%d%HZ"),
            "analysis_fh_used": int(fh_a_used),
            "previous_runtime_used": rt_dt_p_used.strftime("%Y%m%d%HZ"),
            "previous_fh_used": int(fh_p_used),
        },
    }


def fetch_hourly_like(st: Any, target_date: str, cycle_tag: str, root: Path) -> dict[str, Any]:
    runtime_dt = runtime_dt_from_tag(cycle_tag)
    d0 = datetime.strptime(target_date, "%Y-%m-%d").replace(tzinfo=timezone.utc)
    wanted = [d0 + timedelta(hours=h) for h in range(0, 24, 3)]

    out_rows: list[dict[str, Any]] = []
    with _grib_workspace(root, "gfs_grib") as cache_root:
        for vt in wanted:
            rt_tag_used, fh = resolve_runtime_for_valid_time(vt, cycle_tag, cycle_hours=6, max_back_cycles=3)
            if fh < 0:
                continue
            rt_dt_used = runtime_dt_from_tag(rt_tag_used)
            grib, rt_used, fh_used = _ensure_grib_with_cycle_fallback(
                cache_root=cache_root,
                file_suffix=".grib2",
                runtime_dt=rt_dt_used,
                fh=fh,
                build_url=lambda rt, fhx: _nomads_url(rt, fhx, float(st.lat), float(st.lon)),
                max_back_cycles=3,
            )
            row = _extract_point_from_grib(grib, float(st.lat), float(st.lon), root)
            if row.get("time"):
                out_rows.append(row)

    if not out_rows:
        raise RuntimeError("gfs-grib2 fetch returned no usable rows")

    out_rows = sorted(out_rows, key=lambda x: x["time"])
    return {
        "timezone": "UTC",
        "hourly": {
            "time": [r["time"] for r in out_rows],
            "temperature_2m": [r["temperature_2m"] for r in out_rows],
            "temperature_850hPa": [r["temperature_850hPa"] for r in out_rows],
            "wind_speed_850hPa": [r["wind_speed_850hPa"] for r in out_rows],
            "wind_direction_850hPa": [r["wind_direction_850hPa"] for r in out_rows],
            "cloud_cover_low": [r["cloud_cover_low"] for r in out_rows],
            "pressure_msl": [r["pressure_msl"] for r in out_rows],
        },
    }
