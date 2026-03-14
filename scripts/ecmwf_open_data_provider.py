from __future__ import annotations

import json
import os
import subprocess
import time
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from runtime_utils import runtime_dt_from_tag, runtime_tag_from_dt
from venv_utils import repo_venv_python


def _repo_root(root: Path) -> Path:
    candidates = [
        root / "skills" / "polymarket-weatherbot",
        root,
        Path(__file__).resolve().parent.parent,
    ]
    for candidate in candidates:
        if (candidate / ".venv_nwp").exists() or (candidate / ".venv_gfs").exists():
            return candidate
    return Path(__file__).resolve().parent.parent


@contextmanager
def _ecmwf_workspace(root: Path, subdir: str):
    path = _repo_root(root) / "cache" / "runtime" / subdir
    path.mkdir(parents=True, exist_ok=True)
    yield path


def _cooldown_store_path(root: Path) -> Path:
    path = _repo_root(root) / "cache" / "runtime" / "ecmwf_open_data_cooldowns.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    return path


def _read_cooldown_store(root: Path) -> dict[str, Any]:
    path = _cooldown_store_path(root)
    if not path.exists():
        return {}
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
        return data if isinstance(data, dict) else {}
    except Exception:
        return {}


def _write_cooldown_store(root: Path, payload: dict[str, Any]) -> None:
    path = _cooldown_store_path(root)
    tmp = path.with_suffix(".tmp")
    tmp.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")
    tmp.replace(path)


def _prune_cooldowns(payload: dict[str, Any]) -> dict[str, Any]:
    now = datetime.now(timezone.utc)
    entries = dict(payload.get("entries") or {})
    kept: dict[str, Any] = {}
    for key, raw in entries.items():
        if not isinstance(raw, dict):
            continue
        try:
            expires_at = datetime.fromisoformat(str(raw.get("expires_at") or "").replace("Z", "+00:00"))
        except Exception:
            continue
        if expires_at > now:
            kept[str(key)] = raw
    return {"entries": kept}


def _request_cooldown_key(*, source: str, model: str, request: dict[str, Any]) -> str:
    stream = str(request.get("stream") or "")
    req_type = str(request.get("type") or "")
    levtype = str(request.get("levtype") or "")
    return f"{source}:{model}:{stream}:{req_type}:{levtype}"


def _active_fetch_cooldown(*, root: Path, source: str, model: str, request: dict[str, Any]) -> tuple[bool, str]:
    store = _prune_cooldowns(_read_cooldown_store(root))
    entry = dict((store.get("entries") or {}).get(_request_cooldown_key(source=source, model=model, request=request)) or {})
    if not entry:
        return False, ""
    expires_at = str(entry.get("expires_at") or "").strip()
    reason = str(entry.get("reason") or "").strip()
    if not expires_at:
        return False, ""
    return True, f"ecmwf open data cooldown active until {expires_at}: {reason or 'cooldown'}"


def _record_fetch_cooldown(
    *,
    root: Path,
    source: str,
    model: str,
    request: dict[str, Any],
    seconds: int,
    reason: str,
) -> None:
    if seconds <= 0:
        return
    store = _prune_cooldowns(_read_cooldown_store(root))
    entries = dict(store.get("entries") or {})
    now = datetime.now(timezone.utc)
    entries[_request_cooldown_key(source=source, model=model, request=request)] = {
        "created_at": now.isoformat().replace("+00:00", "Z"),
        "expires_at": (now + timedelta(seconds=int(seconds))).isoformat().replace("+00:00", "Z"),
        "reason": str(reason or "").strip(),
        "request": {
            "stream": str(request.get("stream") or ""),
            "type": str(request.get("type") or ""),
            "levtype": str(request.get("levtype") or ""),
        },
    }
    _write_cooldown_store(root, {"entries": entries})


def _clear_fetch_cooldown(*, root: Path, source: str, model: str, request: dict[str, Any]) -> None:
    store = _prune_cooldowns(_read_cooldown_store(root))
    entries = dict(store.get("entries") or {})
    key = _request_cooldown_key(source=source, model=model, request=request)
    if key not in entries:
        return
    entries.pop(key, None)
    _write_cooldown_store(root, {"entries": entries})


def _cooldown_seconds_for_error(*, err_text: str, request: dict[str, Any]) -> int:
    text = str(err_text or "").lower()
    stream = str(request.get("stream") or "").strip().lower()
    req_type = str(request.get("type") or "").strip().lower()
    if "429" in text or "too many requests" in text or "rate_limit" in text or "rate limited" in text:
        return int(os.getenv("ECMWF_OPEN_DATA_RATE_LIMIT_COOLDOWN_SECONDS", "900") or "900")
    if "timeout" in text or "timed out" in text:
        default = "240" if stream == "enfo" and req_type in {"pf", "cf"} else "120"
        return int(os.getenv("ECMWF_OPEN_DATA_TIMEOUT_COOLDOWN_SECONDS", default) or default)
    if any(token in text for token in ("connection", "ssl", "dns")):
        return int(os.getenv("ECMWF_OPEN_DATA_TRANSIENT_COOLDOWN_SECONDS", "180") or "180")
    return 0


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
) -> tuple[dict[str, Any], dict[str, Any] | None]:
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
        surface_request = None
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
    py = repo_venv_python(_repo_root(root))
    if not py.exists():
        raise RuntimeError("ecmwf-opendata runtime missing (.venv_nwp/bin/python)")
    cooldown_active, cooldown_reason = _active_fetch_cooldown(
        root=root,
        source=source,
        model=model,
        request=request,
    )
    if cooldown_active:
        raise RuntimeError(cooldown_reason)
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
    retries = max(1, int(os.getenv("ECMWF_OPEN_DATA_RETRIES", "2") or "2"))
    timeout_s = int(os.getenv("ECMWF_OPEN_DATA_TIMEOUT_SECONDS", "180") or "180")
    tmp_target = target.with_suffix(target.suffix + ".tmp")
    last_err: Exception | None = None
    for attempt in range(1, retries + 1):
        try:
            if tmp_target.exists():
                tmp_target.unlink()
        except Exception:
            pass
        try:
            proc = subprocess.run(
                [str(py), "-c", code, str(tmp_target), str(source), str(model), json.dumps(request)],
                capture_output=True,
                text=True,
                timeout=timeout_s,
            )
            if proc.returncode != 0:
                err_text = proc.stderr.strip() or proc.stdout.strip()
                raise RuntimeError(f"ecmwf open data fetch failed: {err_text}")
            tmp_target.replace(target)
            _clear_fetch_cooldown(
                root=root,
                source=source,
                model=model,
                request=request,
            )
            return
        except Exception as exc:
            last_err = exc
            err_text = str(exc).lower()
            try:
                if tmp_target.exists():
                    tmp_target.unlink()
            except Exception:
                pass
            is_rate_limit = "429" in err_text or "too many requests" in err_text or "rate_limit" in err_text
            is_transient = (
                "timeout" in err_text
                or "timed out" in err_text
                or "connection" in err_text
                or "ssl" in err_text
                or "dns" in err_text
            )
            cooldown_seconds = _cooldown_seconds_for_error(err_text=err_text, request=request)
            if cooldown_seconds > 0:
                _record_fetch_cooldown(
                    root=root,
                    source=source,
                    model=model,
                    request=request,
                    seconds=cooldown_seconds,
                    reason=str(exc),
                )
            if is_rate_limit:
                raise RuntimeError(f"ecmwf open data rate limited: {exc}") from exc
            if (not is_transient) or attempt >= retries:
                raise RuntimeError(str(exc)) from exc
            time.sleep(min(2.0, 0.5 * attempt))
    raise RuntimeError(str(last_err or "ecmwf open data fetch failed"))


def _fetch_pair(
    *,
    workspace: Path,
    runtime_tag: str,
    fh: int,
    stream: str,
    field_profile: str,
    source: str,
    model_name: str,
) -> tuple[Path, Path | None]:
    profile = str(field_profile or "full").strip().lower()
    if profile == "outer500":
        full_pressure_target = workspace / f"ecmwf_{runtime_tag}_{stream}_f{fh:03d}_full_pl.grib2"
        if full_pressure_target.exists():
            return full_pressure_target, None

    pressure_target = workspace / f"ecmwf_{runtime_tag}_{stream}_f{fh:03d}_{profile}_pl.grib2"
    surface_target = None if profile == "outer500" else workspace / f"ecmwf_{runtime_tag}_{stream}_f{fh:03d}_{profile}_sfc.grib2"
    if pressure_target.exists() and (surface_target is None or surface_target.exists()):
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
    if surface_target is not None and surface_request is not None and not surface_target.exists():
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
) -> tuple[Path, Path | None, str, int, str]:
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
            msg = str(exc).lower()
            if "429" in msg or "too many requests" in msg or "rate limited" in msg or "cooldown active" in msg:
                raise RuntimeError(str(exc)) from exc
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

        py = repo_venv_python(_repo_root(root))
        if not py.exists():
            raise RuntimeError("grib parser venv missing (.venv_nwp)")

        if profile == "outer500":
            code = r'''
import json, sys, xarray as xr
import numpy as np
pa, pp = sys.argv[1], sys.argv[2]
lat_min, lat_max, lon_min, lon_max = map(float, sys.argv[3:7])

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

        argv = [str(py), "-c", code]
        if profile == "outer500":
            argv.extend(
                [
                    str(pressure_a),
                    str(pressure_p),
                    str(lat_min),
                    str(lat_max),
                    str(lon_min),
                    str(lon_max),
                ]
            )
        else:
            argv.extend(
                [
                    str(pressure_a),
                    str(surface_a),
                    str(pressure_p),
                    str(surface_p),
                    str(lat_min),
                    str(lat_max),
                    str(lon_min),
                    str(lon_max),
                ]
            )
        proc = subprocess.run(
            argv,
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
