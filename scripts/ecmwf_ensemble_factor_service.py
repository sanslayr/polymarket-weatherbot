from __future__ import annotations

import hashlib
import json
import os
import subprocess
from datetime import datetime, timedelta, timezone
from math import ceil
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

from cache_envelope import extract_payload, make_cache_doc
from ecmwf_open_data_provider import _ecmwf_step_valid, _ecmwf_workspace, _repo_root, _retrieve_grib
from metar_utils import metar_obs_time_utc
from runtime_cache_policy import runtime_cache_enabled
from runtime_utils import runtime_dt_from_tag, runtime_tag_from_dt
from venv_utils import repo_venv_python


ROOT = Path(__file__).resolve().parent.parent
CACHE_DIR = ROOT / "cache" / "runtime"
SCHEMA_VERSION = "ecmwf-ensemble-factor.v7"


def _safe_float(value: Any) -> float | None:
    try:
        if value in (None, ""):
            return None
        return float(value)
    except Exception:
        return None


def ensemble_factor_has_surface_member_detail(payload: dict[str, Any] | None) -> bool:
    data = dict(payload or {})
    for raw in (data.get("members") or []):
        if not isinstance(raw, dict):
            continue
        if any(raw.get(key) not in (None, "") for key in ("t2m_c", "wind_speed_10m_kmh", "msl_hpa")):
            return True
    trajectory = dict(data.get("member_trajectory") or {})
    for raw in (trajectory.get("members") or []):
        if not isinstance(raw, dict):
            continue
        if any(raw.get(key) not in (None, "") for key in ("t2m_current_c", "next3h_t2m_delta_c", "msl_current_hpa")):
            return True
    return False


def ensemble_factor_detail_level(payload: dict[str, Any] | None) -> str:
    data = dict(payload or {})
    source = dict(data.get("source") or {})
    explicit = str(source.get("detail_level") or "").strip().lower()
    if explicit:
        return explicit
    has_surface = ensemble_factor_has_surface_member_detail(data)
    trajectory = dict(data.get("member_trajectory") or {})
    has_surface_trajectory = False
    for raw in (trajectory.get("members") or []):
        if not isinstance(raw, dict):
            continue
        if any(raw.get(key) not in (None, "") for key in ("t2m_current_c", "next3h_t2m_delta_c", "msl_current_hpa")):
            has_surface_trajectory = True
            break
    if has_surface and has_surface_trajectory:
        return "surface_trajectory"
    if has_surface:
        return "surface_anchor"
    if data.get("members") or data.get("summary"):
        return "legacy_pl_only"
    return ""


def _cache_key(*parts: str) -> str:
    raw = "|".join(parts)
    return hashlib.sha1(raw.encode("utf-8")).hexdigest()[:16]


def _cache_path(*parts: str) -> Path:
    return CACHE_DIR / f"ecmwf_ensemble_factor_{_cache_key(*parts)}.json"


def _read_cache(*parts: str, ttl_hours: int = 6) -> dict[str, Any] | None:
    if not runtime_cache_enabled():
        return None
    path = _cache_path(*parts)
    if not path.exists():
        return None
    try:
        doc = json.loads(path.read_text(encoding="utf-8"))
        payload, updated_at, _env = extract_payload(doc)
        if not isinstance(payload, dict):
            return None
        if str(payload.get("schema_version") or "") != SCHEMA_VERSION:
            return None
        if updated_at:
            ts = datetime.fromisoformat(str(updated_at).replace("Z", "+00:00"))
            if datetime.now(timezone.utc) - ts > timedelta(hours=ttl_hours):
                return None
        return payload
    except Exception:
        return None


def _write_cache(payload: dict[str, Any], *parts: str) -> None:
    if not runtime_cache_enabled():
        return
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    path = _cache_path(*parts)
    doc = make_cache_doc(
        payload,
        source_state="fresh",
        payload_schema_version=str(payload.get("schema_version")) if isinstance(payload, dict) else None,
        meta={"kind": "ecmwf_ensemble_factor"},
    )
    path.write_text(json.dumps(doc, ensure_ascii=False), encoding="utf-8")


def _normalize_percent(value: float) -> float:
    return round(max(0.0, min(1.0, float(value))), 3)


def _quantile(values: list[float], q: float) -> float | None:
    if not values:
        return None
    data = sorted(float(item) for item in values)
    if len(data) == 1:
        return round(data[0], 2)
    qv = max(0.0, min(1.0, float(q)))
    pos = qv * (len(data) - 1)
    lo = int(pos)
    hi = min(lo + 1, len(data) - 1)
    frac = pos - lo
    out = data[lo] * (1.0 - frac) + data[hi] * frac
    return round(out, 2)


def _path_split_state(
    dominant_prob: float,
    *,
    signal_dispersion_c: float | None = None,
    dominant_margin_prob: float | None = None,
) -> str:
    if dominant_prob >= 0.62 and (dominant_margin_prob is None or dominant_margin_prob >= 0.14):
        state = "clustered"
    elif dominant_prob >= 0.47 and (dominant_margin_prob is None or dominant_margin_prob >= 0.06):
        state = "mixed"
    else:
        state = "split"

    dispersion = _safe_float(signal_dispersion_c)
    if dispersion is not None:
        if dispersion >= 2.5:
            if state == "clustered":
                state = "mixed"
            elif state == "mixed":
                state = "split"
        elif dispersion >= 1.8 and state == "clustered":
            state = "mixed"
    return state


def _parse_local_dt(value: str | None, tz_name: str) -> datetime | None:
    text = str(value or "").strip()
    if not text:
        return None
    try:
        dt = datetime.fromisoformat(text)
    except Exception:
        return None
    try:
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=ZoneInfo(tz_name))
    except Exception:
        pass
    return dt


def _resolve_ens_runtime_for_valid_time(
    valid_utc: datetime,
    preferred_runtime_tag: str,
    *,
    max_back_cycles: int = 6,
) -> tuple[str, int]:
    rt = runtime_dt_from_tag(preferred_runtime_tag)
    if rt.hour not in {0, 12}:
        rt = rt.replace(hour=12 if rt.hour >= 12 else 0, minute=0, second=0, microsecond=0)
        if rt > valid_utc:
            rt = rt - timedelta(hours=12)
    for _ in range(max_back_cycles + 1):
        raw_fh = (valid_utc - rt).total_seconds() / 3600.0
        fh = int(round(raw_fh / 3.0) * 3)
        if fh >= 0 and _ecmwf_step_valid(rt.hour, fh):
            return runtime_tag_from_dt(rt), fh
        rt = rt - timedelta(hours=12)
    fh = int(round(((valid_utc - rt).total_seconds() / 3600.0) / 3.0) * 3)
    return runtime_tag_from_dt(rt), fh


def _ensemble_surface_request(runtime_dt: datetime, step: int, *, type_name: str) -> dict[str, Any]:
    return {
        "date": runtime_dt.strftime("%Y-%m-%d"),
        "time": int(runtime_dt.strftime("%H")),
        "stream": "enfo",
        "type": str(type_name),
        "step": int(step),
        "levtype": "sfc",
        "param": ["2t", "2d", "10u", "10v", "msl"],
    }


def _fetch_ensemble_surface_files(
    *,
    workspace: Path,
    valid_utc: datetime,
    preferred_runtime_tag: str,
    source: str,
    model_name: str,
    root: Path,
) -> tuple[Path, Path, str, int]:
    runtime_tag, fh = _resolve_ens_runtime_for_valid_time(valid_utc, preferred_runtime_tag)
    pf_sfc_target = workspace / f"ecmwf_ens_{runtime_tag}_enfo_pf_f{fh:03d}_sfc_multi.grib2"
    cf_sfc_target = workspace / f"ecmwf_ens_{runtime_tag}_enfo_cf_f{fh:03d}_sfc_multi.grib2"
    if pf_sfc_target.exists() and cf_sfc_target.exists():
        return pf_sfc_target, cf_sfc_target, runtime_tag, fh

    runtime_dt = runtime_dt_from_tag(runtime_tag)
    targets = (
        (pf_sfc_target, _ensemble_surface_request(runtime_dt, fh, type_name="pf")),
        (cf_sfc_target, _ensemble_surface_request(runtime_dt, fh, type_name="cf")),
    )
    for target, request in targets:
        if target.exists():
            continue
        _retrieve_grib(
            target=target,
            request=request,
            source=source,
            model=model_name,
            root=root,
        )
    return pf_sfc_target, cf_sfc_target, runtime_tag, fh


def _extract_point_surface_members(
    pf_sfc_path: Path,
    cf_sfc_path: Path,
    lat: float,
    lon: float,
    root: Path,
) -> dict[str, Any]:
    py = repo_venv_python(_repo_root(root))
    if not py.exists():
        raise RuntimeError("ecmwf ensemble parser venv missing (.venv_nwp)")

    code = r'''
import json, math, sys
import xarray as xr

pf_sfc_path, cf_sfc_path, lat, lon = sys.argv[1], sys.argv[2], float(sys.argv[3]), float(sys.argv[4])

def norm_lon(value):
    out = float(value)
    return ((out + 180.0) % 360.0) - 180.0

lon = norm_lon(lon)

def select_point(ds):
    return ds.sel(latitude=lat, longitude=lon, method="nearest")

def open_component(path, data_type, filter_keys):
    merged = {"dataType": data_type}
    merged.update(filter_keys)
    return xr.open_dataset(path, engine="cfgrib", backend_kwargs={"filter_by_keys": merged})

def val(arr):
    raw = float(arr.values)
    return raw

def temp_c(raw):
    value = val(raw)
    if value > 170.0:
        value -= 273.15
    return round(value, 2)

def rh_pct(temp_c_value, dewpoint_c_value):
    try:
        es = 6.112 * math.exp((17.67 * temp_c_value) / (temp_c_value + 243.5))
        ed = 6.112 * math.exp((17.67 * dewpoint_c_value) / (dewpoint_c_value + 243.5))
        return round(max(0.0, min(100.0, (ed / es) * 100.0)), 1)
    except Exception:
        return None

def wind_kmh(u_raw, v_raw):
    u = val(u_raw)
    v = val(v_raw)
    wspd = math.sqrt(u * u + v * v) * 3.6
    wdir = (math.degrees(math.atan2(-u, -v)) + 360.0) % 360.0
    return round(u, 2), round(v, 2), round(wspd, 2), round(wdir, 1)

def gh_gpm(raw):
    return round(val(raw) / 9.80665, 1)

def msl_hpa(raw):
    value = val(raw)
    if value > 2000.0:
        value /= 100.0
    return round(value, 1)

def member_payload(number, point_2m, point_10m, point_msl):
    u10, v10, wspd10, wdir10 = wind_kmh(point_10m["u10"], point_10m["v10"])
    t2m = temp_c(point_2m["t2m"])
    td2m = temp_c(point_2m["d2m"])
    return {
        "number": int(number),
        "t2m_c": t2m,
        "td2m_c": td2m,
        "rh2m_pct": rh_pct(t2m, td2m),
        "u10m_ms": u10,
        "v10m_ms": v10,
        "wind_speed_10m_kmh": wspd10,
        "wind_direction_10m_deg": wdir10,
        "msl_hpa": msl_hpa(point_msl["msl"]),
    }

pf_2m = select_point(open_component(pf_sfc_path, "pf", {"typeOfLevel": "heightAboveGround", "level": 2}))
pf_10m = select_point(open_component(pf_sfc_path, "pf", {"typeOfLevel": "heightAboveGround", "level": 10}))
pf_msl = select_point(open_component(pf_sfc_path, "pf", {"typeOfLevel": "meanSea"}))
cf_2m = select_point(open_component(cf_sfc_path, "cf", {"typeOfLevel": "heightAboveGround", "level": 2}))
cf_10m = select_point(open_component(cf_sfc_path, "cf", {"typeOfLevel": "heightAboveGround", "level": 10}))
cf_msl = select_point(open_component(cf_sfc_path, "cf", {"typeOfLevel": "meanSea"}))

members = []
for idx in pf_2m["number"].values.tolist():
    point_2m = pf_2m.sel(number=int(idx))
    point_10m = pf_10m.sel(number=int(idx))
    point_msl = pf_msl.sel(number=int(idx))
    members.append(member_payload(idx, point_2m, point_10m, point_msl))

cf_num = 0
try:
    cf_num = int(cf_2m["number"].values)
except Exception:
    cf_num = 0
members.insert(0, member_payload(cf_num, cf_2m, cf_10m, cf_msl))

out = {
    "selected_lat": round(float(pf_2m["latitude"].values), 3),
    "selected_lon": round(norm_lon(float(pf_2m["longitude"].values)), 3),
    "valid_time": str(pf_2m["valid_time"].values)[:19].replace(" ", "T"),
    "members": members,
}
print(json.dumps(out, ensure_ascii=False))
'''
    proc = subprocess.run(
        [
            str(py),
            "-c",
            code,
            str(pf_sfc_path),
            str(cf_sfc_path),
            str(lat),
            str(lon),
        ],
        capture_output=True,
        text=True,
        timeout=int(os.getenv("ECMWF_ENS_PARSE_TIMEOUT_SECONDS", "120") or "120"),
    )
    if proc.returncode != 0:
        raise RuntimeError(f"ecmwf ensemble parse failed: {proc.stderr.strip() or proc.stdout.strip()}")
    return json.loads(proc.stdout.strip())


def _extract_point_surface_members_multi(
    pf_sfc_path: Path,
    cf_sfc_path: Path,
    stations: list[dict[str, Any]],
    root: Path,
) -> dict[str, dict[str, Any]]:
    valid_stations = []
    for raw in stations:
        station_id = str((raw or {}).get("id") or "").strip()
        if not station_id:
            continue
        try:
            lat = float((raw or {}).get("lat"))
            lon = float((raw or {}).get("lon"))
        except Exception:
            continue
        valid_stations.append({"id": station_id, "lat": lat, "lon": lon})
    if not valid_stations:
        return {}

    py = repo_venv_python(_repo_root(root))
    if not py.exists():
        raise RuntimeError("ecmwf ensemble parser venv missing (.venv_nwp)")

    code = r'''
import json, math, sys
import xarray as xr

pf_sfc_path, cf_sfc_path, stations_json = sys.argv[1], sys.argv[2], sys.argv[3]
stations = json.loads(stations_json)

def norm_lon(value):
    out = float(value)
    return ((out + 180.0) % 360.0) - 180.0

def val(arr):
    return float(arr.values)

def open_component(path, data_type, filter_keys):
    merged = {"dataType": data_type}
    merged.update(filter_keys)
    return xr.open_dataset(path, engine="cfgrib", backend_kwargs={"filter_by_keys": merged})

def temp_c(raw):
    value = val(raw)
    if value > 170.0:
        value -= 273.15
    return round(value, 2)

def rh_pct(temp_c_value, dewpoint_c_value):
    try:
        es = 6.112 * math.exp((17.67 * temp_c_value) / (temp_c_value + 243.5))
        ed = 6.112 * math.exp((17.67 * dewpoint_c_value) / (dewpoint_c_value + 243.5))
        return round(max(0.0, min(100.0, (ed / es) * 100.0)), 1)
    except Exception:
        return None

def wind_kmh(u_raw, v_raw):
    u = val(u_raw)
    v = val(v_raw)
    wspd = math.sqrt(u * u + v * v) * 3.6
    wdir = (math.degrees(math.atan2(-u, -v)) + 360.0) % 360.0
    return round(u, 2), round(v, 2), round(wspd, 2), round(wdir, 1)

def msl_hpa(raw):
    value = val(raw)
    if value > 2000.0:
        value /= 100.0
    return round(value, 1)

def member_payload(number, point_2m, point_10m, point_msl):
    u10, v10, wspd10, wdir10 = wind_kmh(point_10m["u10"], point_10m["v10"])
    t2m = temp_c(point_2m["t2m"])
    td2m = temp_c(point_2m["d2m"])
    return {
        "number": int(number),
        "t2m_c": t2m,
        "td2m_c": td2m,
        "rh2m_pct": rh_pct(t2m, td2m),
        "u10m_ms": u10,
        "v10m_ms": v10,
        "wind_speed_10m_kmh": wspd10,
        "wind_direction_10m_deg": wdir10,
        "msl_hpa": msl_hpa(point_msl["msl"]),
    }

point_ids = [str(item["id"]) for item in stations]
latitudes = xr.DataArray([float(item["lat"]) for item in stations], dims="point")
longitudes = xr.DataArray([norm_lon(float(item["lon"])) for item in stations], dims="point")

pf_2m_ds = open_component(pf_sfc_path, "pf", {"typeOfLevel": "heightAboveGround", "level": 2})
pf_10m_ds = open_component(pf_sfc_path, "pf", {"typeOfLevel": "heightAboveGround", "level": 10})
pf_msl_ds = open_component(pf_sfc_path, "pf", {"typeOfLevel": "meanSea"})
cf_2m_ds = open_component(cf_sfc_path, "cf", {"typeOfLevel": "heightAboveGround", "level": 2})
cf_10m_ds = open_component(cf_sfc_path, "cf", {"typeOfLevel": "heightAboveGround", "level": 10})
cf_msl_ds = open_component(cf_sfc_path, "cf", {"typeOfLevel": "meanSea"})

pf_2m_sel = pf_2m_ds.sel(latitude=latitudes, longitude=longitudes, method="nearest")
pf_10m_sel = pf_10m_ds.sel(latitude=latitudes, longitude=longitudes, method="nearest")
pf_msl_sel = pf_msl_ds.sel(latitude=latitudes, longitude=longitudes, method="nearest")
cf_2m_sel = cf_2m_ds.sel(latitude=latitudes, longitude=longitudes, method="nearest")
cf_10m_sel = cf_10m_ds.sel(latitude=latitudes, longitude=longitudes, method="nearest")
cf_msl_sel = cf_msl_ds.sel(latitude=latitudes, longitude=longitudes, method="nearest")

cf_num = 0
try:
    cf_num = int(cf_2m_sel["number"].values)
except Exception:
    cf_num = 0

out = {}
for idx, point_id in enumerate(point_ids):
    point_pf_2m = pf_2m_sel.isel(point=idx)
    point_pf_10m = pf_10m_sel.isel(point=idx)
    point_pf_msl = pf_msl_sel.isel(point=idx)
    point_cf_2m = cf_2m_sel.isel(point=idx)
    point_cf_10m = cf_10m_sel.isel(point=idx)
    point_cf_msl = cf_msl_sel.isel(point=idx)
    members = []
    for member_idx in point_pf_2m["number"].values.tolist():
        point_2m = point_pf_2m.sel(number=int(member_idx))
        point_10m = point_pf_10m.sel(number=int(member_idx))
        point_msl = point_pf_msl.sel(number=int(member_idx))
        members.append(member_payload(member_idx, point_2m, point_10m, point_msl))
    members.insert(0, member_payload(cf_num, point_cf_2m, point_cf_10m, point_cf_msl))
    out[point_id] = {
        "selected_lat": round(float(point_pf_2m["latitude"].values), 3),
        "selected_lon": round(norm_lon(float(point_pf_2m["longitude"].values)), 3),
        "valid_time": str(point_pf_2m["valid_time"].values)[:19].replace(" ", "T"),
        "members": members,
    }
print(json.dumps(out, ensure_ascii=False))
'''
    proc = subprocess.run(
        [
            str(py),
            "-c",
            code,
            str(pf_sfc_path),
            str(cf_sfc_path),
            json.dumps(valid_stations, ensure_ascii=False),
        ],
        capture_output=True,
        text=True,
        timeout=int(os.getenv("ECMWF_ENS_PARSE_TIMEOUT_SECONDS", "120") or "120"),
    )
    if proc.returncode != 0:
        raise RuntimeError(f"ecmwf ensemble batch parse failed: {proc.stderr.strip() or proc.stdout.strip()}")
    payload = json.loads(proc.stdout.strip() or "{}")
    return payload if isinstance(payload, dict) else {}


def _member_map(payload: dict[str, Any]) -> dict[int, dict[str, Any]]:
    out: dict[int, dict[str, Any]] = {}
    for raw in payload.get("members") or []:
        try:
            out[int(raw.get("number"))] = dict(raw)
        except Exception:
            continue
    return out


def _classify_member_trajectory(
    *,
    prior_surface_delta_c: float | None,
    next_surface_delta_c: float | None,
    prior_delta_c: float | None,
    next_delta_c: float | None,
    accel_c: float | None,
) -> str:
    if _safe_float(next_surface_delta_c) is not None:
        prior = _safe_float(prior_surface_delta_c) or 0.0
        nxt = _safe_float(next_surface_delta_c) or 0.0
    else:
        prior = _safe_float(prior_delta_c) or 0.0
        nxt = _safe_float(next_delta_c) or 0.0
    accel = _safe_float(accel_c) or 0.0
    if nxt >= 0.55 and accel >= -0.12:
        return "warming_follow_through"
    if nxt >= 0.25 and accel >= -0.20:
        return "warming_but_slowing"
    if abs(nxt) <= 0.12 and prior >= 0.20:
        return "plateau_after_warming"
    if abs(nxt) <= 0.12 and abs(prior) <= 0.12:
        return "flat_hold"
    if nxt <= -0.25 and prior >= 0.15:
        return "warm_reversal"
    if nxt <= -0.35:
        return "cooling_follow_through"
    if accel >= 0.24 and nxt > 0.10:
        return "reaccelerating"
    return "mixed_transition"


def _build_member_trajectory(
    *,
    previous_payload: dict[str, Any],
    current_payload: dict[str, Any],
    next_payload: dict[str, Any],
    previous_local: datetime,
    current_local: datetime,
    next_local: datetime,
) -> dict[str, Any]:
    previous_members = _member_map(previous_payload)
    current_members = _member_map(current_payload)
    next_members = _member_map(next_payload)
    shared_numbers = sorted(set(previous_members).intersection(current_members).intersection(next_members))
    if not shared_numbers:
        return {}

    rows: list[dict[str, Any]] = []
    next_deltas: list[float] = []
    prior_deltas: list[float] = []
    accel_values: list[float] = []
    next_surface_deltas: list[float] = []
    prior_surface_deltas: list[float] = []
    shape_counts: dict[str, int] = {}
    for number in shared_numbers:
        prev_row = previous_members[number]
        cur_row = current_members[number]
        next_row = next_members[number]
        prev_t850 = _safe_float(prev_row.get("t850_c"))
        cur_t850 = _safe_float(cur_row.get("t850_c"))
        next_t850 = _safe_float(next_row.get("t850_c"))
        prev_wind = _safe_float(prev_row.get("wind_speed_850_kmh"))
        cur_wind = _safe_float(cur_row.get("wind_speed_850_kmh"))
        next_wind = _safe_float(next_row.get("wind_speed_850_kmh"))
        prev_t2m = _safe_float(prev_row.get("t2m_c"))
        cur_t2m = _safe_float(cur_row.get("t2m_c"))
        next_t2m = _safe_float(next_row.get("t2m_c"))
        prev_td2m = _safe_float(prev_row.get("td2m_c"))
        cur_td2m = _safe_float(cur_row.get("td2m_c"))
        next_td2m = _safe_float(next_row.get("td2m_c"))
        prev_wind10 = _safe_float(prev_row.get("wind_speed_10m_kmh"))
        cur_wind10 = _safe_float(cur_row.get("wind_speed_10m_kmh"))
        next_wind10 = _safe_float(next_row.get("wind_speed_10m_kmh"))
        prev_msl = _safe_float(prev_row.get("msl_hpa"))
        cur_msl = _safe_float(cur_row.get("msl_hpa"))
        next_msl = _safe_float(next_row.get("msl_hpa"))
        prior_delta = None if prev_t850 is None or cur_t850 is None else round(cur_t850 - prev_t850, 2)
        next_delta = None if cur_t850 is None or next_t850 is None else round(next_t850 - cur_t850, 2)
        accel = None if prior_delta is None or next_delta is None else round(next_delta - prior_delta, 2)
        wind_next_delta = None if cur_wind is None or next_wind is None else round(next_wind - cur_wind, 2)
        prior_surface_delta = None if prev_t2m is None or cur_t2m is None else round(cur_t2m - prev_t2m, 2)
        next_surface_delta = None if cur_t2m is None or next_t2m is None else round(next_t2m - cur_t2m, 2)
        next_td2m_delta = None if cur_td2m is None or next_td2m is None else round(next_td2m - cur_td2m, 2)
        next_wind10_delta = None if cur_wind10 is None or next_wind10 is None else round(next_wind10 - cur_wind10, 2)
        next_msl_delta = None if cur_msl is None or next_msl is None else round(next_msl - cur_msl, 2)
        signal_next_delta = next_surface_delta if next_surface_delta is not None else next_delta
        future_room = None if signal_next_delta is None else round(max(0.0, signal_next_delta), 2)
        future_cooling = None if signal_next_delta is None else round(max(0.0, -signal_next_delta), 2)
        trajectory_shape = _classify_member_trajectory(
            prior_surface_delta_c=prior_surface_delta,
            next_surface_delta_c=next_surface_delta,
            prior_delta_c=prior_delta,
            next_delta_c=next_delta,
            accel_c=accel,
        )
        shape_counts[trajectory_shape] = shape_counts.get(trajectory_shape, 0) + 1
        if prior_delta is not None:
            prior_deltas.append(prior_delta)
        if next_delta is not None:
            next_deltas.append(next_delta)
        if prior_surface_delta is not None:
            prior_surface_deltas.append(prior_surface_delta)
        if next_surface_delta is not None:
            next_surface_deltas.append(next_surface_delta)
        if accel is not None:
            accel_values.append(accel)
        rows.append(
            {
                "number": number,
                "t850_prev_c": prev_t850,
                "t850_current_c": cur_t850,
                "t850_next_c": next_t850,
                "wind_prev_850_kmh": prev_wind,
                "wind_current_850_kmh": cur_wind,
                "wind_next_850_kmh": next_wind,
                "t2m_prev_c": prev_t2m,
                "t2m_current_c": cur_t2m,
                "t2m_next_c": next_t2m,
                "td2m_prev_c": prev_td2m,
                "td2m_current_c": cur_td2m,
                "td2m_next_c": next_td2m,
                "wind_10m_prev_kmh": prev_wind10,
                "wind_10m_current_kmh": cur_wind10,
                "wind_10m_next_kmh": next_wind10,
                "msl_prev_hpa": prev_msl,
                "msl_current_hpa": cur_msl,
                "msl_next_hpa": next_msl,
                "prior3h_t850_delta_c": prior_delta,
                "next3h_t850_delta_c": next_delta,
                "prior3h_t2m_delta_c": prior_surface_delta,
                "next3h_t2m_delta_c": next_surface_delta,
                "next3h_td2m_delta_c": next_td2m_delta,
                "trajectory_accel_c": accel,
                "next3h_wind_delta_kmh": wind_next_delta,
                "next3h_wind10_delta_kmh": next_wind10_delta,
                "next3h_msl_delta_hpa": next_msl_delta,
                "future_room_c": future_room,
                "future_cooling_c": future_cooling,
                "trajectory_shape": trajectory_shape,
            }
        )

    total = float(len(rows))
    dominant_shape = max(shape_counts.items(), key=lambda item: item[1])[0] if shape_counts else ""
    return {
        "anchor_local": current_local.strftime("%Y-%m-%dT%H:%M"),
        "previous_local": previous_local.strftime("%Y-%m-%dT%H:%M"),
        "next_local": next_local.strftime("%Y-%m-%dT%H:%M"),
        "dominant_shape": dominant_shape,
        "shape_probabilities": {
            key: _normalize_percent(value / total)
            for key, value in shape_counts.items()
            if total > 0.0
        },
        "diagnostics": {
            "prior3h_t850_p10_c": _quantile(prior_deltas, 0.10),
            "prior3h_t850_p50_c": _quantile(prior_deltas, 0.50),
            "prior3h_t850_p90_c": _quantile(prior_deltas, 0.90),
            "next3h_t850_p10_c": _quantile(next_deltas, 0.10),
            "next3h_t850_p50_c": _quantile(next_deltas, 0.50),
            "next3h_t850_p90_c": _quantile(next_deltas, 0.90),
            "prior3h_t2m_p10_c": _quantile(prior_surface_deltas, 0.10),
            "prior3h_t2m_p50_c": _quantile(prior_surface_deltas, 0.50),
            "prior3h_t2m_p90_c": _quantile(prior_surface_deltas, 0.90),
            "next3h_t2m_p10_c": _quantile(next_surface_deltas, 0.10),
            "next3h_t2m_p50_c": _quantile(next_surface_deltas, 0.50),
            "next3h_t2m_p90_c": _quantile(next_surface_deltas, 0.90),
            "trajectory_accel_p50_c": _quantile(accel_values, 0.50),
        },
        "members": rows,
    }


def _local_day_surface_times(target_local: datetime) -> list[datetime]:
    tz = target_local.tzinfo
    day_start = target_local.replace(hour=0, minute=0, second=0, microsecond=0)
    return [day_start + timedelta(hours=3 * idx) for idx in range(8)]


def _history_surface_local_times(
    anchor_local: datetime,
    runtime_tag: str,
    *,
    max_history_hours: float | None = None,
) -> list[datetime]:
    try:
        anchor_utc = anchor_local.astimezone(timezone.utc)
    except Exception:
        return [anchor_local]
    runtime_utc = runtime_dt_from_tag(runtime_tag).astimezone(timezone.utc)
    if max_history_hours is not None:
        history_limit_h = float(max_history_hours)
    else:
        raw_limit = str(os.getenv("ECMWF_ENS_HISTORY_MAX_HOURS") or "").strip()
        history_limit_h = _safe_float(raw_limit) if raw_limit else None
    if history_limit_h is not None and history_limit_h > 0.0:
        earliest_utc = max(runtime_utc, anchor_utc - timedelta(hours=float(history_limit_h)))
    else:
        earliest_utc = runtime_utc
    offset_h = max(0.0, (earliest_utc - runtime_utc).total_seconds() / 3600.0)
    step_index = int(ceil(offset_h / 3.0))
    current_utc = runtime_utc + timedelta(hours=3 * step_index)
    out: list[datetime] = []
    while current_utc <= anchor_utc + timedelta(minutes=1):
        fh = int(round((current_utc - runtime_utc).total_seconds() / 3600.0))
        if fh >= 0 and _ecmwf_step_valid(runtime_utc.hour, fh):
            out.append(current_utc.astimezone(anchor_local.tzinfo or timezone.utc))
        current_utc += timedelta(hours=3)
    if not out:
        out.append(anchor_local)
    return out


def _summarize_surface_snapshots(snapshots: list[tuple[datetime, dict[str, Any]]]) -> dict[str, Any]:
    items: list[dict[str, Any]] = []
    for local_dt, payload in snapshots:
        rows = [dict(raw) for raw in (payload.get("members") or []) if isinstance(raw, dict)]
        if not rows:
            continue
        t2m = [_safe_float(raw.get("t2m_c")) for raw in rows if _safe_float(raw.get("t2m_c")) is not None]
        td2m = [_safe_float(raw.get("td2m_c")) for raw in rows if _safe_float(raw.get("td2m_c")) is not None]
        rh2m = [_safe_float(raw.get("rh2m_pct")) for raw in rows if _safe_float(raw.get("rh2m_pct")) is not None]
        wind10 = [_safe_float(raw.get("wind_speed_10m_kmh")) for raw in rows if _safe_float(raw.get("wind_speed_10m_kmh")) is not None]
        msl = [_safe_float(raw.get("msl_hpa")) for raw in rows if _safe_float(raw.get("msl_hpa")) is not None]
        items.append(
            {
                "time_local": local_dt.strftime("%Y-%m-%dT%H:%M"),
                "t2m_p10_c": _quantile([float(v) for v in t2m], 0.10) if t2m else None,
                "t2m_p50_c": _quantile([float(v) for v in t2m], 0.50) if t2m else None,
                "t2m_p90_c": _quantile([float(v) for v in t2m], 0.90) if t2m else None,
                "td2m_p50_c": _quantile([float(v) for v in td2m], 0.50) if td2m else None,
                "rh2m_p50_pct": _quantile([float(v) for v in rh2m], 0.50) if rh2m else None,
                "wind10_p50_kmh": _quantile([float(v) for v in wind10], 0.50) if wind10 else None,
                "msl_p50_hpa": _quantile([float(v) for v in msl], 0.50) if msl else None,
            }
        )
    return {"times": items}


def _build_observed_surface_history(
    *,
    metar24: list[dict[str, Any]] | None,
    valid_times_local: list[datetime],
    tz_name: str,
) -> dict[str, Any]:
    rows = [dict(raw) for raw in (metar24 or []) if isinstance(raw, dict)]
    if not rows or not valid_times_local:
        return {"times": [], "matched_count": 0}
    tolerance_min = _safe_float(os.getenv("ECMWF_ENS_HISTORY_MATCH_TOLERANCE_MIN", "100")) or 100.0
    tz = ZoneInfo(tz_name)
    parsed_rows: list[tuple[datetime, dict[str, Any]]] = []
    for raw in rows:
        try:
            report_utc = metar_obs_time_utc(raw)
        except Exception:
            continue
        parsed_rows.append((report_utc, raw))
    parsed_rows.sort(key=lambda item: item[0])
    items: list[dict[str, Any]] = []
    for local_dt in valid_times_local:
        target_utc = local_dt.astimezone(timezone.utc)
        best: tuple[float, datetime, dict[str, Any]] | None = None
        for report_utc, raw in parsed_rows:
            delta_min = abs((report_utc - target_utc).total_seconds()) / 60.0
            if delta_min > tolerance_min:
                continue
            if best is None or delta_min < best[0]:
                best = (delta_min, report_utc, raw)
        if best is None:
            continue
        _delta_min, report_utc, raw = best
        try:
            temp_c = float(raw.get("temp")) if raw.get("temp") not in (None, "") else None
        except Exception:
            temp_c = None
        try:
            dewpoint_c = float(raw.get("dewp")) if raw.get("dewp") not in (None, "") else None
        except Exception:
            dewpoint_c = None
        try:
            wind_speed_10m_kmh = float(raw.get("wspd")) * 1.852 if raw.get("wspd") not in (None, "") else None
        except Exception:
            wind_speed_10m_kmh = None
        try:
            wind_direction_10m_deg = float(raw.get("wdir")) if raw.get("wdir") not in (None, "", "VRB") else None
        except Exception:
            wind_direction_10m_deg = None
        try:
            msl_hpa = float(raw.get("altim")) if raw.get("altim") not in (None, "") else None
        except Exception:
            msl_hpa = None
        items.append(
            {
                "time_local": local_dt.strftime("%Y-%m-%dT%H:%M"),
                "report_local": report_utc.astimezone(tz).strftime("%Y-%m-%dT%H:%M"),
                "report_offset_min": round(float(best[0]), 1),
                "temp_c": round(temp_c, 2) if temp_c is not None else None,
                "dewpoint_c": round(dewpoint_c, 2) if dewpoint_c is not None else None,
                "wind_speed_10m_kmh": round(wind_speed_10m_kmh, 2) if wind_speed_10m_kmh is not None else None,
                "wind_direction_10m_deg": round(wind_direction_10m_deg, 1) if wind_direction_10m_deg is not None else None,
                "msl_hpa": round(msl_hpa, 1) if msl_hpa is not None else None,
            }
        )
    return {"times": items, "matched_count": len(items)}


def _build_member_surface_history_alignment(
    *,
    history_snapshots: list[tuple[datetime, dict[str, Any]]],
    observed_surface_history: dict[str, Any],
) -> dict[str, Any]:
    observed_by_time = {
        str(item.get("time_local") or ""): dict(item)
        for item in (observed_surface_history.get("times") or [])
        if isinstance(item, dict) and str(item.get("time_local") or "").strip()
    }
    if not observed_by_time:
        return {"matched_time_count": 0, "members": [], "diagnostics": {}}

    member_stats: dict[int, dict[str, Any]] = {}
    matched_time_keys: list[str] = []
    for local_dt, payload in history_snapshots:
        time_key = local_dt.strftime("%Y-%m-%dT%H:%M")
        observed = observed_by_time.get(time_key)
        if not observed:
            continue
        matched_time_keys.append(time_key)
        for raw in (payload.get("members") or []):
            if not isinstance(raw, dict):
                continue
            try:
                number = int(raw.get("number"))
            except Exception:
                continue
            entry = member_stats.setdefault(
                number,
                {
                    "number": number,
                    "temp_errors": [],
                    "wind_errors": [],
                    "pressure_errors": [],
                    "slot_scores": [],
                    "temp_pairs": [],
                    "match_count": 0,
                },
            )
            temp_c = _safe_float(raw.get("t2m_c"))
            wind_kmh = _safe_float(raw.get("wind_speed_10m_kmh"))
            msl_hpa = _safe_float(raw.get("msl_hpa"))
            obs_temp_c = _safe_float(observed.get("temp_c"))
            obs_wind_kmh = _safe_float(observed.get("wind_speed_10m_kmh"))
            obs_msl_hpa = _safe_float(observed.get("msl_hpa"))
            slot_terms: list[float] = []
            slot_weights: list[float] = []
            if temp_c is not None and obs_temp_c is not None:
                err = abs(float(temp_c) - float(obs_temp_c))
                entry["temp_errors"].append(err)
                entry["temp_pairs"].append((time_key, float(temp_c), float(obs_temp_c)))
                slot_terms.append(max(0.0, min(1.0, 1.0 - err / 3.5)))
                slot_weights.append(0.62)
            if wind_kmh is not None and obs_wind_kmh is not None:
                err = abs(float(wind_kmh) - float(obs_wind_kmh))
                entry["wind_errors"].append(err)
                slot_terms.append(max(0.0, min(1.0, 1.0 - err / 20.0)))
                slot_weights.append(0.20)
            if msl_hpa is not None and obs_msl_hpa is not None:
                err = abs(float(msl_hpa) - float(obs_msl_hpa))
                entry["pressure_errors"].append(err)
                slot_terms.append(max(0.0, min(1.0, 1.0 - err / 4.0)))
                slot_weights.append(0.18)
            if slot_terms and slot_weights and sum(slot_weights) > 0.0:
                entry["slot_scores"].append(sum(term * weight for term, weight in zip(slot_terms, slot_weights)) / sum(slot_weights))
                entry["match_count"] += 1

    out_rows: list[dict[str, Any]] = []
    alignment_scores: list[float] = []
    temp_maes: list[float] = []
    for number in sorted(member_stats):
        entry = member_stats[number]
        match_count = int(entry.get("match_count") or 0)
        if match_count <= 0:
            continue
        temp_pairs = list(entry.get("temp_pairs") or [])
        temp_mae = _quantile([float(v) for v in entry.get("temp_errors") or []], 0.50)
        wind_mae = _quantile([float(v) for v in entry.get("wind_errors") or []], 0.50)
        pressure_mae = _quantile([float(v) for v in entry.get("pressure_errors") or []], 0.50)
        trend_bias_c = None
        trend_score = None
        if len(temp_pairs) >= 2:
            member_delta = float(temp_pairs[-1][1]) - float(temp_pairs[0][1])
            observed_delta = float(temp_pairs[-1][2]) - float(temp_pairs[0][2])
            trend_bias_c = round(member_delta - observed_delta, 2)
            trend_score = max(0.0, min(1.0, 1.0 - abs(trend_bias_c) / 2.2))
        mean_slot_score = sum(float(v) for v in entry.get("slot_scores") or []) / float(len(entry.get("slot_scores") or []))
        alignment_score = mean_slot_score if trend_score is None else (0.74 * mean_slot_score + 0.26 * trend_score)
        row = {
            "number": number,
            "history_match_count": match_count,
            "history_temp_mae_c": temp_mae,
            "history_wind_mae_kmh": wind_mae,
            "history_msl_mae_hpa": pressure_mae,
            "history_trend_bias_c": trend_bias_c,
            "history_alignment_score": round(alignment_score, 3),
        }
        out_rows.append(row)
        alignment_scores.append(float(row["history_alignment_score"]))
        if temp_mae is not None:
            temp_maes.append(float(temp_mae))
    return {
        "matched_time_count": len(set(matched_time_keys)),
        "matched_times_local": sorted(set(matched_time_keys)),
        "members": out_rows,
        "diagnostics": {
            "history_alignment_p10": _quantile(alignment_scores, 0.10),
            "history_alignment_p50": _quantile(alignment_scores, 0.50),
            "history_alignment_p90": _quantile(alignment_scores, 0.90),
            "history_temp_mae_p50_c": _quantile(temp_maes, 0.50),
        },
    }


def _transition_detail_label(
    *,
    t850_delta_c: float | None,
    wind_speed_850_kmh: float | None,
) -> str:
    delta = t850_delta_c if t850_delta_c is not None else 0.0
    speed = wind_speed_850_kmh if wind_speed_850_kmh is not None else 0.0
    neutral_abs_delta = _safe_float(os.getenv("ECMWF_ENS_NEUTRAL_ABS_T850_DELTA_C", "0.35")) or 0.35
    weak_delta = _safe_float(os.getenv("ECMWF_ENS_WEAK_T850_DELTA_C", "0.15")) or 0.15
    neutral_speed_max = _safe_float(os.getenv("ECMWF_ENS_NEUTRAL_WIND_850_KMH", "22.0")) or 22.0

    if abs(delta) <= neutral_abs_delta and speed <= neutral_speed_max:
        return "neutral_stable"
    if delta >= weak_delta:
        return "weak_warm_transition"
    if delta <= -weak_delta:
        return "weak_cold_transition"
    return "neutral_stable"


def _classify_member_path(
    *,
    t850_delta_c: float | None,
    t925_delta_c: float | None,
    t2m_delta_c: float | None,
    wind_speed_850_kmh: float | None,
    wind_speed_925_kmh: float | None,
    msl_delta_hpa: float | None,
) -> tuple[str, str]:
    warm_threshold = _safe_float(os.getenv("ECMWF_ENS_WARM_T850_DELTA_C", "0.7")) or 0.7
    cold_threshold = _safe_float(os.getenv("ECMWF_ENS_COLD_T850_DELTA_C", "-0.7")) or -0.7
    speed = wind_speed_850_kmh if wind_speed_850_kmh is not None else 0.0
    speed925 = wind_speed_925_kmh if wind_speed_925_kmh is not None else 0.0
    delta = t850_delta_c if t850_delta_c is not None else 0.0
    delta925 = t925_delta_c if t925_delta_c is not None else delta
    delta2m = t2m_delta_c if t2m_delta_c is not None else delta925
    has_surface_signal = t2m_delta_c is not None
    if has_surface_signal:
        proxy_delta = delta2m * 0.55 + delta925 * 0.30 + delta * 0.15
    else:
        proxy_delta = delta * 0.55 + delta925 * 0.45
    pressure_fall = msl_delta_hpa is not None and msl_delta_hpa <= -1.0
    pressure_rise = msl_delta_hpa is not None and msl_delta_hpa >= 1.0

    if (proxy_delta >= warm_threshold or (delta2m >= 0.20 and delta925 >= 0.15)) and max(speed, speed925) >= 10.0:
        return "warm_support", "warm_support"
    if proxy_delta <= cold_threshold or (delta2m <= -0.20 and delta925 <= -0.12):
        return "cold_suppression", "cold_suppression"
    if pressure_fall and delta925 >= 0.15 and delta2m >= 0.10 and max(speed, speed925) >= 14.0:
        return "warm_support", "warm_support"
    if not has_surface_signal and delta >= max(0.3, warm_threshold * 0.5) and max(speed, speed925) >= 20.0:
        return "warm_support", "warm_support"
    if pressure_rise and delta2m <= -0.10:
        return "transition", "weak_cold_transition"
    if proxy_delta <= min(-0.3, cold_threshold * 0.5):
        return "transition", _transition_detail_label(
            t850_delta_c=proxy_delta,
            wind_speed_850_kmh=max(speed, speed925),
        )
    return "transition", _transition_detail_label(
        t850_delta_c=proxy_delta,
        wind_speed_850_kmh=max(speed, speed925),
    )


def summarize_member_path_rows(rows: list[dict[str, Any]]) -> dict[str, Any]:
    valid_rows: list[dict[str, Any]] = []
    counts = {
        "warm_support": 0,
        "transition": 0,
        "cold_suppression": 0,
    }
    detail_counts = {
        "warm_support": 0,
        "neutral_stable": 0,
        "weak_warm_transition": 0,
        "weak_cold_transition": 0,
        "cold_suppression": 0,
    }

    for raw in rows:
        label = str(raw.get("path_label") or "").strip()
        detail = str(raw.get("path_detail") or "").strip()
        if label not in counts:
            continue
        counts[label] += 1
        if detail in detail_counts:
            detail_counts[detail] += 1
        try:
            number = int(raw.get("number"))
        except Exception:
            number = len(valid_rows)
        valid_rows.append(
            {
                "number": number,
                "path_label": label,
                "path_detail": detail,
                "delta_t850_c": _safe_float(raw.get("delta_t850_c")),
                "delta_t925_c": _safe_float(raw.get("delta_t925_c")),
                "delta_t2m_c": _safe_float(raw.get("delta_t2m_c")),
                "wind_speed_850_kmh": _safe_float(raw.get("wind_speed_850_kmh")),
                "wind_speed_925_kmh": _safe_float(raw.get("wind_speed_925_kmh")),
                "msl_delta_hpa": _safe_float(raw.get("msl_delta_hpa")),
            }
        )

    if not valid_rows:
        raise RuntimeError("member path rows are empty")

    total = float(len(valid_rows))
    probabilities = {
        key: _normalize_percent(value / total)
        for key, value in counts.items()
    }
    delta_values = [float(row["delta_t2m_c"]) for row in valid_rows if row.get("delta_t2m_c") is not None]
    delta925_values = [float(row["delta_t925_c"]) for row in valid_rows if row.get("delta_t925_c") is not None]
    delta2m_values = [float(row["delta_t2m_c"]) for row in valid_rows if row.get("delta_t2m_c") is not None]
    speed_values = [float(row["wind_speed_10m_kmh"]) for row in valid_rows if row.get("wind_speed_10m_kmh") is not None]
    dominant_path, dominant_prob = max(probabilities.items(), key=lambda item: item[1])
    sorted_probs = sorted(probabilities.items(), key=lambda item: item[1], reverse=True)
    dominant_margin_prob = round(sorted_probs[0][1] - sorted_probs[1][1], 3) if len(sorted_probs) >= 2 else round(sorted_probs[0][1], 3)
    signal_dispersion_c = round((max(delta_values) - min(delta_values)), 2) if len(delta_values) >= 2 else 0.0
    detail_probabilities = {
        key: _normalize_percent(value / total)
        for key, value in detail_counts.items()
    }
    transition_detail_keys = (
        "neutral_stable",
        "weak_warm_transition",
        "weak_cold_transition",
    )
    transition_detail_counts = {key: detail_counts[key] for key in transition_detail_keys}
    transition_detail = max(transition_detail_counts.items(), key=lambda item: item[1])[0]
    dominant_path_detail = dominant_path if dominant_path != "transition" else transition_detail

    return {
        "schema_version": SCHEMA_VERSION,
        "member_count": int(total),
        "summary": {
            "dominant_path": dominant_path,
            "dominant_path_detail": dominant_path_detail,
            "dominant_detail_prob": detail_probabilities.get(dominant_path_detail),
            "dominant_prob": dominant_prob,
            "transition_detail": transition_detail,
            "transition_detail_prob": detail_probabilities.get(transition_detail),
            "dominant_margin_prob": dominant_margin_prob,
            "split_state": _path_split_state(
                dominant_prob,
                signal_dispersion_c=signal_dispersion_c,
                dominant_margin_prob=dominant_margin_prob,
            ),
            "signal_dispersion_c": signal_dispersion_c,
        },
        "probabilities": probabilities,
        "detail_probabilities": detail_probabilities,
        "diagnostics": {
            "delta_t2m_p10_c": _quantile(delta_values, 0.10),
            "delta_t2m_p50_c": _quantile(delta_values, 0.50),
            "delta_t2m_p90_c": _quantile(delta_values, 0.90),
            "delta_t925_p50_c": _quantile(delta925_values, 0.50),
            "wind10_p50_kmh": _quantile(speed_values, 0.50),
            "neutral_stable_prob": detail_probabilities.get("neutral_stable"),
            "weak_warm_transition_prob": detail_probabilities.get("weak_warm_transition"),
            "weak_cold_transition_prob": detail_probabilities.get("weak_cold_transition"),
        },
        "members": valid_rows,
    }


def summarize_member_paths(
    current_payload: dict[str, Any],
    previous_payload: dict[str, Any],
) -> dict[str, Any]:
    current_members = _member_map(current_payload)
    previous_members = _member_map(previous_payload)
    shared_numbers = sorted(set(current_members).intersection(previous_members))
    if not shared_numbers:
        raise RuntimeError("ecmwf ensemble point payload has no overlapping members")

    rows: list[dict[str, Any]] = []
    for number in shared_numbers:
        current = current_members[number]
        previous = previous_members[number]
        t850_now = _safe_float(current.get("t850_c"))
        t850_prev = _safe_float(previous.get("t850_c"))
        t925_now = _safe_float(current.get("t925_c"))
        t925_prev = _safe_float(previous.get("t925_c"))
        t2m_now = _safe_float(current.get("t2m_c"))
        t2m_prev = _safe_float(previous.get("t2m_c"))
        msl_now = _safe_float(current.get("msl_hpa"))
        msl_prev = _safe_float(previous.get("msl_hpa"))
        wind_speed_10m_kmh = _safe_float(current.get("wind_speed_10m_kmh"))
        delta_t850_c = None if t850_now is None or t850_prev is None else round(t850_now - t850_prev, 2)
        delta_t925_c = None if t925_now is None or t925_prev is None else round(t925_now - t925_prev, 2)
        delta_t2m_c = None if t2m_now is None or t2m_prev is None else round(t2m_now - t2m_prev, 2)
        msl_delta_hpa = None if msl_now is None or msl_prev is None else round(msl_now - msl_prev, 2)
        wind_speed_850_kmh = _safe_float(current.get("wind_speed_850_kmh"))
        wind_speed_925_kmh = _safe_float(current.get("wind_speed_925_kmh"))
        label, detail = _classify_member_path(
            t850_delta_c=delta_t850_c,
            t925_delta_c=delta_t925_c,
            t2m_delta_c=delta_t2m_c,
            wind_speed_850_kmh=wind_speed_850_kmh,
            wind_speed_925_kmh=wind_speed_925_kmh,
            msl_delta_hpa=msl_delta_hpa,
        )
        rows.append(
            {
                "number": number,
                "path_label": label,
                "path_detail": detail,
                "delta_t850_c": delta_t850_c,
                "delta_t925_c": delta_t925_c,
                "delta_t2m_c": delta_t2m_c,
                "wind_speed_850_kmh": wind_speed_850_kmh,
                "wind_speed_925_kmh": wind_speed_925_kmh,
                "wind_speed_10m_kmh": wind_speed_10m_kmh,
                "msl_delta_hpa": msl_delta_hpa,
            }
        )

    return summarize_member_path_rows(rows)


def build_ecmwf_ensemble_factor(
    *,
    station_icao: str,
    station_lat: float,
    station_lon: float,
    peak_local: str,
    analysis_local: str | None = None,
    tz_name: str,
    preferred_runtime_tag: str,
    metar24: list[dict[str, Any]] | None = None,
    detail_stage: str = "auto",
    root: Path = ROOT,
) -> dict[str, Any]:
    normalized_stage = str(detail_stage or "auto").strip().lower()
    if normalized_stage not in {"auto", "anchor", "trajectory"}:
        normalized_stage = "auto"
    analysis_local_key = str(analysis_local or "").strip()
    cache_hit = _read_cache(station_icao, peak_local, analysis_local_key, preferred_runtime_tag, normalized_stage)
    if cache_hit:
        return cache_hit

    peak_dt_local = datetime.strptime(peak_local, "%Y-%m-%dT%H:%M").replace(tzinfo=ZoneInfo(tz_name))
    anchor_local = _parse_local_dt(analysis_local, tz_name) or peak_dt_local
    previous_local = anchor_local - timedelta(hours=3)
    next_local = anchor_local + timedelta(hours=3)
    anchor_utc = anchor_local.astimezone(timezone.utc)
    previous_utc = previous_local.astimezone(timezone.utc)
    next_utc = next_local.astimezone(timezone.utc)

    source = str(os.getenv("ECMWF_OPEN_DATA_SOURCE", "azure") or "azure").strip().lower()
    model_name = str(os.getenv("ECMWF_OPEN_DATA_MODEL", "ifs") or "ifs").strip().lower()

    with _ecmwf_workspace(root, "ecmwf_open_data") as workspace:
        current_pf_sfc, current_cf_sfc, current_runtime, current_fh = _fetch_ensemble_surface_files(
            workspace=workspace,
            valid_utc=anchor_utc,
            preferred_runtime_tag=preferred_runtime_tag,
            source=source,
            model_name=model_name,
            root=root,
        )
        previous_pf_sfc, previous_cf_sfc, previous_runtime, previous_fh = _fetch_ensemble_surface_files(
            workspace=workspace,
            valid_utc=previous_utc,
            preferred_runtime_tag=preferred_runtime_tag,
            source=source,
            model_name=model_name,
            root=root,
        )
        trajectory_files: tuple[Path, Path, str, int] | None = None
        if normalized_stage in {"auto", "trajectory"}:
            try:
                trajectory_files = _fetch_ensemble_surface_files(
                    workspace=workspace,
                    valid_utc=next_utc,
                    preferred_runtime_tag=preferred_runtime_tag,
                    source=source,
                    model_name=model_name,
                    root=root,
                )
            except Exception:
                trajectory_files = None

    current_payload = _extract_point_surface_members(
        current_pf_sfc,
        current_cf_sfc,
        station_lat,
        station_lon,
        root,
    )
    previous_payload = _extract_point_surface_members(
        previous_pf_sfc,
        previous_cf_sfc,
        station_lat,
        station_lon,
        root,
    )
    summary = summarize_member_paths(current_payload, previous_payload)
    member_trajectory: dict[str, Any] = {}
    trajectory_runtime = ""
    trajectory_fh: int | None = None
    if trajectory_files is not None:
        trajectory_pf_sfc, trajectory_cf_sfc, trajectory_runtime, trajectory_fh = trajectory_files
        trajectory_next_payload = _extract_point_surface_members(
            trajectory_pf_sfc,
            trajectory_cf_sfc,
            station_lat,
            station_lon,
            root,
        )
        member_trajectory = _build_member_trajectory(
            previous_payload=previous_payload,
            current_payload=current_payload,
            next_payload=trajectory_next_payload,
            previous_local=previous_local,
            current_local=anchor_local,
            next_local=next_local,
        )
    detail_level = "surface_trajectory" if bool(member_trajectory.get("members")) else "surface_anchor"
    history_times_local = _history_surface_local_times(anchor_local, current_runtime)
    history_snapshots: list[tuple[datetime, dict[str, Any]]] = []
    for local_dt in history_times_local:
        if local_dt in {previous_local, anchor_local, next_local}:
            payload_ref = previous_payload if local_dt == previous_local else (current_payload if local_dt == anchor_local else None)
            if payload_ref is not None:
                history_snapshots.append((local_dt, payload_ref))
                continue
        try:
            with _ecmwf_workspace(root, "ecmwf_open_data") as workspace:
                day_pf_sfc, day_cf_sfc, _day_runtime, _day_fh = _fetch_ensemble_surface_files(
                    workspace=workspace,
                    valid_utc=local_dt.astimezone(timezone.utc),
                    preferred_runtime_tag=preferred_runtime_tag,
                    source=source,
                    model_name=model_name,
                    root=root,
                )
            day_payload = _extract_point_surface_members(
                day_pf_sfc,
                day_cf_sfc,
                station_lat,
                station_lon,
                root,
            )
            history_snapshots.append((local_dt, day_payload))
        except Exception:
            continue
    if not any(local_dt == previous_local for local_dt, _payload in history_snapshots):
        history_snapshots.append((previous_local, previous_payload))
    if not any(local_dt == anchor_local for local_dt, _payload in history_snapshots):
        history_snapshots.append((anchor_local, current_payload))
    day_keys = {item.strftime("%Y-%m-%dT%H:%M") for item in _local_day_surface_times(anchor_local)}
    day_snapshots = [
        (local_dt, payload)
        for local_dt, payload in history_snapshots
        if local_dt.strftime("%Y-%m-%dT%H:%M") in day_keys
    ]
    daily_surface_timeline = _summarize_surface_snapshots(sorted(day_snapshots, key=lambda item: item[0]))
    history_surface_timeline = _summarize_surface_snapshots(sorted(history_snapshots, key=lambda item: item[0]))
    observed_surface_history = _build_observed_surface_history(
        metar24=metar24,
        valid_times_local=sorted(history_times_local),
        tz_name=tz_name,
    )
    member_history_alignment = _build_member_surface_history_alignment(
        history_snapshots=sorted(history_snapshots, key=lambda item: item[0]),
        observed_surface_history=observed_surface_history,
    )
    payload = {
        **summary,
        "member_trajectory": member_trajectory,
        "daily_surface_timeline": daily_surface_timeline,
        "history_surface_timeline": history_surface_timeline,
        "observed_surface_history": observed_surface_history,
        "member_history_alignment": member_history_alignment,
        "source": {
            "provider": "ecmwf-ens-open-data",
            "runtime_requested": preferred_runtime_tag,
            "runtime_used": current_runtime,
            "previous_runtime_used": previous_runtime,
            "detail_level": detail_level,
            "capabilities": {
                "surface_members": True,
                "trajectory_members": bool(member_trajectory.get("members")),
                "vertical_profile_members": False,
            },
            "analysis_fh": current_fh,
            "previous_fh": previous_fh,
            "peak_local": peak_local,
            "analysis_anchor_local": anchor_local.strftime("%Y-%m-%dT%H:%M"),
            "previous_local": previous_local.strftime("%Y-%m-%dT%H:%M"),
            "trajectory_next_local": next_local.strftime("%Y-%m-%dT%H:%M"),
            "day_times_local": [item["time_local"] for item in daily_surface_timeline.get("times", [])],
            "history_times_local": [item["time_local"] for item in history_surface_timeline.get("times", [])],
            "history_matched_obs_count": int(observed_surface_history.get("matched_count") or 0),
            "trajectory_runtime_used": trajectory_runtime,
            "trajectory_fh": trajectory_fh,
            "tz_name": tz_name,
        },
        "selection": {
            "station": station_icao,
            "lat": round(float(station_lat), 4),
            "lon": round(float(station_lon), 4),
            "grid_lat": _safe_float(current_payload.get("selected_lat")),
            "grid_lon": _safe_float(current_payload.get("selected_lon")),
        },
    }
    _write_cache(payload, station_icao, peak_local, analysis_local_key, preferred_runtime_tag, normalized_stage)
    return payload


def build_ecmwf_ensemble_factor_batch(
    *,
    requests: list[dict[str, Any]],
    detail_stage: str = "auto",
    root: Path = ROOT,
) -> dict[str, dict[str, Any]]:
    normalized_stage = str(detail_stage or "auto").strip().lower()
    if normalized_stage not in {"auto", "anchor", "trajectory"}:
        normalized_stage = "auto"

    results: dict[str, dict[str, Any]] = {}
    pending_specs: list[dict[str, Any]] = []
    for index, raw in enumerate(requests):
        req = dict(raw or {})
        request_id = str(req.get("request_id") or f"request-{index}").strip()
        station_icao = str(req.get("station_icao") or "").strip().upper()
        peak_local = str(req.get("peak_local") or "").strip()
        analysis_local_key = str(req.get("analysis_local") or "").strip()
        tz_name = str(req.get("tz_name") or "").strip()
        preferred_runtime_tag = str(req.get("preferred_runtime_tag") or "").strip()
        metar24 = list(req.get("metar24") or []) if isinstance(req.get("metar24"), list) else []
        if not request_id or not station_icao or not peak_local or not tz_name or not preferred_runtime_tag:
            continue
        try:
            station_lat = float(req.get("station_lat"))
            station_lon = float(req.get("station_lon"))
        except Exception:
            continue

        cache_hit = _read_cache(station_icao, peak_local, analysis_local_key, preferred_runtime_tag, normalized_stage)
        if cache_hit:
            results[request_id] = cache_hit
            continue

        peak_dt_local = datetime.strptime(peak_local, "%Y-%m-%dT%H:%M").replace(tzinfo=ZoneInfo(tz_name))
        anchor_local = _parse_local_dt(analysis_local_key, tz_name) or peak_dt_local
        previous_local = anchor_local - timedelta(hours=3)
        next_local = anchor_local + timedelta(hours=3)
        anchor_runtime_tag, _anchor_fh = _resolve_ens_runtime_for_valid_time(anchor_local.astimezone(timezone.utc), preferred_runtime_tag)
        daily_times = _local_day_surface_times(anchor_local)
        history_times = _history_surface_local_times(anchor_local, anchor_runtime_tag)
        pending_specs.append(
            {
                "request_id": request_id,
                "station_icao": station_icao,
                "station_lat": station_lat,
                "station_lon": station_lon,
                "peak_local": peak_local,
                "analysis_local_key": analysis_local_key,
                "tz_name": tz_name,
                "preferred_runtime_tag": preferred_runtime_tag,
                "anchor_runtime_tag": anchor_runtime_tag,
                "anchor_local": anchor_local,
                "previous_local": previous_local,
                "next_local": next_local,
                "daily_times": daily_times,
                "history_times": history_times,
                "metar24": metar24,
            }
        )

    if not pending_specs:
        return results

    source = str(os.getenv("ECMWF_OPEN_DATA_SOURCE", "azure") or "azure").strip().lower()
    model_name = str(os.getenv("ECMWF_OPEN_DATA_MODEL", "ifs") or "ifs").strip().lower()

    fetch_groups: dict[tuple[str, int], dict[str, Any]] = {}
    slot_meta: dict[tuple[str, str], dict[str, Any]] = {}
    for spec in pending_specs:
        point_key = f"{spec['station_icao']}|{spec['station_lat']:.4f}|{spec['station_lon']:.4f}"
        slot_times = {
            "previous": spec["previous_local"],
            "anchor": spec["anchor_local"],
        }
        if normalized_stage in {"auto", "trajectory"}:
            slot_times["next"] = spec["next_local"]
        for local_dt in spec["history_times"]:
            slot_times.setdefault(f"history::{local_dt.strftime('%Y-%m-%dT%H:%M')}", local_dt)

        for slot_key, local_dt in slot_times.items():
            valid_utc = local_dt.astimezone(timezone.utc)
            runtime_tag, fh = _resolve_ens_runtime_for_valid_time(valid_utc, spec["preferred_runtime_tag"])
            group_key = (runtime_tag, fh)
            group = fetch_groups.setdefault(
                group_key,
                {
                    "valid_utc": valid_utc,
                    "runtime_tag": runtime_tag,
                    "fh": fh,
                    "points": {},
                    "aliases": [],
                },
            )
            group["points"][point_key] = {
                "id": point_key,
                "lat": spec["station_lat"],
                "lon": spec["station_lon"],
            }
            group["aliases"].append((spec["request_id"], slot_key, point_key))
            slot_meta[(spec["request_id"], slot_key)] = {"runtime_tag": runtime_tag, "fh": fh}

    extracted_by_slot: dict[tuple[str, str], dict[str, Any]] = {}
    with _ecmwf_workspace(root, "ecmwf_open_data") as workspace:
        for group in fetch_groups.values():
            pf_sfc_path, cf_sfc_path, _runtime_tag, _fh = _fetch_ensemble_surface_files(
                workspace=workspace,
                valid_utc=group["valid_utc"],
                preferred_runtime_tag=group["runtime_tag"],
                source=source,
                model_name=model_name,
                root=root,
            )
            extracted = _extract_point_surface_members_multi(
                pf_sfc_path,
                cf_sfc_path,
                list(group["points"].values()),
                root,
            )
            for request_id, slot_key, point_key in group["aliases"]:
                if point_key not in extracted:
                    continue
                key = (request_id, slot_key)
                if key not in extracted_by_slot:
                    extracted_by_slot[key] = dict(extracted[point_key])

    for spec in pending_specs:
        request_id = spec["request_id"]
        current_payload = extracted_by_slot.get((request_id, "anchor"))
        previous_payload = extracted_by_slot.get((request_id, "previous"))
        if not current_payload or not previous_payload:
            continue

        summary = summarize_member_paths(current_payload, previous_payload)
        member_trajectory: dict[str, Any] = {}
        next_payload = extracted_by_slot.get((request_id, "next"))
        if next_payload:
            member_trajectory = _build_member_trajectory(
                previous_payload=previous_payload,
                current_payload=current_payload,
                next_payload=next_payload,
                previous_local=spec["previous_local"],
                current_local=spec["anchor_local"],
                next_local=spec["next_local"],
            )

        history_snapshots: list[tuple[datetime, dict[str, Any]]] = []
        for local_dt in spec["history_times"]:
            slot_key = f"history::{local_dt.strftime('%Y-%m-%dT%H:%M')}"
            payload = extracted_by_slot.get((request_id, slot_key))
            if not payload:
                continue
            history_snapshots.append((local_dt, payload))
        if not any(local_dt == spec["anchor_local"] for local_dt, _payload in history_snapshots):
            history_snapshots.append((spec["anchor_local"], current_payload))
        if not any(local_dt == spec["previous_local"] for local_dt, _payload in history_snapshots):
            history_snapshots.append((spec["previous_local"], previous_payload))

        detail_level = "surface_trajectory" if bool(member_trajectory.get("members")) else "surface_anchor"
        day_keys = {item.strftime("%Y-%m-%dT%H:%M") for item in spec["daily_times"]}
        day_snapshots = [
            (local_dt, payload)
            for local_dt, payload in history_snapshots
            if local_dt.strftime("%Y-%m-%dT%H:%M") in day_keys
        ]
        daily_surface_timeline = _summarize_surface_snapshots(sorted(day_snapshots, key=lambda item: item[0]))
        history_surface_timeline = _summarize_surface_snapshots(sorted(history_snapshots, key=lambda item: item[0]))
        observed_surface_history = _build_observed_surface_history(
            metar24=list(spec.get("metar24") or []),
            valid_times_local=sorted(spec["history_times"]),
            tz_name=str(spec["tz_name"] or ""),
        )
        member_history_alignment = _build_member_surface_history_alignment(
            history_snapshots=sorted(history_snapshots, key=lambda item: item[0]),
            observed_surface_history=observed_surface_history,
        )
        anchor_meta = slot_meta.get((request_id, "anchor")) or {}
        previous_meta = slot_meta.get((request_id, "previous")) or {}
        next_meta = slot_meta.get((request_id, "next")) or {}
        payload = {
            **summary,
            "member_trajectory": member_trajectory,
            "daily_surface_timeline": daily_surface_timeline,
            "history_surface_timeline": history_surface_timeline,
            "observed_surface_history": observed_surface_history,
            "member_history_alignment": member_history_alignment,
            "source": {
                "provider": "ecmwf-ens-open-data",
                "runtime_requested": spec["preferred_runtime_tag"],
                "runtime_used": str(anchor_meta.get("runtime_tag") or ""),
                "previous_runtime_used": str(previous_meta.get("runtime_tag") or ""),
                "detail_level": detail_level,
                "capabilities": {
                    "surface_members": True,
                    "trajectory_members": bool(member_trajectory.get("members")),
                    "vertical_profile_members": False,
                },
                "analysis_fh": anchor_meta.get("fh"),
                "previous_fh": previous_meta.get("fh"),
                "peak_local": spec["peak_local"],
                "analysis_anchor_local": spec["anchor_local"].strftime("%Y-%m-%dT%H:%M"),
                "previous_local": spec["previous_local"].strftime("%Y-%m-%dT%H:%M"),
                "trajectory_next_local": spec["next_local"].strftime("%Y-%m-%dT%H:%M"),
                "day_times_local": [item["time_local"] for item in daily_surface_timeline.get("times", [])],
                "history_times_local": [item["time_local"] for item in history_surface_timeline.get("times", [])],
                "history_matched_obs_count": int(observed_surface_history.get("matched_count") or 0),
                "trajectory_runtime_used": str(next_meta.get("runtime_tag") or "") if member_trajectory else "",
                "trajectory_fh": next_meta.get("fh") if member_trajectory else None,
                "tz_name": spec["tz_name"],
            },
            "selection": {
                "station": spec["station_icao"],
                "lat": round(float(spec["station_lat"]), 4),
                "lon": round(float(spec["station_lon"]), 4),
                "grid_lat": _safe_float(current_payload.get("selected_lat")),
                "grid_lon": _safe_float(current_payload.get("selected_lon")),
            },
        }
        _write_cache(
            payload,
            spec["station_icao"],
            spec["peak_local"],
            spec["analysis_local_key"],
            spec["preferred_runtime_tag"],
            normalized_stage,
        )
        results[request_id] = payload

    return results
