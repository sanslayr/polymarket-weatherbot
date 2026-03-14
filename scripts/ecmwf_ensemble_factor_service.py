from __future__ import annotations

import hashlib
import json
import os
import subprocess
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from cache_envelope import extract_payload, make_cache_doc
from ecmwf_open_data_provider import _ecmwf_step_valid, _ecmwf_workspace, _repo_root, _retrieve_grib
from runtime_cache_policy import runtime_cache_enabled
from runtime_utils import runtime_dt_from_tag, runtime_tag_from_dt


ROOT = Path(__file__).resolve().parent.parent
CACHE_DIR = ROOT / "cache" / "runtime"
SCHEMA_VERSION = "ecmwf-ensemble-factor.v2"


def _safe_float(value: Any) -> float | None:
    try:
        if value in (None, ""):
            return None
        return float(value)
    except Exception:
        return None


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


def _ensemble_request(runtime_dt: datetime, step: int) -> dict[str, Any]:
    return {
        "date": runtime_dt.strftime("%Y-%m-%d"),
        "time": int(runtime_dt.strftime("%H")),
        "stream": "enfo",
        "type": "ef",
        "step": int(step),
        "levtype": "pl",
        "levelist": 850,
        "param": ["t", "u", "v"],
    }


def _fetch_ensemble_pressure_files(
    *,
    workspace: Path,
    valid_utc: datetime,
    preferred_runtime_tag: str,
    source: str,
    model_name: str,
    root: Path,
) -> tuple[Path, Path, str, int]:
    runtime_tag, fh = _resolve_ens_runtime_for_valid_time(valid_utc, preferred_runtime_tag)
    ef_target = workspace / f"ecmwf_ens_{runtime_tag}_enfo_ef_f{fh:03d}_850_tuv.grib2"
    if ef_target.exists():
        return ef_target, ef_target, runtime_tag, fh

    runtime_dt = runtime_dt_from_tag(runtime_tag)
    _retrieve_grib(
        target=ef_target,
        request=_ensemble_request(runtime_dt, fh),
        source=source,
        model=model_name,
        root=root,
    )
    return ef_target, ef_target, runtime_tag, fh


def _extract_point_members(pf_path: Path, cf_path: Path, lat: float, lon: float, root: Path) -> dict[str, Any]:
    py = _repo_root(root) / ".venv_gfs" / "bin" / "python"
    if not py.exists():
        raise RuntimeError("ecmwf ensemble parser venv missing (.venv_gfs)")

    code = r'''
import json, math, sys
import xarray as xr

pf_path, cf_path, lat, lon = sys.argv[1], sys.argv[2], float(sys.argv[3]), float(sys.argv[4])

def norm_lon(value):
    out = float(value)
    return ((out + 180.0) % 360.0) - 180.0

lon = norm_lon(lon)

def select_point(ds):
    return ds.sel(latitude=lat, longitude=lon, method="nearest")

def val(arr):
    raw = float(arr.values)
    return raw

def member_payload(number, point):
    t850 = val(point["t"])
    u850 = val(point["u"])
    v850 = val(point["v"])
    if t850 > 170.0:
        t850 -= 273.15
    wspd = math.sqrt(u850 * u850 + v850 * v850) * 3.6
    wdir = (math.degrees(math.atan2(-u850, -v850)) + 360.0) % 360.0
    return {
        "number": int(number),
        "t850_c": round(t850, 2),
        "u850_ms": round(u850, 2),
        "v850_ms": round(v850, 2),
        "wind_speed_850_kmh": round(wspd, 2),
        "wind_direction_850_deg": round(wdir, 1),
    }

pf = select_point(xr.open_dataset(pf_path, engine="cfgrib", backend_kwargs={"filter_by_keys": {"dataType": "pf"}}))
cf = select_point(xr.open_dataset(cf_path, engine="cfgrib", backend_kwargs={"filter_by_keys": {"dataType": "cf"}}))

members = []
for idx in pf["number"].values.tolist():
    point = pf.sel(number=int(idx))
    members.append(member_payload(idx, point))

cf_num = 0
try:
    cf_num = int(cf["number"].values)
except Exception:
    cf_num = 0
members.insert(0, member_payload(cf_num, cf))

out = {
    "selected_lat": round(float(pf["latitude"].values), 3),
    "selected_lon": round(norm_lon(float(pf["longitude"].values)), 3),
    "valid_time": str(pf["valid_time"].values)[:19].replace(" ", "T"),
    "members": members,
}
print(json.dumps(out, ensure_ascii=False))
'''
    proc = subprocess.run(
        [str(py), "-c", code, str(pf_path), str(cf_path), str(lat), str(lon)],
        capture_output=True,
        text=True,
        timeout=int(os.getenv("ECMWF_ENS_PARSE_TIMEOUT_SECONDS", "120") or "120"),
    )
    if proc.returncode != 0:
        raise RuntimeError(f"ecmwf ensemble parse failed: {proc.stderr.strip() or proc.stdout.strip()}")
    return json.loads(proc.stdout.strip())


def _member_map(payload: dict[str, Any]) -> dict[int, dict[str, Any]]:
    out: dict[int, dict[str, Any]] = {}
    for raw in payload.get("members") or []:
        try:
            out[int(raw.get("number"))] = dict(raw)
        except Exception:
            continue
    return out


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
    wind_speed_850_kmh: float | None,
) -> tuple[str, str]:
    warm_threshold = _safe_float(os.getenv("ECMWF_ENS_WARM_T850_DELTA_C", "0.7")) or 0.7
    cold_threshold = _safe_float(os.getenv("ECMWF_ENS_COLD_T850_DELTA_C", "-0.7")) or -0.7
    speed = wind_speed_850_kmh if wind_speed_850_kmh is not None else 0.0
    delta = t850_delta_c if t850_delta_c is not None else 0.0

    if delta >= warm_threshold and speed >= 12.0:
        return "warm_support", "warm_support"
    if delta <= cold_threshold:
        return "cold_suppression", "cold_suppression"
    if delta >= max(0.3, warm_threshold * 0.5) and speed >= 20.0:
        return "warm_support", "warm_support"
    if delta <= min(-0.3, cold_threshold * 0.5):
        return "transition", _transition_detail_label(
            t850_delta_c=t850_delta_c,
            wind_speed_850_kmh=wind_speed_850_kmh,
        )
    return "transition", _transition_detail_label(
        t850_delta_c=t850_delta_c,
        wind_speed_850_kmh=wind_speed_850_kmh,
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
                "wind_speed_850_kmh": _safe_float(raw.get("wind_speed_850_kmh")),
            }
        )

    if not valid_rows:
        raise RuntimeError("member path rows are empty")

    total = float(len(valid_rows))
    probabilities = {
        key: _normalize_percent(value / total)
        for key, value in counts.items()
    }
    delta_values = [float(row["delta_t850_c"]) for row in valid_rows if row.get("delta_t850_c") is not None]
    speed_values = [float(row["wind_speed_850_kmh"]) for row in valid_rows if row.get("wind_speed_850_kmh") is not None]
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
            "delta_t850_p10_c": _quantile(delta_values, 0.10),
            "delta_t850_p50_c": _quantile(delta_values, 0.50),
            "delta_t850_p90_c": _quantile(delta_values, 0.90),
            "wind850_p50_kmh": _quantile(speed_values, 0.50),
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
        delta_t850_c = None if t850_now is None or t850_prev is None else round(t850_now - t850_prev, 2)
        wind_speed_850_kmh = _safe_float(current.get("wind_speed_850_kmh"))
        label, detail = _classify_member_path(
            t850_delta_c=delta_t850_c,
            wind_speed_850_kmh=wind_speed_850_kmh,
        )
        rows.append(
            {
                "number": number,
                "path_label": label,
                "path_detail": detail,
                "delta_t850_c": delta_t850_c,
                "wind_speed_850_kmh": wind_speed_850_kmh,
            }
        )

    return summarize_member_path_rows(rows)


def build_ecmwf_ensemble_factor(
    *,
    station_icao: str,
    station_lat: float,
    station_lon: float,
    peak_local: str,
    tz_name: str,
    preferred_runtime_tag: str,
    root: Path = ROOT,
) -> dict[str, Any]:
    cache_hit = _read_cache(station_icao, peak_local, preferred_runtime_tag)
    if cache_hit:
        return cache_hit

    from zoneinfo import ZoneInfo

    peak_dt_local = datetime.strptime(peak_local, "%Y-%m-%dT%H:%M").replace(tzinfo=ZoneInfo(tz_name))
    previous_dt_local = peak_dt_local - timedelta(hours=6)
    peak_dt_utc = peak_dt_local.astimezone(timezone.utc)
    previous_dt_utc = previous_dt_local.astimezone(timezone.utc)

    source = str(os.getenv("ECMWF_OPEN_DATA_SOURCE", "azure") or "azure").strip().lower()
    model_name = str(os.getenv("ECMWF_OPEN_DATA_MODEL", "ifs") or "ifs").strip().lower()

    with _ecmwf_workspace(root, "ecmwf_open_data") as workspace:
        current_pf_file, current_cf_file, current_runtime, current_fh = _fetch_ensemble_pressure_files(
            workspace=workspace,
            valid_utc=peak_dt_utc,
            preferred_runtime_tag=preferred_runtime_tag,
            source=source,
            model_name=model_name,
            root=root,
        )
        previous_pf_file, previous_cf_file, previous_runtime, previous_fh = _fetch_ensemble_pressure_files(
            workspace=workspace,
            valid_utc=previous_dt_utc,
            preferred_runtime_tag=preferred_runtime_tag,
            source=source,
            model_name=model_name,
            root=root,
        )

    current_payload = _extract_point_members(current_pf_file, current_cf_file, station_lat, station_lon, root)
    previous_payload = _extract_point_members(previous_pf_file, previous_cf_file, station_lat, station_lon, root)
    summary = summarize_member_paths(current_payload, previous_payload)
    payload = {
        **summary,
        "source": {
            "provider": "ecmwf-ens-open-data",
            "runtime_requested": preferred_runtime_tag,
            "runtime_used": current_runtime,
            "previous_runtime_used": previous_runtime,
            "analysis_fh": current_fh,
            "previous_fh": previous_fh,
            "peak_local": peak_local,
            "previous_local": previous_dt_local.strftime("%Y-%m-%dT%H:%M"),
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
    _write_cache(payload, station_icao, peak_local, preferred_runtime_tag)
    return payload
