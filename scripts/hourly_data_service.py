from __future__ import annotations

import hashlib
import json
import os
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import requests

import build_station_links as BSL
from cache_envelope import extract_payload, make_cache_doc
from station_catalog import Station
from window_phase_engine import pick_peak_indices

ROOT = Path(__file__).resolve().parent.parent
CACHE_DIR = ROOT / "cache" / "runtime"
CACHE_TTL_HOURS = 3
OPENMETEO_BREAKER_SECONDS = int(os.getenv("OPENMETEO_BREAKER_SECONDS", "900") or "900")
OPENMETEO_BREAKER_FILE = CACHE_DIR / "openmeteo_breaker.json"


def _openmeteo_breaker_info() -> tuple[bool, datetime | None, str | None]:
    try:
        if not OPENMETEO_BREAKER_FILE.exists():
            return False, None, None
        obj = json.loads(OPENMETEO_BREAKER_FILE.read_text(encoding="utf-8"))
        ts = datetime.fromisoformat(str(obj.get("until", "")).replace("Z", "+00:00"))
        reason = str(obj.get("reason") or "")
        active = datetime.now(timezone.utc) < ts
        return active, ts, reason
    except Exception:
        return False, None, None


def _openmeteo_breaker_active() -> bool:
    active, _until, _reason = _openmeteo_breaker_info()
    return active


def openmeteo_breaker_info() -> tuple[bool, datetime | None, str | None]:
    return _openmeteo_breaker_info()


def _retry_after_seconds_from_exc(exc: Exception) -> int | None:
    try:
        resp = getattr(exc, "response", None)
        if resp is None:
            return None
        ra = resp.headers.get("Retry-After")
        if not ra:
            return None
        return max(1, int(float(str(ra).strip())))
    except Exception:
        return None


def _trip_openmeteo_breaker(reason: str = "429", seconds: int | None = None) -> None:
    try:
        CACHE_DIR.mkdir(parents=True, exist_ok=True)
        sec = int(seconds if seconds is not None else OPENMETEO_BREAKER_SECONDS)
        until = (datetime.now(timezone.utc) + timedelta(seconds=max(1, sec))).isoformat().replace("+00:00", "Z")
        OPENMETEO_BREAKER_FILE.write_text(json.dumps({"until": until, "reason": reason}, ensure_ascii=False), encoding="utf-8")
    except Exception:
        pass


def _cache_key(*parts: str) -> str:
    raw = "|".join(parts)
    return hashlib.sha1(raw.encode("utf-8")).hexdigest()[:16]


def _cache_path(kind: str, *parts: str) -> Path:
    return CACHE_DIR / f"{kind}_{_cache_key(*parts)}.json"


def _read_cache(kind: str, *parts: str, ttl_hours: int = CACHE_TTL_HOURS, allow_stale: bool = False) -> dict[str, Any] | None:
    p = _cache_path(kind, *parts)
    if not p.exists():
        return None
    try:
        doc = json.loads(p.read_text(encoding="utf-8"))
        payload, updated_at, _env = extract_payload(doc)
        if payload is None:
            return None
        if (not allow_stale) and updated_at:
            ts = datetime.fromisoformat(str(updated_at).replace("Z", "+00:00"))
            if datetime.now(timezone.utc) - ts > timedelta(hours=ttl_hours):
                return None
        return payload
    except Exception:
        return None


def _write_cache(kind: str, payload: dict[str, Any], *parts: str) -> None:
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    p = _cache_path(kind, *parts)
    doc = make_cache_doc(
        payload,
        source_state="fresh",
        payload_schema_version=str(payload.get("schema_version")) if isinstance(payload, dict) else None,
        meta={"kind": kind},
    )
    p.write_text(json.dumps(doc, ensure_ascii=True), encoding="utf-8")


def prune_runtime_cache(max_age_hours: int = 24) -> None:
    if not CACHE_DIR.exists():
        return
    now = datetime.now(timezone.utc)

    for p in CACHE_DIR.glob("*.json"):
        try:
            mtime = datetime.fromtimestamp(p.stat().st_mtime, tz=timezone.utc)
            if (now - mtime) > timedelta(hours=max_age_hours):
                p.unlink(missing_ok=True)
        except Exception:
            continue

    grib_keep_h = int(os.getenv("GFS_GRIB_CACHE_HOURS", "36") or "36")
    gdir = CACHE_DIR / "gfs_grib"
    if gdir.exists() and gdir.is_dir():
        for p in gdir.glob("*.grib2"):
            try:
                mtime = datetime.fromtimestamp(p.stat().st_mtime, tz=timezone.utc)
                if (now - mtime) > timedelta(hours=grib_keep_h):
                    p.unlink(missing_ok=True)
            except Exception:
                continue


def model_cycle_tag(model: str, now_utc: datetime) -> str:
    m = (model or "").strip().lower()
    cfg = BSL.MODEL_CONFIGS.get(m)
    if cfg is None:
        cycle_h, lag_h = 6, 5
    else:
        cycle_h, lag_h = int(cfg.cycle_hours), int(cfg.availability_lag_hours)
    ready_time = now_utc - timedelta(hours=lag_h)
    hh = (ready_time.hour // cycle_h) * cycle_h
    return f"{ready_time.strftime('%Y%m%d')}{hh:02d}Z"


def _get_cached_hourly_cycle(kind: str, st: Station, target_date: str, model: str, cycle_tag: str) -> dict[str, Any] | None:
    return _read_cache(kind, st.icao, target_date, model.lower(), cycle_tag, allow_stale=True)


def _get_cached_hourly_previous_cycles(kind: str, st: Station, target_date: str, model: str, now_utc: datetime, max_back: int = 3) -> dict[str, Any] | None:
    cfg = BSL.MODEL_CONFIGS.get((model or "").lower())
    cycle_h = int(getattr(cfg, "cycle_hours", 6) or 6)
    for back in range(1, max_back + 1):
        prev_tag = model_cycle_tag(model, now_utc - timedelta(hours=cycle_h * back))
        c = _read_cache(kind, st.icao, target_date, model.lower(), prev_tag, allow_stale=True)
        if c:
            return c
    return None


def fetch_hourly_openmeteo(st: Station, target_date: str, model: str) -> dict[str, Any]:
    now_utc = datetime.now(timezone.utc)
    cycle_tag = model_cycle_tag(model, now_utc)
    cached_cur = _get_cached_hourly_cycle("hourly", st, target_date, model, cycle_tag)
    if cached_cur:
        return cached_cur

    d = datetime.strptime(target_date, "%Y-%m-%d").date()
    start_date = (d - timedelta(days=1)).isoformat()
    end_date = (d + timedelta(days=1)).isoformat()

    url = "https://api.open-meteo.com/v1/forecast"
    params = {
        "latitude": st.lat,
        "longitude": st.lon,
        "hourly": "temperature_2m,temperature_850hPa,wind_speed_850hPa,wind_direction_850hPa,cloud_cover_low,pressure_msl",
        "timezone": "auto",
        "start_date": start_date,
        "end_date": end_date,
    }

    if _openmeteo_breaker_active():
        stale = _read_cache("hourly", st.icao, target_date, model.lower(), cycle_tag, allow_stale=True)
        if stale:
            return stale
        raise RuntimeError("open-meteo breaker active")

    last_err: Exception | None = None
    for _ in range(1, 4):
        try:
            r = requests.get(url, params=params, timeout=45)
            r.raise_for_status()
            data = r.json()
            _write_cache("hourly", data, st.icao, target_date, model.lower(), cycle_tag)
            return data
        except Exception as exc:
            last_err = exc
            if "429" in str(exc) or "Too Many Requests" in str(exc):
                _trip_openmeteo_breaker("hourly_429", seconds=_retry_after_seconds_from_exc(exc))
                break

    stale = _read_cache("hourly", st.icao, target_date, model.lower(), cycle_tag, allow_stale=True)
    if stale:
        return stale
    assert last_err is not None
    raise last_err


def fetch_hourly_gfs_grib2(st: Station, target_date: str, model: str) -> dict[str, Any]:
    now_utc = datetime.now(timezone.utc)
    cycle_tag = model_cycle_tag(model, now_utc)
    cached_cur = _get_cached_hourly_cycle("hourly_gfs", st, target_date, model, cycle_tag)
    if cached_cur:
        return cached_cur

    from gfs_grib_provider import fetch_hourly_like

    try:
        data = fetch_hourly_like(st, target_date, cycle_tag, ROOT.parent.parent)
        _write_cache("hourly_gfs", data, st.icao, target_date, model.lower(), cycle_tag)
        return data
    except Exception as exc:
        if "429" in str(exc) or "Too Many Requests" in str(exc):
            raise RuntimeError("gfs-grib2 429")
        raise


def fetch_hourly_router(st: Station, target_date: str, model: str, provider: str = "auto") -> tuple[dict[str, Any], str]:
    p = (provider or "auto").strip().lower()
    now_utc = datetime.now(timezone.utc)

    if p in {"openmeteo", "open-meteo"}:
        return fetch_hourly_openmeteo(st, target_date, model), "openmeteo"
    if p in {"gfs", "gfs-grib2", "grib2"}:
        return fetch_hourly_gfs_grib2(st, target_date, model), "gfs-grib2"

    om_err = None
    gfs_err = None
    try:
        return fetch_hourly_openmeteo(st, target_date, model), "openmeteo"
    except Exception as exc:
        om_err = exc

    try:
        return fetch_hourly_gfs_grib2(st, target_date, model), "gfs-grib2"
    except Exception as exc:
        gfs_err = exc

    prev_om = _get_cached_hourly_previous_cycles("hourly", st, target_date, model, now_utc, max_back=3)
    if prev_om:
        return prev_om, "openmeteo-prev-cache"

    prev_gfs = _get_cached_hourly_previous_cycles("hourly_gfs", st, target_date, model, now_utc, max_back=3)
    if prev_gfs:
        return prev_gfs, "gfs-grib2-prev-cache"

    raise RuntimeError(f"hourly providers unavailable: openmeteo={om_err}; gfs={gfs_err}")


def slice_hourly_local_day(hourly: dict[str, Any], target_date: str) -> dict[str, Any]:
    times = hourly.get("time") or []
    keep = [i for i, t in enumerate(times) if str(t).startswith(target_date)]
    if not keep:
        return hourly
    out: dict[str, Any] = {}
    for k, v in hourly.items():
        if isinstance(v, list) and len(v) == len(times):
            out[k] = [v[i] for i in keep]
        else:
            out[k] = v
    return out


def _build_window_at_index(hourly: dict[str, Any], idx: int, band_c: float = 0.4) -> dict[str, Any]:
    times = hourly["time"]
    t2m = hourly["temperature_2m"]
    t850 = hourly["temperature_850hPa"]
    wspd850 = hourly["wind_speed_850hPa"]
    wdir850 = hourly["wind_direction_850hPa"]
    clow = hourly["cloud_cover_low"]
    pmsl = hourly["pressure_msl"]

    left = max(0, idx - 3)
    right = min(len(t2m) - 1, idx + 3)
    k0 = max(range(left, right + 1), key=lambda x: t2m[x])

    base = t2m[k0]
    s = k0
    e = k0
    while s - 1 >= 0 and t2m[s - 1] >= base - band_c:
        s -= 1
    while e + 1 < len(t2m) and t2m[e + 1] >= base - band_c:
        e += 1

    k = max(range(s, e + 1), key=lambda x: t2m[x])
    return {
        "start_local": times[s],
        "end_local": times[e],
        "peak_local": times[k],
        "peak_temp_c": t2m[k],
        "t850_c": t850[k],
        "w850_kmh": wspd850[k],
        "wd850_deg": wdir850[k],
        "low_cloud_pct": clow[k],
        "pmsl_hpa": pmsl[k],
    }


def detect_tmax_windows(hourly: dict[str, Any]) -> tuple[list[dict[str, Any]], dict[str, Any], list[dict[str, Any]]]:
    times = hourly["time"]
    t2m = hourly["temperature_2m"]

    tmax = max(t2m)
    idx = [i for i, v in enumerate(t2m) if v >= tmax - 0.5]
    windows: list[dict[str, Any]] = []
    if idx:
        s = idx[0]
        p = idx[0]
        for i in idx[1:]:
            if i == p + 1:
                p = i
            else:
                k = max(range(s, p + 1), key=lambda x: t2m[x])
                windows.append(_build_window_at_index(hourly, k, band_c=0.5))
                s = i
                p = i
        k = max(range(s, p + 1), key=lambda x: t2m[x])
        windows.append(_build_window_at_index(hourly, k, band_c=0.5))

    picked = pick_peak_indices(hourly, limit=4, min_separation_hours=2)

    candidates: list[dict[str, Any]] = []
    for i, s, info in picked:
        w = _build_window_at_index(hourly, i, band_c=0.4)
        candidates.append({
            "score": round(float(s), 3),
            "hour_local": times[i],
            "window": w,
            "factors": info,
        })

    primary = candidates[0]["window"] if candidates else (windows[0] if windows else {})
    return windows, primary, candidates


def build_post_focus_window(hourly: dict[str, Any], metar_diag: dict[str, Any]) -> dict[str, Any] | None:
    try:
        times = list(hourly.get("time") or [])
        t2m = list(hourly.get("temperature_2m") or [])
        if not times or not t2m or len(times) != len(t2m):
            return None

        latest_local = datetime.fromisoformat(str(metar_diag.get("latest_report_local") or ""))
        if latest_local.tzinfo is not None:
            latest_local = latest_local.replace(tzinfo=None)

        cands: list[int] = []
        for i, ts in enumerate(times):
            try:
                dt = datetime.fromisoformat(str(ts))
                if dt >= latest_local + timedelta(hours=1):
                    cands.append(i)
            except Exception:
                continue

        if not cands:
            return None

        k = max(cands, key=lambda i: float(t2m[i]))
        s = max(0, k - 1)
        e = min(len(times) - 1, k + 1)
        w = {
            "start_local": times[s],
            "end_local": times[e],
            "peak_local": times[k],
            "peak_temp_c": hourly["temperature_2m"][k],
            "t850_c": hourly["temperature_850hPa"][k],
            "w850_kmh": hourly["wind_speed_850hPa"][k],
            "wd850_deg": hourly["wind_direction_850hPa"][k],
            "low_cloud_pct": hourly["cloud_cover_low"][k],
            "pmsl_hpa": hourly["pressure_msl"][k],
        }
        try:
            obs_max = float(metar_diag.get("observed_max_temp_c")) if metar_diag.get("observed_max_temp_c") is not None else None
        except Exception:
            obs_max = None
        if obs_max is not None:
            w["rebreak_gap_c"] = round(float(obs_max) - float(w.get("peak_temp_c") or 0.0), 2)
        w["window_role"] = "post_focus_secondary"
        return w
    except Exception:
        return None


def build_post_eval_window(hourly: dict[str, Any], metar_diag: dict[str, Any]) -> dict[str, Any] | None:
    try:
        times = list(hourly.get("time") or [])
        t2m = list(hourly.get("temperature_2m") or [])
        if not times or not t2m or len(times) != len(t2m):
            return None

        latest_local = datetime.fromisoformat(str(metar_diag.get("latest_report_local") or ""))
        if latest_local.tzinfo is not None:
            latest_local = latest_local.replace(tzinfo=None)

        cands: list[int] = []
        for i, ts in enumerate(times):
            try:
                dt = datetime.fromisoformat(str(ts))
                if latest_local + timedelta(hours=1) <= dt <= latest_local + timedelta(hours=3):
                    cands.append(i)
            except Exception:
                continue

        if not cands:
            return None

        k = cands[0]
        s = max(0, k - 1)
        e = min(len(times) - 1, k + 1)
        w = {
            "start_local": times[s],
            "end_local": times[e],
            "peak_local": times[k],
            "peak_temp_c": hourly["temperature_2m"][k],
            "t850_c": hourly["temperature_850hPa"][k],
            "w850_kmh": hourly["wind_speed_850hPa"][k],
            "wd850_deg": hourly["wind_direction_850hPa"][k],
            "low_cloud_pct": hourly["cloud_cover_low"][k],
            "pmsl_hpa": hourly["pressure_msl"][k],
            "window_role": "post_eval_no_rebreak",
        }
        return w
    except Exception:
        return None
