#!/usr/bin/env python3
"""Telegram command entrypoint for city Tmax report.

Examples:
  /look Ankara
  /lookhelp
  /look city=Ankara date=2026-02-26
  /look icao=LTAC model=ecmwf
  /look city=Toronto model=gfs
  /look Ankara modify model gfs date 2026-02-27
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import importlib.util
import json
import math
import os
import re
import sys
import time
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

import requests

from forecast_pipeline import load_or_build_forecast_decision
from realtime_pipeline import classify_window_phase, select_realtime_triggers

ROOT = Path(__file__).resolve().parent.parent
STATION_CSV = ROOT / "station_links.csv"
SCRIPTS_DIR = ROOT / "scripts"
CITY_ALIASES = {
    "nyc": "new york",
    "newyork": "new york",
    "la": "los angeles",
    "sao": "sao paulo",
    "saopaulo": "sao paulo",
    "buenos": "buenos aires",
    "seoul": "seoul",
    "ank": "ankara",
}

STATION_TZ = {
    "LTAC": "Europe/Istanbul",
    "EGLC": "Europe/London",
    "LFPG": "Europe/Paris",
    "NZWN": "Pacific/Auckland",
    "CYYZ": "America/Toronto",
    "KATL": "America/New_York",
    "KJFK": "America/New_York",
    "KDFW": "America/Chicago",
    "KORD": "America/Chicago",
    "KSEA": "America/Los_Angeles",
    "KMIA": "America/New_York",
    "SBSP": "America/Sao_Paulo",
    "RKSI": "Asia/Seoul",
    "SAEZ": "America/Argentina/Buenos_Aires",
}

CACHE_DIR = ROOT / "cache" / "runtime"
CACHE_TTL_HOURS = 3
CACHE_PRUNE_HOURS = 24
PERF_LOG_ENABLED = os.getenv("LOOK_PERF_LOG", "0") == "1"
OPENMETEO_BREAKER_SECONDS = int(os.getenv("OPENMETEO_BREAKER_SECONDS", "900") or "900")
OPENMETEO_BREAKER_FILE = CACHE_DIR / "openmeteo_breaker.json"
SYNOPTIC_PROVIDER = "gfs-grib2"


def _perf_log(stage: str, seconds: float) -> None:
    if not PERF_LOG_ENABLED:
        return
    try:
        CACHE_DIR.mkdir(parents=True, exist_ok=True)
        p = CACHE_DIR / "perf.log"
        line = f"{datetime.now(timezone.utc).isoformat().replace('+00:00','Z')}\t{stage}\t{seconds:.3f}s\n"
        with p.open("a", encoding="utf-8") as f:
            f.write(line)
    except Exception:
        pass


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


_STATION_META_MAP: dict[str, dict[str, str]] | None = None


def _station_meta_for(icao: str) -> dict[str, str]:
    global _STATION_META_MAP
    try:
        if _STATION_META_MAP is None:
            mp: dict[str, dict[str, str]] = {}
            with STATION_CSV.open(newline="", encoding="utf-8") as f:
                for row in csv.DictReader(f):
                    k = str(row.get("icao") or "").upper().strip()
                    if not k:
                        continue
                    t1 = str(row.get("terrain_tag") or "").strip()
                    t2 = str(row.get("terrain_tag2") or "").strip()

                    topo_tokens = ["低地", "高地", "丘陵", "平原", "山地", "高原", "台地"]
                    redundant = False
                    if t1 and t2:
                        redundant = any((tk in t1 and tk in t2) for tk in topo_tokens)
                    terr = t1 if (t1 and (not t2 or redundant)) else (f"{t1}·{t2}" if (t1 and t2) else (t1 or ""))
                    mp[k] = {
                        "terrain": terr,
                        "site_tag": str(row.get("site_tag") or "").strip(),
                        "factor_summary": str(row.get("factor_summary") or "").strip(),
                        "terrain_sector": str(row.get("terrain_sector") or "").strip(),
                        "water_factor": str(row.get("water_factor") or "").strip(),
                        "water_sector": str(row.get("water_sector") or "").strip(),
                        "city_sector": str(row.get("city_sector") or "").strip(),
                        "city_distance_km": str(row.get("city_distance_km") or "").strip(),
                        "urban_position": str(row.get("urban_position") or "").strip(),
                    }
            _STATION_META_MAP = mp
        return (_STATION_META_MAP or {}).get(str(icao).upper(), {})
    except Exception:
        return {}


def _terrain_tag_for(icao: str) -> str | None:
    t = _station_meta_for(icao).get("terrain")
    return t if t else None


def _site_tag_for(icao: str) -> str | None:
    t = _station_meta_for(icao).get("site_tag")
    return t if t else None


def _factor_summary_for(icao: str) -> str | None:
    t = _station_meta_for(icao).get("factor_summary")
    return t if t else None


def _direction_factor_for(icao: str) -> str | None:
    m = _station_meta_for(icao)
    water_sec = str(m.get("water_sector") or "").strip()
    urban_pos = str(m.get("urban_position") or "").strip()
    bits = []
    if water_sec and water_sec not in {"内陆主导", "未知"}:
        bits.append(water_sec)
    if urban_pos and urban_pos != "未知":
        bits.append(urban_pos)
    if not bits:
        return None
    return " | ".join(bits)


def station_timezone_name(st: Station) -> str:
    return STATION_TZ.get(str(st.icao).upper(), "UTC")


def format_utc_offset(dt: datetime) -> str:
    z = dt.strftime("%z")
    if not z:
        return "UTC+00"
    return f"UTC{z[:3]}"


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
        ts = datetime.fromisoformat(doc.get("updated_at", "").replace("Z", "+00:00"))
        if not allow_stale and datetime.now(timezone.utc) - ts > timedelta(hours=ttl_hours):
            return None
        return doc.get("payload")
    except Exception:
        return None


def _write_cache(kind: str, payload: dict[str, Any], *parts: str) -> None:
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    p = _cache_path(kind, *parts)
    doc = {
        "updated_at": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
        "payload": payload,
    }
    p.write_text(json.dumps(doc, ensure_ascii=True), encoding="utf-8")


def _prune_runtime_cache(max_age_hours: int = CACHE_PRUNE_HOURS) -> None:
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


def load_build_station_links_module() -> Any:
    module_path = SCRIPTS_DIR / "build_station_links.py"
    spec = importlib.util.spec_from_file_location("build_station_links", module_path)
    module = importlib.util.module_from_spec(spec)
    assert spec and spec.loader
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


BSL = load_build_station_links_module()


@dataclass
class Station:
    city: str
    icao: str
    lat: float
    lon: float


def norm_text(s: str) -> str:
    return "".join(ch for ch in s.strip().lower() if ch.isalnum())


def parse_telegram_command(text: str) -> dict[str, str]:
    text = text.strip()
    if not text:
        raise ValueError("Empty command")

    parts = text.split()
    cmd = parts[0].lstrip("/").lower()
    params: dict[str, str] = {"cmd": cmd}

    key_aliases = {
        "city": "city",
        "icao": "icao",
        "station": "station",
        "model": "model",
        "date": "date",
        "m": "model",
        "d": "date",
        "模型": "model",
        "日期": "date",
    }
    skip_tokens = {"modify", "mod", "update", "set", "修改"}

    i = 1
    while i < len(parts):
        tok = parts[i].strip()
        if not tok:
            i += 1
            continue

        lower_tok = tok.lower()
        if lower_tok in skip_tokens:
            i += 1
            continue

        if "=" in tok:
            k, v = tok.split("=", 1)
            k_norm = key_aliases.get(k.strip().lower(), k.strip().lower())
            params[k_norm] = v.strip()
            i += 1
            continue

        if ":" in tok:
            k, v = tok.split(":", 1)
            k_norm = key_aliases.get(k.strip().lower(), k.strip().lower())
            params[k_norm] = v.strip()
            i += 1
            continue

        key_norm = key_aliases.get(lower_tok)
        if key_norm and i + 1 < len(parts):
            params[key_norm] = parts[i + 1].strip()
            i += 2
            continue

        # positional city/icao shortcut
        params.setdefault("station", tok)
        i += 1

    if "city" in params and "station" not in params:
        params["station"] = params["city"]
    if "icao" in params and "station" not in params:
        params["station"] = params["icao"]
    return params


def resolve_station(station_hint: str) -> Station:
    raw = station_hint.strip().lower()
    key = CITY_ALIASES.get(raw, raw)
    key_norm = norm_text(key)
    with STATION_CSV.open(newline="", encoding="utf-8") as f:
        rows = list(csv.DictReader(f))

    def as_station(row: dict[str, str]) -> Station:
        return Station(
            city=row["city"].strip(),
            icao=row["icao"].strip().upper(),
            lat=float(row["lat"]),
            lon=float(row["lon"]),
        )

    exact: list[dict[str, str]] = []
    prefix: list[dict[str, str]] = []
    contains: list[dict[str, str]] = []
    for row in rows:
        icao = row["icao"].strip().lower()
        city = row["city"].strip().lower()
        icao_n = norm_text(icao)
        city_n = norm_text(city)

        if icao == key or city == key or icao_n == key_norm or city_n == key_norm:
            exact.append(row)
            continue
        if icao.startswith(key) or city.startswith(key) or icao_n.startswith(key_norm) or city_n.startswith(key_norm):
            prefix.append(row)
            continue
        if key in city or key_norm in city_n:
            contains.append(row)

    if exact:
        return as_station(exact[0])
    if len(prefix) == 1:
        return as_station(prefix[0])
    if len(contains) == 1:
        return as_station(contains[0])

    if prefix or contains:
        cands = prefix if prefix else contains
        labels = ", ".join(sorted({f"{r['city']}({r['icao']})" for r in cands}))
        raise ValueError(f"Ambiguous station hint '{station_hint}'. Candidates: {labels}")
    raise ValueError(f"Unknown station/city: {station_hint}")


def default_model_for_station(st: Station) -> str:
    # Unified default: align headline model with current 3D strategy unless user overrides.
    m = (os.getenv("LOOK_DEFAULT_MODEL", "gfs") or "gfs").strip().lower()
    return m if m in {"gfs", "ecmwf"} else "gfs"


def _model_cycle_tag(model: str, now_utc: datetime) -> str:
    m = (model or "").strip().lower()
    # 按“可用时次”而非整点时次判定：考虑发布延迟（例如00Z常在03-05Z后可用）。
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
        prev_tag = _model_cycle_tag(model, now_utc - timedelta(hours=cycle_h * back))
        c = _read_cache(kind, st.icao, target_date, model.lower(), prev_tag, allow_stale=True)
        if c:
            return c
    return None


def fetch_hourly_openmeteo(st: Station, target_date: str, model: str) -> dict[str, Any]:
    now_utc = datetime.now(timezone.utc)
    cycle_tag = _model_cycle_tag(model, now_utc)
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
    for attempt in range(1, 4):
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
    cycle_tag = _model_cycle_tag(model, now_utc)
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

    # auto policy:
    # 1) prefer open-meteo current cycle (fetch if needed)
    # 2) fallback gfs current cycle (fetch if needed)
    # 3) if both unavailable, fallback to previous-cycle caches (openmeteo then gfs)
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

    base = t2m[idx]
    s = idx
    e = idx
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
    clow = hourly["cloud_cover_low"]

    # Legacy tmax windows (for compatibility)
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

    # Full-day continuous scan: peak potential over all hours.
    tmin, tmax_v = min(t2m), max(t2m)
    span = max(0.5, tmax_v - tmin)
    raw_scores: list[tuple[int, float]] = []
    for i in range(len(t2m)):
        temp_norm = (t2m[i] - tmin) / span
        up1 = max(0.0, (t2m[i + 1] - t2m[i]) if i + 1 < len(t2m) else 0.0)
        up2 = max(0.0, (t2m[i + 2] - t2m[i + 1]) if i + 2 < len(t2m) else 0.0)
        traj = min(2.0, up1 + up2) / 2.0
        cloud_term = 1.0 - min(1.0, max(0.0, float(clow[i] or 0.0) / 100.0))
        score = 0.60 * temp_norm + 0.25 * traj + 0.15 * cloud_term
        raw_scores.append((i, score))

    raw_scores.sort(key=lambda x: x[1], reverse=True)
    picked: list[tuple[int, float]] = []
    for i, s in raw_scores:
        if any(abs(i - j) <= 2 for j, _ in picked):
            continue
        picked.append((i, s))
        if len(picked) >= 4:
            break

    candidates: list[dict[str, Any]] = []
    for i, s in picked:
        w = _build_window_at_index(hourly, i, band_c=0.4)
        candidates.append({
            "score": round(float(s), 3),
            "hour_local": times[i],
            "window": w,
        })

    primary = candidates[0]["window"] if candidates else (windows[0] if windows else {})
    return windows, primary, candidates


def fetch_metar_24h(icao: str) -> list[dict[str, Any]]:
    # Always prefer live METAR, with retries and short backoff.
    url = f"https://aviationweather.gov/api/data/metar?ids={icao}&format=json&hours=24"
    last_err: Exception | None = None
    for attempt in range(1, 4):
        try:
            r = requests.get(url, timeout=40)
            r.raise_for_status()
            return r.json()
        except Exception as exc:
            last_err = exc
    assert last_err is not None
    raise last_err


def _metar_obs_time_utc(metar: dict[str, Any]) -> datetime:
    raw = (metar.get("rawOb") or "").strip()
    # Prefer raw METAR observation timestamp (DDHHMMZ).
    m = None
    import re
    m = re.search(r"\b(\d{2})(\d{2})(\d{2})Z\b", raw)
    if m:
        day, hh, mm = int(m.group(1)), int(m.group(2)), int(m.group(3))
        rt = datetime.fromisoformat(metar["reportTime"].replace("Z", "+00:00")).astimezone(timezone.utc)
        return datetime(rt.year, rt.month, day, hh, mm, tzinfo=timezone.utc)
    return datetime.fromisoformat(metar["reportTime"].replace("Z", "+00:00")).astimezone(timezone.utc)


def metar_observation_block(metar24: list[dict[str, Any]], hourly_local: dict[str, Any], tz_name: str, target_date: str | None = None) -> tuple[str, dict[str, Any]]:
    if not metar24:
        return "无可用METAR数据。", {}

    series = sorted(metar24, key=lambda x: x.get("reportTime", ""))
    latest = series[-1]
    prev = series[-2] if len(series) > 1 else None
    prev2 = series[-3] if len(series) > 2 else None

    tz = ZoneInfo(tz_name)
    latest_dt_local = _metar_obs_time_utc(latest).astimezone(tz)
    hour_key = latest_dt_local.strftime("%Y-%m-%dT%H:00")

    tmap = {t: v for t, v in zip(hourly_local["time"], hourly_local["temperature_2m"])}
    pmap = {t: v for t, v in zip(hourly_local["time"], hourly_local["pressure_msl"])}
    fc_t = tmap.get(hour_key)
    fc_p = pmap.get(hour_key)

    def parse_cloud_layers(raw_ob: str, fallback_cover: str | None) -> str:
        import re
        if not raw_ob:
            return fallback_cover or "N/A"
        if " CAVOK" in raw_ob:
            return "CAVOK"
        if " CLR" in raw_ob:
            return "CLR"
        layers = re.findall(r"\b(FEW|SCT|BKN|OVC|VV)(\d{3})\b", raw_ob)
        if not layers:
            return fallback_cover or "N/A"

        code_meaning = {
            "FEW": "少云",
            "SCT": "疏云",
            "BKN": "多云",
            "OVC": "阴天",
            "VV": "垂直能见度",
        }
        out = []
        for code, h in layers:
            ft = int(h) * 100
            m = int(round(ft * 0.3048))
            meaning = code_meaning.get(code, code)
            out.append(f"{code}{h}({meaning}{ft}ft/{m}m)")
        return ", ".join(out)

    def _wind_dir_text(d: Any) -> str:
        try:
            deg = float(d)
        except Exception:
            return "风向不定"
        deg = deg % 360
        dirs = [
            "北风", "东北偏北风", "东北风", "东北偏东风",
            "东风", "东南偏东风", "东南风", "东南偏南风",
            "南风", "西南偏南风", "西南风", "西南偏西风",
            "西风", "西北偏西风", "西北风", "西北偏北风",
        ]
        idx = int(((deg + 11.25) % 360) // 22.5)
        return dirs[idx]

    def fmt_wind(x: dict[str, Any]) -> str:
        d = x.get("wdir")
        s = x.get("wspd")
        if d in (None, "", "VRB"):
            return f"风向不定（VRB） {s}kt"
        return f"{_wind_dir_text(d)}（{d}°） {s}kt"

    time_label = "UTC" if str(tz_name).upper() == "UTC" else "Local"

    def _delta_text(v: float, unit: str) -> str:
        if abs(v) < 0.05:
            return "较上一报持平"
        return f"较上一报 {v:+.1f}{unit}"

    def fmt_latest_obs(x: dict[str, Any], prev_x: dict[str, Any] | None) -> list[str]:
        local = _metar_obs_time_utc(x).astimezone(tz)
        raw_ob = (x.get("rawOb") or "").strip()
        wx = x.get("wxString") or x.get("wx") or "无降水天气现象"
        cloud = parse_cloud_layers(raw_ob, x.get("cover"))
        prev_cloud = None
        dt = dp = dpres = 0.0
        if prev_x:
            try:
                dt = float(x.get("temp", 0)) - float(prev_x.get("temp", 0))
                dp = float(x.get("dewp", 0)) - float(prev_x.get("dewp", 0))
                dpres = float(x.get("altim", 0)) - float(prev_x.get("altim", 0))
            except Exception:
                pass
            prev_raw = (prev_x.get("rawOb") or "").strip()
            prev_cloud = parse_cloud_layers(prev_raw, prev_x.get("cover"))
        cloud_compare = ""
        if prev_x:
            prev_code = _cloud_token(prev_x) or "未知"
            tr = _cloud_trend(x, prev_x)
            if "稳定" in tr:
                tr_txt = "云层稳定无变化"
            elif "增加" in tr or "回补" in tr:
                tr_txt = "云量增加"
            elif "减弱" in tr or "开窗" in tr:
                tr_txt = "云量减少"
            else:
                tr_txt = "云层趋势待确认"
            cloud_compare = f"（上一报{prev_code}，{tr_txt}）"
        lines = [
            f"**最新报（{local.strftime('%H:%M')} {time_label}）**",
            f"• **气温**：{x.get('temp')}°C（{_delta_text(dt, '°C')}）",
            f"• **露点**：{x.get('dewp')}°C（{_delta_text(dp, '°C')}）",
            f"• **气压**：{x.get('altim')} hPa（{_delta_text(dpres, ' hPa')}）",
            f"• **风**：{fmt_wind(x)}",
            f"• **云层**：{cloud}{cloud_compare}",
        ]
        if wx and wx != "无降水天气现象":
            lines.append(f"• **天气现象**：{wx}")
        return lines

    def _cloud_code(x: dict[str, Any] | None) -> str:
        if not x:
            return ""
        raw = (x.get("rawOb") or "")
        if " CAVOK" in raw:
            return "CAVOK"
        if " CLR" in raw:
            return "CLR"
        m = re.search(r"\b(FEW|SCT|BKN|OVC|VV)\d{3}\b", raw)
        if m:
            return m.group(1)
        return str(x.get("cover") or "").upper()

    def _cloud_token(x: dict[str, Any] | None) -> str:
        if not x:
            return ""
        raw = (x.get("rawOb") or "")
        if " CAVOK" in raw:
            return "CAVOK"
        if " CLR" in raw:
            return "CLR"
        m = re.search(r"\b(FEW|SCT|BKN|OVC|VV)\d{3}\b", raw)
        if m:
            return m.group(0)
        return _cloud_code(x)

    def _cloud_trend(cur: dict[str, Any], prev_x: dict[str, Any] | None) -> str:
        rank = {"CLR": 0, "CAVOK": 0, "FEW": 1, "SCT": 2, "BKN": 3, "OVC": 4, "VV": 5}
        c1 = _cloud_code(cur)
        c0 = _cloud_code(prev_x)
        if not c0 or not c1:
            return "云层趋势不明确"
        r0 = rank.get(c0, 2)
        r1 = rank.get(c1, 2)
        if r1 >= r0 + 2:
            return f"云层快速回补（{c0}→{c1}）"
        if r1 > r0:
            return f"云量增加（{c0}→{c1}）"
        if r1 <= r0 - 2:
            return f"云层明显开窗（{c0}→{c1}）"
        if r1 < r0:
            return f"云量减弱（{c0}→{c1}）"
        return f"云层级别稳定（{c1}）"

    def _hour_bias(obs: dict[str, Any]) -> float | None:
        try:
            k = _metar_obs_time_utc(obs).astimezone(tz).strftime("%Y-%m-%dT%H:00")
            fv = tmap.get(k)
            if fv is None:
                return None
            return round(float(obs.get("temp", 0.0)) - float(fv), 2)
        except Exception:
            return None

    lines = []
    lines.extend(fmt_latest_obs(latest, prev))
    if prev:
        prev_local = _metar_obs_time_utc(prev).astimezone(tz)
        lines.append("")
        lines.append(f"上一报时间：{prev_local.strftime('%H:%M')} {time_label}")

    bias = None if fc_t is None else round(float(latest.get("temp", 0)) - float(fc_t), 2)
    p_bias = None if fc_p is None else round(float(latest.get("altim", 0)) - float(fc_p), 2)
    if bias is not None and p_bias is not None:
        lines.append(f"同小时模式偏差：Temp {bias:+.2f}°C, Pressure {p_bias:+.2f}hPa")

    t_trend = None
    if prev is not None:
        try:
            t_trend = float(latest.get("temp", 0)) - float(prev.get("temp", 0))
        except Exception:
            t_trend = None

    raw_latest = (latest.get("rawOb") or "")
    m_cloud = re.search(r"\b(FEW|SCT|BKN|OVC|VV)\d{3}\b", raw_latest)
    latest_cloud_code = m_cloud.group(1) if m_cloud else latest.get("cover")

    if str(latest_cloud_code or "").upper() in {"CLR", "CAVOK", "FEW", "SCT"}:
        cloud_hint = "云量约束偏弱"
    elif str(latest_cloud_code or "").upper() in {"BKN", "OVC", "VV"}:
        cloud_hint = "低云约束仍在"
    else:
        cloud_hint = "云量约束不确定"

    if isinstance(t_trend, (int, float)):
        if t_trend >= 0.5:
            trend_hint = "短时升温仍在延续"
        elif t_trend <= -0.5:
            trend_hint = "短时升温动能转弱"
        else:
            trend_hint = "短时温度基本横盘"
    else:
        trend_hint = "短时温度节奏待确认"

    lines.append("")
    lines.append(f"• 最新实况简评：{trend_hint}，{cloud_hint}。")

    wind_dir_change = None
    pressure_step = None
    if prev is not None:
        try:
            d1 = latest.get("wdir")
            d0 = prev.get("wdir")
            if d1 not in (None, "", "VRB") and d0 not in (None, "", "VRB"):
                a = abs(float(d1) - float(d0)) % 360.0
                wind_dir_change = min(a, 360.0 - a)
        except Exception:
            wind_dir_change = None
        try:
            pressure_step = float(latest.get("altim", 0)) - float(prev.get("altim", 0))
        except Exception:
            pressure_step = None

    # 3) 同小时模式偏差轨迹（最近2-3报）
    b0 = _hour_bias(latest)
    b1 = _hour_bias(prev) if prev else None
    b2 = _hour_bias(prev2) if prev2 else None
    # Bias trajectory line removed by operator preference: keep report concise.

    # 5) 高影响触发告警（仅触发时显示）
    alert = None
    ctrend = _cloud_trend(latest, prev)
    if isinstance(t_trend, (int, float)) and t_trend >= 0.5 and ("开窗" in ctrend or "减弱" in ctrend):
        alert = "⚠️ 实况触发：短时升温 + 云层转疏，窗口上沿风险上调。"
    elif isinstance(t_trend, (int, float)) and t_trend <= -0.5 and ("回补" in ctrend or "增加" in ctrend):
        alert = "⚠️ 实况触发：降温 + 云层增厚，峰值窗口可能提前结束。"
    if alert:
        lines.append("")
        lines.append(alert)


    observed_points: list[tuple[float, datetime]] = []
    # Use local target-date max (not rolling 24h max), to avoid cross-day contamination.
    target_local_date = None
    if target_date:
        try:
            target_local_date = datetime.strptime(target_date, "%Y-%m-%d").date()
        except Exception:
            target_local_date = None
    if target_local_date is None:
        target_local_date = latest_dt_local.date()

    for x in series:
        try:
            x_local_dt = _metar_obs_time_utc(x).astimezone(tz)
            if x_local_dt.date() != target_local_date:
                continue
            observed_points.append((float(x.get("temp")), x_local_dt))
        except Exception:
            pass
    obs_max_temp = max((p[0] for p in observed_points), default=None)
    obs_max_time_local = None
    if obs_max_temp is not None:
        cands = [dt for t, dt in observed_points if abs(t - obs_max_temp) < 1e-9]
        if cands:
            obs_max_time_local = max(cands)

    cloud_tr = _cloud_trend(latest, prev) if prev else ""
    peak_lock_confirmed = False
    try:
        if prev is not None and prev2 is not None:
            t1 = float(latest.get("temp", 0)) - float(prev.get("temp", 0))
            t0 = float(prev.get("temp", 0)) - float(prev2.get("temp", 0))
            peak_lock_confirmed = (t1 <= -0.2 and t0 <= -0.2)
    except Exception:
        peak_lock_confirmed = False

    return "\n".join(lines), {
        "latest_report_utc": _metar_obs_time_utc(latest).isoformat().replace('+00:00', 'Z'),
        "latest_report_local": latest_dt_local.isoformat(),
        "temp_bias_c": bias,
        "pressure_bias_hpa": p_bias,
        "latest_wdir": latest.get("wdir"),
        "latest_wspd": latest.get("wspd"),
        "latest_temp": latest.get("temp"),
        "latest_dewpoint": latest.get("dewp"),
        "latest_cloud_code": latest_cloud_code,
        "cloud_trend": cloud_tr,
        "temp_trend_1step_c": t_trend,
        "pressure_trend_1step_hpa": pressure_step,
        "wind_dir_change_deg": wind_dir_change,
        "peak_lock_confirmed": peak_lock_confirmed,
        "observed_max_temp_c": obs_max_temp,
        "observed_max_time_local": obs_max_time_local.isoformat() if obs_max_time_local else None,
    }


def _haversine_km(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    r = 6371.0
    p1 = math.radians(lat1)
    p2 = math.radians(lat2)
    dlat = math.radians(lat2 - lat1)
    dlon = math.radians(lon2 - lon1)
    a = math.sin(dlat / 2.0) ** 2 + math.cos(p1) * math.cos(p2) * math.sin(dlon / 2.0) ** 2
    return 2.0 * r * math.atan2(math.sqrt(a), math.sqrt(1.0 - a))


def run_synoptic_section(st: Station, target_date: str, peak_local: str, tz_name: str, model: str, runtime_tag: str) -> dict[str, Any]:
    from synoptic_runner import run_synoptic_section as _run

    return _run(
        st=st,
        target_date=target_date,
        peak_local=peak_local,
        tz_name=tz_name,
        model=model,
        runtime_tag=runtime_tag,
        scripts_dir=SCRIPTS_DIR,
        cache_dir=CACHE_DIR,
        provider=SYNOPTIC_PROVIDER,
        perf_log=_perf_log,
    )


def _poly_num(tok: str) -> int:
    t = str(tok).lower()
    if t.startswith("neg"):
        return -int(t[3:])
    return int(t)


def _poly_parse_interval(slug: str) -> tuple[float, float, str] | None:
    s = slug.lower()
    m = re.search(r"-(neg\d+|\d+)c$", s)
    if m:
        n = _poly_num(m.group(1))
        return (n - 0.5, n + 0.49, "C")
    m = re.search(r"-(neg\d+|\d+)corbelow$", s)
    if m:
        n = _poly_num(m.group(1))
        return (-math.inf, n + 0.49, "C")
    m = re.search(r"-(neg\d+|\d+)corhigher$", s)
    if m:
        n = _poly_num(m.group(1))
        return (n - 0.5, math.inf, "C")
    m = re.search(r"-(neg\d+|\d+)f$", s)
    if m:
        n = _poly_num(m.group(1))
        return (n - 0.5, n + 0.49, "F")
    m = re.search(r"-(neg\d+|\d+)forbelow$", s)
    if m:
        n = _poly_num(m.group(1))
        return (-math.inf, n + 0.49, "F")
    m = re.search(r"-(neg\d+|\d+)forhigher$", s)
    if m:
        n = _poly_num(m.group(1))
        return (n - 0.5, math.inf, "F")
    return None


def _poly_label(slug: str) -> str:
    s = slug.lower()
    for pat, fmt in [
        (r"-(neg\d+|\d+)c$", lambda n: f"{_poly_num(n)}°C"),
        (r"-(neg\d+|\d+)corbelow$", lambda n: f"{_poly_num(n)}°C or below"),
        (r"-(neg\d+|\d+)corhigher$", lambda n: f"{_poly_num(n)}°C or higher"),
        (r"-(neg\d+|\d+)f$", lambda n: f"{_poly_num(n)}°F"),
        (r"-(neg\d+|\d+)forbelow$", lambda n: f"{_poly_num(n)}°F or below"),
        (r"-(neg\d+|\d+)forhigher$", lambda n: f"{_poly_num(n)}°F or higher"),
    ]:
        m = re.search(pat, s)
        if m:
            return fmt(m.group(1))
    return slug


def _build_polymarket_section(polymarket_event_url: str, primary_window: dict[str, Any], metar_diag: dict[str, Any] | None = None) -> str:
    slug = polymarket_event_url.rstrip('/').split('/')[-1]
    r = requests.get("https://gamma-api.polymarket.com/events", params={"limit": 1, "slug": slug}, timeout=5)
    r.raise_for_status()
    arr = r.json()
    if not arr:
        return "Polymarket：未找到对应市场。"
    markets = arr[0].get("markets", [])

    parsed: list[tuple[float, str, Any, Any]] = []
    for m in markets:
        iv = _poly_parse_interval(str(m.get("slug", "")))
        if not iv:
            continue
        lo, hi, unit = iv
        if unit == "F":
            lo = (lo - 32) * 5 / 9 if lo != -math.inf else -math.inf
            hi = (hi - 32) * 5 / 9 if hi != math.inf else math.inf
        if math.isinf(lo) and not math.isinf(hi):
            center = hi - 0.5
        elif math.isinf(hi) and not math.isinf(lo):
            center = lo + 0.5
        elif math.isinf(lo) and math.isinf(hi):
            center = 999.0
        else:
            center = (lo + hi) / 2
        parsed.append((center, _poly_label(str(m.get("slug", ""))), m.get("bestBid"), m.get("bestAsk")))

    parsed.sort(key=lambda x: x[0])
    if not parsed:
        return "Polymarket：当前无可用盘口。"

    def _px(v: Any) -> float:
        try:
            return float(v)
        except Exception:
            return 0.0

    t_now = None if not metar_diag else metar_diag.get("latest_temp")
    try:
        t_now = float(t_now) if t_now is not None else None
    except Exception:
        t_now = None
    peak = float(primary_window.get("peak_temp_c") or 0.0)
    obs_max = None
    if metar_diag:
        try:
            obs_max = float(metar_diag.get("observed_max_temp_c")) if metar_diag.get("observed_max_temp_c") is not None else None
        except Exception:
            obs_max = None

    filtered = []
    for center, label, bid, ask in parsed:
        iv = None
        for m in markets:
            if _poly_label(str(m.get("slug", ""))) == label:
                iv = _poly_parse_interval(str(m.get("slug", "")))
                break
        lo = iv[0] if iv else center - 0.5
        hi = iv[1] if iv else center + 0.49
        # 硬性剔除：已低于实况录得最高温的区间，不再可能结算命中。
        if obs_max is not None and hi < obs_max:
            continue
        # 软剔除：明显低于当前温度太多的旧档位。
        if t_now is not None and hi < t_now - 1.0:
            continue
        filtered.append((center, label, bid, ask, lo, hi))

    if not filtered:
        filtered = [(c, l, b, a, c - 0.5, c + 0.49) for c, l, b, a in parsed]

    def _alpha_score(row: tuple[float, str, Any, Any, float, float]) -> float:
        c, _l, b, a, lo, hi = row
        ask = _px(a)
        bid = _px(b)
        price = max(bid, ask)
        proximity = max(0.0, 1.0 - abs(c - peak) / 3.0)
        cheap = max(0.0, 0.25 - ask) * 2.0 if ask > 0 else 0.0
        tradable = 0.3 if price >= 0.02 else 0.0
        stale_penalty = 0.0
        if t_now is not None and hi <= t_now + 0.3:
            stale_penalty = 1.2  # already near/under current temp, alpha weak for Tmax market
        return 0.55 * proximity + 0.30 * cheap + tradable - stale_penalty

    ranked = sorted(filtered, key=_alpha_score, reverse=True)

    likely_lo = peak - 0.8
    likely_hi = peak + 0.8

    def _overlap_or_near(row: tuple[float, str, Any, Any, float, float]) -> bool:
        _c, _l, _b, _a, lo, hi = row
        if hi < likely_lo - 0.6:
            return False
        if lo > likely_hi + 0.6:
            return False
        return True

    near_pool = [r for r in filtered if _overlap_or_near(r)]
    mismatch = False
    if near_pool:
        seed = sorted(near_pool, key=_alpha_score, reverse=True)[:5]
    else:
        mismatch = True
        # 若主带无直接匹配，向 below/above 边缘档位寻找最近可交易区间
        below = [r for r in filtered if r[5] <= likely_lo]
        above = [r for r in filtered if r[4] >= likely_hi]
        seed = []
        if below:
            seed.append(sorted(below, key=lambda x: x[5], reverse=True)[0])
        if above:
            seed.append(sorted(above, key=lambda x: x[4])[0])
        # 再补一个流动性较好的档位，避免只剩单边
        for r in ranked:
            if all(r[1] != s[1] for s in seed):
                seed.append(r)
                break

    # Build a continuous temperature range around the most relevant bins.
    finite = sorted([r for r in filtered if not (math.isinf(r[4]) or math.isinf(r[5]))], key=lambda x: x[0])
    if seed and finite:
        min_c = min(r[0] for r in seed)
        max_c = max(r[0] for r in seed)
        continuous = [r for r in finite if (min_c - 0.01) <= r[0] <= (max_c + 0.01)]
        if not continuous:
            continuous = seed
    else:
        continuous = seed

    # keep continuity by temperature order; avoid dropping middle bins (e.g. 13/14/15) due hard cap
    focus = sorted(continuous, key=lambda x: x[0])

    # If too many bins, keep a compact contiguous slice around peak center instead of naive head truncation.
    if len(focus) > 8:
        target_c = peak
        k = min(8, len(focus))
        best_i = 0
        best_d = 1e9
        for i in range(0, len(focus) - k + 1):
            mid = focus[i + k // 2][0]
            d = abs(mid - target_c)
            if d < best_d:
                best_d = d
                best_i = i
        focus = focus[best_i: best_i + k]

    # Backfill interior finite bins to keep interval continuity (debug fix for missing middle labels).
    if focus:
        finite_all = sorted([r for r in filtered if not (math.isinf(r[4]) or math.isinf(r[5]))], key=lambda x: x[0])
        if finite_all:
            cmin = min(r[0] for r in focus if not (math.isinf(r[4]) or math.isinf(r[5]))) if any(not (math.isinf(r[4]) or math.isinf(r[5])) for r in focus) else None
            cmax = max(r[0] for r in focus if not (math.isinf(r[4]) or math.isinf(r[5]))) if any(not (math.isinf(r[4]) or math.isinf(r[5])) for r in focus) else None
            if cmin is not None and cmax is not None:
                filler = [r for r in finite_all if (cmin - 0.01) <= r[0] <= (cmax + 0.01)]
                merged = []
                seen = set()
                for r in (focus + filler):
                    if r[1] in seen:
                        continue
                    seen.add(r[1])
                    merged.append(r)
                focus = sorted(merged, key=lambda x: x[0])

    # include edge bins only when market-vs-weather mismatch fallback is active.
    if mismatch and focus:
        low_edge = focus[0][4]
        edge_bins = [r for r in filtered if math.isinf(r[4]) and not math.isinf(r[5]) and (r[5] >= low_edge - 1.1)]
        if edge_bins:
            edge = sorted(edge_bins, key=lambda x: x[5])[-1]
            if all(edge[1] != r[1] for r in focus):
                focus = [edge] + focus

    if mismatch and focus:
        high_edge = focus[-1][5]
        upper_bins = [r for r in filtered if not math.isinf(r[4]) and math.isinf(r[5]) and (r[4] <= high_edge + 1.1)]
        if upper_bins:
            edge = sorted(upper_bins, key=lambda x: x[4])[0]
            if all(edge[1] != r[1] for r in focus):
                focus = focus + [edge]

    # Bridge finite gap to upper edge (e.g. ensure 15°C appears before 16°C or higher when available).
    if focus:
        finite_all = sorted([r for r in filtered if not (math.isinf(r[4]) or math.isinf(r[5]))], key=lambda x: x[0])
        has_upper_edge = any((not math.isinf(r[4]) and math.isinf(r[5])) for r in focus)
        finite_focus = [r for r in focus if not (math.isinf(r[4]) or math.isinf(r[5]))]
        if has_upper_edge and finite_focus and finite_all:
            max_fin = max(r[0] for r in finite_focus)
            # edge lower bound (approx center of last finite bin before edge)
            upper_los = [r[4] for r in focus if (not math.isinf(r[4]) and math.isinf(r[5]))]
            edge_lo = min(upper_los) if upper_los else None
            if edge_lo is not None:
                bridge = [r for r in finite_all if (max_fin + 0.99) <= r[0] <= (edge_lo + 0.01)]
                if bridge:
                    seen = {r[1] for r in focus}
                    for r in bridge:
                        if r[1] not in seen:
                            focus.append(r)
                            seen.add(r[1])
                    focus = sorted(focus, key=lambda x: x[0])

    # Ensure one upper-edge tail bin is visible when forecast upper bound is close to next discrete market bucket.
    # Example: likely_hi=13.4 should include 14°C as tail breakout watch.
    try:
        finite_all = sorted([r for r in filtered if not (math.isinf(r[4]) or math.isinf(r[5]))], key=lambda x: x[0])
        target_center = math.ceil(likely_hi)
        if likely_hi >= (target_center - 0.6):
            cand = [r for r in finite_all if abs(r[0] - target_center) <= 0.26]
            if cand:
                up = cand[0]
                if all(up[1] != r[1] for r in focus):
                    focus.append(up)
            else:
                # fallback: include nearest upper-edge bucket (e.g. "14°C or higher")
                upper_bins = [r for r in filtered if (not math.isinf(r[4]) and math.isinf(r[5]) and r[4] <= (target_center + 0.5))]
                if upper_bins:
                    up = sorted(upper_bins, key=lambda x: x[4])[0]
                    if all(up[1] != r[1] for r in focus):
                        focus.append(up)
            focus = sorted(focus, key=lambda x: x[0])
    except Exception:
        pass

    # Only mark "most likely" when lead is clear enough (avoid over-labeling in tight ranges).
    best_label = None
    if ranked:
        def _mkt_strength(row: tuple[float, str, Any, Any, float, float]) -> float:
            _c, _l, b, a, _lo, _hi = row
            return max(_px(b), _px(a))
        s1 = _mkt_strength(ranked[0])
        s2 = _mkt_strength(ranked[1]) if len(ranked) > 1 else 0.0
        if (s1 - s2) >= 0.08:
            best_label = ranked[0][1]

    # Non-settled markets should show at least 2-3 bins (main + adjacent), avoid single-bin squeeze.
    if len(focus) == 1:
        only = focus[0]
        bid_only = _px(only[2])
        ask_only = _px(only[3])
        settled = (bid_only >= 0.98 or ask_only >= 0.98)
        if not settled:
            finite_all = sorted([r for r in filtered if not (math.isinf(r[4]) or math.isinf(r[5]))], key=lambda x: x[0])
            center = only[0]
            left = [r for r in finite_all if r[0] < center]
            right = [r for r in finite_all if r[0] > center]
            expanded = []
            if left:
                expanded.append(left[-1])
            expanded.append(only)
            if right:
                expanded.append(right[0])
            if len(expanded) < 2:
                edges = [r for r in filtered if (math.isinf(r[4]) or math.isinf(r[5]))]
                if edges:
                    expanded.append(edges[0])
            focus = sorted({r[1]: r for r in expanded}.values(), key=lambda x: x[0])

    score_map = {(lbl, str(bid), str(ask)): _alpha_score((c, lbl, bid, ask, lo, hi)) for c, lbl, bid, ask, lo, hi in focus}

    lines = ["📈 **Polymarket 盘口与博弈**", "**博弈区间**"]
    if len(focus) == 1:
        only = focus[0]
        bid_only = _px(only[2])
        ask_only = _px(only[3])
        if bid_only >= 0.98 or ask_only >= 0.98:
            lines.append("  • ✅ 已定局：当前仅剩单一高置信可交易区间。")
    if mismatch:
        lines.append("  • 注：市场档位与气象主带存在错位，已按最近 below/above 边缘区间回退展示。")
    for _c, label, bid, ask, _lo, _hi in focus:
        ask_v = _px(ask)
        bid_txt = "None" if bid in (None, "") else str(bid)
        ask_txt = "None" if ask in (None, "") else str(ask)
        tag = ""
        if best_label and label == best_label:
            tag = "👍最有可能"
        else:
            # 潜在alpha是可选标记：仅在低价且综合评分足够高时显示。
            s = score_map.get((label, str(bid), str(ask)), 0.0)
            if ask_v > 0 and ask_v <= 0.12 and s >= 1.05:
                tag = "😇潜在Alpha"

        if tag:
            lines.append(f"  • **{label}（{tag}）：Bid {bid_txt} | Ask {ask_txt}**")
        else:
            lines.append(f"  • {label}：Bid {bid_txt} | Ask {ask_txt}")
    return "\n".join(lines)


def choose_section_text(primary_window: dict[str, Any], metar_text: str, metar_diag: dict[str, Any], polymarket_event_url: str, forecast_decision: dict[str, Any] | None = None) -> str:
    """Render-only section builder.

    Decision/diagnostics should come from forecast_pipeline; this function only translates
    structured outputs into report text.
    """

    def _hm(s: Any) -> str:
        try:
            dt = datetime.strptime(str(s), "%Y-%m-%dT%H:%M")
            return dt.strftime("%H:%M")
        except Exception:
            return str(s)

    fdec = forecast_decision if isinstance(forecast_decision, dict) else {}
    d = (fdec.get("decision") or {}) if isinstance(fdec, dict) else {}
    bg = (d.get("background") or fdec.get("background") or {}) if isinstance(fdec, dict) else {}
    quality = (fdec.get("quality") or {}) if isinstance(fdec, dict) else {}

    phase_mode = str(bg.get("phase_mode") or "实况主导")
    line500 = str(bg.get("line_500") or "高空背景信号有限。")
    line850 = str(bg.get("line_850") or "低层输送信号一般。")
    extra = str(bg.get("extra") or "")
    h700_summary = str((((fdec.get("features") or {}).get("h700") or {}).get("summary") if isinstance(fdec, dict) else "") or "")
    h925_summary = str((((fdec.get("features") or {}).get("h925") or {}).get("summary") if isinstance(fdec, dict) else "") or "")
    snd_thermo = ((((fdec.get("features") or {}).get("sounding") or {}).get("thermo") if isinstance(fdec, dict) else None) or {})
    cloud_code_now = str(metar_diag.get("latest_cloud_code") or "").upper()

    syn_lines = ["🧭 **环流背景**"]

    raw_obj = (d.get("object_3d_main") or {}) if isinstance(d, dict) else {}

    def _contains_any(text: str, keys: list[str]) -> bool:
        s = str(text or "")
        return any(k in s for k in keys)

    def _infer_regime_and_desc(otype: str, impact: str) -> tuple[str, str]:
        if ("front" in otype) or ("baroclinic" in otype) or _contains_any(extra + line850, ["锋", "锋生", "斜压"]):
            return "锋面活动主导", "锋区调整"
        if "dry_intrusion" in otype or _contains_any(extra, ["湿层", "低云", "封盖", "压制"]):
            return "稳定层约束主导", "低层受限"
        if _contains_any(line850, ["暖平流"]):
            return "平流主导", "暖平流抬升"
        if _contains_any(line850, ["冷平流"]):
            return "平流主导", "冷平流切入"
        if _contains_any(line500, ["槽", "抬升", "PVA", "涡度"]):
            return "动力抬升主导", "槽前触发"
        if impact == "background_only":
            return "弱信号背景", "背景噪声"
        return "混合主导", "混合扰动"

    candidates = (((fdec.get("features") or {}).get("objects_3d") or {}).get("candidates") or []) if isinstance(fdec, dict) else []

    def _conf_ord(x: str) -> int:
        return {"high": 3, "medium": 2, "low": 1}.get(str(x or "").lower(), 0)

    cov = None
    try:
        cov = float((quality or {}).get("synoptic_coverage")) if (quality or {}).get("synoptic_coverage") is not None else None
    except Exception:
        cov = None

    # If main object is low-confidence, do not force it as dominant.
    obj = dict(raw_obj) if isinstance(raw_obj, dict) else {}
    if cov is not None and cov < 0.5:
        obj = {}
    elif obj and _conf_ord(obj.get("confidence")) <= 1:
        alt = None
        for c in candidates:
            if not isinstance(c, dict):
                continue
            if _conf_ord(c.get("confidence")) >= 2:
                alt = c
                break
        if alt is not None:
            obj = dict(alt)
            obj["_promoted_from_candidate"] = True
        else:
            # no reliable candidate: fall back to "no clear dominant system"
            obj = {}

    def _candidate_groups() -> set[str]:
        gs: set[str] = set()
        for c in candidates[:4]:
            t = str((c or {}).get("type") or "").lower()
            if "advection" in t:
                gs.add("advection")
            elif "baroclinic" in t or "frontal" in t:
                gs.add("baroclinic")
            elif "dry_intrusion" in t or "subsidence" in t:
                gs.add("stability")
            elif "dynamic" in t or "trough" in t:
                gs.add("dynamic")
            elif "shear" in t:
                gs.add("shear")
        return gs

    def _interaction_note(gs: set[str]) -> str | None:
        if {"advection", "stability"}.issubset(gs):
            return "暖平流与稳定层约束并存，强度取决于云层能否持续开窗"
        if {"advection", "baroclinic"}.issubset(gs):
            return "输送与锋生叠加，主要影响峰值时段重排"
        if {"dynamic", "stability"}.issubset(gs):
            return "高空触发存在，但低层落地受约束"
        if {"baroclinic", "shear"}.issubset(gs):
            return "斜压与风切并行，局地变化节奏可能加快"
        return None

    def _regime_scores() -> dict[str, float]:
        s = {
            "advection": 0.0,
            "dynamic": 0.0,
            "stability": 0.0,
            "baroclinic": 0.0,
            "shear": 0.0,
        }

        txt850 = str(line850)
        txt500 = str(line500)
        txtx = str(extra)

        if _contains_any(txt850, ["暖平流", "冷平流", "平流"]):
            s["advection"] += 1.0
        if _contains_any(txt500, ["槽", "抬升", "PVA", "涡度"]):
            s["dynamic"] += 0.9
        if _contains_any(txtx, ["封盖", "压制", "湿层", "低云", "耦合偏弱"]):
            s["stability"] += 1.0
        if _contains_any(txtx + txt850, ["锋", "锋生", "斜压"]):
            s["baroclinic"] += 1.0
        if _contains_any(txtx + txt850, ["风切", "切换"]):
            s["shear"] += 0.7

        o = dict(obj) if isinstance(obj, dict) else {}
        if o:
            t = str(o.get("type") or "").lower()
            conf_boost = {"high": 1.2, "medium": 0.8, "low": 0.3}.get(str(o.get("confidence") or ""), 0.3)
            if "advection" in t:
                s["advection"] += conf_boost
            if "dynamic" in t or "trough" in t:
                s["dynamic"] += conf_boost
            if "dry_intrusion" in t or "subsidence" in t:
                s["stability"] += conf_boost
            if "baroclinic" in t or "front" in t:
                s["baroclinic"] += conf_boost
            if "shear" in t:
                s["shear"] += conf_boost

        for c in candidates[:4]:
            if not isinstance(c, dict):
                continue
            t = str(c.get("type") or "").lower()
            w = {"high": 0.45, "medium": 0.35, "low": 0.12}.get(str(c.get("confidence") or ""), 0.1)
            if "advection" in t:
                s["advection"] += w
            if "dynamic" in t or "trough" in t:
                s["dynamic"] += w
            if "dry_intrusion" in t or "subsidence" in t:
                s["stability"] += w
            if "baroclinic" in t or "front" in t:
                s["baroclinic"] += w
            if "shear" in t:
                s["shear"] += w

        return s

    def _regime_label(k: str) -> str:
        return {
            "advection": "平流输送",
            "dynamic": "高空动力触发",
            "stability": "低层稳定约束",
            "baroclinic": "锋面/斜压调整",
            "shear": "风切节奏扰动",
        }.get(k, k)

    def _dir_cn_from_deg(deg: float) -> str:
        dirs = ["北", "东北", "东", "东南", "南", "西南", "西", "西北"]
        idx = int(((deg % 360) + 22.5) // 45) % 8
        return dirs[idx]

    def _front_plain_desc(otype: str) -> str | None:
        is_front = ("front" in otype) or ("baroclinic" in otype) or ("锋" in str(line850)) or ("锋" in str(extra))
        if not is_front:
            return None

        warm = "暖平流" in str(line850)
        cold = "冷平流" in str(line850)
        if warm and not cold:
            nature = "偏暖锋"
        elif cold and not warm:
            nature = "偏冷锋"
        elif warm and cold:
            nature = "冷暖交汇（近静止锋）"
        else:
            nature = "锋性过渡"

        wdir = metar_diag.get("latest_wdir")
        wspd = metar_diag.get("latest_wspd")
        try:
            wspd_v = float(wspd)
        except Exception:
            wspd_v = None

        if wdir in (None, "", "VRB") or wspd_v is None or wspd_v <= 4:
            move = "移动偏慢，接近准静止"
        else:
            try:
                to_deg = (float(wdir) + 180.0) % 360.0
                move = f"可能向{_dir_cn_from_deg(to_deg)}方向缓慢推进（低置信）"
            except Exception:
                move = "移动方向暂不稳定"

        return f"{nature}；{move}"

    def _system_plain_desc(otype: str) -> str | None:
        fd = _front_plain_desc(otype)
        if fd:
            return fd

        txt850 = str(line850)
        txtx = str(extra)

        if ("advection" in otype) or ("暖平流" in txt850) or ("冷平流" in txt850):
            if "暖平流" in txt850 and "冷平流" not in txt850:
                return "暖空气输送为主，云量若放开，升温会更顺"
            if "冷平流" in txt850 and "暖平流" not in txt850:
                return "冷空气输送偏强，对升温有压制"
            return "冷暖输送并存，短时更容易出现重排"

        if ("dry_intrusion" in otype) or _contains_any(txtx, ["封盖", "湿层", "低云", "压制", "干层"]):
            if _contains_any(txtx, ["干层", "日照", "升温加速"]):
                return "高层偏干，若日照打开，升温会突然加速"
            return "低层受封盖约束，短时升温不容易放大"

        if ("dynamic" in otype) or _contains_any(str(line500), ["槽", "抬升", "涡度", "PVA"]):
            return "高空有触发信号，但是否落地还要看近地风云配合"

        if ("shear" in otype):
            return "风场切换型系统，节奏变化快，峰值时段易前后摆动"

        if ("subsidence" in otype):
            return "下沉背景偏强，整体更偏稳态"

        return None

    def _signal_scores() -> tuple[float, float, str]:
        up = 0.0
        down = 0.0

        if "暖平流" in line850:
            up += 1.0
        if "冷平流" in line850:
            down += 1.0

        if _contains_any(extra, ["封盖", "压制", "湿层", "低云"]):
            down += 1.0
        if _contains_any(extra, ["干层", "日照", "升温加速"]):
            up += 0.8

        try:
            b = float(metar_diag.get("temp_bias_c")) if metar_diag.get("temp_bias_c") is not None else 0.0
        except Exception:
            b = 0.0
        if b >= 0.8:
            up += 0.6
        elif b <= -0.8:
            down += 0.6

        ctrend = str(metar_diag.get("cloud_trend") or "")
        if ("增加" in ctrend) or ("回补" in ctrend):
            down += 0.5
        if ("开窗" in ctrend) or ("减弱" in ctrend):
            up += 0.5

        phase = str(classify_window_phase(primary_window, metar_diag).get("phase") or "unknown")
        return up, down, phase

    def _evidence_routes() -> tuple[str, str]:
        # system route
        sys_score = 0.0
        if obj:
            sys_score += {"high": 1.1, "medium": 0.8, "low": 0.35}.get(str(obj.get("confidence") or ""), 0.2)
            sys_score += {"station_relevant": 0.8, "possible_override": 0.45, "background_only": 0.2}.get(str(obj.get("impact_scope") or ""), 0.2)
        else:
            try:
                rmax = max(_regime_scores().values())
            except Exception:
                rmax = 0.0
            if rmax >= 0.9:
                sys_score += 0.45

        # profile route
        profile_score = 0.0
        if h700_summary:
            profile_score += 0.7
            if "近站" in h700_summary:
                profile_score += 0.45
            elif "外围" in h700_summary:
                profile_score += 0.2
        if h925_summary:
            profile_score += 0.35
            if "偏弱" in h925_summary:
                profile_score -= 0.15
        if snd_thermo.get("has_profile"):
            profile_score += 0.35

        # obs route
        obs_score = 0.0
        try:
            tb = abs(float(metar_diag.get("temp_bias_c") or 0.0))
        except Exception:
            tb = 0.0
        try:
            ts = abs(float(metar_diag.get("temp_trend_1step_c") or 0.0))
        except Exception:
            ts = 0.0
        if tb >= 1.5:
            obs_score += 0.9
        elif tb >= 0.8:
            obs_score += 0.5
        if ts >= 0.8:
            obs_score += 0.55
        elif ts >= 0.4:
            obs_score += 0.3

        routes = [
            ("系统路由", "system", sys_score),
            ("剖面路由", "profile", profile_score),
            ("实况路由", "obs", obs_score),
        ]
        routes.sort(key=lambda x: x[2], reverse=True)
        main = f"{routes[0][0]}({routes[0][1]})"
        aux = f"{routes[1][0]}({routes[1][1]})" if routes[1][2] >= 0.45 else "无明显次级路由"
        return main, aux

    def _impact_direction_and_trigger() -> tuple[str, str]:
        up, down, phase = _signal_scores()

        if abs(up - down) < 0.55:
            direction = "暂时看不出明显偏高或偏低"
        elif up > down:
            direction = "更可能比原先预报略高"
        else:
            direction = "更可能比原先预报略低"

        if phase in {"near_window", "in_window"}:
            trigger = "临窗重点看云量开合和风向变化"
        elif phase == "far":
            trigger = "先看升温是否能连续走强"
        else:
            trigger = "重点看温度斜率与云量是否突变"

        return direction, trigger

    direction_txt, trigger_txt = _impact_direction_and_trigger()
    cgroups = _candidate_groups()
    inter_note = _interaction_note(cgroups)

    rs = _regime_scores()
    r_sorted = sorted(rs.items(), key=lambda x: x[1], reverse=True)
    r1, s1 = r_sorted[0]
    r2, s2 = r_sorted[1]
    has_primary_regime = s1 >= 0.9
    route_main, route_aux = _evidence_routes()

    if obj:
        otype = str(obj.get("type") or "").lower()
        impact = str(obj.get("impact_scope") or "background_only")
        regime, desc = _infer_regime_and_desc(otype, impact)

        # 1) 主导系统（一句话）
        if has_primary_regime:
            syn_lines.append(f"- **主导系统**：{_regime_label(r1)}（{regime}）。")
        else:
            syn_lines.append(f"- **主导系统**：{regime}（{desc}）。")
        sys_desc = _system_plain_desc(otype)
        if sys_desc:
            syn_lines.append(f"- **系统性质**：{sys_desc}。")

        # 2) 次级系统 / 冲突（仅证据足够时展示）
        if s2 >= 0.8 and (s1 - s2) <= 0.55:
            syn_lines.append(f"- **次级系统**：{_regime_label(r2)}。")
        if {"advection", "stability"}.issubset(cgroups):
            syn_lines.append("- **冲突项**：升温输送与低层约束并存，临窗可能出现节奏反复。")

        # 3) 落地影响（方向 + 触发 + 交互）
        if impact == "station_relevant":
            scope_txt = "系统近站，影响将直接落在峰值窗"
        elif impact == "possible_override":
            scope_txt = "系统在外围，主要改写峰值时段"
        else:
            scope_txt = "当前以背景场为主，短时改写概率有限"

        impact_line = f"{direction_txt}；{scope_txt}"
        if inter_note:
            impact_line += f"。当前组合关系：{inter_note}"
        impact_line += f"。建议：{trigger_txt}。"
        syn_lines.append(f"- **落地影响**：{impact_line}")

    else:
        if has_primary_regime:
            syn_lines.append(f"- **主导系统**：{_regime_label(r1)}（结构未闭合，暂不立3D主系统）。")
            if s2 >= 0.8 and (s1 - s2) <= 0.55:
                syn_lines.append(f"- **次级系统**：{_regime_label(r2)}。")
            if {"advection", "stability"}.issubset(cgroups):
                syn_lines.append("- **冲突项**：升温输送与低层约束并存，临窗可能出现节奏反复。")
        else:
            syn_lines.append("- **主导系统**：当前未识别到可稳定追踪的同一套分层系统。")

        tail = f"。当前组合关系：{inter_note}" if inter_note else ""
        syn_lines.append(f"- **落地影响**：{direction_txt}；短时以实况触发为主。建议：{trigger_txt}{tail}。")

    syn_lines.append(f"- **证据路由**：主判 {route_main}；辅助 {route_aux}。")

    # concise evidence line (avoid spreading full layer-by-layer by default)
    def _humanize_850(s: str) -> str:
        txt = str(s or "")
        m = re.search(r"(暖平流|冷平流)([^（]*)（([0-9.]+)，([^）]+)）", txt)
        if m:
            kind = m.group(1)
            conf_raw = float(m.group(3))
            if conf_raw >= 0.67:
                conf = "高"
            elif conf_raw >= 0.34:
                conf = "中"
            else:
                conf = "低"
            eta = m.group(4)
            return f"{kind}（置信度{conf}，可能影响时间{eta}）"
        return txt

    line850_h = _humanize_850(line850)

    def _is_weak_evidence(s: str) -> bool:
        t = str(s or "")
        weak_tokens = ["信号一般", "信号有限", "中性", "背景", "不明", "弱"]
        return any(k in t for k in weak_tokens)

    evidence_bits: list[str] = []
    if line850_h and not _is_weak_evidence(line850_h):
        evidence_bits.append(f"850hPa: {line850_h}")

    if h700_summary and not _is_weak_evidence(h700_summary):
        evidence_bits.append(f"700hPa: {h700_summary}")

    if line500 and not _is_weak_evidence(line500):
        evidence_bits.append(f"500hPa: {line500}")

    if h925_summary and not _is_weak_evidence(h925_summary):
        evidence_bits.append(f"925hPa: {h925_summary}")

    if extra and not _is_weak_evidence(extra):
        evidence_bits.append(f"约束: {extra}")

    if evidence_bits:
        syn_lines.append("- **关键证据**：")
        for e in evidence_bits[:3]:
            syn_lines.append(f"  • {e}")

    def _sounding_layer_note() -> str | None:
        bits: list[str] = []

        if h700_summary:
            if "干层" in h700_summary:
                bits.append("中层(600-700hPa)偏干")
            elif ("湿层" in h700_summary) or ("约束" in h700_summary):
                bits.append("中层(700hPa)湿层约束")

        if snd_thermo.get("has_profile"):
            capev = snd_thermo.get("sbcape_jkg") or snd_thermo.get("mlcape_jkg") or snd_thermo.get("mucape_jkg")
            cinv = snd_thermo.get("sbcin_jkg") if snd_thermo.get("sbcin_jkg") is not None else snd_thermo.get("mlcin_jkg")
            if isinstance(capev, (int, float)):
                bits.append(f"对流能量 CAPE≈{float(capev):.0f}J/kg")
            if isinstance(cinv, (int, float)):
                bits.append(f"抑制 CIN≈{float(cinv):.0f}J/kg")

        if cloud_code_now in {"BKN", "OVC", "VV"}:
            bits.append("当前低层云量偏多（地面辐射受限）")

        if not bits:
            return None
        return "；".join(bits[:2]) + "。"

    snd_note = _sounding_layer_note()
    if snd_note:
        syn_lines.append(f"- **探空层提示**：{snd_note}")

    if str(d.get("override_risk") or "low") == "high":
        syn_lines.append("- **改写风险**：中到高，窗口前后需盯实况触发。")

    if str(quality.get("source_state") or "") == "degraded":
        cov_txt = ""
        try:
            if quality.get("synoptic_coverage") is not None:
                cov_txt = f"；coverage={float(quality.get('synoptic_coverage')):.2f}"
        except Exception:
            cov_txt = ""
        syn_lines.append(f"- **数据状态**：环流链路降级（结论偏保守{cov_txt}）。")

    syn_lines.append(
        f"- **峰值窗口**：{_hm(primary_window.get('start_local'))}~{_hm(primary_window.get('end_local'))} Local。"
    )

    metar_prefix = []
    try:
        if metar_diag and metar_diag.get("observed_max_temp_c") is not None:
            mx = float(metar_diag.get('observed_max_temp_c'))
            mx_txt = f"{int(mx)}" if float(mx).is_integer() else f"{mx:.1f}"
            tmax_local = str(metar_diag.get("observed_max_time_local") or "")
            tmax_txt = ""
            if tmax_local:
                try:
                    tmax_txt = datetime.fromisoformat(tmax_local).strftime("%H:%M Local")
                except Exception:
                    tmax_txt = ""
            if tmax_txt:
                metar_prefix.append(f"• 今日已观测最高温：{mx_txt}°C（{tmax_txt}）")
            else:
                metar_prefix.append(f"• 今日已观测最高温：{mx_txt}°C")
    except Exception:
        pass
    metar_block = "📡 **最新实况分析（METAR）**\n" + ("\n".join(metar_prefix + [metar_text]) if metar_prefix else metar_text)

    peak_c = float(primary_window.get('peak_temp_c') or 0.0)
    obs_max = None
    try:
        obs_max = float(metar_diag.get('observed_max_temp_c')) if metar_diag.get('observed_max_temp_c') is not None else None
    except Exception:
        obs_max = None

    gate = classify_window_phase(primary_window, metar_diag)
    phase_now = str(gate.get('phase') or 'unknown')

    # Quantization-aware dynamic range (METAR usually integer-rounded; avoid overfitting tiny jumps)
    # Target: main-band (majority scenarios) + optional tail-risk note.
    u = 0.0
    q_state = str((quality or {}).get("source_state") or "")
    if q_state in {"degraded", "fallback-cache"}:
        u += 0.22

    if phase_now == "far":
        u += 0.12
    elif phase_now == "near_window":
        u += 0.06
    elif phase_now == "in_window":
        u -= 0.06
    elif phase_now == "post":
        u += 0.03

    conf = str((obj or {}).get("confidence") or "")
    if conf == "high":
        u -= 0.12
    elif conf == "low":
        u += 0.12
    elif not conf:
        u += 0.08

    imp = str((obj or {}).get("impact_scope") or "")
    if imp == "station_relevant":
        u -= 0.08
    elif imp == "possible_override":
        u += 0.06
    elif imp == "background_only":
        u += 0.12

    try:
        b = float(metar_diag.get("temp_bias_c") or 0.0)
        babs = abs(b)
    except Exception:
        b = 0.0
        babs = 0.0
    if babs >= 2.0:
        u += 0.15
    elif babs >= 1.0:
        u += 0.08

    try:
        wchg = float(metar_diag.get("wind_dir_change_deg") or 0.0)
    except Exception:
        wchg = 0.0
    if wchg >= 45:
        u += 0.06

    half_range = min(1.25, max(0.55, 0.75 + u))

    # center = model baseline + observed deviation (window-weighted) + excess-bias correction
    center = float(peak_c)
    try:
        tstep = float(metar_diag.get("temp_trend_1step_c") or 0.0)
    except Exception:
        tstep = 0.0
    if obs_max is not None:
        obs_proj = float(obs_max) + max(0.0, min(0.9, tstep * 0.35))
        w_center = {
            "far": 0.20,
            "near_window": 0.38,
            "in_window": 0.58,
            "post": 0.70,
        }.get(phase_now, 0.26)
        center = (1 - w_center) * center + w_center * obs_proj

    # avoid double-counting model priced-in move: only use excess bias above tolerance.
    excess_up = max(0.0, b - 0.9)
    excess_dn = max(0.0, -b - 0.9)
    center += min(0.45, 0.18 * excess_up)
    center -= min(0.45, 0.18 * excess_dn)

    up_s, down_s, _ = _signal_scores()
    denom = max(1e-6, up_s + down_s)
    skew = max(-0.8, min(0.8, (up_s - down_s) / denom))

    major_half = min(1.05, max(0.45, half_range * 0.68))
    left_hw = major_half * (1.0 - 0.35 * skew)
    right_hw = major_half * (1.0 + 0.35 * skew)

    lo = center - left_hw
    hi = center + right_hw
    if obs_max is not None:
        lo = max(lo, float(obs_max) - 0.25)

    def _soft_snap(v: float) -> float:
        iv = round(v)
        if abs(v - iv) <= 0.12:
            return float(iv)
        return round(v, 1)

    lo = _soft_snap(lo)
    hi = _soft_snap(max(hi, lo + 0.2))

    peak_range_block = [
        "🌡️ **可能最高温区间**",
        f"- **主带 {lo:.1f}~{hi:.1f}°C**（峰值窗 {_hm(primary_window.get('start_local'))}~{_hm(primary_window.get('end_local'))} Local）",
        "- 注：主带覆盖大多数情景，不包含极端尾部。",
    ]

    if skew >= 0.20:
        tail_hi = _soft_snap(hi + min(0.8, 0.4 + 0.3 * max(0.0, skew)))
        peak_range_block.append(f"- 尾部上破风险：若云量继续开窗且升温斜率延续，最高温可触及 **{hi:.1f}~{tail_hi:.1f}°C**。")
    elif skew <= -0.20:
        tail_lo = _soft_snap(max(lo - min(0.8, 0.4 + 0.3 * max(0.0, -skew)), lo - 1.0))
        peak_range_block.append(f"- 尾部下探风险：若云量回补并伴随偏冷来流增强，最高温可能回落到 **{tail_lo:.1f}~{lo:.1f}°C**。")
    if bool(metar_diag.get("obs_correction_applied")):
        peak_range_block.append("- 注：已应用实况纠偏（模型峰值偏低，窗口锚定到当日实况峰值时段）。")

    phase_map = {"far": "远离窗口", "near_window": "接近窗口", "in_window": "窗口内", "post": "窗口后", "unknown": "窗口状态未知"}
    vars_block = [f"⚠️ **关注变量**（{phase_map.get(phase_now, '窗口状态未知')}）"]
    cloud_code = str(metar_diag.get("latest_cloud_code") or "").upper()
    t_bias = metar_diag.get("temp_bias_c")
    if cloud_code in {"BKN", "OVC", "VV"}:
        vars_block.append("• 低云维持/继续增厚 → 最高温上沿下压，峰值可能提前结束。")
    else:
        vars_block.append("• 云量继续开窗 → 地面增温效率抬升，最高温上沿有上修空间。")

    try:
        wdir = metar_diag.get("latest_wdir")
        wspd = metar_diag.get("latest_wspd")
        if wdir not in (None, "", "VRB") and wspd is not None:
            vars_block.append(f"• 近地风若从当前风场（{wdir}° {wspd}kt）明显转向/增风 → 峰值时段与幅度都可能重排。")
        else:
            vars_block.append("• 近地风场若由不定转为稳定单一来流 → 峰值时段可能后移并抬升。")
    except Exception:
        vars_block.append("• 近地风向/风速若突变 → 峰值出现时段与幅度可能改写。")

    if isinstance(t_bias, (int, float)):
        if t_bias >= 1.5:
            vars_block.append("• 实况持续高于同小时模式（偏暖延续） → 最高温更偏上沿。")
        elif t_bias <= -1.5:
            vars_block.append("• 实况持续低于同小时模式（偏冷延续） → 最高温更偏下沿。")
        else:
            vars_block.append("• 未来30-60分钟温度斜率若重新转正并放大 → 仍可小幅上修最高温。")
    else:
        vars_block.append("• 未来30-60分钟温度斜率若持续为正 → 仍有上修空间。")

    try:
        if snd_thermo.get("has_profile"):
            capev = snd_thermo.get("sbcape_jkg") or snd_thermo.get("mlcape_jkg") or snd_thermo.get("mucape_jkg")
            cinv = snd_thermo.get("sbcin_jkg") if snd_thermo.get("sbcin_jkg") is not None else snd_thermo.get("mlcin_jkg")
            if isinstance(capev, (int, float)) and capev >= 300 and (not isinstance(cinv, (int, float)) or cinv > -75):
                vars_block.append("• 探空显示可用对流能量且抑制偏弱 → 午后云量/阵性扰动上升，峰值波动风险增加。")
            elif isinstance(cinv, (int, float)) and cinv <= -125:
                vars_block.append("• 探空抑制偏强（CIN较大） → 云对流触发受限，升温路径更看近地风云条件。")
    except Exception:
        pass

    # P1 short-term triggers (window-gated)
    try:
        rt_triggers = select_realtime_triggers(primary_window, metar_diag)
        phase_now = str(gate.get("phase") or "unknown")
        if rt_triggers:
            vars_block = vars_block[:1] + rt_triggers[:3]
        elif phase_now == "far":
            vars_block = [
                vars_block[0],
                "• 当前远离峰值窗口：先跟踪上午到中午的升温斜率是否连续转正。",
                "• 临窗前关键观察：云量是否由SCT走向BKN/OVC（压制）或继续开窗（上修）。",
                "• 进入窗口前1-2小时再重点评估：风向切换与偏差漂移是否触发改判。",
            ]
        else:
            vars_block = vars_block[:4]
    except Exception:
        vars_block = vars_block[:4]

    try:
        poly_block = _build_polymarket_section(polymarket_event_url, primary_window, metar_diag=metar_diag)
    except Exception:
        poly_block = "📈 **Polymarket 盘口与博弈**\n盘口读取异常，请稍后重试。"

    return "\n\n".join([
        "\n".join(syn_lines),
        metar_block,
        "\n".join(peak_range_block),
        "\n".join(vars_block),
        poly_block,
    ])

def _is_openmeteo_rate_limited_error(exc: Exception) -> bool:
    msg = str(exc)
    return ("429" in msg) or ("Too Many Requests" in msg) or ("open-meteo breaker active" in msg)


def _render_metar_only_report(st: Station, model: str, links_payload: dict[str, Any], reason: str, tz_name: str) -> str:
    metar24 = fetch_metar_24h(st.icao)
    metar_text, _metar_diag = metar_observation_block(
        metar24,
        {"time": [], "temperature_2m": [], "pressure_msl": []},
        tz_name,
    )

    lat_hemi = "N" if st.lat >= 0 else "S"
    lon_hemi = "E" if st.lon >= 0 else "W"
    rt = str(links_payload.get("runtime_utc") or "")
    rt_fmt = rt
    if len(rt) == 10 and rt.isdigit():
        rt_fmt = f"{rt[0:4]}/{rt[4:6]}/{rt[6:8]} {rt[8:10]}Z"

    now_utc = datetime.now(timezone.utc)
    tz = ZoneInfo(tz_name)
    now_local = now_utc.astimezone(tz)
    header = (
        f"📍 **{st.icao} ({st.city}) | {abs(st.lat):.4f}{lat_hemi}, {abs(st.lon):.4f}{lon_hemi}**\n"
        f"判断时间: {now_utc.strftime('%Y-%m-%d %H:%M')} UTC | {now_local.strftime('%Y-%m-%d %H:%M')} Local ({format_utc_offset(now_local)})\n"
        f"分析基准模型: {model.upper()}（运行时次: {rt_fmt}）"
    )

    pseudo_peak = float(_metar_diag.get("latest_temp") or 0.0)
    pseudo_window = {
        "peak_temp_c": pseudo_peak,
        "start_local": now_utc.strftime("%Y-%m-%dT%H:%M"),
        "end_local": now_utc.strftime("%Y-%m-%dT%H:%M"),
    }
    try:
        poly_block = _build_polymarket_section(links_payload["links"]["polymarket_event"], pseudo_window, metar_diag=_metar_diag)
    except Exception:
        poly_block = "📈 **Polymarket 盘口与博弈**\n盘口读取异常，请稍后重试。"

    body = (
        "📡 **最新实况分析（METAR-only 降级）**\n"
        f"- 触发原因：{reason}\n"
        "- 说明：Open-Meteo 当前不可用，已降级为实况-only 输出；背景/窗口判断暂不展开。\n\n"
        f"{metar_text}\n\n{poly_block}"
    )

    links = links_payload["links"]
    footer = (
        f"🔗[Polymarket]({links['polymarket_event']}) | "
        f"[METAR]({links['metar_24h']}) | "
        f"[Wunderground]({links['wunderground']}) | "
        f"[探空图（Tropicaltidbits）]({links['sounding_tropicaltidbits']})"
    )
    return f"{header}\n\n{body}\n\n{footer}"


def render_report(command_text: str) -> str:
    req_start_utc = datetime.now(timezone.utc)
    t_e2e = time.perf_counter()
    perf_local: dict[str, float] = {}

    def _mark(stage: str, seconds: float) -> None:
        perf_local[stage] = round(float(seconds), 3)
        _perf_log(stage, seconds)

    _prune_runtime_cache()
    params = parse_telegram_command(command_text)
    if params.get("cmd") == "lookhelp":
        return render_look_help()
    if params.get("cmd") != "look":
        raise ValueError("Unsupported command. Use /look or /lookhelp")

    station_hint = params.get("station")
    if not station_hint:
        return render_look_help()
    if str(station_hint).strip().lower() in {"help", "帮助", "h"}:
        return render_look_help()

    try:
        st = resolve_station(station_hint)
    except ValueError as exc:
        return f"{exc}\n\n{render_look_help()}"
    tz_name_station = station_timezone_name(st)

    raw_target_date = params.get("date")
    if raw_target_date and len(raw_target_date) == 8 and raw_target_date.isdigit():
        raw_target_date = f"{raw_target_date[0:4]}-{raw_target_date[4:6]}-{raw_target_date[6:8]}"
    target_date = raw_target_date or datetime.now(timezone.utc).strftime("%Y-%m-%d")
    try:
        datetime.strptime(target_date, "%Y-%m-%d")
    except ValueError as exc:
        raise ValueError("date must be YYYY-MM-DD (or YYYYMMDD)") from exc

    # 统一输出：固定走简版主报告（不再支持 mode/section/model/provider 参数）。
    model = default_model_for_station(st).lower()
    if model not in {"gfs", "ecmwf"}:
        model = "gfs"
    provider = "auto"

    t0 = time.perf_counter()
    provider_used = "unknown"
    try:
        om, provider_used = fetch_hourly_router(st, target_date, model, provider=provider)
    except Exception as exc:
        # open-meteo rate-limit OR gfs provider unavailable/rate-limited => degrade to METAR-only
        if _is_openmeteo_rate_limited_error(exc) or "gfs" in str(exc).lower() or "429" in str(exc):
            fallback_valid_utc = datetime.strptime(target_date, "%Y-%m-%d").replace(tzinfo=timezone.utc) + timedelta(hours=12)
            degrade_model = "gfs" if provider in {"auto", "gfs", "gfs-grib2", "grib2"} else model
            links_payload = BSL.build_links(
                row=BSL.load_station(STATION_CSV, st.icao),
                model=degrade_model,
                now_utc=datetime.now(timezone.utc),
                target_valid_utc=fallback_valid_utc,
                target_date_utc=datetime.strptime(target_date, "%Y-%m-%d").replace(tzinfo=timezone.utc),
            )
            _mark("hourly_fetch", time.perf_counter() - t0)
            return _render_metar_only_report(st, degrade_model, links_payload, reason=f"{provider} provider degraded: {exc}", tz_name=tz_name_station)
        raise
    _mark("hourly_fetch", time.perf_counter() - t0)

    global SYNOPTIC_PROVIDER
    # Strategy: hourly forecast prefers open-meteo; 3D field prefers gfs-grib2 by default.
    pref_3d = (os.getenv("FORECAST_3D_PROVIDER", "gfs-grib2") or "gfs-grib2").strip().lower()
    SYNOPTIC_PROVIDER = "gfs-grib2" if pref_3d in {"gfs", "gfs-grib2", "grib2"} else provider_used

    tz_name = tz_name_station or om.get("timezone", "UTC")
    hourly_day = slice_hourly_local_day(om["hourly"], target_date)
    windows, primary, peak_candidates = detect_tmax_windows(hourly_day)
    if not primary:
        raise RuntimeError("No Tmax window detected from forecast hourly data")

    tz = ZoneInfo(tz_name)

    t0 = time.perf_counter()
    metar24 = fetch_metar_24h(st.icao)
    metar_text, metar_diag = metar_observation_block(metar24, hourly_day, tz_name, target_date=target_date)
    _mark("metar_fetch_parse", time.perf_counter() - t0)

    # 实况纠偏：当实况显著高于模型峰值时，优先以实况峰值时段重锚窗口。
    try:
        obs_max = float(metar_diag.get("observed_max_temp_c")) if metar_diag.get("observed_max_temp_c") is not None else None
    except Exception:
        obs_max = None
    try:
        model_peak = float(primary.get("peak_temp_c"))
    except Exception:
        model_peak = None

    if obs_max is not None and model_peak is not None and (obs_max - model_peak) >= 1.5:
        tmax_local_s = str(metar_diag.get("observed_max_time_local") or metar_diag.get("latest_report_local") or "")
        try:
            tmax_local = datetime.fromisoformat(tmax_local_s)
        except Exception:
            tmax_local = datetime.now(tz)
        sdt = tmax_local - timedelta(hours=1)
        edt = tmax_local + timedelta(hours=2)
        primary["start_local"] = sdt.strftime("%Y-%m-%dT%H:%M")
        primary["end_local"] = edt.strftime("%Y-%m-%dT%H:%M")
        primary["peak_local"] = tmax_local.strftime("%Y-%m-%dT%H:%M")
        primary["peak_temp_c"] = max(model_peak, obs_max)
        metar_diag["obs_correction_applied"] = True

    peak_local_dt = datetime.strptime(primary["peak_local"], "%Y-%m-%dT%H:%M").replace(tzinfo=tz)
    peak_utc = peak_local_dt.astimezone(timezone.utc)

    analysis_model = model
    link_model = "gfs" if SYNOPTIC_PROVIDER == "gfs-grib2" else analysis_model
    links_payload = BSL.build_links(
        row=BSL.load_station(STATION_CSV, st.icao),
        model=link_model,
        now_utc=datetime.now(timezone.utc),
        target_valid_utc=peak_utc,
        target_date_utc=datetime.strptime(target_date, "%Y-%m-%d").replace(tzinfo=timezone.utc),
    )

    now_utc = datetime.now(timezone.utc)
    now_local = now_utc.astimezone(tz)

    t0 = time.perf_counter()
    forecast_decision, _synoptic, _synoptic_error = load_or_build_forecast_decision(
        station=st,
        target_date=target_date,
        model=model,
        synoptic_provider=SYNOPTIC_PROVIDER,
        now_utc=now_utc,
        now_local=now_local,
        station_lat=st.lat,
        station_lon=st.lon,
        primary_window=primary,
        tz_name=tz_name,
        run_synoptic_fn=run_synoptic_section,
        perf_log=_mark,
    )
    forecast_elapsed = time.perf_counter() - t0
    _mark("forecast_pipeline", forecast_elapsed)
    lat_hemi = "N" if st.lat >= 0 else "S"
    lon_hemi = "E" if st.lon >= 0 else "W"
    rt = str(links_payload.get("runtime_utc") or "")
    rt_fmt = rt
    if len(rt) == 10 and rt.isdigit():
        rt_fmt = f"{rt[0:4]}/{rt[4:6]}/{rt[6:8]} {rt[8:10]}Z"

    fc_cache = perf_local.get("forecast.cache_read", 0.0)
    fc_syn = perf_local.get("forecast.synoptic_build", 0.0)
    fc_dec = perf_local.get("forecast.decision_build", 0.0)
    fc_write = perf_local.get("forecast.cache_write", 0.0)
    fc_fallback = perf_local.get("forecast.synoptic_fallback_cache", 0.0)

    terrain_tag = _terrain_tag_for(st.icao)
    site_tag = _site_tag_for(st.icao)
    direction_factor = _direction_factor_for(st.icao)
    head_geo = f"{abs(st.lat):.4f}{lat_hemi}, {abs(st.lon):.4f}{lon_hemi}"
    if site_tag:
        head_geo = f"{head_geo} ({site_tag})"
    elif terrain_tag:
        head_geo = f"{head_geo} ({terrain_tag})"

    header_lines = [
        f"📍 **{st.icao} ({st.city}) | {head_geo}**",
        f"生成时间: {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M')} UTC | {now_local.strftime('%H:%M')} Local (UTC{now_local.strftime('%z')[:3]})",
        f"分析基准模型: {analysis_model.upper()}（运行时次: {rt_fmt}） | 小时预报源: {provider_used} | 3D场源: {SYNOPTIC_PROVIDER}",
    ]
    if direction_factor:
        header_lines.append(f"方位因子: {direction_factor}")

    # Show timing only when forecast-data acquisition is meaningful (non-trivial fetch cost).
    hf = float(perf_local.get('hourly_fetch', 0.0) or 0.0)
    if hf >= 1.0:
        header_lines.append(f"⏱️ forecast取数耗时: {hf:.2f}s")

    try:
        quality = (forecast_decision.get("quality") or {}) if isinstance(forecast_decision, dict) else {}
        missing = set(quality.get("missing_layers") or [])
        degraded = str(quality.get("source_state") or "") == "degraded"
        syn_fail = ("synoptic" in missing) or degraded
        err_txt = str(_synoptic_error or "")
        breaker_active, breaker_until, breaker_reason = _openmeteo_breaker_info()
        rate_limited = (
            ("429" in err_txt)
            or ("Too Many Requests" in err_txt)
            or ("breaker active" in err_txt)
            or breaker_active
            or (breaker_active and (("429" in str(breaker_reason)) or ("grid_429" in str(breaker_reason))))
        )
        if syn_fail and rate_limited:
            if breaker_until is not None:
                header_lines.append(
                    f"⚠️ 数据提醒：Open-Meteo 请求过多（429），环流层已降级；预计 {breaker_until.strftime('%H:%M:%S')} UTC 后恢复"
                )
            else:
                header_lines.append("⚠️ 数据提醒：Open-Meteo 请求过多（429），环流层已降级。")
    except Exception:
        pass

    header = "\n".join(header_lines)

    t0 = time.perf_counter()
    body = choose_section_text(
        primary,
        metar_text,
        metar_diag,
        links_payload["links"]["polymarket_event"],
        forecast_decision=forecast_decision,
    )
    _mark("render_body", time.perf_counter() - t0)

    links = links_payload["links"]
    footer = (
        f"🔗[Polymarket]({links['polymarket_event']}) | "
        f"[METAR]({links['metar_24h']}) | "
        f"[Wunderground]({links['wunderground']}) | "
        f"[探空图（Tropicaltidbits）]({links['sounding_tropicaltidbits']})"
    )

    return f"{header}\n\n{body}\n\n{footer}"


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="Generate report text from Telegram-style command")
    p.add_argument("--command", required=True, help="Telegram command text, e.g. '/look Ankara model=ecmwf' or '/lookhelp'")
    return p


def main() -> None:
    args = build_parser().parse_args()
    try:
        print(render_report(args.command))
    except Exception as exc:
        print(f"❌ /look 执行失败: {exc}")


if __name__ == "__main__":
    main()
