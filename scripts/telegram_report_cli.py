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
from cache_envelope import extract_payload, make_cache_doc
from window_phase_engine import pick_peak_indices

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
    "KLGA": "America/New_York",
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


def _prune_runtime_cache(max_age_hours: int = CACHE_PRUNE_HOURS) -> None:
    if not CACHE_DIR.exists():
        return
    now = datetime.now(timezone.utc)

    # JSON runtime cache
    for p in CACHE_DIR.glob("*.json"):
        try:
            mtime = datetime.fromtimestamp(p.stat().st_mtime, tz=timezone.utc)
            if (now - mtime) > timedelta(hours=max_age_hours):
                p.unlink(missing_ok=True)
        except Exception:
            continue

    # Binary GRIB cache (separate retention; defaults slightly longer)
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


def render_look_help() -> str:
    return (
        "📘 /look 用法\n"
        "- /look <城市或机场代码>\n"
        "- 示例：/look ank | /look London | /look par\n"
        "\n支持城市（示例）：ank / lon / par / nyc / chi / dal / mia / atl / sea / tor / sel / ba / wel\n"
        "提示：统一单条最终报告输出，不发送预告消息。"
    )


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

    # Re-center candidate around nearby local peak first, then build band window.
    # This avoids early-hour candidates creating unrealistically long windows.
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
    # Window-phase module (prior + offsets + model factors) handles candidate scoring.
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


def _build_post_focus_window(hourly: dict[str, Any], metar_diag: dict[str, Any]) -> dict[str, Any] | None:
    """Build a prospective secondary-peak focus window after latest observation."""
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


def _build_post_eval_window(hourly: dict[str, Any], metar_diag: dict[str, Any]) -> dict[str, Any] | None:
    """Fallback post window for 'verify no second peak' situations.

    When no clear secondary peak candidate exists, still anchor links/analysis
    to the next 1-2h period rather than stale first-peak time.
    """
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
    prev3 = series[-4] if len(series) > 3 else None

    tz = ZoneInfo(tz_name)
    latest_dt_local = _metar_obs_time_utc(latest).astimezone(tz)
    hour_key = latest_dt_local.strftime("%Y-%m-%dT%H:00")

    tmap = {t: v for t, v in zip(hourly_local["time"], hourly_local["temperature_2m"])}
    pmap = {t: v for t, v in zip(hourly_local["time"], hourly_local["pressure_msl"])}
    fc_t = tmap.get(hour_key)
    fc_p = pmap.get(hour_key)

    def _collect_cloud_pairs(obs: dict[str, Any]) -> list[tuple[str, int | None]]:
        raw_ob = (obs.get("rawOb") or "")
        if " CAVOK" in raw_ob:
            return [("CAVOK", None)]
        if " CLR" in raw_ob:
            return [("CLR", None)]

        pairs: list[tuple[str, int | None]] = []
        seen: set[tuple[str, int | None]] = set()

        for code, h in re.findall(r"\b(FEW|SCT|BKN|OVC|VV)(\d{3})\b", raw_ob):
            ft = int(h) * 100
            key = (code, ft)
            if key not in seen:
                seen.add(key)
                pairs.append((code, ft))

        clouds = obs.get("clouds") if isinstance(obs.get("clouds"), list) else []
        for c in clouds:
            if not isinstance(c, dict):
                continue
            code = str(c.get("cover") or "").upper()
            if code not in {"FEW", "SCT", "BKN", "OVC", "VV"}:
                continue
            base = c.get("base")
            ft = None
            try:
                ft = int(float(base)) if base is not None else None
            except Exception:
                ft = None
            key = (code, ft)
            if key not in seen:
                seen.add(key)
                pairs.append((code, ft))

        if not pairs:
            fallback_cover = str(obs.get("cover") or "").upper()
            if fallback_cover in {"CAVOK", "CLR"}:
                return [(fallback_cover, None)]
            if fallback_cover in {"FEW", "SCT", "BKN", "OVC", "VV"}:
                return [(fallback_cover, None)]
        return pairs

    def _cloud_compact(obs: dict[str, Any]) -> str:
        pairs = _collect_cloud_pairs(obs)
        if not pairs:
            return str(obs.get("cover") or "N/A")
        if len(pairs) == 1 and pairs[0][0] in {"CAVOK", "CLR"}:
            return pairs[0][0]
        toks: list[str] = []
        for code, ft in pairs:
            if ft is None:
                toks.append(code)
            else:
                toks.append(f"{code}{int(round(ft/100.0)):03d}")
        return " ".join(toks)

    def _cloud_tokens(obs: dict[str, Any]) -> list[str]:
        pairs = _collect_cloud_pairs(obs)
        if not pairs:
            return []
        if len(pairs) == 1 and pairs[0][0] in {"CAVOK", "CLR"}:
            return [pairs[0][0]]
        toks: list[str] = []
        for code, ft in pairs:
            if ft is None:
                toks.append(code)
            else:
                toks.append(f"{code}{int(round(ft/100.0)):03d}")
        return toks

    def parse_cloud_layers(obs: dict[str, Any]) -> str:
        pairs = _collect_cloud_pairs(obs)
        if not pairs:
            return str(obs.get("cover") or "N/A")
        if len(pairs) == 1 and pairs[0][0] in {"CAVOK", "CLR"}:
            return pairs[0][0]

        code_meaning = {
            "FEW": "少云",
            "SCT": "疏云",
            "BKN": "多云",
            "OVC": "阴天",
            "VV": "垂直能见度",
            "CAVOK": "能见良好",
            "CLR": "净空",
        }
        out = []
        for code, ft in pairs:
            meaning = code_meaning.get(code, code)
            if ft is None:
                out.append(f"{code}({meaning})")
            else:
                m = int(round(ft * 0.3048))
                h = int(round(ft / 100.0))
                out.append(f"{code}{h:03d}({meaning}{ft}ft/{m}m)")
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

    def _wx_human_desc(wx_raw: Any) -> str:
        t = str(wx_raw or "").upper().strip()
        if not t or t == "无降水天气现象":
            return ""

        parts: list[str] = []
        intensity = ""
        if "+" in t:
            intensity = "强"
        elif "-" in t:
            intensity = "小"

        if "TS" in t:
            parts.append("雷暴")
        if "SH" in t:
            parts.append("阵性")

        if "FZRA" in t:
            parts.append("冻雨")
        elif "DZ" in t:
            parts.append((intensity or "毛毛") + "雨")
        elif "RA" in t:
            parts.append((intensity + "雨") if intensity else "雨")
        elif "SN" in t:
            parts.append((intensity + "雪") if intensity else "雪")

        if "FG" in t:
            parts.append("雾")
        elif "BR" in t:
            parts.append("轻雾")
        elif "HZ" in t:
            parts.append("霾")

        if not parts:
            return ""
        # de-duplicate while preserving order
        uniq = list(dict.fromkeys(parts))
        return "、".join(uniq)

    def _wind_change_text(cur: dict[str, Any], prev_x: dict[str, Any] | None) -> str:
        if not prev_x:
            return ""

        cur_d = cur.get("wdir")
        cur_s = cur.get("wspd")
        prv_d = prev_x.get("wdir")
        prv_s = prev_x.get("wspd")

        def _f(v: Any) -> float | None:
            try:
                if v in (None, "", "VRB"):
                    return None
                return float(v)
            except Exception:
                return None

        cd = _f(cur_d)
        pd = _f(prv_d)
        try:
            cs = float(cur_s)
        except Exception:
            cs = None
        try:
            ps = float(prv_s)
        except Exception:
            ps = None

        prev_wind_txt = f"{prv_d}° {prv_s}kt" if (prv_d not in (None, "", "VRB") and prv_s not in (None, "")) else f"{prv_d or 'VRB'} {prv_s or '?'}kt"

        if cd is None and pd is None:
            dir_msg = "风向信息不足"
        elif cd is None and pd is not None:
            dir_msg = "转为风向不定"
        elif cd is not None and pd is None:
            dir_msg = "风向由不定转为可判定"
        else:
            d = abs((cd - pd + 180.0) % 360.0 - 180.0)
            if d >= 60:
                dir_msg = "风向明显转向"
            elif d >= 25:
                dir_msg = "风向小幅转向"
            else:
                dir_msg = "风向基本稳定"

        if cs is None or ps is None:
            spd_msg = "风速变化待确认"
        else:
            ds = cs - ps
            if ds >= 3.0:
                spd_msg = f"风速增强{ds:.0f}kt"
            elif ds <= -3.0:
                spd_msg = f"风速减弱{abs(ds):.0f}kt"
            else:
                spd_msg = "风速变化不大"

        return f"较上一报{prev_wind_txt}，{dir_msg}，{spd_msg}"

    def _cloud_change_parts(cur: dict[str, Any], prev_x: dict[str, Any] | None) -> dict[str, str]:
        if not prev_x:
            return {}
        prev_compact = _cloud_compact(prev_x)
        cur_tokens = _cloud_tokens(cur)
        prev_tokens = _cloud_tokens(prev_x)
        tr = _cloud_trend(cur, prev_x)

        if cur_tokens == prev_tokens:
            tr_txt = "云层稳定无变化"
            return {
                "prev": prev_compact,
                "trend": tr_txt,
                "inline": f"（上一报{prev_compact}，{tr_txt}）",
            }

        cur_set = set(cur_tokens)
        prev_set = set(prev_tokens)
        added = [t for t in cur_tokens if t not in prev_set]
        removed = [t for t in prev_tokens if t not in cur_set]

        if added and removed:
            tr_txt = f"云层重排（新增{'/'.join(added)}；消退{'/'.join(removed)}）"
        elif added:
            tr_txt = f"云量增加（新增{'/'.join(added)}）"
        elif removed:
            tr_txt = f"云量减少（消退{'/'.join(removed)}）"
        else:
            tr_txt = tr if tr else "云层结构有调整"

        return {
            "prev": prev_compact,
            "trend": tr_txt,
            "inline": f"（上一报{prev_compact}，{tr_txt}）",
        }

    def fmt_latest_obs(x: dict[str, Any], prev_x: dict[str, Any] | None) -> list[str]:
        local = _metar_obs_time_utc(x).astimezone(tz)
        wx = x.get("wxString") or x.get("wx") or "无降水天气现象"
        wx_desc = _wx_human_desc(wx)
        cloud = parse_cloud_layers(x)

        dt = dp = dpres = 0.0
        if prev_x:
            try:
                dt = float(x.get("temp", 0)) - float(prev_x.get("temp", 0))
                dp = float(x.get("dewp", 0)) - float(prev_x.get("dewp", 0))
                dpres = float(x.get("altim", 0)) - float(prev_x.get("altim", 0))
            except Exception:
                pass

        latest_hdr = f"**最新报（{local.strftime('%H:%M')} {time_label}）**"
        if prev_x:
            prev_local = _metar_obs_time_utc(prev_x).astimezone(tz)
            latest_hdr = f"**最新报（{local.strftime('%H:%M')} {time_label}）（上一报 {prev_local.strftime('%H:%M')} {time_label}）**"

        wind_line = fmt_wind(x)
        wind_cmp = _wind_change_text(x, prev_x)
        if wind_cmp:
            wind_line = f"{wind_line}（{wind_cmp}）"

        cloud_line = cloud
        cloud_cmp = _cloud_change_parts(x, prev_x)
        if cloud_cmp:
            cur_tokens_n = len(_cloud_tokens(x))
            prev_tokens_n = len(_cloud_tokens(prev_x)) if prev_x else 0
            multiline_cmp = (cur_tokens_n >= 3) or (prev_tokens_n >= 3) or (cloud.count(",") >= 2)
            if multiline_cmp:
                cloud_line = (
                    f"{cloud}\n"
                    f"  ↳ 上一报：{cloud_cmp.get('prev', '')}\n"
                    f"  ↳ 变化：{cloud_cmp.get('trend', '')}"
                )
            else:
                cloud_line = f"{cloud}{cloud_cmp.get('inline', '')}"

        lines = [
            latest_hdr,
            f"• **气温**：{x.get('temp')}°C（{_delta_text(dt, '°C')}）",
            f"• **露点**：{x.get('dewp')}°C（{_delta_text(dp, '°C')}）",
            f"• **气压**：{x.get('altim')} hPa（{_delta_text(dpres, ' hPa')}）",
            f"• **风**：{wind_line}",
            f"• **云层**：{cloud_line}",
        ]
        if wx and wx != "无降水天气现象":
            if wx_desc:
                lines.append(f"• **天气现象**：{wx}（{wx_desc}）")
            else:
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

        rank = {"CLR": 0, "CAVOK": 0, "FEW": 1, "SCT": 2, "BKN": 3, "OVC": 4, "VV": 5}
        codes: list[str] = []

        for c, _h in re.findall(r"\b(FEW|SCT|BKN|OVC|VV)(\d{3})\b", raw):
            codes.append(c)

        clouds = x.get("clouds") if isinstance(x.get("clouds"), list) else []
        for c in clouds:
            if isinstance(c, dict):
                cv = str(c.get("cover") or "").upper()
                if cv:
                    codes.append(cv)

        cover = str(x.get("cover") or "").upper()
        if cover:
            codes.append(cover)

        codes = [c for c in codes if c in rank]
        if not codes:
            return ""
        return sorted(codes, key=lambda c: rank.get(c, 0), reverse=True)[0]

    def _cloud_token(x: dict[str, Any] | None) -> str:
        if not x:
            return ""
        raw = (x.get("rawOb") or "")
        if " CAVOK" in raw:
            return "CAVOK"
        if " CLR" in raw:
            return "CLR"

        rank = {"CLR": 0, "CAVOK": 0, "FEW": 1, "SCT": 2, "BKN": 3, "OVC": 4, "VV": 5}
        toks: list[tuple[str, int | None]] = []

        for c, h in re.findall(r"\b(FEW|SCT|BKN|OVC|VV)(\d{3})\b", raw):
            toks.append((c, int(h) * 100))

        clouds = x.get("clouds") if isinstance(x.get("clouds"), list) else []
        for c in clouds:
            if not isinstance(c, dict):
                continue
            cv = str(c.get("cover") or "").upper()
            if cv not in rank:
                continue
            base = c.get("base")
            ft = None
            try:
                ft = int(float(base)) if base is not None else None
            except Exception:
                ft = None
            toks.append((cv, ft))

        if not toks:
            c = _cloud_code(x)
            return c

        # choose strongest cover; if tie, lower base first (more restrictive)
        toks = sorted(toks, key=lambda z: (rank.get(z[0], 0), -(z[1] if z[1] is not None else 10**9)), reverse=True)
        c, ft = toks[0]
        if ft is None:
            return c
        return f"{c}{int(round(ft/100.0)):03d}"

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

    def _is_intish(v: Any) -> bool:
        try:
            return abs(float(v) - round(float(v))) < 0.05
        except Exception:
            return False

    # Quantization-aware smoothing: avoid overreacting to single METAR step (often integer-quantized).
    trend_steps: list[float] = []
    for a, b in ((latest, prev), (prev, prev2), (prev2, prev3)):
        if a is None or b is None:
            continue
        try:
            trend_steps.append(float(a.get("temp", 0)) - float(b.get("temp", 0)))
        except Exception:
            continue

    t_trend_smooth = None
    if trend_steps:
        ws = [0.55, 0.30, 0.15]
        use_n = min(len(trend_steps), len(ws))
        num = sum(trend_steps[i] * ws[i] for i in range(use_n))
        den = sum(ws[:use_n])
        t_trend_smooth = num / den if den > 0 else None

        # integer METAR deadband
        int_q = _is_intish(latest.get("temp")) and (prev is None or _is_intish(prev.get("temp")))
        deadband = 0.55 if int_q else 0.35
        if t_trend_smooth is not None and abs(t_trend_smooth) < deadband:
            t_trend_smooth = 0.0

    latest_cloud_code = _cloud_code(latest)

    if str(latest_cloud_code or "").upper() in {"CLR", "CAVOK", "FEW", "SCT"}:
        cloud_hint = "云量约束偏弱"
    elif str(latest_cloud_code or "").upper() in {"BKN", "OVC", "VV"}:
        cloud_hint = "低云约束仍在"
    else:
        cloud_hint = "云量约束不确定"

    trend_ref = t_trend_smooth if isinstance(t_trend_smooth, (int, float)) else t_trend
    if isinstance(trend_ref, (int, float)):
        if trend_ref >= 0.5:
            trend_hint = "短时升温仍在延续"
        elif trend_ref <= -0.5:
            trend_hint = "短时升温动能转弱"
        else:
            trend_hint = "短时温度基本横盘"
    else:
        trend_hint = "短时温度节奏待确认"

    def _wx_state(wx: str) -> str:
        s = str(wx or "").upper()
        if not s:
            return "none"
        if "TS" in s:
            return "convective"
        if any(k in s for k in ["RA", "DZ", "SN", "PL", "GR", "GS"]):
            if "+" in s:
                return "heavy"
            if "-" in s:
                return "light"
            return "moderate"
        return "none"

    def _wx_hint(wx: str) -> str:
        s = str(wx or "").upper()
        if not s:
            return ""
        if "TS" in s:
            return "对流降水干扰在场"
        if any(k in s for k in ["RA", "DZ"]):
            if "-RA" in s or "-DZ" in s:
                return "弱降雨干扰在场"
            if "+RA" in s or "+DZ" in s:
                return "较强降雨干扰在场"
            return "降雨干扰在场"
        if any(k in s for k in ["SN", "PL", "GR", "GS"]):
            return "降水相态干扰在场"
        return ""

    wx_now = str(latest.get("wxString") or latest.get("wx") or "").upper()
    wx_prev = str((prev or {}).get("wxString") or (prev or {}).get("wx") or "").upper() if prev else ""
    wx_state_now = _wx_state(wx_now)
    wx_state_prev = _wx_state(wx_prev)

    rank = {"none": 0, "light": 1, "moderate": 2, "heavy": 3, "convective": 4}
    if rank.get(wx_state_now, 0) > 0 and rank.get(wx_state_prev, 0) == 0:
        wx_trend = "new"
    elif rank.get(wx_state_now, 0) == 0 and rank.get(wx_state_prev, 0) > 0:
        wx_trend = "end"
    elif rank.get(wx_state_now, 0) > rank.get(wx_state_prev, 0):
        wx_trend = "intensify"
    elif rank.get(wx_state_now, 0) < rank.get(wx_state_prev, 0):
        wx_trend = "weaken"
    elif rank.get(wx_state_now, 0) > 0:
        wx_trend = "steady"
    else:
        wx_trend = "none"

    wx_hint = _wx_hint(wx_now)
    wx_change_hint = ""
    if wx_trend == "new":
        wx_change_hint = "降水新出现"
    elif wx_trend == "intensify":
        wx_change_hint = "降水在增强"
    elif wx_trend == "weaken":
        wx_change_hint = "降水在减弱"
    elif wx_trend == "end":
        wx_change_hint = "降水已结束"

    lines.append("")
    summary_bits = [trend_hint, cloud_hint]
    if wx_hint:
        summary_bits.append(wx_hint)
    if wx_change_hint:
        summary_bits.append(wx_change_hint)
    lines.append(f"• 最新实况简评：{'，'.join(summary_bits)}。")

    # 追加“近两小时节奏”一句：把短样本变化压缩成可执行线索
    rhythm_line = None
    try:
        if prev is not None and prev2 is not None:
            t0 = float(latest.get("temp", 0.0))
            t1 = float(prev.get("temp", 0.0))
            t2 = float(prev2.get("temp", 0.0))
            dt_now = t0 - t1
            dt_prev = t1 - t2

            p0 = float(latest.get("altim", 0.0))
            p2 = float(prev2.get("altim", 0.0))
            dp2h = p0 - p2

            w0 = latest.get("wdir")
            w1 = prev.get("wdir")
            dchg = 0.0
            if w0 not in (None, "", "VRB") and w1 not in (None, "", "VRB"):
                a = abs(float(w0) - float(w1)) % 360.0
                dchg = min(a, 360.0 - a)

            cloud_txt = _cloud_trend(latest, prev)

            temp_signal = (abs(dt_now) >= 0.5) or (abs(dt_prev) >= 0.5)
            press_signal = abs(dp2h) >= 1.2
            wind_signal = dchg >= 35
            cloud_signal = ("回补" in cloud_txt) or ("开窗" in cloud_txt) or ("减少" in cloud_txt) or ("增加" in cloud_txt)

            # No meaningful change -> skip this line to avoid mechanical noise.
            if not (temp_signal or press_signal or wind_signal or cloud_signal):
                rhythm_line = None
            else:
                chunks: list[str] = []
                if dt_now >= 0.5 and dt_prev >= 0.2:
                    chunks.append("温度仍在上行")
                elif dt_now <= -0.5 and dt_prev <= -0.2:
                    chunks.append("温度有回落迹象")
                elif temp_signal:
                    chunks.append("温度在窄幅震荡")

                if press_signal:
                    if dp2h <= -1.2:
                        chunks.append("气压继续走低")
                    elif dp2h >= 1.2:
                        chunks.append("气压明显回升")

                if wind_signal:
                    chunks.append("风向正在重排")

                if cloud_signal:
                    # compress cloud phrase into plain short cue
                    if "回补" in cloud_txt or "增加" in cloud_txt:
                        chunks.append("云量有回补")
                    elif "开窗" in cloud_txt or "减少" in cloud_txt:
                        chunks.append("云量在转疏")

                if chunks:
                    rhythm_line = f"• 近两小时节奏：{'，'.join(chunks)}。"
                else:
                    rhythm_line = None
    except Exception:
        rhythm_line = None

    if rhythm_line:
        lines.append(rhythm_line)

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
    bvals = [v for v in [b0, b1, b2] if isinstance(v, (int, float))]
    bias_smooth = None
    if bvals:
        ws = [0.55, 0.30, 0.15]
        num = 0.0
        den = 0.0
        for i, v in enumerate([b0, b1, b2]):
            if isinstance(v, (int, float)):
                w = ws[i]
                num += float(v) * w
                den += w
        if den > 0:
            bias_smooth = round(num / den, 2)
    # Bias trajectory line removed by operator preference: keep report concise.

    # 5) 高影响触发告警（仅触发时显示）
    alert = None
    ctrend = _cloud_trend(latest, prev)
    if wx_trend in {"new", "intensify"}:
        alert = "⚠️ 实况触发：降水出现/增强，短时压温风险上升。"
    elif isinstance(t_trend, (int, float)) and t_trend >= 0.5 and ("开窗" in ctrend or "减弱" in ctrend):
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
        "temp_bias_smooth_c": bias_smooth,
        "pressure_bias_hpa": p_bias,
        "latest_wdir": latest.get("wdir"),
        "latest_wspd": latest.get("wspd"),
        "latest_temp": latest.get("temp"),
        "latest_dewpoint": latest.get("dewp"),
        "latest_cloud_code": latest_cloud_code,
        "latest_wx": latest.get("wxString") or latest.get("wx") or "",
        "latest_precip_state": wx_state_now,
        "precip_trend": wx_trend,
        "cloud_trend": cloud_tr,
        "temp_trend_1step_c": t_trend,
        "temp_trend_smooth_c": t_trend_smooth,
        "metar_temp_quantized": _is_intish(latest.get("temp")) and (prev is None or _is_intish(prev.get("temp"))),
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

    # explicit ranged bins first, e.g. "-42-43f" / "-12-13c"
    m = re.search(r"-(neg\d+|\d{1,3})-(neg\d+|\d{1,3})c$", s)
    if m:
        n1 = _poly_num(m.group(1))
        n2 = _poly_num(m.group(2))
        if abs(n1 - n2) <= 8:
            lo, hi = (n1, n2) if n1 <= n2 else (n2, n1)
            return (lo - 0.5, hi + 0.49, "C")
    m = re.search(r"-(neg\d+|\d{1,3})-(neg\d+|\d{1,3})f$", s)
    if m:
        n1 = _poly_num(m.group(1))
        n2 = _poly_num(m.group(2))
        if abs(n1 - n2) <= 8:
            lo, hi = (n1, n2) if n1 <= n2 else (n2, n1)
            return (lo - 0.5, hi + 0.49, "F")

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
        (r"-(neg\d+|\d{1,3})-(neg\d+|\d{1,3})c$", lambda a, b: f"{_poly_num(a)}-{_poly_num(b)}°C"),
        (r"-(neg\d+|\d{1,3})-(neg\d+|\d{1,3})f$", lambda a, b: f"{_poly_num(a)}-{_poly_num(b)}°F"),
        (r"-(neg\d+|\d+)c$", lambda n: f"{_poly_num(n)}°C"),
        (r"-(neg\d+|\d+)corbelow$", lambda n: f"{_poly_num(n)}°C or below"),
        (r"-(neg\d+|\d+)corhigher$", lambda n: f"{_poly_num(n)}°C or higher"),
        (r"-(neg\d+|\d+)f$", lambda n: f"{_poly_num(n)}°F"),
        (r"-(neg\d+|\d+)forbelow$", lambda n: f"{_poly_num(n)}°F or below"),
        (r"-(neg\d+|\d+)forhigher$", lambda n: f"{_poly_num(n)}°F or higher"),
    ]:
        m = re.search(pat, s)
        if m:
            if len(m.groups()) == 2:
                return fmt(m.group(1), m.group(2))
            return fmt(m.group(1))
    return slug


def _build_polymarket_section(
    polymarket_event_url: str,
    primary_window: dict[str, Any],
    metar_diag: dict[str, Any] | None = None,
    range_hint: dict[str, float] | None = None,
) -> str:
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
        if iv and len(iv) >= 3 and str(iv[2]).upper() == "F":
            lo = (lo - 32) * 5 / 9 if lo != -math.inf else -math.inf
            hi = (hi - 32) * 5 / 9 if hi != math.inf else math.inf
        # 硬性剔除：已低于实况录得最高温的区间，不再可能结算命中。
        if obs_max is not None and hi < obs_max:
            continue
        # 软剔除：明显低于当前温度太多的旧档位。
        if t_now is not None and hi < t_now - 1.0:
            continue
        filtered.append((center, label, bid, ask, lo, hi))

    if not filtered:
        filtered = [(c, l, b, a, c - 0.5, c + 0.49) for c, l, b, a in parsed]

    likely_lo = peak - 0.8
    likely_hi = peak + 0.8

    hint = range_hint or {}
    try:
        hint_lo = float(hint.get("display_lo")) if hint.get("display_lo") is not None else None
    except Exception:
        hint_lo = None
    try:
        hint_hi = float(hint.get("display_hi")) if hint.get("display_hi") is not None else None
    except Exception:
        hint_hi = None

    target_lo = hint_lo if hint_lo is not None else likely_lo
    target_hi = hint_hi if hint_hi is not None else likely_hi

    try:
        core_lo = float(hint.get("core_lo")) if hint.get("core_lo") is not None else None
    except Exception:
        core_lo = None
    try:
        core_hi = float(hint.get("core_hi")) if hint.get("core_hi") is not None else None
    except Exception:
        core_hi = None
    if core_lo is None:
        core_lo = likely_lo
    if core_hi is None:
        core_hi = likely_hi

    def _ov_len(a0: float, a1: float, b0: float, b1: float) -> float:
        lo = max(a0, b0)
        hi = min(a1, b1)
        return max(0.0, hi - lo)

    def _weather_score(row: tuple[float, str, Any, Any, float, float]) -> float:
        c, _l, _b, _a, lo, hi = row
        core_w = max(0.4, float(core_hi - core_lo))
        disp_w = max(core_w, float(target_hi - target_lo), 0.8)
        ov_core = _ov_len(lo, hi, core_lo, core_hi) / core_w
        ov_disp = _ov_len(lo, hi, target_lo, target_hi) / disp_w
        mid = 0.5 * (core_lo + core_hi)
        c_term = max(0.0, 1.0 - abs(c - mid) / 2.0)
        return 0.55 * ov_core + 0.30 * ov_disp + 0.15 * c_term

    def _market_strength(row: tuple[float, str, Any, Any, float, float]) -> float:
        _c, _l, b, a, _lo, _hi = row
        bid = _px(b)
        ask = _px(a)
        if bid > 0 and ask > 0:
            mid = 0.5 * (bid + ask)
            spread = max(0.0, ask - bid)
        else:
            mid = max(bid, ask)
            spread = 0.25
        liquid = 0.08 if max(bid, ask) >= 0.02 else -0.05
        return mid - 0.35 * spread + liquid

    def _alpha_score(row: tuple[float, str, Any, Any, float, float]) -> float:
        _c, _l, b, a, _lo, hi = row
        bid = _px(b)
        ask = _px(a)
        spread = max(0.0, ask - bid)
        w = _weather_score(row)
        m = _market_strength(row)
        mispricing = max(0.0, w - m)

        cheap_bonus = 0.0
        if ask > 0 and ask <= 0.15:
            cheap_bonus = 0.14 + 0.10 * (0.15 - ask) / 0.15
        elif ask > 0.15 and ask <= 0.20 and spread <= 0.06 and w >= 0.45:
            cheap_bonus = 0.05 + 0.05 * (0.20 - ask) / 0.05

        tradable = 0.05 if max(bid, ask) >= 0.02 else -0.04
        stale_penalty = 0.0
        if t_now is not None and hi <= t_now + 0.3:
            stale_penalty = 0.35

        return 0.85 * mispricing + 0.25 * w + cheap_bonus + tradable - 0.25 * spread - stale_penalty

    ranked = sorted(filtered, key=lambda r: (0.75 * _weather_score(r) + 0.25 * _market_strength(r)), reverse=True)

    def _overlap_or_near(row: tuple[float, str, Any, Any, float, float]) -> bool:
        _c, _l, _b, _a, lo, hi = row
        if hi < target_lo - 0.5:
            return False
        if lo > target_hi + 0.6:
            return False
        return True

    near_pool = [r for r in filtered if _overlap_or_near(r)]
    mismatch = False
    if near_pool:
        seed = sorted(near_pool, key=_alpha_score, reverse=True)[:5]
    else:
        mismatch = True
        # 若主带无直接匹配，向 below/above 边缘档位寻找最近可交易区间
        below = [r for r in filtered if r[5] <= target_lo]
        above = [r for r in filtered if r[4] >= target_hi]
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

    # Ensure one upper-tail bin is visible when merged upper bound approaches market upper buckets.
    # Handles both finite next-step bins and "X°C or higher" style edge bins.
    try:
        finite_all = sorted([r for r in filtered if not (math.isinf(r[4]) or math.isinf(r[5]))], key=lambda x: x[0])
        edge_ref_hi = max(likely_hi, target_hi)

        # 1) Prefer finite next-step bucket if close enough.
        target_center = round(edge_ref_hi)
        cand = [r for r in finite_all if abs(r[0] - target_center) <= 0.26 and r[0] >= (core_hi - 0.2)]
        if cand:
            up = sorted(cand, key=lambda x: abs(x[0] - edge_ref_hi))[0]
            if all(up[1] != r[1] for r in focus):
                focus.append(up)

        # 2) Add nearest upper-edge bucket when display upper range reaches it.
        upper_edges = sorted(
            [r for r in filtered if (not math.isinf(r[4]) and math.isinf(r[5]))],
            key=lambda x: x[4],
        )
        if upper_edges:
            # include first edge whose lower bound is not far above merged upper range
            edge_cands = [r for r in upper_edges if r[4] <= (target_hi + 0.4)]
            if edge_cands:
                up = edge_cands[0]
                if all(up[1] != r[1] for r in focus):
                    focus.append(up)

        focus = sorted(focus, key=lambda x: x[0])
    except Exception:
        pass

    # “最有可能”以天气预测一致性为主，市场强度仅作并列裁决。
    best_label = None
    if focus:
        core_bins = [r for r in focus if (r[5] >= core_lo and r[4] <= core_hi)]
        pick_pool = core_bins if core_bins else focus

        def _likely_score(row: tuple[float, str, Any, Any, float, float]) -> float:
            return 0.82 * _weather_score(row) + 0.18 * _market_strength(row)

        pick_sorted = sorted(pick_pool, key=_likely_score, reverse=True)
        s1 = _likely_score(pick_sorted[0])
        s2 = _likely_score(pick_sorted[1]) if len(pick_sorted) > 1 else -999.0
        # require clear lead to avoid over-tagging in tight distributions
        if (s1 - s2) >= 0.045 and _weather_score(pick_sorted[0]) >= 0.28:
            best_label = pick_sorted[0][1]

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

    # Final clipping by merged weather range to avoid displaying bins that are clearly too cold.
    if focus and hint:
        min_keep = target_lo - 0.4
        max_keep = target_hi + 0.9
        clipped = [r for r in focus if (r[5] >= min_keep and r[4] <= max_keep)]
        if clipped:
            focus = sorted(clipped, key=lambda x: x[0])
            if len(focus) < 3:
                finite_all = sorted([r for r in filtered if not (math.isinf(r[4]) or math.isinf(r[5]))], key=lambda x: x[0])
                if finite_all:
                    mid = 0.5 * (target_lo + target_hi)
                    nearest = sorted(finite_all, key=lambda x: abs(x[0] - mid))[:3]
                    merged = {r[1]: r for r in (focus + nearest)}
                    focus = sorted(merged.values(), key=lambda x: x[0])

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
        bid_v = _px(bid)
        spread_v = max(0.0, ask_v - bid_v)
        bid_txt = "None" if bid in (None, "") else str(bid)
        ask_txt = "None" if ask in (None, "") else str(ask)
        tag = ""
        if best_label and label == best_label:
            tag = "👍最有可能"
        else:
            # 潜在alpha：
            # - 常规: Ask<=0.15
            # - 扩展: 0.15<Ask<=0.18 且天气一致性高
            row = (_c, label, bid, ask, _lo, _hi)
            s = score_map.get((label, str(bid), str(ask)), 0.0)
            w = _weather_score(row)
            # 常规alpha也必须满足最低天气一致性，避免“很便宜但偏离天气区间太远”的误标。
            if ask_v > 0 and ask_v <= 0.15 and w >= 0.12 and s >= 0.22:
                tag = "😇潜在Alpha"
            elif ask_v > 0.15 and ask_v <= 0.18 and w >= 0.45 and s >= 0.30:
                tag = "😇潜在Alpha"

        if tag:
            lines.append(f"  • **{label}（{tag}）：Bid {bid_txt} | Ask {ask_txt}**")
        else:
            lines.append(f"  • {label}：Bid {bid_txt} | Ask {ask_txt}")
    return "\n".join(lines)


def choose_section_text(
    primary_window: dict[str, Any],
    metar_text: str,
    metar_diag: dict[str, Any],
    polymarket_event_url: str,
    forecast_decision: dict[str, Any] | None = None,
    compact_synoptic: bool = False,
    temp_unit: str = "C",
    synoptic_window: dict[str, Any] | None = None,
) -> str:
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

    unit = "F" if str(temp_unit).upper() == "F" else "C"

    def _to_unit(c: float) -> float:
        return (c * 9.0 / 5.0 + 32.0) if unit == "F" else c

    def _fmt_temp(v_c: float) -> str:
        v = _to_unit(float(v_c))
        return f"{v:.1f}°{unit}"

    def _fmt_range(lo_c: float, hi_c: float) -> str:
        lo_u = _to_unit(float(lo_c))
        hi_u = _to_unit(float(hi_c))
        return f"{lo_u:.1f}~{hi_u:.1f}°{unit}"

    fdec = forecast_decision if isinstance(forecast_decision, dict) else {}
    d = (fdec.get("decision") or {}) if isinstance(fdec, dict) else {}
    bg = (d.get("background") or fdec.get("background") or {}) if isinstance(fdec, dict) else {}
    quality = (fdec.get("quality") or {}) if isinstance(fdec, dict) else {}

    meta_window = (((fdec.get("meta") or {}).get("window")) if isinstance(fdec, dict) else {}) or {}
    if isinstance(synoptic_window, dict) and synoptic_window.get("start_local") and synoptic_window.get("end_local"):
        syn_w = synoptic_window
    elif isinstance(meta_window, dict) and meta_window.get("start_local") and meta_window.get("end_local"):
        syn_w = meta_window
    else:
        syn_w = primary_window

    post_focus_active = bool(metar_diag.get("post_focus_window_active"))
    calc_window = syn_w if post_focus_active else primary_window

    phase_mode = str(bg.get("phase_mode") or "实况主导")
    line500 = str(bg.get("line_500") or "高空背景信号有限。")
    line850 = str(bg.get("line_850") or "低层输送信号一般。")
    extra = str(bg.get("extra") or "")
    h700_summary = str((((fdec.get("features") or {}).get("h700") or {}).get("summary") if isinstance(fdec, dict) else "") or "")
    h925_summary = str((((fdec.get("features") or {}).get("h925") or {}).get("summary") if isinstance(fdec, dict) else "") or "")
    snd_thermo = ((((fdec.get("features") or {}).get("sounding") or {}).get("thermo") if isinstance(fdec, dict) else None) or {})
    cloud_code_now = str(metar_diag.get("latest_cloud_code") or "").upper()
    precip_state = str(metar_diag.get("latest_precip_state") or "none").lower()
    precip_trend = str(metar_diag.get("precip_trend") or "none").lower()

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
            s["advection"] += 0.95
        if _contains_any(txt500, ["槽", "抬升", "PVA", "涡度"]):
            s["dynamic"] += 0.85
        if _contains_any(txtx, ["封盖", "压制", "湿层", "低云", "耦合偏弱"]):
            s["stability"] += 0.9
        if _contains_any(txtx + txt850, ["锋", "锋生", "斜压"]):
            # text-only frontal cues are useful but should not dominate without object support
            s["baroclinic"] += 0.55
            if _contains_any(txt850, ["暖平流", "冷平流"]):
                s["baroclinic"] += 0.15
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
                b_boost = conf_boost
                try:
                    dmin = float(o.get("distance_km_min") or 0.0)
                    if dmin >= 700:
                        b_boost *= 0.75
                except Exception:
                    pass
                if str(o.get("confidence") or "").lower() == "low":
                    b_boost *= 0.85
                s["baroclinic"] += b_boost
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
                wb = w
                try:
                    dmin = float(c.get("distance_km_min") or 0.0)
                    if dmin >= 700:
                        wb *= 0.8
                except Exception:
                    pass
                s["baroclinic"] += wb
            if "shear" in t:
                s["shear"] += w

        # low synoptic coverage: damp baroclinic/shear textual dominance
        try:
            if cov is not None and float(cov) < 0.65:
                s["baroclinic"] *= 0.86
                s["shear"] *= 0.9
        except Exception:
            pass

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
        is_front = ("front" in otype) or ("baroclinic" in otype) or _contains_any(str(line850) + str(extra), ["锋", "锋生", "斜压"])
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

    def _sounding_factor_pack() -> dict[str, Any]:
        def _f(v: Any) -> float | None:
            try:
                return float(v)
            except Exception:
                return None

        low_cloud = _f(calc_window.get("low_cloud_pct"))
        w850 = _f(calc_window.get("w850_kmh"))
        wind_chg = _f(metar_diag.get("wind_dir_change_deg"))
        t_now = _f(metar_diag.get("latest_temp"))
        td_now = _f(metar_diag.get("latest_dewpoint"))
        wx = str(metar_diag.get("latest_wx") or "").upper()

        up_adj = 0.0
        down_adj = 0.0
        profile_score = 0.0
        tags: list[str] = []

        # 1) stability / inversion
        inv = 0.0
        if low_cloud is not None and low_cloud >= 70:
            inv += 0.6
        if w850 is not None and w850 <= 15:
            inv += 0.35
        if "耦合偏弱" in h925_summary:
            inv += 0.35
        if inv >= 0.9:
            down_adj += 0.55
            profile_score += 0.35
            tags.append("逆温/稳定约束偏强")
        elif inv >= 0.45:
            down_adj += 0.25
            profile_score += 0.25
            tags.append("低层稳定约束")

        # 2) convection
        capev = snd_thermo.get("sbcape_jkg") or snd_thermo.get("mlcape_jkg") or snd_thermo.get("mucape_jkg")
        cinv = snd_thermo.get("sbcin_jkg") if snd_thermo.get("sbcin_jkg") is not None else snd_thermo.get("mlcin_jkg")
        if isinstance(capev, (int, float)):
            profile_score += 0.2
            if float(capev) >= 500 and (not isinstance(cinv, (int, float)) or float(cinv) > -75):
                down_adj += 0.2
                tags.append("对流可触发（云发展风险）")
            elif isinstance(cinv, (int, float)) and float(cinv) <= -125:
                up_adj += 0.15
                tags.append("抑制偏强（对流受限）")

        # 3) phase-change / latent-cooling risk
        if any(k in wx for k in ["RA", "SN", "PL", "FZ", "DZ"]):
            if t_now is not None and -1.5 <= t_now <= 2.0:
                down_adj += 0.25
                profile_score += 0.2
                tags.append("近0°C相变/潜热冷却风险")

        # 4) moisture structure (mid-dry / upper moist hints)
        mid_dry = "干层" in h700_summary
        if mid_dry:
            profile_score += 0.45
            if cloud_code_now in {"CLR", "CAVOK", "FEW", "SCT"}:
                up_adj += 0.45
                tags.append("中层偏干+云开（增温效率高）")
            else:
                up_adj += 0.15
                tags.append("中层偏干（但低层云仍有限制）")
        elif ("湿层" in h700_summary) or ("约束" in h700_summary):
            profile_score += 0.35
            down_adj += 0.25
            tags.append("中层湿层约束")

        # 5) shear / mixing
        if w850 is not None:
            if w850 >= 25 and (low_cloud is None or low_cloud <= 55):
                up_adj += 0.2
                profile_score += 0.2
                tags.append("混合条件较好")
            elif w850 <= 12 and low_cloud is not None and low_cloud >= 65:
                down_adj += 0.18
                profile_score += 0.15
                tags.append("混合偏弱")
        if wind_chg is not None and wind_chg >= 45:
            up_adj += 0.08
            down_adj += 0.08
            profile_score += 0.1
            tags.append("风切节奏扰动")

        return {
            "up_adj": up_adj,
            "down_adj": down_adj,
            "profile_score": profile_score,
            "tags": tags,
        }

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
            bsrc = metar_diag.get("temp_bias_smooth_c") if metar_diag.get("temp_bias_smooth_c") is not None else metar_diag.get("temp_bias_c")
            b = float(bsrc) if bsrc is not None else 0.0
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

        # precipitation evolution effect (change > state)
        if precip_trend in {"new", "intensify"}:
            down += 0.75
        elif precip_trend in {"weaken", "end"}:
            up += 0.35
        elif precip_trend == "steady" and precip_state in {"moderate", "heavy", "convective"}:
            down += 0.45
        if precip_state == "convective":
            down += 0.25

        sf = _sounding_factor_pack()
        up += float(sf.get("up_adj") or 0.0)
        down += float(sf.get("down_adj") or 0.0)

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
        sf = _sounding_factor_pack()
        profile_score += float(sf.get("profile_score") or 0.0)

        # obs route
        obs_score = 0.0
        try:
            tb_src = metar_diag.get("temp_bias_smooth_c") if metar_diag.get("temp_bias_smooth_c") is not None else metar_diag.get("temp_bias_c")
            tb = abs(float(tb_src or 0.0))
        except Exception:
            tb = 0.0
        try:
            ts_src = metar_diag.get("temp_trend_smooth_c") if metar_diag.get("temp_trend_smooth_c") is not None else metar_diag.get("temp_trend_1step_c")
            ts = abs(float(ts_src or 0.0))
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

    def _dominant_nature_text(rkey: str, otype: str, fallback_desc: str) -> str:
        fd = _front_plain_desc(otype)
        if rkey == "baroclinic" and fd:
            return fd
        if rkey == "advection":
            if "暖平流" in str(line850) and "冷平流" not in str(line850):
                return "暖输送主导"
            if "冷平流" in str(line850) and "暖平流" not in str(line850):
                return "冷输送主导"
            return "冷暖输送交替"
        if rkey == "stability":
            return "低层稳定约束"
        if rkey == "dynamic":
            return "高空触发主导"
        if rkey == "shear":
            return "风切重排主导"
        return fallback_desc or "背景过渡"

    rs = _regime_scores()
    r_sorted = sorted(rs.items(), key=lambda x: x[1], reverse=True)
    r1, s1 = r_sorted[0]
    r2, s2 = r_sorted[1]
    has_primary_regime = s1 >= 0.9

    if obj:
        otype = str(obj.get("type") or "").lower()
        impact = str(obj.get("impact_scope") or "background_only")
        regime, desc = _infer_regime_and_desc(otype, impact)

        # 1) 主导系统（一句话，含性质）
        if has_primary_regime:
            nature_txt = _dominant_nature_text(r1, otype, regime)
            syn_lines.append(f"- **主导系统**：{_regime_label(r1)}（{nature_txt}）。")
        else:
            nature_txt = _dominant_nature_text("baroclinic" if ("baroclinic" in otype or "front" in otype) else "mixed", otype, desc)
            syn_lines.append(f"- **主导系统**：{regime}（{nature_txt}）。")

        # 2) 落地影响（方向 + 触发 + 交互）
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
            nature_txt = _dominant_nature_text(r1, "", "结构未闭合")
            syn_lines.append(f"- **主导系统**：{_regime_label(r1)}（{nature_txt}；结构未闭合，暂不立3D主系统）。")
        else:
            syn_lines.append("- **主导系统**：当前未识别到可稳定追踪的同一套分层系统。")

        tail = f"。当前组合关系：{inter_note}" if inter_note else ""
        syn_lines.append(f"- **落地影响**：{direction_txt}；短时以实况触发为主。建议：{trigger_txt}{tail}。")

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

    def _h700_dist_km(s: str) -> float | None:
        t = str(s or "")
        m = re.search(r"约\s*([0-9]+(?:\.[0-9]+)?)\s*km", t)
        if not m:
            return None
        try:
            return float(m.group(1))
        except Exception:
            return None

    def _is_generic_500(s: str) -> bool:
        t = str(s or "")
        generic_tokens = [
            "高空仍有抬升触发条件",
            "云层若放开更易再冲高",
            "高空背景信号有限",
            "高空背景一般",
        ]
        return any(k in t for k in generic_tokens)

    evidence_bits: list[str] = []
    if line850_h and not _is_weak_evidence(line850_h):
        evidence_bits.append(f"850hPa: {line850_h}")

    if h700_summary and not _is_weak_evidence(h700_summary):
        d700 = _h700_dist_km(h700_summary)
        h700_key = ("近站" in h700_summary) or ((d700 is not None) and (d700 <= 360)) or ("湿层" in h700_summary) or ("约束" in h700_summary)
        if h700_key:
            evidence_bits.append(f"700hPa: {h700_summary}")

    if line500 and (not _is_weak_evidence(line500)) and (not _is_generic_500(line500)):
        strong500 = any(k in str(line500) for k in ["槽", "短波", "涡度", "PVA", "急流", "冷涡"])
        if strong500:
            evidence_bits.append(f"500hPa: {line500}")

    if h925_summary and not _is_weak_evidence(h925_summary):
        evidence_bits.append(f"925hPa: {h925_summary}")

    if extra and not _is_weak_evidence(extra):
        evidence_bits.append(f"约束: {extra}")

    if evidence_bits:
        if len(evidence_bits) == 1:
            syn_lines.append(f"- **关键证据**：{evidence_bits[0]}。")
        else:
            syn_lines.append("- **关键证据**：")
            for e in evidence_bits[:3]:
                syn_lines.append(f"  • {e}")

    def _sounding_layer_note() -> str | None:
        bits: list[str] = []
        sf = _sounding_factor_pack()

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

        tags = [str(x) for x in (sf.get("tags") or [])]
        for t in tags:
            if t not in bits:
                bits.append(t)

        if cloud_code_now in {"BKN", "OVC", "VV"}:
            bits.append("当前低层云量偏多（地面辐射受限）")

        if not bits:
            return None
        return "；".join(bits[:3]) + "。"

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

    phase_for_syn = str(classify_window_phase(primary_window, metar_diag).get("phase") or "unknown")
    post_mode = str(metar_diag.get("post_window_mode") or "")
    syn_win_label = "峰值窗口"
    if phase_for_syn == "post" and bool(metar_diag.get("post_focus_window_active")):
        syn_win_label = "潜在反超窗口" if post_mode != "no_rebreak_eval" else "后段验证窗口"
    syn_lines.append(
        f"- **{syn_win_label}**：{_hm(syn_w.get('start_local'))}~{_hm(syn_w.get('end_local'))} Local。"
    )

    if compact_synoptic:
        short_cue = "以实况触发为主"
        if "暖平流" in line850 and "冷平流" not in line850:
            short_cue = "暖平流对上沿仍有支撑"
        elif "冷平流" in line850:
            short_cue = "冷平流对上沿有抑制"
        elif "干层" in h700_summary:
            short_cue = "中层偏干有利白天增温"

        # expose thermal-balance/window-prior constraints in human wording
        thermal_txt = ""
        try:
            ph = int(str(primary_window.get("peak_local") or "")[11:13])
        except Exception:
            ph = -1
        try:
            lowc = float(calc_window.get("low_cloud_pct") or 0.0)
        except Exception:
            lowc = 0.0
        try:
            w850 = float(calc_window.get("w850_kmh") or 0.0)
        except Exception:
            w850 = 0.0

        if 13 <= ph <= 15:
            thermal_txt = "热力节律仍指向午后峰值"
        elif ph >= 16:
            thermal_txt = "峰值相位偏后，需看风场/云量是否继续支撑"
        elif 0 <= ph <= 11:
            thermal_txt = "峰值相位偏早，需警惕平流主导改写"

        if lowc >= 75:
            thermal_txt = (thermal_txt + "，低云压制仍在") if thermal_txt else "低云压制仍在"
        elif lowc <= 25 and thermal_txt:
            thermal_txt = thermal_txt + "，辐射效率相对较高"

        if w850 >= 38 and thermal_txt:
            thermal_txt = thermal_txt + "，强风混合使节奏更易重排"

        precip_tail = ""
        if precip_trend in {"new", "intensify"}:
            precip_tail = "；降水正在增强，短时压温风险抬升"
        elif precip_state in {"moderate", "heavy", "convective"}:
            precip_tail = "；降水仍在，白天增温效率受抑"

        tail = f"；{thermal_txt}" if thermal_txt else ""
        syn_lines = [
            "🧭 **今日最高温影响（一句话）**",
            f"- {direction_txt}；{short_cue}，{trigger_txt}{tail}{precip_tail}。",
        ]

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

    peak_c = float(calc_window.get('peak_temp_c') or 0.0)
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
        b_src = metar_diag.get("temp_bias_smooth_c") if metar_diag.get("temp_bias_smooth_c") is not None else metar_diag.get("temp_bias_c")
        b = float(b_src or 0.0)
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
        tstep_src = metar_diag.get("temp_trend_smooth_c") if metar_diag.get("temp_trend_smooth_c") is not None else metar_diag.get("temp_trend_1step_c")
        tstep = float(tstep_src or 0.0)
    except Exception:
        tstep = 0.0

    # Bias/trend-driven center adjustment (quantization-aware):
    # avoid using current absolute temp directly, which can understate rapid-rise regimes before peak.
    b_cap = max(-1.2, min(1.8, b))
    t_up = max(0.0, min(0.9, tstep))
    obs_proj = float(peak_c) + 0.55 * b_cap + 0.35 * t_up
    w_center = {
        "far": 0.18,
        "near_window": 0.26,
        "in_window": 0.35,
        "post": 0.45,
    }.get(phase_now, 0.22)
    center = (1 - w_center) * center + w_center * obs_proj

    # avoid double-counting model priced-in move: only use excess bias above tolerance.
    excess_up = max(0.0, b - 0.9)
    excess_dn = max(0.0, -b - 0.9)
    center += min(0.45, 0.18 * excess_up)
    center -= min(0.45, 0.18 * excess_dn)

    up_s, down_s, _ = _signal_scores()
    denom = max(1e-6, up_s + down_s)
    skew = max(-0.8, min(0.8, (up_s - down_s) / denom))
    if phase_now == "post":
        # Post-window: suppress aggressive one-sided skew tails unless strong rebound is observed in real time.
        skew = max(-0.12, min(0.12, skew))

    # Direction-range consistency correction:
    # when directional evidence is strongly one-sided, interval center should follow with higher weight.
    try:
        b_cons = float((metar_diag.get("temp_bias_smooth_c") if metar_diag.get("temp_bias_smooth_c") is not None else metar_diag.get("temp_bias_c")) or 0.0)
    except Exception:
        b_cons = 0.0
    try:
        t_cons = float((metar_diag.get("temp_trend_smooth_c") if metar_diag.get("temp_trend_smooth_c") is not None else metar_diag.get("temp_trend_1step_c")) or 0.0)
    except Exception:
        t_cons = 0.0

    dir_delta = up_s - down_s
    consistency_shift = 0.0
    if dir_delta >= 0.8:
        consistency_shift += 0.22
        if b_cons >= 1.0:
            consistency_shift += 0.25
        elif b_cons >= 0.6:
            consistency_shift += 0.15
        if t_cons >= 0.4:
            consistency_shift += 0.14
        if phase_now in {"near_window", "in_window"}:
            consistency_shift += 0.08
    elif dir_delta <= -0.8:
        consistency_shift -= 0.22
        if b_cons <= -1.0:
            consistency_shift -= 0.25
        elif b_cons <= -0.6:
            consistency_shift -= 0.15
        if t_cons <= -0.4:
            consistency_shift -= 0.14
        if phase_now in {"near_window", "in_window"}:
            consistency_shift -= 0.08

    # Clear-sky solar guard: under stable clear/less-cloud states, once slope stops accelerating
    # near peak window, avoid inertial warm over-shift.
    cloud_trend_txt = str(metar_diag.get("cloud_trend") or "")
    clear_sky_stable = (
        cloud_code_now in {"CLR", "CAVOK", "FEW", "SCT"}
        and ("回补" not in cloud_trend_txt)
        and ("增加" not in cloud_trend_txt)
    )
    if clear_sky_stable and phase_now in {"near_window", "in_window"}:
        if t_cons <= 0.15:
            consistency_shift *= 0.62
        if b_cons > 0.0 and t_cons <= 0.05:
            consistency_shift = min(consistency_shift, 0.12)

    precip_cooling = False
    precip_residual = False
    if phase_now in {"near_window", "in_window"}:
        if precip_trend in {"new", "intensify"}:
            precip_cooling = True
        elif precip_state in {"moderate", "heavy", "convective"} and precip_trend in {"steady", "none"}:
            precip_cooling = True
        elif precip_trend == "end" and cloud_code_now in {"BKN", "OVC", "VV"}:
            # rain just ended but cloud deck remains: cooling impact can linger
            precip_residual = True

    if precip_cooling:
        consistency_shift = min(consistency_shift, 0.10)
        center -= 0.18
    elif precip_residual:
        consistency_shift = min(consistency_shift, 0.15)
        center -= 0.08

    # Persistent low-cloud guard: avoid pushing center too high when BKN/OVC remains and no opening signal.
    cloud_opening = ("开窗" in str(metar_diag.get("cloud_trend") or "")) or ("减弱" in str(metar_diag.get("cloud_trend") or ""))
    if phase_now in {"near_window", "in_window"} and cloud_code_now in {"BKN", "OVC", "VV"} and not cloud_opening:
        consistency_shift = min(consistency_shift, 0.16)

    consistency_shift = max(-0.75, min(0.75, consistency_shift))
    center += consistency_shift

    if phase_now in {"near_window", "in_window"} and cloud_code_now in {"BKN", "OVC", "VV"} and not cloud_opening:
        # cap center by model peak + modest allowance; only slightly relax if slope is clearly positive
        cap_add = 0.65 + 0.20 * max(0.0, min(0.8, t_cons - 0.35))
        center = min(center, float(peak_c) + cap_add)

    major_half = min(1.05, max(0.45, half_range * 0.68))
    left_hw = major_half * (1.0 - 0.35 * skew)
    right_hw = major_half * (1.0 + 0.35 * skew)

    lo = center - left_hw
    hi = center + right_hw
    if obs_max is not None:
        lo = max(lo, float(obs_max) - 0.25)

    # anti-collapse guard near peak: avoid over-compressing main band when warm-support evidence is aligned.
    sf_local = _sounding_factor_pack()

    try:
        low_cloud_peak = float(calc_window.get("low_cloud_pct") or 0.0)
    except Exception:
        low_cloud_peak = 0.0
    try:
        w850_peak = float(calc_window.get("w850_kmh") or 0.0)
    except Exception:
        w850_peak = 0.0
    if cloud_code_now in {"BKN", "OVC", "VV"}:
        low_cloud_peak = max(low_cloud_peak, 75.0)

    warm_support = 0.0
    if "暖平流" in line850:
        warm_support += 0.6
    if "干层" in h700_summary:
        warm_support += 0.4
    if cloud_code_now not in {"BKN", "OVC", "VV"}:
        warm_support += 0.5
    if isinstance(metar_diag.get("temp_bias_smooth_c"), (int, float)) and float(metar_diag.get("temp_bias_smooth_c") or 0.0) >= 0.5:
        warm_support += 0.4
    warm_support += min(0.6, max(0.0, float(sf_local.get("up_adj") or 0.0) - 0.3 * float(sf_local.get("down_adj") or 0.0)))

    if (
        phase_now in {"near_window", "in_window"}
        and warm_support >= 1.2
        and obs_max is not None
        and low_cloud_peak < 60
        and cloud_code_now not in {"BKN", "OVC", "VV"}
        and (not precip_cooling)
        and (not precip_residual)
    ):
        try:
            peak_local_txt = str(primary_window.get("peak_local") or "")
            latest_local_txt = str(metar_diag.get("latest_report_local") or "")
            if peak_local_txt and latest_local_txt:
                hleft = max(0.0, (datetime.fromisoformat(peak_local_txt) - datetime.fromisoformat(latest_local_txt)).total_seconds() / 3600.0)
            else:
                hleft = 0.0
        except Exception:
            hleft = 0.0

        floor_hi = float(obs_max) + min(4.2, 1.5 + 0.55 * hleft)
        floor_lo = float(obs_max) + min(2.8, 0.9 + 0.35 * hleft)
        hi = max(hi, floor_hi)
        lo = max(lo, min(floor_lo, hi - 0.2))

    # Thermal-balance cap: prevent inertial high overestimation under persistent low-cloud constraint.
    thermal_cap_hi = None
    if phase_now in {"near_window", "in_window"}:
        if low_cloud_peak >= 80:
            thermal_cap_hi = float(peak_c) + 0.9
        elif low_cloud_peak >= 65:
            thermal_cap_hi = float(peak_c) + 1.2

        if precip_cooling:
            thermal_cap_hi = min(thermal_cap_hi, float(peak_c) + 1.0) if thermal_cap_hi is not None else (float(peak_c) + 1.0)
        elif precip_residual:
            thermal_cap_hi = min(thermal_cap_hi, float(peak_c) + 1.05) if thermal_cap_hi is not None else (float(peak_c) + 1.05)

        if thermal_cap_hi is not None:
            if t_cons <= 0.15:
                thermal_cap_hi -= 0.15
            if w850_peak >= 30:
                thermal_cap_hi += 0.20  # strong-wind cities can keep mixed layer warmer
            if "暖平流" in line850 and b_cons >= 1.0:
                thermal_cap_hi += 0.10
            if precip_trend in {"new", "intensify"}:
                thermal_cap_hi -= 0.20
            if obs_max is not None:
                thermal_cap_hi = max(thermal_cap_hi, float(obs_max) + 0.6)
            hi = min(hi, thermal_cap_hi)
            lo = min(lo, hi - 0.2)

    strict_late_cap = False

    # Afternoon solar-decay + plateau gate:
    # if already in/near window with weak temp slope, avoid optimistic late-window rebound tails.
    if phase_now in {"near_window", "in_window"} and clear_sky_stable and (not precip_cooling) and (not precip_residual):
        try:
            latest_local_txt = str(metar_diag.get("latest_report_local") or "")
            peak_local_txt = str(primary_window.get("peak_local") or "")
            latest_dt = datetime.fromisoformat(latest_local_txt) if latest_local_txt else None
            peak_dt = datetime.fromisoformat(peak_local_txt) if peak_local_txt else None
            if latest_dt is not None and peak_dt is not None:
                if latest_dt.tzinfo is not None and peak_dt.tzinfo is None:
                    peak_dt = peak_dt.replace(tzinfo=latest_dt.tzinfo)
                elif latest_dt.tzinfo is None and peak_dt.tzinfo is not None:
                    latest_dt = latest_dt.replace(tzinfo=peak_dt.tzinfo)
            hour_local = (latest_dt.hour + latest_dt.minute / 60.0) if latest_dt else None
            hleft = max(0.0, (peak_dt - latest_dt).total_seconds() / 3600.0) if (latest_dt and peak_dt) else None
        except Exception:
            hour_local = None
            hleft = None
        try:
            t_now = float(metar_diag.get("latest_temp"))
        except Exception:
            t_now = None

        if t_now is not None and t_cons <= 0.12:
            late_enough = (hour_local is None) or (hour_local >= 14.0)
            close_to_peak = (hleft is None) or (hleft <= 1.6)
            if late_enough and close_to_peak:
                add = 1.05
                if hour_local is not None and hour_local >= 15.0:
                    add = 0.85
                if "暖平流" in line850 and b_cons >= 0.6 and t_cons > 0.05:
                    add += 0.15
                solar_plateau_cap = t_now + add
                if obs_max is not None:
                    solar_plateau_cap = max(solar_plateau_cap, float(obs_max) + 0.35)
                hi = min(hi, solar_plateau_cap)
                lo = min(lo, hi - 0.2)

    # Quantized-METAR near-end guard:
    # many stations report integer-like temp steps; when close to window end, avoid reading a single +1C step
    # as sustained acceleration.
    if phase_now in {"near_window", "in_window"} and bool(metar_diag.get("metar_temp_quantized")):
        try:
            latest_local_txt = str(metar_diag.get("latest_report_local") or "")
            end_local_txt = str(primary_window.get("end_local") or "")
            latest_dt = datetime.fromisoformat(latest_local_txt) if latest_local_txt else None
            end_dt = datetime.fromisoformat(end_local_txt) if end_local_txt else None
            if latest_dt is not None and end_dt is not None:
                if latest_dt.tzinfo is not None and end_dt.tzinfo is None:
                    end_dt = end_dt.replace(tzinfo=latest_dt.tzinfo)
                elif latest_dt.tzinfo is None and end_dt.tzinfo is not None:
                    latest_dt = latest_dt.replace(tzinfo=end_dt.tzinfo)
            h_to_end = max(0.0, (end_dt - latest_dt).total_seconds() / 3600.0) if (latest_dt and end_dt) else None
        except Exception:
            h_to_end = None
        try:
            t_now = float(metar_diag.get("latest_temp"))
        except Exception:
            t_now = None
        if t_now is not None and (h_to_end is not None) and h_to_end <= 1.0:
            q_add = 0.95
            if t_cons <= 0.20:
                q_add = 0.65
            elif t_cons <= 0.45:
                q_add = 0.80
            elif t_cons <= 0.75:
                q_add = 0.95
            else:
                q_add = 1.05
            if "暖平流" in line850 and b_cons >= 0.8 and t_cons >= 0.4:
                q_add += 0.10
            q_cap = t_now + q_add
            if obs_max is not None:
                q_cap = max(q_cap, float(obs_max) + 0.25)
            hi = min(hi, q_cap)
            lo = min(lo, hi - 0.2)
            strict_late_cap = True

    # Post-window realized-peak guard:
    # once peak window is over, avoid optimistic "new high" tails unless there is clear rebound evidence.
    if phase_now == "post" and obs_max is not None:
        try:
            latest_local_txt = str(metar_diag.get("latest_report_local") or "")
            obs_peak_local_txt = str(metar_diag.get("observed_max_time_local") or "")
            latest_dt = datetime.fromisoformat(latest_local_txt) if latest_local_txt else None
            obs_peak_dt = datetime.fromisoformat(obs_peak_local_txt) if obs_peak_local_txt else None
            if latest_dt is not None and obs_peak_dt is not None:
                if latest_dt.tzinfo is not None and obs_peak_dt.tzinfo is None:
                    obs_peak_dt = obs_peak_dt.replace(tzinfo=latest_dt.tzinfo)
                elif latest_dt.tzinfo is None and obs_peak_dt.tzinfo is not None:
                    latest_dt = latest_dt.replace(tzinfo=obs_peak_dt.tzinfo)
            h_since_obs_peak = max(0.0, (latest_dt - obs_peak_dt).total_seconds() / 3600.0) if (latest_dt and obs_peak_dt) else None
        except Exception:
            h_since_obs_peak = None

        try:
            t_now = float(metar_diag.get("latest_temp"))
        except Exception:
            t_now = None

        wet_now = (precip_state in {"light", "moderate", "heavy", "convective"}) or (precip_trend in {"new", "intensify", "steady", "end"})
        cloudy_now = cloud_code_now in {"BKN", "OVC", "VV"}

        rebound_ok = (
            clear_sky_stable
            and (not wet_now)
            and (not cloudy_now)
            and t_cons >= 0.45
            and b_cons >= 0.4
            and ((h_since_obs_peak is None) or (h_since_obs_peak <= 2.0))
        )

        if rebound_ok:
            post_add = 1.15
        else:
            post_add = 0.55
            if h_since_obs_peak is not None and h_since_obs_peak >= 4.0:
                post_add = 0.35
            if cloudy_now:
                post_add -= 0.10
            if wet_now:
                post_add -= 0.10
            if t_cons <= 0.05:
                post_add -= 0.05
            post_add = max(0.20, post_add)

        post_cap_hi = float(obs_max) + post_add

        # Programmatic re-break feasibility gate:
        # when the prospective secondary-peak model itself does not challenge obs max,
        # or current weather remains wet/cloudy with weak slope, cap aggressively.
        try:
            pf_peak = float(metar_diag.get("post_focus_peak_temp_c")) if metar_diag.get("post_focus_peak_temp_c") is not None else None
        except Exception:
            pf_peak = None
        if pf_peak is not None and pf_peak <= float(obs_max) - 0.4:
            post_cap_hi = min(post_cap_hi, float(obs_max) + 0.25)
        if bool(metar_diag.get("post_focus_window_active")) and (wet_now or cloudy_now) and t_cons <= 0.2:
            post_cap_hi = min(post_cap_hi, float(obs_max) + 0.25)

        if t_now is not None:
            post_cap_hi = max(post_cap_hi, t_now + 0.15)

        hi = min(hi, post_cap_hi)
        lo = min(lo, hi - 0.2)

    # Far-window cold-advection sanity cap: avoid over-projecting rapid afternoon rebound
    # when low-level northerly cold feed persists.
    if phase_now == "far" and "冷平流" in line850:
        try:
            wdir_now = float(metar_diag.get("latest_wdir")) if metar_diag.get("latest_wdir") not in (None, "", "VRB") else None
        except Exception:
            wdir_now = None
        northerly = (wdir_now is not None) and ((wdir_now >= 300.0) or (wdir_now <= 60.0))
        if northerly and b_cons <= 0.3:
            try:
                peak_local_txt = str(primary_window.get("peak_local") or "")
                latest_local_txt = str(metar_diag.get("latest_report_local") or "")
                if peak_local_txt and latest_local_txt:
                    hleft = max(0.0, (datetime.fromisoformat(peak_local_txt) - datetime.fromisoformat(latest_local_txt)).total_seconds() / 3600.0)
                else:
                    hleft = 2.0
            except Exception:
                hleft = 2.0
            try:
                t_now = float(metar_diag.get("latest_temp"))
            except Exception:
                t_now = None
            if t_now is not None:
                rise_cap = min(3.2, 1.6 + 0.45 * hleft)
                far_cap_hi = t_now + rise_cap
                if obs_max is not None:
                    far_cap_hi = max(far_cap_hi, float(obs_max) + 0.3)
                hi = min(hi, far_cap_hi)
                lo = min(lo, hi - 0.2)

    # Physical consistency: daily Tmax cannot be below already observed daily max.
    if obs_max is not None:
        lo = max(lo, float(obs_max) - 0.05)
        hi = max(hi, lo + 0.2)

    def _soft_snap(v: float) -> float:
        iv = round(v)
        if abs(v - iv) <= 0.12:
            return float(iv)
        return round(v, 1)

    lo = _soft_snap(lo)
    hi = _soft_snap(max(hi, lo + 0.2))

    peak_range_block = ["🌡️ **可能最高温区间**"]

    if bool(metar_diag.get("post_focus_window_active")) and syn_w:
        post_mode = str(metar_diag.get("post_window_mode") or "")
        window_label = "潜在二峰窗" if post_mode != "no_rebreak_eval" else "后段验证窗"
        window_txt = f"{_hm(syn_w.get('start_local'))}~{_hm(syn_w.get('end_local'))} Local"
    else:
        window_label = "峰值窗"
        window_txt = f"{_hm(primary_window.get('start_local'))}~{_hm(primary_window.get('end_local'))} Local"
    cloud_code = str(metar_diag.get("latest_cloud_code") or "").upper()
    if cloud_code in {"CLR", "CAVOK"}:
        tail_up_cond = "若晴空维持且升温斜率延续"
    elif cloud_code in {"FEW", "SCT"}:
        tail_up_cond = "若少云继续维持且升温斜率延续"
    else:
        tail_up_cond = "若云量继续开窗且升温斜率延续"

    core_lo, core_hi = lo, hi
    disp_lo, disp_hi = lo, hi

    if skew >= 0.20:
        tail_ext = min(0.8, 0.4 + 0.3 * max(0.0, skew))
        if strict_late_cap:
            tail_ext = min(tail_ext, 0.18)
        if clear_sky_stable and phase_now in {"near_window", "in_window"} and t_cons <= 0.15:
            tail_ext = min(tail_ext, 0.45)
        if phase_now in {"near_window", "in_window"} and low_cloud_peak >= 70 and t_cons <= 0.20:
            tail_ext = min(tail_ext, 0.25)
        if precip_cooling:
            tail_ext = min(tail_ext, 0.20)
        elif precip_residual:
            tail_ext = min(tail_ext, 0.24)
        tail_hi = _soft_snap(hi + tail_ext)
        disp_hi = tail_hi
        peak_range_block.append(
            f"- **{_fmt_range(disp_lo, disp_hi)}**（主看 {_fmt_range(core_lo, core_hi)}；{window_label} {window_txt}；{tail_up_cond}）"
        )
    elif skew <= -0.20:
        tail_lo = _soft_snap(max(lo - min(0.8, 0.4 + 0.3 * max(0.0, -skew)), lo - 1.0))
        disp_lo = tail_lo
        peak_range_block.append(
            f"- **{_fmt_range(disp_lo, disp_hi)}**（主看 {_fmt_range(core_lo, core_hi)}；{window_label} {window_txt}；若云量回补并伴随偏冷来流增强）"
        )
    else:
        peak_range_block.append(f"- **{_fmt_range(disp_lo, disp_hi)}**（{window_label} {window_txt}）")
    if bool(metar_diag.get("obs_correction_applied")):
        peak_range_block.append("- 注：已应用实况纠偏（模型峰值偏低，窗口锚定到当日实况峰值时段）。")

    phase_map = {"far": "远离窗口", "near_window": "接近窗口", "in_window": "窗口内", "post": "窗口后", "unknown": "窗口状态未知"}
    vars_block = [f"⚠️ **关注变量**（{phase_map.get(phase_now, '窗口状态未知')}）"]

    t_bias = metar_diag.get("temp_bias_smooth_c") if metar_diag.get("temp_bias_smooth_c") is not None else metar_diag.get("temp_bias_c")
    t_tr = metar_diag.get("temp_trend_smooth_c") if metar_diag.get("temp_trend_smooth_c") is not None else metar_diag.get("temp_trend_1step_c")
    cloud_tr = str(metar_diag.get("cloud_trend") or "")
    focus: list[tuple[float, str]] = []

    # 温度趋势（优先）
    try:
        tv = float(t_tr or 0.0)
        if tv >= 0.6:
            focus.append((1.0, "• 未来30-60分钟升温斜率若继续维持正值 → 最高温上沿仍可上修。"))
        elif tv <= -0.6:
            focus.append((1.0, "• 短时斜率若持续转负 → 峰值可能提前锁定并压低上沿。"))
        else:
            focus.append((0.55, "• 先盯温度斜率是否重新放大，这是临窗改判的最快信号。"))
    except Exception:
        focus.append((0.45, "• 先盯温度斜率是否重新放大，这是临窗改判的最快信号。"))

    # 偏差驱动
    if isinstance(t_bias, (int, float)):
        if t_bias >= 1.5:
            focus.append((0.95, "• 实况持续高于同小时模式（偏暖延续） → 最高温更偏上沿。"))
        elif t_bias <= -1.5:
            focus.append((0.95, "• 实况持续低于同小时模式（偏冷延续） → 最高温更偏下沿。"))

    # 风场重排（给出具体方向场景）
    try:
        wdir = metar_diag.get("latest_wdir")
        wspd = metar_diag.get("latest_wspd")
        wdchg = float(metar_diag.get("wind_dir_change_deg") or 0.0)
        st_lat = float(metar_diag.get("station_lat") or 0.0)
        nh = st_lat >= 0
        warm_sector = "偏南到西南" if nh else "偏北到西北"
        cool_sector = "偏北到东北" if nh else "偏南到东南"

        if wdir not in (None, "", "VRB") and wspd is not None:
            try:
                ws = float(wspd)
                wind_gate = int(max(14.0, round(ws + 3.0)))
            except Exception:
                ws = None
                wind_gate = 15

            sc = 0.95 if wdchg >= 35 else 0.72
            if "冷平流" in line850:
                txt = (
                    f"• 风场改判阈值：若转{cool_sector}并增至≈{wind_gate}kt以上，冷输送压温会更明显；"
                    f"若回摆到{warm_sector}且风速回落，才可能释放小幅反超空间（当前{wdir}° {wspd}kt）。"
                )
            elif "暖平流" in line850:
                txt = (
                    f"• 风场改判阈值：若转{warm_sector}并增至≈{wind_gate}kt以上，暖输送更易落地；"
                    f"若转{cool_sector}并增强，后段上沿会被压住（当前{wdir}° {wspd}kt）。"
                )
            else:
                txt = (
                    f"• 风场改判阈值：若转{cool_sector}并增至≈{wind_gate}kt以上，多偏压温；"
                    f"若转{warm_sector}且维持正斜率，才有后段上修空间（当前{wdir}° {wspd}kt）。"
                )
            focus.append((sc, txt))
        else:
            focus.append((0.45, "• 近地风场若由不定转为稳定单一来流：偏冷象限通常压温，偏暖象限才支持后段反超。"))
    except Exception:
        focus.append((0.45, "• 近地风向/风速若突变 → 峰值出现时段与幅度可能改写。"))

    # 云量只在“有信号”时上提，不再每次默认主重点
    if cloud_code in {"BKN", "OVC", "VV"}:
        focus.append((0.95, "• 低云维持/继续增厚 → 最高温上沿下压，峰值可能提前结束。"))
    elif ("回补" in cloud_tr) or ("增加" in cloud_tr):
        focus.append((0.85, "• 云量回补迹象增强 → 临窗压制风险抬升。"))
    elif ("开窗" in cloud_tr) or ("减弱" in cloud_tr):
        focus.append((0.7, "• 云量转疏在延续 → 地面增温效率仍有支撑。"))

    # precipitation evolution: prioritize change signal
    if precip_trend in {"new", "intensify"}:
        focus.append((1.08, "• 降水出现/增强 → 上沿偏下修，且短时不确定性增大（对降水强度/相态变化敏感）。"))
    elif precip_trend in {"weaken", "end"}:
        focus.append((0.82, "• 降水减弱/结束 → 压温约束减轻，若云层不回补上沿可恢复。"))
    elif precip_state in {"moderate", "heavy", "convective"}:
        focus.append((0.9, "• 降水持续 → 白天增温效率受抑，最高温更偏下沿；短时波动可能放大。"))


    try:
        if snd_thermo.get("has_profile"):
            capev = snd_thermo.get("sbcape_jkg") or snd_thermo.get("mlcape_jkg") or snd_thermo.get("mucape_jkg")
            cinv = snd_thermo.get("sbcin_jkg") if snd_thermo.get("sbcin_jkg") is not None else snd_thermo.get("mlcin_jkg")
            if isinstance(capev, (int, float)) and capev >= 300 and (not isinstance(cinv, (int, float)) or cinv > -75):
                focus.append((0.8, "• 探空显示可用对流能量且抑制偏弱 → 午后云量/阵性扰动上升，峰值波动风险增加。"))
            elif isinstance(cinv, (int, float)) and cinv <= -125:
                focus.append((0.6, "• 探空抑制偏强（CIN较大） → 对流触发受限，升温路径更看近地风场。"))
    except Exception:
        pass

    # P1 short-term triggers (window-gated)
    try:
        rt_triggers = select_realtime_triggers(primary_window, metar_diag)
        phase_now = str(gate.get("phase") or "unknown")
        focus_sorted = [txt for _s, txt in sorted(focus, key=lambda x: x[0], reverse=True)]
        # 去重保序
        uniq_focus: list[str] = []
        for t in focus_sorted:
            if t not in uniq_focus:
                uniq_focus.append(t)

        if rt_triggers:
            if phase_now == "post":
                post_extras = [
                    t for t in uniq_focus
                    if (
                        ("云" in t) or ("降水" in t) or ("风" in t) or ("斜率" in t)
                    ) and ("偏暖延续" not in t) and ("偏冷延续" not in t)
                ]
                merged = []
                for t in (rt_triggers + post_extras):
                    if t not in merged:
                        merged.append(t)
                vars_block = vars_block[:1] + merged[:3]
            else:
                vars_block = vars_block[:1] + rt_triggers[:3]
        else:
            if phase_now == "far":
                uniq_focus = ["• 当前远离峰值窗口：先跟踪上午到中午的升温斜率是否连续转正。"] + [x for x in uniq_focus if "远离峰值窗口" not in x]
            vars_block = vars_block[:1] + uniq_focus[:3]
            if len(vars_block) == 1:
                vars_block.append("• 临窗前继续跟踪温度斜率与风向节奏，必要时再改判。")
    except Exception as _e:
        if str(os.getenv("LOOK_DEBUG_ERRORS", "0") or "0").lower() in {"1", "true", "yes", "on"}:
            vars_block = vars_block[:1] + [
                f"• 变量块调试：{type(_e).__name__}: {_e}",
            ]
        else:
            vars_block = vars_block[:1] + [
                "• 临窗前继续跟踪温度斜率与风向节奏，必要时再改判。"
            ]

    try:
        poly_block = _build_polymarket_section(
            polymarket_event_url,
            primary_window,
            metar_diag=metar_diag,
            range_hint={
                "display_lo": float(disp_lo),
                "display_hi": float(disp_hi),
                "core_lo": float(core_lo),
                "core_hi": float(core_hi),
            },
        )
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

    now_utc = datetime.now(timezone.utc)
    if raw_target_date:
        target_date = raw_target_date
    else:
        try:
            target_date = now_utc.astimezone(ZoneInfo(tz_name_station)).strftime("%Y-%m-%d")
        except Exception:
            target_date = now_utc.strftime("%Y-%m-%d")

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
    try:
        metar_diag["station_lat"] = float(st.lat)
        metar_diag["station_lon"] = float(st.lon)
    except Exception:
        pass
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

    analysis_window = dict(primary)
    try:
        gate_pre = classify_window_phase(primary, metar_diag)
        if str(gate_pre.get("phase") or "") == "post":
            post_focus = _build_post_focus_window(hourly_day, metar_diag)
            if isinstance(post_focus, dict) and post_focus.get("peak_local"):
                analysis_window = post_focus
                metar_diag["post_focus_window_active"] = True
                metar_diag["post_window_mode"] = "focus_rebreak"
                metar_diag["post_focus_peak_local"] = str(post_focus.get("peak_local") or "")
                try:
                    pf = float(post_focus.get("peak_temp_c"))
                    metar_diag["post_focus_peak_temp_c"] = pf
                    try:
                        obs_mx = float(metar_diag.get("observed_max_temp_c")) if metar_diag.get("observed_max_temp_c") is not None else None
                    except Exception:
                        obs_mx = None
                    if obs_mx is not None and pf <= obs_mx - 0.4:
                        metar_diag["post_window_mode"] = "no_rebreak_eval"
                except Exception:
                    pass
            else:
                post_eval = _build_post_eval_window(hourly_day, metar_diag)
                if isinstance(post_eval, dict) and post_eval.get("peak_local"):
                    analysis_window = post_eval
                    metar_diag["post_focus_window_active"] = True
                    metar_diag["post_window_mode"] = "no_rebreak_eval"
    except Exception:
        analysis_window = dict(primary)

    peak_local_dt = datetime.strptime(analysis_window["peak_local"], "%Y-%m-%dT%H:%M").replace(tzinfo=tz)
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
        primary_window=analysis_window,
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

    gate_now = classify_window_phase(primary, metar_diag)
    phase_now = str(gate_now.get("phase") or "unknown")
    compact_synoptic = phase_now in {"near_window", "in_window"}

    header_lines = [
        f"📍 **{st.icao} ({st.city}) | {head_geo}**",
        f"生成时间: {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M')} UTC | {now_local.strftime('%H:%M')} Local (UTC{now_local.strftime('%z')[:3]})",
    ]
    if not compact_synoptic:
        header_lines.append(
            f"分析基准模型: {analysis_model.upper()}（运行时次: {rt_fmt}） | 小时预报源: {provider_used} | 3D场源: {SYNOPTIC_PROVIDER}"
        )
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
    unit_pref = "F" if str(st.icao).upper().startswith("K") else "C"
    body = choose_section_text(
        primary,
        metar_text,
        metar_diag,
        links_payload["links"]["polymarket_event"],
        forecast_decision=forecast_decision,
        compact_synoptic=compact_synoptic,
        temp_unit=unit_pref,
        synoptic_window=analysis_window,
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
