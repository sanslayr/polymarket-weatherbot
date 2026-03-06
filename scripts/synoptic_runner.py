from __future__ import annotations

import hashlib
import json
import os
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Callable
from zoneinfo import ZoneInfo

from build_2d_grid_payload import build_2d_grid_payload_openmeteo
from contracts import SYNOPTIC_CACHE_SCHEMA_VERSION
from cache_envelope import extract_payload, make_cache_doc
from runtime_cache_policy import runtime_cache_enabled
from synoptic_2d_detector import analyze as analyze_synoptic_2d


def _cache_key(*parts: str) -> str:
    raw = "|".join(parts)
    return hashlib.sha1(raw.encode("utf-8")).hexdigest()[:16]


def _cache_path(cache_dir: Path, kind: str, *parts: str) -> Path:
    return cache_dir / f"{kind}_{_cache_key(*parts)}.json"


def _read_cache(cache_dir: Path, kind: str, *parts: str) -> dict[str, Any] | None:
    if not runtime_cache_enabled():
        return None
    p = _cache_path(cache_dir, kind, *parts)
    if not p.exists():
        return None
    try:
        doc = json.loads(p.read_text(encoding="utf-8"))
        payload, _updated_at, env = extract_payload(doc)
        if isinstance(payload, dict) and isinstance(payload.get("scale_summary"), dict):
            return payload
        # ultra-legacy fallback
        if isinstance(doc, dict) and isinstance(doc.get("scale_summary"), dict):
            return doc
        # legacy wrapper using schema_version on top-level
        if isinstance(doc, dict) and doc.get("schema_version") == SYNOPTIC_CACHE_SCHEMA_VERSION and isinstance(doc.get("payload"), dict):
            return doc.get("payload")
        _ = env  # keep unpacked for forward-compatible extension
    except Exception:
        return None
    return None


def _write_cache(cache_dir: Path, kind: str, payload: dict[str, Any], *parts: str) -> None:
    if not runtime_cache_enabled():
        return
    cache_dir.mkdir(parents=True, exist_ok=True)
    p = _cache_path(cache_dir, kind, *parts)
    doc = make_cache_doc(
        payload,
        source_state="fresh",
        payload_schema_version=SYNOPTIC_CACHE_SCHEMA_VERSION,
        meta={"kind": kind},
    )
    p.write_text(json.dumps(doc, ensure_ascii=False), encoding="utf-8")


def _is_rate_limit_error(exc: Exception) -> bool:
    msg = str(exc)
    if "429" in msg or "Too Many Requests" in msg:
        return True
    stderr = getattr(exc, "stderr", None)
    if stderr and ("429" in str(stderr) or "Too Many Requests" in str(stderr)):
        return True
    return False


def _classify_error_type(msg: str) -> str:
    s = str(msg or "").lower()
    if "429" in s or "too many requests" in s or "rate_limit" in s:
        return "rate_limit_429"
    if "404" in s or "not found" in s:
        return "not_found_404"
    if "timed out" in s or "timeout" in s:
        return "timeout"
    if "connection" in s or "ssl" in s or "dns" in s:
        return "network"
    if "subprocess" in s or "parse failed" in s:
        return "subprocess"
    return "unknown"


def _short_err(exc: Exception | str, n: int = 280) -> str:
    t = str(exc)
    t = " ".join(t.split())
    return t[:n]


def run_synoptic_section(
    *,
    st: Any,
    target_date: str,
    peak_local: str,
    tz_name: str,
    model: str,
    runtime_tag: str,
    scripts_dir: Path,
    cache_dir: Path,
    provider: str = "openmeteo",
    pass_mode: str = "full",
    perf_log: Callable[[str, float], None] | None = None,
) -> dict[str, Any]:
    mode = str(pass_mode or "full").strip().lower()
    if mode not in {"full", "inner_only", "outer500_only"}:
        mode = "full"

    cache_parts = (
        st.icao,
        target_date,
        model.lower(),
        str(provider or "").lower(),
        mode,
        runtime_tag,
        peak_local,
        tz_name,
        SYNOPTIC_CACHE_SCHEMA_VERSION,
    )
    cached = _read_cache(cache_dir, "synoptic", *cache_parts)
    if cached:
        return cached

    tz = ZoneInfo(tz_name)
    peak_local_dt = datetime.strptime(peak_local, "%Y-%m-%dT%H:%M").replace(tzinfo=tz)
    prev_local_dt = peak_local_dt - timedelta(hours=6)
    all_passes = [
        {
            "name": "inner",
            "lat_span": float(os.getenv("FORECAST_INNER_LAT_SPAN", "6.0") or "6.0"),
            "lon_span": float(os.getenv("FORECAST_INNER_LON_SPAN", "8.0") or "8.0"),
            "step": float(os.getenv("FORECAST_INNER_STEP_DEG", "1.0") or "1.0"),
            "batch": int(os.getenv("FORECAST_INNER_BATCH_SIZE", "80") or "80"),
            "field_profile": "full",
            "detector_mode": "full",
            "history": True,
        },
        {
            "name": "outer500",
            "lat_span": float(os.getenv("FORECAST_OUTER500_LAT_SPAN", "30.0") or "30.0"),
            "lon_span": float(os.getenv("FORECAST_OUTER500_LON_SPAN", "40.0") or "40.0"),
            "step": float(os.getenv("FORECAST_OUTER500_STEP_DEG", "2.0") or "2.0"),
            "batch": int(os.getenv("FORECAST_OUTER500_BATCH_SIZE", "120") or "120"),
            "field_profile": "outer500",
            "detector_mode": "outer500_only",
            "history": True,
        },
    ]
    if mode == "inner_only":
        passes = [all_passes[0]]
    elif mode == "outer500_only":
        passes = [all_passes[1]]
    else:
        passes = all_passes
    last_err: Exception | None = None
    collected: list[dict[str, Any]] = []
    pass_events: list[dict[str, Any]] = []

    for i, cfg in enumerate(passes, start=1):
        pass_t0 = time.perf_counter()
        ev: dict[str, Any] = {
            "pass": str(cfg.get("name") or i),
            "provider": str(provider or ""),
            "status": "running",
        }
        start_date = min(prev_local_dt.date(), peak_local_dt.date()).isoformat()
        end_date = max(prev_local_dt.date(), peak_local_dt.date()).isoformat()

        t_build = time.perf_counter()
        try:
            if provider == "gfs-grib2":
                from gfs_grib_provider import build_2d_grid_payload_gfs
                payload = build_2d_grid_payload_gfs(
                    station_icao=st.icao,
                    station_lat=float(st.lat),
                    station_lon=float(st.lon),
                    lat_min=float(st.lat - cfg["lat_span"]),
                    lat_max=float(st.lat + cfg["lat_span"]),
                    lon_min=float(st.lon - cfg["lon_span"]),
                    lon_max=float(st.lon + cfg["lon_span"]),
                    analysis_time_local=peak_local_dt.strftime("%Y-%m-%dT%H:%M"),
                    previous_time_local=prev_local_dt.strftime("%Y-%m-%dT%H:%M"),
                    tz_name=tz_name,
                    cycle_tag=runtime_tag,
                    field_profile=str(cfg.get("field_profile") or "full"),
                    root=scripts_dir.parents[2],
                )
            else:
                payload = build_2d_grid_payload_openmeteo(
                    station_icao=st.icao,
                    station_lat=float(st.lat),
                    station_lon=float(st.lon),
                    lat_min=float(st.lat - cfg["lat_span"]),
                    lat_max=float(st.lat + cfg["lat_span"]),
                    lon_min=float(st.lon - cfg["lon_span"]),
                    lon_max=float(st.lon + cfg["lon_span"]),
                    step_deg=float(cfg["step"]),
                    analysis_time=peak_local_dt.strftime("%Y-%m-%dT%H:%M"),
                    previous_time=prev_local_dt.strftime("%Y-%m-%dT%H:%M"),
                    start_date=start_date,
                    end_date=end_date,
                    batch_size=int(cfg["batch"]),
                    field_profile=str(cfg.get("field_profile") or "full"),
                )
            ev["build_s"] = round(time.perf_counter() - t_build, 3)
            if perf_log:
                perf_log(f"synoptic.{cfg['name']}.build", float(ev["build_s"]))
        except Exception as exc:
            last_err = exc
            ev.update({
                "status": "failed",
                "stage": "build",
                "error_type": _classify_error_type(str(exc)),
                "error": _short_err(exc),
                "elapsed_s": round(time.perf_counter() - pass_t0, 3),
            })
            pass_events.append(ev)
            if perf_log:
                perf_log(f"synoptic.{cfg['name']}.failed", time.perf_counter() - pass_t0)
            if _is_rate_limit_error(exc):
                break
            continue

        try:
            t_detect = time.perf_counter()
            data = analyze_synoptic_2d(payload, mode=str(cfg.get("detector_mode") or "full"))
            ev["detect_s"] = round(time.perf_counter() - t_detect, 3)
            if perf_log:
                perf_log(f"synoptic.{cfg['name']}.detect", float(ev["detect_s"]))
        except Exception as exc:
            last_err = exc
            ev.update({
                "status": "failed",
                "stage": "detect",
                "error_type": _classify_error_type(str(exc)),
                "error": _short_err(exc),
                "elapsed_s": round(time.perf_counter() - pass_t0, 3),
            })
            pass_events.append(ev)
            if perf_log:
                perf_log(f"synoptic.{cfg['name']}.failed", time.perf_counter() - pass_t0)
            if _is_rate_limit_error(exc):
                break
            continue

        if cfg.get("name") == "outer500":
            cur = ((data.get("scale_summary") or {}).get("synoptic") or {}).get("systems", [])
            cur = [s for s in cur if str(s.get("level") or "") == "500"]
            data.setdefault("scale_summary", {}).setdefault("synoptic", {})["systems"] = cur

        systems_cnt = len(((data.get("scale_summary") or {}).get("synoptic") or {}).get("systems") or [])
        ev.update({
            "status": "ok",
            "stage": "done",
            "systems": int(systems_cnt),
            "elapsed_s": round(time.perf_counter() - pass_t0, 3),
        })
        pass_events.append(ev)
        if perf_log:
            perf_log(f"synoptic.{cfg['name']}.total", time.perf_counter() - pass_t0)
        collected.append(data)

    if collected:
        merged = {"scale_summary": {"synoptic": {"systems": []}}}
        out = merged["scale_summary"]["synoptic"]["systems"]
        seen: set[tuple[str, str, int, int]] = set()
        for data in collected:
            systems = ((data.get("scale_summary") or {}).get("synoptic") or {}).get("systems", [])
            for s in systems:
                k = (
                    str(s.get("level") or ""),
                    str(s.get("system_type") or ""),
                    int(round(float(s.get("center_lat") or 0.0) * 10)),
                    int(round(float(s.get("center_lon") or 0.0) * 10)),
                )
                if k in seen:
                    continue
                seen.add(k)
                out.append(s)

        merged["_telemetry"] = {
            "provider": str(provider or ""),
            "pass_mode": mode,
            "passes": pass_events,
            "degraded": any(str(e.get("status")) == "failed" for e in pass_events),
        }
        _write_cache(cache_dir, "synoptic", merged, *cache_parts)
        return merged

    stale = _read_cache(cache_dir, "synoptic", *cache_parts)
    if stale:
        stale.setdefault("_telemetry", {
            "provider": str(provider or ""),
            "pass_mode": mode,
            "passes": pass_events,
            "degraded": True,
            "from_cache": True,
        })
        return stale

    err_txt = _short_err(last_err or "synoptic_unknown_error")
    raise RuntimeError(f"synoptic section failed: {err_txt}")
