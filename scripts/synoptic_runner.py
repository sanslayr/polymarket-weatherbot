from __future__ import annotations

import hashlib
import json
import math
import subprocess
import tempfile
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Callable
from zoneinfo import ZoneInfo

from contracts import SYNOPTIC_CACHE_SCHEMA_VERSION


def _cache_key(*parts: str) -> str:
    raw = "|".join(parts)
    return hashlib.sha1(raw.encode("utf-8")).hexdigest()[:16]


def _cache_path(cache_dir: Path, kind: str, *parts: str) -> Path:
    return cache_dir / f"{kind}_{_cache_key(*parts)}.json"


def _read_cache(cache_dir: Path, kind: str, *parts: str) -> dict[str, Any] | None:
    p = _cache_path(cache_dir, kind, *parts)
    if not p.exists():
        return None
    try:
        doc = json.loads(p.read_text(encoding="utf-8"))
        # v2 wrapped cache
        if isinstance(doc, dict) and doc.get("schema_version") == SYNOPTIC_CACHE_SCHEMA_VERSION and isinstance(doc.get("payload"), dict):
            return doc.get("payload")
        # legacy payload-only cache
        if isinstance(doc, dict) and isinstance(doc.get("scale_summary"), dict):
            return doc
    except Exception:
        return None
    return None


def _write_cache(cache_dir: Path, kind: str, payload: dict[str, Any], *parts: str) -> None:
    cache_dir.mkdir(parents=True, exist_ok=True)
    p = _cache_path(cache_dir, kind, *parts)
    doc = {
        "schema_version": SYNOPTIC_CACHE_SCHEMA_VERSION,
        "updated_at": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
        "payload": payload,
    }
    p.write_text(json.dumps(doc, ensure_ascii=False), encoding="utf-8")


def _is_rate_limit_error(exc: Exception) -> bool:
    msg = str(exc)
    if "429" in msg or "Too Many Requests" in msg:
        return True
    stderr = getattr(exc, "stderr", None)
    if stderr and ("429" in str(stderr) or "Too Many Requests" in str(stderr)):
        return True
    return False


def _haversine_km(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    r = 6371.0
    p1 = math.radians(lat1)
    p2 = math.radians(lat2)
    dlat = math.radians(lat2 - lat1)
    dlon = math.radians(lon2 - lon1)
    a = math.sin(dlat / 2.0) ** 2 + math.cos(p1) * math.cos(p2) * math.sin(dlon / 2.0) ** 2
    return 2.0 * r * math.atan2(math.sqrt(a), math.sqrt(1.0 - a))


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
    perf_log: Callable[[str, float], None] | None = None,
) -> dict[str, Any]:
    cache_parts = (
        st.icao,
        target_date,
        model.lower(),
        str(provider or "").lower(),
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

    with tempfile.TemporaryDirectory(prefix="syn2d_") as td:
        passes = [
            {"name": "inner", "lat_span": 8.0, "lon_span": 10.0, "step": 1.0, "batch": 80, "history": True},
            {"name": "outer500", "lat_span": 36.0, "lon_span": 48.0, "step": 3.0, "batch": 120, "history": True},
        ]
        last_err: Exception | None = None
        collected: list[dict[str, Any]] = []

        for i, cfg in enumerate(passes, start=1):
            pass_t0 = time.perf_counter()
            in_json = Path(td) / f"in_{i}.json"
            out_json = Path(td) / f"out_{i}.json"

            start_date = min(prev_local_dt.date(), peak_local_dt.date()).isoformat()
            end_date = max(prev_local_dt.date(), peak_local_dt.date()).isoformat()
            try:
                t_build = time.perf_counter()
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
                        root=scripts_dir.parents[2],
                    )
                    in_json.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")
                else:
                    cmd_build = [
                        "python3", str(scripts_dir / "build_2d_grid_payload.py"),
                        "--station-icao", st.icao,
                        "--station-lat", str(st.lat),
                        "--station-lon", str(st.lon),
                        "--lat-min", str(st.lat - cfg["lat_span"]),
                        "--lat-max", str(st.lat + cfg["lat_span"]),
                        "--lon-min", str(st.lon - cfg["lon_span"]),
                        "--lon-max", str(st.lon + cfg["lon_span"]),
                        "--step-deg", str(cfg["step"]),
                        "--batch-size", str(cfg["batch"]),
                        "--analysis-time", peak_local_dt.strftime("%Y-%m-%dT%H:%M"),
                        "--previous-time", prev_local_dt.strftime("%Y-%m-%dT%H:%M"),
                        "--start-date", start_date,
                        "--end-date", end_date,
                        "--output", str(in_json),
                    ]
                    subprocess.run(cmd_build, check=True, capture_output=True, text=True, timeout=35)
                if perf_log:
                    perf_log(f"synoptic.{cfg['name']}.build", time.perf_counter() - t_build)

                cmd_syn = [
                    "python3", str(scripts_dir / "synoptic_2d_detector.py"),
                    "--input", str(in_json),
                    "--output", str(out_json),
                ]
                t_detect = time.perf_counter()
                subprocess.run(cmd_syn, check=True, capture_output=True, text=True, timeout=25)
                if perf_log:
                    perf_log(f"synoptic.{cfg['name']}.detect", time.perf_counter() - t_detect)
                data = json.loads(out_json.read_text(encoding="utf-8"))

                if cfg.get("name") == "outer500":
                    cur = ((data.get("scale_summary") or {}).get("synoptic") or {}).get("systems", [])
                    cur = [s for s in cur if str(s.get("level") or "") == "500"]
                    data.setdefault("scale_summary", {}).setdefault("synoptic", {})["systems"] = cur

                if perf_log:
                    perf_log(f"synoptic.{cfg['name']}.total", time.perf_counter() - pass_t0)
                collected.append(data)
            except Exception as exc:
                last_err = exc
                if perf_log:
                    perf_log(f"synoptic.{cfg['name']}.failed", time.perf_counter() - pass_t0)
                if _is_rate_limit_error(exc):
                    break
                continue

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
            _write_cache(cache_dir, "synoptic", merged, *cache_parts)
            return merged

        stale = _read_cache(cache_dir, "synoptic", *cache_parts)
        if stale:
            return stale
        raise RuntimeError(f"synoptic section failed: {last_err}")
