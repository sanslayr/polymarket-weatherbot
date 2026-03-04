#!/usr/bin/env python3
"""Build 2D gridded payload for synoptic_2d_detector from Open-Meteo.

This builder supports configurable bbox and grid step, and batches requests
for reliability when grid points are many.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import os
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import requests


FULL_HOURLY_FIELDS = (
    "pressure_msl,geopotential_height_500hPa,temperature_850hPa,wind_speed_850hPa,"
    "wind_direction_850hPa,temperature_700hPa,wind_speed_700hPa,wind_direction_700hPa,"
    "temperature_925hPa,wind_speed_925hPa,wind_direction_925hPa"
)
OUTER500_HOURLY_FIELDS = "geopotential_height_500hPa,wind_speed_850hPa,wind_direction_850hPa"


def hourly_fields_by_profile(profile: str) -> str:
    p = str(profile or "full").strip().lower()
    if p == "outer500":
        return OUTER500_HOURLY_FIELDS
    return FULL_HOURLY_FIELDS


def frange(start: float, stop: float, step: float) -> list[float]:
    values = []
    x = start
    eps = step * 0.1
    while x <= stop + eps:
        values.append(round(x, 6))
        x += step
    return values


def uv_from_met_wind(speed_kmh: float, direction_deg: float) -> tuple[float, float]:
    sp = speed_kmh / 3.6
    rad = math.radians(direction_deg)
    u = -sp * math.sin(rad)
    v = -sp * math.cos(rad)
    return u, v


def _is_429_error(exc: Exception) -> bool:
    msg = str(exc)
    if "429" in msg or "Too Many Requests" in msg:
        return True
    resp = getattr(exc, "response", None)
    if resp is not None and getattr(resp, "status_code", None) == 429:
        return True
    return False


def _breaker_file() -> Path:
    root = Path(__file__).resolve().parent.parent / "cache" / "runtime"
    root.mkdir(parents=True, exist_ok=True)
    return root / "openmeteo_breaker.json"


def _breaker_active() -> bool:
    try:
        p = _breaker_file()
        if not p.exists():
            return False
        obj = json.loads(p.read_text(encoding="utf-8"))
        until = datetime.fromisoformat(str(obj.get("until", "")).replace("Z", "+00:00"))
        return datetime.now(timezone.utc) < until
    except Exception:
        return False


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


def _trip_breaker(reason: str = "429", seconds: int | None = None) -> None:
    try:
        sec = int(seconds if seconds is not None else int(os.getenv("OPENMETEO_BREAKER_SECONDS", "900") or "900"))
        until = (datetime.now(timezone.utc) + timedelta(seconds=max(1, sec))).isoformat().replace("+00:00", "Z")
        _breaker_file().write_text(json.dumps({"until": until, "reason": reason}, ensure_ascii=False), encoding="utf-8")
    except Exception:
        pass


def fetch_points(points: list[tuple[float, float]], start_date: str, end_date: str, hourly_fields: str) -> list[dict[str, Any]]:
    url = "https://api.open-meteo.com/v1/forecast"
    params = {
        "latitude": ",".join(f"{la:.6f}" for la, _ in points),
        "longitude": ",".join(f"{lo:.6f}" for _, lo in points),
        "hourly": hourly_fields,
        "timezone": "UTC",
        "start_date": start_date,
        "end_date": end_date,
    }

    if _breaker_active():
        raise RuntimeError("RATE_LIMIT_429")

    last_err: Exception | None = None
    # Batch-level retries (fast mode): no exponential backoff, keep latency bounded.
    max_attempts = 3
    for attempt in range(1, max_attempts + 1):
        timeout = 45
        try:
            r = requests.get(url, params=params, timeout=timeout)
            r.raise_for_status()
            data = r.json()
            return data if isinstance(data, list) else [data]
        except Exception as exc:  # pragma: no cover - network transient
            # Hard short-circuit on 429 to avoid wasting time in retries.
            if _is_429_error(exc):
                _trip_breaker("grid_429", seconds=_retry_after_seconds_from_exc(exc))
                raise RuntimeError("RATE_LIMIT_429")
            last_err = exc
            if attempt >= max_attempts:
                break

    assert last_err is not None
    raise last_err


def chunked(seq: list[Any], n: int) -> list[list[Any]]:
    return [seq[i : i + n] for i in range(0, len(seq), n)]


def nearest_value(values: list[float], target: float) -> float:
    return min(values, key=lambda x: abs(x - target))


def _cache_path(cache_key: str) -> Path:
    root = Path(__file__).resolve().parent.parent / "cache" / "runtime"
    root.mkdir(parents=True, exist_ok=True)
    return root / f"grid_rows_{cache_key}.json"


def _rows_cache_key(
    *,
    lat_min: float,
    lat_max: float,
    lon_min: float,
    lon_max: float,
    step_deg: float,
    batch_size: int,
    start_date: str,
    end_date: str,
    hourly_fields: str,
    field_profile: str,
) -> str:
    raw = "|".join(
        [
            f"{lat_min:.4f}", f"{lat_max:.4f}", f"{lon_min:.4f}", f"{lon_max:.4f}",
            f"{step_deg:.4f}", str(batch_size), start_date, end_date,
            str(field_profile or "full"),
            hourly_fields,
        ]
    )
    return hashlib.sha1(raw.encode("utf-8")).hexdigest()[:16]


def build_2d_grid_payload_openmeteo(
    *,
    station_icao: str,
    station_lat: float,
    station_lon: float,
    lat_min: float,
    lat_max: float,
    lon_min: float,
    lon_max: float,
    step_deg: float,
    analysis_time: str,
    previous_time: str,
    date: str | None = None,
    start_date: str | None = None,
    end_date: str | None = None,
    batch_size: int = 80,
    field_profile: str = "full",
) -> dict[str, Any]:
    profile = str(field_profile or "full").strip().lower()
    hourly_fields = hourly_fields_by_profile(profile)

    lats = frange(lat_min, lat_max, step_deg)
    lons = frange(lon_min, lon_max, step_deg)
    points = [(la, lo) for la in lats for lo in lons]

    d0 = start_date or date or str(previous_time).split("T")[0]
    d1 = end_date or date or str(analysis_time).split("T")[0]
    if d0 > d1:
        d0, d1 = d1, d0

    ckey = _rows_cache_key(
        lat_min=lat_min,
        lat_max=lat_max,
        lon_min=lon_min,
        lon_max=lon_max,
        step_deg=step_deg,
        batch_size=batch_size,
        start_date=d0,
        end_date=d1,
        hourly_fields=hourly_fields,
        field_profile=profile,
    )
    cpath = _cache_path(ckey)

    rows: list[dict[str, Any]] = []
    if cpath.exists():
        try:
            rows = json.loads(cpath.read_text(encoding="utf-8"))
        except Exception:
            rows = []

    if not rows:
        batches = chunked(points, batch_size)
        failed_batches: list[list[tuple[float, float]]] = []

        for batch in batches:
            try:
                rows.extend(fetch_points(batch, d0, d1, hourly_fields))
            except Exception as exc:
                if "RATE_LIMIT_429" in str(exc):
                    raise
                failed_batches.append(batch)

        retry_failed: list[list[tuple[float, float]]] = []
        if failed_batches:
            for batch in failed_batches:
                for sub in chunked(batch, max(10, batch_size // 2)):
                    try:
                        rows.extend(fetch_points(sub, d0, d1, hourly_fields))
                    except Exception as exc:
                        if "RATE_LIMIT_429" in str(exc):
                            raise
                        retry_failed.append(sub)

        if retry_failed:
            for batch in retry_failed:
                for tiny in chunked(batch, 8):
                    try:
                        rows.extend(fetch_points(tiny, d0, d1, hourly_fields))
                    except Exception as exc:
                        if "RATE_LIMIT_429" in str(exc):
                            raise
                        continue
        try:
            cpath.write_text(json.dumps(rows, ensure_ascii=False), encoding="utf-8")
        except Exception:
            pass

    lookup: dict[tuple[float, float], dict[str, Any]] = {}
    available_points: dict[tuple[float, float], dict[str, Any]] = {}
    available_lats: set[float] = set()
    available_lons: set[float] = set()
    for item in rows:
        la = round(float(item["latitude"]), 6)
        lo = round(float(item["longitude"]), 6)
        key = (la, lo)
        lookup[key] = item
        available_points[key] = item
        available_lats.add(la)
        available_lons.add(lo)

    lat_axis = sorted(available_lats)
    lon_axis = sorted(available_lons)

    def read_time_value(item: dict[str, Any], key: str, t: str) -> float:
        h = item["hourly"]
        idx = h["time"].index(t)
        return float(h[key][idx])

    def read_time_value_opt(item: dict[str, Any], key: str, t: str, default: float = float("nan")) -> float:
        try:
            return read_time_value(item, key, t)
        except Exception:
            return default

    nlat, nlon = len(lats), len(lons)
    z500 = [[0.0] * nlon for _ in range(nlat)]
    u850 = [[0.0] * nlon for _ in range(nlat)]
    if profile == "full":
        mslp = [[0.0] * nlon for _ in range(nlat)]
        t850 = [[0.0] * nlon for _ in range(nlat)]
        v850 = [[0.0] * nlon for _ in range(nlat)]
        t700 = [[float("nan")] * nlon for _ in range(nlat)]
        u700 = [[float("nan")] * nlon for _ in range(nlat)]
        v700 = [[float("nan")] * nlon for _ in range(nlat)]
        t925 = [[float("nan")] * nlon for _ in range(nlat)]
        u925 = [[float("nan")] * nlon for _ in range(nlat)]
        v925 = [[float("nan")] * nlon for _ in range(nlat)]
        prev_mslp = [[0.0] * nlon for _ in range(nlat)]
    prev_z500 = [[0.0] * nlon for _ in range(nlat)]

    for i, la in enumerate(lats):
        for j, lo in enumerate(lons):
            key = (round(float(la), 6), round(float(lo), 6))
            item = lookup.get(key)
            if item is None:
                nla = nearest_value(lat_axis, float(la))
                nlo = nearest_value(lon_axis, float(lo))
                nkey = (nla, nlo)
                item = available_points.get(nkey)
                d2 = (nla - la) ** 2 + (nlo - lo) ** 2
                if item is None:
                    best_key = None
                    best_d2 = 1e9
                    for aa, bb in available_points.keys():
                        cur = (aa - la) ** 2 + (bb - lo) ** 2
                        if cur < best_d2:
                            best_d2 = cur
                            best_key = (aa, bb)
                    if best_key is not None:
                        item = available_points[best_key]
                        nkey = best_key
                        d2 = best_d2
                if item is None:
                    raise RuntimeError(f"Missing grid point in response: {key}, nearest={nkey}, nearest_d2={d2}")

            z500[i][j] = read_time_value(item, "geopotential_height_500hPa", analysis_time)
            sp = read_time_value(item, "wind_speed_850hPa", analysis_time)
            wd = read_time_value(item, "wind_direction_850hPa", analysis_time)
            u, v = uv_from_met_wind(sp, wd)
            u850[i][j] = u
            if profile == "full":
                v850[i][j] = v
                mslp[i][j] = read_time_value(item, "pressure_msl", analysis_time)
                t850[i][j] = read_time_value(item, "temperature_850hPa", analysis_time)

                t700[i][j] = read_time_value_opt(item, "temperature_700hPa", analysis_time)
                sp700 = read_time_value_opt(item, "wind_speed_700hPa", analysis_time)
                wd700 = read_time_value_opt(item, "wind_direction_700hPa", analysis_time)
                if not math.isnan(sp700) and not math.isnan(wd700):
                    u, v = uv_from_met_wind(sp700, wd700)
                    u700[i][j] = u
                    v700[i][j] = v

                t925[i][j] = read_time_value_opt(item, "temperature_925hPa", analysis_time)
                sp925 = read_time_value_opt(item, "wind_speed_925hPa", analysis_time)
                wd925 = read_time_value_opt(item, "wind_direction_925hPa", analysis_time)
                if not math.isnan(sp925) and not math.isnan(wd925):
                    u, v = uv_from_met_wind(sp925, wd925)
                    u925[i][j] = u
                    v925[i][j] = v

                prev_mslp[i][j] = read_time_value(item, "pressure_msl", previous_time)
            prev_z500[i][j] = read_time_value(item, "geopotential_height_500hPa", previous_time)

    return {
        "analysis_time_utc": analysis_time + ":00Z",
        "station": {
            "icao": station_icao,
            "lat": station_lat,
            "lon": station_lon,
        },
        "lat": lats,
        "lon": lons,
        "fields": (
            {
                "mslp_hpa": mslp,
                "z500_gpm": z500,
                "t850_c": t850,
                "u850_ms": u850,
                "v850_ms": v850,
                "t700_c": t700,
                "u700_ms": u700,
                "v700_ms": v700,
                "t925_c": t925,
                "u925_ms": u925,
                "v925_ms": v925,
            }
            if profile == "full"
            else {
                "z500_gpm": z500,
                "u850_ms": u850,
            }
        ),
        "previous_fields": (
            {"mslp_hpa": prev_mslp, "z500_gpm": prev_z500}
            if profile == "full"
            else {"z500_gpm": prev_z500}
        ),
        "grid_meta": {
            "lat_count": nlat,
            "lon_count": nlon,
            "points": nlat * nlon,
            "fetched_unique_points": len(available_points),
            "coverage_pct": round((len(available_points) / max(1, nlat * nlon)) * 100.0, 1),
            "step_deg": step_deg,
            "bbox": {
                "lat_min": lat_min,
                "lat_max": lat_max,
                "lon_min": lon_min,
                "lon_max": lon_max,
            },
            "date_range": {"start_date": d0, "end_date": d1},
            "rows_cache_key": ckey,
            "field_profile": profile,
        },
    }


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="Build gridded payload JSON for synoptic 2D detection")
    p.add_argument("--station-icao", required=True)
    p.add_argument("--station-lat", required=True, type=float)
    p.add_argument("--station-lon", required=True, type=float)
    p.add_argument("--lat-min", required=True, type=float)
    p.add_argument("--lat-max", required=True, type=float)
    p.add_argument("--lon-min", required=True, type=float)
    p.add_argument("--lon-max", required=True, type=float)
    p.add_argument("--step-deg", default=1.0, type=float)
    p.add_argument("--analysis-time", required=True, help="YYYY-MM-DDTHH:MM")
    p.add_argument("--previous-time", required=True, help="YYYY-MM-DDTHH:MM")
    p.add_argument("--date", required=False, help="YYYY-MM-DD (legacy, single-day)")
    p.add_argument("--start-date", required=False, help="YYYY-MM-DD")
    p.add_argument("--end-date", required=False, help="YYYY-MM-DD")
    p.add_argument("--batch-size", default=80, type=int)
    p.add_argument("--field-profile", default="full", choices=["full", "outer500"])
    p.add_argument("--output", required=True)
    return p


def main() -> None:
    args = build_parser().parse_args()
    payload = build_2d_grid_payload_openmeteo(
        station_icao=args.station_icao,
        station_lat=float(args.station_lat),
        station_lon=float(args.station_lon),
        lat_min=float(args.lat_min),
        lat_max=float(args.lat_max),
        lon_min=float(args.lon_min),
        lon_max=float(args.lon_max),
        step_deg=float(args.step_deg),
        analysis_time=str(args.analysis_time),
        previous_time=str(args.previous_time),
        date=args.date,
        start_date=args.start_date,
        end_date=args.end_date,
        batch_size=int(args.batch_size),
        field_profile=str(args.field_profile or "full"),
    )
    Path(args.output).write_text(json.dumps(payload, ensure_ascii=True), encoding="utf-8")
    print(json.dumps(payload["grid_meta"], ensure_ascii=True))


if __name__ == "__main__":
    main()
