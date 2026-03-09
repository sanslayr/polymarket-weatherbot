from __future__ import annotations

from pathlib import Path
from typing import Any

from ecmwf_open_data_provider import build_2d_grid_payload_ecmwf
from gfs_grib_provider import build_2d_grid_payload_gfs


DEFAULT_SYNOPTIC_PROVIDER = "ecmwf-open-data"

_ALIASES = {
    "auto": DEFAULT_SYNOPTIC_PROVIDER,
    "ecmwf": "ecmwf-open-data",
    "ecmwf-open": "ecmwf-open-data",
    "ecmwf-open-data": "ecmwf-open-data",
    "ifs": "ecmwf-open-data",
    "gfs": "gfs-grib2",
    "grib2": "gfs-grib2",
    "gfs-grib2": "gfs-grib2",
}


def normalize_synoptic_provider(provider: str | None) -> str:
    key = str(provider or DEFAULT_SYNOPTIC_PROVIDER).strip().lower()
    return _ALIASES.get(key, DEFAULT_SYNOPTIC_PROVIDER)


def provider_candidates(provider: str | None) -> list[str]:
    normalized = normalize_synoptic_provider(provider)
    if normalized == "ecmwf-open-data":
        return ["ecmwf-open-data", "gfs-grib2"]
    return [normalized]


def build_synoptic_grid_payload(
    provider: str,
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
    field_profile: str,
    root: Path,
) -> dict[str, Any]:
    normalized = normalize_synoptic_provider(provider)
    if normalized == "ecmwf-open-data":
        return build_2d_grid_payload_ecmwf(
            station_icao=station_icao,
            station_lat=station_lat,
            station_lon=station_lon,
            lat_min=lat_min,
            lat_max=lat_max,
            lon_min=lon_min,
            lon_max=lon_max,
            analysis_time_local=analysis_time_local,
            previous_time_local=previous_time_local,
            tz_name=tz_name,
            cycle_tag=cycle_tag,
            field_profile=field_profile,
            root=root,
        )
    if normalized == "gfs-grib2":
        return build_2d_grid_payload_gfs(
            station_icao=station_icao,
            station_lat=station_lat,
            station_lon=station_lon,
            lat_min=lat_min,
            lat_max=lat_max,
            lon_min=lon_min,
            lon_max=lon_max,
            analysis_time_local=analysis_time_local,
            previous_time_local=previous_time_local,
            tz_name=tz_name,
            cycle_tag=cycle_tag,
            field_profile=field_profile,
            root=root,
        )
    raise RuntimeError(f"unsupported synoptic provider: {provider}")
