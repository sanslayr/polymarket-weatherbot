#!/usr/bin/env python3
"""2D synoptic pattern detector from gridded fields.

Input JSON keys:
- analysis_time_utc: str
- station: {icao, lat, lon}
- lat: [nlat]
- lon: [nlon]
- fields:
  - mslp_hpa: [nlat][nlon]
  - z500_gpm: [nlat][nlon]
  - t850_c: [nlat][nlon]
  - u850_ms: [nlat][nlon]
  - v850_ms: [nlat][nlon]
  - t925_c/u925_ms/v925_ms (optional)
  - t700_c/u700_ms/v700_ms (optional)

Optional:
- previous_fields: same keys as fields (for trend sign)

Output:
- detected systems and scale-layer diagnostics for circulation analysis.
"""

from __future__ import annotations

import argparse
import json
import math
from pathlib import Path
from typing import Any

import numpy as np

EARTH_RADIUS_KM = 6371.0


def grid_step_degrees(lat: np.ndarray, lon: np.ndarray) -> tuple[float, float]:
    lat_step = float(np.median(np.abs(np.diff(lat)))) if len(lat) > 1 else 1.0
    lon_step = float(np.median(np.abs(np.diff(lon)))) if len(lon) > 1 else 1.0
    return max(lat_step, 1e-6), max(lon_step, 1e-6)


def approx_cell_area_km2(lat_center: float, lat_step: float, lon_step: float) -> float:
    dlat_km = 111.0 * lat_step
    dlon_km = 111.0 * lon_step * max(math.cos(math.radians(lat_center)), 0.2)
    return dlat_km * dlon_km


def haversine_km(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    p1 = math.radians(lat1)
    p2 = math.radians(lat2)
    dlat = math.radians(lat2 - lat1)
    dlon = math.radians(lon2 - lon1)
    a = math.sin(dlat / 2.0) ** 2 + math.cos(p1) * math.cos(p2) * math.sin(dlon / 2.0) ** 2
    return 2.0 * EARTH_RADIUS_KM * math.atan2(math.sqrt(a), math.sqrt(1.0 - a))


def local_extrema(field: np.ndarray, mode: str = "min", radius_i: int = 2, radius_j: int = 2) -> list[tuple[int, int]]:
    nlat, nlon = field.shape
    points: list[tuple[int, int]] = []
    for i in range(radius_i, nlat - radius_i):
        for j in range(radius_j, nlon - radius_j):
            win = field[i - radius_i : i + radius_i + 1, j - radius_j : j + radius_j + 1]
            val = field[i, j]
            if mode == "min":
                if val == np.min(win) and np.sum(win == val) == 1:
                    points.append((i, j))
            else:
                if val == np.max(win) and np.sum(win == val) == 1:
                    points.append((i, j))
    return points


def finite_diff(lat: np.ndarray, lon: np.ndarray, field: np.ndarray) -> tuple[np.ndarray, np.ndarray]:
    lat_rad = np.radians(lat)
    lon_rad = np.radians(lon)

    dlat = np.gradient(lat_rad)
    dlon = np.gradient(lon_rad)

    dphi = np.zeros_like(field)
    dlmb = np.zeros_like(field)

    # d/dphi (north-south)
    dphi[1:-1, :] = (field[2:, :] - field[:-2, :]) / (dlat[2:, None] + dlat[:-2, None])
    dphi[0, :] = (field[1, :] - field[0, :]) / dlat[1]
    dphi[-1, :] = (field[-1, :] - field[-2, :]) / dlat[-1]

    # d/dlambda (east-west)
    dlmb[:, 1:-1] = (field[:, 2:] - field[:, :-2]) / (dlon[None, 2:] + dlon[None, :-2])
    dlmb[:, 0] = (field[:, 1] - field[:, 0]) / dlon[1]
    dlmb[:, -1] = (field[:, -1] - field[:, -2]) / dlon[-1]

    # convert to per-meter gradients
    coslat = np.cos(lat_rad)[:, None]
    dy = EARTH_RADIUS_KM * 1000.0
    dx = EARTH_RADIUS_KM * 1000.0 * np.maximum(coslat, 1e-4)

    dfdlat_m = dphi / dy
    dfdlon_m = dlmb / dx
    return dfdlat_m, dfdlon_m


def connected_components(mask: np.ndarray) -> list[list[tuple[int, int]]]:
    nlat, nlon = mask.shape
    seen = np.zeros_like(mask, dtype=bool)
    comps: list[list[tuple[int, int]]] = []
    neigh = [(-1, 0), (1, 0), (0, -1), (0, 1)]

    for i in range(nlat):
        for j in range(nlon):
            if not mask[i, j] or seen[i, j]:
                continue
            stack = [(i, j)]
            seen[i, j] = True
            comp: list[tuple[int, int]] = []
            while stack:
                ci, cj = stack.pop()
                comp.append((ci, cj))
                for di, dj in neigh:
                    ni, nj = ci + di, cj + dj
                    if 0 <= ni < nlat and 0 <= nj < nlon and mask[ni, nj] and not seen[ni, nj]:
                        seen[ni, nj] = True
                        stack.append((ni, nj))
            comps.append(comp)
    return comps


def centroid(lat: np.ndarray, lon: np.ndarray, pixels: list[tuple[int, int]]) -> tuple[float, float]:
    lats = np.array([lat[i] for i, _ in pixels])
    lons = np.array([lon[j] for _, j in pixels])
    return float(np.mean(lats)), float(np.mean(lons))


def station_sector_label(st_lat: float, st_lon: float, lat0: float, lon0: float, dist_km: float) -> str:
    ns = "north" if lat0 >= st_lat else "south"
    ew = "east" if lon0 >= st_lon else "west"
    band = "0-300km" if dist_km < 300 else "300-800km" if dist_km < 800 else "800km+"
    return f"{ns}-{ew}-{band}"


def build_geo_context(
    station: dict[str, float],
    center_lat: float,
    center_lon: float,
    dist_km: float,
) -> dict[str, Any]:
    ns = "north" if center_lat >= station["lat"] else "south"
    ew = "east" if center_lon >= station["lon"] else "west"
    if dist_km < 300:
        distance_band = "0-300km"
    elif dist_km < 800:
        distance_band = "300-800km"
    else:
        distance_band = "800km+"
    return {
        "sector_ns": ns,
        "sector_ew": ew,
        "distance_km": round(dist_km, 1),
        "distance_band": distance_band,
        "center_lat": round(center_lat, 3),
        "center_lon": round(center_lon, 3),
    }


def detect_pressure_centers(
    lat: np.ndarray,
    lon: np.ndarray,
    mslp: np.ndarray,
    station: dict[str, float],
    kind: str,
    prev_mslp: np.ndarray | None,
) -> list[dict[str, Any]]:
    lat_step, lon_step = grid_step_degrees(lat, lon)
    lat0 = float(np.median(lat))
    radius_i = max(1, int(round(250.0 / (111.0 * lat_step))))
    radius_j = max(1, int(round(250.0 / (111.0 * lon_step * max(math.cos(math.radians(lat0)), 0.3)))))

    mode = "min" if kind == "surface_low" else "max"
    ex = local_extrema(mslp, mode=mode, radius_i=radius_i, radius_j=radius_j)
    systems: list[dict[str, Any]] = []

    for i, j in ex:
        val = float(mslp[i, j])
        i0 = max(0, i - radius_i)
        i1 = min(mslp.shape[0], i + radius_i + 1)
        j0 = max(0, j - radius_j)
        j1 = min(mslp.shape[1], j + radius_j + 1)
        ring = mslp[i0:i1, j0:j1]
        ring_mean = float(np.mean(ring))
        prominence = (ring_mean - val) if kind == "surface_low" else (val - ring_mean)
        if prominence < 0.5:
            continue

        c_lat = float(lat[i])
        c_lon = float(lon[j])
        dist = haversine_km(station["lat"], station["lon"], c_lat, c_lon)
        geo = build_geo_context(station, c_lat, c_lon, dist)
        trend = None
        if prev_mslp is not None:
            prev = float(prev_mslp[i, j])
            trend = "deepening" if (kind == "surface_low" and val < prev) else "filling"
            if kind == "surface_high":
                trend = "strengthening" if val > prev else "weakening"

        systems.append(
            {
                "system_type": kind,
                "scale": "synoptic",
                "level": "mslp",
                "center_lat": round(c_lat, 3),
                "center_lon": round(c_lon, 3),
                "intensity_hpa": round(val, 1),
                "prominence_hpa": round(prominence, 2),
                "distance_to_station_km": geo["distance_km"],
                "region_name": station_sector_label(station["lat"], station["lon"], c_lat, c_lon, dist),
                "geo_context": geo,
                "trend": trend,
            }
        )

    systems.sort(key=lambda x: x["distance_to_station_km"])
    return systems[:6]


def detect_850_bands(
    lat: np.ndarray,
    lon: np.ndarray,
    t850: np.ndarray,
    u850: np.ndarray,
    v850: np.ndarray,
    station: dict[str, float],
) -> dict[str, list[dict[str, Any]]]:
    lat_step, lon_step = grid_step_degrees(lat, lon)
    cell_area = approx_cell_area_km2(float(np.median(lat)), lat_step, lon_step)
    min_pixels = max(6, int(round(120000.0 / max(cell_area, 1.0))))

    dtdy, dtdx = finite_diff(lat, lon, t850)
    # advection in K/s; positive -> warm advection if -V.gradT > 0
    advec = -(u850 * dtdx + v850 * dtdy)
    advec_k6h = advec * 21600.0

    warm_mask = advec_k6h >= 1.5
    cold_mask = advec_k6h <= -1.5

    out: dict[str, list[dict[str, Any]]] = {"warm_advection": [], "cold_advection": []}

    for key, mask in (("warm_advection", warm_mask), ("cold_advection", cold_mask)):
        comps = connected_components(mask)
        for comp in comps:
            if len(comp) < min_pixels:
                continue
            c_lat, c_lon = centroid(lat, lon, comp)
            vals = np.array([advec_k6h[i, j] for i, j in comp])
            peak = float(np.max(vals) if key == "warm_advection" else np.min(vals))
            dist = haversine_km(station["lat"], station["lon"], c_lat, c_lon)
            geo = build_geo_context(station, c_lat, c_lon, dist)
            out[key].append(
                {
                    "system_type": key,
                    "scale": "synoptic",
                    "level": "850",
                    "center_lat": geo["center_lat"],
                    "center_lon": geo["center_lon"],
                    "intensity_k_per_6h": round(peak, 2),
                    "area_pixels": len(comp),
                    "distance_to_station_km": geo["distance_km"],
                    "region_name": station_sector_label(station["lat"], station["lon"], c_lat, c_lon, dist),
                    "geo_context": geo,
                }
            )
        out[key].sort(key=lambda x: x["distance_to_station_km"])
        out[key] = out[key][:4]

    return out


def detect_llj_shear_zones(
    lat: np.ndarray,
    lon: np.ndarray,
    u850: np.ndarray,
    v850: np.ndarray,
    u925: np.ndarray | None,
    v925: np.ndarray | None,
    station: dict[str, float],
) -> list[dict[str, Any]]:
    if u925 is None or v925 is None:
        return []
    lat_step, lon_step = grid_step_degrees(lat, lon)
    cell_area = approx_cell_area_km2(float(np.median(lat)), lat_step, lon_step)
    min_pixels = max(5, int(round(80000.0 / max(cell_area, 1.0))))

    ws925 = np.sqrt(u925 * u925 + v925 * v925)
    ws850 = np.sqrt(u850 * u850 + v850 * v850)
    shear = np.sqrt((u850 - u925) ** 2 + (v850 - v925) ** 2)

    j_cut = np.nanpercentile(ws925, 85)
    s_cut = np.nanpercentile(shear, 75)
    mask = (ws925 >= j_cut) & (shear >= s_cut)

    systems: list[dict[str, Any]] = []
    for comp in connected_components(mask):
        if len(comp) < min_pixels:
            continue
        c_lat, c_lon = centroid(lat, lon, comp)
        dist = haversine_km(station["lat"], station["lon"], c_lat, c_lon)
        geo = build_geo_context(station, c_lat, c_lon, dist)
        llj = float(np.nanmean([ws925[i, j] for i, j in comp]))
        shk = float(np.nanmean([shear[i, j] for i, j in comp]))
        systems.append(
            {
                "system_type": "llj_shear_zone",
                "scale": "synoptic",
                "level": "925-850",
                "center_lat": geo["center_lat"],
                "center_lon": geo["center_lon"],
                "llj_ms": round(llj, 2),
                "shear_ms": round(shk, 2),
                "area_pixels": len(comp),
                "distance_to_station_km": geo["distance_km"],
                "region_name": station_sector_label(station["lat"], station["lon"], c_lat, c_lon, dist),
                "geo_context": geo,
            }
        )
    systems.sort(key=lambda x: x["distance_to_station_km"])
    return systems[:4]


def detect_dry_intrusion_700(
    lat: np.ndarray,
    lon: np.ndarray,
    t700: np.ndarray | None,
    t850: np.ndarray,
    station: dict[str, float],
) -> list[dict[str, Any]]:
    if t700 is None:
        return []
    lat_step, lon_step = grid_step_degrees(lat, lon)
    cell_area = approx_cell_area_km2(float(np.median(lat)), lat_step, lon_step)
    min_pixels = max(5, int(round(80000.0 / max(cell_area, 1.0))))

    lapse = t850 - t700
    l_cut = np.nanpercentile(lapse, 80)
    mask = lapse >= l_cut
    systems: list[dict[str, Any]] = []
    for comp in connected_components(mask):
        if len(comp) < min_pixels:
            continue
        c_lat, c_lon = centroid(lat, lon, comp)
        dist = haversine_km(station["lat"], station["lon"], c_lat, c_lon)
        geo = build_geo_context(station, c_lat, c_lon, dist)
        lv = float(np.nanmean([lapse[i, j] for i, j in comp]))
        systems.append(
            {
                "system_type": "dry_intrusion_700",
                "scale": "synoptic",
                "level": "700",
                "center_lat": geo["center_lat"],
                "center_lon": geo["center_lon"],
                "lapse_t850_t700_c": round(lv, 2),
                "area_pixels": len(comp),
                "distance_to_station_km": geo["distance_km"],
                "region_name": station_sector_label(station["lat"], station["lon"], c_lat, c_lon, dist),
                "geo_context": geo,
            }
        )
    systems.sort(key=lambda x: x["distance_to_station_km"])
    return systems[:4]


def detect_baroclinic_coupling(
    lat: np.ndarray,
    lon: np.ndarray,
    mslp: np.ndarray,
    t850: np.ndarray,
    station: dict[str, float],
) -> list[dict[str, Any]]:
    lat_step, lon_step = grid_step_degrees(lat, lon)
    cell_area = approx_cell_area_km2(float(np.median(lat)), lat_step, lon_step)
    min_pixels = max(5, int(round(100000.0 / max(cell_area, 1.0))))

    dpy, dpx = finite_diff(lat, lon, mslp)
    dtY, dtX = finite_diff(lat, lon, t850)
    g_p = np.sqrt(dpx * dpx + dpy * dpy)
    g_t = np.sqrt(dtX * dtX + dtY * dtY)

    # Normalize to robust quantiles to keep score in interpretable range [0,1]
    gp_q90 = max(1e-12, float(np.nanpercentile(g_p, 90)))
    gt_q90 = max(1e-12, float(np.nanpercentile(g_t, 90)))
    gp_n = np.clip(g_p / gp_q90, 0.0, 2.0)
    gt_n = np.clip(g_t / gt_q90, 0.0, 2.0)
    score_raw = np.sqrt(gp_n * gt_n)
    score = np.clip(score_raw / 1.5, 0.0, 1.0)

    cut = np.nanpercentile(score, 85)
    mask = score >= cut
    systems: list[dict[str, Any]] = []
    for comp in connected_components(mask):
        if len(comp) < min_pixels:
            continue
        c_lat, c_lon = centroid(lat, lon, comp)
        dist = haversine_km(station["lat"], station["lon"], c_lat, c_lon)
        geo = build_geo_context(station, c_lat, c_lon, dist)
        sc = float(np.nanmean([score[i, j] for i, j in comp]))
        gp = float(np.nanmean([gp_n[i, j] for i, j in comp]))
        gt = float(np.nanmean([gt_n[i, j] for i, j in comp]))
        systems.append(
            {
                "system_type": "baroclinic_coupling",
                "scale": "synoptic",
                "level": "mslp-850",
                "center_lat": geo["center_lat"],
                "center_lon": geo["center_lon"],
                "coupling_score": round(sc, 3),
                "pressure_gradient_norm": round(gp, 3),
                "thermal_gradient_norm": round(gt, 3),
                "area_pixels": len(comp),
                "distance_to_station_km": geo["distance_km"],
                "region_name": station_sector_label(station["lat"], station["lon"], c_lat, c_lon, dist),
                "geo_context": geo,
            }
        )
    systems.sort(key=lambda x: x["distance_to_station_km"])
    return systems[:4]


def detect_frontogenesis_zones(
    lat: np.ndarray,
    lon: np.ndarray,
    t850: np.ndarray,
    u850: np.ndarray,
    v850: np.ndarray,
    t925: np.ndarray | None,
    u925: np.ndarray | None,
    v925: np.ndarray | None,
    mslp: np.ndarray,
    station: dict[str, float],
) -> list[dict[str, Any]]:
    lat_step, lon_step = grid_step_degrees(lat, lon)
    cell_area = approx_cell_area_km2(float(np.median(lat)), lat_step, lon_step)
    min_pixels = max(5, int(round(100000.0 / max(cell_area, 1.0))))

    dty850, dtx850 = finite_diff(lat, lon, t850)
    grad850 = np.sqrt(dtx850 * dtx850 + dty850 * dty850) * 1e5  # K/100km

    if t925 is not None and u925 is not None and v925 is not None:
        dty925, dtx925 = finite_diff(lat, lon, t925)
        grad925 = np.sqrt(dtx925 * dtx925 + dty925 * dty925) * 1e5
        du = u850 - u925
        dv = v850 - v925
        shear = np.sqrt(du * du + dv * dv)
        grad = 0.55 * grad850 + 0.45 * grad925
    else:
        shear = np.sqrt(u850 * u850 + v850 * v850) * 0.0
        grad = grad850

    p_anom = np.abs(mslp - np.nanmean(mslp))
    p_cut = np.nanpercentile(p_anom, 65)
    g_cut = np.nanpercentile(grad, 80)
    s_cut = np.nanpercentile(shear, 70) if np.nanmax(shear) > 0 else 0.0

    if s_cut > 0:
        mask = (grad >= g_cut) & (p_anom >= p_cut) & (shear >= s_cut)
    else:
        mask = (grad >= g_cut) & (p_anom >= p_cut)

    systems: list[dict[str, Any]] = []
    comps = connected_components(mask)
    for comp in comps:
        if len(comp) < min_pixels:
            continue
        c_lat, c_lon = centroid(lat, lon, comp)
        dist = haversine_km(station["lat"], station["lon"], c_lat, c_lon)
        geo = build_geo_context(station, c_lat, c_lon, dist)
        gpk = float(np.nanmean([grad[i, j] for i, j in comp]))
        shk = float(np.nanmean([shear[i, j] for i, j in comp])) if s_cut > 0 else None
        score = min(1.0, max(0.0, (gpk / max(g_cut, 1e-6) - 0.8) * 0.6 + (0.4 if shk is None else min(1.0, shk / max(s_cut, 1e-6)) * 0.4)))

        systems.append(
            {
                "system_type": "frontogenesis_zone",
                "scale": "synoptic",
                "level": "925-850",
                "center_lat": geo["center_lat"],
                "center_lon": geo["center_lon"],
                "temp_gradient_k_per_100km": round(gpk, 2),
                "shear_ms": (round(shk, 2) if shk is not None else None),
                "frontogenesis_score": round(score, 3),
                "area_pixels": len(comp),
                "distance_to_station_km": geo["distance_km"],
                "region_name": station_sector_label(station["lat"], station["lon"], c_lat, c_lon, dist),
                "geo_context": geo,
            }
        )

    systems.sort(key=lambda x: x["distance_to_station_km"])
    return systems[:4]


def detect_500_axes(lat: np.ndarray, lon: np.ndarray, z500: np.ndarray, station: dict[str, float]) -> list[dict[str, Any]]:
    lat_step, lon_step = grid_step_degrees(lat, lon)
    cell_area = approx_cell_area_km2(float(np.median(lat)), lat_step, lon_step)
    min_pixels = max(6, int(round(140000.0 / max(cell_area, 1.0))))

    dzy, dzx = finite_diff(lat, lon, z500)
    # trough proxy: strong west-east gradient with cyclonic curvature proxy
    d2y, d2x = finite_diff(lat, lon, dzx)
    curvature = d2x

    trough_mask = (np.abs(dzx) > np.percentile(np.abs(dzx), 85)) & (curvature < np.percentile(curvature, 20))
    ridge_mask = (np.abs(dzx) > np.percentile(np.abs(dzx), 85)) & (curvature > np.percentile(curvature, 80))

    systems: list[dict[str, Any]] = []
    for system_type, mask in (("shortwave_trough", trough_mask), ("ridge", ridge_mask)):
        comps = connected_components(mask)
        for comp in comps:
            if len(comp) < min_pixels:
                continue
            c_lat, c_lon = centroid(lat, lon, comp)
            dist = haversine_km(station["lat"], station["lon"], c_lat, c_lon)
            geo = build_geo_context(station, c_lat, c_lon, dist)
            strength = float(np.mean(np.abs([dzx[i, j] for i, j in comp])))
            systems.append(
                {
                    "system_type": system_type,
                    "scale": "synoptic",
                    "level": "500",
                    "center_lat": geo["center_lat"],
                    "center_lon": geo["center_lon"],
                    "axis_strength": round(strength, 6),
                    "area_pixels": len(comp),
                    "distance_to_station_km": geo["distance_km"],
                    "region_name": station_sector_label(station["lat"], station["lon"], c_lat, c_lon, dist),
                    "geo_context": geo,
                }
            )

    systems.sort(key=lambda x: x["distance_to_station_km"])
    return systems[:6]


def diagnose_planetary(lat: np.ndarray, z500: np.ndarray, u850: np.ndarray) -> list[dict[str, Any]]:
    systems: list[dict[str, Any]] = []
    lat2d = lat[:, None] + np.zeros_like(z500)

    subtropical_band = (lat2d >= 20) & (lat2d <= 35)
    if np.any(subtropical_band):
        p90 = float(np.percentile(z500[subtropical_band], 90))
        if p90 >= 5860:  # gpm proxy in meter space would be 5860 m
            systems.append(
                {
                    "system_type": "subtropical_high",
                    "scale": "planetary",
                    "level": "500",
                    "intensity_gpm": round(p90, 1),
                    "description": "subtropical high ridge signal over 20-35N",
                }
            )

    midlat = (lat2d >= 35) & (lat2d <= 60)
    if np.any(midlat):
        westerly = float(np.percentile(u850[midlat], 75))
        if westerly >= 12.0:
            systems.append(
                {
                    "system_type": "westerly_belt",
                    "scale": "planetary",
                    "level": "850",
                    "intensity_ms": round(westerly, 1),
                    "description": "enhanced midlatitude westerly flow",
                }
            )

    return systems


def build_scale_summary(systems: list[dict[str, Any]]) -> dict[str, Any]:
    by_scale = {"planetary": [], "synoptic": [], "mesoscale": []}
    for s in systems:
        by_scale[s.get("scale", "synoptic")].append(s)

    summary: dict[str, Any] = {}
    for k in by_scale:
        arr = by_scale[k]
        summary[k] = {
            "count": len(arr),
            "primary": arr[0] if arr else None,
            "systems": arr,
        }
    return summary


def analyze(payload: dict[str, Any]) -> dict[str, Any]:
    station = payload["station"]
    lat = np.asarray(payload["lat"], dtype=float)
    lon = np.asarray(payload["lon"], dtype=float)

    fields = payload["fields"]
    mslp = np.asarray(fields["mslp_hpa"], dtype=float)
    z500 = np.asarray(fields["z500_gpm"], dtype=float)
    t850 = np.asarray(fields["t850_c"], dtype=float)
    u850 = np.asarray(fields["u850_ms"], dtype=float)
    v850 = np.asarray(fields["v850_ms"], dtype=float)

    t925 = np.asarray(fields.get("t925_c"), dtype=float) if fields.get("t925_c") is not None else None
    u925 = np.asarray(fields.get("u925_ms"), dtype=float) if fields.get("u925_ms") is not None else None
    v925 = np.asarray(fields.get("v925_ms"), dtype=float) if fields.get("v925_ms") is not None else None
    t700 = np.asarray(fields.get("t700_c"), dtype=float) if fields.get("t700_c") is not None else None

    prev = payload.get("previous_fields")
    prev_mslp = None
    if prev and "mslp_hpa" in prev:
        prev_mslp = np.asarray(prev["mslp_hpa"], dtype=float)

    lows = detect_pressure_centers(lat, lon, mslp, station, "surface_low", prev_mslp)
    highs = detect_pressure_centers(lat, lon, mslp, station, "surface_high", prev_mslp)
    bands = detect_850_bands(lat, lon, t850, u850, v850, station)
    fronts = detect_frontogenesis_zones(lat, lon, t850, u850, v850, t925, u925, v925, mslp, station)
    llj = detect_llj_shear_zones(lat, lon, u850, v850, u925, v925, station)
    dry700 = detect_dry_intrusion_700(lat, lon, t700, t850, station)
    baro = detect_baroclinic_coupling(lat, lon, mslp, t850, station)
    axes = detect_500_axes(lat, lon, z500, station)
    planetary = diagnose_planetary(lat, z500, u850)

    systems = planetary + lows + highs + axes + fronts + llj + dry700 + baro + bands["warm_advection"] + bands["cold_advection"]
    systems_sorted = sorted(
        systems,
        key=lambda x: (
            0 if x.get("scale") == "planetary" else 1,
            float(x.get("distance_to_station_km", 0.0)),
        ),
    )

    return {
        "analysis_time_utc": payload["analysis_time_utc"],
        "station": station,
        "scale_summary": build_scale_summary(systems_sorted),
        "regional_picture": [
            {
                "region_name": s.get("region_name", "planetary-domain"),
                "system_type": s.get("system_type"),
                "level": s.get("level"),
                "scale": s.get("scale"),
                "distance_to_station_km": s.get("distance_to_station_km"),
                "geo_context": s.get("geo_context"),
                "intensity": s.get("intensity_hpa", s.get("intensity_k_per_6h", s.get("intensity_gpm"))),
                "trend": s.get("trend"),
            }
            for s in systems_sorted
        ],
    }


def build_arg_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="2D synoptic detector")
    p.add_argument("--input", required=True)
    p.add_argument("--output", default="")
    return p


def main() -> None:
    args = build_arg_parser().parse_args()
    payload = json.loads(Path(args.input).read_text(encoding="utf-8"))
    result = analyze(payload)
    text = json.dumps(result, ensure_ascii=True, indent=2)
    if args.output:
        Path(args.output).write_text(text + "\n", encoding="utf-8")
    else:
        print(text)


if __name__ == "__main__":
    main()
