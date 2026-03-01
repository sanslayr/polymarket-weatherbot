from __future__ import annotations

import math
from typing import Any


def _haversine_km(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    r = 6371.0
    p1 = math.radians(lat1)
    p2 = math.radians(lat2)
    dlat = math.radians(lat2 - lat1)
    dlon = math.radians(lon2 - lon1)
    a = math.sin(dlat / 2.0) ** 2 + math.cos(p1) * math.cos(p2) * math.sin(dlon / 2.0) ** 2
    return 2.0 * r * math.atan2(math.sqrt(a), math.sqrt(1.0 - a))


def diagnose_700(
    primary_window: dict[str, Any],
    *,
    synoptic: dict[str, Any] | None = None,
    station_lat: float | None = None,
    station_lon: float | None = None,
) -> dict[str, Any] | None:
    """700hPa diagnosis.

    Priority:
    1) synoptic-level 700 dry intrusion signals (distance-aware, station-agnostic)
    2) fallback low-cloud proxy from primary window
    """
    dry_nearest_km: float | None = None
    dry_strength: float | None = None

    if isinstance(synoptic, dict):
        systems = ((synoptic.get("scale_summary") or {}).get("synoptic") or {}).get("systems") or []
        for s in systems:
            stype = str(s.get("system_type") or "").lower()
            lvl = str(s.get("level") or "")
            if "dry_intrusion" not in stype and "700" not in lvl:
                continue

            d = None
            try:
                d = float(s.get("distance_to_station_km"))
            except Exception:
                d = None

            if d is None and station_lat is not None and station_lon is not None:
                try:
                    d = _haversine_km(float(station_lat), float(station_lon), float(s.get("center_lat")), float(s.get("center_lon")))
                except Exception:
                    d = None

            if d is None:
                continue

            if dry_nearest_km is None or d < dry_nearest_km:
                dry_nearest_km = d
                try:
                    dry_strength = float(s.get("lapse_t850_t700_c"))
                except Exception:
                    dry_strength = None

    if dry_nearest_km is not None:
        d = float(dry_nearest_km)
        d_txt = f"{d:.0f}km"
        if d <= 350:
            scope = "near"
            summary = f"700hPa 干层信号近站（约{d_txt}）"
            impact = "中层干空气下传条件较好，云开时更易维持升温效率"
        elif d <= 900:
            scope = "peripheral"
            summary = f"700hPa 干层信号在外围（约{d_txt}）"
            impact = "作为偏暖背景加分项，需配合低层不回补云才易落地"
        else:
            scope = "remote"
            summary = f"700hPa 干层信号偏远（约{d_txt}）"
            impact = "对本站直接作用有限，更多体现为背景约束"

        if dry_strength is not None and dry_strength >= 11.0:
            impact += "（干侵强度偏强）"

        return {
            "summary": summary,
            "impact": impact,
            "dry_intrusion_nearest_km": round(d, 1),
            "dry_intrusion_scope": scope,
            "dry_intrusion_strength": round(float(dry_strength), 2) if dry_strength is not None else None,
            "source": "synoptic-700",
        }

    cloud = primary_window.get("low_cloud_pct")
    if cloud is None:
        return None
    if cloud >= 70:
        return {
            "summary": "700hPa 湿层约束偏强，云量维持能力高",
            "impact": "日照受限，Tmax上沿下修风险增加",
            "source": "cloud-proxy",
        }
    if cloud <= 20:
        return {
            "summary": "700hPa 干层特征偏明显",
            "impact": "云量维持弱，白天增温更易维持",
            "source": "cloud-proxy",
        }
    return None
