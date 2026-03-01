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


def _stype_group(stype: str) -> str:
    s = (stype or "").lower()
    if "front" in s:
        return "frontal"
    if "trough" in s or "vortic" in s:
        return "dynamic"
    if "ridge" in s or "subsidence" in s:
        return "subsidence"
    if "advection" in s:
        return "advection"
    if "dry" in s:
        return "dry_intrusion"
    return "generic"


def build_3d_objects(
    *,
    synoptic: dict[str, Any],
    station_lat: float,
    station_lon: float,
    primary_window: dict[str, Any],
    diag700: dict[str, Any] | None = None,
    diag925: dict[str, Any] | None = None,
    max_match_km: float = 260.0,
) -> dict[str, Any]:
    systems = ((synoptic.get("scale_summary") or {}).get("synoptic") or {}).get("systems") or []
    items: list[dict[str, Any]] = []
    for s in systems:
        try:
            lat = float(s.get("center_lat"))
            lon = float(s.get("center_lon"))
        except Exception:
            continue
        lvl = str(s.get("level") or "")
        stype = str(s.get("system_type") or "")
        items.append({
            "level": lvl,
            "stype": stype,
            "group": _stype_group(stype),
            "lat": lat,
            "lon": lon,
            "distance_km": _haversine_km(station_lat, station_lon, lat, lon),
        })

    objects: list[dict[str, Any]] = []
    used = set()
    oid = 0
    for i, x in enumerate(items):
        if i in used:
            continue
        cluster = [i]
        used.add(i)
        for j, y in enumerate(items):
            if j in used:
                continue
            if y["group"] != x["group"]:
                continue
            d = _haversine_km(x["lat"], x["lon"], y["lat"], y["lon"])
            if d <= max_match_km:
                cluster.append(j)
                used.add(j)

        obj_items = [items[k] for k in cluster]
        levels = sorted(set(it["level"] for it in obj_items))
        n_levels = len(levels)
        coherence = min(1.0, n_levels / 5.0)
        min_dist = min((it["distance_km"] for it in obj_items), default=9999.0)
        near_surface = any(it["level"] in {"sfc", "925", "925hPa", "1000"} for it in obj_items)
        surface_coupling = 0.75 if near_surface else 0.35

        low_cloud = float(primary_window.get("low_cloud_pct") or 0.0)
        blockers: list[str] = []
        if low_cloud >= 75:
            blockers.append("low_cloud_cover")
            surface_coupling = max(0.1, surface_coupling - 0.25)

        if min_dist > 500:
            impact = "background_only"
        elif min_dist > 250:
            impact = "possible_override"
        else:
            impact = "station_relevant"

        score = 0.0
        reasons_pos: list[str] = []
        reasons_neg: list[str] = []

        if n_levels >= 2:
            score += 1.0
            reasons_pos.append(f"多层一致性({n_levels}层)")
        else:
            reasons_neg.append("层间一致性弱(单层主导)")

        if min_dist <= 250:
            score += 1.0
            reasons_pos.append("系统贴近站点")
        elif min_dist <= 500:
            score += 0.4
            reasons_pos.append("系统处于可影响圈")
        else:
            score -= 0.5
            reasons_neg.append("系统离站偏远")

        if surface_coupling >= 0.65:
            score += 0.8
            reasons_pos.append("近地耦合较强")
        elif surface_coupling <= 0.2:
            score -= 0.6
            reasons_neg.append("近地耦合较弱")

        d700 = str((diag700 or {}).get("summary") or "")
        if "湿层约束偏强" in d700:
            score -= 0.4
            reasons_neg.append("700层湿层封盖")
        elif "干层特征偏明显" in d700:
            score += 0.2
            reasons_pos.append("700层干层利于日照")

        d925 = str((diag925 or {}).get("summary") or "")
        if "耦合偏强" in d925:
            score += 0.4
            reasons_pos.append("925层传输落地效率较好")
        elif "耦合偏弱" in d925:
            score -= 0.3
            reasons_neg.append("925层传输落地效率有限")

        if score >= 1.6:
            conf = "high"
        elif score >= 0.5:
            conf = "medium"
        else:
            conf = "low"

        oid += 1
        objects.append({
            "object_id": f"obj3d_{oid}",
            "type": f"{x['group']}_3d",
            "levels": levels,
            "vertical_coherence_score": round(coherence, 3),
            "surface_coupling_score": round(surface_coupling, 3),
            "distance_km_min": round(min_dist, 1),
            "impact_scope": impact,
            "surface_blockers": blockers,
            "evolution": "unknown",
            "confidence": conf,
            "evidence": {
                "support": reasons_pos[:3],
                "conflict": reasons_neg[:3],
            },
        })

    objects = sorted(objects, key=lambda z: (z["distance_km_min"], -z["vertical_coherence_score"]))
    main = objects[0] if objects else None
    candidates = objects[:3]

    return {
        "schema_version": "objects-3d.v1",
        "main_object": main,
        "candidates": candidates,
        "count": len(objects),
    }
