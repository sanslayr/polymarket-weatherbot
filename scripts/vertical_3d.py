from __future__ import annotations

import math
from datetime import datetime
from typing import Any

from contracts import OBJECTS_3D_SCHEMA_VERSION


def _haversine_km(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    r = 6371.0
    p1 = math.radians(lat1)
    p2 = math.radians(lat2)
    dlat = math.radians(lat2 - lat1)
    dlon = math.radians(lon2 - lon1)
    a = math.sin(dlat / 2.0) ** 2 + math.cos(p1) * math.cos(p2) * math.sin(dlon / 2.0) ** 2
    return 2.0 * r * math.atan2(math.sqrt(a), math.sqrt(1.0 - a))


def _safe_float(value: Any) -> float | None:
    try:
        if value in (None, ""):
            return None
        return float(value)
    except Exception:
        return None


def _parse_time_utc(value: Any) -> datetime | None:
    text = str(value or "").strip()
    if not text:
        return None
    try:
        return datetime.fromisoformat(text.replace("Z", "+00:00"))
    except Exception:
        return None


def _stype_group(stype: str) -> str:
    s = (stype or "").lower()
    if "front" in s:
        return "frontal"
    if "baroclinic" in s:
        return "baroclinic"
    if "llj" in s or "shear" in s:
        return "shear"
    if "trough" in s or "vortic" in s:
        return "dynamic"
    if "ridge" in s or "subsidence" in s:
        return "subsidence"
    if "advection" in s:
        return "advection"
    if "dry" in s:
        return "dry_intrusion"
    return "generic"


def _normalize_level(level: str) -> str:
    text = str(level or "").strip().lower()
    if text in {"925hpa", "925"}:
        return "925"
    if text in {"850hpa", "850"}:
        return "850"
    if text in {"700hpa", "700"}:
        return "700"
    if text in {"500hpa", "500"}:
        return "500"
    if text in {"surface", "mslp", "sfc"}:
        return "sfc"
    return text or "unknown"


def _extract_intensity(system: dict[str, Any]) -> float | None:
    for key in (
        "intensity_hpa",
        "intensity_gpm",
        "intensity_ms",
        "intensity_k_per_6h",
        "prominence_hpa",
        "amplitude_gpm",
    ):
        value = _safe_float(system.get(key))
        if value is not None:
            return value
    return None


def _impact_scope(min_dist: float) -> str:
    if min_dist > 500:
        return "background_only"
    if min_dist > 250:
        return "possible_override"
    return "station_relevant"


def _confidence_order(value: str) -> int:
    return {"high": 3, "medium": 2, "low": 1}.get(str(value or "").lower(), 0)


def _collect_anchor_slices(synoptic: dict[str, Any]) -> list[dict[str, Any]]:
    raw = synoptic.get("anchor_slices")
    slices: list[dict[str, Any]] = []
    if isinstance(raw, list):
        for idx, item in enumerate(raw):
            if not isinstance(item, dict):
                continue
            systems = ((item.get("scale_summary") or {}).get("synoptic") or {}).get("systems")
            if not isinstance(systems, list):
                systems = item.get("systems")
            systems = [system for system in (systems or []) if isinstance(system, dict)]
            slices.append(
                {
                    "analysis_time_utc": str(item.get("analysis_time_utc") or ""),
                    "analysis_time_local": str(item.get("analysis_time_local") or ""),
                    "systems": systems,
                    "_order": idx,
                }
            )

    if not slices:
        systems = ((synoptic.get("scale_summary") or {}).get("synoptic") or {}).get("systems") or []
        slices = [
            {
                "analysis_time_utc": str(synoptic.get("analysis_time_utc") or ""),
                "analysis_time_local": str(synoptic.get("analysis_time_local") or ""),
                "systems": [system for system in systems if isinstance(system, dict)],
                "_order": 0,
            }
        ]

    def _sort_key(item: dict[str, Any]) -> tuple[int, datetime | None, int]:
        parsed = _parse_time_utc(item.get("analysis_time_utc"))
        return (0 if parsed is not None else 1, parsed, int(item.get("_order") or 0))

    return sorted(slices, key=_sort_key)


def _cluster_systems_within_slice(
    *,
    systems: list[dict[str, Any]],
    station_lat: float,
    station_lon: float,
    primary_window: dict[str, Any],
    diag700: dict[str, Any] | None,
    diag925: dict[str, Any] | None,
    max_match_km: float,
) -> list[dict[str, Any]]:
    items: list[dict[str, Any]] = []
    for system in systems:
        lat = _safe_float(system.get("center_lat"))
        lon = _safe_float(system.get("center_lon"))
        if lat is None or lon is None:
            continue
        distance_km = _safe_float(system.get("distance_to_station_km"))
        if distance_km is None:
            distance_km = _haversine_km(station_lat, station_lon, lat, lon)
        items.append(
            {
                "level": str(system.get("level") or ""),
                "stype": str(system.get("system_type") or ""),
                "group": _stype_group(str(system.get("system_type") or "")),
                "lat": float(lat),
                "lon": float(lon),
                "distance_km": float(distance_km),
                "intensity": _extract_intensity(system),
            }
        )

    objects: list[dict[str, Any]] = []
    used: set[int] = set()
    oid = 0
    low_cloud = _safe_float(primary_window.get("low_cloud_pct")) or 0.0
    d700 = str((diag700 or {}).get("summary") or "")
    d925 = str((diag925 or {}).get("summary") or "")

    for i, base in enumerate(items):
        if i in used:
            continue
        queue = [i]
        cluster: list[int] = []
        used.add(i)
        while queue:
            idx = queue.pop()
            cluster.append(idx)
            current = items[idx]
            for j, candidate in enumerate(items):
                if j in used or candidate["group"] != base["group"]:
                    continue
                dist = _haversine_km(current["lat"], current["lon"], candidate["lat"], candidate["lon"])
                if dist <= max_match_km:
                    used.add(j)
                    queue.append(j)

        cluster_items = [items[idx] for idx in cluster]
        levels = sorted({_normalize_level(item["level"]) for item in cluster_items})
        n_levels = len(levels)
        coherence = min(1.0, n_levels / 5.0)
        min_dist = min((item["distance_km"] for item in cluster_items), default=9999.0)
        near_surface = any(level in {"sfc", "925", "1000"} for level in levels)
        surface_coupling = 0.75 if near_surface else 0.35
        blockers: list[str] = []
        if low_cloud >= 75:
            blockers.append("low_cloud_cover")
            surface_coupling = max(0.1, surface_coupling - 0.25)

        score = 0.0
        support: list[str] = []
        conflict: list[str] = []

        if n_levels >= 2:
            score += 1.0
            support.append(f"多层一致性({n_levels}层)")
        else:
            conflict.append("层间一致性弱(单层主导)")

        if min_dist <= 250:
            score += 1.0
            support.append("系统贴近站点")
        elif min_dist <= 500:
            score += 0.4
            support.append("系统处于可影响圈")
        else:
            score -= 0.5
            conflict.append("系统离站偏远")

        if surface_coupling >= 0.65:
            score += 0.8
            support.append("近地耦合较强")
        elif surface_coupling <= 0.2:
            score -= 0.6
            conflict.append("近地耦合较弱")

        if "湿层约束偏强" in d700:
            score -= 0.4
            conflict.append("700层湿层封盖")
        elif "干层特征偏明显" in d700:
            score += 0.2
            support.append("700层干层利于日照")

        if "耦合偏强" in d925:
            score += 0.4
            support.append("925层传输落地效率较好")
        elif "耦合偏弱" in d925:
            score -= 0.3
            conflict.append("925层传输落地效率有限")

        if score >= 1.6:
            confidence = "high"
        elif score >= 0.5:
            confidence = "medium"
        else:
            confidence = "low"

        group = base["group"]
        type_weight = {
            "baroclinic": 2.6,
            "frontal": 2.5,
            "dynamic": 2.2,
            "advection": 2.0,
            "shear": 1.9,
            "dry_intrusion": 1.8,
            "subsidence": 1.6,
            "generic": 1.0,
        }.get(group, 1.0)
        conf_weight = {"high": 1.2, "medium": 0.7, "low": 0.2}.get(confidence, 0.2)
        impact_weight = {"station_relevant": 1.2, "possible_override": 0.6, "background_only": 0.0}.get(_impact_scope(min_dist), 0.0)
        dist_penalty = min(1.2, max(0.0, (min_dist - 200.0) / 800.0))
        rank_score = round(type_weight + conf_weight + impact_weight + 0.5 * coherence + 0.4 * surface_coupling - dist_penalty, 3)

        intensity_values = [item["intensity"] for item in cluster_items if item.get("intensity") is not None]
        oid += 1
        objects.append(
            {
                "object_id": f"slice_obj_{oid}",
                "group": group,
                "type": f"{group}_3d",
                "levels": levels,
                "vertical_coherence_score": round(coherence, 3),
                "surface_coupling_score": round(surface_coupling, 3),
                "distance_km_min": round(min_dist, 1),
                "impact_scope": _impact_scope(min_dist),
                "surface_blockers": blockers,
                "confidence": confidence,
                "rank_score": rank_score,
                "centroid_lat": round(sum(item["lat"] for item in cluster_items) / len(cluster_items), 3),
                "centroid_lon": round(sum(item["lon"] for item in cluster_items) / len(cluster_items), 3),
                "intensity_value": round(sum(intensity_values) / len(intensity_values), 3) if intensity_values else None,
                "evidence": {
                    "support": support[:3],
                    "conflict": conflict[:3],
                },
            }
        )

    objects.sort(key=lambda item: (-float(item.get("rank_score", 0.0)), item["distance_km_min"], -item["vertical_coherence_score"]))
    return objects


def _classify_track_evolution(distances: list[float]) -> str:
    if len(distances) < 2:
        return "steady"
    first = float(distances[0])
    last = float(distances[-1])
    min_dist = min(distances)
    min_idx = distances.index(min_dist)
    if 0 < min_idx < len(distances) - 1 and (first - min_dist) >= 120.0 and (last - min_dist) >= 80.0:
        return "passing"
    if (first - last) >= 120.0:
        return "approaching"
    if (last - first) >= 120.0:
        return "receding"
    return "steady"


def _classify_intensity_trend(values: list[float | None]) -> str:
    valid = [float(value) for value in values if value is not None]
    if len(valid) < 2:
        return "unknown"
    delta = valid[-1] - valid[0]
    if delta >= 0.4:
        return "strengthening"
    if delta <= -0.4:
        return "weakening"
    return "steady"


def _track_objects(
    *,
    anchor_slices: list[dict[str, Any]],
    station_lat: float,
    station_lon: float,
    primary_window: dict[str, Any],
    diag700: dict[str, Any] | None,
    diag925: dict[str, Any] | None,
    max_match_km: float,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    tracked_slices: list[dict[str, Any]] = []
    tracks: list[dict[str, Any]] = []
    max_track_step_km = max(520.0, max_match_km * 3.0)

    for slice_doc in anchor_slices:
        objects = _cluster_systems_within_slice(
            systems=list(slice_doc.get("systems") or []),
            station_lat=station_lat,
            station_lon=station_lon,
            primary_window=primary_window,
            diag700=diag700,
            diag925=diag925,
            max_match_km=max_match_km,
        )
        parsed_time = _parse_time_utc(slice_doc.get("analysis_time_utc"))
        tracked_slices.append(
            {
                "analysis_time_utc": str(slice_doc.get("analysis_time_utc") or ""),
                "analysis_time_local": str(slice_doc.get("analysis_time_local") or ""),
                "objects": objects,
                "_parsed_time": parsed_time,
            }
        )

        used_track_ids: set[str] = set()
        for obj in objects:
            best_track: dict[str, Any] | None = None
            best_score: float | None = None
            for track in tracks:
                if track["group"] != obj["group"] or track["track_id"] in used_track_ids:
                    continue
                last_time = track.get("_last_time")
                if parsed_time is not None and last_time is not None:
                    gap_hours = abs((parsed_time - last_time).total_seconds()) / 3600.0
                    if gap_hours > 12.5:
                        continue
                move_km = _haversine_km(
                    float(track["_last_lat"]),
                    float(track["_last_lon"]),
                    float(obj["centroid_lat"]),
                    float(obj["centroid_lon"]),
                )
                if move_km > max_track_step_km:
                    continue
                overlap = len(set(track["levels_union"]) & set(obj["levels"]))
                score = move_km + 0.25 * abs(float(track["_last_distance_km"]) - float(obj["distance_km_min"])) - 40.0 * overlap
                if best_score is None or score < best_score:
                    best_score = score
                    best_track = track

            point = {
                "analysis_time_utc": str(slice_doc.get("analysis_time_utc") or ""),
                "analysis_time_local": str(slice_doc.get("analysis_time_local") or ""),
                "centroid_lat": float(obj["centroid_lat"]),
                "centroid_lon": float(obj["centroid_lon"]),
                "distance_km": float(obj["distance_km_min"]),
                "levels": list(obj["levels"]),
                "rank_score": float(obj["rank_score"]),
                "confidence": str(obj["confidence"]),
                "impact_scope": str(obj["impact_scope"]),
                "surface_coupling_score": float(obj["surface_coupling_score"]),
                "vertical_coherence_score": float(obj["vertical_coherence_score"]),
                "surface_blockers": list(obj.get("surface_blockers") or []),
                "intensity_value": obj.get("intensity_value"),
                "evidence": dict(obj.get("evidence") or {}),
            }

            if best_track is None:
                track_id = f"track_{len(tracks) + 1}"
                tracks.append(
                    {
                        "track_id": track_id,
                        "group": obj["group"],
                        "type": obj["type"],
                        "points": [point],
                        "levels_union": set(obj["levels"]),
                        "blockers_union": set(obj.get("surface_blockers") or []),
                        "support_union": set((obj.get("evidence") or {}).get("support") or []),
                        "conflict_union": set((obj.get("evidence") or {}).get("conflict") or []),
                        "_last_time": parsed_time,
                        "_last_lat": float(obj["centroid_lat"]),
                        "_last_lon": float(obj["centroid_lon"]),
                        "_last_distance_km": float(obj["distance_km_min"]),
                        "_best_rank_score": float(obj["rank_score"]),
                    }
                )
                used_track_ids.add(track_id)
                continue

            best_track["points"].append(point)
            best_track["levels_union"].update(obj["levels"])
            best_track["blockers_union"].update(obj.get("surface_blockers") or [])
            best_track["support_union"].update((obj.get("evidence") or {}).get("support") or [])
            best_track["conflict_union"].update((obj.get("evidence") or {}).get("conflict") or [])
            best_track["_last_time"] = parsed_time
            best_track["_last_lat"] = float(obj["centroid_lat"])
            best_track["_last_lon"] = float(obj["centroid_lon"])
            best_track["_last_distance_km"] = float(obj["distance_km_min"])
            best_track["_best_rank_score"] = max(float(best_track["_best_rank_score"]), float(obj["rank_score"]))
            used_track_ids.add(str(best_track["track_id"]))

    finalized: list[dict[str, Any]] = []
    for track in tracks:
        points = list(track.get("points") or [])
        if not points:
            continue
        distances = [float(point["distance_km"]) for point in points]
        coherence_values = [float(point["vertical_coherence_score"]) for point in points]
        surface_values = [float(point["surface_coupling_score"]) for point in points]
        confidence_values = [str(point["confidence"]) for point in points]
        parsed_times = [_parse_time_utc(point.get("analysis_time_utc")) for point in points]
        first_time = next((pt for pt in parsed_times if pt is not None), None)
        last_time = next((pt for pt in reversed(parsed_times) if pt is not None), None)
        if first_time is not None and last_time is not None:
            time_span_hours = max(0.0, (last_time - first_time).total_seconds() / 3600.0)
        else:
            time_span_hours = 0.0

        min_index = distances.index(min(distances))
        closest_point = points[min_index]
        best_conf = max((_confidence_order(value) for value in confidence_values), default=0)
        if len(points) >= 3 and min(distances) <= 500.0:
            confidence = "high"
        elif len(points) >= 2 or best_conf >= 2:
            confidence = "medium"
        else:
            confidence = "low"

        base_rank = max(float(point["rank_score"]) for point in points)
        continuity_bonus = 0.18 * min(len(points), 5)
        proximity_bonus = 0.35 if min(distances) <= 250.0 else 0.15 if min(distances) <= 500.0 else 0.0
        track_rank = round(base_rank + continuity_bonus + proximity_bonus, 3)
        levels = sorted(track["levels_union"])
        impact_scope = _impact_scope(min(distances))

        finalized.append(
            {
                "object_id": str(track["track_id"]),
                "track_id": str(track["track_id"]),
                "type": str(track["type"]),
                "levels": levels,
                "vertical_coherence_score": round(max(coherence_values), 3),
                "surface_coupling_score": round(max(surface_values), 3),
                "distance_km_min": round(min(distances), 1),
                "impact_scope": impact_scope,
                "station_relevance": impact_scope,
                "surface_blockers": sorted(track["blockers_union"]),
                "evolution": _classify_track_evolution(distances),
                "intensity_trend": _classify_intensity_trend([point.get("intensity_value") for point in points]),
                "confidence": confidence,
                "rank_score": track_rank,
                "anchors_count": len(points),
                "time_span_hours": round(time_span_hours, 1),
                "closest_approach_time_utc": str(closest_point.get("analysis_time_utc") or ""),
                "closest_approach_time_local": str(closest_point.get("analysis_time_local") or ""),
                "closest_approach_distance_km": round(float(closest_point["distance_km"]), 1),
                "track_points": points,
                "evidence": {
                    "support": sorted(track["support_union"])[:4],
                    "conflict": sorted(track["conflict_union"])[:4],
                },
            }
        )

    finalized.sort(
        key=lambda item: (
            -float(item.get("rank_score", 0.0)),
            float(item.get("distance_km_min", 9999.0)),
            -int(item.get("anchors_count", 0)),
        )
    )
    return tracked_slices, finalized


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
    anchor_slices = _collect_anchor_slices(synoptic)
    tracked_slices, tracks = _track_objects(
        anchor_slices=anchor_slices,
        station_lat=station_lat,
        station_lon=station_lon,
        primary_window=primary_window,
        diag700=diag700,
        diag925=diag925,
        max_match_km=max_match_km,
    )

    main = tracks[0] if tracks else None
    candidates = tracks[:3]
    return {
        "schema_version": OBJECTS_3D_SCHEMA_VERSION,
        "main_object": main,
        "candidates": candidates,
        "tracks": tracks[:5],
        "anchor_slices": [
            {
                "analysis_time_utc": str(item.get("analysis_time_utc") or ""),
                "analysis_time_local": str(item.get("analysis_time_local") or ""),
                "objects": list(item.get("objects") or []),
            }
            for item in tracked_slices
        ],
        "count": len(tracks),
        "anchors_count": len(tracked_slices),
    }
