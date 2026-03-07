from __future__ import annotations

from typing import Any


def _safe_float(value: Any) -> float | None:
    try:
        if value in (None, ""):
            return None
        return float(value)
    except Exception:
        return None


def _confidence_scale(confidence: str) -> float:
    return {
        "中-高": 1.0,
        "中": 0.82,
        "低": 0.55,
    }.get(str(confidence or ""), 0.6)


def _proximity_scale(proximity: str) -> float:
    return {
        "近区": 1.0,
        "中近区": 0.82,
        "远区": 0.58,
    }.get(str(proximity or ""), 0.6)


def _subtropical_height_text(intensity_gpm: float | None) -> str:
    if intensity_gpm is None:
        return ""
    if intensity_gpm >= 5900.0:
        return "位势高度很高"
    if intensity_gpm >= 5885.0:
        return "位势高度偏高"
    if intensity_gpm >= 5860.0:
        return "位势高度略偏高"
    return ""


def _subtropical_relation_rank(relation: str) -> int:
    return {
        "core": 3,
        "inner": 2,
        "edge": 1,
        "outside": 0,
    }.get(str(relation or "").strip().lower(), 0)


def _phase_hint_from_sector(system: dict[str, Any] | None) -> str | None:
    if not isinstance(system, dict):
        return None
    geo = system.get("geo_context", {}) if isinstance(system, dict) else {}
    ns = str(geo.get("sector_ns", "")).lower()
    ew = str(geo.get("sector_ew", "")).lower()
    stype = str(system.get("system_type", "")).lower()
    if "trough" in stype:
        if "west" in ew:
            return "槽前"
        if "east" in ew:
            return "槽后"
    if "ridge" in stype:
        if "west" in ew:
            return "脊前"
        if "east" in ew:
            return "脊后"
    if "south" in ns or "north" in ns:
        return "南北向过渡"
    return None


def _axis_strength_abs(system: dict[str, Any] | None) -> float:
    if not isinstance(system, dict):
        return 0.0
    return abs(_safe_float(system.get("axis_strength")) or 0.0)


def diagnose_500hpa(synoptic: dict[str, Any]) -> dict[str, Any]:
    scale_summary = synoptic.get("scale_summary", {}) if isinstance(synoptic, dict) else {}
    syn = scale_summary.get("synoptic", {}) if isinstance(scale_summary, dict) else {}
    planetary = scale_summary.get("planetary", {}) if isinstance(scale_summary, dict) else {}
    systems = syn.get("systems", []) if isinstance(syn, dict) else []
    planetary_systems = planetary.get("systems", []) if isinstance(planetary, dict) else []

    s500 = [s for s in systems if str(s.get("level", "")) == "500"]
    ridge = [s for s in s500 if "ridge" in str(s.get("system_type", "")).lower()]
    trough = [s for s in s500 if "trough" in str(s.get("system_type", "")).lower()]
    shortwave = [s for s in s500 if "shortwave" in str(s.get("system_type", "")).lower()]
    fallback_weak = [s for s in s500 if str(s.get("detection_mode", "")) == "fallback_weak"]
    sfc_high = [s for s in systems if str(s.get("system_type", "")).lower() == "surface_high"]
    sfc_low = [s for s in systems if str(s.get("system_type", "")).lower() == "surface_low"]
    warm_adv = [s for s in systems if "warm_advection" in str(s.get("system_type", "")).lower()]
    cold_adv = [s for s in systems if "cold_advection" in str(s.get("system_type", "")).lower()]
    subtropical_high = [
        s
        for s in planetary_systems
        if str(s.get("system_type", "")).lower() == "subtropical_high"
    ]
    westerly_belt = [
        s
        for s in planetary_systems
        if str(s.get("system_type", "")).lower() == "westerly_belt"
    ]

    def _dist(system: dict[str, Any]) -> float:
        geo = system.get("geo_context", {}) if isinstance(system, dict) else {}
        try:
            return float(geo.get("distance_km"))
        except Exception:
            return 9999.0

    def _nearest(systems_in: list[dict[str, Any]]) -> dict[str, Any] | None:
        if not systems_in:
            return None
        return sorted(
            systems_in,
            key=lambda row: (
                str(row.get("detection_mode", "")) == "fallback_weak",
                _dist(row),
                -(float(row.get("axis_strength") or 0.0) if str(row.get("axis_strength") or "").strip() else 0.0),
            ),
        )[0]

    def _strongest_subtropical(systems_in: list[dict[str, Any]]) -> dict[str, Any] | None:
        if not systems_in:
            return None
        return sorted(
            systems_in,
            key=lambda row: (
                -_subtropical_relation_rank(str(row.get("station_relation") or "")),
                -(_safe_float(row.get("station_support_score")) or 0.0),
                -(_safe_float(row.get("station_pct_in_band")) or 0.0),
                -(_safe_float(row.get("station_z500_gpm")) or 0.0),
                -(_safe_float(row.get("intensity_gpm")) or 0.0),
            ),
        )[0]

    nearest_ridge = _nearest(ridge)
    nearest_trough = _nearest(trough)
    nearest_shortwave = _nearest(shortwave)
    nearest_surface_high = _nearest(sfc_high)
    nearest_surface_low = _nearest(sfc_low)
    nearest_warm_adv = _nearest(warm_adv)
    nearest_cold_adv = _nearest(cold_adv)
    strongest_subtropical = _strongest_subtropical(subtropical_high)
    subtropical_intensity = _safe_float((strongest_subtropical or {}).get("intensity_gpm"))
    subtropical_station_z500 = _safe_float((strongest_subtropical or {}).get("station_z500_gpm"))
    subtropical_station_pct = _safe_float((strongest_subtropical or {}).get("station_pct_in_band"))
    subtropical_support = _safe_float((strongest_subtropical or {}).get("station_support_score")) or 0.0
    subtropical_relation = str((strongest_subtropical or {}).get("station_relation") or "").strip().lower()
    subtropical_edge586_margin = _safe_float((strongest_subtropical or {}).get("edge_586_margin_deg"))
    subtropical_edge588_margin = _safe_float((strongest_subtropical or {}).get("edge_588_margin_deg"))
    strongest_westerly = None
    if westerly_belt:
        strongest_westerly = sorted(
            westerly_belt,
            key=lambda row: -(_safe_float(row.get("intensity_ms")) or 0.0),
        )[0]
    westerly_intensity = _safe_float((strongest_westerly or {}).get("intensity_ms"))

    def _station_relevance(system: dict[str, Any] | None) -> float:
        if not isinstance(system, dict):
            return 0.0
        distance_km = _dist(system)
        if distance_km <= 350.0:
            score = 1.0
        elif distance_km <= 700.0:
            score = 0.86
        elif distance_km <= 1100.0:
            score = 0.68
        elif distance_km <= 1500.0:
            score = 0.44
        else:
            score = 0.18
        score += min(0.16, _axis_strength_abs(system) / 14.0)
        if str(system.get("detection_mode", "")).strip().lower() == "fallback_weak":
            score -= 0.18
            if distance_km > 700.0:
                score -= 0.16
            if _axis_strength_abs(system) < 2.8:
                score -= 0.08
        if _phase_hint_from_sector(system):
            score += 0.04
        return max(0.0, min(1.15, score))

    ridge_relevance = _station_relevance(nearest_ridge)
    trough_relevance = _station_relevance(nearest_trough)
    shortwave_relevance = _station_relevance(nearest_shortwave)

    dominant_500_kind = "weak"
    dominant_focus = None
    if nearest_trough and (
        trough_relevance >= ridge_relevance + 0.18
        or (_dist(nearest_trough) <= 650.0 and (not nearest_ridge or _dist(nearest_ridge) >= 980.0))
    ):
        dominant_500_kind = "trough"
        dominant_focus = nearest_trough
    elif nearest_ridge and (
        ridge_relevance >= trough_relevance + 0.18
        or (_dist(nearest_ridge) <= 650.0 and (not nearest_trough or _dist(nearest_trough) >= 980.0))
    ):
        dominant_500_kind = "ridge"
        dominant_focus = nearest_ridge
    elif (
        nearest_ridge
        and nearest_trough
        and ridge_relevance >= 0.58
        and trough_relevance >= 0.58
        and abs(ridge_relevance - trough_relevance) <= 0.16
        and max(_dist(nearest_ridge), _dist(nearest_trough)) <= 1150.0
    ):
        dominant_500_kind = "transition"
        dominant_focus = nearest_trough if trough_relevance >= ridge_relevance else nearest_ridge
    elif nearest_trough and trough_relevance >= 0.42 and trough_relevance > ridge_relevance:
        dominant_500_kind = "trough"
        dominant_focus = nearest_trough
    elif nearest_ridge and ridge_relevance >= 0.42 and ridge_relevance > trough_relevance:
        dominant_500_kind = "ridge"
        dominant_focus = nearest_ridge

    def _dominant_low_level_coupling(kind: str) -> int:
        if kind == "trough":
            return int(bool(nearest_surface_low and _dist(nearest_surface_low) <= 850.0)) + int(shortwave_relevance >= 0.55)
        if kind == "ridge":
            return (
                int(bool(nearest_surface_high and _dist(nearest_surface_high) <= 850.0))
                + int(bool(nearest_warm_adv and _dist(nearest_warm_adv) <= 950.0))
                + int(subtropical_relation in {"core", "inner"})
            )
        return 0

    if dominant_focus and str(dominant_focus.get("detection_mode", "")).strip().lower() == "fallback_weak":
        fallback_coupling = _dominant_low_level_coupling(dominant_500_kind)
        fallback_distance = _dist(dominant_focus)
        fallback_axis = _axis_strength_abs(dominant_focus)
        if (
            (fallback_distance > 650.0 or fallback_axis < 2.8)
            and fallback_coupling < 2
        ) or (
            dominant_500_kind == "trough"
            and fallback_distance > 800.0
            and shortwave_relevance < 0.55
        ):
            dominant_500_kind = "weak"
            dominant_focus = None

    phase_hints = []
    dominant_hint = _phase_hint_from_sector(dominant_focus)
    if dominant_hint:
        phase_hints.append(dominant_hint)

    trends = [str(s.get("trend") or "") for s in s500]
    deepening = any(t == "deepening" for t in trends)
    strengthening = any(t == "strengthening" for t in trends)
    continuity_flag = "不明确"
    if deepening and dominant_500_kind in {"trough", "transition"} and trough_relevance >= ridge_relevance:
        continuity_flag = "槽加强"
    elif strengthening and dominant_500_kind in {"ridge", "transition"} and ridge_relevance >= trough_relevance:
        continuity_flag = "脊加强"
    elif any(t in {"filling", "weakening"} for t in trends):
        continuity_flag = "系统减弱"

    confidence = "低"
    signal_count = (
        int(ridge_relevance >= 0.45)
        + int(trough_relevance >= 0.45)
        + int(shortwave_relevance >= 0.45)
        + int(bool(phase_hints))
        + int(continuity_flag != "不明确")
    )
    if signal_count >= 2:
        confidence = "中"
    if signal_count >= 4:
        confidence = "中-高"

    phase = "中性"
    if dominant_500_kind == "transition":
        phase = "近区槽脊过渡"
    elif dominant_500_kind == "trough":
        phase = "槽相主导"
    elif dominant_500_kind == "ridge":
        phase = "脊相主导"

    # weak fallback-only detection should not overstate confidence
    if s500 and len(fallback_weak) == len(s500):
        if confidence == "中-高":
            confidence = "中"
        phase = phase if phase != "中性" else "弱信号背景"

    if subtropical_support >= 0.72:
        confidence = "中-高"
    elif subtropical_support >= 0.45 and confidence == "低":
        confidence = "中"

    regime_label = "高空弱信号背景"
    thermal_role = "weak"
    cold_high_lock = bool(
        ridge
        and nearest_surface_high
        and (_dist(nearest_surface_high) <= 1100.0)
        and nearest_cold_adv
        and (_dist(nearest_cold_adv) <= 1250.0)
    )
    subtropical_support_score = subtropical_support
    if strongest_subtropical:
        if subtropical_station_pct is not None:
            subtropical_support_score += min(0.18, max(0.0, (subtropical_station_pct - 60.0) / 120.0))
        if subtropical_edge588_margin is not None and subtropical_edge588_margin >= 0.5:
            subtropical_support_score += 0.14
        elif subtropical_edge586_margin is not None and subtropical_edge586_margin >= 0.0:
            subtropical_support_score += 0.08
        if bool(ridge) and (not nearest_ridge or _dist(nearest_ridge) <= 1400.0):
            subtropical_support_score += 0.10
        if nearest_surface_high and _dist(nearest_surface_high) <= 1200.0:
            subtropical_support_score += 0.06
        if nearest_warm_adv and _dist(nearest_warm_adv) <= 1400.0:
            subtropical_support_score += 0.10
        if dominant_500_kind == "ridge":
            subtropical_support_score += 0.08
        if nearest_cold_adv and _dist(nearest_cold_adv) <= 1200.0:
            subtropical_support_score -= 0.18
        if dominant_500_kind == "trough":
            subtropical_support_score -= 0.14
        if shortwave_relevance >= 0.45:
            subtropical_support_score -= 0.10
        if nearest_trough and _dist(nearest_trough) <= 1200.0 and trough_relevance >= 0.45:
            subtropical_support_score -= 0.18
        if westerly_intensity is not None and westerly_intensity >= 16.0:
            subtropical_support_score -= 0.08
    subtropical_support_score = max(0.0, min(1.0, subtropical_support_score))
    has_subtropical_control = bool(
        strongest_subtropical
        and subtropical_relation in {"core", "inner"}
        and subtropical_support_score >= 0.68
    )
    has_subtropical_edge = bool(
        strongest_subtropical
        and subtropical_relation in {"core", "inner", "edge"}
        and subtropical_support_score >= 0.40
    )

    if has_subtropical_control and not (
        shortwave_relevance >= 0.45
        or (nearest_trough and _dist(nearest_trough) <= 1100.0 and trough_relevance >= 0.52)
        or dominant_500_kind == "trough"
    ):
        regime_label = "副热带高压控制"
        thermal_role = "warm_high_subsidence"
    elif has_subtropical_edge and not (
        (nearest_trough and _dist(nearest_trough) <= 850.0 and trough_relevance >= 0.58)
        or (dominant_500_kind == "trough" and shortwave_relevance >= 0.45)
    ):
        regime_label = "副热带高压边缘"
        thermal_role = "warm_high_edge"
    elif dominant_500_kind == "transition":
        regime_label = "近区槽脊过渡"
        thermal_role = "transition"
    elif dominant_500_kind == "trough":
        trough_deep = deepening or bool(shortwave) or (_dist(nearest_surface_low) <= 900 if nearest_surface_low else False)
        regime_label = "低压深槽" if trough_deep else "低压槽"
        thermal_role = "trough_lift"
    elif dominant_500_kind == "ridge":
        ridge_warm = (
            strengthening
            or (_dist(nearest_surface_high) <= 900 if nearest_surface_high else False)
            or (_dist(nearest_warm_adv) <= 900 if nearest_warm_adv else False)
        )
        if cold_high_lock:
            regime_label = "冷高压稳定压温"
            thermal_role = "cold_high_suppression"
        else:
            regime_label = "高压暖脊" if ridge_warm else "高压脊"
            thermal_role = "warm_high_subsidence" if ridge_warm else "warm_high_edge"

    if phase == "中性":
        if regime_label in {"副热带高压控制", "副热带高压边缘", "高压暖脊", "高压脊", "冷高压稳定压温"}:
            phase = "脊相主导"
        elif regime_label in {"低压深槽", "低压槽"}:
            phase = "槽相主导"

    proximity = ""
    focus = dominant_focus or nearest_trough or nearest_ridge
    if focus:
        d = _dist(focus)
        if d <= 350:
            proximity = "近区"
        elif d <= 900:
            proximity = "中近区"
        else:
            proximity = "远区"
    elif regime_label == "副热带高压控制":
        proximity = "近区"
    elif regime_label == "副热带高压边缘":
        proximity = "中近区"

    pva_proxy = "中性"
    if regime_label in {"低压深槽", "低压槽"}:
        if shortwave_relevance >= 0.72 or (deepening and nearest_trough and _dist(nearest_trough) <= 700.0):
            pva_proxy = "PVA代理偏强（上升背景）"
        elif shortwave_relevance >= 0.45 or deepening:
            pva_proxy = "PVA代理中等（上升背景）"
        else:
            pva_proxy = "PVA代理偏弱（弱上升背景）"
    elif regime_label in {"副热带高压控制", "副热带高压边缘", "高压暖脊", "高压脊", "冷高压稳定压温"}:
        if has_subtropical_control or strengthening or (nearest_ridge and _dist(nearest_ridge) <= 700.0):
            pva_proxy = "NVA代理偏强（下沉背景）"
        else:
            pva_proxy = "NVA代理偏弱（弱下沉背景）"
    elif regime_label == "近区槽脊过渡":
        if trough_relevance >= ridge_relevance + 0.08 and shortwave_relevance >= 0.45:
            pva_proxy = "PVA代理偏弱（弱上升背景）"
        elif ridge_relevance >= trough_relevance + 0.08:
            pva_proxy = "NVA代理偏弱（弱下沉背景）"
        elif shortwave_relevance >= 0.55:
            pva_proxy = "PVA代理偏弱（弱上升背景）"

    vertical_motion_bg = "中性"
    if "上升" in pva_proxy:
        vertical_motion_bg = "上升倾向"
    elif "下沉" in pva_proxy:
        vertical_motion_bg = "下沉倾向"

    surface_coupling = "中性"
    if regime_label in {"低压深槽", "低压槽"} and nearest_surface_low and _dist(nearest_surface_low) <= 1000:
        surface_coupling = "地面低压配合"
    elif regime_label in {"副热带高压控制", "副热带高压边缘", "高压暖脊", "高压脊", "冷高压稳定压温"} and nearest_surface_high and _dist(nearest_surface_high) <= 1000:
        surface_coupling = "地面高压配合"

    forcing_text = "高空动力背景中性"
    if "上升" in pva_proxy:
        forcing_text = "动力抬升偏强" if "偏强" in pva_proxy or "中等" in pva_proxy else "弱动力抬升"
    elif "下沉" in pva_proxy:
        forcing_text = "下沉稳定偏强" if "偏强" in pva_proxy else "弱下沉稳定"

    pva_explained = "中性"
    if "PVA" in pva_proxy:
        pva_explained = f"{forcing_text}（PVA，正涡度平流，表示高空更利于上升）"
    elif "NVA" in pva_proxy:
        pva_explained = f"{forcing_text}（NVA，负涡度平流，表示高空更利于下沉）"

    height_text = ""
    if strongest_subtropical:
        height_text = _subtropical_height_text(subtropical_station_z500 or subtropical_intensity)
    elif regime_label == "冷高压稳定压温":
        height_text = "高度场平直偏高"
    elif regime_label in {"高压暖脊", "高压脊"}:
        height_text = "高度场偏高" if strengthening else "高度场平直偏高"
    elif regime_label == "低压深槽":
        height_text = "高度场偏低"
    elif regime_label == "低压槽":
        height_text = "高度场不高"

    score = {
        "warm_high_subsidence": 0.86,
        "warm_high_edge": 0.46,
        "cold_high_suppression": -0.82,
        "trough_lift": -0.72,
        "transition": 0.0,
        "weak": 0.0,
    }.get(thermal_role, 0.0)
    score *= _confidence_scale(confidence) * _proximity_scale(proximity)
    if score == 0.0 and regime_label == "近区槽脊过渡":
        if "NVA" in pva_proxy:
            score = 0.14 * _confidence_scale(confidence) * _proximity_scale(proximity)
        elif "PVA" in pva_proxy:
            score = -0.14 * _confidence_scale(confidence) * _proximity_scale(proximity)
    if strongest_subtropical and score > 0:
        score = max(score, 0.28 + 0.52 * subtropical_support_score)
        score += min(0.12, max(0.0, (((subtropical_station_z500 or subtropical_intensity or 5860.0) - 5860.0) / 180.0)))
    if nearest_surface_high and _dist(nearest_surface_high) <= 900.0:
        if score > 0:
            score += 0.05
        elif thermal_role == "cold_high_suppression":
            score -= 0.04
    if nearest_warm_adv and _dist(nearest_warm_adv) <= 1000.0 and score > 0:
        score += 0.10
    if nearest_cold_adv and _dist(nearest_cold_adv) <= 1000.0:
        if score > 0:
            score -= 0.14
        elif score < 0:
            score -= 0.10
    if bool(shortwave) and score < 0:
        score -= 0.08
    if "NVA" in pva_proxy:
        if score > 0:
            score += 0.10
        elif score < 0:
            score -= 0.08
    elif "PVA" in pva_proxy:
        if score < 0:
            score -= 0.10
        elif score > 0:
            score -= 0.12
        else:
            score = -0.08 * _confidence_scale(confidence) * _proximity_scale(proximity)
    if westerly_intensity is not None:
        if score > 0 and westerly_intensity >= 16.0:
            score -= 0.06
        elif score < 0 and westerly_intensity >= 16.0:
            score -= 0.04
    score = max(-1.0, min(1.0, score))

    impact_weight = "low"
    if abs(score) >= 0.35:
        impact_weight = "medium"
    if abs(score) >= 0.65:
        impact_weight = "high"

    tmax_bias_label = "中性"
    if score >= 0.35:
        tmax_bias_label = "明显增温支持"
    elif score >= 0.15:
        tmax_bias_label = "轻度增温支持"
    elif score <= -0.35:
        tmax_bias_label = "明显压温支持"
    elif score <= -0.15:
        tmax_bias_label = "轻度压温支持"

    notable_params: list[str] = []
    if proximity:
        notable_params.append(f"{proximity}主导")
    if surface_coupling != "中性":
        notable_params.append(surface_coupling)
    if shortwave_relevance >= 0.45 and regime_label in {"低压深槽", "低压槽", "近区槽脊过渡"}:
        notable_params.append("短波扰动嵌入")
    if continuity_flag != "不明确":
        notable_params.append(continuity_flag)
    if strongest_subtropical:
        relation_label = {
            "core": "副高核心",
            "inner": "副高内侧",
            "edge": "副高边缘",
        }.get(subtropical_relation, "")
        if relation_label:
            notable_params.append(relation_label)

    return {
        "phase": phase,
        "phase_hint": phase_hints[0] if phase_hints else None,
        "pva_proxy": pva_proxy,
        "vertical_motion_bg": vertical_motion_bg,
        "trend_12_24h": continuity_flag,
        "confidence": confidence,
        "regime_label": regime_label,
        "proximity": proximity,
        "surface_coupling": surface_coupling,
        "forcing_text": forcing_text,
        "pva_explained": pva_explained,
        "impact_weight": impact_weight,
        "notable_params": notable_params[:3],
        "height_text": height_text,
        "thermal_role": thermal_role,
        "tmax_weight_score": round(score, 3),
        "tmax_bias_label": tmax_bias_label,
        "subtropical_high_detected": bool(strongest_subtropical),
        "subtropical_high_strength_gpm": round(subtropical_intensity, 1) if subtropical_intensity is not None else None,
        "subtropical_station_z500_gpm": round(subtropical_station_z500, 1) if subtropical_station_z500 is not None else None,
        "subtropical_station_pct_in_band": round(subtropical_station_pct, 1) if subtropical_station_pct is not None else None,
        "subtropical_relation": subtropical_relation or None,
        "subtropical_support_score": round(subtropical_support_score, 3) if strongest_subtropical else None,
        "subtropical_edge_586_margin_deg": round(subtropical_edge586_margin, 2) if subtropical_edge586_margin is not None else None,
        "subtropical_edge_588_margin_deg": round(subtropical_edge588_margin, 2) if subtropical_edge588_margin is not None else None,
        "westerly_belt_detected": bool(strongest_westerly),
        "westerly_belt_intensity_ms": round(westerly_intensity, 1) if westerly_intensity is not None else None,
    }
