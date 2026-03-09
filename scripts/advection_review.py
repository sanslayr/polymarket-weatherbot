from __future__ import annotations

from datetime import datetime, timedelta
from typing import Any

from diagnostics_850 import advection_eta_local, distance_km_from_system, label_from_score
from synoptic_regime import advection_reach_score


ADVECTION_REVIEW_SCHEMA_VERSION = "advection-review.v2"

_ROLE_RANK = {
    "low_representativeness": 0,
    "background": 1,
    "influence": 2,
    "dominant": 3,
}
_ROLE_WEIGHT = {
    "low_representativeness": 0.0,
    "background": 0.22,
    "influence": 0.58,
    "dominant": 1.0,
}
_COUPLING_RANK = {
    "weak": 0.0,
    "partial": 0.55,
    "strong": 1.0,
}


def _safe_float(value: Any) -> float | None:
    try:
        if value in (None, ""):
            return None
        return float(value)
    except Exception:
        return None


def _clamp01(value: float) -> float:
    return max(0.0, min(1.0, float(value)))


def _window_dt_bounds(primary_window: dict[str, Any], fallback_now: datetime) -> tuple[datetime, datetime]:
    start_txt = str(primary_window.get("start_local") or "")
    end_txt = str(primary_window.get("end_local") or "")
    try:
        start_dt = datetime.strptime(start_txt, "%Y-%m-%dT%H:%M")
    except Exception:
        start_dt = fallback_now
    try:
        end_dt = datetime.strptime(end_txt, "%Y-%m-%dT%H:%M")
    except Exception:
        end_dt = start_dt
    if fallback_now.tzinfo is not None:
        if start_dt.tzinfo is None:
            start_dt = start_dt.replace(tzinfo=fallback_now.tzinfo)
        if end_dt.tzinfo is None:
            end_dt = end_dt.replace(tzinfo=fallback_now.tzinfo)
    if end_dt < start_dt:
        end_dt = start_dt
    return start_dt, end_dt


def _estimate_advection_eta_hours(distance_km: float | None, score: float, w850_kmh: float | None) -> tuple[float, float]:
    if distance_km is None or distance_km <= 0:
        return (0.0, 0.0)
    base_speed = max(18.0, (w850_kmh or 30.0) * 0.55)
    center = distance_km / base_speed
    spread = max(1.0, 4.0 - 2.5 * score)
    lo = max(0.0, center - spread)
    hi = max(lo + 0.5, center + spread)
    return (lo, hi)


def select_primary_850_advection_system(
    systems: list[dict[str, Any]],
    *,
    now_local: datetime,
    primary_window: dict[str, Any],
    w850_kmh: float | None,
) -> tuple[dict[str, Any] | None, float | None, str]:
    if not systems:
        return None, None, ""

    window_start, window_end = _window_dt_bounds(primary_window, now_local)
    window_mid = window_start + (window_end - window_start) / 2
    best_system: dict[str, Any] | None = None
    best_score: float | None = None
    best_tag = ""

    for system in systems:
        reach_score, _ = advection_reach_score(system, w850_kmh)
        distance_km = distance_km_from_system(system)
        eta_lo_h, eta_hi_h = _estimate_advection_eta_hours(distance_km, reach_score, w850_kmh)
        impact_start = now_local + timedelta(hours=eta_lo_h)
        impact_end = now_local + timedelta(hours=eta_hi_h)

        overlap = not (
            impact_end < (window_start - timedelta(hours=1.0))
            or impact_start > (window_end + timedelta(hours=1.0))
        )
        window_distance_h = abs(
            ((impact_start + (impact_end - impact_start) / 2) - window_mid).total_seconds()
        ) / 3600.0

        ranking = reach_score * 1.2
        tag = "窗口外"
        if overlap:
            ranking += 0.8
            tag = "窗口期内"
        elif window_distance_h <= 2.5:
            ranking += 0.45
            tag = "窗口期附近"
        elif impact_start > window_end:
            tag = "偏后段"
        else:
            tag = "偏前段"

        if distance_km is not None:
            ranking -= min(distance_km / 4000.0, 0.25)

        if best_score is None or ranking > best_score:
            best_system = system
            best_score = ranking
            best_tag = tag

    return best_system, best_score, best_tag


def _advection_type(system: dict[str, Any] | None) -> str:
    stype = str((system or {}).get("system_type") or "").lower()
    if "warm" in stype:
        return "warm"
    if "cold" in stype:
        return "cold"
    return "mixed"


def _timing_score(window_tag: str) -> float:
    return {
        "窗口期内": 1.0,
        "窗口期附近": 0.78,
        "偏后段": 0.38,
        "偏前段": 0.32,
        "窗口外": 0.18,
    }.get(str(window_tag or ""), 0.25)


def _terrain_penalty(terrain_tag: str) -> tuple[float, str]:
    txt = str(terrain_tag or "")
    if not txt:
        return 0.0, "unknown"
    if any(token in txt for token in ("山地", "高原", "高地", "盆地", "谷地")):
        return 0.18, "complex"
    if any(token in txt for token in ("丘陵", "海陆", "填海", "海湾", "河口")):
        return 0.10, "mixed"
    return 0.0, "simple"


def _coupling_support_score(
    *,
    h925_summary: str,
    w850_kmh: float | None,
    low_cloud_pct: float | None,
) -> float:
    score = 0.52
    if "耦合偏强" in str(h925_summary or ""):
        score += 0.28
    elif "耦合偏弱" in str(h925_summary or ""):
        score -= 0.35

    if w850_kmh is not None:
        if w850_kmh >= 42.0:
            score += 0.18
        elif w850_kmh >= 28.0:
            score += 0.08
        elif w850_kmh <= 16.0:
            score -= 0.12

    if low_cloud_pct is not None:
        if low_cloud_pct >= 85.0:
            score -= 0.08
        elif low_cloud_pct <= 25.0:
            score += 0.05

    return _clamp01(score)


def _surface_role(
    *,
    distance_km: float | None,
    timing_score: float,
    coupling_support_score: float,
    representativeness_score: float,
    window_tag: str,
) -> tuple[str, list[str]]:
    reason_codes: list[str] = []
    if distance_km is not None:
        if distance_km >= 800.0:
            reason_codes.append("remote_over_800km")
        elif distance_km >= 400.0:
            reason_codes.append("remote_over_400km")
        elif distance_km <= 200.0:
            reason_codes.append("near_station")

    if coupling_support_score <= 0.35:
        reason_codes.append("weak_925_coupling")
    elif coupling_support_score >= 0.7:
        reason_codes.append("good_925_coupling")

    if timing_score <= 0.4:
        reason_codes.append("timing_not_in_primary_window")
    elif timing_score >= 0.75:
        reason_codes.append("timing_matches_primary_window")

    role = "background"
    if (
        representativeness_score >= 0.78
        and coupling_support_score >= 0.58
        and timing_score >= 0.75
    ):
        role = "dominant"
    elif (
        representativeness_score >= 0.55
        and coupling_support_score >= 0.42
        and timing_score >= 0.38
    ):
        role = "influence"
    elif representativeness_score < 0.24:
        role = "low_representativeness"

    if distance_km is not None and distance_km >= 800.0:
        role = "low_representativeness" if coupling_support_score < 0.55 else "background"
    elif distance_km is not None and distance_km >= 400.0 and coupling_support_score < 0.45:
        role = "background" if representativeness_score >= 0.30 else "low_representativeness"
    elif window_tag == "偏后段" and coupling_support_score < 0.55:
        role = "background"

    return role, reason_codes


def _thermal_advection_state(
    *,
    advection_type: str,
    surface_role: str,
    reach_score: float,
    coupling_support_score: float,
    timing_score: float,
) -> str:
    if advection_type not in {"warm", "cold"}:
        return "none"
    if (
        surface_role == "dominant"
        and reach_score >= 0.58
        and coupling_support_score >= 0.58
        and timing_score >= 0.75
    ):
        return "confirmed"
    if (
        surface_role in {"dominant", "influence"}
        and reach_score >= 0.42
        and coupling_support_score >= 0.42
        and timing_score >= 0.38
    ):
        return "probable"
    if reach_score >= 0.26:
        return "weak"
    return "none"


def _transport_state(advection_type: str) -> str:
    if advection_type == "warm":
        return "warm"
    if advection_type == "cold":
        return "cold"
    if advection_type == "mixed":
        return "mixed"
    return "neutral"


def _surface_coupling_state(coupling_support_score: float) -> str:
    if coupling_support_score >= 0.68:
        return "strong"
    if coupling_support_score >= 0.42:
        return "partial"
    return "weak"


def build_850_advection_review(
    systems: list[dict[str, Any]],
    *,
    now_local: datetime,
    primary_window: dict[str, Any],
    h925_summary: str = "",
    terrain_tag: str = "",
) -> dict[str, Any]:
    w850_kmh = _safe_float(primary_window.get("w850_kmh"))
    low_cloud_pct = _safe_float(primary_window.get("low_cloud_pct"))
    selected_system, ranking, window_tag = select_primary_850_advection_system(
        systems,
        now_local=now_local,
        primary_window=primary_window,
        w850_kmh=w850_kmh,
    )
    if not isinstance(selected_system, dict):
        return {
            "schema_version": ADVECTION_REVIEW_SCHEMA_VERSION,
            "has_signal": False,
            "summary_line": "低层输送信号一般。",
            "advection_type": "none",
            "thermal_advection_state": "none",
            "transport_state": "neutral",
            "surface_coupling_state": "weak",
            "surface_role": "none",
            "surface_bias": "none",
            "surface_effect_weight": 0.0,
            "selected_system": None,
            "representativeness_score": 0.0,
            "coupling_support_score": 0.0,
            "timing_score": 0.0,
            "distance_km": None,
            "distance_band": "",
            "timing_tag": "",
            "terrain_context": terrain_tag or "",
            "terrain_representativeness": "unknown",
            "reach_score": 0.0,
            "reach_level": "低",
            "eta_local": "",
            "reason_codes": ["no_advection_object"],
            "ranking_score": ranking,
        }

    distance_km = distance_km_from_system(selected_system)
    geo = dict(selected_system.get("geo_context") or {})
    distance_band = str(geo.get("distance_band") or "")
    reach_score, reach_level = advection_reach_score(selected_system, w850_kmh)
    timing_score = _timing_score(window_tag)
    coupling_support_score = _coupling_support_score(
        h925_summary=h925_summary,
        w850_kmh=w850_kmh,
        low_cloud_pct=low_cloud_pct,
    )
    terrain_penalty, terrain_repr = _terrain_penalty(terrain_tag)
    representativeness_score = _clamp01(
        0.45 * reach_score
        + 0.35 * coupling_support_score
        + 0.20 * timing_score
        - terrain_penalty
    )
    surface_role, reason_codes = _surface_role(
        distance_km=distance_km,
        timing_score=timing_score,
        coupling_support_score=coupling_support_score,
        representativeness_score=representativeness_score,
        window_tag=window_tag,
    )
    advection_type = _advection_type(selected_system)
    thermal_advection_state = _thermal_advection_state(
        advection_type=advection_type,
        surface_role=surface_role,
        reach_score=reach_score,
        coupling_support_score=coupling_support_score,
        timing_score=timing_score,
    )
    transport_state = _transport_state(advection_type)
    surface_coupling_state = _surface_coupling_state(coupling_support_score)
    surface_bias = advection_type if advection_type in {"warm", "cold"} else "none"
    effect_weight = round(
        _ROLE_WEIGHT.get(surface_role, 0.0)
        * _COUPLING_RANK.get(surface_coupling_state, 0.0),
        3,
    )
    eta_local = advection_eta_local(now_local, distance_km, reach_score, w850_kmh)
    prob_label = label_from_score(reach_score)

    type_label = {
        "warm": "暖平流",
        "cold": "冷平流",
    }.get(advection_type, "输送")
    bg_bias_label = {
        "warm": "偏暖输送",
        "cold": "偏冷输送",
    }.get(advection_type, "温度输送")

    if surface_role == "dominant":
        summary_line = f"{type_label}{window_tag}，近站落地链条较完整（{prob_label}，{eta_local}）"
    elif surface_role == "influence":
        summary_line = f"{type_label}{window_tag}，但是否主导仍需看925耦合与近地风场（{prob_label}，{eta_local}）"
    elif surface_role == "background":
        phase_txt = "后段背景项" if window_tag == "偏后段" else "背景约束项"
        summary_line = f"850{bg_bias_label}距站偏远或落地不完整，先按{phase_txt}处理（{prob_label}，{eta_local}）"
    else:
        summary_line = f"850{bg_bias_label}信号距站偏远或代表性不足，暂不前置为站点主导。"

    return {
        "schema_version": ADVECTION_REVIEW_SCHEMA_VERSION,
        "has_signal": True,
        "summary_line": summary_line,
        "advection_type": advection_type,
        "thermal_advection_state": thermal_advection_state,
        "transport_state": transport_state,
        "surface_coupling_state": surface_coupling_state,
        "surface_role": surface_role,
        "surface_bias": surface_bias,
        "surface_effect_weight": effect_weight,
        "selected_system": selected_system,
        "representativeness_score": round(representativeness_score, 3),
        "coupling_support_score": round(coupling_support_score, 3),
        "timing_score": round(timing_score, 3),
        "distance_km": round(float(distance_km), 1) if distance_km is not None else None,
        "distance_band": distance_band,
        "timing_tag": window_tag,
        "terrain_context": terrain_tag or "",
        "terrain_representativeness": terrain_repr,
        "reach_score": round(reach_score, 3),
        "reach_level": reach_level,
        "eta_local": eta_local,
        "reason_codes": reason_codes,
        "ranking_score": round(float(ranking or 0.0), 3),
    }


def effective_advection_weight(
    review: dict[str, Any] | None,
    *,
    bias: str,
    line850: str = "",
) -> float:
    review_dict = review if isinstance(review, dict) else {}
    if review_dict.get("has_signal"):
        if str(review_dict.get("transport_state") or review_dict.get("surface_bias") or "") == bias:
            strength = str(review_dict.get("thermal_advection_state") or "")
            if strength == "confirmed":
                return max(float(review_dict.get("surface_effect_weight") or 0.0), 0.7)
            if strength == "probable":
                return max(float(review_dict.get("surface_effect_weight") or 0.0), 0.4)
            if strength == "weak":
                return max(float(review_dict.get("surface_effect_weight") or 0.0), 0.18)
            return float(review_dict.get("surface_effect_weight") or 0.0)
        return 0.0

    if bias == "warm" and ("暖平流" in str(line850)) and ("冷平流" not in str(line850)):
        return 0.55
    if bias == "cold" and ("冷平流" in str(line850)) and ("暖平流" not in str(line850)):
        return 0.55
    return 0.0


def has_surface_advection_signal(
    review: dict[str, Any] | None,
    *,
    bias: str,
    line850: str = "",
    min_weight: float = 0.28,
) -> bool:
    return effective_advection_weight(review, bias=bias, line850=line850) >= min_weight


def surface_role_rank(review: dict[str, Any] | None) -> int:
    if not isinstance(review, dict):
        return 0
    return _ROLE_RANK.get(str(review.get("surface_role") or ""), 0)


def thermal_advection_direction(review: dict[str, Any] | None, *, line850: str = "") -> str:
    review_dict = review if isinstance(review, dict) else {}
    if str(review_dict.get("thermal_advection_state") or "") in {"confirmed", "probable", "weak"}:
        direction = str(review_dict.get("transport_state") or review_dict.get("surface_bias") or "")
        if direction in {"warm", "cold"}:
            return direction
    txt = str(line850 or "")
    if ("暖平流" in txt) and ("冷平流" not in txt):
        return "warm"
    if ("冷平流" in txt) and ("暖平流" not in txt):
        return "cold"
    return "neutral"
