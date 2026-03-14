#!/usr/bin/env python3
"""Uncalibrated weather posterior core from quantitative feature contracts."""

from __future__ import annotations

import math
from datetime import datetime, timedelta
from typing import Any

from contracts import WEATHER_POSTERIOR_CORE_SCHEMA_VERSION
from posterior_regime_adjuster import apply_regime_effects
from regime_detector import detect_station_regimes


def _safe_float(value: Any) -> float | None:
    try:
        if value in (None, ""):
            return None
        return float(value)
    except Exception:
        return None


def _clamp(value: float, lower: float, upper: float) -> float:
    return max(lower, min(upper, float(value)))


def _logistic(score: float) -> float:
    return 1.0 / (1.0 + math.exp(-float(score)))


def _ensure_monotonic(values: list[float], *, floor: float | None = None) -> list[float]:
    out: list[float] = []
    running = floor if floor is not None else None
    for value in values:
        adjusted = float(value)
        if running is not None:
            adjusted = max(adjusted, running)
        out.append(adjusted)
        running = adjusted
    return out


def _weighted_mean_pairs(pairs: list[tuple[float, float | None]]) -> float | None:
    valid = [(float(weight), float(value)) for weight, value in pairs if value is not None and float(weight) > 0.0]
    if not valid:
        return None
    total_weight = sum(weight for weight, _value in valid)
    if total_weight <= 0.0:
        return None
    return sum(weight * value for weight, value in valid) / total_weight


def _weighted_quantile_pairs(pairs: list[tuple[float, float | None]], quantile: float) -> float | None:
    valid = sorted((float(value), float(weight)) for weight, value in pairs if value is not None and float(weight) > 0.0)
    if not valid:
        return None
    total_weight = sum(weight for _value, weight in valid)
    if total_weight <= 0.0:
        return None
    target = total_weight * _clamp(float(quantile), 0.0, 1.0)
    running = 0.0
    for value, weight in valid:
        running += weight
        if running >= target:
            return value
    return valid[-1][0]


def _blend_value(left: float, right: float, weight: float) -> float:
    w = _clamp(float(weight), 0.0, 1.0)
    return (1.0 - w) * float(left) + w * float(right)


def _member_distribution_blend_weights(
    *,
    phase: str,
    active_source: str,
    effective_member_count: float | None,
    dominant_share: float | None,
) -> tuple[float, float]:
    center_blend = {
        "far": 0.18,
        "same_day": 0.32,
        "near_window": 0.52,
        "in_window": 0.64,
        "post": 0.58,
    }.get(phase, 0.28)
    quantile_blend = {
        "far": 0.10,
        "same_day": 0.22,
        "near_window": 0.44,
        "in_window": 0.58,
        "post": 0.54,
    }.get(phase, 0.20)

    eff = _safe_float(effective_member_count)
    if eff is not None:
        if eff >= 12.0:
            center_blend += 0.08
            quantile_blend += 0.08
        elif eff >= 8.0:
            center_blend += 0.04
            quantile_blend += 0.05
        elif eff <= 4.0:
            center_blend -= 0.08
            quantile_blend -= 0.08

    dom = _safe_float(dominant_share)
    if dom is not None:
        if dom >= 0.76:
            center_blend += 0.06
            quantile_blend += 0.08
        elif dom < 0.50:
            center_blend -= 0.08
            quantile_blend -= 0.10

    if active_source == "matched_subset":
        center_blend += 0.10
        quantile_blend += 0.12
    elif active_source == "observed_path":
        center_blend += 0.05
        quantile_blend += 0.06

    # ENS surface gives useful branch direction, but its 3-hour cadence is still too coarse
    # for the shortest-term structure; near the peak, keep more weight on observed progress.
    if phase == "near_window":
        center_blend -= 0.04
        quantile_blend -= 0.05
    elif phase in {"in_window", "post"}:
        center_blend -= 0.08
        quantile_blend -= 0.10

    return _clamp(center_blend, 0.0, 0.82), _clamp(quantile_blend, 0.0, 0.86)


def _build_member_conditioned_distribution(
    *,
    phase: str,
    member_evolution_state: dict[str, Any],
    floor_c: float | None,
    observed_anchor_c: float | None,
    latest_temp_c: float | None,
    modeled_peak_c: float | None,
    modeled_headroom_c: float | None,
    hours_to_peak: float | None,
    temp_trend_c: float,
    temp_bias_c: float,
    cloud_cover: float | None,
    radiation_eff: float | None,
    precip_state: str,
) -> dict[str, Any]:
    state = dict(member_evolution_state or {})
    rows = [dict(raw) for raw in state.get("members") or [] if isinstance(raw, dict)]
    if not rows:
        return {}

    anchor_base = observed_anchor_c
    if anchor_base is None:
        anchor_base = floor_c if floor_c is not None else latest_temp_c
    if anchor_base is None:
        anchor_base = modeled_peak_c
    if anchor_base is None:
        return {}

    floor_value = floor_c if floor_c is not None else anchor_base
    positive_trend = max(0.0, float(temp_trend_c or 0.0))
    positive_bias = max(0.0, float(temp_bias_c or 0.0))
    cloud_drag = 0.0
    if cloud_cover is not None:
        if cloud_cover >= 0.80:
            cloud_drag += 0.12
        elif cloud_cover >= 0.65:
            cloud_drag += 0.06
    if radiation_eff is not None and radiation_eff <= 0.58:
        cloud_drag += 0.05
    if str(precip_state or "").strip().lower() not in {"", "none"}:
        cloud_drag += 0.10

    phase_room_floor = {
        "far": 0.55,
        "same_day": 0.44,
        "near_window": 0.30,
        "in_window": 0.20,
        "post": 0.08,
    }.get(phase, 0.36)
    runway = 0.55
    if hours_to_peak is not None:
        runway = _clamp(float(hours_to_peak) / 3.0, 0.12, 1.0)
    base_room = max(
        phase_room_floor,
        max(0.0, float(modeled_headroom_c or 0.0)) * 0.74
        + positive_trend * 0.52
        + positive_bias * 0.16
        + runway * 0.10
        - cloud_drag,
    )
    overshoot_base = max(0.0, positive_trend * 0.18 + positive_bias * 0.08 - cloud_drag * 0.20)

    projected_pairs: list[tuple[float, float]] = []
    path_weight_scores: dict[str, float] = {}
    family_weight_scores: dict[str, float] = {}
    surface_alignment_pairs: list[tuple[float, float]] = []
    for raw in rows:
        weight = _safe_float(raw.get("compatibility_weight"))
        if weight is None or weight <= 0.0:
            continue
        room_factor = _safe_float(raw.get("room_factor")) or 0.40
        overshoot_factor = _safe_float(raw.get("overshoot_factor")) or 0.0
        stall_risk = _safe_float(raw.get("stall_risk")) or 0.0
        delta_t850_c = _safe_float(raw.get("delta_t850_c")) or 0.0
        prior_surface_delta_c = _safe_float(raw.get("prior3h_t2m_delta_c"))
        future_room_c = _safe_float(raw.get("future_room_c"))
        next_surface_delta_c = _safe_float(raw.get("next3h_t2m_delta_c"))
        surface_temp_gap_c = _safe_float(raw.get("surface_temp_gap_c"))
        surface_alignment_score = _safe_float(raw.get("surface_alignment_score"))
        future_family = str(raw.get("future_family") or "")
        path_label = str(raw.get("path_label") or "")
        path_side = str(raw.get("path_side") or "")
        has_surface_member_detail = any(
            value is not None
            for value in (next_surface_delta_c, prior_surface_delta_c, surface_temp_gap_c, surface_alignment_score)
        )
        surface_signal_c = next_surface_delta_c
        if surface_signal_c is None:
            surface_signal_c = prior_surface_delta_c

        live_bonus = 0.0
        if path_side == "warm":
            live_bonus += positive_trend * 0.32 + positive_bias * 0.10
        elif path_side == "cold":
            live_bonus -= positive_trend * 0.16 + positive_bias * 0.06

        if has_surface_member_detail:
            dynamic_room_c = (
                base_room * room_factor
                + max(0.0, float(surface_signal_c or 0.0)) * 0.26
                + delta_t850_c * 0.06
                + live_bonus
                - stall_risk * 0.24
            )
        else:
            dynamic_room_c = base_room * room_factor + delta_t850_c * 0.18 + live_bonus - stall_risk * 0.24
        surface_room_c = 0.0
        if future_room_c is not None:
            surface_room_c += max(0.0, float(future_room_c)) * 0.92
        if next_surface_delta_c is not None:
            surface_room_c += max(0.0, float(next_surface_delta_c)) * 0.56
        if surface_room_c > 0.0:
            remaining_room_c = max(0.0, dynamic_room_c * 0.54 + surface_room_c * 0.46)
        else:
            remaining_room_c = max(0.0, dynamic_room_c)
        if surface_alignment_score is not None:
            remaining_room_c *= 0.84 + 0.26 * _clamp(surface_alignment_score, 0.0, 1.0)
        if surface_temp_gap_c is not None:
            gap_abs = abs(float(surface_temp_gap_c))
            if gap_abs >= 2.8:
                remaining_room_c *= 0.72
            elif gap_abs >= 1.8:
                remaining_room_c *= 0.86
            elif gap_abs <= 0.45:
                remaining_room_c *= 1.05
        if future_family in {"cold_hold", "cold_landing_pending"}:
            remaining_room_c *= 0.32 if future_family == "cold_hold" else 0.48
        elif future_family == "neutral_plateau":
            remaining_room_c *= 0.62
        elif future_family == "volatile_transition":
            remaining_room_c *= 0.86

        projected_peak_c = float(anchor_base) + remaining_room_c + max(0.0, overshoot_base * overshoot_factor)
        projected_peak_c = max(float(floor_value), projected_peak_c)
        if modeled_peak_c is not None:
            soft_cap = float(modeled_peak_c) + 0.18
            surface_warm_signal_c = max(0.0, float(surface_signal_c or 0.0))
            if future_family == "warm_follow_through":
                soft_cap = float(modeled_peak_c) + 0.58 + surface_warm_signal_c * 0.20
                if not has_surface_member_detail:
                    soft_cap += max(0.0, delta_t850_c) * 0.10
            elif future_family in {"warm_landing_pending", "second_peak_retest"}:
                soft_cap = float(modeled_peak_c) + 0.34 + surface_warm_signal_c * 0.16
                if not has_surface_member_detail:
                    soft_cap += max(0.0, delta_t850_c) * 0.08
            elif future_family == "volatile_transition":
                soft_cap = float(modeled_peak_c) + 0.34
            if surface_alignment_score is not None and _clamp(surface_alignment_score, 0.0, 1.0) >= 0.72:
                if future_family == "warm_follow_through":
                    soft_cap += 0.18
                elif future_family in {"warm_landing_pending", "second_peak_retest"}:
                    soft_cap += 0.12
            if next_surface_delta_c is not None and float(next_surface_delta_c) >= 0.25 and future_family in {"warm_follow_through", "warm_landing_pending"}:
                soft_cap += 0.08
            projected_peak_c = min(projected_peak_c, max(float(floor_value), soft_cap))

        projected_pairs.append((float(weight), round(projected_peak_c, 3)))
        if surface_alignment_score is not None:
            surface_alignment_pairs.append((float(weight), float(surface_alignment_score)))
        path_weight_scores[path_label] = path_weight_scores.get(path_label, 0.0) + float(weight)
        family_weight_scores[future_family] = family_weight_scores.get(future_family, 0.0) + float(weight)

    if not projected_pairs:
        return {}

    quantiles = {
        "p10_c": _weighted_quantile_pairs(projected_pairs, 0.10),
        "p25_c": _weighted_quantile_pairs(projected_pairs, 0.25),
        "p50_c": _weighted_quantile_pairs(projected_pairs, 0.50),
        "p75_c": _weighted_quantile_pairs(projected_pairs, 0.75),
        "p90_c": _weighted_quantile_pairs(projected_pairs, 0.90),
    }
    if any(value is None for value in quantiles.values()):
        return {}

    total_weight = sum(weight for weight, _value in projected_pairs)
    dominant_share = None
    if total_weight > 0.0 and path_weight_scores:
        dominant_share = max(path_weight_scores.values()) / total_weight
    weighted_surface_alignment = _weighted_mean_pairs(surface_alignment_pairs) if surface_alignment_pairs else None
    effective_member_count = _safe_float(state.get("effective_member_count"))
    center_blend, quantile_blend = _member_distribution_blend_weights(
        phase=phase,
        active_source=str(state.get("active_source") or ""),
        effective_member_count=effective_member_count,
        dominant_share=dominant_share,
    )
    if weighted_surface_alignment is not None:
        center_blend += max(0.0, float(weighted_surface_alignment) - 0.60) * 0.16
        quantile_blend += max(0.0, float(weighted_surface_alignment) - 0.58) * 0.14
    spread_proxy_c = max(
        0.18,
        (
            float(quantiles["p75_c"]) - float(quantiles["p25_c"])
        ) / 1.27,
    )
    return {
        "quantiles": {
            key: round(float(value), 2)
            for key, value in quantiles.items()
        },
        "weighted_mean_c": round(_weighted_mean_pairs(projected_pairs) or float(quantiles["p50_c"]), 2),
        "spread_proxy_c": round(spread_proxy_c, 2),
        "center_blend_weight": round(center_blend, 2),
        "quantile_blend_weight": round(quantile_blend, 2),
        "effective_member_count": effective_member_count,
        "dominant_weighted_path": str(state.get("dominant_weighted_path") or ""),
        "dominant_weighted_future_family": str(state.get("dominant_weighted_future_family") or ""),
        "weighted_surface_alignment": round(float(weighted_surface_alignment), 2) if weighted_surface_alignment is not None else None,
    }


def _advection_signed_adjustment(transport_state: str, thermal_state: str) -> float:
    if thermal_state not in {"probable", "confirmed"}:
        return 0.0
    magnitude = 0.18 if thermal_state == "probable" else 0.32
    if transport_state == "warm":
        return magnitude
    if transport_state == "cold":
        return -magnitude
    return 0.0


def _path_side(path_label: str, *, path_detail: str = "") -> str:
    key = path_detail if str(path_label or "") == "transition" and str(path_detail or "") else str(path_label or "")
    mapping = {
        "warm_support": "warm",
        "weak_warm_transition": "warm",
        "cold_suppression": "cold",
        "weak_cold_transition": "cold",
        "transition": "neutral",
        "neutral_stable": "neutral",
    }
    return mapping.get(key, "")


def _branch_family_label(value: str) -> str:
    return {
        "warm_landing_watch": "偏暖风抬温未站稳",
        "warm_support_track": "偏暖风抬温延续",
        "warm_transition_probe": "偏暖风试探入场",
        "cold_suppression_track": "偏冷风压温延续",
        "cold_transition_probe": "偏冷风试探压温",
        "convective_interrupt_risk": "对流前沿打断风险",
        "convective_cold_hold": "对流冷压延续",
        "cloud_release_watch": "云层压制待松动",
        "neutral_plateau": "风热条件平台",
        "volatile_split": "高波动分歧分支",
        "second_peak_retest": "后段再摸前高",
        "mixed_transition": "过渡分支",
    }.get(str(value or ""), str(value or "当前分支"))


def _branch_gate_label(value: str) -> str:
    return {
        "low_level_coupling": "地面偏暖风和升温能否真正续上",
        "cloud_release": "云层能否真正松开",
        "convective_intrusion": "雷达回波会不会压进站点",
        "convective_persistence": "雷雨压温会不会继续维持",
        "cold_advection_hold": "偏冷风和压温会不会继续维持",
        "rebreak_signal": "能不能再破前高",
        "branch_resolution": "分支何时收敛",
        "reacceleration": "升温能不能重新提速",
        "follow_through": "后段升温能不能继续续上",
    }.get(str(value or ""), str(value or "后续门槛"))


def _branch_path_label(path_label: str, *, path_detail: str = "") -> str:
    key = path_detail if str(path_label or "") == "transition" and str(path_detail or "") else str(path_label or "")
    return {
        "warm_support": "偏暖风抬温延续",
        "weak_warm_transition": "偏暖风抬温未站稳",
        "cold_suppression": "偏冷风压温延续",
        "weak_cold_transition": "偏冷风压温未站稳",
        "neutral_stable": "风热条件平台",
        "transition": "过渡路径",
    }.get(key, "当前路径")


def _branch_stage_clause(branch_family: str) -> str:
    return {
        "warm_landing_watch": "眼下偏暖风已进场，但地面抬温还没完全接上",
        "warm_support_track": "偏暖风和抬温已经在同步兑现",
        "warm_transition_probe": "偏暖风还在试探入场，地面响应还不稳",
        "cold_suppression_track": "偏冷风和压温已经在同步兑现",
        "cold_transition_probe": "偏冷风还在试探入场，压温还没完全接上",
        "convective_interrupt_risk": "但雷达回波前沿还可能提前打断升温",
        "convective_cold_hold": "雷雨压温这一步还在维持",
        "cloud_release_watch": "云层压制还没完全松开",
        "neutral_plateau": "风热条件暂时都在走平",
        "volatile_split": "风场和热力响应还没收敛",
        "second_peak_retest": "后段还留着再摸前高的可能",
        "mixed_transition": "眼下仍在过渡段里",
    }.get(str(branch_family or ""), "眼下还在等下一步演化")


def _branch_match_count_text(
    *,
    branch_source: str,
    ensemble_member_count: float | None,
    branch_member_count: float | None,
    matched_member_count: float | None,
    branch_dominant_prob: float | None,
) -> str:
    total = _safe_float(ensemble_member_count)
    if total is None or total < 1.0:
        return ""
    total_count = max(1, int(round(total)))
    branch_count = _safe_float(branch_member_count)
    if branch_count is not None and branch_count >= 1.0:
        active_count = max(1, min(total_count, int(round(branch_count))))
        if str(branch_source or "") == "matched_subset" and _safe_float(matched_member_count) is not None:
            return f"（匹配 {active_count}/{total_count}支）"
        return f"（{active_count}/{total_count}支）"
    matched = _safe_float(matched_member_count)
    if str(branch_source or "") == "matched_subset" and matched is not None and matched >= 1.0:
        matched_count = max(1, min(total_count, int(round(matched))))
        return f"（匹配 {matched_count}/{total_count}支）"
    prob = _safe_float(branch_dominant_prob)
    if prob is None or prob <= 0.0:
        return ""
    dominant_count = max(1, min(total_count, int(round(prob * total_count))))
    return f"（{dominant_count}/{total_count}支）"


def _watch_text(items: list[str]) -> str:
    cleaned = [str(item).strip() for item in items if str(item).strip()]
    if not cleaned:
        return ""
    if len(cleaned) == 1:
        return cleaned[0]
    if len(cleaned) == 2:
        return f"{cleaned[0]}和{cleaned[1]}"
    return f"{'、'.join(cleaned[:-1])}和{cleaned[-1]}"


def _format_abs_delta(value: float | None, unit: str = "C", *, digits: int = 1) -> str:
    if value is None:
        return ""
    return f"{abs(float(value)):.{digits}f}°{unit}"


def _branch_resolution_text(
    *,
    branch_family: str,
    expected_label: str,
    fallback_label: str,
    member_context: dict[str, Any],
) -> str:
    context = dict(member_context or {})
    next_t2m_delta_c = _safe_float(context.get("next3h_t2m_delta_c_p50"))
    next_td2m_delta_c = _safe_float(context.get("next3h_td2m_delta_c_p50"))
    next_wind10_delta_kmh = _safe_float(context.get("next3h_wind10_delta_kmh_p50"))
    next_msl_delta_hpa = _safe_float(context.get("next3h_msl_delta_hpa_p50"))
    warm_room = next_t2m_delta_c is not None and float(next_t2m_delta_c) >= 0.25
    strong_warm_room = next_t2m_delta_c is not None and float(next_t2m_delta_c) >= 0.45
    weak_room = next_t2m_delta_c is not None and abs(float(next_t2m_delta_c)) <= 0.12
    cooling_signal = next_t2m_delta_c is not None and float(next_t2m_delta_c) <= -0.15
    wind_rising = next_wind10_delta_kmh is not None and float(next_wind10_delta_kmh) >= 3.0
    wind_fading = next_wind10_delta_kmh is not None and float(next_wind10_delta_kmh) <= -3.0
    pressure_falling = next_msl_delta_hpa is not None and float(next_msl_delta_hpa) <= -0.8
    pressure_rising = next_msl_delta_hpa is not None and float(next_msl_delta_hpa) >= 0.8
    dewpoint_rising = next_td2m_delta_c is not None and float(next_td2m_delta_c) >= 0.4
    dewpoint_falling = next_td2m_delta_c is not None and float(next_td2m_delta_c) <= -0.4

    if branch_family == "convective_interrupt_risk":
        return f"这支真正的分界点不是慢变量，而是雷达回波前沿会不会先压进站点；一旦压进来，更像转到{expected_label}，压不进来才更容易回到{fallback_label}"

    if branch_family == "convective_cold_hold":
        return f"这支要继续成立，关键不是泛看变量，而是雷达前沿和附近 PWS 是否继续先降温增风；只要这一步还在，温度就更像继续被压在{expected_label}"

    if branch_family in {"warm_support_track", "warm_landing_watch", "warm_transition_probe"}:
        conditions: list[str] = []
        if strong_warm_room:
            conditions.append(f"下一两报至少还要再抬约 {_format_abs_delta(next_t2m_delta_c)}")
        elif warm_room:
            conditions.append(f"下一两报还要继续抬约 {_format_abs_delta(next_t2m_delta_c)}")
        if wind_rising:
            conditions.append(f"10米风再起约 {abs(float(next_wind10_delta_kmh)):.0f}km/h")
        if pressure_falling:
            conditions.append(f"气压继续降约 {abs(float(next_msl_delta_hpa)):.1f}hPa")
        if dewpoint_rising:
            conditions.append(f"露点再抬约 {_format_abs_delta(next_td2m_delta_c)}")
        if conditions:
            joined = "、".join(conditions[:3])
            return f"这支要继续成立，{joined}；少掉这些配合，就更像转成{fallback_label}"
        if weak_room or cooling_signal or wind_fading:
            return f"眼下继续兑现的抓手已经不多，若下一两报抬温续不上，就更像先转成{fallback_label}"
        return f"若下一两报还抬不出新的升温段，这支就更像先转成{fallback_label}"

    if branch_family in {"cold_suppression_track", "cold_transition_probe"}:
        conditions = []
        if cooling_signal:
            conditions.append(f"下一两报继续回吐约 {_format_abs_delta(next_t2m_delta_c)}")
        elif weak_room:
            conditions.append("下一两报继续走平")
        if pressure_rising:
            conditions.append(f"气压再升约 {abs(float(next_msl_delta_hpa)):.1f}hPa")
        if wind_rising:
            conditions.append(f"10米风再起约 {abs(float(next_wind10_delta_kmh)):.0f}km/h")
        if dewpoint_falling:
            conditions.append(f"露点再降约 {_format_abs_delta(next_td2m_delta_c)}")
        if conditions:
            joined = "、".join(conditions[:3])
            return f"这支要继续压住温度，{joined}；一旦这些信号转弱，就更像松回{fallback_label}"
        return f"若下一两报压不出继续走平或回落，这支就更像松回{fallback_label}"

    if branch_family == "cloud_release_watch":
        if warm_room:
            return f"云层要真松开，下一两报至少还要再抬约 {_format_abs_delta(next_t2m_delta_c)}；若温度只剩平台摆动，就更像继续停在{fallback_label}"
        return f"云层松开后若下一两报还抬不起来，这支就更像继续停在{fallback_label}"

    if branch_family == "second_peak_retest":
        if warm_room:
            return f"再摸前高要成立，下一两报至少还要再抬约 {_format_abs_delta(next_t2m_delta_c)}；抬不动，就更像回到{fallback_label}"
        return f"若下一两报还提不起新的上冲，这支就更像回到{fallback_label}"

    if branch_family == "volatile_split":
        if warm_room and wind_rising:
            return f"暖侧若想胜出，下一两报得继续抬约 {_format_abs_delta(next_t2m_delta_c)}，同时10米风再起约 {abs(float(next_wind10_delta_kmh)):.0f}km/h；否则仍按双侧拉扯看"
        return "这支暂时没有单一决定因子，关键看下一两报先往升温还是平台哪边偏；分不出胜负前仍按双侧拉扯看"

    return ""


def _member_matches_branch_context(
    raw: dict[str, Any],
    *,
    branch_path: str,
    branch_path_detail: str,
    branch_side: str,
) -> bool:
    row_path = str(raw.get("path_label") or "")
    row_detail = str(raw.get("path_detail") or row_path or "")
    row_side = str(raw.get("path_side") or _path_side(row_path, path_detail=row_detail))
    if branch_path_detail and row_detail == branch_path_detail:
        return True
    if branch_path and row_path == branch_path:
        return True
    if branch_side and row_side == branch_side:
        return True
    return False


def _member_context_summary(
    *,
    branch_outlook_state: dict[str, Any],
    member_evolution_state: dict[str, Any],
) -> dict[str, Any]:
    state = dict(member_evolution_state or {})
    rows = [dict(raw) for raw in (state.get("members") or []) if isinstance(raw, dict)]
    if not rows:
        return {}

    branch = dict(branch_outlook_state or {})
    branch_path = str(branch.get("branch_path") or "")
    branch_path_detail = str(branch.get("branch_path_detail") or branch_path or "")
    branch_side = str(branch.get("branch_side") or _path_side(branch_path, path_detail=branch_path_detail))
    selected_rows = [
        raw
        for raw in rows
        if _member_matches_branch_context(
            raw,
            branch_path=branch_path,
            branch_path_detail=branch_path_detail,
            branch_side=branch_side,
        )
    ]
    if len(selected_rows) < 2:
        selected_rows = list(rows)

    def _pairs(key: str) -> list[tuple[float, float | None]]:
        pairs: list[tuple[float, float | None]] = []
        for raw in selected_rows:
            weight = _safe_float(raw.get("compatibility_weight"))
            value = _safe_float(raw.get(key))
            if weight is None or weight <= 0.0 or value is None:
                continue
            pairs.append((float(weight), float(value)))
        return pairs

    def _family_share() -> tuple[str, float | None]:
        family_scores: dict[str, float] = {}
        total = 0.0
        for raw in selected_rows:
            weight = _safe_float(raw.get("compatibility_weight"))
            family = str(raw.get("future_family") or "")
            if weight is None or weight <= 0.0 or not family:
                continue
            family_scores[family] = family_scores.get(family, 0.0) + float(weight)
            total += float(weight)
        if not family_scores or total <= 0.0:
            return "", None
        dominant_family, dominant_weight = max(family_scores.items(), key=lambda item: item[1])
        return dominant_family, round(dominant_weight / total, 3)

    dominant_future_family, dominant_future_family_share = _family_share()
    branch_effective_member_count = None
    try:
        weights = [
            float(raw.get("compatibility_weight"))
            for raw in selected_rows
            if _safe_float(raw.get("compatibility_weight")) is not None
        ]
        if weights:
            weight_sum = sum(weights)
            squared_sum = sum(weight * weight for weight in weights)
            if weight_sum > 0.0 and squared_sum > 0.0:
                branch_effective_member_count = round((weight_sum * weight_sum) / squared_sum, 2)
    except Exception:
        branch_effective_member_count = None

    return {
        "has_surface_member_detail": bool(_pairs("surface_temp_gap_c")),
        "surface_temp_gap_c_p50": _weighted_quantile_pairs(_pairs("surface_temp_gap_c"), 0.50),
        "surface_dewpoint_gap_c_p50": _weighted_quantile_pairs(_pairs("surface_dewpoint_gap_c"), 0.50),
        "surface_rh_gap_pct_p50": _weighted_quantile_pairs(_pairs("surface_rh_gap_pct"), 0.50),
        "surface_alignment_score": _weighted_mean_pairs(_pairs("surface_alignment_score")),
        "history_alignment_score": _weighted_mean_pairs(_pairs("history_alignment_score")),
        "history_temp_mae_c_p50": _weighted_quantile_pairs(_pairs("history_temp_mae_c"), 0.50),
        "history_trend_bias_c_mean": _weighted_mean_pairs(_pairs("history_trend_bias_c")),
        "history_match_count_p50": _weighted_quantile_pairs(_pairs("history_match_count"), 0.50),
        "surface_wind_gap_kmh_mean": _weighted_mean_pairs(_pairs("wind_gap_kmh")),
        "surface_wind_dir_gap_deg_p50": _weighted_quantile_pairs(_pairs("wind_dir_gap_deg"), 0.50),
        "surface_pressure_gap_hpa_mean": _weighted_mean_pairs(_pairs("pressure_gap_hpa")),
        "next3h_t2m_delta_c_p50": _weighted_quantile_pairs(_pairs("next3h_t2m_delta_c"), 0.50),
        "next3h_t2m_delta_c_p75": _weighted_quantile_pairs(_pairs("next3h_t2m_delta_c"), 0.75),
        "next3h_td2m_delta_c_p50": _weighted_quantile_pairs(_pairs("next3h_td2m_delta_c"), 0.50),
        "next3h_wind10_delta_kmh_p50": _weighted_quantile_pairs(_pairs("next3h_wind10_delta_kmh"), 0.50),
        "next3h_msl_delta_hpa_p50": _weighted_quantile_pairs(_pairs("next3h_msl_delta_hpa"), 0.50),
        "dominant_future_family": dominant_future_family,
        "dominant_future_family_share": dominant_future_family_share,
        "branch_effective_member_count": branch_effective_member_count,
    }


def _branch_watch_profile(
    *,
    branch_family: str,
    next_gate: str,
) -> tuple[list[str], list[str]]:
    observation_items: list[str]
    realtime_items: list[str] = []

    if branch_family in {"warm_landing_watch", "warm_transition_probe"} or next_gate == "low_level_coupling":
        observation_items = ["地面风向是否继续偏暖", "最新1-2报升温能否续上", "云量是否继续放开"]
    elif branch_family == "cloud_release_watch" or next_gate == "cloud_release":
        observation_items = ["低云底是否继续抬升", "短时日照能否铺开", "升温斜率能否立刻转强"]
    elif branch_family in {"convective_interrupt_risk", "convective_cold_hold"} or next_gate in {"convective_intrusion", "convective_persistence"}:
        observation_items = ["雷达回波前沿离站还有多远", "附近 PWS 是否已先降温增风", "站点是否已被雷雨冷风压温"]
        realtime_items = list(observation_items)
    elif branch_family in {"cold_suppression_track", "cold_transition_probe"} or next_gate == "cold_advection_hold":
        observation_items = ["地面风向是否继续偏冷", "最新1-2报是否继续走平或掉温", "云量或降水压温是否继续维持"]
    elif branch_family == "second_peak_retest" or next_gate == "rebreak_signal":
        observation_items = ["最新气温是否重新贴近前高", "风向/风速是否再转暖", "云量是否再次放开"]
    elif branch_family == "volatile_split" or next_gate == "branch_resolution":
        observation_items = ["最新两报温度斜率", "地面风向切换是否真正落地", "云量或降水边界是否扫到站点"]
    elif branch_family == "neutral_plateau":
        observation_items = ["最新两报温度斜率", "云量是否继续压着", "地面风向会不会重新转暖或转冷"]
    else:
        observation_items = ["最新两报温度斜率", "地面风向/风速是否继续偏向当前路径", "云量变化"]

    return observation_items, realtime_items


def _surface_signature_context(
    *,
    active_path_label: str,
    match_count_text: str,
    member_context: dict[str, Any],
) -> tuple[str, float]:
    context = dict(member_context or {})
    if not context:
        return "", 0.0

    has_surface_detail = bool(context.get("has_surface_member_detail"))
    surface_gap_c = _safe_float(context.get("surface_temp_gap_c_p50"))
    surface_dewpoint_gap_c = _safe_float(context.get("surface_dewpoint_gap_c_p50"))
    surface_rh_gap_pct = _safe_float(context.get("surface_rh_gap_pct_p50"))
    surface_alignment = _safe_float(context.get("surface_alignment_score"))
    history_alignment = _safe_float(context.get("history_alignment_score"))
    history_temp_mae_c = _safe_float(context.get("history_temp_mae_c_p50"))
    history_match_count = _safe_float(context.get("history_match_count_p50"))
    next_t2m_delta_c = _safe_float(context.get("next3h_t2m_delta_c_p50"))
    next_td2m_delta_c = _safe_float(context.get("next3h_td2m_delta_c_p50"))
    next_wind10_delta_kmh = _safe_float(context.get("next3h_wind10_delta_kmh_p50"))
    next_msl_delta_hpa = _safe_float(context.get("next3h_msl_delta_hpa_p50"))
    surface_wind_dir_gap_deg = _safe_float(context.get("surface_wind_dir_gap_deg_p50"))
    dominant_future_family = str(context.get("dominant_future_family") or "")
    dominant_future_family_share = _safe_float(context.get("dominant_future_family_share"))

    if not has_surface_detail and next_t2m_delta_c is None:
        return "", 0.0

    parts: list[str] = []
    if active_path_label:
        parts.append(f"当前更贴近{active_path_label}分支{match_count_text}")

    if history_match_count is not None and history_match_count >= 2:
        history_part = f"从初始场以来最近{int(round(history_match_count))}个对照时次看"
        if history_temp_mae_c is not None:
            history_part += f"，匹配成员的地面温度中位误差约 {abs(history_temp_mae_c):.1f}°C"
        if history_alignment is not None:
            if float(history_alignment) >= 0.78:
                history_part += "，风压节奏也基本能对上"
            elif float(history_alignment) >= 0.64:
                history_part += "，风压节奏大体还能跟上"
        parts.append(history_part)

    if has_surface_detail and surface_gap_c is not None:
        extras: list[str] = []
        if surface_dewpoint_gap_c is not None and abs(float(surface_dewpoint_gap_c)) >= 1.0:
            if float(surface_dewpoint_gap_c) > 0.0:
                extras.append(f"露点还偏低约 {abs(float(surface_dewpoint_gap_c)):.1f}°C")
            else:
                extras.append(f"露点还偏高约 {abs(float(surface_dewpoint_gap_c)):.1f}°C")
        if surface_rh_gap_pct is not None and abs(float(surface_rh_gap_pct)) >= 8.0:
            if float(surface_rh_gap_pct) > 0.0:
                extras.append(f"湿度偏低约 {abs(float(surface_rh_gap_pct)):.0f}%")
            else:
                extras.append(f"湿度偏高约 {abs(float(surface_rh_gap_pct)):.0f}%")
        if surface_wind_dir_gap_deg is not None and float(surface_wind_dir_gap_deg) >= 30.0:
            extras.append(f"10米风向差约 {float(surface_wind_dir_gap_deg):.0f}°")
        if abs(float(surface_gap_c)) <= 0.25:
            text = "匹配成员当前和实况贴得很近"
        elif float(surface_gap_c) > 0.0:
            text = "匹配成员当前整体比实况偏冷"
        else:
            text = "匹配成员当前整体比实况偏暖"
        if extras:
            text += "，" + "，".join(extras[:2])
        parts.append(text)

    if next_t2m_delta_c is not None:
        family_label = _branch_family_label(dominant_future_family) if dominant_future_family else ""
        if float(next_t2m_delta_c) >= 0.45:
            clause = "未来1-3小时多数成员仍偏继续补涨"
        elif float(next_t2m_delta_c) >= 0.15:
            clause = "未来1-3小时多数成员仍偏小幅补涨"
        elif float(next_t2m_delta_c) <= -0.15:
            clause = "未来1-3小时多数成员更像先回落"
        else:
            clause = "未来1-3小时多数成员更像先走平台"
        if family_label and family_label not in {"风热条件平台", ""}:
            clause += f"，再看会不会转到{family_label}"
        if dominant_future_family_share is not None and dominant_future_family_share >= 0.58:
            clause += f"（约 {int(round(float(dominant_future_family_share) * 100.0))}%）"
        extras: list[str] = []
        if next_td2m_delta_c is not None and abs(float(next_td2m_delta_c)) >= 0.4:
            if next_td2m_delta_c > 0.0:
                extras.append(f"露点中位再抬约 {float(next_td2m_delta_c):.1f}°C")
            else:
                extras.append(f"露点中位再降约 {abs(float(next_td2m_delta_c)):.1f}°C")
        if next_wind10_delta_kmh is not None and abs(float(next_wind10_delta_kmh)) >= 3.0:
            extras.append("10米风也还在继续变化")
        if next_msl_delta_hpa is not None and abs(float(next_msl_delta_hpa)) >= 0.8:
            extras.append("气压节奏也还在配合")
        if extras:
            clause += f"，{'、'.join(extras)}"
        parts.append(clause)

    if not parts:
        return "", 0.0

    score = 0.76
    if surface_alignment is not None:
        score += max(0.0, float(surface_alignment) - 0.60) * 0.20
    if history_alignment is not None:
        score += max(0.0, float(history_alignment) - 0.58) * 0.16
    if next_t2m_delta_c is not None:
        score += 0.04
    score = _clamp(score, 0.0, 0.92)
    return "；".join(parts[:2]), round(score, 2)


def _build_path_context(
    *,
    branch_outlook_state: dict[str, Any],
    member_evolution_state: dict[str, Any] | None = None,
) -> dict[str, Any]:
    branch = dict(branch_outlook_state or {})
    active_path_source = str(branch.get("branch_source") or "")
    active_path = str(branch.get("branch_path") or "")
    active_path_detail = str(branch.get("branch_path_detail") or active_path or "")
    active_side = str(branch.get("branch_side") or _path_side(active_path, path_detail=active_path_detail))
    ensemble_member_count = _safe_float(branch.get("ensemble_member_count"))
    branch_member_count = _safe_float(branch.get("branch_member_count"))
    matched_member_count = _safe_float(branch.get("matched_member_count"))
    branch_family = str(branch.get("branch_family") or "")
    branch_stage_now = str(branch.get("branch_stage_now") or "")
    branch_volatility = str(branch.get("branch_volatility") or "")
    expected_next_family = str(branch.get("expected_next_family") or "")
    expected_next_stage = str(branch.get("expected_next_stage") or "")
    fallback_family = str(branch.get("fallback_family") or "")
    fallback_stage = str(branch.get("fallback_stage") or "")
    next_gate = str(branch.get("next_transition_gate") or "")
    branch_dominant_prob = _safe_float(branch.get("branch_dominant_prob")) or 0.0
    expected_prob = _safe_float(branch.get("expected_follow_through_prob")) or 0.50
    fallback_prob = _safe_float(branch.get("fallback_prob")) or 0.30
    history_supported = bool(branch.get("history_supported"))
    history_matched_time_count = _safe_float(branch.get("history_matched_time_count"))
    circulation_signature_text = str(branch.get("circulation_signature_text") or "").strip()
    circulation_signature_score = _safe_float(branch.get("circulation_signature_score")) or 0.0
    if bool(branch.get("matched_subset_active")):
        signal_weight = 1.0
    elif active_path_source == "history_surface_match" and branch_dominant_prob >= 0.70:
        signal_weight = 0.97
    elif active_path_source == "observed_path" and branch_dominant_prob >= 0.80:
        signal_weight = 0.95
    elif active_path_source == "observed_path" and branch_dominant_prob >= 0.65:
        signal_weight = 0.90
    elif branch_dominant_prob >= 0.70:
        signal_weight = 0.86
    else:
        signal_weight = 0.75

    upper_tail_adjust_c = 0.0
    cold_tail_allowance_c = 0.0
    significant_detail_key = branch_family or "matched_branch"
    branch_volatility_penalty = {"low": 0.0, "medium": 0.03, "high": 0.06}.get(branch_volatility, 0.0)

    if branch_family == "warm_support_track":
        upper_tail_adjust_c += 0.08 + max(0.0, expected_prob - 0.55) * 0.16
        cold_tail_allowance_c -= 0.05
    elif branch_family == "warm_landing_watch":
        upper_tail_adjust_c += max(-0.02, (expected_prob - 0.58) * 0.22 + (branch_dominant_prob - 0.78) * 0.18)
        upper_tail_adjust_c -= fallback_prob * 0.04
        if branch_dominant_prob >= 0.88 and expected_prob >= 0.64 and branch_volatility == "low":
            upper_tail_adjust_c += 0.07
    elif branch_family == "warm_transition_probe":
        upper_tail_adjust_c -= 0.02 + fallback_prob * 0.08
        if branch_dominant_prob >= 0.85 and expected_prob >= 0.66 and branch_volatility == "low":
            upper_tail_adjust_c += 0.04
    elif branch_family == "cloud_release_watch":
        upper_tail_adjust_c -= 0.06 + fallback_prob * 0.18
    elif branch_family == "convective_interrupt_risk":
        upper_tail_adjust_c -= 0.08 + expected_prob * 0.18
        cold_tail_allowance_c += 0.04 + expected_prob * 0.08
    elif branch_family in {"cold_suppression_track", "convective_cold_hold"}:
        cold_tail_allowance_c += 0.08 + expected_prob * 0.18
        upper_tail_adjust_c -= 0.03 + expected_prob * 0.06
    elif branch_family == "cold_transition_probe":
        cold_tail_allowance_c += 0.06 + expected_prob * 0.10
    elif branch_family == "neutral_plateau":
        upper_tail_adjust_c -= 0.02 + fallback_prob * 0.06
        cold_tail_allowance_c += 0.03 if active_side == "cold" else 0.0
    elif branch_family == "volatile_split":
        upper_tail_adjust_c += 0.03 - branch_volatility_penalty
        cold_tail_allowance_c += 0.05 + branch_volatility_penalty
    elif branch_family == "second_peak_retest":
        upper_tail_adjust_c += 0.05 + max(0.0, expected_prob - 0.50) * 0.10
        cold_tail_allowance_c += 0.02

    if branch_volatility == "medium":
        upper_tail_adjust_c -= 0.02
        cold_tail_allowance_c += 0.02
    elif branch_volatility == "high":
        upper_tail_adjust_c -= 0.04
        cold_tail_allowance_c += 0.04

    upper_tail_adjust_c = round(upper_tail_adjust_c * signal_weight, 2)
    cold_tail_allowance_c = round(cold_tail_allowance_c * signal_weight, 2)

    gate_label = _branch_gate_label(next_gate)
    expected_label = _branch_family_label(expected_next_family)
    fallback_label = _branch_family_label(fallback_family)
    active_path_label = _branch_path_label(active_path, path_detail=active_path_detail)
    stage_clause = _branch_stage_clause(branch_family)
    match_count_text = _branch_match_count_text(
        branch_source=active_path_source,
        ensemble_member_count=ensemble_member_count,
        branch_member_count=branch_member_count,
        matched_member_count=matched_member_count,
        branch_dominant_prob=branch_dominant_prob,
    )
    observation_watch_items, realtime_watch_items = _branch_watch_profile(
        branch_family=branch_family,
        next_gate=next_gate,
    )
    member_context = _member_context_summary(
        branch_outlook_state=branch,
        member_evolution_state=dict(member_evolution_state or {}),
    )
    resolution_text = _branch_resolution_text(
        branch_family=branch_family,
        expected_label=expected_label,
        fallback_label=fallback_label,
        member_context=member_context,
    )
    surface_signature_text, surface_signature_score = _surface_signature_context(
        active_path_label=active_path_label,
        match_count_text=match_count_text,
        member_context=member_context,
    )
    observation_watch_text = _watch_text(observation_watch_items)
    realtime_watch_text = _watch_text(realtime_watch_items)
    significant_detail_score = 0.80
    significant_detail_parts: list[str] = []

    if history_supported and history_matched_time_count is not None and history_matched_time_count >= 2:
        significant_detail_prefix = f"从初始场以来最近{int(round(history_matched_time_count))}个对照时次看"
    else:
        significant_detail_prefix = "当前实况"

    if branch_family == "warm_support_track":
        significant_detail_parts = [
            f"{significant_detail_prefix}更贴近系集里的{active_path_label}分支{match_count_text}",
            stage_clause,
            resolution_text or f"若继续兑现，更可能维持{expected_label}；若掉链子，更容易转成{fallback_label}",
        ]
        significant_detail_score = 0.86
    elif branch_family in {"warm_landing_watch", "warm_transition_probe"}:
        significant_detail_parts = [
            f"{significant_detail_prefix}更贴近系集里的{active_path_label}分支{match_count_text}",
            stage_clause,
            resolution_text or f"若接上，更可能转到{expected_label}；若接不上，更容易转成{fallback_label}",
        ]
        significant_detail_score = 0.90
    elif branch_family == "cloud_release_watch":
        significant_detail_parts = [
            f"{significant_detail_prefix}更贴近系集里的{active_path_label}分支{match_count_text}",
            stage_clause,
            resolution_text or f"若松开，更可能转到{expected_label}；若不松，更容易继续停在{fallback_label}",
        ]
        significant_detail_score = 0.90
    elif branch_family == "convective_interrupt_risk":
        significant_detail_parts = [
            f"{significant_detail_prefix}更贴近系集里的{active_path_label}分支{match_count_text}",
            stage_clause,
            resolution_text or f"短时强天气优先盯{realtime_watch_text}；若压进来，更可能转到{expected_label}；若没压进来，分支才更容易回到{fallback_label}",
        ]
        significant_detail_score = 0.92
    elif branch_family in {"cold_suppression_track", "convective_cold_hold", "cold_transition_probe"}:
        significant_detail_parts = [
            f"{significant_detail_prefix}更贴近系集里的{active_path_label}分支{match_count_text}",
            stage_clause,
            resolution_text or f"若继续落地，更可能维持{expected_label}；若松动，更容易转回{fallback_label}",
        ]
        significant_detail_score = 0.89
    elif branch_family == "second_peak_retest":
        significant_detail_parts = [
            f"{significant_detail_prefix}更贴近系集里的{active_path_label}分支{match_count_text}",
            stage_clause,
            resolution_text or f"若再破，更可能转到{expected_label}；若不破，更容易回到{fallback_label}",
        ]
        significant_detail_score = 0.88
    elif branch_family == "volatile_split":
        significant_detail_parts = [
            f"{significant_detail_prefix}更贴近系集里的{active_path_label}分支{match_count_text}",
            stage_clause,
            resolution_text or f"若暖侧胜出，更可能转到{expected_label}；若分歧继续，区间仍要保留双侧机动",
        ]
        significant_detail_score = 0.86
    else:
        significant_detail_parts = [
            f"{significant_detail_prefix}更贴近系集里的{active_path_label}分支{match_count_text}",
            stage_clause,
            resolution_text or f"若继续演化，更可能转到{expected_label}；若不顺，更容易回到{fallback_label}",
        ]
        significant_detail_score = 0.82

    significant_detail_text = "；".join(
        str(part).strip().rstrip("。；，")
        for part in significant_detail_parts
        if str(part).strip()
    )

    if branch_stage_now in {"watch", "pending", "testing"}:
        significant_detail_score += 0.02
    if branch_volatility == "high":
        significant_detail_score += 0.01
    significant_detail_score = round(_clamp(significant_detail_score * signal_weight, 0.55, 0.96), 2)

    return {
        "active_path_source": active_path_source,
        "active_path": active_path,
        "active_path_detail": active_path_detail,
        "active_path_label": active_path_label,
        "active_path_side": active_side,
        "ensemble_member_count": int(round(ensemble_member_count)) if ensemble_member_count is not None else None,
        "branch_member_count": int(round(branch_member_count)) if branch_member_count is not None else None,
        "matched_member_count": int(round(matched_member_count)) if matched_member_count is not None else None,
        "match_count_text": match_count_text,
        "branch_family": branch_family,
        "branch_stage_now": branch_stage_now,
        "branch_volatility": branch_volatility,
        "next_transition_gate": next_gate,
        "next_transition_gate_label": gate_label,
        "branch_resolution_text": resolution_text,
        "expected_next_family": expected_next_family,
        "expected_next_stage": expected_next_stage,
        "expected_next_label": expected_label,
        "expected_follow_through_prob": round(expected_prob, 3),
        "fallback_family": fallback_family,
        "fallback_stage": fallback_stage,
        "fallback_label": fallback_label,
        "fallback_prob": round(fallback_prob, 3),
        "observation_watch_items": observation_watch_items,
        "observation_watch_text": observation_watch_text,
        "realtime_watch_items": realtime_watch_items,
        "realtime_watch_text": realtime_watch_text,
        "high_impact_realtime_focus": bool(realtime_watch_items),
        "signal_weight": round(signal_weight, 2),
        "upper_tail_allowance_adjust_c": round(upper_tail_adjust_c, 2),
        "cold_tail_allowance_c": round(cold_tail_allowance_c, 2),
        "surface_signature_text": surface_signature_text,
        "surface_signature_score": round(surface_signature_score, 2) if surface_signature_text else 0.0,
        "circulation_signature_text": circulation_signature_text or surface_signature_text,
        "circulation_signature_score": (
            round(circulation_signature_score, 2)
            if circulation_signature_text
            else (round(surface_signature_score, 2) if surface_signature_text else 0.0)
        ),
        "significant_detail_key": significant_detail_key,
        "significant_forecast_detail_parts": significant_detail_parts,
        "significant_forecast_detail_text": significant_detail_text,
        "significant_forecast_detail_score": round(significant_detail_score, 2) if significant_detail_text else 0.0,
        **member_context,
    }


def _branch_conviction(
    *,
    branch_outlook_state: dict[str, Any],
) -> float:
    branch = dict(branch_outlook_state or {})
    dominant_prob = _safe_float(branch.get("branch_dominant_prob")) or 0.0
    expected_prob = _safe_float(branch.get("expected_follow_through_prob")) or 0.50
    branch_source = str(branch.get("branch_source") or "")
    volatility = str(branch.get("branch_volatility") or "")
    matched_subset_active = bool(branch.get("matched_subset_active"))

    conviction = 0.42 * dominant_prob + 0.38 * expected_prob
    if matched_subset_active:
        conviction += 0.10
    elif branch_source == "observed_path":
        conviction += 0.05
    elif branch_source == "ensemble_dominant" and dominant_prob >= 0.90:
        conviction += 0.04

    conviction -= {
        "low": 0.0,
        "medium": 0.06,
        "high": 0.12,
    }.get(volatility, 0.0)
    return _clamp(conviction, 0.0, 0.98)


def _branch_conditioned_anchor(
    *,
    phase: str,
    branch_outlook_state: dict[str, Any],
    observed_anchor_c: float | None,
    modeled_peak_c: float | None,
    modeled_headroom_c: float | None,
    hours_to_peak: float | None,
    temp_trend_c: float | None,
    temp_bias_c: float | None,
    cloud_cover: float | None,
    radiation_eff: float | None,
    precip_state: str,
) -> tuple[float | None, float, list[str]]:
    if phase not in {"near_window", "in_window", "post"}:
        return None, 0.0, []

    branch = dict(branch_outlook_state or {})
    family = str(branch.get("branch_family") or "")
    dominant_prob = _safe_float(branch.get("branch_dominant_prob")) or 0.0
    volatility = str(branch.get("branch_volatility") or "")
    if dominant_prob < 0.72 or observed_anchor_c is None:
        return None, 0.0, []

    conviction = _branch_conviction(branch_outlook_state=branch)
    if conviction < 0.58:
        return None, 0.0, []

    precip_active = str(precip_state or "").strip().lower() not in {"", "none"}
    runway = 0.55
    if hours_to_peak is not None:
        runway = _clamp(float(hours_to_peak) / 3.0, 0.18, 1.0)

    cloud_drag = 0.0
    if cloud_cover is not None:
        if cloud_cover >= 0.80:
            cloud_drag += 0.16
        elif cloud_cover >= 0.65:
            cloud_drag += 0.08
    if radiation_eff is not None and radiation_eff <= 0.55:
        cloud_drag += 0.06
    if precip_active:
        cloud_drag += 0.12
    trend_support = max(0.0, _clamp(temp_trend_c, -0.8, 0.8)) * 0.34
    trend_drag = max(0.0, _clamp(-(temp_trend_c or 0.0), 0.0, 0.8)) * 0.20
    bias_support = max(0.0, _clamp(temp_bias_c, -2.2, 2.2)) * 0.08
    bias_drag = max(0.0, _clamp(-(temp_bias_c or 0.0), 0.0, 2.2)) * 0.06

    blend_weight = {
        "near_window": 0.28,
        "in_window": 0.34,
        "post": 0.20,
    }.get(phase, 0.24)
    blend_weight += max(0.0, conviction - 0.60) * 0.46
    blend_weight += trend_support * 0.16
    if family in {"warm_landing_watch", "warm_transition_probe", "cold_transition_probe"}:
        blend_weight *= 0.90
    if volatility == "medium":
        blend_weight *= 0.86
    elif volatility == "high":
        blend_weight *= 0.72
    blend_weight = _clamp(blend_weight, 0.0, 0.62)

    if family in {"warm_support_track", "warm_landing_watch", "warm_transition_probe"}:
        warm_room = max(0.0, float(modeled_headroom_c or 0.0))
        continuation_room = max(
            warm_room,
            0.16 + 0.42 * conviction + 0.24 * runway + trend_support + bias_support - cloud_drag,
        )
        if family == "warm_landing_watch":
            continuation_room -= 0.02
        elif family == "warm_transition_probe":
            continuation_room -= 0.04
        overshoot_room = 0.0
        if modeled_peak_c is not None:
            gap_to_modeled = float(modeled_peak_c) - float(observed_anchor_c)
            if gap_to_modeled <= 0.25:
                overshoot_room = max(0.0, conviction - 0.60) * (0.38 + 0.24 * runway + 0.16 * trend_support)
            elif family == "warm_support_track":
                overshoot_room = max(0.0, conviction - 0.68) * (0.24 + 0.08 * trend_support)
        if family == "warm_landing_watch":
            overshoot_room *= 0.88
        elif family == "warm_transition_probe":
            overshoot_room *= 0.85
        target_c = float(observed_anchor_c) + max(0.10, continuation_room) + overshoot_room
        if modeled_peak_c is not None:
            warm_cap = float(modeled_peak_c) + {
                "near_window": 0.75,
                "in_window": 0.65,
                "post": 0.35,
            }.get(phase, 0.60)
            if bool(branch.get("matched_subset_active")):
                warm_cap += 0.08
            if family == "warm_landing_watch" and conviction >= 0.66 and trend_support >= 0.05:
                warm_cap += 0.10
            target_c = min(target_c, warm_cap)
        return round(target_c, 2), round(blend_weight, 2), ["branch_warm_conditioned_anchor"]

    if family in {"cold_suppression_track", "cold_transition_probe", "convective_cold_hold"}:
        cold_room = max(0.10, 0.14 + 0.34 * conviction + 0.14 * runway + trend_drag + bias_drag)
        if family == "cold_transition_probe":
            cold_room -= 0.04
        if cloud_cover is not None and cloud_cover <= 0.35 and not precip_active:
            cold_room -= 0.06
        if modeled_headroom_c is not None and modeled_headroom_c <= 0.30:
            cold_room += 0.03
        target_c = float(observed_anchor_c) - max(0.10, cold_room)
        return round(target_c, 2), round(blend_weight, 2), ["branch_cold_conditioned_anchor"]

    return None, 0.0, []


def _branch_conditioned_spread_adjustment(
    *,
    phase: str,
    branch_outlook_state: dict[str, Any],
) -> tuple[float, list[str]]:
    if phase not in {"near_window", "in_window", "post"}:
        return 0.0, []

    branch = dict(branch_outlook_state or {})
    family = str(branch.get("branch_family") or "")
    volatility = str(branch.get("branch_volatility") or "")
    conviction = _branch_conviction(branch_outlook_state=branch)
    if conviction < 0.60:
        return 0.0, []

    adjust = 0.0
    reason_codes: list[str] = []
    if family in {"warm_support_track", "cold_suppression_track"} and volatility == "low":
        adjust -= 0.08 + max(0.0, conviction - 0.70) * 0.14
        reason_codes.append("branch_conviction_spread_tighten")
    elif family == "volatile_split" or volatility == "high":
        adjust += 0.08 + max(0.0, conviction - 0.70) * 0.10
        reason_codes.append("branch_volatility_spread_widen")
    elif family in {"warm_landing_watch", "warm_transition_probe", "cold_transition_probe"} and volatility == "low":
        tighten_scale = 0.12 if family == "warm_landing_watch" else 0.08
        adjust -= max(0.0, conviction - 0.68) * tighten_scale
        if adjust < 0.0:
            reason_codes.append("branch_transition_spread_tighten")
    return round(adjust, 2), reason_codes


def _near_term_model_weight(hours_to_peak: float | None) -> float:
    if hours_to_peak is None:
        return 1.0
    hours = float(hours_to_peak)
    if hours <= 1.0:
        return 0.18
    if hours <= 2.0:
        return 0.30
    if hours <= 4.0:
        return 0.45
    if hours <= 6.0:
        return 0.60
    return 1.0


def _ensemble_uncertainty_active(hours_to_peak: float | None) -> bool:
    return hours_to_peak is not None and float(hours_to_peak) >= 3.0


def _ensemble_center_adjust_active(hours_to_peak: float | None) -> bool:
    return hours_to_peak is not None and float(hours_to_peak) >= 6.0


def _ensemble_live_spread_adjustment(
    *,
    hours_to_peak: float | None,
    ensemble_split_state: str,
    ensemble_dominant_prob: float | None,
    alignment_match_state: str,
    alignment_confidence: str,
    alignment_score: float | None,
    observed_path_locked: bool,
) -> float:
    if hours_to_peak is None or float(hours_to_peak) < 0.0 or float(hours_to_peak) > 6.0:
        return 0.0
    if alignment_confidence == "none" or alignment_match_state not in {"exact", "path"}:
        return 0.0

    reduction = 0.0
    if alignment_confidence == "high":
        reduction = 0.22 if observed_path_locked else 0.18
    elif alignment_confidence == "partial":
        reduction = 0.10

    if alignment_match_state == "path":
        reduction *= 0.75

    if ensemble_split_state == "mixed":
        reduction *= 0.85
    elif ensemble_split_state == "split":
        reduction *= 0.65

    if ensemble_dominant_prob is not None:
        if ensemble_dominant_prob >= 0.72:
            reduction += 0.02
        elif ensemble_dominant_prob < 0.50:
            reduction *= 0.75

    if alignment_score is not None:
        if alignment_score >= 0.82:
            reduction += 0.02
        elif alignment_score < 0.62:
            reduction *= 0.80

    if reduction <= 0.0:
        return 0.0
    return -round(reduction, 2)


def _progress_spread_adjustment(
    *,
    phase: str,
    daily_peak_state: str,
    short_term_state: str,
    second_peak_potential: str,
    multi_peak_state: str,
    plateau_hold_state: str,
    analysis_window_mode: str,
    hours_to_peak: float | None,
    hours_to_window_end: float | None,
    modeled_headroom_c: float | None,
    observed_peak_age_h: float | None,
    reports_since_observed_peak: float | None,
    latest_gap_below_observed_c: float | None,
    temp_trend_c: float,
    temp_bias_c: float,
) -> tuple[float, list[str]]:
    if phase not in {"near_window", "in_window", "post"}:
        return 0.0, []

    reduction = 0.0
    reason_codes: list[str] = []
    if analysis_window_mode == "obs_plateau_reanchor":
        reduction += 0.28
        reason_codes.append("progress_plateau_reanchor")
    elif analysis_window_mode == "obs_peak_reanchor":
        reduction += 0.16
        reason_codes.append("progress_obs_peak_reanchor")

    if hours_to_window_end is not None:
        window_tail_h = float(hours_to_window_end)
        if window_tail_h <= 0.40:
            reduction += 0.18
            reason_codes.append("progress_window_nearly_closed")
        elif window_tail_h <= 0.75:
            reduction += 0.12
            reason_codes.append("progress_window_closing")
        elif phase == "in_window" and window_tail_h <= 1.50:
            reduction += 0.06
            reason_codes.append("progress_window_late")

    if hours_to_peak is not None:
        peak_offset_h = float(hours_to_peak)
        if peak_offset_h <= 0.0:
            reduction += 0.10
            reason_codes.append("progress_past_modeled_peak")
        elif peak_offset_h <= 0.35:
            reduction += 0.06
            reason_codes.append("progress_at_modeled_peak")

    if modeled_headroom_c is not None:
        headroom = max(0.0, float(modeled_headroom_c))
        if headroom <= 0.15:
            reduction += 0.22
            reason_codes.append("progress_headroom_exhausted")
        elif headroom <= 0.40:
            reduction += 0.18
            reason_codes.append("progress_headroom_low")
        elif headroom <= 0.75:
            reduction += 0.10
            reason_codes.append("progress_headroom_limited")

    if plateau_hold_state in {"holding", "sustained"}:
        reduction += 0.08
        reason_codes.append("progress_near_peak_plateau")

    if observed_peak_age_h is not None:
        age = max(0.0, float(observed_peak_age_h))
        if age >= 1.0:
            reduction += 0.16
            reason_codes.append("progress_obs_peak_aging")
        elif age >= 0.5:
            reduction += 0.08
            reason_codes.append("progress_obs_peak_recent")

    if reports_since_observed_peak is not None:
        reports = max(0.0, float(reports_since_observed_peak))
        if reports >= 3.0:
            reduction += 0.10
            reason_codes.append("progress_three_reports_since_high")
        elif reports >= 2.0:
            reduction += 0.06
            reason_codes.append("progress_two_reports_since_high")

    if latest_gap_below_observed_c is not None:
        latest_gap = max(0.0, float(latest_gap_below_observed_c))
        if latest_gap >= 0.35:
            reduction += 0.14
            reason_codes.append("progress_latest_below_high")
        elif latest_gap >= 0.15:
            reduction += 0.08
            reason_codes.append("progress_latest_off_high")

    if temp_trend_c <= -0.12:
        reduction += 0.12
        reason_codes.append("progress_negative_trend")
    elif temp_trend_c <= 0.05:
        reduction += 0.06
        reason_codes.append("progress_flat_trend")

    if temp_bias_c <= -0.10:
        reduction += 0.04

    reduction *= {
        "near_window": 0.70,
        "in_window": 1.00,
        "post": 1.12,
    }.get(phase, 1.0)

    if second_peak_potential in {"moderate", "high"} or multi_peak_state == "likely":
        reduction *= 0.35
    elif second_peak_potential == "weak" or multi_peak_state == "possible":
        reduction *= 0.60
    elif short_term_state == "reaccelerating" and (
        modeled_headroom_c is None or float(modeled_headroom_c) > 0.60
    ):
        reduction *= 0.70

    if daily_peak_state == "locked":
        reduction *= 0.55

    if reduction <= 0.0:
        return 0.0, []
    return -round(min(0.55, reduction), 2), reason_codes


def _spread_from_state(
    *,
    daily_peak_state: str,
    second_peak_potential: str,
    multi_peak_state: str,
    coverage_density: str,
    synoptic_provider_fallback: bool,
    missing_layers: list[str],
    plateau_hold_state: str,
    hours_to_window_end: float | None,
    main_track_confidence: str,
    hours_to_peak: float | None,
    ensemble_split_state: str,
    ensemble_dominant_prob: float | None,
) -> float:
    spread = 0.95
    if daily_peak_state == "open":
        spread += 0.32
    elif daily_peak_state == "lean_locked":
        spread += 0.10
    elif daily_peak_state == "locked":
        spread -= 0.28

    spread += {
        "none": 0.0,
        "weak": 0.12,
        "moderate": 0.32,
        "high": 0.52,
    }.get(second_peak_potential, 0.0)
    spread += {
        "none": 0.0,
        "possible": 0.18,
        "likely": 0.34,
    }.get(multi_peak_state, 0.0)
    spread += {
        "rich": -0.08,
        "moderate": 0.05,
        "sparse": 0.22,
    }.get(coverage_density, 0.10)

    if synoptic_provider_fallback:
        spread += 0.15
    spread += min(len(missing_layers), 4) * 0.04

    if plateau_hold_state in {"holding", "sustained"}:
        spread -= 0.10
    if hours_to_window_end is not None and hours_to_window_end <= 1.0:
        spread -= 0.10
    if main_track_confidence == "high":
        spread -= 0.05
    elif main_track_confidence == "low":
        spread += 0.08

    if _ensemble_uncertainty_active(hours_to_peak):
        spread += {
            "clustered": -0.04,
            "mixed": 0.16,
            "split": 0.32,
        }.get(ensemble_split_state, 0.0)
        if ensemble_dominant_prob is not None:
            if ensemble_dominant_prob < 0.45:
                spread += 0.08
            elif ensemble_dominant_prob >= 0.72:
                spread -= 0.04

    if daily_peak_state == "locked":
        spread = min(spread, 0.18)
    elif daily_peak_state == "lean_locked":
        spread = min(spread, 0.34)
    return _clamp(spread, 0.18, 2.40)


def _shift_local_time_text(value: str, shift_h: float) -> str:
    text = str(value or "").strip()
    if not text or abs(float(shift_h or 0.0)) < 0.01:
        return text
    try:
        dt = datetime.fromisoformat(text)
    except Exception:
        return text
    shifted = dt + timedelta(hours=float(shift_h))
    if shifted.second == 0 and shifted.microsecond == 0:
        return shifted.isoformat(timespec="minutes")
    return shifted.isoformat()


def build_weather_posterior_core(
    *,
    canonical_raw_state: dict[str, Any],
    posterior_feature_vector: dict[str, Any],
) -> dict[str, Any]:
    raw = canonical_raw_state if isinstance(canonical_raw_state, dict) else {}
    feat = posterior_feature_vector if isinstance(posterior_feature_vector, dict) else {}

    obs = dict(raw.get("observations") or {})
    forecast = dict(raw.get("forecast") or {})
    forecast_meta = dict(forecast.get("meta") or {})
    station_icao = str(forecast_meta.get("station") or "").upper()
    window = dict(raw.get("window") or {})
    primary_window = dict(window.get("primary") or {})
    calc_window = dict(window.get("calc") or {})

    time_phase = dict(feat.get("time_phase") or {})
    obs_state = dict(feat.get("observation_state") or {})
    cloud_state = dict(feat.get("cloud_radiation_state") or {})
    moisture_state = dict(feat.get("moisture_stability_state") or {})
    mixing_state = dict(feat.get("mixing_coupling_state") or {})
    transport_state = dict(feat.get("transport_state") or {})
    vertical_state = dict(feat.get("vertical_structure_state") or {})
    shape_state = dict(feat.get("forecast_shape_state") or {})
    peak_state = dict(feat.get("peak_phase_state") or {})
    track_state = dict(feat.get("track_state") or {})
    quality_state = dict(feat.get("quality_state") or {})
    ensemble_state = dict(feat.get("ensemble_path_state") or {})
    branch_outlook_state = dict(feat.get("matched_branch_outlook_state") or {})
    member_evolution_state = dict(feat.get("member_evolution_state") or {})

    latest_temp_c = _safe_float(obs_state.get("latest_temp_c"))
    observed_max_temp_c = _safe_float(obs_state.get("observed_max_temp_c"))
    observed_floor_c = _safe_float(obs.get("observed_max_interval_lo_c"))
    modeled_peak_c = _safe_float(calc_window.get("peak_temp_c"))
    if modeled_peak_c is None:
        modeled_peak_c = _safe_float(primary_window.get("peak_temp_c"))

    phase = str(time_phase.get("phase") or "")
    hours_to_peak = _safe_float(time_phase.get("hours_to_peak"))
    hours_to_window_end = _safe_float(time_phase.get("hours_to_window_end"))
    analysis_window_mode = str(
        time_phase.get("analysis_window_mode")
        or ((raw.get("source") or {}).get("analysis_window_mode"))
        or ""
    )
    daily_peak_state = str(peak_state.get("daily_peak_state") or "open")
    short_term_state = str(peak_state.get("short_term_state") or "holding")
    second_peak_potential = str(peak_state.get("second_peak_potential") or "none")
    plateau_hold_state = str(peak_state.get("plateau_hold_state") or "none")
    latest_gap_below_observed_c = _safe_float(obs_state.get("latest_gap_below_observed_c"))
    observed_progress_anchor_c = _safe_float(obs_state.get("observed_progress_anchor_c"))
    modeled_headroom_c = _safe_float(obs_state.get("modeled_headroom_c"))
    observed_peak_age_h = _safe_float(obs_state.get("time_since_observed_peak_h"))
    reports_since_observed_peak = _safe_float(obs_state.get("reports_since_observed_peak"))

    temp_trend_c = _safe_float(obs_state.get("temp_trend_effective_c"))
    if temp_trend_c is None:
        temp_trend_c = _safe_float(obs_state.get("temp_trend_c"))
    temp_trend_c = temp_trend_c or 0.0
    temp_bias_c = _safe_float(obs_state.get("temp_bias_c")) or 0.0
    cloud_cover = _safe_float(cloud_state.get("cloud_effective_cover"))
    radiation_eff = _safe_float(cloud_state.get("radiation_eff"))
    precip_state = str(moisture_state.get("precip_state") or "none")
    transport = str(transport_state.get("transport_state") or "neutral")
    thermal_adv_state = str(transport_state.get("thermal_advection_state") or "none")
    surface_role = str(transport_state.get("surface_role") or "")
    surface_coupling = str(mixing_state.get("surface_coupling_state") or "")
    low_level_cap_score = _safe_float(vertical_state.get("low_level_cap_score")) or 0.0
    h925_coupling_state = str(vertical_state.get("h925_coupling_state") or "")
    h700_scope = str(vertical_state.get("h700_scope") or "")
    h700_dry_intrusion_strength = _safe_float(vertical_state.get("h700_dry_intrusion_strength"))
    multi_peak_state = str(shape_state.get("multi_peak_state") or "none")
    coverage_density = str(vertical_state.get("coverage_density") or "")
    main_track_confidence = str(track_state.get("main_track_confidence") or "")
    ensemble_full_dominant_path = str(ensemble_state.get("dominant_path") or "")
    ensemble_full_dominant_path_detail = str(ensemble_state.get("dominant_path_detail") or "")
    ensemble_full_split_state = str(ensemble_state.get("split_state") or "")
    ensemble_full_dominant_prob = _safe_float(ensemble_state.get("dominant_prob"))
    ensemble_alignment_match_state = str(ensemble_state.get("observed_alignment_match_state") or "")
    ensemble_alignment_confidence = str(ensemble_state.get("observed_alignment_confidence") or "")
    ensemble_alignment_score = _safe_float(ensemble_state.get("observed_alignment_score"))
    ensemble_observed_path = str(ensemble_state.get("observed_path") or "")
    ensemble_observed_path_detail = str(ensemble_state.get("observed_path_detail") or "")
    ensemble_observed_path_locked = bool(ensemble_state.get("observed_path_locked"))
    ensemble_matched_subset_active = bool(ensemble_state.get("matched_subset_active"))
    ensemble_matched_member_share = _safe_float(ensemble_state.get("matched_member_share"))
    ensemble_matched_dominant_path = str(ensemble_state.get("matched_dominant_path") or "")
    ensemble_matched_dominant_path_detail = str(ensemble_state.get("matched_dominant_path_detail") or ensemble_state.get("matched_transition_detail") or "")
    ensemble_active_source = "matched_subset" if ensemble_matched_subset_active else "full_ensemble"
    ensemble_dominant_path = str(
        (
            ensemble_state.get("matched_dominant_path")
            if ensemble_matched_subset_active
            else ensemble_full_dominant_path
        ) or ""
    ) or ensemble_full_dominant_path
    ensemble_split_state = str(
        (
            ensemble_state.get("matched_split_state")
            if ensemble_matched_subset_active
            else ensemble_full_split_state
        ) or ""
    ) or ensemble_full_split_state
    ensemble_dominant_prob = _safe_float(
        ensemble_state.get("matched_dominant_prob")
        if ensemble_matched_subset_active
        else ensemble_full_dominant_prob
    )
    if ensemble_dominant_prob is None:
        ensemble_dominant_prob = ensemble_full_dominant_prob
    path_context = _build_path_context(
        branch_outlook_state=branch_outlook_state,
        member_evolution_state=member_evolution_state,
    )

    floor_c = observed_floor_c
    if floor_c is None:
        floor_c = observed_max_temp_c
    if floor_c is None:
        floor_c = latest_temp_c

    reason_codes: list[str] = []
    if daily_peak_state == "locked" and observed_max_temp_c is not None:
        median_c = observed_max_temp_c
        reason_codes.append("daily_peak_locked")
    elif daily_peak_state == "lean_locked" and observed_max_temp_c is not None and modeled_peak_c is not None:
        median_c = 0.72 * observed_max_temp_c + 0.28 * modeled_peak_c
        reason_codes.append("lean_locked_blend")
    elif modeled_peak_c is not None:
        median_c = modeled_peak_c
        if hours_to_peak is not None and hours_to_peak >= 6.0:
            reason_codes.append("far_modeled_peak_soft_anchor")
        else:
            reason_codes.append("modeled_peak_anchor")
        ref_obs = observed_max_temp_c if observed_max_temp_c is not None else latest_temp_c
        if ref_obs is not None and hours_to_peak is not None:
            model_weight = _near_term_model_weight(hours_to_peak)
            if model_weight < 1.0:
                obs_weight = 1.0 - model_weight
                median_c = model_weight * modeled_peak_c + obs_weight * ref_obs
                if hours_to_peak <= 2.0:
                    reason_codes.append("near_peak_obs_anchor")
                elif hours_to_peak <= 4.0:
                    reason_codes.append("mid_window_obs_anchor")
                else:
                    reason_codes.append("near_window_obs_anchor")
    elif observed_max_temp_c is not None:
        median_c = observed_max_temp_c
        reason_codes.append("observed_max_anchor")
    else:
        median_c = latest_temp_c or 0.0
        reason_codes.append("latest_temp_anchor")

    adjustment_c = 0.0
    adjustment_c += _clamp(temp_bias_c, -2.0, 2.0) * 0.22
    adjustment_c += _clamp(temp_trend_c, -0.6, 0.6) * 0.72
    adjustment_c += _advection_signed_adjustment(transport, thermal_adv_state)

    if short_term_state == "reaccelerating":
        adjustment_c += 0.22
        reason_codes.append("short_term_reaccelerating")
    elif short_term_state == "fading":
        adjustment_c -= 0.24
        reason_codes.append("short_term_fading")

    if radiation_eff is not None and radiation_eff >= 0.80 and (cloud_cover is not None and cloud_cover <= 0.30):
        adjustment_c += 0.18
        reason_codes.append("radiation_support")
    if cloud_cover is not None and cloud_cover >= 0.80:
        adjustment_c -= 0.22
        reason_codes.append("high_cloud_cover")
    if precip_state not in {"", "none"}:
        adjustment_c -= 0.20
        reason_codes.append("precipitation_drag")
    if surface_coupling == "strong" and transport in {"warm", "cold"}:
        adjustment_c += 0.08 if transport == "warm" else -0.08
        reason_codes.append("surface_coupling_adjustment")
    if low_level_cap_score >= 0.60:
        adjustment_c -= 0.18
        reason_codes.append("low_level_cap")
    if second_peak_potential == "moderate":
        adjustment_c += 0.10
        reason_codes.append("second_peak_moderate")
    elif second_peak_potential == "high":
        adjustment_c += 0.24
        reason_codes.append("second_peak_high")
    if _ensemble_center_adjust_active(hours_to_peak) and ensemble_dominant_prob is not None and ensemble_dominant_prob >= 0.58:
        if ensemble_dominant_path == "warm_support":
            adjustment_c += 0.14
            reason_codes.append("ensemble_warm_support")
        elif ensemble_dominant_path == "cold_suppression":
            adjustment_c -= 0.14
            reason_codes.append("ensemble_cold_suppression")
    elif _ensemble_uncertainty_active(hours_to_peak) and ensemble_split_state in {"mixed", "split"}:
        reason_codes.append("ensemble_path_split")

    median_c += adjustment_c
    branch_anchor_target_c, branch_anchor_blend_weight, branch_anchor_reason_codes = _branch_conditioned_anchor(
        phase=phase,
        branch_outlook_state=branch_outlook_state,
        observed_anchor_c=(
            observed_progress_anchor_c
            if observed_progress_anchor_c is not None
            else (observed_max_temp_c if observed_max_temp_c is not None else latest_temp_c)
        ),
        modeled_peak_c=modeled_peak_c,
        modeled_headroom_c=modeled_headroom_c,
        hours_to_peak=hours_to_peak,
        temp_trend_c=temp_trend_c,
        temp_bias_c=temp_bias_c,
        cloud_cover=cloud_cover,
        radiation_eff=radiation_eff,
        precip_state=precip_state,
    )
    if branch_anchor_target_c is not None and branch_anchor_blend_weight > 0.0:
        branch_family = str(branch_outlook_state.get("branch_family") or "")
        target_c = float(branch_anchor_target_c)
        if branch_family in {"warm_support_track", "warm_landing_watch", "warm_transition_probe"}:
            target_c = max(target_c, median_c)
        elif branch_family in {"cold_suppression_track", "cold_transition_probe", "convective_cold_hold"}:
            target_c = min(target_c, median_c)
        median_c = (1.0 - branch_anchor_blend_weight) * median_c + branch_anchor_blend_weight * target_c
        reason_codes.extend(branch_anchor_reason_codes)
    member_distribution = _build_member_conditioned_distribution(
        phase=phase,
        member_evolution_state=member_evolution_state,
        floor_c=floor_c,
        observed_anchor_c=observed_progress_anchor_c,
        latest_temp_c=latest_temp_c,
        modeled_peak_c=modeled_peak_c,
        modeled_headroom_c=modeled_headroom_c,
        hours_to_peak=hours_to_peak,
        temp_trend_c=temp_trend_c,
        temp_bias_c=temp_bias_c,
        cloud_cover=cloud_cover,
        radiation_eff=radiation_eff,
        precip_state=precip_state,
    )
    member_distribution_quantiles = dict(member_distribution.get("quantiles") or {})
    member_distribution_p50 = _safe_float(member_distribution_quantiles.get("p50_c"))
    member_center_blend_weight = _safe_float(member_distribution.get("center_blend_weight")) or 0.0
    member_quantile_blend_weight = _safe_float(member_distribution.get("quantile_blend_weight")) or 0.0
    member_spread_proxy_c = _safe_float(member_distribution.get("spread_proxy_c"))
    if member_distribution_p50 is not None and member_center_blend_weight > 0.0:
        median_c = _blend_value(median_c, member_distribution_p50, member_center_blend_weight)
        reason_codes.append("member_conditioned_center_anchor")

    if floor_c is not None:
        median_c = max(median_c, floor_c)
    if daily_peak_state == "locked" and observed_max_temp_c is not None:
        median_c = min(median_c, observed_max_temp_c + 0.10)
    elif daily_peak_state == "lean_locked" and observed_max_temp_c is not None:
        median_c = min(median_c, observed_max_temp_c + 0.40)

    spread_c = _spread_from_state(
        daily_peak_state=daily_peak_state,
        second_peak_potential=second_peak_potential,
        multi_peak_state=multi_peak_state,
        coverage_density=coverage_density,
        synoptic_provider_fallback=bool(quality_state.get("synoptic_provider_fallback")),
        missing_layers=list(quality_state.get("missing_layers") or []),
        plateau_hold_state=plateau_hold_state,
        hours_to_window_end=hours_to_window_end,
        main_track_confidence=main_track_confidence,
        hours_to_peak=hours_to_peak,
        ensemble_split_state=ensemble_split_state,
        ensemble_dominant_prob=ensemble_dominant_prob,
    )
    live_alignment_spread_adjustment = _ensemble_live_spread_adjustment(
        hours_to_peak=hours_to_peak,
        ensemble_split_state=ensemble_split_state,
        ensemble_dominant_prob=ensemble_dominant_prob,
        alignment_match_state=ensemble_alignment_match_state,
        alignment_confidence=ensemble_alignment_confidence,
        alignment_score=ensemble_alignment_score,
        observed_path_locked=ensemble_observed_path_locked,
    )
    progress_spread_adjustment, progress_reason_codes = _progress_spread_adjustment(
        phase=phase,
        daily_peak_state=daily_peak_state,
        short_term_state=short_term_state,
        second_peak_potential=second_peak_potential,
        multi_peak_state=multi_peak_state,
        plateau_hold_state=plateau_hold_state,
        analysis_window_mode=analysis_window_mode,
        hours_to_peak=hours_to_peak,
        hours_to_window_end=hours_to_window_end,
        modeled_headroom_c=modeled_headroom_c,
        observed_peak_age_h=observed_peak_age_h,
        reports_since_observed_peak=reports_since_observed_peak,
        latest_gap_below_observed_c=latest_gap_below_observed_c,
        temp_trend_c=temp_trend_c,
        temp_bias_c=temp_bias_c,
    )
    branch_spread_adjustment, branch_spread_reason_codes = _branch_conditioned_spread_adjustment(
        phase=phase,
        branch_outlook_state=branch_outlook_state,
    )
    min_spread_floor = 0.18 if daily_peak_state == "locked" else (
        0.24 if phase in {"in_window", "post"} else 0.30
    )
    spread_c = _clamp(
        spread_c + live_alignment_spread_adjustment + progress_spread_adjustment + branch_spread_adjustment,
        min_spread_floor,
        2.40,
    )
    if member_spread_proxy_c is not None and member_center_blend_weight > 0.0:
        spread_blend_weight = _clamp(member_center_blend_weight + 0.08, 0.0, 0.78)
        spread_c = _blend_value(spread_c, member_spread_proxy_c, spread_blend_weight)
        reason_codes.append("member_conditioned_spread_anchor")
    if live_alignment_spread_adjustment < 0.0 and ensemble_alignment_confidence == "high" and ensemble_observed_path_locked:
        reason_codes.append("ensemble_path_alignment_locked")
    elif live_alignment_spread_adjustment < 0.0 and ensemble_alignment_confidence in {"high", "partial"} and ensemble_alignment_match_state in {"exact", "path"}:
        reason_codes.append("ensemble_path_live_confirmed")
    if progress_spread_adjustment < 0.0:
        reason_codes.extend(progress_reason_codes)
    if abs(branch_spread_adjustment) >= 0.01:
        reason_codes.extend(branch_spread_reason_codes)
    if ensemble_matched_subset_active:
        reason_codes.append("ensemble_obs_matched_subset")

    baseline_median_c = float(median_c)
    baseline_spread_c = float(spread_c)
    regime_detection = detect_station_regimes(raw)
    regime_adjustment = apply_regime_effects(
        station_icao=station_icao,
        baseline_posterior={
            "median_c": baseline_median_c,
            "spread_c": baseline_spread_c,
            "warm_tail_boost": 0.0,
            "lower_tail_lift_c": 0.0,
            "timing_shift_h": 0.0,
        },
        active_regimes=list(regime_detection.get("active_regimes") or []),
        raw_state=raw,
    )
    adjusted_distribution = dict(regime_adjustment.get("distribution") or {})
    median_c = float(adjusted_distribution.get("median_c") or median_c)
    spread_c = float(adjusted_distribution.get("spread_c") or spread_c)
    warm_tail_boost = float(adjusted_distribution.get("warm_tail_boost") or 0.0)
    lower_tail_lift_c = float(adjusted_distribution.get("lower_tail_lift_c") or 0.0)
    timing_shift_h = float(adjusted_distribution.get("timing_shift_h") or 0.0)
    if floor_c is not None:
        median_c = max(median_c, floor_c)
    if daily_peak_state == "locked" and observed_max_temp_c is not None:
        median_c = min(median_c, observed_max_temp_c + 0.10)
    elif daily_peak_state == "lean_locked" and observed_max_temp_c is not None:
        median_c = min(median_c, observed_max_temp_c + 0.40)
    reason_codes.extend(list(regime_adjustment.get("reason_codes") or []))

    legacy_quantile_values = _ensure_monotonic(
        [
            median_c - 1.15 * spread_c + lower_tail_lift_c,
            median_c - 0.55 * spread_c + 0.65 * lower_tail_lift_c,
            median_c,
            median_c + (0.72 + warm_tail_boost) * spread_c,
            median_c + (1.28 + warm_tail_boost) * spread_c,
        ],
        floor=floor_c,
    )
    quantile_values = list(legacy_quantile_values)
    if member_distribution_quantiles and member_quantile_blend_weight > 0.0:
        member_p10 = _safe_float(member_distribution_quantiles.get("p10_c"))
        member_p25 = _safe_float(member_distribution_quantiles.get("p25_c"))
        member_p50 = _safe_float(member_distribution_quantiles.get("p50_c"))
        member_p75 = _safe_float(member_distribution_quantiles.get("p75_c"))
        member_p90 = _safe_float(member_distribution_quantiles.get("p90_c"))
        if None not in {member_p10, member_p25, member_p50, member_p75, member_p90}:
            member_shift_c = median_c - float(member_p50)
            member_quantile_values = _ensure_monotonic(
                [
                    float(member_p10) + member_shift_c + lower_tail_lift_c,
                    float(member_p25) + member_shift_c + 0.65 * lower_tail_lift_c,
                    float(member_p50) + member_shift_c,
                    float(member_p75) + member_shift_c + warm_tail_boost * spread_c * 0.40,
                    float(member_p90) + member_shift_c + warm_tail_boost * spread_c * 0.65,
                ],
                floor=floor_c,
            )
            quantile_values = _ensure_monotonic(
                [
                    _blend_value(legacy_quantile_values[0], member_quantile_values[0], member_quantile_blend_weight),
                    _blend_value(legacy_quantile_values[1], member_quantile_values[1], member_quantile_blend_weight),
                    _blend_value(legacy_quantile_values[2], member_quantile_values[2], member_quantile_blend_weight),
                    _blend_value(legacy_quantile_values[3], member_quantile_values[3], member_quantile_blend_weight),
                    _blend_value(legacy_quantile_values[4], member_quantile_values[4], member_quantile_blend_weight),
                ],
                floor=floor_c,
            )
            reason_codes.append("member_conditioned_quantiles")
    p10, p25, p50, p75, p90 = [round(value, 2) for value in quantile_values]
    median_c = float(p50)

    new_high_score = -0.10
    new_high_score += {"open": 0.55, "lean_locked": 0.05, "locked": -1.00}.get(daily_peak_state, 0.0)
    new_high_score += {"reaccelerating": 0.75, "holding": 0.05, "fading": -0.75}.get(short_term_state, 0.0)
    new_high_score += {"none": 0.0, "weak": 0.12, "moderate": 0.45, "high": 0.78}.get(second_peak_potential, 0.0)
    new_high_score += _clamp(temp_trend_c, -0.5, 0.5) * 1.35
    if hours_to_peak is not None:
        if hours_to_peak > 2.5:
            new_high_score += 0.35
        elif hours_to_peak > 0.0:
            new_high_score += 0.12
        else:
            new_high_score -= 0.45
    if radiation_eff is not None and radiation_eff >= 0.80 and (cloud_cover is not None and cloud_cover <= 0.35):
        new_high_score += 0.22
    if cloud_cover is not None and cloud_cover >= 0.80:
        new_high_score -= 0.28
    if precip_state not in {"", "none"}:
        new_high_score -= 0.25
    new_high_score += _advection_signed_adjustment(transport, thermal_adv_state) * 1.4

    lock_score = -0.10
    lock_score += {"locked": 1.90, "lean_locked": 0.95, "open": -0.35}.get(daily_peak_state, 0.0)
    lock_score += {"fading": 0.55, "holding": 0.12, "reaccelerating": -0.60}.get(short_term_state, 0.0)
    lock_score += {"none": 0.0, "weak": -0.12, "moderate": -0.55, "high": -0.95}.get(second_peak_potential, 0.0)
    if hours_to_window_end is not None:
        if hours_to_window_end <= 1.0:
            lock_score += 0.85
        elif hours_to_window_end <= 2.0:
            lock_score += 0.38
    if plateau_hold_state in {"holding", "sustained"}:
        lock_score += 0.28
    lock_score -= _advection_signed_adjustment(transport, thermal_adv_state) * 0.6

    exceed_modeled_peak_prob = None
    if modeled_peak_c is not None:
        exceed_score = -0.45
        exceed_score += {"open": 0.18, "lean_locked": -0.05, "locked": -0.85}.get(daily_peak_state, 0.0)
        exceed_score += _clamp(temp_bias_c, -1.8, 1.8) * 0.38
        exceed_score += _clamp(temp_trend_c, -0.5, 0.5) * 0.90
        exceed_score += _advection_signed_adjustment(transport, thermal_adv_state) * 1.55
        gap_to_modeled_peak = _safe_float(obs_state.get("forecast_peak_minus_latest_c"))
        if gap_to_modeled_peak is not None:
            if gap_to_modeled_peak <= 0.6:
                exceed_score += 0.28
            elif gap_to_modeled_peak >= 2.0:
                exceed_score -= 0.22
        if radiation_eff is not None and radiation_eff >= 0.80 and (cloud_cover is not None and cloud_cover <= 0.30):
            exceed_score += 0.20
        if observed_max_temp_c is not None and observed_max_temp_c >= modeled_peak_c:
            exceed_score += 0.95
        exceed_modeled_peak_prob = round(_clamp(_logistic(exceed_score), 0.02, 0.98), 3)

    best_time_local = str(calc_window.get("peak_local") or primary_window.get("peak_local") or "")
    timing_source = "forecast_peak"
    if daily_peak_state == "locked":
        observed_time = str(obs.get("observed_max_time_local") or "")
        if observed_time:
            best_time_local = observed_time
            timing_source = "observed_peak"
    elif (
        str(track_state.get("main_track_evolution") or "") == "approaching"
        and str(track_state.get("main_track_closest_time_local") or "")
    ):
        best_time_local = str(track_state.get("main_track_closest_time_local") or "")
        timing_source = "track_eta"
    best_time_local = _shift_local_time_text(best_time_local, timing_shift_h)

    timing_confidence = "medium"
    if daily_peak_state == "locked":
        timing_confidence = "high"
    elif multi_peak_state == "likely" or second_peak_potential in {"moderate", "high"}:
        timing_confidence = "low"
    elif str(track_state.get("main_track_confidence") or "") == "high":
        timing_confidence = "medium_high"

    return {
        "schema_version": WEATHER_POSTERIOR_CORE_SCHEMA_VERSION,
        "unit": str(raw.get("unit") or "C"),
        "source": "heuristic-core.v2",
        "anchor": {
            "modeled_peak_c": modeled_peak_c,
            "latest_temp_c": latest_temp_c,
            "observed_max_temp_c": observed_max_temp_c,
            "observed_floor_c": floor_c,
            "observed_ceiling_c": _safe_float(obs.get("observed_max_interval_hi_c")),
            "posterior_median_c": round(median_c, 2),
            "spread_c": round(spread_c, 2),
            "ensemble_dominant_path": ensemble_dominant_path,
            "ensemble_dominant_prob": ensemble_dominant_prob,
            "ensemble_active_source": ensemble_active_source,
            "ensemble_matched_subset_active": ensemble_matched_subset_active,
            "ensemble_matched_member_share": round(ensemble_matched_member_share, 3) if ensemble_matched_member_share is not None else None,
            "ensemble_full_dominant_path": ensemble_full_dominant_path,
            "ensemble_full_dominant_prob": ensemble_full_dominant_prob,
            "ensemble_observed_path": ensemble_observed_path,
            "ensemble_alignment_confidence": ensemble_alignment_confidence,
            "baseline_posterior_median_c": round(baseline_median_c, 2),
            "baseline_spread_c": round(baseline_spread_c, 2),
            "branch_conditioned_target_c": round(branch_anchor_target_c, 2) if branch_anchor_target_c is not None else None,
            "branch_conditioned_blend_weight": round(branch_anchor_blend_weight, 2) if branch_anchor_blend_weight else 0.0,
            "member_conditioned_p50_c": round(member_distribution_p50, 2) if member_distribution_p50 is not None else None,
            "member_conditioned_spread_proxy_c": round(member_spread_proxy_c, 2) if member_spread_proxy_c is not None else None,
            "member_conditioned_center_blend_weight": round(member_center_blend_weight, 2),
            "member_conditioned_quantile_blend_weight": round(member_quantile_blend_weight, 2),
            "member_conditioned_effective_count": round(_safe_float(member_distribution.get("effective_member_count")) or 0.0, 2),
            "member_conditioned_dominant_path": str(member_distribution.get("dominant_weighted_path") or ""),
            "member_conditioned_future_family": str(member_distribution.get("dominant_weighted_future_family") or ""),
            "regime_median_shift_c": round(median_c - baseline_median_c, 2),
            "regime_lower_tail_lift_c": round(lower_tail_lift_c, 2),
            "regime_warm_tail_boost": round(warm_tail_boost, 2),
        },
        "quantiles": {
            "p10_c": p10,
            "p25_c": p25,
            "p50_c": p50,
            "p75_c": p75,
            "p90_c": p90,
        },
        "range_hint": {
            "display": {"lo_c": p10, "hi_c": p90},
            "core": {"lo_c": p25, "hi_c": p75},
        },
        "progress": {
            "phase": phase,
            "analysis_window_mode": analysis_window_mode,
            "hours_to_peak": round(hours_to_peak, 2) if hours_to_peak is not None else None,
            "hours_to_window_end": round(hours_to_window_end, 2) if hours_to_window_end is not None else None,
            "daily_peak_state": daily_peak_state,
            "short_term_state": short_term_state,
            "second_peak_potential": second_peak_potential,
            "multi_peak_state": multi_peak_state,
            "observed_anchor_c": observed_progress_anchor_c,
            "modeled_headroom_c": round(modeled_headroom_c, 2) if modeled_headroom_c is not None else None,
            "observed_peak_age_h": round(observed_peak_age_h, 2) if observed_peak_age_h is not None else None,
            "reports_since_observed_peak": int(reports_since_observed_peak) if reports_since_observed_peak is not None else None,
            "latest_gap_below_observed_c": round(latest_gap_below_observed_c, 2) if latest_gap_below_observed_c is not None else None,
            "progress_spread_adjustment_c": round(progress_spread_adjustment, 2),
        },
        "event_probs": {
            "new_high_next_60m": round(_clamp(_logistic(new_high_score), 0.02, 0.98), 3),
            "lock_by_window_end": round(_clamp(_logistic(lock_score), 0.02, 0.98), 3),
            "exceed_modeled_peak": exceed_modeled_peak_prob,
        },
        "peak_time": {
            "best_time_local": best_time_local,
            "window_start_local": str(calc_window.get("start_local") or primary_window.get("start_local") or ""),
            "window_end_local": str(calc_window.get("end_local") or primary_window.get("end_local") or ""),
            "timing_source": timing_source,
            "confidence": timing_confidence,
        },
        "path_context": path_context,
        "raw_scores": {
            "new_high_score": round(new_high_score, 3),
            "lock_score": round(lock_score, 3),
        },
        "regimes": {
            "station": station_icao,
            "profile": dict(regime_adjustment.get("profile") or regime_detection.get("profile") or {}),
            "active_regimes": list(regime_adjustment.get("applied_regimes") or []),
        },
        "reason_codes": sorted(set(reason_codes)),
    }
