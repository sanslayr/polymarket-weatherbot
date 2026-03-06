#!/usr/bin/env python3
"""Range-blending policy for historical analog guidance."""

from __future__ import annotations

from typing import Any

from historical_payload import get_historical_payload, get_weighted_reference

_FEATURE_RULES = {
    "wind-shift sensitive": {"weight_scale": 1.08, "cap_scale": 1.08, "width_scale": 1.04, "reason": "站点对风向切换敏感"},
    "frequent cloud-break rebounds": {"weight_scale": 1.08, "cap_scale": 1.05, "width_scale": 1.03, "reason": "站点常见开窗反弹"},
    "midday cloud suppression risk": {"weight_scale": 1.05, "cap_scale": 1.00, "width_scale": 1.08, "reason": "午间低云压制常见"},
    "frequent precip resets": {"weight_scale": 1.03, "cap_scale": 0.98, "width_scale": 1.08, "reason": "降水重置路径常见"},
    "humid-heat persistence": {"weight_scale": 1.03, "cap_scale": 0.98, "width_scale": 1.05, "reason": "湿热持续型占比不低"},
    "late-day surge risk": {"weight_scale": 1.06, "cap_scale": 1.05, "width_scale": 1.02, "reason": "站点晚峰倾向较强"},
    "balanced baseline station": {"weight_scale": 0.96, "cap_scale": 0.95, "width_scale": 1.00, "reason": "站点长期基线更平衡"},
}

_BRANCH_MODE_RULES = {
    "converged": {"weight_scale": 1.06, "cap_scale": 1.02, "width_scale": 0.95, "reason": "高相似样本基本收敛"},
    "preferred": {"weight_scale": 1.00, "cap_scale": 1.00, "width_scale": 0.98, "reason": "主分支较明确"},
    "competitive": {"weight_scale": 0.82, "cap_scale": 0.86, "width_scale": 1.10, "reason": "高相似样本存在竞争分支"},
    "split": {"weight_scale": 0.58, "cap_scale": 0.72, "width_scale": 1.22, "reason": "高相似样本明显分叉"},
}


def _safe_float(value: Any) -> float | None:
    try:
        if value in (None, ""):
            return None
        return float(value)
    except Exception:
        return None


def _clamp(value: float, low: float, high: float) -> float:
    return max(low, min(high, value))


def _parse_feature_tokens(context: dict[str, Any] | None) -> set[str]:
    if not isinstance(context, dict):
        return set()
    prior = context.get("station_prior")
    if not isinstance(prior, dict):
        return set()
    raw = str(prior.get("special_features") or "")
    return {item.strip().lower() for item in raw.split(",") if item.strip()}


def _station_policy(context: dict[str, Any] | None) -> dict[str, Any]:
    tokens = _parse_feature_tokens(context)
    weight_scale = 1.0
    cap_scale = 1.0
    width_scale = 1.0
    reasons: list[str] = []
    for token in sorted(tokens):
        rule = _FEATURE_RULES.get(token)
        if not rule:
            continue
        weight_scale *= float(rule["weight_scale"])
        cap_scale *= float(rule["cap_scale"])
        width_scale *= float(rule["width_scale"])
        reasons.append(str(rule["reason"]))
    return {
        "weight_scale": weight_scale,
        "cap_scale": cap_scale,
        "width_scale": width_scale,
        "reasons": reasons[:3],
    }


def _branch_policy(branch_assessment: dict[str, Any] | None) -> dict[str, Any]:
    if not isinstance(branch_assessment, dict):
        return {"weight_scale": 1.0, "cap_scale": 1.0, "width_scale": 1.0, "reason": "", "mode": ""}
    mode = str(branch_assessment.get("branch_mode") or "")
    rule = _BRANCH_MODE_RULES.get(mode, {"weight_scale": 1.0, "cap_scale": 1.0, "width_scale": 1.0, "reason": ""})
    return {
        "weight_scale": float(rule["weight_scale"]),
        "cap_scale": float(rule["cap_scale"]),
        "width_scale": float(rule["width_scale"]),
        "reason": str(rule["reason"]),
        "mode": mode,
    }


def blend_historical_range(
    *,
    metar_diag: dict[str, Any],
    phase_now: str,
    compact_settled_mode: bool,
    core_lo: float,
    core_hi: float,
    disp_lo: float,
    disp_hi: float,
) -> tuple[float, float, float, float, dict[str, Any] | None]:
    weighted = get_weighted_reference(metar_diag)
    payload = get_historical_payload(metar_diag)
    if not isinstance(weighted, dict) or compact_settled_mode:
        return core_lo, core_hi, disp_lo, disp_hi, None

    recommended = _safe_float(weighted.get("recommended_tmax_c"))
    if recommended is None:
        return core_lo, core_hi, disp_lo, disp_hi, None

    analog_count = int(_safe_float(weighted.get("analog_count")) or 0)
    if analog_count <= 0:
        return core_lo, core_hi, disp_lo, disp_hi, None

    branch_assessment = payload.get("branch_assessment") if isinstance(payload, dict) else None
    context = payload.get("context") if isinstance(payload, dict) else None

    strength = str(weighted.get("reference_strength") or "weak")
    selected_branch = str(weighted.get("selected_branch") or "")
    synoptic_alignment = str(weighted.get("synoptic_alignment") or "neutral")

    base_center = (float(core_lo) + float(core_hi)) / 2.0
    base_core_half = max(0.1, (float(core_hi) - float(core_lo)) / 2.0)
    base_disp_half = max(base_core_half, (float(disp_hi) - float(disp_lo)) / 2.0)

    ref_weight = {"weak": 0.18, "medium": 0.42, "strong": 0.68}.get(strength, 0.18)
    ref_cap = {"weak": 0.45, "medium": 0.95, "strong": 1.55}.get(strength, 0.45)
    width_blend = {"weak": 0.10, "medium": 0.30, "strong": 0.45}.get(strength, 0.10)
    notes: list[str] = []

    branch_policy = _branch_policy(branch_assessment)
    if branch_policy.get("reason"):
        notes.append(str(branch_policy["reason"]))
    ref_weight *= float(branch_policy["weight_scale"])
    ref_cap *= float(branch_policy["cap_scale"])
    width_blend *= float(branch_policy["width_scale"])

    station_policy = _station_policy(context if isinstance(context, dict) else None)
    ref_weight *= float(station_policy["weight_scale"])
    ref_cap *= float(station_policy["cap_scale"])
    width_blend *= float(station_policy["width_scale"])
    notes.extend(station_policy["reasons"])

    if synoptic_alignment == "supportive":
        ref_weight += 0.06
        ref_cap += 0.15
        notes.append("环流背景支持当前分支")
    elif synoptic_alignment == "conflicting":
        ref_weight = max(0.08, ref_weight - 0.10)
        ref_cap = max(0.25, ref_cap - 0.20)
        width_blend *= 1.08
        notes.append("环流背景与当前分支存在冲突")

    phase_scale = {"far": 0.70, "near_window": 1.0, "in_window": 1.08, "post": 0.55}.get(phase_now, 0.85)
    if phase_now == "post":
        notes.append("峰值窗已过，历史参考只作轻修正")

    same_hour_delta = abs(_safe_float(weighted.get("live_temp_delta_c")) or 0.0)
    if same_hour_delta >= 3.5:
        ref_weight *= 0.45
        ref_cap *= 0.65
        width_blend *= 1.15
        notes.append("当前实况已明显偏离历史同小时路径")
    elif same_hour_delta >= 2.0:
        ref_weight *= 0.72
        ref_cap *= 0.82
        width_blend *= 1.08
        notes.append("当前实况与历史同小时存在偏差")

    if analog_count == 1:
        ref_weight *= 0.70
        ref_cap *= 0.80
        width_blend *= 1.06
        notes.append("高置信样本只有 1 天")
    elif analog_count >= 3 and str(branch_policy.get("mode") or "") in {"converged", "preferred"}:
        ref_weight *= 1.04

    ref_weight = _clamp(ref_weight, 0.08, 0.88)
    ref_cap = _clamp(ref_cap, 0.25, 2.10)
    width_blend = _clamp(width_blend, 0.05, 0.65)

    shift = _clamp((recommended - base_center) * ref_weight * phase_scale, -ref_cap, ref_cap)

    ref_low = _safe_float(weighted.get("recommended_tmax_p25_c"))
    ref_high = _safe_float(weighted.get("recommended_tmax_p75_c"))
    if ref_low is not None and ref_high is not None and ref_high > ref_low:
        ref_half = max(0.1, (ref_high - ref_low) / 2.0)
        core_half = base_core_half * (1.0 - width_blend) + ref_half * width_blend
        disp_half = base_disp_half * (1.0 - width_blend) + max(ref_half + 0.15, ref_half * 1.20) * width_blend
    else:
        core_half = base_core_half
        disp_half = base_disp_half

    new_center = base_center + shift
    new_core_lo = new_center - core_half
    new_core_hi = new_center + core_half
    new_disp_lo = new_center - disp_half
    new_disp_hi = new_center + disp_half

    return new_core_lo, new_core_hi, new_disp_lo, new_disp_hi, {
        "applied": True,
        "strength": strength,
        "selected_branch": selected_branch,
        "synoptic_alignment": synoptic_alignment,
        "shift_c": round(shift, 2),
        "base_center_c": round(base_center, 2),
        "recommended_center_c": round(recommended, 2),
        "analog_count": analog_count,
        "branch_mode": str(branch_policy.get("mode") or ""),
        "policy_notes": notes[:4],
    }
