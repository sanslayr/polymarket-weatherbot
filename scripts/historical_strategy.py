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

_REFERENCE_STRENGTH_RULES = {
    "weak": {"center_weight": 0.0, "center_cap_c": 0.0, "width_weight": 0.0, "display": False},
    "medium": {"center_weight": 0.18, "center_cap_c": 0.80, "width_weight": 0.14, "display": True},
    "strong": {"center_weight": 0.30, "center_cap_c": 1.20, "width_weight": 0.22, "display": True},
}

_PHASE_BLEND_RULES = {
    "far": {"center_scale": 0.58, "cap_scale": 0.85, "width_scale": 1.10},
    "near_window": {"center_scale": 0.90, "cap_scale": 1.00, "width_scale": 1.00},
    "in_window": {"center_scale": 1.00, "cap_scale": 1.00, "width_scale": 0.96},
    "post": {"center_scale": 0.72, "cap_scale": 0.82, "width_scale": 0.94},
    "unknown": {"center_scale": 0.75, "cap_scale": 0.90, "width_scale": 1.00},
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


def _blend(base: float, target: float, weight: float) -> float:
    alpha = _clamp(weight, 0.0, 1.0)
    return float(base) + (float(target) - float(base)) * alpha


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

    strength_rule = _REFERENCE_STRENGTH_RULES.get(strength, _REFERENCE_STRENGTH_RULES["weak"])
    phase_rule = _PHASE_BLEND_RULES.get(phase_now, _PHASE_BLEND_RULES["unknown"])
    ref_weight = float(strength_rule["center_weight"])
    ref_cap = float(strength_rule["center_cap_c"])
    width_blend = float(strength_rule["width_weight"])
    notes: list[str] = []

    branch_policy = _branch_policy(branch_assessment)
    station_policy = _station_policy(context if isinstance(context, dict) else None)
    if branch_policy.get("reason"):
        notes.append(str(branch_policy["reason"]))
    notes.extend(station_policy["reasons"])
    if synoptic_alignment == "supportive":
        notes.append("环流背景与历史分支大体相容")
    elif synoptic_alignment == "conflicting":
        notes.append("环流背景与历史分支不完全相容")
    if phase_now == "post":
        notes.append("峰值窗已过，历史参考仅保留旁证")
    display_strength = bool(strength_rule.get("display"))

    ref_weight *= float(phase_rule["center_scale"])
    ref_weight *= float(branch_policy.get("weight_scale") or 1.0)
    ref_weight *= float(station_policy.get("weight_scale") or 1.0)
    if synoptic_alignment == "supportive":
        ref_weight += 0.03
    elif synoptic_alignment == "conflicting":
        ref_weight -= 0.08
    ref_weight = _clamp(ref_weight, 0.0, 0.42)

    ref_cap *= float(phase_rule["cap_scale"])
    ref_cap *= float(branch_policy.get("cap_scale") or 1.0)
    ref_cap *= float(station_policy.get("cap_scale") or 1.0)
    if synoptic_alignment == "conflicting":
        ref_cap *= 0.75
    ref_cap = max(0.0, ref_cap)

    width_blend *= float(phase_rule["width_scale"])
    width_blend = _clamp(width_blend, 0.0, 0.30)

    apply_center = display_strength and ref_weight >= 0.05 and ref_cap > 0.0
    if synoptic_alignment == "conflicting" and strength != "strong":
        apply_center = False

    raw_shift = float(recommended) - float(base_center)
    applied_shift = 0.0
    if apply_center:
        applied_shift = _clamp(raw_shift, -ref_cap, ref_cap) * ref_weight
        if abs(applied_shift) < 0.03:
            applied_shift = 0.0

    hist_low = _safe_float(weighted.get("recommended_tmax_p25_c"))
    hist_high = _safe_float(weighted.get("recommended_tmax_p75_c"))
    hist_core_half = base_core_half
    hist_disp_half = base_disp_half
    if hist_low is not None and hist_high is not None and hist_high > hist_low:
        hist_core_half = max(0.1, (hist_high - hist_low) / 2.0)
        hist_disp_half = max(hist_core_half, hist_core_half * 1.35)

    target_core_half = hist_core_half * float(branch_policy.get("width_scale") or 1.0) * float(station_policy.get("width_scale") or 1.0)
    target_disp_half = hist_disp_half * float(branch_policy.get("width_scale") or 1.0) * float(station_policy.get("width_scale") or 1.0)
    if hist_low is None or hist_high is None:
        target_core_half = base_core_half * float(branch_policy.get("width_scale") or 1.0) * float(station_policy.get("width_scale") or 1.0)
        target_disp_half = base_disp_half * float(branch_policy.get("width_scale") or 1.0) * float(station_policy.get("width_scale") or 1.0)

    width_adjust = display_strength and width_blend > 0.0
    if width_adjust and synoptic_alignment == "conflicting":
        width_blend = max(width_blend, 0.10)
    elif not width_adjust:
        width_blend = 0.0

    new_core_half = _blend(base_core_half, target_core_half, width_blend) if width_blend > 0.0 else base_core_half
    new_disp_half = _blend(base_disp_half, max(target_disp_half, target_core_half), max(width_blend, 0.08 if width_adjust and str(branch_policy.get("mode") or "") in {"competitive", "split"} else 0.0)) if width_blend > 0.0 or (width_adjust and str(branch_policy.get("mode") or "") in {"competitive", "split"}) else base_disp_half
    new_disp_half = max(new_core_half, new_disp_half)

    new_center = base_center + applied_shift
    new_core_lo = new_center - new_core_half
    new_core_hi = new_center + new_core_half
    new_disp_lo = new_center - new_disp_half
    new_disp_hi = new_center + new_disp_half

    applied = bool(abs(applied_shift) >= 0.03 or abs(new_core_half - base_core_half) >= 0.03 or abs(new_disp_half - base_disp_half) >= 0.03)
    advisory_only = not applied
    if applied:
        notes.append("历史同型日作为偏差修正因子参与了区间定标")
    else:
        notes.append("历史同型日仅作为路径旁证，未直接改写中心值")

    return (
        float(new_core_lo if applied else core_lo),
        float(new_core_hi if applied else core_hi),
        float(new_disp_lo if applied else disp_lo),
        float(new_disp_hi if applied else disp_hi),
        {
            "applied": applied,
            "display": display_strength,
            "advisory_only": advisory_only,
            "strength": strength,
            "selected_branch": selected_branch,
            "synoptic_alignment": synoptic_alignment,
            "shift_c": round(applied_shift, 2),
            "base_center_c": round(base_center, 2),
            "recommended_center_c": round(recommended, 2),
            "analog_count": analog_count,
            "branch_mode": str(branch_policy.get("mode") or ""),
            "policy_notes": notes[:4],
        },
    )
