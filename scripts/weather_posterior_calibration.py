#!/usr/bin/env python3
"""Calibration hook for weather posterior uncertainty adjustment."""

from __future__ import annotations

from typing import Any

from contracts import WEATHER_POSTERIOR_SCHEMA_VERSION


def _safe_float(value: Any) -> float | None:
    try:
        if value in (None, ""):
            return None
        return float(value)
    except Exception:
        return None


def _clamp(value: float, lower: float, upper: float) -> float:
    return max(lower, min(upper, float(value)))


def _shrink_probability(prob: float | None, shrinkage: float) -> float | None:
    if prob is None:
        return None
    return round(0.5 + (float(prob) - 0.5) * (1.0 - float(shrinkage)), 3)


def _expand_quantiles(
    quantiles: dict[str, Any],
    *,
    center: float,
    multiplier: float,
    floor: float | None,
) -> dict[str, float]:
    out: dict[str, float] = {}
    for key in ("p10_c", "p25_c", "p50_c", "p75_c", "p90_c"):
        value = _safe_float(quantiles.get(key))
        if value is None:
            continue
        if key == "p50_c":
            adjusted = center
        else:
            adjusted = center + (value - center) * multiplier
        if floor is not None:
            adjusted = max(adjusted, floor)
        out[key] = round(adjusted, 2)

    ordered_keys = ["p10_c", "p25_c", "p50_c", "p75_c", "p90_c"]
    running = floor
    for key in ordered_keys:
        if key not in out:
            continue
        if running is not None:
            out[key] = round(max(out[key], running), 2)
        running = out[key]
    return out


def _progress_spread_multiplier(
    *,
    posterior_core: dict[str, Any],
    event_probs: dict[str, Any],
) -> float:
    core = dict(posterior_core or {})
    progress = dict(core.get("progress") or {})
    phase = str(progress.get("phase") or "")
    if phase not in {"near_window", "in_window", "post"}:
        return 1.0

    observed_anchor_c = _safe_float(progress.get("observed_anchor_c"))
    if observed_anchor_c is None:
        return 1.0

    lock_prob = _safe_float(event_probs.get("lock_by_window_end"))
    new_high_prob = _safe_float(event_probs.get("new_high_next_60m"))
    modeled_headroom_c = _safe_float(progress.get("modeled_headroom_c"))
    observed_peak_age_h = _safe_float(progress.get("observed_peak_age_h"))
    latest_gap_below_observed_c = _safe_float(progress.get("latest_gap_below_observed_c"))
    reports_since_observed_peak = _safe_float(progress.get("reports_since_observed_peak"))
    hours_to_peak = _safe_float(progress.get("hours_to_peak"))
    hours_to_window_end = _safe_float(progress.get("hours_to_window_end"))
    analysis_window_mode = str(progress.get("analysis_window_mode") or "")
    second_peak_potential = str(progress.get("second_peak_potential") or "")
    multi_peak_state = str(progress.get("multi_peak_state") or "")

    multiplier = 1.0
    if analysis_window_mode == "obs_plateau_reanchor":
        multiplier -= 0.18
    elif analysis_window_mode == "obs_peak_reanchor":
        multiplier -= 0.10

    if hours_to_window_end is not None:
        window_tail_h = float(hours_to_window_end)
        if window_tail_h <= 0.40:
            multiplier -= 0.10
        elif window_tail_h <= 0.75:
            multiplier -= 0.07
        elif phase == "in_window" and window_tail_h <= 1.25:
            multiplier -= 0.04

    if hours_to_peak is not None:
        peak_offset_h = float(hours_to_peak)
        if peak_offset_h <= 0.0:
            multiplier -= 0.05
        elif peak_offset_h <= 0.35:
            multiplier -= 0.03

    if modeled_headroom_c is not None:
        headroom = max(0.0, float(modeled_headroom_c))
        if headroom <= 0.25:
            multiplier -= 0.16
        elif headroom <= 0.55:
            multiplier -= 0.10
        elif phase in {"in_window", "post"} and headroom <= 0.85:
            multiplier -= 0.06

    if observed_peak_age_h is not None:
        if observed_peak_age_h >= 0.75:
            multiplier -= 0.06
        elif observed_peak_age_h >= 0.40:
            multiplier -= 0.03

    if latest_gap_below_observed_c is not None:
        if latest_gap_below_observed_c >= 0.25:
            multiplier -= 0.08
        elif latest_gap_below_observed_c >= 0.15:
            multiplier -= 0.04

    if reports_since_observed_peak is not None and reports_since_observed_peak >= 2.0:
        multiplier -= 0.04

    if lock_prob is not None:
        if lock_prob >= 0.72:
            multiplier -= 0.14
        elif lock_prob >= 0.58:
            multiplier -= 0.07

    if new_high_prob is not None:
        if new_high_prob <= 0.25:
            multiplier -= 0.08
        elif new_high_prob <= 0.40:
            multiplier -= 0.04

    floor = {
        "near_window": 0.72,
        "in_window": 0.62,
        "post": 0.55,
    }.get(phase, 0.72)
    if second_peak_potential in {"moderate", "high"} or multi_peak_state == "likely":
        floor = max(floor, 0.86)
    elif second_peak_potential == "weak" or multi_peak_state == "possible":
        floor = max(floor, 0.78)

    return round(_clamp(multiplier, floor, 1.0), 3)


def _cap_upper_tail(
    quantiles: dict[str, float],
    *,
    cap_hi: float | None,
    floor: float | None,
) -> dict[str, float]:
    if cap_hi is None:
        return dict(quantiles)

    out = dict(quantiles)
    for key in ("p50_c", "p75_c", "p90_c"):
        value = _safe_float(out.get(key))
        if value is None:
            continue
        out[key] = round(min(float(value), float(cap_hi)), 2)

    ordered_keys = ["p10_c", "p25_c", "p50_c", "p75_c", "p90_c"]
    running = floor
    for key in ordered_keys:
        value = _safe_float(out.get(key))
        if value is None:
            continue
        if running is not None:
            value = max(float(value), float(running))
        out[key] = round(value, 2)
        running = value
    return out


def _apply_cold_tail_allowance(
    quantiles: dict[str, float],
    *,
    allowance_c: float | None,
    floor: float | None,
) -> dict[str, float]:
    if allowance_c is None or abs(float(allowance_c)) < 0.01:
        return dict(quantiles)

    out = dict(quantiles)
    p10 = _safe_float(out.get("p10_c"))
    p25 = _safe_float(out.get("p25_c"))
    p50 = _safe_float(out.get("p50_c"))
    if None in {p10, p25, p50}:
        return out

    shift = float(allowance_c)
    out["p10_c"] = round(float(p10) - shift, 2)
    out["p25_c"] = round(float(p25) - 0.65 * shift, 2)

    ordered_keys = ["p10_c", "p25_c", "p50_c", "p75_c", "p90_c"]
    running = floor
    for key in ordered_keys:
        value = _safe_float(out.get(key))
        if value is None:
            continue
        if running is not None:
            value = max(float(value), float(running))
        out[key] = round(value, 2)
        running = value
    return out


def _progress_upper_tail_cap(
    *,
    posterior_core: dict[str, Any],
    event_probs: dict[str, Any],
) -> float | None:
    core = dict(posterior_core or {})
    progress = dict(core.get("progress") or {})
    phase = str(progress.get("phase") or "")
    if phase not in {"near_window", "in_window", "post"}:
        return None

    observed_anchor_c = _safe_float(progress.get("observed_anchor_c"))
    if observed_anchor_c is None:
        return None

    lock_prob = _safe_float(event_probs.get("lock_by_window_end"))
    new_high_prob = _safe_float(event_probs.get("new_high_next_60m"))
    modeled_headroom_c = _safe_float(progress.get("modeled_headroom_c"))
    observed_peak_age_h = _safe_float(progress.get("observed_peak_age_h"))
    latest_gap_below_observed_c = _safe_float(progress.get("latest_gap_below_observed_c"))
    latest_temp_c = _safe_float((core.get("anchor") or {}).get("latest_temp_c"))
    hours_to_peak = _safe_float(progress.get("hours_to_peak"))
    hours_to_window_end = _safe_float(progress.get("hours_to_window_end"))
    analysis_window_mode = str(progress.get("analysis_window_mode") or "")
    second_peak_potential = str(progress.get("second_peak_potential") or "")
    multi_peak_state = str(progress.get("multi_peak_state") or "")
    path_context = dict(core.get("path_context") or {})

    if (
        new_high_prob is not None
        and new_high_prob >= 0.60
        and (lock_prob is None or lock_prob < 0.50)
    ):
        return None

    allowance = {
        "near_window": 0.70,
        "in_window": 0.45,
        "post": 0.28,
    }.get(phase, 0.45)

    if analysis_window_mode == "obs_plateau_reanchor":
        allowance = min(allowance, 0.22)
    elif analysis_window_mode == "obs_peak_reanchor":
        allowance = min(allowance, 0.35)

    if hours_to_window_end is not None:
        window_tail_h = float(hours_to_window_end)
        if window_tail_h <= 0.40:
            allowance = min(allowance, 0.28 if phase == "near_window" else 0.20)
        elif window_tail_h <= 0.75:
            allowance = min(allowance, 0.36 if phase == "near_window" else 0.24)
        elif phase == "in_window" and window_tail_h <= 1.25:
            allowance = min(allowance, 0.40)

    if modeled_headroom_c is not None:
        allowance = min(
            allowance,
            max(0.16, max(0.0, float(modeled_headroom_c)) * 0.90 + 0.10),
        )

    if hours_to_peak is not None:
        peak_offset_h = float(hours_to_peak)
        if peak_offset_h <= 0.0:
            allowance -= 0.06
        elif peak_offset_h <= 0.35:
            allowance -= 0.03

    if observed_peak_age_h is not None:
        if observed_peak_age_h >= 1.0:
            allowance -= 0.08
        elif observed_peak_age_h >= 0.50:
            allowance -= 0.04

    if latest_gap_below_observed_c is not None:
        if latest_gap_below_observed_c >= 0.25:
            allowance -= 0.08
        elif latest_gap_below_observed_c >= 0.15:
            allowance -= 0.04

    if lock_prob is not None and new_high_prob is not None:
        if lock_prob >= 0.80 and new_high_prob <= 0.20:
            allowance = min(allowance, 0.26 if phase == "near_window" else 0.18)
        elif lock_prob >= 0.65 and new_high_prob <= 0.35:
            allowance = min(allowance, 0.34 if phase == "near_window" else 0.24)

    if second_peak_potential in {"moderate", "high"} or multi_peak_state == "likely":
        allowance += 0.20
    elif second_peak_potential == "weak" or multi_peak_state == "possible":
        allowance += 0.08

    allowance += _safe_float(path_context.get("upper_tail_allowance_adjust_c")) or 0.0
    allowance = _clamp(allowance, 0.15, 1.20)
    cap_hi = observed_anchor_c + allowance
    if latest_temp_c is not None:
        cap_hi = max(cap_hi, latest_temp_c + 0.12)
    return round(cap_hi, 2)


def _progress_cold_tail_allowance(
    *,
    posterior_core: dict[str, Any],
    event_probs: dict[str, Any],
) -> float:
    core = dict(posterior_core or {})
    progress = dict(core.get("progress") or {})
    phase = str(progress.get("phase") or "")
    if phase not in {"near_window", "in_window", "post"}:
        return 0.0

    path_context = dict(core.get("path_context") or {})
    allowance = _safe_float(path_context.get("cold_tail_allowance_c")) or 0.0
    if abs(allowance) < 0.01:
        return 0.0

    lock_prob = _safe_float(event_probs.get("lock_by_window_end"))
    new_high_prob = _safe_float(event_probs.get("new_high_next_60m"))
    second_peak_potential = str(progress.get("second_peak_potential") or "")
    multi_peak_state = str(progress.get("multi_peak_state") or "")

    if allowance > 0.0 and lock_prob is not None and new_high_prob is not None:
        if lock_prob >= 0.70 and new_high_prob <= 0.30:
            allowance += 0.04
        elif new_high_prob >= 0.55:
            allowance -= 0.05

    if allowance > 0.0 and (second_peak_potential in {"moderate", "high"} or multi_peak_state == "likely"):
        allowance *= 0.75

    return round(_clamp(allowance, -0.18, 0.55), 2)


def _display_tail_weight(
    *,
    posterior_core: dict[str, Any],
    event_probs: dict[str, Any],
    confidence_label: str,
) -> float:
    phase = str(((posterior_core.get("progress") or {}).get("phase")) or "")
    tail_weight = {
        "far": 0.55,
        "same_day": 0.35,
        "near_window": 0.0,
        "in_window": 0.0,
        "post": 0.0,
    }.get(phase, 0.25)

    path_context = dict(posterior_core.get("path_context") or {})
    active_side = str(path_context.get("active_path_side") or "")
    branch_family = str(path_context.get("branch_family") or "")
    follow_prob = _safe_float(path_context.get("expected_follow_through_prob")) or 0.0
    new_high_prob = _safe_float(event_probs.get("new_high_next_60m"))
    lock_prob = _safe_float(event_probs.get("lock_by_window_end"))
    if phase in {"near_window", "in_window"}:
        if (
            active_side == "warm"
            and branch_family in {"warm_support_track", "warm_landing_watch", "warm_transition_probe"}
            and follow_prob >= 0.64
            and new_high_prob is not None
            and new_high_prob >= 0.58
            and (lock_prob is None or lock_prob <= 0.55)
        ):
            tail_weight = max(tail_weight, 0.18 if phase == "in_window" else 0.12)

    if confidence_label == "high":
        tail_weight -= 0.10
    elif confidence_label == "low":
        tail_weight += 0.10
    return round(_clamp(tail_weight, 0.0, 1.0), 2)


def _progress_display_cap_active(
    *,
    posterior_core: dict[str, Any],
    event_probs: dict[str, Any],
) -> bool:
    core = dict(posterior_core or {})
    progress = dict(core.get("progress") or {})
    anchor = dict(core.get("anchor") or {})
    phase = str(progress.get("phase") or "")
    if phase not in {"near_window", "in_window", "post"}:
        return False

    observed_floor_c = _safe_float(anchor.get("observed_floor_c"))
    observed_ceiling_c = _safe_float(anchor.get("observed_ceiling_c"))
    if observed_floor_c is None and observed_ceiling_c is None:
        return False

    second_peak_potential = str(progress.get("second_peak_potential") or "")
    multi_peak_state = str(progress.get("multi_peak_state") or "")
    if second_peak_potential in {"moderate", "high"} or multi_peak_state == "likely":
        return False

    analysis_window_mode = str(progress.get("analysis_window_mode") or "")
    if analysis_window_mode in {"obs_plateau_reanchor", "obs_peak_reanchor"}:
        return True

    lock_prob = _safe_float(event_probs.get("lock_by_window_end"))
    new_high_prob = _safe_float(event_probs.get("new_high_next_60m"))
    if (
        lock_prob is not None
        and new_high_prob is not None
        and lock_prob >= 0.72
        and new_high_prob <= 0.28
    ):
        return True

    observed_peak_age_h = _safe_float(progress.get("observed_peak_age_h"))
    latest_gap_below_observed_c = _safe_float(progress.get("latest_gap_below_observed_c"))
    reports_since_observed_peak = _safe_float(progress.get("reports_since_observed_peak"))
    if (
        observed_peak_age_h is not None
        and observed_peak_age_h >= 0.50
        and latest_gap_below_observed_c is not None
        and latest_gap_below_observed_c >= 0.15
    ):
        return True
    if (
        reports_since_observed_peak is not None
        and reports_since_observed_peak >= 2.0
        and latest_gap_below_observed_c is not None
        and latest_gap_below_observed_c >= 0.10
    ):
        return True
    if phase == "post" and latest_gap_below_observed_c is not None and latest_gap_below_observed_c >= 0.10:
        return True
    return False


def _integrated_range_hint(
    *,
    quantiles: dict[str, float],
    posterior_core: dict[str, Any],
    event_probs: dict[str, Any],
    confidence_label: str,
) -> tuple[dict[str, Any], dict[str, Any]]:
    p10 = _safe_float(quantiles.get("p10_c"))
    p25 = _safe_float(quantiles.get("p25_c"))
    p75 = _safe_float(quantiles.get("p75_c"))
    p90 = _safe_float(quantiles.get("p90_c"))
    tail_weight = _display_tail_weight(
        posterior_core=posterior_core,
        event_probs=event_probs,
        confidence_label=confidence_label,
    )
    if None in {p10, p25, p75, p90}:
        return {
            "display": {"lo_c": p10, "hi_c": p90},
            "core": {"lo_c": p25, "hi_c": p75},
        }, {
            "source": "posterior_quantiles",
            "tail_weight": tail_weight,
        }

    disp_lo = float(p25) - float(tail_weight) * float(p25 - p10)
    disp_hi = float(p75) + float(tail_weight) * float(p90 - p75)
    core_lo = float(p25)
    core_hi = float(p75)
    source = "posterior_quantiles"

    core = dict(posterior_core or {})
    anchor = dict(core.get("anchor") or {})
    observed_floor_c = _safe_float(anchor.get("observed_floor_c"))
    observed_ceiling_c = _safe_float(anchor.get("observed_ceiling_c"))
    if _progress_display_cap_active(
        posterior_core=posterior_core,
        event_probs=event_probs,
    ):
        if observed_floor_c is not None:
            disp_lo = max(float(disp_lo), float(observed_floor_c))
            core_lo = max(float(core_lo), float(observed_floor_c))
        if observed_ceiling_c is not None:
            disp_hi = min(float(disp_hi), float(observed_ceiling_c))
            core_hi = min(float(core_hi), float(observed_ceiling_c))

        fallback_lo = float(observed_floor_c) if observed_floor_c is not None else min(float(disp_lo), float(core_lo))
        fallback_hi = float(observed_ceiling_c) if observed_ceiling_c is not None else max(float(disp_hi), float(core_hi))
        if core_hi < core_lo:
            core_lo, core_hi = fallback_lo, fallback_hi
        if disp_hi < disp_lo:
            disp_lo, disp_hi = fallback_lo, fallback_hi
        disp_lo = min(float(disp_lo), float(core_lo))
        disp_hi = max(float(disp_hi), float(core_hi))
        source = "posterior_quantiles_progress_capped"

    return {
        "display": {"lo_c": round(float(disp_lo), 2), "hi_c": round(float(disp_hi), 2)},
        "core": {"lo_c": round(float(core_lo), 2), "hi_c": round(float(core_hi), 2)},
    }, {
        "source": source,
        "tail_weight": tail_weight,
    }


def apply_weather_posterior_calibration(
    *,
    posterior_core: dict[str, Any],
    quality_snapshot: dict[str, Any],
) -> dict[str, Any]:
    core = dict(posterior_core or {})
    quality = dict(quality_snapshot or {})
    adjustments = dict(quality.get("posterior_adjustments") or {})
    confidence_label = str(((quality.get("scores") or {}).get("confidence_label")) or "")

    spread_multiplier = float(adjustments.get("spread_multiplier") or 1.0)
    probability_shrinkage = float(adjustments.get("probability_shrinkage") or 0.0)
    timing_cap = str(adjustments.get("timing_confidence_cap") or "")

    quantiles = dict(core.get("quantiles") or {})
    anchor = dict(core.get("anchor") or {})
    center = _safe_float(anchor.get("posterior_median_c")) or _safe_float(quantiles.get("p50_c")) or 0.0
    floor = _safe_float(anchor.get("observed_floor_c"))
    quality_calibrated_quantiles = _expand_quantiles(
        quantiles,
        center=center,
        multiplier=spread_multiplier,
        floor=floor,
    )

    event_probs = dict(core.get("event_probs") or {})
    calibrated_event_probs = {
        "new_high_next_60m": _shrink_probability(_safe_float(event_probs.get("new_high_next_60m")), probability_shrinkage),
        "lock_by_window_end": _shrink_probability(_safe_float(event_probs.get("lock_by_window_end")), probability_shrinkage),
        "exceed_modeled_peak": _shrink_probability(_safe_float(event_probs.get("exceed_modeled_peak")), probability_shrinkage),
    }

    progress_spread_multiplier = _progress_spread_multiplier(
        posterior_core=core,
        event_probs=calibrated_event_probs,
    )
    calibrated_quantiles = _expand_quantiles(
        quality_calibrated_quantiles,
        center=center,
        multiplier=progress_spread_multiplier,
        floor=floor,
    )
    cold_tail_allowance_c = _progress_cold_tail_allowance(
        posterior_core=core,
        event_probs=calibrated_event_probs,
    )
    calibrated_quantiles = _apply_cold_tail_allowance(
        calibrated_quantiles,
        allowance_c=cold_tail_allowance_c,
        floor=floor,
    )
    upper_tail_cap_c = _progress_upper_tail_cap(
        posterior_core=core,
        event_probs=calibrated_event_probs,
    )
    calibrated_quantiles = _cap_upper_tail(
        calibrated_quantiles,
        cap_hi=upper_tail_cap_c,
        floor=floor,
    )

    peak_time = dict(core.get("peak_time") or {})
    timing_confidence = str(peak_time.get("confidence") or "")
    if timing_cap and timing_confidence not in {"", timing_cap, "low"}:
        timing_confidence = timing_cap

    range_hint, range_hint_meta = _integrated_range_hint(
        quantiles=calibrated_quantiles,
        posterior_core=core,
        event_probs=calibrated_event_probs,
        confidence_label=confidence_label,
    )

    return {
        "schema_version": WEATHER_POSTERIOR_SCHEMA_VERSION,
        "unit": str(core.get("unit") or "C"),
        "source": "heuristic-core.v1+quality-calibration.v1",
        "core": core,
        "quality_snapshot_ref": {
            "confidence_label": confidence_label,
            "spread_multiplier": round(spread_multiplier, 3),
            "progress_spread_multiplier": round(progress_spread_multiplier, 3),
            "probability_shrinkage": round(probability_shrinkage, 3),
        },
        "anchor": anchor,
        "quantiles": calibrated_quantiles,
        "range_hint": range_hint,
        "range_hint_meta": range_hint_meta,
        "event_probs": calibrated_event_probs,
        "peak_time": {
            **peak_time,
            "confidence": timing_confidence or str(peak_time.get("confidence") or ""),
        },
        "calibration": {
            "applied": True,
            "confidence_label": confidence_label,
            "quality_spread_multiplier": round(spread_multiplier, 3),
            "progress_spread_multiplier": round(progress_spread_multiplier, 3),
            "cold_tail_allowance_c": cold_tail_allowance_c,
            "upper_tail_cap_c": upper_tail_cap_c,
        },
        "regimes": dict(core.get("regimes") or {}),
        "reason_codes": list(core.get("reason_codes") or []),
    }
