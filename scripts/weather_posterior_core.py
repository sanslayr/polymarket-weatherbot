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
        "warm_landing_watch": "暖输送待接地",
        "warm_support_track": "暖输送兑现",
        "warm_transition_probe": "暖侧试探",
        "cold_suppression_track": "压温持续",
        "cold_transition_probe": "冷侧试探",
        "convective_interrupt_risk": "对流打断风险",
        "convective_cold_hold": "对流压温延续",
        "cloud_release_watch": "低云待松动",
        "neutral_plateau": "平台过渡",
        "volatile_split": "高波动分支",
        "second_peak_retest": "后段二峰观察",
        "mixed_transition": "过渡分支",
    }.get(str(value or ""), str(value or "当前分支"))


def _branch_gate_label(value: str) -> str:
    return {
        "low_level_coupling": "低层耦合",
        "cloud_release": "低云破碎",
        "convective_intrusion": "对流是否压进峰值窗",
        "convective_persistence": "对流压温能否继续维持",
        "cold_advection_hold": "冷抑制是否继续落地",
        "rebreak_signal": "是否再破前高",
        "branch_resolution": "分支何时收敛",
        "reacceleration": "升温能否重新提速",
        "follow_through": "后段能否继续兑现",
    }.get(str(value or ""), str(value or "后续门槛"))


def _build_path_context(
    *,
    branch_outlook_state: dict[str, Any],
) -> dict[str, Any]:
    branch = dict(branch_outlook_state or {})
    active_path_source = str(branch.get("branch_source") or "")
    active_path = str(branch.get("branch_path") or "")
    active_path_detail = str(branch.get("branch_path_detail") or active_path or "")
    active_side = str(branch.get("branch_side") or _path_side(active_path, path_detail=active_path_detail))
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
    if bool(branch.get("matched_subset_active")):
        signal_weight = 1.0
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
    elif branch_family in {"warm_landing_watch", "warm_transition_probe"}:
        upper_tail_adjust_c -= 0.03 + fallback_prob * 0.16
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
    current_label = _branch_family_label(branch_family)
    significant_detail_score = 0.80

    if branch_family == "warm_support_track":
        significant_detail_text = f"当前匹配的是{current_label}这支，下一步主看{gate_label}；若继续兑现，更可能维持{expected_label}，若掉链子，更容易转成{fallback_label}"
        significant_detail_score = 0.86
    elif branch_family in {"warm_landing_watch", "warm_transition_probe"}:
        significant_detail_text = f"当前匹配的是{current_label}这支，下一步主看{gate_label}；若接上，更可能转到{expected_label}，若接不上，更容易转成{fallback_label}"
        significant_detail_score = 0.90
    elif branch_family == "cloud_release_watch":
        significant_detail_text = f"当前匹配的是{current_label}这支，下一步主看{gate_label}；若松开，更可能转到{expected_label}，若不松，更容易继续停在{fallback_label}"
        significant_detail_score = 0.90
    elif branch_family == "convective_interrupt_risk":
        significant_detail_text = f"当前匹配的是{current_label}这支，下一步主看{gate_label}；若压进来，更可能转到{expected_label}，若没压进来，分支才更容易回到{fallback_label}"
        significant_detail_score = 0.92
    elif branch_family in {"cold_suppression_track", "convective_cold_hold", "cold_transition_probe"}:
        significant_detail_text = f"当前匹配的是{current_label}这支，下一步主看{gate_label}；若继续落地，更可能维持{expected_label}，若松动，更容易转回{fallback_label}"
        significant_detail_score = 0.89
    elif branch_family == "second_peak_retest":
        significant_detail_text = f"当前匹配的是{current_label}这支，下一步主看{gate_label}；若再破，更可能转到{expected_label}，若不破，更容易回到{fallback_label}"
        significant_detail_score = 0.88
    elif branch_family == "volatile_split":
        significant_detail_text = f"当前匹配的是{current_label}这支，下一步主看{gate_label}；若暖侧胜出，更可能转到{expected_label}，若分歧继续，区间仍要保留双侧机动"
        significant_detail_score = 0.86
    else:
        significant_detail_text = f"当前匹配的是{current_label}这支，下一步主看{gate_label}；若继续演化，更可能转到{expected_label}，若不顺，更容易回到{fallback_label}"
        significant_detail_score = 0.82

    if branch_stage_now in {"watch", "pending", "testing"}:
        significant_detail_score += 0.02
    if branch_volatility == "high":
        significant_detail_score += 0.01
    significant_detail_score = round(_clamp(significant_detail_score * signal_weight, 0.55, 0.96), 2)

    return {
        "active_path_source": active_path_source,
        "active_path": active_path,
        "active_path_detail": active_path_detail,
        "active_path_side": active_side,
        "branch_family": branch_family,
        "branch_stage_now": branch_stage_now,
        "branch_volatility": branch_volatility,
        "next_transition_gate": next_gate,
        "expected_next_family": expected_next_family,
        "expected_next_stage": expected_next_stage,
        "expected_follow_through_prob": round(expected_prob, 3),
        "fallback_family": fallback_family,
        "fallback_stage": fallback_stage,
        "fallback_prob": round(fallback_prob, 3),
        "signal_weight": round(signal_weight, 2),
        "upper_tail_allowance_adjust_c": round(upper_tail_adjust_c, 2),
        "cold_tail_allowance_c": round(cold_tail_allowance_c, 2),
        "significant_detail_key": significant_detail_key,
        "significant_forecast_detail_text": significant_detail_text,
        "significant_forecast_detail_score": round(significant_detail_score, 2) if significant_detail_text else 0.0,
    }


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
    path_context = _build_path_context(branch_outlook_state=branch_outlook_state)

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
    min_spread_floor = 0.18 if daily_peak_state == "locked" else (
        0.24 if phase in {"in_window", "post"} else 0.30
    )
    spread_c = _clamp(
        spread_c + live_alignment_spread_adjustment + progress_spread_adjustment,
        min_spread_floor,
        2.40,
    )
    if live_alignment_spread_adjustment < 0.0 and ensemble_alignment_confidence == "high" and ensemble_observed_path_locked:
        reason_codes.append("ensemble_path_alignment_locked")
    elif live_alignment_spread_adjustment < 0.0 and ensemble_alignment_confidence in {"high", "partial"} and ensemble_alignment_match_state in {"exact", "path"}:
        reason_codes.append("ensemble_path_live_confirmed")
    if progress_spread_adjustment < 0.0:
        reason_codes.extend(progress_reason_codes)
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

    quantile_values = _ensure_monotonic(
        [
            median_c - 1.15 * spread_c + lower_tail_lift_c,
            median_c - 0.55 * spread_c + 0.65 * lower_tail_lift_c,
            median_c,
            median_c + (0.72 + warm_tail_boost) * spread_c,
            median_c + (1.28 + warm_tail_boost) * spread_c,
        ],
        floor=floor_c,
    )
    p10, p25, p50, p75, p90 = [round(value, 2) for value in quantile_values]

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
