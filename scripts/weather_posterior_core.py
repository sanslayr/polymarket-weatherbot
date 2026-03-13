#!/usr/bin/env python3
"""Uncalibrated weather posterior core from quantitative feature contracts."""

from __future__ import annotations

import math
from typing import Any

from contracts import WEATHER_POSTERIOR_CORE_SCHEMA_VERSION


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
        spread = min(spread, 0.45)
    return _clamp(spread, 0.30, 2.40)


def build_weather_posterior_core(
    *,
    canonical_raw_state: dict[str, Any],
    posterior_feature_vector: dict[str, Any],
) -> dict[str, Any]:
    raw = canonical_raw_state if isinstance(canonical_raw_state, dict) else {}
    feat = posterior_feature_vector if isinstance(posterior_feature_vector, dict) else {}

    obs = dict(raw.get("observations") or {})
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

    latest_temp_c = _safe_float(obs_state.get("latest_temp_c"))
    observed_max_temp_c = _safe_float(obs_state.get("observed_max_temp_c"))
    observed_floor_c = _safe_float(obs.get("observed_max_interval_lo_c"))
    modeled_peak_c = _safe_float(calc_window.get("peak_temp_c"))
    if modeled_peak_c is None:
        modeled_peak_c = _safe_float(primary_window.get("peak_temp_c"))

    hours_to_peak = _safe_float(time_phase.get("hours_to_peak"))
    hours_to_window_end = _safe_float(time_phase.get("hours_to_window_end"))
    daily_peak_state = str(peak_state.get("daily_peak_state") or "open")
    short_term_state = str(peak_state.get("short_term_state") or "holding")
    second_peak_potential = str(peak_state.get("second_peak_potential") or "none")
    plateau_hold_state = str(peak_state.get("plateau_hold_state") or "none")

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
    surface_coupling = str(mixing_state.get("surface_coupling_state") or "")
    low_level_cap_score = _safe_float(vertical_state.get("low_level_cap_score")) or 0.0
    multi_peak_state = str(shape_state.get("multi_peak_state") or "none")
    coverage_density = str(vertical_state.get("coverage_density") or "")
    main_track_confidence = str(track_state.get("main_track_confidence") or "")
    ensemble_dominant_path = str(ensemble_state.get("dominant_path") or "")
    ensemble_split_state = str(ensemble_state.get("split_state") or "")
    ensemble_dominant_prob = _safe_float(ensemble_state.get("dominant_prob"))
    ensemble_alignment_match_state = str(ensemble_state.get("observed_alignment_match_state") or "")
    ensemble_alignment_confidence = str(ensemble_state.get("observed_alignment_confidence") or "")
    ensemble_alignment_score = _safe_float(ensemble_state.get("observed_alignment_score"))
    ensemble_observed_path = str(ensemble_state.get("observed_path") or "")
    ensemble_observed_path_locked = bool(ensemble_state.get("observed_path_locked"))

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
    spread_c = _clamp(spread_c + live_alignment_spread_adjustment, 0.30, 2.40)
    if live_alignment_spread_adjustment < 0.0 and ensemble_alignment_confidence == "high" and ensemble_observed_path_locked:
        reason_codes.append("ensemble_path_alignment_locked")
    elif live_alignment_spread_adjustment < 0.0 and ensemble_alignment_confidence in {"high", "partial"} and ensemble_alignment_match_state in {"exact", "path"}:
        reason_codes.append("ensemble_path_live_confirmed")

    quantile_values = _ensure_monotonic(
        [
            median_c - 1.25 * spread_c,
            median_c - 0.65 * spread_c,
            median_c,
            median_c + 0.65 * spread_c,
            median_c + 1.25 * spread_c,
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
            "posterior_median_c": round(median_c, 2),
            "spread_c": round(spread_c, 2),
            "ensemble_dominant_path": ensemble_dominant_path,
            "ensemble_dominant_prob": ensemble_dominant_prob,
            "ensemble_observed_path": ensemble_observed_path,
            "ensemble_alignment_confidence": ensemble_alignment_confidence,
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
        "raw_scores": {
            "new_high_score": round(new_high_score, 3),
            "lock_score": round(lock_score, 3),
        },
        "reason_codes": sorted(set(reason_codes)),
    }
