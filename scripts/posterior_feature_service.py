#!/usr/bin/env python3
"""Posterior-ready quantitative feature vector builder."""

from __future__ import annotations

from datetime import datetime
from typing import Any

from contracts import POSTERIOR_FEATURE_VECTOR_SCHEMA_VERSION


def _safe_float(value: Any) -> float | None:
    try:
        if value in (None, ""):
            return None
        return float(value)
    except Exception:
        return None


def _parse_iso_dt(value: Any) -> datetime | None:
    try:
        text = str(value or "").strip()
        return datetime.fromisoformat(text) if text else None
    except Exception:
        return None


def _clamp(value: float, lower: float, upper: float) -> float:
    return max(lower, min(upper, float(value)))


def _coerce_same_tz(a: datetime | None, b: datetime | None) -> tuple[datetime | None, datetime | None]:
    if a is None or b is None:
        return a, b
    try:
        if a.tzinfo is not None and b.tzinfo is None:
            b = b.replace(tzinfo=a.tzinfo)
        elif a.tzinfo is None and b.tzinfo is not None:
            a = a.replace(tzinfo=b.tzinfo)
    except Exception:
        pass
    return a, b


def _hours_between(later: datetime | None, earlier: datetime | None) -> float | None:
    later, earlier = _coerce_same_tz(later, earlier)
    if later is None or earlier is None:
        return None
    try:
        return (later - earlier).total_seconds() / 3600.0
    except Exception:
        return None


def _hours_between_iso(later: Any, earlier: Any) -> float | None:
    return _hours_between(_parse_iso_dt(later), _parse_iso_dt(earlier))


def _cloud_trend_signal(value: str) -> int:
    text = str(value or "").strip().lower()
    if not text:
        return 0
    warm_tokens = (
        "thin",
        "thinning",
        "break",
        "breaking",
        "clear",
        "clearing",
        "improv",
        "lift",
        "lifting",
        "scatter",
        "scattering",
        "decrease",
        "decreasing",
        "reduce",
        "reducing",
        "open",
        "opening",
        "散",
        "开",
        "减",
        "薄",
        "晴",
    )
    cold_tokens = (
        "thick",
        "thickening",
        "build",
        "building",
        "increase",
        "increasing",
        "fill",
        "filling",
        "overcast",
        "worsen",
        "worsening",
        "close",
        "closing",
        "增",
        "厚",
        "阴",
        "封",
    )
    warm_hit = any(token in text for token in warm_tokens)
    cold_hit = any(token in text for token in cold_tokens)
    if warm_hit and not cold_hit:
        return 1
    if cold_hit and not warm_hit:
        return -1
    return 0


def _surface_bias_polarity(value: str) -> str:
    text = str(value or "").strip().lower()
    if not text:
        return ""
    if ("warm" in text) or ("暖" in text):
        return "warm"
    if ("cold" in text) or ("冷" in text):
        return "cold"
    if ("neutral" in text) or ("中性" in text):
        return "neutral"
    return ""


def _live_ensemble_path_alignment(
    *,
    temp_trend_c: float | None,
    temp_bias_c: float | None,
    cloud_effective_cover: float | None,
    radiation_eff: float | None,
    cloud_trend: str,
    precip_state: str,
    transport_state: str,
    thermal_advection_state: str,
    surface_bias: str,
    dominant_path: str,
    dominant_path_detail: str,
    dominant_prob: float | None,
    dominant_detail_prob: float | None,
    dominant_margin_prob: float | None,
) -> dict[str, Any]:
    warm_score = 0.0
    cold_score = 0.0
    neutral_score = 0.0

    if temp_trend_c is not None:
        if temp_trend_c >= 0.28:
            warm_score += 0.34
        elif temp_trend_c >= 0.14:
            warm_score += 0.22
        elif temp_trend_c <= -0.20:
            cold_score += 0.30
        elif temp_trend_c <= -0.08:
            cold_score += 0.18
        else:
            neutral_score += 0.12

    if temp_bias_c is not None:
        if temp_bias_c >= 0.35:
            warm_score += 0.22
        elif temp_bias_c >= 0.15:
            warm_score += 0.12
        elif temp_bias_c <= -0.35:
            cold_score += 0.22
        elif temp_bias_c <= -0.15:
            cold_score += 0.12
        else:
            neutral_score += 0.12

    if radiation_eff is not None:
        if radiation_eff >= 0.80:
            warm_score += 0.18
        elif radiation_eff >= 0.62:
            warm_score += 0.08
            neutral_score += 0.04
        elif radiation_eff <= 0.42:
            cold_score += 0.18
        elif radiation_eff <= 0.55:
            cold_score += 0.08
        else:
            neutral_score += 0.06

    if cloud_effective_cover is not None:
        if cloud_effective_cover <= 0.30:
            warm_score += 0.18
        elif cloud_effective_cover <= 0.55:
            warm_score += 0.08
            neutral_score += 0.04
        elif cloud_effective_cover >= 0.80:
            cold_score += 0.18
        elif cloud_effective_cover >= 0.65:
            cold_score += 0.10
        else:
            neutral_score += 0.04

    cloud_signal = _cloud_trend_signal(cloud_trend)
    if cloud_signal > 0:
        warm_score += 0.12
    elif cloud_signal < 0:
        cold_score += 0.12
    else:
        neutral_score += 0.04

    advection_bonus = {
        "confirmed": 0.22,
        "probable": 0.14,
    }.get(str(thermal_advection_state or "").strip().lower(), 0.0)
    transport_key = str(transport_state or "").strip().lower()
    if transport_key == "warm":
        warm_score += 0.12 + advection_bonus
    elif transport_key == "cold":
        cold_score += 0.12 + advection_bonus
    else:
        neutral_score += 0.10

    surface_bias_key = _surface_bias_polarity(surface_bias)
    if surface_bias_key == "warm":
        warm_score += 0.08
    elif surface_bias_key == "cold":
        cold_score += 0.08
    elif surface_bias_key == "neutral":
        neutral_score += 0.04

    precip_key = str(precip_state or "").strip().lower()
    if precip_key not in {"", "none"}:
        cold_score += 0.16
    else:
        neutral_score += 0.02

    warm_score = round(_clamp(warm_score, 0.0, 1.0), 3)
    cold_score = round(_clamp(cold_score, 0.0, 1.0), 3)
    neutral_score = round(_clamp(neutral_score, 0.0, 1.0), 3)

    balance = warm_score - cold_score
    if warm_score >= 0.78 and warm_score >= cold_score + 0.20:
        observed_path = "warm_support"
        observed_path_detail = "warm_support"
        observed_path_score = warm_score
    elif cold_score >= 0.78 and cold_score >= warm_score + 0.20:
        observed_path = "cold_suppression"
        observed_path_detail = "cold_suppression"
        observed_path_score = cold_score
    else:
        observed_path = "transition"
        if balance >= 0.16:
            observed_path_detail = "weak_warm_transition"
            observed_path_score = max(warm_score, neutral_score + 0.10)
        elif balance <= -0.16:
            observed_path_detail = "weak_cold_transition"
            observed_path_score = max(cold_score, neutral_score + 0.10)
        else:
            observed_path_detail = "neutral_stable"
            observed_path_score = max(neutral_score, 0.42)

    dominant_key = dominant_path_detail if str(dominant_path or "") == "transition" else dominant_path
    observed_key = observed_path_detail if observed_path == "transition" else observed_path

    match_state = "none"
    if observed_key and dominant_key and observed_key == dominant_key:
        match_state = "exact"
    elif observed_path and dominant_path and observed_path == dominant_path:
        match_state = "path"

    alignment_score = 0.0
    if match_state == "exact":
        alignment_score = 0.44 + 0.30 * float(observed_path_score)
        if dominant_prob is not None:
            alignment_score += 0.16 * _clamp(dominant_prob, 0.0, 1.0)
        if dominant_detail_prob is not None:
            alignment_score += 0.10 * _clamp(dominant_detail_prob, 0.0, 1.0)
    elif match_state == "path":
        alignment_score = 0.32 + 0.26 * float(observed_path_score)
        if dominant_prob is not None:
            alignment_score += 0.12 * _clamp(dominant_prob, 0.0, 1.0)
    else:
        alignment_score = 0.12 * float(observed_path_score)

    if dominant_margin_prob is not None and match_state != "none":
        alignment_score += 0.06 * _clamp(dominant_margin_prob * 4.0, 0.0, 1.0)
    alignment_score = round(_clamp(alignment_score, 0.0, 0.98), 3)

    alignment_confidence = "none"
    dominant_prob_v = dominant_prob if dominant_prob is not None else 0.0
    if (
        match_state == "exact"
        and alignment_score >= 0.76
        and observed_path_score >= 0.62
        and dominant_prob_v >= 0.54
    ):
        alignment_confidence = "high"
    elif match_state in {"exact", "path"} and alignment_score >= 0.56 and observed_path_score >= 0.48:
        alignment_confidence = "partial"

    observed_path_locked = (
        alignment_confidence == "high"
        and match_state == "exact"
        and dominant_prob_v >= 0.60
    )
    return {
        "observed_path": observed_path,
        "observed_path_detail": observed_path_detail,
        "observed_path_score": round(float(observed_path_score), 3),
        "observed_warm_signal": warm_score,
        "observed_cold_signal": cold_score,
        "observed_neutral_signal": neutral_score,
        "observed_alignment_match_state": match_state,
        "observed_alignment_matches_dominant": match_state in {"exact", "path"},
        "observed_alignment_exact": match_state == "exact",
        "observed_alignment_score": alignment_score,
        "observed_alignment_confidence": alignment_confidence,
        "observed_path_locked": observed_path_locked,
    }


def build_posterior_feature_vector(
    *,
    canonical_raw_state: dict[str, Any],
    boundary_layer_regime: dict[str, Any] | None = None,
    temp_phase_decision: dict[str, Any] | None = None,
) -> dict[str, Any]:
    raw = canonical_raw_state if isinstance(canonical_raw_state, dict) else {}
    obs = dict(raw.get("observations") or {})
    forecast = dict(raw.get("forecast") or {})
    window = dict(raw.get("window") or {})
    primary_window = dict(window.get("primary") or {})
    calc_window = dict(window.get("calc") or {})
    shape = dict(raw.get("shape") or {})
    shape_forecast = dict(shape.get("forecast") or {})
    shape_observed = dict(shape.get("observed") or {})
    quality = dict(forecast.get("quality") or {})
    context = dict(forecast.get("context") or {})
    source = dict(raw.get("source") or {})
    ensemble_factor = dict(forecast.get("ensemble_factor") or {})
    ensemble_summary = dict(ensemble_factor.get("summary") or {})
    ensemble_probs = dict(ensemble_factor.get("probabilities") or {})
    ensemble_diag = dict(ensemble_factor.get("diagnostics") or {})
    h850_review = dict(forecast.get("h850_review") or {})
    h700 = dict(forecast.get("h700") or {})
    h925 = dict(forecast.get("h925") or {})
    sounding = dict(forecast.get("sounding") or {})
    track_summary = dict(forecast.get("track_summary") or {})
    thermo = dict(sounding.get("thermo") or {})
    coverage = dict(thermo.get("coverage") or {})
    relationships = dict(thermo.get("layer_relationships") or {})

    regime = boundary_layer_regime if isinstance(boundary_layer_regime, dict) else {}
    phase = temp_phase_decision if isinstance(temp_phase_decision, dict) else {}

    latest_temp_c = _safe_float(obs.get("latest_temp_c"))
    observed_max_temp_c = _safe_float(obs.get("observed_max_temp_c"))
    peak_temp_c = _safe_float(primary_window.get("peak_temp_c"))
    latest_report_local = obs.get("latest_report_local")
    peak_local = calc_window.get("peak_local") or primary_window.get("peak_local")
    start_local = calc_window.get("start_local") or primary_window.get("start_local")
    end_local = calc_window.get("end_local") or primary_window.get("end_local")

    dewpoint_c = _safe_float(obs.get("latest_dewpoint_c"))
    cloud_effective_cover = _safe_float(obs.get("cloud_effective_cover"))
    radiation_eff = _safe_float(obs.get("radiation_eff"))
    cloud_trend = str(obs.get("cloud_trend") or "")
    precip_state = str(obs.get("precip_state") or "")
    thermal_advection_state = str(h850_review.get("thermal_advection_state") or "")
    transport_state_label = str(h850_review.get("transport_state") or "")
    surface_bias = str(h850_review.get("surface_bias") or "")
    dewpoint_spread_c = None
    if latest_temp_c is not None and dewpoint_c is not None:
        dewpoint_spread_c = latest_temp_c - dewpoint_c

    gap_to_observed_max_c = None
    if observed_max_temp_c is not None and latest_temp_c is not None:
        gap_to_observed_max_c = observed_max_temp_c - latest_temp_c

    latest_gap_below_observed_c = None
    if observed_max_temp_c is not None and latest_temp_c is not None:
        latest_gap_below_observed_c = max(0.0, observed_max_temp_c - latest_temp_c)

    forecast_peak_minus_latest_c = None
    if peak_temp_c is not None and latest_temp_c is not None:
        forecast_peak_minus_latest_c = peak_temp_c - latest_temp_c

    observed_progress_anchor_c = None
    if observed_max_temp_c is not None and latest_temp_c is not None:
        observed_progress_anchor_c = max(observed_max_temp_c, latest_temp_c)
    elif observed_max_temp_c is not None:
        observed_progress_anchor_c = observed_max_temp_c
    elif latest_temp_c is not None:
        observed_progress_anchor_c = latest_temp_c

    modeled_headroom_c = None
    if peak_temp_c is not None and observed_progress_anchor_c is not None:
        modeled_headroom_c = peak_temp_c - observed_progress_anchor_c

    time_since_observed_peak_h = None
    observed_peak_local = obs.get("observed_max_time_local")
    if observed_peak_local:
        age_h = _hours_between_iso(latest_report_local, observed_peak_local)
        if age_h is not None and age_h >= 0.0:
            time_since_observed_peak_h = age_h

    reports_since_observed_peak = None
    cadence_for_reports = _safe_float(obs.get("metar_routine_cadence_min"))
    if (
        time_since_observed_peak_h is not None
        and cadence_for_reports is not None
        and cadence_for_reports > 0.0
    ):
        reports_since_observed_peak = int(
            max(0.0, (time_since_observed_peak_h * 60.0) / cadence_for_reports)
        )

    ensemble_alignment = _live_ensemble_path_alignment(
        temp_trend_c=_safe_float(obs.get("temp_trend_c")),
        temp_bias_c=_safe_float(obs.get("temp_bias_c")),
        cloud_effective_cover=cloud_effective_cover,
        radiation_eff=radiation_eff,
        cloud_trend=cloud_trend,
        precip_state=precip_state,
        transport_state=transport_state_label,
        thermal_advection_state=thermal_advection_state,
        surface_bias=surface_bias,
        dominant_path=str(ensemble_summary.get("dominant_path") or ""),
        dominant_path_detail=str(ensemble_summary.get("dominant_path_detail") or ensemble_summary.get("transition_detail") or ""),
        dominant_prob=_safe_float(ensemble_summary.get("dominant_prob")),
        dominant_detail_prob=_safe_float(ensemble_summary.get("dominant_detail_prob")),
        dominant_margin_prob=_safe_float(ensemble_summary.get("dominant_margin_prob")),
    )

    return {
        "schema_version": POSTERIOR_FEATURE_VECTOR_SCHEMA_VERSION,
        "unit": str(raw.get("unit") or "C"),
        "meta": {
            "station": str(((forecast.get("meta") or {}).get("station")) or ""),
            "date": str(((forecast.get("meta") or {}).get("date")) or ""),
            "model": str(((forecast.get("meta") or {}).get("model")) or ""),
            "synoptic_provider": str(((forecast.get("meta") or {}).get("synoptic_provider")) or ""),
            "runtime": str(((forecast.get("meta") or {}).get("runtime")) or ""),
        },
        "time_phase": {
            "phase": str(phase.get("phase") or ""),
            "display_phase": str(phase.get("display_phase") or ""),
            "hours_to_window_start": _hours_between_iso(start_local, latest_report_local),
            "hours_to_peak": _hours_between_iso(peak_local, latest_report_local),
            "hours_to_window_end": _hours_between_iso(end_local, latest_report_local),
            "window_width_h": _hours_between_iso(end_local, start_local),
            "analysis_window_mode": str(source.get("analysis_window_mode") or ""),
            "analysis_window_override_active": bool(source.get("analysis_window_override_active")),
        },
        "observation_state": {
            "latest_temp_c": latest_temp_c,
            "observed_max_temp_c": observed_max_temp_c,
            "gap_to_observed_max_c": gap_to_observed_max_c,
            "latest_gap_below_observed_c": latest_gap_below_observed_c,
            "observed_progress_anchor_c": observed_progress_anchor_c,
            "modeled_headroom_c": modeled_headroom_c,
            "time_since_observed_peak_h": time_since_observed_peak_h,
            "reports_since_observed_peak": reports_since_observed_peak,
            "forecast_peak_minus_latest_c": forecast_peak_minus_latest_c,
            "temp_trend_c": _safe_float(obs.get("temp_trend_c")),
            "temp_trend_effective_c": _safe_float(obs.get("temp_trend_effective_c")),
            "temp_bias_c": _safe_float(obs.get("temp_bias_c")),
            "temp_accel_2step_c": _safe_float(obs.get("temp_accel_2step_c")),
            "temp_accel_raw_2step_c": _safe_float(obs.get("temp_accel_raw_2step_c")),
            "peak_lock_confirmed": bool(obs.get("peak_lock_confirmed")),
        },
        "cloud_radiation_state": {
            "latest_cloud_code": str(obs.get("latest_cloud_code") or ""),
            "latest_cloud_lowest_base_ft": _safe_float(obs.get("latest_cloud_lowest_base_ft")),
            "cloud_effective_cover": cloud_effective_cover,
            "radiation_eff": radiation_eff,
            "cloud_trend": cloud_trend,
            "low_cloud_pct_model": _safe_float(primary_window.get("low_cloud_pct")),
        },
        "moisture_stability_state": {
            "latest_rh": _safe_float(obs.get("latest_rh")),
            "dewpoint_spread_c": dewpoint_spread_c,
            "precip_state": precip_state,
            "precip_trend": str(obs.get("precip_trend") or ""),
            "latest_wx": str(obs.get("latest_wx") or ""),
        },
        "mixing_coupling_state": {
            "latest_wspd_kt": _safe_float(obs.get("latest_wspd_kt")),
            "latest_wdir_deg": _safe_float(obs.get("latest_wdir_deg")),
            "wind_dir_change_deg": _safe_float(obs.get("wind_dir_change_deg")),
            "w850_kmh": _safe_float(primary_window.get("w850_kmh")),
            "surface_coupling_state": str(h850_review.get("surface_coupling_state") or ""),
            "mixing_support_score": _safe_float(thermo.get("mixing_support_score")),
            "wind_profile_mix_score": _safe_float(thermo.get("wind_profile_mix_score")),
        },
        "transport_state": {
            "thermal_advection_state": thermal_advection_state,
            "transport_state": transport_state_label,
            "surface_role": str(h850_review.get("surface_role") or ""),
            "surface_bias": surface_bias,
            "surface_effect_weight": _safe_float(h850_review.get("surface_effect_weight")),
            "timing_score": _safe_float(h850_review.get("timing_score")),
            "reach_score": _safe_float(h850_review.get("reach_score")),
            "distance_km": _safe_float(h850_review.get("distance_km")),
        },
        "vertical_structure_state": {
            "profile_source": str(thermo.get("profile_source") or ""),
            "sounding_confidence": str(thermo.get("sounding_confidence") or ""),
            "coverage_density": str(coverage.get("density_class") or ""),
            "h700_source": str(h700.get("source") or ""),
            "h700_scope": str(h700.get("dry_intrusion_scope") or ""),
            "h700_distance_km": _safe_float(h700.get("dry_intrusion_nearest_km")),
            "h700_dry_intrusion_strength": _safe_float(h700.get("dry_intrusion_strength")),
            "h925_coupling_state": str(h925.get("coupling_state") or ""),
            "h925_landing_signal": str(h925.get("landing_signal") or ""),
            "h925_coupling_score": _safe_float(h925.get("coupling_score")),
            "t925_t850_c": _safe_float(thermo.get("t925_t850_c")),
            "rh925_pct": _safe_float(thermo.get("rh925_pct")),
            "rh850_pct": _safe_float(thermo.get("rh850_pct")),
            "rh700_pct": _safe_float(thermo.get("rh700_pct")),
            "midlevel_rh_pct": _safe_float(thermo.get("midlevel_rh_pct")),
            "low_level_cap_score": _safe_float(thermo.get("low_level_cap_score")),
            "midlevel_dry_score": _safe_float(thermo.get("midlevel_dry_score")),
            "midlevel_moist_score": _safe_float(thermo.get("midlevel_moist_score")),
            "thermal_structure": str(relationships.get("thermal_structure") or ""),
            "moisture_layering": str(relationships.get("moisture_layering") or ""),
            "wind_turning_state": str(relationships.get("wind_turning_state") or ""),
            "coupling_chain_state": str(relationships.get("coupling_chain_state") or ""),
        },
        "forecast_shape_state": {
            "shape_type": str(shape_forecast.get("shape_type") or ""),
            "multi_peak_state": str(shape_forecast.get("multi_peak_state") or ""),
            "plateau_state": str(shape_forecast.get("plateau_state") or ""),
            "observed_plateau_state": str(shape_observed.get("plateau_state") or ""),
            "observed_plateau_hold_h": _safe_float(shape_observed.get("hold_duration_hours")),
        },
        "peak_phase_state": {
            "short_term_state": str(phase.get("short_term_state") or ""),
            "daily_peak_state": str(phase.get("daily_peak_state") or ""),
            "second_peak_potential": str(phase.get("second_peak_potential") or ""),
            "rebound_mode": str(phase.get("rebound_mode") or ""),
            "dominant_shape": str(phase.get("dominant_shape") or ""),
            "plateau_hold_state": str(phase.get("plateau_hold_state") or ""),
        },
        "track_state": {
            "track_count": _safe_float(track_summary.get("track_count")),
            "anchor_count": _safe_float(track_summary.get("anchor_count")),
            "main_track_type": str(track_summary.get("main_track_type") or ""),
            "main_track_evolution": str(track_summary.get("main_track_evolution") or ""),
            "main_track_intensity_trend": str(track_summary.get("main_track_intensity_trend") or ""),
            "main_track_distance_km": _safe_float(track_summary.get("main_track_distance_km")),
            "main_track_closest_distance_km": _safe_float(track_summary.get("main_track_closest_distance_km")),
            "main_track_anchors_count": _safe_float(track_summary.get("main_track_anchors_count")),
            "main_track_confidence": str(track_summary.get("main_track_confidence") or ""),
            "main_track_closest_time_local": str(track_summary.get("main_track_closest_time_local") or ""),
        },
        "regime_state": {
            "regime_key": str(regime.get("regime_key") or ""),
            "dominant_mechanism": str(regime.get("dominant_mechanism") or ""),
            "confidence": str(regime.get("confidence") or ""),
            "advection_role": str(regime.get("advection_role") or ""),
            "bottleneck_code": str(context.get("bottleneck_code") or ""),
            "bottleneck_polarity": str(context.get("bottleneck_polarity") or ""),
            "bottleneck_source": str(context.get("bottleneck_source") or ""),
        },
        "quality_state": {
            "source_state": str(quality.get("source_state") or ""),
            "missing_layers": list(quality.get("missing_layers") or []),
            "synoptic_coverage": _safe_float(quality.get("synoptic_coverage")),
            "synoptic_provider_requested": str(quality.get("synoptic_provider_requested") or ""),
            "synoptic_provider_used": str(quality.get("synoptic_provider_used") or ""),
            "synoptic_provider_fallback": bool(quality.get("synoptic_provider_fallback")),
            "metar_temp_quantized": bool(obs.get("metar_temp_quantized")),
            "metar_routine_cadence_min": _safe_float(obs.get("metar_routine_cadence_min")),
            "metar_recent_interval_min": _safe_float(obs.get("metar_recent_interval_min")),
            "metar_prev_interval_min": _safe_float(obs.get("metar_prev_interval_min")),
            "metar_speci_active": bool(obs.get("metar_speci_active")),
            "metar_speci_likely": bool(obs.get("metar_speci_likely")),
        },
        "ensemble_path_state": {
            "provider": str((ensemble_factor.get("source") or {}).get("provider") or ""),
            "member_count": _safe_float(ensemble_factor.get("member_count")),
            "dominant_path": str(ensemble_summary.get("dominant_path") or ""),
            "dominant_path_detail": str(ensemble_summary.get("dominant_path_detail") or ""),
            "dominant_prob": _safe_float(ensemble_summary.get("dominant_prob")),
            "dominant_detail_prob": _safe_float(ensemble_summary.get("dominant_detail_prob")),
            "dominant_margin_prob": _safe_float(ensemble_summary.get("dominant_margin_prob")),
            "split_state": str(ensemble_summary.get("split_state") or ""),
            "signal_dispersion_c": _safe_float(ensemble_summary.get("signal_dispersion_c")),
            "transition_detail": str(ensemble_summary.get("transition_detail") or ""),
            "transition_detail_prob": _safe_float(ensemble_summary.get("transition_detail_prob")),
            "warm_support_prob": _safe_float(ensemble_probs.get("warm_support")),
            "transition_prob": _safe_float(ensemble_probs.get("transition")),
            "cold_suppression_prob": _safe_float(ensemble_probs.get("cold_suppression")),
            "delta_t850_p10_c": _safe_float(ensemble_diag.get("delta_t850_p10_c")),
            "delta_t850_p50_c": _safe_float(ensemble_diag.get("delta_t850_p50_c")),
            "delta_t850_p90_c": _safe_float(ensemble_diag.get("delta_t850_p90_c")),
            "wind850_p50_kmh": _safe_float(ensemble_diag.get("wind850_p50_kmh")),
            "neutral_stable_prob": _safe_float(ensemble_diag.get("neutral_stable_prob")),
            "weak_warm_transition_prob": _safe_float(ensemble_diag.get("weak_warm_transition_prob")),
            "weak_cold_transition_prob": _safe_float(ensemble_diag.get("weak_cold_transition_prob")),
            "observed_path": str(ensemble_alignment.get("observed_path") or ""),
            "observed_path_detail": str(ensemble_alignment.get("observed_path_detail") or ""),
            "observed_path_score": _safe_float(ensemble_alignment.get("observed_path_score")),
            "observed_warm_signal": _safe_float(ensemble_alignment.get("observed_warm_signal")),
            "observed_cold_signal": _safe_float(ensemble_alignment.get("observed_cold_signal")),
            "observed_neutral_signal": _safe_float(ensemble_alignment.get("observed_neutral_signal")),
            "observed_alignment_match_state": str(ensemble_alignment.get("observed_alignment_match_state") or ""),
            "observed_alignment_matches_dominant": bool(ensemble_alignment.get("observed_alignment_matches_dominant")),
            "observed_alignment_exact": bool(ensemble_alignment.get("observed_alignment_exact")),
            "observed_alignment_score": _safe_float(ensemble_alignment.get("observed_alignment_score")),
            "observed_alignment_confidence": str(ensemble_alignment.get("observed_alignment_confidence") or ""),
            "observed_path_locked": bool(ensemble_alignment.get("observed_path_locked")),
        },
    }
