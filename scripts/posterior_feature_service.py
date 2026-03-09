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
    dewpoint_spread_c = None
    if latest_temp_c is not None and dewpoint_c is not None:
        dewpoint_spread_c = latest_temp_c - dewpoint_c

    gap_to_observed_max_c = None
    if observed_max_temp_c is not None and latest_temp_c is not None:
        gap_to_observed_max_c = observed_max_temp_c - latest_temp_c

    forecast_peak_minus_latest_c = None
    if peak_temp_c is not None and latest_temp_c is not None:
        forecast_peak_minus_latest_c = peak_temp_c - latest_temp_c

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
        },
        "observation_state": {
            "latest_temp_c": latest_temp_c,
            "observed_max_temp_c": observed_max_temp_c,
            "gap_to_observed_max_c": gap_to_observed_max_c,
            "forecast_peak_minus_latest_c": forecast_peak_minus_latest_c,
            "temp_trend_c": _safe_float(obs.get("temp_trend_c")),
            "temp_bias_c": _safe_float(obs.get("temp_bias_c")),
            "temp_accel_2step_c": _safe_float(obs.get("temp_accel_2step_c")),
            "peak_lock_confirmed": bool(obs.get("peak_lock_confirmed")),
        },
        "cloud_radiation_state": {
            "latest_cloud_code": str(obs.get("latest_cloud_code") or ""),
            "latest_cloud_lowest_base_ft": _safe_float(obs.get("latest_cloud_lowest_base_ft")),
            "cloud_effective_cover": _safe_float(obs.get("cloud_effective_cover")),
            "radiation_eff": _safe_float(obs.get("radiation_eff")),
            "cloud_trend": str(obs.get("cloud_trend") or ""),
            "low_cloud_pct_model": _safe_float(primary_window.get("low_cloud_pct")),
        },
        "moisture_stability_state": {
            "latest_rh": _safe_float(obs.get("latest_rh")),
            "dewpoint_spread_c": dewpoint_spread_c,
            "precip_state": str(obs.get("precip_state") or ""),
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
            "thermal_advection_state": str(h850_review.get("thermal_advection_state") or ""),
            "transport_state": str(h850_review.get("transport_state") or ""),
            "surface_role": str(h850_review.get("surface_role") or ""),
            "surface_bias": str(h850_review.get("surface_bias") or ""),
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
            "metar_speci_active": bool(obs.get("metar_speci_active")),
            "metar_speci_likely": bool(obs.get("metar_speci_likely")),
        },
    }
