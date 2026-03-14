#!/usr/bin/env python3
"""Canonical raw-state builder for weather-side runtime analysis."""

from __future__ import annotations

from typing import Any

from condition_state import build_condition_context, build_live_condition_signals, safe_float
from contracts import CANONICAL_RAW_STATE_SCHEMA_VERSION


def _window_fields(window: dict[str, Any] | None) -> dict[str, Any]:
    node = window if isinstance(window, dict) else {}
    return {
        "start_local": str(node.get("start_local") or ""),
        "peak_local": str(node.get("peak_local") or ""),
        "end_local": str(node.get("end_local") or ""),
        "window_role": str(node.get("window_role") or ""),
        "peak_temp_c": safe_float(node.get("peak_temp_c")),
        "low_cloud_pct": safe_float(node.get("low_cloud_pct")),
        "w850_kmh": safe_float(node.get("w850_kmh")),
        "resolved_by": str(node.get("resolved_by") or ""),
    }


def _track_summary(objects_3d: dict[str, Any] | None) -> dict[str, Any]:
    node = objects_3d if isinstance(objects_3d, dict) else {}
    main = dict(node.get("main_object") or {})
    return {
        "track_count": int(node.get("count") or 0),
        "anchor_count": int(node.get("anchors_count") or 0),
        "main_track_id": str(main.get("track_id") or main.get("object_id") or ""),
        "main_track_type": str(main.get("type") or ""),
        "main_track_evolution": str(main.get("evolution") or ""),
        "main_track_intensity_trend": str(main.get("intensity_trend") or ""),
        "main_track_distance_km": safe_float(main.get("distance_km_min")),
        "main_track_closest_distance_km": safe_float(main.get("closest_approach_distance_km")),
        "main_track_closest_time_local": str(main.get("closest_approach_time_local") or ""),
        "main_track_anchors_count": int(main.get("anchors_count") or 0),
        "main_track_confidence": str(main.get("confidence") or ""),
    }


def build_canonical_raw_state(
    *,
    primary_window: dict[str, Any],
    metar_diag: dict[str, Any],
    forecast_decision: dict[str, Any] | None = None,
    ensemble_factor: dict[str, Any] | None = None,
    synoptic_window: dict[str, Any] | None = None,
    temp_shape_analysis: dict[str, Any] | None = None,
    temp_unit: str = "C",
    condition_state: dict[str, Any] | None = None,
    live_signals: dict[str, Any] | None = None,
) -> dict[str, Any]:
    state = (
        dict(condition_state)
        if isinstance(condition_state, dict)
        else build_condition_context(
            primary_window=primary_window,
            metar_diag=metar_diag,
            forecast_decision=forecast_decision,
            synoptic_window=synoptic_window,
        )
    )
    signals = (
        dict(live_signals)
        if isinstance(live_signals, dict)
        else build_live_condition_signals(metar_diag)
    )
    fdec = dict(state.get("fdec") or {})
    meta = dict(fdec.get("meta") or {})
    quality = dict(state.get("quality") or {})
    decision = dict((fdec.get("decision") or {}) if isinstance(fdec, dict) else {})
    features = dict(fdec.get("features") or {})
    h500 = dict(features.get("h500") or {})
    h850 = dict(features.get("h850") or {})
    h700 = dict(features.get("h700") or {})
    h925 = dict(features.get("h925") or {})
    sounding = dict(features.get("sounding") or {})
    objects_3d = dict(features.get("objects_3d") or {})

    observations = {
        "latest_report_local": str(metar_diag.get("latest_report_local") or ""),
        "latest_temp_c": safe_float(metar_diag.get("latest_temp")),
        "latest_dewpoint_c": safe_float(metar_diag.get("latest_dewpoint")),
        "latest_rh": safe_float(metar_diag.get("latest_rh")),
        "latest_wspd_kt": safe_float(metar_diag.get("latest_wspd")),
        "latest_wdir_deg": safe_float(metar_diag.get("latest_wdir")),
        "wind_dir_change_deg": safe_float(metar_diag.get("wind_dir_change_deg")),
        "latest_cloud_code": str(metar_diag.get("latest_cloud_code") or "").upper(),
        "latest_cloud_lowest_base_ft": safe_float(metar_diag.get("latest_cloud_lowest_base_ft")),
        "latest_wx": str(metar_diag.get("latest_wx") or ""),
        "cloud_effective_cover": safe_float(signals.get("cloud_effective_cover")),
        "radiation_eff": safe_float(signals.get("radiation_eff")),
        "cloud_trend": str(signals.get("cloud_trend") or ""),
        "precip_state": str(signals.get("precip_state") or "none"),
        "precip_trend": str(signals.get("precip_trend") or "none"),
        "temp_trend_c": safe_float(signals.get("temp_trend_c")),
        "temp_trend_effective_c": safe_float(metar_diag.get("temp_trend_effective_c")),
        "temp_bias_c": safe_float(signals.get("temp_bias_c")),
        "temp_accel_2step_c": (
            safe_float(metar_diag.get("temp_accel_effective_c"))
            if safe_float(metar_diag.get("temp_accel_effective_c")) is not None
            else safe_float(metar_diag.get("temp_accel_2step_c"))
        ),
        "temp_accel_raw_2step_c": safe_float(metar_diag.get("temp_accel_2step_c")),
        "observed_max_temp_c": safe_float(metar_diag.get("observed_max_temp_c")),
        "observed_max_time_local": str(metar_diag.get("observed_max_time_local") or ""),
        "observed_max_interval_lo_c": safe_float(metar_diag.get("observed_max_interval_lo_c")),
        "observed_max_interval_hi_c": safe_float(metar_diag.get("observed_max_interval_hi_c")),
        "metar_temp_quantized": bool(metar_diag.get("metar_temp_quantized")),
        "metar_routine_cadence_min": safe_float(metar_diag.get("metar_routine_cadence_min")),
        "metar_recent_interval_min": safe_float(metar_diag.get("metar_recent_interval_min")),
        "metar_prev_interval_min": safe_float(metar_diag.get("metar_prev_interval_min")),
        "metar_speci_active": bool(metar_diag.get("metar_speci_active")),
        "metar_speci_likely": bool(metar_diag.get("metar_speci_likely")),
        "peak_lock_confirmed": bool(metar_diag.get("peak_lock_confirmed")),
        "latest_pressure_hpa": safe_float(metar_diag.get("latest_pressure_hpa")),
        "pressure_change_hpa": safe_float(metar_diag.get("pressure_change_hpa")),
    }

    forecast = {
        "meta": {
            "station": str(meta.get("station") or ""),
            "date": str(meta.get("date") or ""),
            "model": str(meta.get("model") or ""),
            "synoptic_provider": str(meta.get("synoptic_provider") or ""),
            "runtime": str(meta.get("runtime") or ""),
            "requested_window_start_local": str((meta.get("window") or {}).get("start_local") or ""),
            "requested_window_end_local": str((meta.get("window") or {}).get("end_local") or ""),
        },
        "quality": {
            "source_state": str(quality.get("source_state") or ""),
            "missing_layers": list(quality.get("missing_layers") or []),
            "synoptic_coverage": safe_float(quality.get("synoptic_coverage")),
            "synoptic_provider_requested": str(quality.get("synoptic_provider_requested") or ""),
            "synoptic_provider_used": str(quality.get("synoptic_provider_used") or ""),
            "synoptic_provider_fallback": bool(quality.get("synoptic_provider_fallback")),
            "synoptic_anchors_total": quality.get("synoptic_anchors_total"),
            "synoptic_anchors_ok": quality.get("synoptic_anchors_ok"),
        },
        "context": {
            "main_path": str(decision.get("main_path") or ""),
            "trigger": str(decision.get("trigger") or ""),
            "override_risk": str(decision.get("override_risk") or ""),
            "bottleneck_text": str(decision.get("bottleneck") or ""),
            "bottleneck_code": str((decision.get("context") or {}).get("code") or ""),
            "bottleneck_polarity": str((decision.get("context") or {}).get("polarity") or ""),
            "bottleneck_source": str((decision.get("context") or {}).get("source") or ""),
        },
        "h500": h500,
        "h850_review": dict(h850.get("review") or {}),
        "h700": h700,
        "h925": h925,
        "sounding": sounding,
        "objects_3d": objects_3d,
        "track_summary": _track_summary(objects_3d),
        "ensemble_factor": dict(ensemble_factor or {}),
    }

    return {
        "schema_version": CANONICAL_RAW_STATE_SCHEMA_VERSION,
        "unit": "F" if str(temp_unit).upper() == "F" else "C",
        "window": {
            "primary": _window_fields(primary_window),
            "synoptic": _window_fields(state.get("syn_w") or {}),
            "calc": _window_fields(state.get("calc_window") or {}),
        },
        "observations": observations,
        "forecast": forecast,
        "shape": {
            "forecast": dict((temp_shape_analysis or {}).get("forecast") or {}),
            "observed": dict((temp_shape_analysis or {}).get("observed") or {}),
        },
        "source": {
            "post_focus_window_active": bool(metar_diag.get("post_focus_window_active")),
            "analysis_window_mode": str(metar_diag.get("analysis_window_mode") or ""),
            "analysis_window_override_active": bool(metar_diag.get("analysis_window_override_active")),
            "analysis_window_reason_codes": list(metar_diag.get("analysis_window_reason_codes") or []),
        },
    }
