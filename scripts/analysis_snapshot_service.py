#!/usr/bin/env python3
"""Structured analysis snapshot builder for /look runtime."""

from __future__ import annotations

import math
from datetime import datetime
from typing import Any

from boundary_layer_regime import build_boundary_layer_regime
from canonical_raw_state_service import build_canonical_raw_state
from condition_state import build_condition_context, build_live_condition_signals
from contracts import ANALYSIS_SNAPSHOT_SCHEMA_VERSION
from param_store import load_tmax_learning_params
from peak_range_render_service import render_peak_range_block
from peak_range_service import build_peak_range_summary
from posterior_feature_service import build_posterior_feature_vector
from quality_snapshot_service import build_quality_snapshot
from synoptic_summary_service import build_synoptic_summary
from temperature_phase_decision import build_temperature_phase_decision
from weather_posterior_service import build_weather_posterior


def load_analysis_runtime_params() -> dict[str, float]:
    lp = load_tmax_learning_params() or {}
    lp_rt = (lp.get("rounded_top") or {}) if isinstance(lp, dict) else {}
    lp_night = (lp.get("nocturnal_rewarm") or {}) if isinstance(lp, dict) else {}
    return {
        "rt_accel_neg": float(lp_rt.get("temp_accel_neg_threshold", -0.25)),
        "rt_flat": float(lp_rt.get("flat_trend_threshold", 0.12)),
        "rt_weak": float(lp_rt.get("weak_trend_threshold", 0.22)),
        "rt_near_peak_h": float(lp_rt.get("near_peak_hours", 1.8)),
        "rt_near_end_h": float(lp_rt.get("near_end_hours", 1.0)),
        "rt_solar_stall": float(lp_rt.get("solar_stalling_slope", 0.012)),
        "rt_solar_rise": float(lp_rt.get("solar_strong_rise_slope", 0.030)),
        "rt_rad_low": float(lp_rt.get("rad_low_threshold", 0.55)),
        "rt_rad_recover": float(lp_rt.get("rad_recover_threshold", 0.72)),
        "rt_rad_recover_tr": float(lp_rt.get("rad_recover_trend", 0.025)),
        "rt_night_solar": float(lp_night.get("night_solar_max", 0.08)),
        "rt_night_hour_start": float(lp_night.get("night_hour_start", 17.5)),
        "rt_night_hour_end": float(lp_night.get("night_hour_end", 7.0)),
        "rt_night_warm_bias": float(lp_night.get("warm_advection_bias_min", 0.45)),
        "rt_night_wind_jump": float(lp_night.get("wind_speed_jump_kt", 3.0)),
        "rt_night_wind_mix_min": float(lp_night.get("wind_speed_mix_min_kt", 7.0)),
        "rt_night_dp_rise": float(lp_night.get("dewpoint_rise_min_c", 0.8)),
        "rt_night_pres_fall": float(lp_night.get("pressure_fall_min_hpa", -0.6)),
        "rt_night_score_min": float(lp_night.get("score_min", 1.5)),
    }


def _build_fmt_range_fn(unit: str):
    def _to_unit(c: float) -> float:
        return (c * 9.0 / 5.0 + 32.0) if unit == "F" else c

    def _fmt_range(lo_c: float, hi_c: float) -> str:
        lo_u = _to_unit(float(lo_c))
        hi_u = _to_unit(float(hi_c))
        return f"{lo_u:.1f}~{hi_u:.1f}°{unit}"

    return _fmt_range


def _solar_clear_score(lat_deg: float, lon_deg: float, dt_local: datetime) -> float:
    tz_off_h = 0.0
    try:
        if dt_local.tzinfo is not None and dt_local.utcoffset() is not None:
            tz_off_h = float(dt_local.utcoffset().total_seconds() / 3600.0)
    except Exception:
        tz_off_h = 0.0

    doy = int(dt_local.timetuple().tm_yday)
    hour = float(dt_local.hour + dt_local.minute / 60.0 + dt_local.second / 3600.0)
    gamma = 2.0 * math.pi / 365.0 * (doy - 1 + (hour - 12.0) / 24.0)

    decl = (
        0.006918
        - 0.399912 * math.cos(gamma)
        + 0.070257 * math.sin(gamma)
        - 0.006758 * math.cos(2 * gamma)
        + 0.000907 * math.sin(2 * gamma)
        - 0.002697 * math.cos(3 * gamma)
        + 0.00148 * math.sin(3 * gamma)
    )
    eqtime = 229.18 * (
        0.000075
        + 0.001868 * math.cos(gamma)
        - 0.032077 * math.sin(gamma)
        - 0.014615 * math.cos(2 * gamma)
        - 0.040849 * math.sin(2 * gamma)
    )

    tst_min = hour * 60.0 + eqtime + 4.0 * float(lon_deg) - 60.0 * tz_off_h
    tst_min = tst_min % 1440.0
    ha_deg = tst_min / 4.0 - 180.0

    lat_rad = math.radians(float(lat_deg))
    ha_rad = math.radians(ha_deg)
    cosz = (
        math.sin(lat_rad) * math.sin(decl)
        + math.cos(lat_rad) * math.cos(decl) * math.cos(ha_rad)
    )
    cosz = max(-1.0, min(1.0, cosz))
    if cosz <= 0.0:
        return 0.0
    return max(0.0, min(1.0, cosz ** 1.15))


def build_analysis_snapshot(
    *,
    primary_window: dict[str, Any],
    metar_diag: dict[str, Any],
    forecast_decision: dict[str, Any] | None = None,
    temp_unit: str = "C",
    synoptic_window: dict[str, Any] | None = None,
    temp_shape_analysis: dict[str, Any] | None = None,
) -> dict[str, Any]:
    unit = "F" if str(temp_unit).upper() == "F" else "C"
    runtime_params = load_analysis_runtime_params()
    state = build_condition_context(
        primary_window=primary_window,
        metar_diag=metar_diag,
        forecast_decision=forecast_decision,
        synoptic_window=synoptic_window,
    )
    signals = build_live_condition_signals(metar_diag)
    canonical_raw_state = build_canonical_raw_state(
        primary_window=primary_window,
        metar_diag=metar_diag,
        forecast_decision=forecast_decision,
        synoptic_window=synoptic_window,
        temp_shape_analysis=temp_shape_analysis,
        temp_unit=unit,
        condition_state=state,
        live_signals=signals,
    )

    boundary_layer_regime = build_boundary_layer_regime(
        primary_window=primary_window,
        metar_diag=metar_diag,
        snd_thermo=state["snd_thermo"],
        advection_review=state["advection_review"],
        h700_summary=state["h700_summary"],
        h925_summary=state["h925_summary"],
        line850=state["line850"],
        extra=state["extra"],
        h500_regime=str((state["h500_feature"] or {}).get("regime_label") or ""),
        object_type=str((state["obj"] or {}).get("type") or ""),
        cloud_code_now=state["cloud_code_now"],
    )

    temp_phase_decision = build_temperature_phase_decision(
        primary_window,
        metar_diag,
        line850=state["line850"],
        advection_review=state["advection_review"],
        temp_shape_analysis=temp_shape_analysis,
    )

    peak_summary = build_peak_range_summary(
        primary_window=primary_window,
        syn_w=state["syn_w"],
        calc_window=state["calc_window"],
        metar_diag=metar_diag,
        quality=state["quality"],
        obj=state["obj"],
        line500=state["line500"],
        h500_feature=state["h500_feature"],
        line850=state["line850"],
        advection_review=state["advection_review"],
        extra=state["extra"],
        h700_summary=state["h700_summary"],
        h925_summary=state["h925_summary"],
        snd_thermo=state["snd_thermo"],
        cloud_code_now=state["cloud_code_now"],
        precip_state=state["precip_state"],
        precip_trend=state["precip_trend"],
        unit=unit,
        rt_accel_neg=runtime_params["rt_accel_neg"],
        rt_flat=runtime_params["rt_flat"],
        rt_weak=runtime_params["rt_weak"],
        rt_near_peak_h=runtime_params["rt_near_peak_h"],
        rt_near_end_h=runtime_params["rt_near_end_h"],
        rt_solar_stall=runtime_params["rt_solar_stall"],
        rt_solar_rise=runtime_params["rt_solar_rise"],
        rt_rad_low=runtime_params["rt_rad_low"],
        rt_rad_recover=runtime_params["rt_rad_recover"],
        rt_rad_recover_tr=runtime_params["rt_rad_recover_tr"],
        rt_night_solar=runtime_params["rt_night_solar"],
        rt_night_hour_start=runtime_params["rt_night_hour_start"],
        rt_night_hour_end=runtime_params["rt_night_hour_end"],
        rt_night_warm_bias=runtime_params["rt_night_warm_bias"],
        rt_night_wind_jump=runtime_params["rt_night_wind_jump"],
        rt_night_wind_mix_min=runtime_params["rt_night_wind_mix_min"],
        rt_night_dp_rise=runtime_params["rt_night_dp_rise"],
        rt_night_pres_fall=runtime_params["rt_night_pres_fall"],
        rt_night_score_min=runtime_params["rt_night_score_min"],
        solar_clear_score_fn=_solar_clear_score,
        temp_phase_decision=temp_phase_decision,
    )
    peak_data = {
        "summary": peak_summary,
        "block": render_peak_range_block(
            peak_summary,
            unit=unit,
            fmt_range_fn=_build_fmt_range_fn(unit),
        ),
    }

    synoptic_summary = build_synoptic_summary(
        primary_window=primary_window,
        metar_diag=metar_diag,
        syn_w=state["syn_w"],
        calc_window=state["calc_window"],
        obj=state["obj"],
        candidates=state["candidates"],
        cov=state["cov"],
        line500=state["line500"],
        h500_feature=state["h500_feature"],
        line850=state["line850"],
        advection_review=state["advection_review"],
        extra=state["extra"],
        h700_summary=state["h700_summary"],
        h925_summary=state["h925_summary"],
        snd_thermo=state["snd_thermo"],
        cloud_code_now=state["cloud_code_now"],
        boundary_layer_regime=boundary_layer_regime,
    )
    posterior_feature_vector = build_posterior_feature_vector(
        canonical_raw_state=canonical_raw_state,
        boundary_layer_regime=boundary_layer_regime,
        temp_phase_decision=temp_phase_decision,
    )
    quality_snapshot = build_quality_snapshot(
        canonical_raw_state=canonical_raw_state,
        posterior_feature_vector=posterior_feature_vector,
    )
    weather_posterior = build_weather_posterior(
        canonical_raw_state=canonical_raw_state,
        posterior_feature_vector=posterior_feature_vector,
        quality_snapshot=quality_snapshot,
    )

    return {
        "schema_version": ANALYSIS_SNAPSHOT_SCHEMA_VERSION,
        "unit": unit,
        "runtime_params": runtime_params,
        "canonical_raw_state": canonical_raw_state,
        "posterior_feature_vector": posterior_feature_vector,
        "quality_snapshot": quality_snapshot,
        "weather_posterior": weather_posterior,
        "condition_state": state,
        "signals": signals,
        "boundary_layer_regime": boundary_layer_regime,
        "temp_phase_decision": temp_phase_decision,
        "peak_data": peak_data,
        "synoptic_summary": synoptic_summary,
    }
