#!/usr/bin/env python3
"""Realtime temperature phase decision layer for /look rendering."""

from __future__ import annotations

from datetime import datetime
from typing import Any

from advection_review import has_surface_advection_signal
from condition_state import build_live_condition_signals
from historical_context_provider import get_station_prior
from realtime_pipeline import classify_window_phase


TEMP_PHASE_SCHEMA_VERSION = "temperature-phase.v1"


def _safe_float(value: Any) -> float | None:
    try:
        if value in (None, ""):
            return None
        return float(value)
    except Exception:
        return None


def _hour_float(value: Any) -> float | None:
    try:
        dt = datetime.fromisoformat(str(value))
    except Exception:
        return None
    return float(dt.hour + dt.minute / 60.0)


def _contains_any(text: str, keys: tuple[str, ...]) -> bool:
    raw = str(text or "")
    return any(key in raw for key in keys)


def build_temperature_phase_decision(
    primary_window: dict[str, Any],
    metar_diag: dict[str, Any],
    *,
    line850: str = "",
    advection_review: dict[str, Any] | None = None,
    station_icao: str | None = None,
    temp_shape_analysis: dict[str, Any] | None = None,
) -> dict[str, Any]:
    gate = classify_window_phase(primary_window, metar_diag)
    phase = str(gate.get("phase") or "unknown")
    signals = build_live_condition_signals(metar_diag)

    station_key = str(
        station_icao
        or metar_diag.get("station_icao")
        or metar_diag.get("icao")
        or ""
    ).upper()
    station_prior = get_station_prior(station_key) if station_key else None
    shape = temp_shape_analysis if isinstance(temp_shape_analysis, dict) else {}
    shape_forecast = dict(shape.get("forecast") or {})
    shape_observed = dict(shape.get("observed") or {})
    forecast_shape_type = str(shape_forecast.get("shape_type") or "single_peak")
    forecast_multi_peak_state = str(shape_forecast.get("multi_peak_state") or "none")
    forecast_plateau_state = str(shape_forecast.get("plateau_state") or "none")
    future_candidate = dict(shape_forecast.get("future_candidate") or {})
    future_candidate_peak_c = _safe_float(future_candidate.get("peak_temp_c"))
    future_gap_vs_obs = _safe_float(future_candidate.get("gap_vs_observed_c"))
    future_gap_vs_current = _safe_float(future_candidate.get("gap_vs_current_c"))
    observed_plateau_state = str(shape_observed.get("plateau_state") or "none")
    observed_plateau_hold_h = _safe_float(shape_observed.get("hold_duration_hours")) or 0.0

    local_hour = _hour_float(metar_diag.get("latest_report_local"))
    observed_peak_hour = _hour_float(metar_diag.get("observed_max_time_local"))
    observed_max_c = _safe_float(metar_diag.get("observed_max_temp_c"))
    latest_temp_c = _safe_float(metar_diag.get("latest_temp"))
    warm_peak_hour_median = _safe_float((station_prior or {}).get("warm_peak_hour_median"))
    warm_peak_hour_p75 = _safe_float((station_prior or {}).get("warm_peak_hour_p75"))
    late_peak_share = _safe_float((station_prior or {}).get("late_peak_share")) or 0.0
    very_late_peak_share = _safe_float((station_prior or {}).get("very_late_peak_share")) or 0.0
    cloud_break_share = _safe_float((station_prior or {}).get("cloud_break_day_share")) or 0.0

    hours_to_climo_peak = None
    if local_hour is not None and warm_peak_hour_median is not None:
        hours_to_climo_peak = warm_peak_hour_median - local_hour

    before_typical_peak = bool(hours_to_climo_peak is not None and hours_to_climo_peak >= 1.5)
    far_before_typical_peak = bool(hours_to_climo_peak is not None and hours_to_climo_peak >= 3.0)
    after_typical_peak = bool(hours_to_climo_peak is not None and hours_to_climo_peak <= -0.75)

    observed_early_peak = bool(
        observed_peak_hour is not None
        and local_hour is not None
        and warm_peak_hour_median is not None
        and observed_peak_hour <= (warm_peak_hour_median - 2.0)
        and local_hour <= (warm_peak_hour_median - 0.75)
    )
    if future_candidate_peak_c is not None and future_gap_vs_obs is None and observed_max_c is not None:
        future_gap_vs_obs = future_candidate_peak_c - observed_max_c
    if future_candidate_peak_c is not None and future_gap_vs_current is None and latest_temp_c is not None:
        future_gap_vs_current = future_candidate_peak_c - latest_temp_c

    temp_trend_c = _safe_float(signals.get("temp_trend_c")) or 0.0
    temp_bias_c = _safe_float(signals.get("temp_bias_c")) or 0.0
    cloud_trend = str(signals.get("cloud_trend") or "")
    cloud_code = str(metar_diag.get("latest_cloud_code") or "").upper()
    precip_state = str(signals.get("precip_state") or "none").lower()
    precip_trend = str(signals.get("precip_trend") or "none").lower()
    peak_lock_confirmed = bool(metar_diag.get("peak_lock_confirmed"))

    cloud_opening = _contains_any(cloud_trend, ("开窗", "减弱"))
    cloud_filling = _contains_any(cloud_trend, ("增加", "回补"))
    low_cloud_present = cloud_code in {"BKN", "OVC", "VV"}

    precip_active = precip_state in {"light", "moderate", "heavy", "convective"}
    precip_cooling = bool(
        precip_state in {"moderate", "heavy", "convective"}
        or precip_trend in {"new", "intensify"}
    )
    precip_easing = precip_trend in {"weaken", "end"}

    warm_advection = has_surface_advection_signal(advection_review, bias="warm", line850=line850, min_weight=0.28)
    cold_advection = has_surface_advection_signal(advection_review, bias="cold", line850=line850, min_weight=0.28)

    if temp_trend_c >= 0.3 and (cloud_opening or precip_easing or warm_advection):
        short_term_state = "reaccelerating"
    elif temp_trend_c <= -0.25 or peak_lock_confirmed or (cloud_filling and precip_active):
        short_term_state = "fading"
    else:
        short_term_state = "holding"

    second_peak_score = 0.0
    if phase == "far":
        second_peak_score += 0.20
    if phase == "post" and before_typical_peak:
        second_peak_score += 0.45
    if before_typical_peak:
        second_peak_score += 0.90
    if far_before_typical_peak:
        second_peak_score += 0.35
    if late_peak_share >= 0.55:
        second_peak_score += 0.70
    if late_peak_share >= 0.70:
        second_peak_score += 0.25
    if very_late_peak_share >= 0.35:
        second_peak_score += 0.20
    if cloud_break_share >= 0.05:
        second_peak_score += 0.40
    if observed_early_peak:
        second_peak_score += 0.60
    if cloud_opening or precip_easing:
        second_peak_score += 0.55
    if warm_advection and (not precip_cooling):
        second_peak_score += 0.25
    if temp_trend_c >= 0.2:
        second_peak_score += 0.15
    if forecast_multi_peak_state == "possible":
        second_peak_score += 0.35
    elif forecast_multi_peak_state == "likely":
        second_peak_score += 0.75
    if future_candidate_peak_c is not None and future_gap_vs_obs is not None and future_gap_vs_obs >= 0.25:
        second_peak_score += 0.35
    elif future_candidate_peak_c is not None and future_gap_vs_current is not None and future_gap_vs_current >= 0.35:
        second_peak_score += 0.20
    elif future_candidate_peak_c is not None and forecast_plateau_state in {"narrow", "broad"}:
        second_peak_score += 0.12

    if cold_advection:
        second_peak_score -= 0.35
    if low_cloud_present and (not cloud_opening):
        second_peak_score -= 0.20
    if precip_cooling:
        second_peak_score -= 0.35
    if after_typical_peak:
        second_peak_score -= 0.90
    if phase == "post" and (not before_typical_peak):
        second_peak_score -= 0.55
    if observed_plateau_state == "holding" and future_candidate_peak_c is None:
        second_peak_score -= 0.10
    elif observed_plateau_state == "sustained" and future_candidate_peak_c is None:
        second_peak_score -= 0.25
    if forecast_plateau_state == "broad" and future_candidate_peak_c is None:
        second_peak_score -= 0.15

    if second_peak_score >= 1.90:
        second_peak_potential = "high"
    elif second_peak_score >= 1.15:
        second_peak_potential = "moderate"
    elif second_peak_score >= 0.45:
        second_peak_potential = "weak"
    else:
        second_peak_potential = "none"

    lock_score = 0.0
    if phase == "post":
        lock_score += 0.85
    if not before_typical_peak:
        lock_score += 0.75
    if after_typical_peak:
        lock_score += 0.35
    if peak_lock_confirmed:
        lock_score += 0.35
    if temp_trend_c <= -0.20:
        lock_score += 0.20
    if cold_advection:
        lock_score += 0.15
    if low_cloud_present and (not cloud_opening):
        lock_score += 0.10
    if precip_cooling:
        lock_score += 0.15
    if observed_plateau_state == "holding":
        lock_score += 0.20
    elif observed_plateau_state == "sustained":
        lock_score += 0.55
    if forecast_plateau_state == "narrow":
        lock_score += 0.10
    elif forecast_plateau_state == "broad":
        lock_score += 0.22

    if before_typical_peak:
        lock_score -= 1.15
    if observed_early_peak:
        lock_score -= 0.70
    if late_peak_share >= 0.55:
        lock_score -= 0.35
    if second_peak_potential in {"moderate", "high"}:
        lock_score -= 0.95
    elif second_peak_potential == "weak":
        lock_score -= 0.35
    if cloud_opening or precip_easing or warm_advection:
        lock_score -= 0.30
    if forecast_multi_peak_state == "possible":
        lock_score -= 0.20
    elif forecast_multi_peak_state == "likely":
        lock_score -= 0.45
    if future_candidate_peak_c is not None and future_gap_vs_obs is not None and future_gap_vs_obs >= 0.25:
        lock_score -= 0.35

    if (lock_score >= 1.75) and (second_peak_potential == "none"):
        daily_peak_state = "locked"
    elif (lock_score >= 1.10) and (second_peak_potential in {"none", "weak"}):
        daily_peak_state = "lean_locked"
    else:
        daily_peak_state = "open"

    should_use_early_peak_wording = bool(
        short_term_state == "fading"
        and daily_peak_state == "open"
        and (observed_early_peak or before_typical_peak or second_peak_potential in {"moderate", "high"})
    )
    should_keep_second_peak_open = bool(
        daily_peak_state != "locked"
        and second_peak_potential in {"weak", "moderate", "high"}
    )
    should_avoid_lock_wording = bool(daily_peak_state != "locked")
    plateau_hold_state = "none"
    if observed_plateau_state in {"holding", "sustained"}:
        plateau_hold_state = observed_plateau_state
    elif forecast_plateau_state == "broad":
        plateau_hold_state = "forecast_broad"
    elif forecast_plateau_state == "narrow":
        plateau_hold_state = "forecast_narrow"

    plateau_dominant = bool(
        plateau_hold_state != "none"
        and (future_gap_vs_obs is None or future_gap_vs_obs < 0.25)
    )
    if should_keep_second_peak_open:
        if (
            forecast_multi_peak_state in {"possible", "likely"}
            and (future_candidate_peak_c is not None or second_peak_potential in {"moderate", "high"})
            and (not plateau_dominant)
        ):
            rebound_mode = "second_peak"
        elif second_peak_potential in {"moderate", "high"} and (not plateau_dominant) and plateau_hold_state == "none":
            rebound_mode = "second_peak"
        else:
            rebound_mode = "retest"
    else:
        rebound_mode = "none"
    should_prefer_plateau_wording = bool(plateau_hold_state != "none")
    should_discuss_second_peak = bool(
        rebound_mode == "second_peak"
        and (
            forecast_multi_peak_state == "likely"
            or (
                forecast_multi_peak_state == "possible"
                and second_peak_potential in {"moderate", "high"}
            )
            or (
                observed_early_peak
                and before_typical_peak
                and second_peak_potential in {"moderate", "high"}
            )
            or (
                future_gap_vs_obs is not None
                and future_gap_vs_obs >= 0.35
                and second_peak_potential in {"moderate", "high"}
            )
        )
    )
    should_discuss_multi_peak = should_discuss_second_peak

    if should_discuss_second_peak:
        dominant_shape = "multi_peak_watch"
    elif plateau_hold_state in {"holding", "sustained", "forecast_broad"}:
        dominant_shape = "peak_plateau"
    elif rebound_mode == "retest":
        dominant_shape = "retest"
    elif phase == "post" and short_term_state == "fading":
        dominant_shape = "single_peak_tail"
    elif phase == "post" and short_term_state == "holding":
        dominant_shape = "tail_oscillation"
    elif daily_peak_state == "locked":
        dominant_shape = "single_peak_tail"
    else:
        dominant_shape = "single_peak"

    display_phase = phase
    if phase == "post" and should_use_early_peak_wording:
        display_phase = "early_peak_watch"

    reason_codes: list[str] = []
    if before_typical_peak:
        reason_codes.append("before_typical_peak")
    if observed_early_peak:
        reason_codes.append("observed_early_peak")
    if late_peak_share >= 0.55:
        reason_codes.append("late_peak_station")
    if cloud_break_share >= 0.05:
        reason_codes.append("cloud_break_station")
    if cloud_opening:
        reason_codes.append("cloud_opening")
    if precip_easing:
        reason_codes.append("precip_easing")
    if low_cloud_present and (not cloud_opening):
        reason_codes.append("low_cloud_persistent")
    if precip_cooling:
        reason_codes.append("precip_cooling")
    if cold_advection:
        reason_codes.append("cold_advection")
    if warm_advection:
        reason_codes.append("warm_advection")
    if forecast_multi_peak_state != "none":
        reason_codes.append(f"forecast_multi_peak_{forecast_multi_peak_state}")
    if forecast_plateau_state == "broad":
        reason_codes.append("forecast_broad_plateau")
    elif forecast_plateau_state == "narrow":
        reason_codes.append("forecast_near_peak_plateau")
    if observed_plateau_state == "holding":
        reason_codes.append("obs_holding_near_peak")
    elif observed_plateau_state == "sustained":
        reason_codes.append("obs_sustained_near_peak")

    return {
        "schema_version": TEMP_PHASE_SCHEMA_VERSION,
        "gate": gate,
        "phase": phase,
        "display_phase": display_phase,
        "station": {
            "icao": station_key,
            "warm_peak_hour_median": warm_peak_hour_median,
            "warm_peak_hour_p75": warm_peak_hour_p75,
            "late_peak_share": late_peak_share,
            "very_late_peak_share": very_late_peak_share,
            "cloud_break_day_share": cloud_break_share,
        },
        "timing": {
            "local_hour": local_hour,
            "observed_peak_hour": observed_peak_hour,
            "hours_to_climo_peak": hours_to_climo_peak,
            "before_typical_peak": before_typical_peak,
            "after_typical_peak": after_typical_peak,
            "observed_early_peak": observed_early_peak,
        },
        "signals": {
            "temp_trend_c": temp_trend_c,
            "temp_bias_c": temp_bias_c,
            "peak_lock_confirmed": peak_lock_confirmed,
            "cloud_opening": cloud_opening,
            "cloud_filling": cloud_filling,
            "low_cloud_present": low_cloud_present,
            "precip_active": precip_active,
            "precip_cooling": precip_cooling,
            "precip_easing": precip_easing,
            "warm_advection": warm_advection,
            "cold_advection": cold_advection,
        },
        "shape": {
            "forecast_shape_type": forecast_shape_type,
            "forecast_multi_peak_state": forecast_multi_peak_state,
            "forecast_plateau_state": forecast_plateau_state,
            "observed_plateau_state": observed_plateau_state,
            "observed_plateau_hold_h": observed_plateau_hold_h,
            "future_candidate_peak_c": future_candidate_peak_c,
            "future_gap_vs_obs": future_gap_vs_obs,
            "future_gap_vs_current": future_gap_vs_current,
        },
        "short_term_state": short_term_state,
        "daily_peak_state": daily_peak_state,
        "second_peak_potential": second_peak_potential,
        "second_peak_score": round(second_peak_score, 2),
        "lock_score": round(lock_score, 2),
        "should_use_early_peak_wording": should_use_early_peak_wording,
        "should_keep_second_peak_open": should_keep_second_peak_open,
        "should_avoid_lock_wording": should_avoid_lock_wording,
        "rebound_mode": rebound_mode,
        "dominant_shape": dominant_shape,
        "plateau_hold_state": plateau_hold_state,
        "should_prefer_plateau_wording": should_prefer_plateau_wording,
        "should_discuss_second_peak": should_discuss_second_peak,
        "should_discuss_multi_peak": should_discuss_multi_peak,
        "reason_codes": reason_codes,
    }
