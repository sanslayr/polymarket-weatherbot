#!/usr/bin/env python3
"""Resolve forecast Tmax windows against realtime observations."""

from __future__ import annotations

from datetime import datetime, timedelta
from typing import Any

from condition_state import build_live_condition_signals
from historical_context_provider import get_station_prior


def _safe_float(value: Any) -> float | None:
    try:
        if value in (None, ""):
            return None
        return float(value)
    except Exception:
        return None


def _parse_dt(value: Any) -> datetime | None:
    try:
        dt = datetime.fromisoformat(str(value))
    except Exception:
        return None
    if dt.tzinfo is not None:
        dt = dt.replace(tzinfo=None)
    return dt


def _hour_float(value: Any) -> float | None:
    dt = _parse_dt(value)
    if dt is None:
        return None
    return float(dt.hour + dt.minute / 60.0)


def _format_local(dt: datetime | None) -> str | None:
    if dt is None:
        return None
    return dt.strftime("%Y-%m-%dT%H:%M")


def _contains_any(text: str, keys: tuple[str, ...]) -> bool:
    raw = str(text or "")
    return any(key in raw for key in keys)


def _future_model_peak(
    hourly_day: dict[str, Any],
    latest_local: datetime | None,
) -> tuple[float | None, datetime | None]:
    if latest_local is None:
        return None, None
    times = list(hourly_day.get("time") or [])
    temps = list(hourly_day.get("temperature_2m") or [])
    if not times or len(times) != len(temps):
        return None, None

    future: list[tuple[float, datetime]] = []
    cutoff = latest_local + timedelta(minutes=15)
    for ts, temp in zip(times, temps):
        dt = _parse_dt(ts)
        value = _safe_float(temp)
        if dt is None or value is None:
            continue
        if dt >= cutoff:
            future.append((value, dt))
    if not future:
        return None, None

    peak_temp, peak_dt = max(future, key=lambda item: item[0])
    return peak_temp, peak_dt


def _window_with_peak(
    primary_window: dict[str, Any],
    *,
    peak_dt: datetime | None,
    peak_temp_c: float | None,
    lead_hours: float,
    tail_hours: float,
) -> dict[str, Any]:
    resolved = dict(primary_window or {})
    if peak_dt is None:
        return resolved

    start_dt = peak_dt - timedelta(hours=lead_hours)
    end_dt = peak_dt + timedelta(hours=tail_hours)
    resolved["start_local"] = _format_local(start_dt)
    resolved["peak_local"] = _format_local(peak_dt)
    resolved["end_local"] = _format_local(end_dt)
    if peak_temp_c is not None:
        resolved["peak_temp_c"] = round(float(peak_temp_c), 2)
    return resolved


def _window_with_bounds(
    primary_window: dict[str, Any],
    *,
    start_dt: datetime | None,
    peak_dt: datetime | None,
    end_dt: datetime | None,
    peak_temp_c: float | None,
) -> dict[str, Any]:
    resolved = dict(primary_window or {})
    if start_dt is not None:
        resolved["start_local"] = _format_local(start_dt)
    if peak_dt is not None:
        resolved["peak_local"] = _format_local(peak_dt)
    if end_dt is not None:
        resolved["end_local"] = _format_local(end_dt)
    if peak_temp_c is not None:
        resolved["peak_temp_c"] = round(float(peak_temp_c), 2)
    return resolved


def resolve_temperature_window(
    primary_window: dict[str, Any],
    hourly_day: dict[str, Any],
    metar_diag: dict[str, Any],
    *,
    station_icao: str | None = None,
    temp_shape_analysis: dict[str, Any] | None = None,
) -> dict[str, Any]:
    resolved = dict(primary_window or {})
    station_key = str(
        station_icao
        or metar_diag.get("station_icao")
        or metar_diag.get("icao")
        or ""
    ).upper()
    station_prior = get_station_prior(station_key) if station_key else None
    signals = build_live_condition_signals(metar_diag)

    latest_local_dt = _parse_dt(metar_diag.get("latest_report_local"))
    observed_peak_dt = _parse_dt(
        metar_diag.get("observed_max_time_local") or metar_diag.get("latest_report_local")
    )
    forecast_peak_dt = _parse_dt(primary_window.get("peak_local"))

    latest_temp = _safe_float(metar_diag.get("latest_temp"))
    obs_max = _safe_float(metar_diag.get("observed_max_temp_c"))
    model_peak = _safe_float(primary_window.get("peak_temp_c"))
    future_peak_c, future_peak_dt = _future_model_peak(hourly_day, latest_local_dt)
    shape = temp_shape_analysis if isinstance(temp_shape_analysis, dict) else {}
    shape_forecast = dict(shape.get("forecast") or {})
    shape_observed = dict(shape.get("observed") or {})
    forecast_multi_peak_state = str(shape_forecast.get("multi_peak_state") or "none")
    forecast_plateau_state = str(shape_forecast.get("plateau_state") or "none")
    observed_plateau_state = str(shape_observed.get("plateau_state") or "none")
    shape_future_candidate = dict(shape_forecast.get("future_candidate") or {})
    shape_future_peak_c = _safe_float(shape_future_candidate.get("peak_temp_c"))
    shape_future_peak_dt = _parse_dt(shape_future_candidate.get("peak_local"))

    local_hour = _hour_float(metar_diag.get("latest_report_local"))
    observed_peak_hour = _hour_float(metar_diag.get("observed_max_time_local"))
    forecast_peak_hour = _hour_float(primary_window.get("peak_local"))
    warm_peak_hour_median = _safe_float((station_prior or {}).get("warm_peak_hour_median"))
    warm_peak_hour_p75 = _safe_float((station_prior or {}).get("warm_peak_hour_p75"))
    late_peak_share = _safe_float((station_prior or {}).get("late_peak_share")) or 0.0

    temp_trend_c = _safe_float(signals.get("temp_trend_c")) or 0.0
    cloud_trend = str(signals.get("cloud_trend") or "")
    cloud_code = str(metar_diag.get("latest_cloud_code") or "").upper()
    precip_state = str(signals.get("precip_state") or "none").lower()
    precip_trend = str(signals.get("precip_trend") or "none").lower()

    cloud_opening = _contains_any(cloud_trend, ("开窗", "减弱"))
    cloud_filling = _contains_any(cloud_trend, ("回补", "增加"))
    wet_now = precip_state in {"light", "moderate", "heavy", "convective"} or precip_trend in {
        "new",
        "intensify",
        "steady",
    }
    stable_surface = (not cloud_opening) and (not cloud_filling) and (not wet_now)
    clear_sky = cloud_code in {"CLR", "CAVOK", "SKC", "FEW", "SCT"}

    latest_matches_obs_peak = bool(
        latest_local_dt is not None
        and observed_peak_dt is not None
        and abs((latest_local_dt - observed_peak_dt).total_seconds()) <= 3600.0
    )
    flat_at_obs_peak = bool(
        latest_temp is not None
        and obs_max is not None
        and abs(latest_temp - obs_max) <= 0.1
        and abs(temp_trend_c) <= 0.12
        and latest_matches_obs_peak
    )
    near_climo_peak = bool(
        local_hour is not None
        and (
            warm_peak_hour_median is None
            or local_hour >= (warm_peak_hour_median - 0.5)
        )
    )
    model_tail_delay = bool(
        forecast_peak_hour is not None
        and local_hour is not None
        and (forecast_peak_hour - local_hour) >= 4.0
    )
    model_tail_vs_climo = bool(
        forecast_peak_hour is not None
        and warm_peak_hour_p75 is not None
        and forecast_peak_hour >= (warm_peak_hour_p75 + 2.5)
    )
    observed_peak_far_before_forecast = bool(
        observed_peak_hour is not None
        and forecast_peak_hour is not None
        and observed_peak_hour <= (forecast_peak_hour - 2.0)
    )
    future_gap_vs_obs = None
    if future_peak_c is not None and obs_max is not None:
        future_gap_vs_obs = future_peak_c - obs_max
    shape_future_gap_vs_obs = None
    if shape_future_peak_c is not None and obs_max is not None:
        shape_future_gap_vs_obs = shape_future_peak_c - obs_max
    competitive_future_gap_vs_obs = shape_future_gap_vs_obs if shape_future_gap_vs_obs is not None else future_gap_vs_obs
    competitive_future_peak_dt = shape_future_peak_dt if shape_future_peak_dt is not None else future_peak_dt
    plateau_hold_active = observed_plateau_state in {"holding", "sustained"}
    competitive_gap_cap = 0.45 if plateau_hold_active else 0.35

    result = {
        "override_active": False,
        "mode": "forecast_primary",
        "reason_codes": [],
        "base_window": dict(primary_window or {}),
        "resolved_window": resolved,
        "station": {
            "icao": station_key,
            "warm_peak_hour_median": warm_peak_hour_median,
            "warm_peak_hour_p75": warm_peak_hour_p75,
            "late_peak_share": late_peak_share,
        },
        "realtime": {
            "local_hour": local_hour,
            "observed_peak_hour": observed_peak_hour,
            "forecast_peak_hour": forecast_peak_hour,
            "temp_trend_c": temp_trend_c,
            "future_peak_temp_c": future_peak_c,
            "future_gap_vs_obs": future_gap_vs_obs,
            "competitive_future_gap_vs_obs": competitive_future_gap_vs_obs,
            "flat_at_obs_peak": flat_at_obs_peak,
            "near_climo_peak": near_climo_peak,
            "clear_sky": clear_sky,
            "forecast_multi_peak_state": forecast_multi_peak_state,
            "forecast_plateau_state": forecast_plateau_state,
            "observed_plateau_state": observed_plateau_state,
        },
    }

    if obs_max is not None and model_peak is not None and (obs_max - model_peak) >= 1.5:
        resolved = _window_with_peak(
            primary_window,
            peak_dt=observed_peak_dt or latest_local_dt,
            peak_temp_c=max(obs_max, model_peak),
            lead_hours=1.0,
            tail_hours=2.0,
        )
        result["override_active"] = True
        result["mode"] = "obs_peak_reanchor"
        result["reason_codes"] = ["obs_above_model_peak"]
        result["resolved_window"] = resolved
        return result

    plateau_reanchor = bool(
        (flat_at_obs_peak or plateau_hold_active)
        and near_climo_peak
        and stable_surface
        and (clear_sky or cloud_code in {"CLR", "CAVOK", "SKC"})
        and observed_peak_far_before_forecast
        and (model_tail_delay or model_tail_vs_climo)
        and late_peak_share < 0.55
        and forecast_multi_peak_state != "likely"
        and (competitive_future_gap_vs_obs is None or competitive_future_gap_vs_obs <= competitive_gap_cap)
    )
    if plateau_reanchor:
        peak_cap = obs_max
        future_cap_source = shape_future_peak_c if shape_future_peak_c is not None else future_peak_c
        if future_cap_source is not None and obs_max is not None:
            peak_cap = max(obs_max, min(future_cap_source, obs_max + 0.25))
        if plateau_hold_active and observed_peak_dt is not None and latest_local_dt is not None:
            start_dt = observed_peak_dt - timedelta(minutes=30)
            end_anchor = latest_local_dt if latest_local_dt >= observed_peak_dt else observed_peak_dt
            end_dt = end_anchor + timedelta(hours=1.0)
            resolved = _window_with_bounds(
                primary_window,
                start_dt=start_dt,
                peak_dt=observed_peak_dt,
                end_dt=end_dt,
                peak_temp_c=peak_cap,
            )
        else:
            resolved = _window_with_peak(
                primary_window,
                peak_dt=observed_peak_dt or latest_local_dt,
                peak_temp_c=peak_cap,
                lead_hours=1.0,
                tail_hours=1.0,
            )
        result["override_active"] = True
        result["mode"] = "obs_plateau_reanchor"
        result["reason_codes"] = [
            "obs_flat_at_peak",
            "late_model_tail",
            "late_tail_low_confidence",
        ]
        if plateau_hold_active:
            result["reason_codes"].append("obs_hold_near_peak")
        if forecast_plateau_state in {"narrow", "broad"}:
            result["reason_codes"].append("forecast_plateau_shape")
        if competitive_future_peak_dt is not None:
            result["realtime"]["competitive_future_peak_local"] = _format_local(competitive_future_peak_dt)
        result["resolved_window"] = resolved
        return result

    return result
