#!/usr/bin/env python3
"""Analyze forecast temperature curve shape and near-peak hold states."""

from __future__ import annotations

from datetime import datetime
from typing import Any

from window_phase_engine import hour_score


TEMP_SHAPE_SCHEMA_VERSION = "temperature-shape.v1"


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


def _format_local(value: Any) -> str | None:
    dt = _parse_dt(value)
    if dt is None:
        return None
    return dt.strftime("%Y-%m-%dT%H:%M")


def _hours_between(a: datetime | None, b: datetime | None) -> float | None:
    if a is None or b is None:
        return None
    return (a - b).total_seconds() / 3600.0


def _shape_thresholds(temps: list[float]) -> dict[str, float]:
    if not temps:
        return {
            "range_c": 0.0,
            "turn_eps": 0.08,
            "window_band_c": 0.40,
            "near_top_eps": 0.22,
            "merge_dip_c": 0.20,
            "multi_peak_dip_c": 0.30,
            "secondary_gap_c": 0.50,
        }

    temp_range = max(temps) - min(temps)
    return {
        "range_c": round(temp_range, 2),
        "turn_eps": max(0.06, min(0.12, 0.03 * temp_range + 0.05)),
        "window_band_c": max(0.35, min(0.70, 0.12 * temp_range + 0.30)),
        "near_top_eps": max(0.20, min(0.45, 0.08 * temp_range + 0.18)),
        "merge_dip_c": max(0.18, min(0.32, 0.05 * temp_range + 0.16)),
        "multi_peak_dip_c": max(0.28, min(0.55, 0.10 * temp_range + 0.24)),
        "secondary_gap_c": max(0.45, min(0.90, 0.14 * temp_range + 0.40)),
    }


def _classify_diff(value: float, eps: float) -> int:
    if value > eps:
        return 1
    if value < -eps:
        return -1
    return 0


def _last_nonzero(values: list[int], start: int) -> int:
    for idx in range(start, -1, -1):
        if values[idx] != 0:
            return values[idx]
    return 0


def _next_nonzero(values: list[int], start: int) -> int:
    for idx in range(start, len(values)):
        if values[idx] != 0:
            return values[idx]
    return 0


def _detect_peak_clusters(
    temps: list[float],
    *,
    turn_eps: float,
    merge_dip_c: float,
) -> list[list[int]]:
    n = len(temps)
    if n == 0:
        return []
    if n == 1:
        return [[0]]

    diffs = [_classify_diff(temps[i + 1] - temps[i], turn_eps) for i in range(n - 1)]
    raw_peaks: list[int] = []
    for idx in range(n):
        left_sign = _last_nonzero(diffs, idx - 1) if idx - 1 >= 0 else 0
        right_sign = _next_nonzero(diffs, idx) if idx < len(diffs) else 0
        if (left_sign > 0 and right_sign < 0) or (idx == 0 and right_sign < 0) or (idx == n - 1 and left_sign > 0):
            raw_peaks.append(idx)

    if not raw_peaks:
        peak_idx = max(range(n), key=lambda i: temps[i])
        return [[int(peak_idx)]]

    merged: list[list[int]] = []
    for idx in raw_peaks:
        if not merged:
            merged.append([idx])
            continue
        prev_idx = merged[-1][-1]
        valley = min(temps[prev_idx:idx + 1])
        valley_dip = min(temps[prev_idx], temps[idx]) - valley
        if (idx - prev_idx) <= 2 and valley_dip <= merge_dip_c:
            merged[-1].append(idx)
        else:
            merged.append([idx])
    return merged


def _build_window_from_cluster(
    hourly_day: dict[str, Any],
    temps: list[float],
    cluster: list[int],
    *,
    band_c: float,
) -> tuple[dict[str, Any], int, float]:
    times = list(hourly_day.get("time") or [])
    cluster_peak_temp = max(temps[idx] for idx in cluster)
    cluster_peak_indices = [idx for idx in cluster if abs(temps[idx] - cluster_peak_temp) <= 0.01]
    peak_idx = cluster_peak_indices[len(cluster_peak_indices) // 2]

    start_idx = min(cluster)
    end_idx = max(cluster)
    while start_idx - 1 >= 0 and temps[start_idx - 1] >= cluster_peak_temp - band_c:
        start_idx -= 1
    while end_idx + 1 < len(temps) and temps[end_idx + 1] >= cluster_peak_temp - band_c:
        end_idx += 1

    window_peak_idx = max(range(start_idx, end_idx + 1), key=lambda idx: temps[idx])
    window = {
        "start_local": _format_local(times[start_idx]) if start_idx < len(times) else None,
        "end_local": _format_local(times[end_idx]) if end_idx < len(times) else None,
        "peak_local": _format_local(times[window_peak_idx]) if window_peak_idx < len(times) else None,
        "peak_temp_c": round(float(temps[window_peak_idx]), 2),
        "peak_index": int(window_peak_idx),
        "start_index": int(start_idx),
        "end_index": int(end_idx),
    }

    for key in ("temperature_850hPa", "wind_speed_850hPa", "wind_direction_850hPa", "cloud_cover_low", "pressure_msl"):
        series = list(hourly_day.get(key) or [])
        if len(series) == len(temps):
            value = _safe_float(series[window_peak_idx])
            if value is not None:
                if key == "temperature_850hPa":
                    window["t850_c"] = value
                elif key == "wind_speed_850hPa":
                    window["w850_kmh"] = value
                elif key == "wind_direction_850hPa":
                    window["wd850_deg"] = value
                elif key == "cloud_cover_low":
                    window["low_cloud_pct"] = value
                elif key == "pressure_msl":
                    window["pmsl_hpa"] = value

    return window, peak_idx, cluster_peak_temp


def _longest_run_hours(temps: list[float], threshold: float) -> float:
    longest = 0
    current = 0
    for value in temps:
        if value >= threshold:
            current += 1
            longest = max(longest, current)
        else:
            current = 0
    return float(longest)


def analyze_temperature_shape(
    hourly_day: dict[str, Any],
    *,
    metar_diag: dict[str, Any] | None = None,
    station_icao: str | None = None,
) -> dict[str, Any]:
    times = list(hourly_day.get("time") or [])
    temps = [_safe_float(value) for value in list(hourly_day.get("temperature_2m") or [])]
    if not times or len(times) != len(temps) or any(value is None for value in temps):
        return {
            "schema_version": TEMP_SHAPE_SCHEMA_VERSION,
            "station": {"icao": str(station_icao or "").upper()},
            "forecast": {
                "shape_type": "single_peak",
                "plateau_state": "none",
                "multi_peak_state": "none",
                "windows": [],
                "candidates": [],
                "primary_window": {},
                "future_candidate": None,
            },
            "observed": {
                "plateau_state": "none",
                "hold_duration_hours": None,
                "obs_gap_to_peak_c": None,
                "latest_matches_obs_peak": False,
                "flat_trend": False,
            },
            "reason_codes": [],
        }

    t2m = [float(value) for value in temps if value is not None]
    thresholds = _shape_thresholds(t2m)
    peak_clusters = _detect_peak_clusters(
        t2m,
        turn_eps=float(thresholds["turn_eps"]),
        merge_dip_c=float(thresholds["merge_dip_c"]),
    )

    candidate_meta: list[dict[str, Any]] = []
    for cluster in peak_clusters:
        window, peak_idx, cluster_peak_temp = _build_window_from_cluster(
            hourly_day,
            t2m,
            cluster,
            band_c=float(thresholds["window_band_c"]),
        )
        raw_score, score_factors = hour_score(hourly_day, peak_idx)
        candidate_meta.append(
            {
                "cluster": list(cluster),
                "cluster_kind": "plateau_peak" if len(cluster) > 1 else "local_peak",
                "peak_index": int(peak_idx),
                "peak_local": window.get("peak_local"),
                "peak_temp_c": round(float(cluster_peak_temp), 2),
                "window": window,
                "window_hours": float(max(1, int(window["end_index"]) - int(window["start_index"]) + 1)),
                "near_top_hours": 0.0,
                "temp_gap_to_global_c": 0.0,
                "dip_from_prev_c": None,
                "dip_to_next_c": None,
                "combined_score": 0.0,
                "hour_score": round(float(raw_score), 3),
                "score_factors": score_factors,
            }
        )

    candidate_meta.sort(key=lambda item: item["peak_index"])
    global_peak_c = max(t2m)
    global_threshold = float(global_peak_c - thresholds["near_top_eps"])

    for idx, candidate in enumerate(candidate_meta):
        peak_idx = int(candidate["peak_index"])
        peak_temp = float(candidate["peak_temp_c"])
        start_idx = int(candidate["window"]["start_index"])
        end_idx = int(candidate["window"]["end_index"])
        candidate["near_top_hours"] = float(
            sum(1 for value in t2m[start_idx:end_idx + 1] if value >= peak_temp - thresholds["near_top_eps"])
        )
        candidate["temp_gap_to_global_c"] = round(float(global_peak_c - peak_temp), 2)

        if idx > 0:
            prev_peak_idx = int(candidate_meta[idx - 1]["peak_index"])
            valley = min(t2m[prev_peak_idx:peak_idx + 1])
            candidate["dip_from_prev_c"] = round(float(min(t2m[prev_peak_idx], peak_temp) - valley), 2)
        if idx + 1 < len(candidate_meta):
            next_peak_idx = int(candidate_meta[idx + 1]["peak_index"])
            valley = min(t2m[peak_idx:next_peak_idx + 1])
            candidate["dip_to_next_c"] = round(float(min(t2m[next_peak_idx], peak_temp) - valley), 2)

        temp_closeness = max(
            0.0,
            min(1.0, 1.0 - (float(candidate["temp_gap_to_global_c"]) / max(0.30, thresholds["secondary_gap_c"]))),
        )
        prominence = max(
            0.0,
            float(candidate["dip_from_prev_c"] or 0.0),
            float(candidate["dip_to_next_c"] or 0.0),
        )
        prominence_norm = max(
            0.0,
            min(1.0, prominence / max(0.18, float(thresholds["multi_peak_dip_c"]))),
        )
        hour_score_norm = max(0.0, min(1.0, float(candidate["hour_score"]) / 1.20))
        candidate["combined_score"] = round(
            0.52 * temp_closeness + 0.33 * hour_score_norm + 0.15 * prominence_norm,
            3,
        )

    ranked_candidates = sorted(
        candidate_meta,
        key=lambda item: (
            float(item["combined_score"]),
            float(item["peak_temp_c"]),
            float(item["hour_score"]),
        ),
        reverse=True,
    )

    primary_candidate = ranked_candidates[0] if ranked_candidates else None
    primary_peak_idx = int(primary_candidate["peak_index"]) if primary_candidate else None
    primary_peak_temp = float(primary_candidate["peak_temp_c"]) if primary_candidate else None

    multi_peak_state = "none"
    credible_secondary: list[dict[str, Any]] = []
    if primary_candidate is not None:
        for candidate in ranked_candidates[1:]:
            peak_idx = int(candidate["peak_index"])
            temp_gap = float(primary_peak_temp - float(candidate["peak_temp_c"])) if primary_peak_temp is not None else None
            valley = min(t2m[min(primary_peak_idx, peak_idx):max(primary_peak_idx, peak_idx) + 1])
            valley_dip = round(float(min(primary_peak_temp, float(candidate["peak_temp_c"])) - valley), 2)
            gap_hours = abs(peak_idx - primary_peak_idx)
            candidate["temp_gap_to_primary_c"] = round(float(temp_gap or 0.0), 2)
            candidate["gap_hours_to_primary"] = float(gap_hours)
            candidate["valley_dip_to_primary_c"] = valley_dip
            if temp_gap is None or gap_hours < 2:
                continue
            if temp_gap > float(thresholds["secondary_gap_c"]):
                continue
            if valley_dip < min(0.22, float(thresholds["multi_peak_dip_c"]) * 0.75) and gap_hours < 4:
                continue
            credible_secondary.append(candidate)

    if credible_secondary:
        strongest_secondary = credible_secondary[0]
        temp_gap = float(strongest_secondary.get("temp_gap_to_primary_c") or 0.0)
        valley_dip = float(strongest_secondary.get("valley_dip_to_primary_c") or 0.0)
        gap_hours = float(strongest_secondary.get("gap_hours_to_primary") or 0.0)
        if temp_gap <= 0.45 and valley_dip >= max(0.28, float(thresholds["multi_peak_dip_c"]) * 0.9) and gap_hours >= 3:
            multi_peak_state = "likely"
        else:
            multi_peak_state = "possible"

    near_top_hours = _longest_run_hours(t2m, global_threshold)
    plateau_state = "none"
    if near_top_hours >= 4.0:
        plateau_state = "broad"
    elif near_top_hours >= 3.0:
        plateau_state = "narrow"

    shape_type = "single_peak"
    if plateau_state == "broad":
        shape_type = "broad_plateau"
    elif multi_peak_state in {"possible", "likely"}:
        shape_type = "multi_peak"
    elif plateau_state == "narrow":
        shape_type = "plateau_peak"

    latest_local_dt = _parse_dt((metar_diag or {}).get("latest_report_local"))
    obs_max = _safe_float((metar_diag or {}).get("observed_max_temp_c"))
    latest_temp = _safe_float((metar_diag or {}).get("latest_temp"))

    future_candidate = None
    if latest_local_dt is not None:
        future_ranked = []
        for candidate in ranked_candidates:
            peak_dt = _parse_dt(candidate.get("peak_local"))
            if peak_dt is None:
                continue
            if peak_dt <= latest_local_dt:
                continue
            enriched = dict(candidate)
            enriched["hours_ahead"] = round(float(_hours_between(peak_dt, latest_local_dt) or 0.0), 2)
            if obs_max is not None:
                enriched["gap_vs_observed_c"] = round(float(candidate["peak_temp_c"]) - obs_max, 2)
            else:
                enriched["gap_vs_observed_c"] = None
            if latest_temp is not None:
                enriched["gap_vs_current_c"] = round(float(candidate["peak_temp_c"]) - latest_temp, 2)
            else:
                enriched["gap_vs_current_c"] = None
            enriched["candidate_role"] = (
                "primary_remaining_peak"
                if primary_candidate is not None and int(candidate["peak_index"]) == int(primary_candidate["peak_index"])
                else "secondary_peak_candidate"
            )
            future_ranked.append(enriched)
        if future_ranked:
            future_candidate = future_ranked[0]

    observed_peak_dt = _parse_dt((metar_diag or {}).get("observed_max_time_local"))
    latest_matches_obs_peak = bool(
        latest_local_dt is not None
        and observed_peak_dt is not None
        and abs((latest_local_dt - observed_peak_dt).total_seconds()) <= 3600.0
    )
    temp_trend_c = _safe_float((metar_diag or {}).get("temp_trend_effective_c"))
    if temp_trend_c is None:
        temp_trend_c = _safe_float((metar_diag or {}).get("temp_trend_smooth_c"))
    if temp_trend_c is None:
        temp_trend_c = _safe_float((metar_diag or {}).get("temp_trend_1step_c"))
    flat_trend = bool(temp_trend_c is not None and abs(temp_trend_c) <= 0.15)

    hold_duration_hours = None
    if latest_local_dt is not None and observed_peak_dt is not None:
        hold_duration_hours = max(0.0, (latest_local_dt - observed_peak_dt).total_seconds() / 3600.0)

    obs_gap_to_peak_c = None
    if obs_max is not None and latest_temp is not None:
        obs_gap_to_peak_c = round(float(obs_max - latest_temp), 2)

    observed_plateau_state = "none"
    if (
        hold_duration_hours is not None
        and hold_duration_hours >= 1.5
        and obs_gap_to_peak_c is not None
        and obs_gap_to_peak_c <= 0.20
        and flat_trend
    ):
        observed_plateau_state = "sustained"
    elif (
        hold_duration_hours is not None
        and hold_duration_hours >= 0.75
        and obs_gap_to_peak_c is not None
        and obs_gap_to_peak_c <= 0.30
        and temp_trend_c is not None
        and abs(temp_trend_c) <= 0.20
    ):
        observed_plateau_state = "holding"
    elif latest_matches_obs_peak and obs_gap_to_peak_c is not None and obs_gap_to_peak_c <= 0.12 and flat_trend:
        observed_plateau_state = "holding"

    reason_codes: list[str] = []
    if multi_peak_state != "none":
        reason_codes.append(f"forecast_multi_peak_{multi_peak_state}")
    if plateau_state == "broad":
        reason_codes.append("forecast_broad_plateau")
    elif plateau_state == "narrow":
        reason_codes.append("forecast_near_peak_plateau")
    if observed_plateau_state == "holding":
        reason_codes.append("obs_holding_near_peak")
    elif observed_plateau_state == "sustained":
        reason_codes.append("obs_sustained_near_peak")

    forecast_candidates: list[dict[str, Any]] = []
    for rank, candidate in enumerate(ranked_candidates, start=1):
        payload = {
            "rank": rank,
            "cluster_kind": candidate["cluster_kind"],
            "score": float(candidate["combined_score"]),
            "hour_score": float(candidate["hour_score"]),
            "window_hours": float(candidate["window_hours"]),
            "near_top_hours": float(candidate["near_top_hours"]),
            "peak_index": int(candidate["peak_index"]),
            "peak_local": candidate.get("peak_local"),
            "peak_temp_c": float(candidate["peak_temp_c"]),
            "temp_gap_to_global_c": float(candidate["temp_gap_to_global_c"]),
            "temp_gap_to_primary_c": _safe_float(candidate.get("temp_gap_to_primary_c")),
            "gap_hours_to_primary": _safe_float(candidate.get("gap_hours_to_primary")),
            "valley_dip_to_primary_c": _safe_float(candidate.get("valley_dip_to_primary_c")),
            "window": dict(candidate["window"]),
            "score_factors": dict(candidate.get("score_factors") or {}),
        }
        if latest_local_dt is not None:
            peak_dt = _parse_dt(candidate.get("peak_local"))
            payload["is_future"] = bool(peak_dt is not None and peak_dt > latest_local_dt)
        forecast_candidates.append(payload)

    forecast_windows = [dict(candidate["window"]) for candidate in ranked_candidates]
    primary_window = dict(primary_candidate["window"]) if primary_candidate is not None else {}

    return {
        "schema_version": TEMP_SHAPE_SCHEMA_VERSION,
        "station": {"icao": str(station_icao or (metar_diag or {}).get("station_icao") or "").upper()},
        "forecast": {
            "shape_type": shape_type,
            "plateau_state": plateau_state,
            "multi_peak_state": multi_peak_state,
            "day_range_c": float(thresholds["range_c"]),
            "global_peak_temp_c": round(float(global_peak_c), 2),
            "near_top_hours": float(near_top_hours),
            "thresholds": {k: round(float(v), 3) for k, v in thresholds.items()},
            "windows": forecast_windows,
            "candidates": forecast_candidates,
            "primary_window": primary_window,
            "future_candidate": future_candidate,
        },
        "observed": {
            "plateau_state": observed_plateau_state,
            "hold_duration_hours": round(float(hold_duration_hours), 2) if hold_duration_hours is not None else None,
            "obs_gap_to_peak_c": obs_gap_to_peak_c,
            "latest_matches_obs_peak": latest_matches_obs_peak,
            "flat_trend": flat_trend,
        },
        "reason_codes": reason_codes,
    }
