#!/usr/bin/env python3
"""Shared normalized condition state for /look runtime modules."""

from __future__ import annotations

from datetime import datetime
from typing import Any


def safe_float(value: Any) -> float | None:
    try:
        if value in (None, ""):
            return None
        return float(value)
    except Exception:
        return None


def extract_hour(value: Any) -> int | None:
    try:
        return datetime.fromisoformat(str(value)).hour
    except Exception:
        return None


def extract_minute(value: Any) -> int | None:
    try:
        return datetime.fromisoformat(str(value)).minute
    except Exception:
        return None


def kt_to_ms(value: float | None) -> float | None:
    if value is None:
        return None
    return value * 0.514444


def pick_smoothed(metar_diag: dict[str, Any], smooth_key: str, raw_key: str) -> float | None:
    smoothed = safe_float(metar_diag.get(smooth_key))
    if smoothed is not None:
        return smoothed
    return safe_float(metar_diag.get(raw_key))


def build_live_condition_signals(metar_diag: dict[str, Any]) -> dict[str, Any]:
    wind_speed_kt = safe_float(metar_diag.get("latest_wspd"))
    latest_report_local = metar_diag.get("latest_report_local")
    return {
        "cloud_effective_cover": pick_smoothed(metar_diag, "cloud_effective_cover_smooth", "cloud_effective_cover"),
        "radiation_eff": pick_smoothed(metar_diag, "radiation_eff_smooth", "radiation_eff"),
        "temp_trend_c": pick_smoothed(metar_diag, "temp_trend_smooth_c", "temp_trend_1step_c"),
        "temp_bias_c": pick_smoothed(metar_diag, "temp_bias_smooth_c", "temp_bias_c"),
        "dewpoint_c": safe_float(metar_diag.get("latest_dewpoint")),
        "latest_temp_c": safe_float(metar_diag.get("latest_temp")),
        "latest_rh": safe_float(metar_diag.get("latest_rh")),
        "wind_dir_change_deg": safe_float(metar_diag.get("wind_dir_change_deg")),
        "latest_wspd_kt": wind_speed_kt,
        "latest_wspd_ms": kt_to_ms(wind_speed_kt),
        "latest_wdir_deg": safe_float(metar_diag.get("latest_wdir")),
        "cloud_trend": str(metar_diag.get("cloud_trend") or ""),
        "precip_state": str(metar_diag.get("latest_precip_state") or "none").lower(),
        "precip_trend": str(metar_diag.get("precip_trend") or "none").lower(),
        "latest_report_local": latest_report_local,
        "local_hour": extract_hour(latest_report_local),
        "local_minute": extract_minute(latest_report_local),
    }


def build_condition_context(
    primary_window: dict[str, Any],
    metar_diag: dict[str, Any],
    forecast_decision: dict[str, Any] | None,
    synoptic_window: dict[str, Any] | None,
) -> dict[str, Any]:
    fdec = forecast_decision if isinstance(forecast_decision, dict) else {}
    d = (fdec.get("decision") or {}) if isinstance(fdec, dict) else {}
    bg = (d.get("background") or fdec.get("background") or {}) if isinstance(fdec, dict) else {}
    quality = (fdec.get("quality") or {}) if isinstance(fdec, dict) else {}

    meta_window = (((fdec.get("meta") or {}).get("window")) if isinstance(fdec, dict) else {}) or {}
    if isinstance(synoptic_window, dict) and synoptic_window.get("start_local") and synoptic_window.get("end_local"):
        syn_w = synoptic_window
    elif isinstance(meta_window, dict) and meta_window.get("start_local") and meta_window.get("end_local"):
        syn_w = meta_window
    else:
        syn_w = primary_window

    post_focus_active = bool(metar_diag.get("post_focus_window_active"))
    calc_window = syn_w if post_focus_active else primary_window

    line500 = str(bg.get("line_500") or "高空背景信号有限。")
    line850 = str(bg.get("line_850") or "低层输送信号一般。")
    extra = str(bg.get("extra") or "")
    h500_feature = dict((((fdec.get("features") or {}).get("h500") or {}) if isinstance(fdec, dict) else {}) or {})
    advection_review = dict((((fdec.get("features") or {}).get("h850") or {}).get("review") if isinstance(fdec, dict) else {}) or {})
    h700_summary = str((((fdec.get("features") or {}).get("h700") or {}).get("summary") if isinstance(fdec, dict) else "") or "")
    h925_summary = str((((fdec.get("features") or {}).get("h925") or {}).get("summary") if isinstance(fdec, dict) else "") or "")
    snd_thermo = ((((fdec.get("features") or {}).get("sounding") or {}).get("thermo") if isinstance(fdec, dict) else None) or {})
    cloud_code_now = str(metar_diag.get("latest_cloud_code") or "").upper()
    precip_state = str(metar_diag.get("latest_precip_state") or "none").lower()
    precip_trend = str(metar_diag.get("precip_trend") or "none").lower()
    candidates = (((fdec.get("features") or {}).get("objects_3d") or {}).get("candidates") or []) if isinstance(fdec, dict) else []

    try:
        cov = float((quality or {}).get("synoptic_coverage")) if (quality or {}).get("synoptic_coverage") is not None else None
    except Exception:
        cov = None

    def _conf_ord(value: str) -> int:
        return {"high": 3, "medium": 2, "low": 1}.get(str(value or "").lower(), 0)

    raw_obj = (d.get("object_3d_main") or {}) if isinstance(d, dict) else {}
    obj = dict(raw_obj) if isinstance(raw_obj, dict) else {}
    if cov is not None and cov < 0.5:
        obj = {}
    elif obj and _conf_ord(obj.get("confidence")) <= 1:
        alt = None
        for candidate in candidates:
            if not isinstance(candidate, dict):
                continue
            if _conf_ord(candidate.get("confidence")) >= 2:
                alt = candidate
                break
        if alt is not None:
            obj = dict(alt)
            obj["_promoted_from_candidate"] = True
        else:
            obj = {}

    return {
        "fdec": fdec,
        "d": d,
        "quality": quality,
        "syn_w": syn_w,
        "calc_window": calc_window,
        "line500": line500,
        "line850": line850,
        "extra": extra,
        "h500_feature": h500_feature,
        "advection_review": advection_review,
        "h700_summary": h700_summary,
        "h925_summary": h925_summary,
        "snd_thermo": snd_thermo,
        "cloud_code_now": cloud_code_now,
        "precip_state": precip_state,
        "precip_trend": precip_trend,
        "candidates": candidates,
        "cov": cov,
        "obj": obj,
    }
