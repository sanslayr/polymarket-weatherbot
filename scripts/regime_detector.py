#!/usr/bin/env python3
"""Dynamic station regime detection from canonical raw state."""

from __future__ import annotations

from datetime import datetime
from typing import Any

from station_profile_registry import get_station_profile
from station_regime_rules import get_station_regime_rules


def _safe_float(value: Any) -> float | None:
    try:
        if value in (None, ""):
            return None
        return float(value)
    except Exception:
        return None


def _parse_iso(value: Any) -> datetime | None:
    try:
        text = str(value or "").strip()
        return datetime.fromisoformat(text) if text else None
    except Exception:
        return None


def _hours_to_peak(raw_state: dict[str, Any]) -> float | None:
    observations = dict(raw_state.get("observations") or {})
    window = dict(raw_state.get("window") or {})
    calc = dict(window.get("calc") or {})
    primary = dict(window.get("primary") or {})
    latest_dt = _parse_iso(observations.get("latest_report_local"))
    peak_dt = _parse_iso(calc.get("peak_local") or primary.get("peak_local"))
    if latest_dt is None or peak_dt is None:
        return None
    try:
        return (peak_dt - latest_dt).total_seconds() / 3600.0
    except Exception:
        return None


def _build_effect(spec: dict[str, Any], strength: float) -> dict[str, float]:
    def _scaled(node: dict[str, Any], *, lower_key: str | None = None) -> float:
        base = float(node.get("base") or 0.0)
        per_strength = float(node.get("per_strength") or 0.0)
        value = base + per_strength * float(strength)
        if node.get("cap") is not None:
            value = min(float(node.get("cap") or value), value)
        if lower_key and node.get(lower_key) is not None:
            value = max(float(node.get(lower_key) or value), value)
        return round(value, 3)

    return {
        "median_shift_c": _scaled(dict(spec.get("median_shift_c") or {})),
        "spread_scale": _scaled(dict(spec.get("spread_scale") or {}), lower_key="floor"),
        "warm_tail_bias": _scaled(dict(spec.get("warm_tail_bias") or {})),
        "floor_lift_c": _scaled(dict(spec.get("floor_lift_c") or {})),
        "timing_shift_h": _scaled(dict(spec.get("timing_shift_h") or {})),
    }


def _detect_ltac_sunny_highland_dry_mix(
    *,
    raw_state: dict[str, Any],
    rule: dict[str, Any],
) -> dict[str, Any]:
    observations = dict(raw_state.get("observations") or {})
    gate = dict(rule.get("gate") or {})
    cloud_code = str(observations.get("latest_cloud_code") or "").upper()
    precip_state = str(observations.get("precip_state") or "none").lower()
    latest_temp_c = _safe_float(observations.get("latest_temp_c"))
    latest_dewpoint_c = _safe_float(observations.get("latest_dewpoint_c"))
    latest_rh = _safe_float(observations.get("latest_rh"))
    latest_wspd_kt = _safe_float(observations.get("latest_wspd_kt"))
    radiation_eff = _safe_float(observations.get("radiation_eff"))
    temp_trend_c = _safe_float(observations.get("temp_trend_effective_c"))
    if temp_trend_c is None:
        temp_trend_c = _safe_float(observations.get("temp_trend_c"))
    temp_trend_c = temp_trend_c or 0.0
    hours_to_peak = _hours_to_peak(raw_state)

    if gate.get("nonprecip_only") and precip_state not in {"", "none"}:
        return {"active": False}
    if cloud_code not in set(gate.get("cloud_codes") or []):
        return {"active": False}
    if latest_temp_c is None or latest_dewpoint_c is None:
        return {"active": False}

    dewpoint_dep = latest_temp_c - latest_dewpoint_c
    if latest_wspd_kt is not None and latest_wspd_kt > float(gate.get("max_wind_kt") or 99.0):
        return {"active": False}
    if hours_to_peak is not None and hours_to_peak <= float(gate.get("min_hours_to_peak") or 0.0):
        return {"active": False}
    if temp_trend_c < float(gate.get("min_non_fading_trend_c") or -9.0):
        return {"active": False}
    max_rh = _safe_float(gate.get("max_rh_pct"))
    min_dep = float(gate.get("min_dewpoint_dep_c") or 0.0)
    if dewpoint_dep < min_dep and (latest_rh is None or max_rh is None or latest_rh > max_rh):
        return {"active": False}

    strength = 0.0
    reason_codes: list[str] = ["ltac_regime_sunny_highland_dry_mix"]
    if radiation_eff is not None and radiation_eff >= 0.78:
        strength += 1.0
        reason_codes.append("clean_solar")
    elif radiation_eff is None:
        strength += 0.55
        reason_codes.append("solar_proxy")
    else:
        strength += 0.40
        reason_codes.append("partial_solar")

    if dewpoint_dep >= 14.0:
        strength += 0.9
        reason_codes.append("dry_mixing")
    elif dewpoint_dep >= 12.0:
        strength += 0.6
        reason_codes.append("dry_air")

    if latest_rh is not None:
        if latest_rh <= 42.0:
            strength += 0.30
            reason_codes.append("low_rh")
        elif latest_rh <= 50.0:
            strength += 0.15
            reason_codes.append("moderate_rh")

    if latest_wspd_kt is not None and latest_wspd_kt <= 4.0:
        strength += 0.45
        reason_codes.append("weak_wind")
    elif latest_wspd_kt is not None and latest_wspd_kt <= 6.0:
        strength += 0.20
        reason_codes.append("light_wind")

    if temp_trend_c >= 0.18:
        strength += 0.55
        reason_codes.append("positive_trend")
    elif temp_trend_c >= 0.08:
        strength += 0.25
        reason_codes.append("gentle_rise")
    elif temp_trend_c >= 0.0:
        strength += 0.10
        reason_codes.append("non_fading")

    if hours_to_peak is not None and hours_to_peak >= 2.0:
        strength += 0.35
        reason_codes.append("midday_runway")

    if strength < 1.5:
        return {"active": False}

    confidence = "high" if strength >= 2.4 else ("medium" if strength >= 1.9 else "low")
    posterior_effect = _build_effect(dict(rule.get("posterior_effect") or {}), strength)
    return {
        "active": True,
        "id": str(rule.get("id") or "sunny_highland_dry_mix"),
        "label": str(rule.get("label") or ""),
        "description": str(rule.get("description") or ""),
        "strength": round(strength, 2),
        "confidence": confidence,
        "reason_codes": reason_codes,
        "posterior_effect": posterior_effect,
        "diagnostics": {
            "dewpoint_dep_c": round(dewpoint_dep, 2),
            "latest_rh": latest_rh,
            "latest_wspd_kt": latest_wspd_kt,
            "temp_trend_c": round(temp_trend_c, 2),
            "radiation_eff": radiation_eff,
            "hours_to_peak": round(hours_to_peak, 2) if hours_to_peak is not None else None,
        },
    }


def detect_station_regimes(raw_state: dict[str, Any]) -> dict[str, Any]:
    state = raw_state if isinstance(raw_state, dict) else {}
    forecast = dict(state.get("forecast") or {})
    meta = dict(forecast.get("meta") or {})
    station = str(meta.get("station") or "").upper()
    profile = get_station_profile(station)
    rules = get_station_regime_rules(station)

    active_regimes: list[dict[str, Any]] = []
    reason_codes: list[str] = []
    for rule in rules:
        rule_id = str(rule.get("id") or "")
        if rule_id == "sunny_highland_dry_mix":
            regime = _detect_ltac_sunny_highland_dry_mix(raw_state=state, rule=rule)
        else:
            regime = {"active": False}
        if bool(regime.get("active")):
            active_regimes.append(regime)
            reason_codes.extend(list(regime.get("reason_codes") or []))

    return {
        "station": station,
        "profile": profile,
        "active_regimes": active_regimes,
        "reason_codes": sorted(set(reason_codes)),
    }
