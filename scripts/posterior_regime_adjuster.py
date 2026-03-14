#!/usr/bin/env python3
"""Posterior distribution adjustments from detected station regimes."""

from __future__ import annotations

from typing import Any

from station_profile_registry import get_station_profile


def _clamp(value: float, lower: float, upper: float) -> float:
    return max(lower, min(upper, float(value)))


def apply_regime_effects(
    *,
    station_icao: str,
    baseline_posterior: dict[str, Any],
    active_regimes: list[dict[str, Any]],
    raw_state: dict[str, Any],
) -> dict[str, Any]:
    _ = raw_state
    profile = get_station_profile(station_icao)
    baseline = dict(baseline_posterior or {})
    adjusted = {
        "median_c": float(baseline.get("median_c") or 0.0),
        "spread_c": float(baseline.get("spread_c") or 0.0),
        "warm_tail_boost": float(baseline.get("warm_tail_boost") or 0.0),
        "lower_tail_lift_c": float(baseline.get("lower_tail_lift_c") or 0.0),
        "timing_shift_h": float(baseline.get("timing_shift_h") or 0.0),
    }

    applied_regimes: list[dict[str, Any]] = []
    reason_codes: list[str] = []
    for regime in active_regimes:
        if not isinstance(regime, dict) or not regime.get("active"):
            continue
        effect = dict(regime.get("posterior_effect") or {})
        adjusted["median_c"] += float(effect.get("median_shift_c") or 0.0)
        adjusted["spread_c"] *= float(effect.get("spread_scale") or 1.0)
        adjusted["warm_tail_boost"] += float(effect.get("warm_tail_bias") or 0.0)
        adjusted["lower_tail_lift_c"] += float(effect.get("floor_lift_c") or 0.0)
        adjusted["timing_shift_h"] += float(effect.get("timing_shift_h") or 0.0)
        applied_regimes.append(regime)
        regime_id = str(regime.get("id") or "")
        if regime_id:
            reason_codes.append(f"regime_{regime_id}")
        reason_codes.extend(list(regime.get("reason_codes") or []))

    adjusted["spread_c"] = _clamp(adjusted["spread_c"], 0.18, 2.10)
    adjusted["warm_tail_boost"] = _clamp(adjusted["warm_tail_boost"], 0.0, 0.35)
    adjusted["lower_tail_lift_c"] = _clamp(adjusted["lower_tail_lift_c"], 0.0, 0.60)
    adjusted["timing_shift_h"] = _clamp(adjusted["timing_shift_h"], -0.75, 0.75)

    return {
        "station": str(station_icao or "").upper(),
        "profile": profile,
        "distribution": adjusted,
        "applied_regimes": applied_regimes,
        "reason_codes": sorted(set(reason_codes)),
    }
