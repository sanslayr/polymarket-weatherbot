#!/usr/bin/env python3
"""Calibration hook for weather posterior uncertainty adjustment."""

from __future__ import annotations

from typing import Any

from contracts import WEATHER_POSTERIOR_SCHEMA_VERSION


def _safe_float(value: Any) -> float | None:
    try:
        if value in (None, ""):
            return None
        return float(value)
    except Exception:
        return None


def _clamp(value: float, lower: float, upper: float) -> float:
    return max(lower, min(upper, float(value)))


def _shrink_probability(prob: float | None, shrinkage: float) -> float | None:
    if prob is None:
        return None
    return round(0.5 + (float(prob) - 0.5) * (1.0 - float(shrinkage)), 3)


def _expand_quantiles(
    quantiles: dict[str, Any],
    *,
    center: float,
    multiplier: float,
    floor: float | None,
) -> dict[str, float]:
    out: dict[str, float] = {}
    for key in ("p10_c", "p25_c", "p50_c", "p75_c", "p90_c"):
        value = _safe_float(quantiles.get(key))
        if value is None:
            continue
        if key == "p50_c":
            adjusted = center
        else:
            adjusted = center + (value - center) * multiplier
        if floor is not None:
            adjusted = max(adjusted, floor)
        out[key] = round(adjusted, 2)

    ordered_keys = ["p10_c", "p25_c", "p50_c", "p75_c", "p90_c"]
    running = floor
    for key in ordered_keys:
        if key not in out:
            continue
        if running is not None:
            out[key] = round(max(out[key], running), 2)
        running = out[key]
    return out


def apply_weather_posterior_calibration(
    *,
    posterior_core: dict[str, Any],
    quality_snapshot: dict[str, Any],
) -> dict[str, Any]:
    core = dict(posterior_core or {})
    quality = dict(quality_snapshot or {})
    adjustments = dict(quality.get("posterior_adjustments") or {})
    confidence_label = str(((quality.get("scores") or {}).get("confidence_label")) or "")

    spread_multiplier = float(adjustments.get("spread_multiplier") or 1.0)
    probability_shrinkage = float(adjustments.get("probability_shrinkage") or 0.0)
    timing_cap = str(adjustments.get("timing_confidence_cap") or "")

    quantiles = dict(core.get("quantiles") or {})
    anchor = dict(core.get("anchor") or {})
    center = _safe_float(anchor.get("posterior_median_c")) or _safe_float(quantiles.get("p50_c")) or 0.0
    floor = _safe_float(anchor.get("observed_floor_c"))
    calibrated_quantiles = _expand_quantiles(
        quantiles,
        center=center,
        multiplier=spread_multiplier,
        floor=floor,
    )

    event_probs = dict(core.get("event_probs") or {})
    calibrated_event_probs = {
        "new_high_next_60m": _shrink_probability(_safe_float(event_probs.get("new_high_next_60m")), probability_shrinkage),
        "lock_by_window_end": _shrink_probability(_safe_float(event_probs.get("lock_by_window_end")), probability_shrinkage),
        "exceed_modeled_peak": _shrink_probability(_safe_float(event_probs.get("exceed_modeled_peak")), probability_shrinkage),
    }

    peak_time = dict(core.get("peak_time") or {})
    timing_confidence = str(peak_time.get("confidence") or "")
    if timing_cap and timing_confidence not in {"", timing_cap, "low"}:
        timing_confidence = timing_cap

    p10 = calibrated_quantiles.get("p10_c")
    p25 = calibrated_quantiles.get("p25_c")
    p75 = calibrated_quantiles.get("p75_c")
    p90 = calibrated_quantiles.get("p90_c")

    return {
        "schema_version": WEATHER_POSTERIOR_SCHEMA_VERSION,
        "unit": str(core.get("unit") or "C"),
        "source": "heuristic-core.v1+quality-calibration.v1",
        "core": core,
        "quality_snapshot_ref": {
            "confidence_label": confidence_label,
            "spread_multiplier": round(spread_multiplier, 3),
            "probability_shrinkage": round(probability_shrinkage, 3),
        },
        "anchor": anchor,
        "quantiles": calibrated_quantiles,
        "range_hint": {
            "display": {"lo_c": p10, "hi_c": p90},
            "core": {"lo_c": p25, "hi_c": p75},
        },
        "event_probs": calibrated_event_probs,
        "peak_time": {
            **peak_time,
            "confidence": timing_confidence or str(peak_time.get("confidence") or ""),
        },
        "calibration": {
            "applied": True,
            "confidence_label": confidence_label,
        },
        "reason_codes": list(core.get("reason_codes") or []),
    }
