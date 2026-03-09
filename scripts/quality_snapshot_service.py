#!/usr/bin/env python3
"""Structured runtime quality snapshot for posterior uncertainty control."""

from __future__ import annotations

from typing import Any

from contracts import QUALITY_SNAPSHOT_SCHEMA_VERSION


def _safe_float(value: Any) -> float | None:
    try:
        if value in (None, ""):
            return None
        return float(value)
    except Exception:
        return None


def _clamp(value: float, lower: float, upper: float) -> float:
    return max(lower, min(upper, float(value)))


def build_quality_snapshot(
    *,
    canonical_raw_state: dict[str, Any],
    posterior_feature_vector: dict[str, Any],
) -> dict[str, Any]:
    raw = canonical_raw_state if isinstance(canonical_raw_state, dict) else {}
    feat = posterior_feature_vector if isinstance(posterior_feature_vector, dict) else {}

    forecast = dict(raw.get("forecast") or {})
    obs = dict(raw.get("observations") or {})
    quality = dict(forecast.get("quality") or {})
    sounding = dict(forecast.get("sounding") or {})
    track_summary = dict(forecast.get("track_summary") or {})
    meta = dict(forecast.get("meta") or {})
    vertical_state = dict(feat.get("vertical_structure_state") or {})
    quality_state = dict(feat.get("quality_state") or {})

    synoptic_coverage = _safe_float(quality.get("synoptic_coverage"))
    anchors_total = _safe_float(quality.get("synoptic_anchors_total"))
    anchors_ok = _safe_float(quality.get("synoptic_anchors_ok"))
    track_anchor_count = _safe_float(track_summary.get("main_track_anchors_count"))
    sounding_density = str(vertical_state.get("coverage_density") or "")
    profile_source = str(vertical_state.get("profile_source") or sounding.get("profile_source") or "")
    cadence_min = _safe_float(obs.get("metar_routine_cadence_min"))
    recent_interval_min = _safe_float(obs.get("metar_recent_interval_min"))

    provider_score = 1.0
    flags: list[str] = []
    if bool(quality.get("synoptic_provider_fallback")):
        provider_score -= 0.16
        flags.append("provider_fallback")
    if str(quality.get("source_state") or "") != "fresh":
        provider_score -= 0.12
        flags.append("source_not_fresh")
    provider_score = _clamp(provider_score, 0.30, 1.0)

    coverage_score = 0.82
    if synoptic_coverage is not None:
        coverage_score = 0.55 + 0.45 * _clamp(synoptic_coverage, 0.0, 1.0)
    if anchors_total is not None and anchors_total > 0 and anchors_ok is not None:
        coverage_score *= 0.70 + 0.30 * _clamp(anchors_ok / anchors_total, 0.0, 1.0)
    if track_anchor_count is not None:
        if track_anchor_count >= 3:
            coverage_score += 0.06
        elif track_anchor_count <= 1:
            coverage_score -= 0.08
            flags.append("limited_track_anchors")
    coverage_score = _clamp(coverage_score, 0.30, 1.0)

    sounding_score = {
        "rich": 0.96,
        "moderate": 0.80,
        "sparse": 0.58,
        "": 0.52,
    }.get(sounding_density, 0.60)
    if profile_source in {"obs", "obs_sounding"}:
        sounding_score += 0.06
    elif profile_source in {"missing", "missing_profile"}:
        sounding_score -= 0.10
        flags.append("missing_profile")
    if sounding_density in {"sparse", ""}:
        flags.append("sparse_sounding")
    sounding_score = _clamp(sounding_score, 0.25, 1.0)

    obs_score = 0.92
    if bool(obs.get("metar_temp_quantized")):
        obs_score -= 0.10
        flags.append("quantized_temperature")
    if cadence_min is not None:
        if cadence_min > 45.0:
            obs_score -= 0.08
            flags.append("slow_metar_cadence")
        elif cadence_min <= 20.0:
            obs_score += 0.03
    if recent_interval_min is not None and recent_interval_min > 60.0:
        obs_score -= 0.08
        flags.append("stale_metar_interval")
    if bool(obs.get("metar_speci_active")):
        obs_score += 0.03
    obs_score = _clamp(obs_score, 0.35, 1.0)

    missing_layers = list(quality_state.get("missing_layers") or [])
    overall_score = (
        0.32 * provider_score
        + 0.28 * coverage_score
        + 0.22 * sounding_score
        + 0.18 * obs_score
    )
    overall_score -= min(len(missing_layers), 4) * 0.025
    overall_score = _clamp(overall_score, 0.20, 1.0)

    spread_multiplier = round(_clamp(1.55 - 0.55 * overall_score, 1.00, 1.50), 3)
    probability_shrinkage = round(_clamp(0.65 - 0.55 * overall_score, 0.08, 0.45), 3)
    if overall_score >= 0.82:
        confidence_label = "high"
    elif overall_score >= 0.62:
        confidence_label = "medium"
    else:
        confidence_label = "low"

    return {
        "schema_version": QUALITY_SNAPSHOT_SCHEMA_VERSION,
        "meta": {
            "station": str(meta.get("station") or ""),
            "date": str(meta.get("date") or ""),
            "model": str(meta.get("model") or ""),
            "synoptic_provider": str(meta.get("synoptic_provider") or ""),
            "runtime": str(meta.get("runtime") or ""),
        },
        "source": {
            "source_state": str(quality.get("source_state") or ""),
            "synoptic_provider_requested": str(quality.get("synoptic_provider_requested") or ""),
            "synoptic_provider_used": str(quality.get("synoptic_provider_used") or ""),
            "synoptic_provider_fallback": bool(quality.get("synoptic_provider_fallback")),
        },
        "coverage": {
            "synoptic_coverage": synoptic_coverage,
            "synoptic_anchors_total": anchors_total,
            "synoptic_anchors_ok": anchors_ok,
            "track_anchor_count": track_anchor_count,
            "sounding_density": sounding_density,
            "profile_source": profile_source,
            "missing_layers": missing_layers,
        },
        "observational": {
            "metar_temp_quantized": bool(obs.get("metar_temp_quantized")),
            "metar_routine_cadence_min": cadence_min,
            "metar_recent_interval_min": recent_interval_min,
            "metar_speci_active": bool(obs.get("metar_speci_active")),
            "metar_speci_likely": bool(obs.get("metar_speci_likely")),
        },
        "scores": {
            "provider_score": round(provider_score, 3),
            "coverage_score": round(coverage_score, 3),
            "sounding_score": round(sounding_score, 3),
            "obs_score": round(obs_score, 3),
            "overall_score": round(overall_score, 3),
            "confidence_label": confidence_label,
        },
        "posterior_adjustments": {
            "spread_multiplier": spread_multiplier,
            "probability_shrinkage": probability_shrinkage,
            "timing_confidence_cap": "medium" if confidence_label == "low" else "",
        },
        "flags": sorted(set(flags)),
    }
