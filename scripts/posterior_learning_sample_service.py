from __future__ import annotations

import hashlib
from datetime import datetime, timezone
from typing import Any

from analysis_snapshot_view import (
    snapshot_boundary_layer_regime,
    snapshot_branch_outlook,
    snapshot_canonical_raw_state,
    snapshot_condition_state,
    snapshot_path_context,
    snapshot_peak_summary,
    snapshot_posterior_feature_vector,
    snapshot_quality_snapshot,
    snapshot_synoptic_summary,
    snapshot_temp_phase_decision,
    snapshot_weather_posterior,
    snapshot_weather_posterior_anchor,
    snapshot_weather_posterior_calibration,
    snapshot_weather_posterior_core,
    snapshot_weather_posterior_event_probs,
)
from contracts import POSTERIOR_LEARNING_SAMPLE_SCHEMA_VERSION


def _as_dict(value: Any) -> dict[str, Any]:
    return dict(value or {}) if isinstance(value, dict) else {}


def _safe_float(value: Any) -> float | None:
    try:
        if value in (None, ""):
            return None
        return float(value)
    except Exception:
        return None


def _utc_now_z() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def _hash_parts(*parts: Any) -> str:
    raw = "|".join(str(part or "") for part in parts)
    return hashlib.sha1(raw.encode("utf-8")).hexdigest()[:20]


def _round_map(node: dict[str, Any] | None, *keys: str, digits: int = 3) -> dict[str, float | None]:
    out: dict[str, float | None] = {}
    src = _as_dict(node)
    for key in keys:
        value = _safe_float(src.get(key))
        out[key] = round(value, digits) if value is not None else None
    return out


def build_posterior_learning_sample(
    *,
    analysis_snapshot: dict[str, Any],
    sampling_reason: str,
    sample_source: str = "runtime_snapshot",
    sampled_at_utc: str | None = None,
    source_context: dict[str, Any] | None = None,
) -> dict[str, Any]:
    snapshot = _as_dict(analysis_snapshot)
    canonical = snapshot_canonical_raw_state(snapshot)
    forecast = _as_dict(canonical.get("forecast"))
    forecast_meta = _as_dict(forecast.get("meta"))
    observations = _as_dict(canonical.get("observations"))
    posterior_feature_vector = snapshot_posterior_feature_vector(snapshot)
    temp_phase = snapshot_temp_phase_decision(snapshot)
    time_phase = _as_dict(posterior_feature_vector.get("time_phase"))
    quality_snapshot = snapshot_quality_snapshot(snapshot)
    weather_posterior = snapshot_weather_posterior(snapshot)
    posterior_core = snapshot_weather_posterior_core(snapshot)
    peak_summary = snapshot_peak_summary(snapshot)
    branch_outlook = snapshot_branch_outlook(snapshot)
    path_context = snapshot_path_context(snapshot)
    boundary_layer_regime = snapshot_boundary_layer_regime(snapshot)
    synoptic_summary = snapshot_synoptic_summary(snapshot)
    condition_state = snapshot_condition_state(snapshot)
    anchor = snapshot_weather_posterior_anchor(snapshot)
    event_probs = snapshot_weather_posterior_event_probs(snapshot)
    calibration = snapshot_weather_posterior_calibration(snapshot)

    station_icao = str(forecast_meta.get("station") or "")
    target_date_local = str(forecast_meta.get("date") or "")
    sample_time_local = str(observations.get("latest_report_local") or "")
    display_phase = str(temp_phase.get("display_phase") or time_phase.get("display_phase") or "")
    phase = str(temp_phase.get("phase") or time_phase.get("phase") or display_phase or "")
    runtime_tag = str(forecast_meta.get("runtime") or "")
    model = str(forecast_meta.get("model") or "")
    synoptic_provider = str(forecast_meta.get("synoptic_provider") or "")
    sampled_at = str(sampled_at_utc or _utc_now_z())
    sample_id = _hash_parts(
        station_icao,
        target_date_local,
        sample_time_local,
        runtime_tag,
        phase,
        sampling_reason,
        str(snapshot.get("schema_version") or ""),
    )

    ranges = _as_dict(peak_summary.get("ranges"))
    display_range = _as_dict(ranges.get("display"))
    core_range = _as_dict(ranges.get("core"))
    quality_scores = _as_dict(quality_snapshot.get("scores"))
    quality_state = _as_dict(posterior_feature_vector.get("quality_state"))

    return {
        "schema_version": POSTERIOR_LEARNING_SAMPLE_SCHEMA_VERSION,
        "sample_id": sample_id,
        "sampling_reason": str(sampling_reason or ""),
        "sample_source": str(sample_source or "runtime_snapshot"),
        "sampled_at_utc": sampled_at,
        "station_icao": station_icao,
        "target_date_local": target_date_local,
        "sample_time_local": sample_time_local,
        "phase": phase,
        "display_phase": display_phase,
        "runtime": {
            "model": model,
            "synoptic_provider": synoptic_provider,
            "runtime_tag": runtime_tag,
            "unit": str(snapshot.get("unit") or canonical.get("unit") or "C"),
        },
        "lineage": {
            "analysis_snapshot_schema_version": str(snapshot.get("schema_version") or ""),
            "canonical_raw_state_schema_version": str(canonical.get("schema_version") or ""),
            "posterior_feature_vector_schema_version": str(posterior_feature_vector.get("schema_version") or ""),
            "quality_snapshot_schema_version": str(quality_snapshot.get("schema_version") or ""),
            "weather_posterior_schema_version": str(weather_posterior.get("schema_version") or ""),
            "weather_posterior_core_schema_version": str(posterior_core.get("schema_version") or ""),
            "range_truth_source": str(peak_summary.get("range_truth_source") or ""),
            "range_source": str(ranges.get("source") or ""),
        },
        "canonical_ref": {
            "latest_report_local": sample_time_local,
            "window_start_local": str((_as_dict(_as_dict(canonical.get("window")).get("calc"))).get("start_local") or ""),
            "window_peak_local": str((_as_dict(_as_dict(canonical.get("window")).get("calc"))).get("peak_local") or ""),
            "window_end_local": str((_as_dict(_as_dict(canonical.get("window")).get("calc"))).get("end_local") or ""),
        },
        "feature_blocks": {
            "time_phase": _as_dict(posterior_feature_vector.get("time_phase")),
            "observation_state": _as_dict(posterior_feature_vector.get("observation_state")),
            "peak_phase_state": _as_dict(posterior_feature_vector.get("peak_phase_state")),
            "ensemble_path_state": _as_dict(posterior_feature_vector.get("ensemble_path_state")),
            "matched_branch_outlook_state": branch_outlook,
            "quality_state": quality_state,
            "cloud_radiation_state": _as_dict(posterior_feature_vector.get("cloud_radiation_state")),
            "moisture_stability_state": _as_dict(posterior_feature_vector.get("moisture_stability_state")),
            "mixing_coupling_state": _as_dict(posterior_feature_vector.get("mixing_coupling_state")),
            "transport_state": _as_dict(posterior_feature_vector.get("transport_state")),
            "vertical_structure_state": _as_dict(posterior_feature_vector.get("vertical_structure_state")),
            "forecast_shape_state": _as_dict(posterior_feature_vector.get("forecast_shape_state")),
            "regime_state": _as_dict(posterior_feature_vector.get("regime_state")),
        },
        "posterior_context": {
            "path_context": path_context,
            "anchor": anchor,
            "reason_codes": list(weather_posterior.get("reason_codes") or []),
        },
        "posterior_output": {
            "quantiles": _as_dict(weather_posterior.get("quantiles")),
            "event_probs": event_probs,
            "calibration": calibration,
            "range_hint": _as_dict(weather_posterior.get("range_hint")),
            "range_hint_meta": _as_dict(weather_posterior.get("range_hint_meta")),
            "peak_time": _as_dict(weather_posterior.get("peak_time")),
        },
        "display_output": {
            "range_truth_source": str(peak_summary.get("range_truth_source") or ""),
            "display_range": {
                "lo_c": _safe_float(display_range.get("lo")),
                "hi_c": _safe_float(display_range.get("hi")),
            },
            "core_range": {
                "lo_c": _safe_float(core_range.get("lo")),
                "hi_c": _safe_float(core_range.get("hi")),
            },
            "observed_max_temp_c": _safe_float((_as_dict(peak_summary.get("observed"))).get("max_temp_c")),
        },
        "case_tags": {
            "branch_family": str(branch_outlook.get("branch_family") or ""),
            "branch_stage_now": str(branch_outlook.get("branch_stage_now") or ""),
            "next_transition_gate": str(branch_outlook.get("next_transition_gate") or ""),
            "branch_volatility": str(branch_outlook.get("branch_volatility") or ""),
            "daily_peak_state": str((_as_dict(posterior_feature_vector.get("peak_phase_state"))).get("daily_peak_state") or ""),
            "second_peak_potential": str((_as_dict(posterior_feature_vector.get("peak_phase_state"))).get("second_peak_potential") or ""),
            "quality_confidence_label": str(quality_scores.get("confidence_label") or ""),
            "analysis_window_mode": str((_as_dict(posterior_core.get("progress"))).get("analysis_window_mode") or ""),
        },
        "analysis_refs": {
            "boundary_layer_regime_key": str(boundary_layer_regime.get("regime_key") or ""),
            "synoptic_pathway": str((_as_dict(synoptic_summary.get("summary"))).get("pathway") or ""),
            "condition_state_source_state": str((_as_dict(condition_state.get("quality"))).get("source_state") or ""),
        },
        "metrics": {
            "display_width_c": (
                round(float(display_range.get("hi")) - float(display_range.get("lo")), 3)
                if _safe_float(display_range.get("hi")) is not None and _safe_float(display_range.get("lo")) is not None
                else None
            ),
            "core_width_c": (
                round(float(core_range.get("hi")) - float(core_range.get("lo")), 3)
                if _safe_float(core_range.get("hi")) is not None and _safe_float(core_range.get("lo")) is not None
                else None
            ),
            **_round_map(weather_posterior.get("event_probs"), "new_high_next_60m", "lock_by_window_end", "exceed_modeled_peak"),
            **_round_map(weather_posterior.get("quantiles"), "p10_c", "p25_c", "p50_c", "p75_c", "p90_c", digits=2),
        },
        "source_context": _as_dict(source_context),
    }
