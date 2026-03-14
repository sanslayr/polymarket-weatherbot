from __future__ import annotations

from typing import Any


def _as_dict(value: Any) -> dict[str, Any]:
    return dict(value or {}) if isinstance(value, dict) else {}


def snapshot_root(snapshot: dict[str, Any] | None) -> dict[str, Any]:
    return _as_dict(snapshot)


def snapshot_canonical_raw_state(snapshot: dict[str, Any] | None) -> dict[str, Any]:
    return _as_dict(snapshot_root(snapshot).get("canonical_raw_state"))


def snapshot_posterior_feature_vector(snapshot: dict[str, Any] | None) -> dict[str, Any]:
    return _as_dict(snapshot_root(snapshot).get("posterior_feature_vector"))


def snapshot_quality_snapshot(snapshot: dict[str, Any] | None) -> dict[str, Any]:
    return _as_dict(snapshot_root(snapshot).get("quality_snapshot"))


def snapshot_weather_posterior(snapshot: dict[str, Any] | None) -> dict[str, Any]:
    return _as_dict(snapshot_root(snapshot).get("weather_posterior"))


def snapshot_weather_posterior_core(snapshot: dict[str, Any] | None) -> dict[str, Any]:
    return _as_dict(snapshot_weather_posterior(snapshot).get("core"))


def snapshot_weather_posterior_anchor(snapshot: dict[str, Any] | None) -> dict[str, Any]:
    posterior = snapshot_weather_posterior(snapshot)
    anchor = _as_dict(posterior.get("anchor"))
    if anchor:
        return anchor
    return _as_dict(snapshot_weather_posterior_core(snapshot).get("anchor"))


def snapshot_weather_posterior_event_probs(snapshot: dict[str, Any] | None) -> dict[str, Any]:
    return _as_dict(snapshot_weather_posterior(snapshot).get("event_probs"))


def snapshot_weather_posterior_calibration(snapshot: dict[str, Any] | None) -> dict[str, Any]:
    return _as_dict(snapshot_weather_posterior(snapshot).get("calibration"))


def snapshot_path_context(snapshot: dict[str, Any] | None) -> dict[str, Any]:
    return _as_dict(snapshot_weather_posterior_core(snapshot).get("path_context"))


def snapshot_progress_state(snapshot: dict[str, Any] | None) -> dict[str, Any]:
    return _as_dict(snapshot_weather_posterior_core(snapshot).get("progress"))


def snapshot_temp_phase_decision(snapshot: dict[str, Any] | None) -> dict[str, Any]:
    return _as_dict(snapshot_root(snapshot).get("temp_phase_decision"))


def snapshot_peak_data(snapshot: dict[str, Any] | None) -> dict[str, Any]:
    return _as_dict(snapshot_root(snapshot).get("peak_data"))


def snapshot_peak_summary(snapshot: dict[str, Any] | None) -> dict[str, Any]:
    return _as_dict(snapshot_peak_data(snapshot).get("summary"))


def snapshot_boundary_layer_regime(snapshot: dict[str, Any] | None) -> dict[str, Any]:
    return _as_dict(snapshot_root(snapshot).get("boundary_layer_regime"))


def snapshot_synoptic_summary(snapshot: dict[str, Any] | None) -> dict[str, Any]:
    return _as_dict(snapshot_root(snapshot).get("synoptic_summary"))


def snapshot_condition_state(snapshot: dict[str, Any] | None) -> dict[str, Any]:
    return _as_dict(snapshot_root(snapshot).get("condition_state"))


def snapshot_branch_outlook(snapshot: dict[str, Any] | None) -> dict[str, Any]:
    return _as_dict(snapshot_posterior_feature_vector(snapshot).get("matched_branch_outlook_state"))
