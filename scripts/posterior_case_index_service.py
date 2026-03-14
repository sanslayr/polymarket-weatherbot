from __future__ import annotations

from typing import Any

from contracts import POSTERIOR_CASE_INDEX_SCHEMA_VERSION


def _as_dict(value: Any) -> dict[str, Any]:
    return dict(value or {}) if isinstance(value, dict) else {}


def _safe_float(value: Any) -> float | None:
    try:
        if value in (None, ""):
            return None
        return float(value)
    except Exception:
        return None


def build_posterior_case_index(sample: dict[str, Any]) -> dict[str, Any]:
    node = _as_dict(sample)
    feature_blocks = _as_dict(node.get("feature_blocks"))
    branch_outlook = _as_dict(feature_blocks.get("matched_branch_outlook_state"))
    ensemble_state = _as_dict(feature_blocks.get("ensemble_path_state"))
    display_output = _as_dict(node.get("display_output"))
    display_range = _as_dict(display_output.get("display_range"))
    metrics = _as_dict(node.get("metrics"))
    case_tags = _as_dict(node.get("case_tags"))
    posterior_context = _as_dict(node.get("posterior_context"))
    posterior_output = _as_dict(node.get("posterior_output"))
    event_probs = _as_dict(posterior_output.get("event_probs"))

    return {
        "schema_version": POSTERIOR_CASE_INDEX_SCHEMA_VERSION,
        "sample_id": str(node.get("sample_id") or ""),
        "station_icao": str(node.get("station_icao") or ""),
        "target_date_local": str(node.get("target_date_local") or ""),
        "sample_time_local": str(node.get("sample_time_local") or ""),
        "phase": str(node.get("phase") or ""),
        "display_phase": str(node.get("display_phase") or ""),
        "sampling_reason": str(node.get("sampling_reason") or ""),
        "branch_family": str(branch_outlook.get("branch_family") or ""),
        "branch_stage_now": str(branch_outlook.get("branch_stage_now") or ""),
        "next_transition_gate": str(branch_outlook.get("next_transition_gate") or ""),
        "branch_volatility": str(branch_outlook.get("branch_volatility") or ""),
        "dominant_path": str(ensemble_state.get("dominant_path") or ""),
        "observed_path": str(ensemble_state.get("observed_path") or ""),
        "range_truth_source": str((_as_dict(node.get("lineage"))).get("range_truth_source") or ""),
        "quality_confidence_label": str(case_tags.get("quality_confidence_label") or ""),
        "display_range_lo_c": _safe_float(display_range.get("lo_c")),
        "display_range_hi_c": _safe_float(display_range.get("hi_c")),
        "display_width_c": _safe_float(metrics.get("display_width_c")),
        "posterior_p50_c": _safe_float(metrics.get("p50_c")),
        "lock_by_window_end": _safe_float(event_probs.get("lock_by_window_end")),
        "new_high_next_60m": _safe_float(event_probs.get("new_high_next_60m")),
        "reason_codes": list(posterior_context.get("reason_codes") or []),
    }
