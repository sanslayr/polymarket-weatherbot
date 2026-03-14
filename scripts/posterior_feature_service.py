#!/usr/bin/env python3
"""Posterior-ready quantitative feature vector builder."""

from __future__ import annotations

from datetime import datetime
from math import ceil
from typing import Any

from contracts import POSTERIOR_FEATURE_VECTOR_SCHEMA_VERSION
from ecmwf_ensemble_factor_service import summarize_member_path_rows


def _safe_float(value: Any) -> float | None:
    try:
        if value in (None, ""):
            return None
        return float(value)
    except Exception:
        return None


def _parse_iso_dt(value: Any) -> datetime | None:
    try:
        text = str(value or "").strip()
        return datetime.fromisoformat(text) if text else None
    except Exception:
        return None


def _clamp(value: float, lower: float, upper: float) -> float:
    return max(lower, min(upper, float(value)))


def _coerce_same_tz(a: datetime | None, b: datetime | None) -> tuple[datetime | None, datetime | None]:
    if a is None or b is None:
        return a, b
    try:
        if a.tzinfo is not None and b.tzinfo is None:
            b = b.replace(tzinfo=a.tzinfo)
        elif a.tzinfo is None and b.tzinfo is not None:
            a = a.replace(tzinfo=b.tzinfo)
    except Exception:
        pass
    return a, b


def _hours_between(later: datetime | None, earlier: datetime | None) -> float | None:
    later, earlier = _coerce_same_tz(later, earlier)
    if later is None or earlier is None:
        return None
    try:
        return (later - earlier).total_seconds() / 3600.0
    except Exception:
        return None


def _hours_between_iso(later: Any, earlier: Any) -> float | None:
    return _hours_between(_parse_iso_dt(later), _parse_iso_dt(earlier))


def _cloud_trend_signal(value: str) -> int:
    text = str(value or "").strip().lower()
    if not text:
        return 0
    warm_tokens = (
        "thin",
        "thinning",
        "break",
        "breaking",
        "clear",
        "clearing",
        "improv",
        "lift",
        "lifting",
        "scatter",
        "scattering",
        "decrease",
        "decreasing",
        "reduce",
        "reducing",
        "open",
        "opening",
        "散",
        "开",
        "减",
        "薄",
        "晴",
    )
    cold_tokens = (
        "thick",
        "thickening",
        "build",
        "building",
        "increase",
        "increasing",
        "fill",
        "filling",
        "overcast",
        "worsen",
        "worsening",
        "close",
        "closing",
        "增",
        "厚",
        "阴",
        "封",
    )
    warm_hit = any(token in text for token in warm_tokens)
    cold_hit = any(token in text for token in cold_tokens)
    if warm_hit and not cold_hit:
        return 1
    if cold_hit and not warm_hit:
        return -1
    return 0


def _surface_bias_polarity(value: str) -> str:
    text = str(value or "").strip().lower()
    if not text:
        return ""
    if ("warm" in text) or ("暖" in text):
        return "warm"
    if ("cold" in text) or ("冷" in text):
        return "cold"
    if ("neutral" in text) or ("中性" in text):
        return "neutral"
    return ""


def _live_ensemble_path_alignment(
    *,
    temp_trend_c: float | None,
    temp_bias_c: float | None,
    cloud_effective_cover: float | None,
    radiation_eff: float | None,
    cloud_trend: str,
    precip_state: str,
    transport_state: str,
    thermal_advection_state: str,
    surface_bias: str,
    dominant_path: str,
    dominant_path_detail: str,
    dominant_prob: float | None,
    dominant_detail_prob: float | None,
    dominant_margin_prob: float | None,
) -> dict[str, Any]:
    warm_score = 0.0
    cold_score = 0.0
    neutral_score = 0.0

    if temp_trend_c is not None:
        if temp_trend_c >= 0.28:
            warm_score += 0.34
        elif temp_trend_c >= 0.14:
            warm_score += 0.22
        elif temp_trend_c <= -0.20:
            cold_score += 0.30
        elif temp_trend_c <= -0.08:
            cold_score += 0.18
        else:
            neutral_score += 0.12

    if temp_bias_c is not None:
        if temp_bias_c >= 0.35:
            warm_score += 0.22
        elif temp_bias_c >= 0.15:
            warm_score += 0.12
        elif temp_bias_c <= -0.35:
            cold_score += 0.22
        elif temp_bias_c <= -0.15:
            cold_score += 0.12
        else:
            neutral_score += 0.12

    if radiation_eff is not None:
        if radiation_eff >= 0.80:
            warm_score += 0.18
        elif radiation_eff >= 0.62:
            warm_score += 0.08
            neutral_score += 0.04
        elif radiation_eff <= 0.42:
            cold_score += 0.18
        elif radiation_eff <= 0.55:
            cold_score += 0.08
        else:
            neutral_score += 0.06

    if cloud_effective_cover is not None:
        if cloud_effective_cover <= 0.30:
            warm_score += 0.18
        elif cloud_effective_cover <= 0.55:
            warm_score += 0.08
            neutral_score += 0.04
        elif cloud_effective_cover >= 0.80:
            cold_score += 0.18
        elif cloud_effective_cover >= 0.65:
            cold_score += 0.10
        else:
            neutral_score += 0.04

    cloud_signal = _cloud_trend_signal(cloud_trend)
    if cloud_signal > 0:
        warm_score += 0.12
    elif cloud_signal < 0:
        cold_score += 0.12
    else:
        neutral_score += 0.04

    advection_bonus = {
        "confirmed": 0.22,
        "probable": 0.14,
    }.get(str(thermal_advection_state or "").strip().lower(), 0.0)
    transport_key = str(transport_state or "").strip().lower()
    if transport_key == "warm":
        warm_score += 0.12 + advection_bonus
    elif transport_key == "cold":
        cold_score += 0.12 + advection_bonus
    else:
        neutral_score += 0.10

    surface_bias_key = _surface_bias_polarity(surface_bias)
    if surface_bias_key == "warm":
        warm_score += 0.08
    elif surface_bias_key == "cold":
        cold_score += 0.08
    elif surface_bias_key == "neutral":
        neutral_score += 0.04

    precip_key = str(precip_state or "").strip().lower()
    if precip_key not in {"", "none"}:
        cold_score += 0.16
    else:
        neutral_score += 0.02

    warm_score = round(_clamp(warm_score, 0.0, 1.0), 3)
    cold_score = round(_clamp(cold_score, 0.0, 1.0), 3)
    neutral_score = round(_clamp(neutral_score, 0.0, 1.0), 3)

    balance = warm_score - cold_score
    if warm_score >= 0.78 and warm_score >= cold_score + 0.20:
        observed_path = "warm_support"
        observed_path_detail = "warm_support"
        observed_path_score = warm_score
    elif cold_score >= 0.78 and cold_score >= warm_score + 0.20:
        observed_path = "cold_suppression"
        observed_path_detail = "cold_suppression"
        observed_path_score = cold_score
    else:
        observed_path = "transition"
        if balance >= 0.16:
            observed_path_detail = "weak_warm_transition"
            observed_path_score = max(warm_score, neutral_score + 0.10)
        elif balance <= -0.16:
            observed_path_detail = "weak_cold_transition"
            observed_path_score = max(cold_score, neutral_score + 0.10)
        else:
            observed_path_detail = "neutral_stable"
            observed_path_score = max(neutral_score, 0.42)

    dominant_key = dominant_path_detail if str(dominant_path or "") == "transition" else dominant_path
    observed_key = observed_path_detail if observed_path == "transition" else observed_path

    match_state = "none"
    if observed_key and dominant_key and observed_key == dominant_key:
        match_state = "exact"
    elif observed_path and dominant_path and observed_path == dominant_path:
        match_state = "path"

    alignment_score = 0.0
    if match_state == "exact":
        alignment_score = 0.44 + 0.30 * float(observed_path_score)
        if dominant_prob is not None:
            alignment_score += 0.16 * _clamp(dominant_prob, 0.0, 1.0)
        if dominant_detail_prob is not None:
            alignment_score += 0.10 * _clamp(dominant_detail_prob, 0.0, 1.0)
    elif match_state == "path":
        alignment_score = 0.32 + 0.26 * float(observed_path_score)
        if dominant_prob is not None:
            alignment_score += 0.12 * _clamp(dominant_prob, 0.0, 1.0)
    else:
        alignment_score = 0.12 * float(observed_path_score)

    if dominant_margin_prob is not None and match_state != "none":
        alignment_score += 0.06 * _clamp(dominant_margin_prob * 4.0, 0.0, 1.0)
    alignment_score = round(_clamp(alignment_score, 0.0, 0.98), 3)

    alignment_confidence = "none"
    dominant_prob_v = dominant_prob if dominant_prob is not None else 0.0
    if (
        match_state == "exact"
        and alignment_score >= 0.76
        and observed_path_score >= 0.62
        and dominant_prob_v >= 0.54
    ):
        alignment_confidence = "high"
    elif match_state in {"exact", "path"} and alignment_score >= 0.56 and observed_path_score >= 0.48:
        alignment_confidence = "partial"

    observed_path_locked = (
        alignment_confidence == "high"
        and match_state == "exact"
        and dominant_prob_v >= 0.60
    )
    return {
        "observed_path": observed_path,
        "observed_path_detail": observed_path_detail,
        "observed_path_score": round(float(observed_path_score), 3),
        "observed_warm_signal": warm_score,
        "observed_cold_signal": cold_score,
        "observed_neutral_signal": neutral_score,
        "observed_alignment_match_state": match_state,
        "observed_alignment_matches_dominant": match_state in {"exact", "path"},
        "observed_alignment_exact": match_state == "exact",
        "observed_alignment_score": alignment_score,
        "observed_alignment_confidence": alignment_confidence,
        "observed_path_locked": observed_path_locked,
    }


def _path_side(path_label: str, *, path_detail: str = "") -> str:
    key = path_detail if str(path_label or "") == "transition" and str(path_detail or "") else str(path_label or "")
    mapping = {
        "warm_support": "warm",
        "weak_warm_transition": "warm",
        "cold_suppression": "cold",
        "weak_cold_transition": "cold",
        "transition": "neutral",
        "neutral_stable": "neutral",
    }
    return mapping.get(key, "")


def _member_row_key(raw: dict[str, Any], fallback_index: int) -> tuple[int, str, str]:
    try:
        number = int(raw.get("number"))
    except Exception:
        number = fallback_index
    return (
        number,
        str(raw.get("path_label") or ""),
        str(raw.get("path_detail") or ""),
    )


def _merge_member_rows(*groups: list[dict[str, Any]]) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    seen: set[tuple[int, str, str]] = set()
    fallback_index = 0
    for group in groups:
        for raw in group:
            key = _member_row_key(raw, fallback_index)
            fallback_index += 1
            if key in seen:
                continue
            seen.add(key)
            out.append(dict(raw))
    return out


def _member_match_tier(
    raw: dict[str, Any],
    *,
    observed_path: str,
    observed_path_detail: str,
) -> str:
    row_path = str(raw.get("path_label") or "")
    row_detail = str(raw.get("path_detail") or "")
    if not row_path or not observed_path:
        return ""

    if observed_path == "warm_support":
        if row_path == "warm_support":
            return "exact"
        if row_path == "transition" and row_detail == "weak_warm_transition":
            return "side"
        return ""

    if observed_path == "cold_suppression":
        if row_path == "cold_suppression":
            return "exact"
        if row_path == "transition" and row_detail == "weak_cold_transition":
            return "side"
        return ""

    if observed_path == "transition":
        if row_path == "transition" and row_detail and row_detail == observed_path_detail:
            return "exact"
        if row_path == "transition":
            return "path"
        if observed_path_detail == "weak_warm_transition" and row_path == "warm_support":
            return "side"
        if observed_path_detail == "weak_cold_transition" and row_path == "cold_suppression":
            return "side"
        return ""

    return ""


def _build_observation_matched_subset(
    *,
    members: list[dict[str, Any]],
    phase: str,
    hours_to_peak: float | None,
    observed_path: str,
    observed_path_detail: str,
    observed_path_score: float | None,
    match_state: str,
    confidence: str,
) -> dict[str, Any]:
    rows = [dict(raw) for raw in members if str((raw or {}).get("path_label") or "").strip()]
    if not rows:
        return {}

    live_window = str(phase or "") in {"near_window", "in_window", "post"}
    if not live_window and (hours_to_peak is None or hours_to_peak > 4.5):
        return {}
    if not observed_path:
        return {}
    if len(rows) < 5:
        return {}

    effective_confidence = ""
    subset_reason = ""
    if match_state in {"exact", "path"} and confidence in {"high", "partial"}:
        effective_confidence = confidence
        subset_reason = (
            "observed_exact_high"
            if confidence == "high" and match_state == "exact"
            else "observed_path_confirmed"
        )
    else:
        score = float(observed_path_score) if observed_path_score is not None else 0.0
        if score >= 0.74:
            effective_confidence = "high"
            subset_reason = "observed_path_override"
        elif score >= 0.60:
            effective_confidence = "partial"
            subset_reason = "observed_path_override_soft"
        else:
            return {}

    exact_rows: list[dict[str, Any]] = []
    path_rows: list[dict[str, Any]] = []
    side_rows: list[dict[str, Any]] = []
    for raw in rows:
        tier = _member_match_tier(
            raw,
            observed_path=observed_path,
            observed_path_detail=observed_path_detail,
        )
        if tier == "exact":
            exact_rows.append(raw)
            path_rows.append(raw)
        elif tier == "path":
            path_rows.append(raw)
        elif tier == "side":
            side_rows.append(raw)

    path_plus_side_rows = _merge_member_rows(path_rows, side_rows)
    total_count = len(rows)
    min_exact_count = max(3, int(ceil(total_count * 0.16)))
    min_path_count = max(4, int(ceil(total_count * 0.18)))
    min_path_side_count = max(5, int(ceil(total_count * 0.22)))

    selected_rows: list[dict[str, Any]] = []
    selection_mode = ""
    if effective_confidence == "high":
        if len(exact_rows) >= min_exact_count:
            selected_rows = exact_rows
            selection_mode = "exact_only"
        elif len(path_rows) >= min_path_count:
            selected_rows = path_rows
            selection_mode = "path_only"
        elif len(path_plus_side_rows) >= min_path_side_count:
            selected_rows = path_plus_side_rows
            selection_mode = "path_plus_side"
    else:
        if len(path_plus_side_rows) >= min_path_side_count:
            selected_rows = path_plus_side_rows
            selection_mode = "path_plus_side"
        elif len(path_rows) >= min_path_count:
            selected_rows = path_rows
            selection_mode = "path_only"
        elif len(exact_rows) >= min_exact_count:
            selected_rows = exact_rows
            selection_mode = "exact_only"

    if not selected_rows:
        return {}

    selected_keys = {_member_row_key(raw, idx) for idx, raw in enumerate(selected_rows)}
    rejected_rows = [
        dict(raw)
        for idx, raw in enumerate(rows)
        if _member_row_key(raw, idx) not in selected_keys
    ]
    rejected_count = max(0, total_count - len(selected_rows))
    rejected_share = (float(rejected_count) / float(total_count)) if total_count else 0.0
    min_rejected_share = 0.12 if effective_confidence == "high" else 0.18

    try:
        subset_summary = summarize_member_path_rows(selected_rows)
    except Exception:
        return {}

    rejected_summary: dict[str, Any] = {}
    if rejected_rows:
        try:
            rejected_summary = summarize_member_path_rows(rejected_rows)
        except Exception:
            rejected_summary = {}

    summary = dict(subset_summary.get("summary") or {})
    probs = dict(subset_summary.get("probabilities") or {})
    diag = dict(subset_summary.get("diagnostics") or {})
    rejected_summary_data = dict(rejected_summary.get("summary") or {})

    return {
        "matched_subset_available": True,
        "matched_subset_active": rejected_share >= min_rejected_share,
        "matched_subset_reason": subset_reason,
        "matched_subset_selection_mode": selection_mode,
        "matched_member_count": int(subset_summary.get("member_count") or len(selected_rows)),
        "matched_member_share": round(float(len(selected_rows)) / float(total_count), 3),
        "rejected_member_count": int(rejected_count),
        "rejected_member_share": round(rejected_share, 3),
        "matched_dominant_path": str(summary.get("dominant_path") or ""),
        "matched_dominant_path_detail": str(summary.get("dominant_path_detail") or ""),
        "matched_dominant_prob": _safe_float(summary.get("dominant_prob")),
        "matched_dominant_detail_prob": _safe_float(summary.get("dominant_detail_prob")),
        "matched_dominant_margin_prob": _safe_float(summary.get("dominant_margin_prob")),
        "matched_split_state": str(summary.get("split_state") or ""),
        "matched_transition_detail": str(summary.get("transition_detail") or ""),
        "matched_transition_detail_prob": _safe_float(summary.get("transition_detail_prob")),
        "matched_warm_support_prob": _safe_float(probs.get("warm_support")),
        "matched_transition_prob": _safe_float(probs.get("transition")),
        "matched_cold_suppression_prob": _safe_float(probs.get("cold_suppression")),
        "matched_delta_t850_p10_c": _safe_float(diag.get("delta_t850_p10_c")),
        "matched_delta_t850_p50_c": _safe_float(diag.get("delta_t850_p50_c")),
        "matched_delta_t850_p90_c": _safe_float(diag.get("delta_t850_p90_c")),
        "matched_wind850_p50_kmh": _safe_float(diag.get("wind850_p50_kmh")),
        "rejected_dominant_path": str(rejected_summary_data.get("dominant_path") or ""),
        "rejected_dominant_prob": _safe_float(rejected_summary_data.get("dominant_prob")),
        "matched_side": _path_side(
            str(summary.get("dominant_path") or ""),
            path_detail=str(summary.get("dominant_path_detail") or ""),
        ),
    }


def _build_matched_branch_outlook(
    *,
    phase: str,
    hours_to_peak: float | None,
    hours_to_window_end: float | None,
    observed_path: str,
    observed_path_detail: str,
    alignment_confidence: str,
    ensemble_summary: dict[str, Any],
    ensemble_diag: dict[str, Any],
    matched_subset: dict[str, Any],
    transport_state: str,
    thermal_advection_state: str,
    surface_role: str,
    surface_coupling_state: str,
    h925_coupling_state: str,
    h700_scope: str,
    h700_dry_intrusion_strength: float | None,
    low_level_cap_score: float | None,
    cloud_effective_cover: float | None,
    radiation_eff: float | None,
    precip_state: str,
    second_peak_potential: str,
    multi_peak_state: str,
) -> dict[str, Any]:
    matched_subset_active = bool(matched_subset.get("matched_subset_active"))
    active_source = "ensemble_dominant"
    active_path = str(ensemble_summary.get("dominant_path") or "")
    active_path_detail = str(ensemble_summary.get("dominant_path_detail") or ensemble_summary.get("transition_detail") or active_path)
    active_split_state = str(ensemble_summary.get("split_state") or "")
    active_dominant_prob = _safe_float(ensemble_summary.get("dominant_prob"))
    active_signal_dispersion_c = _safe_float(ensemble_summary.get("signal_dispersion_c"))
    if matched_subset_active and str(matched_subset.get("matched_dominant_path") or ""):
        active_source = "matched_subset"
        active_path = str(matched_subset.get("matched_dominant_path") or "")
        active_path_detail = str(matched_subset.get("matched_dominant_path_detail") or matched_subset.get("matched_transition_detail") or active_path)
        active_split_state = str(matched_subset.get("matched_split_state") or active_split_state)
        active_dominant_prob = _safe_float(matched_subset.get("matched_dominant_prob"))
        p10 = _safe_float(matched_subset.get("matched_delta_t850_p10_c"))
        p90 = _safe_float(matched_subset.get("matched_delta_t850_p90_c"))
        if p10 is not None and p90 is not None:
            active_signal_dispersion_c = round(float(p90) - float(p10), 2)
    elif alignment_confidence in {"high", "partial"} and observed_path:
        active_source = "observed_path"
        active_path = str(observed_path or "")
        active_path_detail = str(observed_path_detail or active_path or "")

    active_side = _path_side(active_path, path_detail=active_path_detail)
    precip_key = str(precip_state or "").strip().lower()
    precip_active = precip_key not in {"", "none"}
    cloud_locked = bool(
        cloud_effective_cover is not None
        and radiation_eff is not None
        and float(cloud_effective_cover) >= 0.68
        and float(radiation_eff) <= 0.58
    )
    warm_landing_pending = bool(
        transport_state == "warm"
        and (
            str(thermal_advection_state or "").strip().lower() in {"weak", "probable"}
            or str(surface_role or "").strip().lower() in {"background", "low_representativeness"}
            or str(surface_coupling_state or "").strip().lower() in {"partial", "weak", "none"}
            or str(h925_coupling_state or "").strip().lower() in {"partial", "weak", "decoupled", "none", ""}
        )
    )
    warm_landing_ready = bool(
        (
            transport_state == "warm"
            and str(surface_coupling_state or "").strip().lower() == "strong"
        )
        or (
            transport_state == "warm"
            and str(h925_coupling_state or "").strip().lower() == "strong"
            and (radiation_eff is None or float(radiation_eff) >= 0.72)
            and (cloud_effective_cover is None or float(cloud_effective_cover) <= 0.40)
        )
    )
    cold_landing_ready = bool(
        transport_state == "cold"
        and (
            str(thermal_advection_state or "").strip().lower() in {"probable", "confirmed"}
            or str(surface_coupling_state or "").strip().lower() == "strong"
            or str(h925_coupling_state or "").strip().lower() == "strong"
        )
    )
    dry_mix_support = bool(
        str(h700_scope or "").strip().lower() in {"near", "station"}
        and h700_dry_intrusion_strength is not None
        and float(h700_dry_intrusion_strength) >= 8.0
        and (radiation_eff is None or float(radiation_eff) >= 0.72)
        and (cloud_effective_cover is None or float(cloud_effective_cover) <= 0.35)
    )
    cap_hold = bool((low_level_cap_score or 0.0) >= 0.55)
    second_peak_watch = bool(
        str(second_peak_potential or "").strip().lower() in {"moderate", "high"}
        or str(multi_peak_state or "").strip().lower() == "likely"
    )

    volatility_score = 0
    if active_split_state == "split":
        volatility_score += 3
    elif active_split_state == "mixed":
        volatility_score += 2
    elif active_split_state == "clustered":
        volatility_score += 0
    if active_signal_dispersion_c is not None:
        if float(active_signal_dispersion_c) >= 2.5:
            volatility_score += 2
        elif float(active_signal_dispersion_c) >= 1.6:
            volatility_score += 1
    rejected_share = _safe_float(matched_subset.get("rejected_member_share"))
    if rejected_share is not None and rejected_share >= 0.35:
        volatility_score += 1
    if second_peak_watch:
        volatility_score += 1
    if active_path_detail in {"weak_warm_transition", "weak_cold_transition"}:
        volatility_score += 1

    if volatility_score >= 4:
        branch_volatility = "high"
    elif volatility_score >= 2:
        branch_volatility = "medium"
    else:
        branch_volatility = "low"

    family = "mixed_transition"
    stage_now = "tracking"
    next_gate = "follow_through"
    expected_family = family
    expected_stage = "tracking"
    fallback_family = "neutral_plateau"
    fallback_stage = "holding"

    if second_peak_watch:
        family = "second_peak_retest"
        stage_now = "watch"
        next_gate = "rebreak_signal"
        expected_family = "second_peak_retest"
        expected_stage = "retest"
        fallback_family = "neutral_plateau"
        fallback_stage = "stalling"
    elif precip_active and active_side == "warm":
        family = "convective_interrupt_risk"
        stage_now = "interrupt_watch"
        next_gate = "convective_intrusion"
        expected_family = "convective_interrupt_risk"
        expected_stage = "interrupting"
        fallback_family = "warm_support_track"
        fallback_stage = "resuming"
    elif precip_active and active_side == "cold":
        family = "convective_cold_hold"
        stage_now = "holding"
        next_gate = "convective_persistence"
        expected_family = "cold_suppression_track"
        expected_stage = "holding"
        fallback_family = "neutral_plateau"
        fallback_stage = "releasing"
    elif cloud_locked and active_side in {"warm", "neutral"}:
        family = "cloud_release_watch"
        stage_now = "locked"
        next_gate = "cloud_release"
        expected_family = "warm_support_track" if active_side == "warm" else "warm_transition_probe"
        expected_stage = "building"
        fallback_family = "neutral_plateau"
        fallback_stage = "stalling"
    elif warm_landing_pending:
        family = "warm_landing_watch"
        stage_now = "pending"
        next_gate = "low_level_coupling"
        expected_family = "warm_support_track"
        expected_stage = "building"
        fallback_family = "neutral_plateau"
        fallback_stage = "stalling"
    elif warm_landing_ready:
        family = "warm_support_track"
        stage_now = "verified"
        next_gate = "follow_through"
        expected_family = "warm_support_track"
        expected_stage = "follow_through"
        fallback_family = "neutral_plateau"
        fallback_stage = "stalling"
    elif cold_landing_ready:
        family = "cold_suppression_track"
        stage_now = "holding"
        next_gate = "cold_advection_hold"
        expected_family = "cold_suppression_track"
        expected_stage = "holding"
        fallback_family = "neutral_plateau"
        fallback_stage = "releasing"
    elif active_path_detail == "weak_warm_transition":
        family = "warm_transition_probe"
        stage_now = "testing"
        next_gate = "low_level_coupling"
        expected_family = "warm_support_track"
        expected_stage = "building"
        fallback_family = "neutral_plateau"
        fallback_stage = "stalling"
    elif active_path_detail == "weak_cold_transition":
        family = "cold_transition_probe"
        stage_now = "testing"
        next_gate = "cold_advection_hold"
        expected_family = "cold_suppression_track"
        expected_stage = "holding"
        fallback_family = "neutral_plateau"
        fallback_stage = "releasing"
    elif branch_volatility == "high" and active_split_state in {"mixed", "split"}:
        family = "volatile_split"
        stage_now = "swinging"
        next_gate = "branch_resolution"
        expected_family = (
            "warm_support_track" if active_side == "warm" else (
                "cold_suppression_track" if active_side == "cold" else "neutral_plateau"
            )
        )
        expected_stage = "building" if active_side == "warm" else ("holding" if active_side == "cold" else "holding")
        fallback_family = "volatile_split"
        fallback_stage = "swinging"
    elif active_path_detail == "neutral_stable":
        family = "neutral_plateau"
        stage_now = "holding"
        next_gate = "reacceleration"
        if transport_state == "warm":
            expected_family = "warm_transition_probe"
            expected_stage = "testing"
        elif transport_state == "cold":
            expected_family = "cold_transition_probe"
            expected_stage = "testing"
        else:
            expected_family = "neutral_plateau"
            expected_stage = "holding"
        fallback_family = "neutral_plateau"
        fallback_stage = "holding"
    elif active_side == "warm":
        family = "warm_support_track"
        stage_now = "tracking"
        next_gate = "follow_through"
        expected_family = "warm_support_track"
        expected_stage = "follow_through"
        fallback_family = "neutral_plateau"
        fallback_stage = "stalling"
    elif active_side == "cold":
        family = "cold_suppression_track"
        stage_now = "tracking"
        next_gate = "cold_advection_hold"
        expected_family = "cold_suppression_track"
        expected_stage = "holding"
        fallback_family = "neutral_plateau"
        fallback_stage = "releasing"

    expected_prob = 0.58
    dominant_prob_v = float(active_dominant_prob) if active_dominant_prob is not None else 0.50
    expected_prob += (dominant_prob_v - 0.50) * 0.35
    if matched_subset_active:
        expected_prob += 0.06
    elif active_source == "observed_path":
        expected_prob -= 0.04
    if warm_landing_ready or cold_landing_ready:
        expected_prob += 0.10
    if dry_mix_support:
        expected_prob += 0.05
    if warm_landing_pending:
        expected_prob -= 0.05
    if cloud_locked:
        expected_prob -= 0.10
    if precip_active:
        expected_prob += 0.06 if active_side == "cold" else -0.14
    if cap_hold:
        expected_prob += 0.04 if active_side == "cold" else -0.05
    if branch_volatility == "medium":
        expected_prob -= 0.06
    elif branch_volatility == "high":
        expected_prob -= 0.12
    if second_peak_watch:
        expected_prob -= 0.08
    if hours_to_peak is not None and float(hours_to_peak) <= 0.75 and family in {"warm_landing_watch", "warm_transition_probe"}:
        expected_prob -= 0.04
    if hours_to_window_end is not None and float(hours_to_window_end) <= 0.60 and family in {"convective_interrupt_risk", "convective_cold_hold"}:
        expected_prob += 0.04
    expected_prob = round(_clamp(expected_prob, 0.28, 0.86), 3)

    fallback_prob = 1.0 - expected_prob
    if branch_volatility == "high":
        fallback_prob += 0.08
    elif branch_volatility == "medium":
        fallback_prob += 0.04
    if family == "volatile_split":
        fallback_prob = max(fallback_prob, 0.42)
    fallback_prob = round(_clamp(fallback_prob, 0.14, 0.68), 3)

    return {
        "branch_source": active_source,
        "branch_family": family,
        "branch_stage_now": stage_now,
        "branch_path": active_path,
        "branch_path_detail": active_path_detail,
        "branch_side": active_side,
        "branch_split_state": active_split_state,
        "branch_dominant_prob": round(dominant_prob_v, 3),
        "branch_signal_dispersion_c": active_signal_dispersion_c,
        "branch_volatility": branch_volatility,
        "branch_volatility_score": float(volatility_score),
        "next_transition_gate": next_gate,
        "expected_next_family": expected_family,
        "expected_next_stage": expected_stage,
        "expected_follow_through_prob": expected_prob,
        "fallback_family": fallback_family,
        "fallback_stage": fallback_stage,
        "fallback_prob": fallback_prob,
        "cloud_locked": cloud_locked,
        "precip_active": precip_active,
        "warm_landing_pending": warm_landing_pending,
        "warm_landing_ready": warm_landing_ready,
        "cold_landing_ready": cold_landing_ready,
        "dry_mix_support": dry_mix_support,
        "cap_hold": cap_hold,
        "second_peak_watch": second_peak_watch,
        "matched_subset_active": matched_subset_active,
        "matched_member_share": _safe_float(matched_subset.get("matched_member_share")),
        "rejected_member_share": _safe_float(matched_subset.get("rejected_member_share")),
    }


def build_posterior_feature_vector(
    *,
    canonical_raw_state: dict[str, Any],
    boundary_layer_regime: dict[str, Any] | None = None,
    temp_phase_decision: dict[str, Any] | None = None,
) -> dict[str, Any]:
    raw = canonical_raw_state if isinstance(canonical_raw_state, dict) else {}
    obs = dict(raw.get("observations") or {})
    forecast = dict(raw.get("forecast") or {})
    window = dict(raw.get("window") or {})
    primary_window = dict(window.get("primary") or {})
    calc_window = dict(window.get("calc") or {})
    shape = dict(raw.get("shape") or {})
    shape_forecast = dict(shape.get("forecast") or {})
    shape_observed = dict(shape.get("observed") or {})
    quality = dict(forecast.get("quality") or {})
    context = dict(forecast.get("context") or {})
    source = dict(raw.get("source") or {})
    ensemble_factor = dict(forecast.get("ensemble_factor") or {})
    ensemble_summary = dict(ensemble_factor.get("summary") or {})
    ensemble_probs = dict(ensemble_factor.get("probabilities") or {})
    ensemble_diag = dict(ensemble_factor.get("diagnostics") or {})
    h850_review = dict(forecast.get("h850_review") or {})
    h700 = dict(forecast.get("h700") or {})
    h925 = dict(forecast.get("h925") or {})
    sounding = dict(forecast.get("sounding") or {})
    track_summary = dict(forecast.get("track_summary") or {})
    thermo = dict(sounding.get("thermo") or {})
    coverage = dict(thermo.get("coverage") or {})
    relationships = dict(thermo.get("layer_relationships") or {})

    regime = boundary_layer_regime if isinstance(boundary_layer_regime, dict) else {}
    phase = temp_phase_decision if isinstance(temp_phase_decision, dict) else {}

    latest_temp_c = _safe_float(obs.get("latest_temp_c"))
    observed_max_temp_c = _safe_float(obs.get("observed_max_temp_c"))
    peak_temp_c = _safe_float(primary_window.get("peak_temp_c"))
    latest_report_local = obs.get("latest_report_local")
    peak_local = calc_window.get("peak_local") or primary_window.get("peak_local")
    start_local = calc_window.get("start_local") or primary_window.get("start_local")
    end_local = calc_window.get("end_local") or primary_window.get("end_local")
    phase_name = str(phase.get("phase") or "")
    hours_to_window_start = _hours_between_iso(start_local, latest_report_local)
    hours_to_peak = _hours_between_iso(peak_local, latest_report_local)
    hours_to_window_end = _hours_between_iso(end_local, latest_report_local)

    dewpoint_c = _safe_float(obs.get("latest_dewpoint_c"))
    cloud_effective_cover = _safe_float(obs.get("cloud_effective_cover"))
    radiation_eff = _safe_float(obs.get("radiation_eff"))
    cloud_trend = str(obs.get("cloud_trend") or "")
    precip_state = str(obs.get("precip_state") or "")
    thermal_advection_state = str(h850_review.get("thermal_advection_state") or "")
    transport_state_label = str(h850_review.get("transport_state") or "")
    surface_bias = str(h850_review.get("surface_bias") or "")
    dewpoint_spread_c = None
    if latest_temp_c is not None and dewpoint_c is not None:
        dewpoint_spread_c = latest_temp_c - dewpoint_c

    gap_to_observed_max_c = None
    if observed_max_temp_c is not None and latest_temp_c is not None:
        gap_to_observed_max_c = observed_max_temp_c - latest_temp_c

    latest_gap_below_observed_c = None
    if observed_max_temp_c is not None and latest_temp_c is not None:
        latest_gap_below_observed_c = max(0.0, observed_max_temp_c - latest_temp_c)

    forecast_peak_minus_latest_c = None
    if peak_temp_c is not None and latest_temp_c is not None:
        forecast_peak_minus_latest_c = peak_temp_c - latest_temp_c

    observed_progress_anchor_c = None
    if observed_max_temp_c is not None and latest_temp_c is not None:
        observed_progress_anchor_c = max(observed_max_temp_c, latest_temp_c)
    elif observed_max_temp_c is not None:
        observed_progress_anchor_c = observed_max_temp_c
    elif latest_temp_c is not None:
        observed_progress_anchor_c = latest_temp_c

    modeled_headroom_c = None
    if peak_temp_c is not None and observed_progress_anchor_c is not None:
        modeled_headroom_c = peak_temp_c - observed_progress_anchor_c

    time_since_observed_peak_h = None
    observed_peak_local = obs.get("observed_max_time_local")
    if observed_peak_local:
        age_h = _hours_between_iso(latest_report_local, observed_peak_local)
        if age_h is not None and age_h >= 0.0:
            time_since_observed_peak_h = age_h

    reports_since_observed_peak = None
    cadence_for_reports = _safe_float(obs.get("metar_routine_cadence_min"))
    if (
        time_since_observed_peak_h is not None
        and cadence_for_reports is not None
        and cadence_for_reports > 0.0
    ):
        reports_since_observed_peak = int(
            max(0.0, (time_since_observed_peak_h * 60.0) / cadence_for_reports)
        )

    ensemble_alignment = _live_ensemble_path_alignment(
        temp_trend_c=_safe_float(obs.get("temp_trend_c")),
        temp_bias_c=_safe_float(obs.get("temp_bias_c")),
        cloud_effective_cover=cloud_effective_cover,
        radiation_eff=radiation_eff,
        cloud_trend=cloud_trend,
        precip_state=precip_state,
        transport_state=transport_state_label,
        thermal_advection_state=thermal_advection_state,
        surface_bias=surface_bias,
        dominant_path=str(ensemble_summary.get("dominant_path") or ""),
        dominant_path_detail=str(ensemble_summary.get("dominant_path_detail") or ensemble_summary.get("transition_detail") or ""),
        dominant_prob=_safe_float(ensemble_summary.get("dominant_prob")),
        dominant_detail_prob=_safe_float(ensemble_summary.get("dominant_detail_prob")),
        dominant_margin_prob=_safe_float(ensemble_summary.get("dominant_margin_prob")),
    )
    matched_subset = _build_observation_matched_subset(
        members=list(ensemble_factor.get("members") or []),
        phase=phase_name,
        hours_to_peak=hours_to_peak,
        observed_path=str(ensemble_alignment.get("observed_path") or ""),
        observed_path_detail=str(ensemble_alignment.get("observed_path_detail") or ""),
        observed_path_score=_safe_float(ensemble_alignment.get("observed_path_score")),
        match_state=str(ensemble_alignment.get("observed_alignment_match_state") or ""),
        confidence=str(ensemble_alignment.get("observed_alignment_confidence") or ""),
    )
    matched_branch_outlook = _build_matched_branch_outlook(
        phase=phase_name,
        hours_to_peak=hours_to_peak,
        hours_to_window_end=hours_to_window_end,
        observed_path=str(ensemble_alignment.get("observed_path") or ""),
        observed_path_detail=str(ensemble_alignment.get("observed_path_detail") or ""),
        alignment_confidence=str(ensemble_alignment.get("observed_alignment_confidence") or ""),
        ensemble_summary=ensemble_summary,
        ensemble_diag=ensemble_diag,
        matched_subset=matched_subset,
        transport_state=transport_state_label,
        thermal_advection_state=thermal_advection_state,
        surface_role=str(h850_review.get("surface_role") or ""),
        surface_coupling_state=str(h850_review.get("surface_coupling_state") or ""),
        h925_coupling_state=str(h925.get("coupling_state") or ""),
        h700_scope=str(h700.get("dry_intrusion_scope") or ""),
        h700_dry_intrusion_strength=_safe_float(h700.get("dry_intrusion_strength")),
        low_level_cap_score=_safe_float(thermo.get("low_level_cap_score")),
        cloud_effective_cover=cloud_effective_cover,
        radiation_eff=radiation_eff,
        precip_state=precip_state,
        second_peak_potential=str(phase.get("second_peak_potential") or ""),
        multi_peak_state=str(shape_forecast.get("multi_peak_state") or ""),
    )

    return {
        "schema_version": POSTERIOR_FEATURE_VECTOR_SCHEMA_VERSION,
        "unit": str(raw.get("unit") or "C"),
        "meta": {
            "station": str(((forecast.get("meta") or {}).get("station")) or ""),
            "date": str(((forecast.get("meta") or {}).get("date")) or ""),
            "model": str(((forecast.get("meta") or {}).get("model")) or ""),
            "synoptic_provider": str(((forecast.get("meta") or {}).get("synoptic_provider")) or ""),
            "runtime": str(((forecast.get("meta") or {}).get("runtime")) or ""),
        },
        "time_phase": {
            "phase": phase_name,
            "display_phase": str(phase.get("display_phase") or ""),
            "hours_to_window_start": hours_to_window_start,
            "hours_to_peak": hours_to_peak,
            "hours_to_window_end": hours_to_window_end,
            "window_width_h": _hours_between_iso(end_local, start_local),
            "analysis_window_mode": str(source.get("analysis_window_mode") or ""),
            "analysis_window_override_active": bool(source.get("analysis_window_override_active")),
        },
        "observation_state": {
            "latest_temp_c": latest_temp_c,
            "observed_max_temp_c": observed_max_temp_c,
            "gap_to_observed_max_c": gap_to_observed_max_c,
            "latest_gap_below_observed_c": latest_gap_below_observed_c,
            "observed_progress_anchor_c": observed_progress_anchor_c,
            "modeled_headroom_c": modeled_headroom_c,
            "time_since_observed_peak_h": time_since_observed_peak_h,
            "reports_since_observed_peak": reports_since_observed_peak,
            "forecast_peak_minus_latest_c": forecast_peak_minus_latest_c,
            "temp_trend_c": _safe_float(obs.get("temp_trend_c")),
            "temp_trend_effective_c": _safe_float(obs.get("temp_trend_effective_c")),
            "temp_bias_c": _safe_float(obs.get("temp_bias_c")),
            "temp_accel_2step_c": _safe_float(obs.get("temp_accel_2step_c")),
            "temp_accel_raw_2step_c": _safe_float(obs.get("temp_accel_raw_2step_c")),
            "peak_lock_confirmed": bool(obs.get("peak_lock_confirmed")),
        },
        "cloud_radiation_state": {
            "latest_cloud_code": str(obs.get("latest_cloud_code") or ""),
            "latest_cloud_lowest_base_ft": _safe_float(obs.get("latest_cloud_lowest_base_ft")),
            "cloud_effective_cover": cloud_effective_cover,
            "radiation_eff": radiation_eff,
            "cloud_trend": cloud_trend,
            "low_cloud_pct_model": _safe_float(primary_window.get("low_cloud_pct")),
        },
        "moisture_stability_state": {
            "latest_rh": _safe_float(obs.get("latest_rh")),
            "dewpoint_spread_c": dewpoint_spread_c,
            "precip_state": precip_state,
            "precip_trend": str(obs.get("precip_trend") or ""),
            "latest_wx": str(obs.get("latest_wx") or ""),
        },
        "mixing_coupling_state": {
            "latest_wspd_kt": _safe_float(obs.get("latest_wspd_kt")),
            "latest_wdir_deg": _safe_float(obs.get("latest_wdir_deg")),
            "wind_dir_change_deg": _safe_float(obs.get("wind_dir_change_deg")),
            "w850_kmh": _safe_float(primary_window.get("w850_kmh")),
            "surface_coupling_state": str(h850_review.get("surface_coupling_state") or ""),
            "mixing_support_score": _safe_float(thermo.get("mixing_support_score")),
            "wind_profile_mix_score": _safe_float(thermo.get("wind_profile_mix_score")),
        },
        "transport_state": {
            "thermal_advection_state": thermal_advection_state,
            "transport_state": transport_state_label,
            "surface_role": str(h850_review.get("surface_role") or ""),
            "surface_bias": surface_bias,
            "surface_effect_weight": _safe_float(h850_review.get("surface_effect_weight")),
            "timing_score": _safe_float(h850_review.get("timing_score")),
            "reach_score": _safe_float(h850_review.get("reach_score")),
            "distance_km": _safe_float(h850_review.get("distance_km")),
        },
        "vertical_structure_state": {
            "profile_source": str(thermo.get("profile_source") or ""),
            "sounding_confidence": str(thermo.get("sounding_confidence") or ""),
            "coverage_density": str(coverage.get("density_class") or ""),
            "h700_source": str(h700.get("source") or ""),
            "h700_scope": str(h700.get("dry_intrusion_scope") or ""),
            "h700_distance_km": _safe_float(h700.get("dry_intrusion_nearest_km")),
            "h700_dry_intrusion_strength": _safe_float(h700.get("dry_intrusion_strength")),
            "h925_coupling_state": str(h925.get("coupling_state") or ""),
            "h925_landing_signal": str(h925.get("landing_signal") or ""),
            "h925_coupling_score": _safe_float(h925.get("coupling_score")),
            "t925_t850_c": _safe_float(thermo.get("t925_t850_c")),
            "rh925_pct": _safe_float(thermo.get("rh925_pct")),
            "rh850_pct": _safe_float(thermo.get("rh850_pct")),
            "rh700_pct": _safe_float(thermo.get("rh700_pct")),
            "midlevel_rh_pct": _safe_float(thermo.get("midlevel_rh_pct")),
            "low_level_cap_score": _safe_float(thermo.get("low_level_cap_score")),
            "midlevel_dry_score": _safe_float(thermo.get("midlevel_dry_score")),
            "midlevel_moist_score": _safe_float(thermo.get("midlevel_moist_score")),
            "thermal_structure": str(relationships.get("thermal_structure") or ""),
            "moisture_layering": str(relationships.get("moisture_layering") or ""),
            "wind_turning_state": str(relationships.get("wind_turning_state") or ""),
            "coupling_chain_state": str(relationships.get("coupling_chain_state") or ""),
        },
        "forecast_shape_state": {
            "shape_type": str(shape_forecast.get("shape_type") or ""),
            "multi_peak_state": str(shape_forecast.get("multi_peak_state") or ""),
            "plateau_state": str(shape_forecast.get("plateau_state") or ""),
            "observed_plateau_state": str(shape_observed.get("plateau_state") or ""),
            "observed_plateau_hold_h": _safe_float(shape_observed.get("hold_duration_hours")),
        },
        "peak_phase_state": {
            "short_term_state": str(phase.get("short_term_state") or ""),
            "daily_peak_state": str(phase.get("daily_peak_state") or ""),
            "second_peak_potential": str(phase.get("second_peak_potential") or ""),
            "rebound_mode": str(phase.get("rebound_mode") or ""),
            "dominant_shape": str(phase.get("dominant_shape") or ""),
            "plateau_hold_state": str(phase.get("plateau_hold_state") or ""),
        },
        "track_state": {
            "track_count": _safe_float(track_summary.get("track_count")),
            "anchor_count": _safe_float(track_summary.get("anchor_count")),
            "main_track_type": str(track_summary.get("main_track_type") or ""),
            "main_track_evolution": str(track_summary.get("main_track_evolution") or ""),
            "main_track_intensity_trend": str(track_summary.get("main_track_intensity_trend") or ""),
            "main_track_distance_km": _safe_float(track_summary.get("main_track_distance_km")),
            "main_track_closest_distance_km": _safe_float(track_summary.get("main_track_closest_distance_km")),
            "main_track_anchors_count": _safe_float(track_summary.get("main_track_anchors_count")),
            "main_track_confidence": str(track_summary.get("main_track_confidence") or ""),
            "main_track_closest_time_local": str(track_summary.get("main_track_closest_time_local") or ""),
        },
        "regime_state": {
            "regime_key": str(regime.get("regime_key") or ""),
            "dominant_mechanism": str(regime.get("dominant_mechanism") or ""),
            "confidence": str(regime.get("confidence") or ""),
            "advection_role": str(regime.get("advection_role") or ""),
            "bottleneck_code": str(context.get("bottleneck_code") or ""),
            "bottleneck_polarity": str(context.get("bottleneck_polarity") or ""),
            "bottleneck_source": str(context.get("bottleneck_source") or ""),
        },
        "quality_state": {
            "source_state": str(quality.get("source_state") or ""),
            "missing_layers": list(quality.get("missing_layers") or []),
            "synoptic_coverage": _safe_float(quality.get("synoptic_coverage")),
            "synoptic_provider_requested": str(quality.get("synoptic_provider_requested") or ""),
            "synoptic_provider_used": str(quality.get("synoptic_provider_used") or ""),
            "synoptic_provider_fallback": bool(quality.get("synoptic_provider_fallback")),
            "metar_temp_quantized": bool(obs.get("metar_temp_quantized")),
            "metar_routine_cadence_min": _safe_float(obs.get("metar_routine_cadence_min")),
            "metar_recent_interval_min": _safe_float(obs.get("metar_recent_interval_min")),
            "metar_prev_interval_min": _safe_float(obs.get("metar_prev_interval_min")),
            "metar_speci_active": bool(obs.get("metar_speci_active")),
            "metar_speci_likely": bool(obs.get("metar_speci_likely")),
        },
        "ensemble_path_state": {
            "provider": str((ensemble_factor.get("source") or {}).get("provider") or ""),
            "member_count": _safe_float(ensemble_factor.get("member_count")),
            "dominant_path": str(ensemble_summary.get("dominant_path") or ""),
            "dominant_path_detail": str(ensemble_summary.get("dominant_path_detail") or ""),
            "dominant_prob": _safe_float(ensemble_summary.get("dominant_prob")),
            "dominant_detail_prob": _safe_float(ensemble_summary.get("dominant_detail_prob")),
            "dominant_margin_prob": _safe_float(ensemble_summary.get("dominant_margin_prob")),
            "split_state": str(ensemble_summary.get("split_state") or ""),
            "signal_dispersion_c": _safe_float(ensemble_summary.get("signal_dispersion_c")),
            "transition_detail": str(ensemble_summary.get("transition_detail") or ""),
            "transition_detail_prob": _safe_float(ensemble_summary.get("transition_detail_prob")),
            "warm_support_prob": _safe_float(ensemble_probs.get("warm_support")),
            "transition_prob": _safe_float(ensemble_probs.get("transition")),
            "cold_suppression_prob": _safe_float(ensemble_probs.get("cold_suppression")),
            "delta_t850_p10_c": _safe_float(ensemble_diag.get("delta_t850_p10_c")),
            "delta_t850_p50_c": _safe_float(ensemble_diag.get("delta_t850_p50_c")),
            "delta_t850_p90_c": _safe_float(ensemble_diag.get("delta_t850_p90_c")),
            "wind850_p50_kmh": _safe_float(ensemble_diag.get("wind850_p50_kmh")),
            "neutral_stable_prob": _safe_float(ensemble_diag.get("neutral_stable_prob")),
            "weak_warm_transition_prob": _safe_float(ensemble_diag.get("weak_warm_transition_prob")),
            "weak_cold_transition_prob": _safe_float(ensemble_diag.get("weak_cold_transition_prob")),
            "observed_path": str(ensemble_alignment.get("observed_path") or ""),
            "observed_path_detail": str(ensemble_alignment.get("observed_path_detail") or ""),
            "observed_path_score": _safe_float(ensemble_alignment.get("observed_path_score")),
            "observed_warm_signal": _safe_float(ensemble_alignment.get("observed_warm_signal")),
            "observed_cold_signal": _safe_float(ensemble_alignment.get("observed_cold_signal")),
            "observed_neutral_signal": _safe_float(ensemble_alignment.get("observed_neutral_signal")),
            "observed_alignment_match_state": str(ensemble_alignment.get("observed_alignment_match_state") or ""),
            "observed_alignment_matches_dominant": bool(ensemble_alignment.get("observed_alignment_matches_dominant")),
            "observed_alignment_exact": bool(ensemble_alignment.get("observed_alignment_exact")),
            "observed_alignment_score": _safe_float(ensemble_alignment.get("observed_alignment_score")),
            "observed_alignment_confidence": str(ensemble_alignment.get("observed_alignment_confidence") or ""),
            "observed_path_locked": bool(ensemble_alignment.get("observed_path_locked")),
            "matched_subset_available": bool(matched_subset.get("matched_subset_available")),
            "matched_subset_active": bool(matched_subset.get("matched_subset_active")),
            "matched_subset_reason": str(matched_subset.get("matched_subset_reason") or ""),
            "matched_subset_selection_mode": str(matched_subset.get("matched_subset_selection_mode") or ""),
            "matched_member_count": _safe_float(matched_subset.get("matched_member_count")),
            "matched_member_share": _safe_float(matched_subset.get("matched_member_share")),
            "rejected_member_count": _safe_float(matched_subset.get("rejected_member_count")),
            "rejected_member_share": _safe_float(matched_subset.get("rejected_member_share")),
            "matched_dominant_path": str(matched_subset.get("matched_dominant_path") or ""),
            "matched_dominant_path_detail": str(matched_subset.get("matched_dominant_path_detail") or ""),
            "matched_dominant_prob": _safe_float(matched_subset.get("matched_dominant_prob")),
            "matched_dominant_detail_prob": _safe_float(matched_subset.get("matched_dominant_detail_prob")),
            "matched_dominant_margin_prob": _safe_float(matched_subset.get("matched_dominant_margin_prob")),
            "matched_split_state": str(matched_subset.get("matched_split_state") or ""),
            "matched_transition_detail": str(matched_subset.get("matched_transition_detail") or ""),
            "matched_transition_detail_prob": _safe_float(matched_subset.get("matched_transition_detail_prob")),
            "matched_warm_support_prob": _safe_float(matched_subset.get("matched_warm_support_prob")),
            "matched_transition_prob": _safe_float(matched_subset.get("matched_transition_prob")),
            "matched_cold_suppression_prob": _safe_float(matched_subset.get("matched_cold_suppression_prob")),
            "matched_delta_t850_p10_c": _safe_float(matched_subset.get("matched_delta_t850_p10_c")),
            "matched_delta_t850_p50_c": _safe_float(matched_subset.get("matched_delta_t850_p50_c")),
            "matched_delta_t850_p90_c": _safe_float(matched_subset.get("matched_delta_t850_p90_c")),
            "matched_wind850_p50_kmh": _safe_float(matched_subset.get("matched_wind850_p50_kmh")),
            "rejected_dominant_path": str(matched_subset.get("rejected_dominant_path") or ""),
            "rejected_dominant_prob": _safe_float(matched_subset.get("rejected_dominant_prob")),
            "matched_side": str(matched_subset.get("matched_side") or ""),
        },
        "matched_branch_outlook_state": {
            "branch_source": str(matched_branch_outlook.get("branch_source") or ""),
            "branch_family": str(matched_branch_outlook.get("branch_family") or ""),
            "branch_stage_now": str(matched_branch_outlook.get("branch_stage_now") or ""),
            "branch_path": str(matched_branch_outlook.get("branch_path") or ""),
            "branch_path_detail": str(matched_branch_outlook.get("branch_path_detail") or ""),
            "branch_side": str(matched_branch_outlook.get("branch_side") or ""),
            "branch_split_state": str(matched_branch_outlook.get("branch_split_state") or ""),
            "branch_dominant_prob": _safe_float(matched_branch_outlook.get("branch_dominant_prob")),
            "branch_signal_dispersion_c": _safe_float(matched_branch_outlook.get("branch_signal_dispersion_c")),
            "branch_volatility": str(matched_branch_outlook.get("branch_volatility") or ""),
            "branch_volatility_score": _safe_float(matched_branch_outlook.get("branch_volatility_score")),
            "next_transition_gate": str(matched_branch_outlook.get("next_transition_gate") or ""),
            "expected_next_family": str(matched_branch_outlook.get("expected_next_family") or ""),
            "expected_next_stage": str(matched_branch_outlook.get("expected_next_stage") or ""),
            "expected_follow_through_prob": _safe_float(matched_branch_outlook.get("expected_follow_through_prob")),
            "fallback_family": str(matched_branch_outlook.get("fallback_family") or ""),
            "fallback_stage": str(matched_branch_outlook.get("fallback_stage") or ""),
            "fallback_prob": _safe_float(matched_branch_outlook.get("fallback_prob")),
            "cloud_locked": bool(matched_branch_outlook.get("cloud_locked")),
            "precip_active": bool(matched_branch_outlook.get("precip_active")),
            "warm_landing_pending": bool(matched_branch_outlook.get("warm_landing_pending")),
            "warm_landing_ready": bool(matched_branch_outlook.get("warm_landing_ready")),
            "cold_landing_ready": bool(matched_branch_outlook.get("cold_landing_ready")),
            "dry_mix_support": bool(matched_branch_outlook.get("dry_mix_support")),
            "cap_hold": bool(matched_branch_outlook.get("cap_hold")),
            "second_peak_watch": bool(matched_branch_outlook.get("second_peak_watch")),
            "matched_subset_active": bool(matched_branch_outlook.get("matched_subset_active")),
            "matched_member_share": _safe_float(matched_branch_outlook.get("matched_member_share")),
            "rejected_member_share": _safe_float(matched_branch_outlook.get("rejected_member_share")),
        },
    }
