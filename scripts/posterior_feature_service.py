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


def _angular_gap_deg(left: Any, right: Any) -> float | None:
    left_v = _safe_float(left)
    right_v = _safe_float(right)
    if left_v is None or right_v is None:
        return None
    gap = abs(float(left_v) - float(right_v)) % 360.0
    return min(gap, 360.0 - gap)


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


def _median(values: list[float]) -> float | None:
    clean = sorted(float(value) for value in values if value is not None)
    if not clean:
        return None
    mid = len(clean) // 2
    if len(clean) % 2 == 1:
        return clean[mid]
    return (clean[mid - 1] + clean[mid]) / 2.0


def _weighted_mean_pairs(pairs: list[tuple[float, float | None]]) -> float | None:
    clean = [(float(weight), float(value)) for weight, value in pairs if weight is not None and value is not None and float(weight) > 0.0]
    if not clean:
        return None
    total_weight = sum(weight for weight, _value in clean)
    if total_weight <= 0.0:
        return None
    return round(sum(weight * value for weight, value in clean) / total_weight, 3)


def _history_alignment_member_map(member_history_alignment: dict[str, Any] | None) -> dict[int, dict[str, Any]]:
    out: dict[int, dict[str, Any]] = {}
    payload = dict(member_history_alignment or {})
    for raw in payload.get("members") or []:
        if not isinstance(raw, dict):
            continue
        try:
            number = int(raw.get("number"))
        except Exception:
            continue
        out[number] = dict(raw)
    return out


def _history_alignment_summary(
    *,
    members: list[dict[str, Any]],
    member_history_alignment: dict[str, Any] | None,
) -> dict[str, Any]:
    rows = [dict(raw) for raw in members if isinstance(raw, dict)]
    history_payload = dict(member_history_alignment or {})
    history_map = _history_alignment_member_map(history_payload)
    matched_time_count = int(history_payload.get("matched_time_count") or 0)
    if not rows or not history_map or matched_time_count <= 0:
        return {
            "history_supported": False,
            "matched_time_count": matched_time_count,
        }

    path_scores: dict[str, float] = {}
    detail_scores: dict[str, float] = {}
    path_member_counts: dict[str, int] = {}
    weighted_alignment_by_path: dict[str, list[tuple[float, float]]] = {}
    weighted_temp_mae_by_path: dict[str, list[tuple[float, float]]] = {}
    weighted_trend_bias_by_path: dict[str, list[tuple[float, float]]] = {}
    used_members = 0
    total_weight = 0.0

    for raw in rows:
        try:
            number = int(raw.get("number"))
        except Exception:
            continue
        history_row = history_map.get(number)
        if not history_row:
            continue
        alignment_score = _safe_float(history_row.get("history_alignment_score"))
        match_count = _safe_float(history_row.get("history_match_count"))
        temp_mae_c = _safe_float(history_row.get("history_temp_mae_c"))
        trend_bias_c = _safe_float(history_row.get("history_trend_bias_c"))
        if alignment_score is None:
            continue

        weight = float(alignment_score)
        if match_count is not None:
            if match_count >= 5:
                weight += 0.12
            elif match_count >= 3:
                weight += 0.08
            elif match_count >= 2:
                weight += 0.04
        if temp_mae_c is not None:
            if temp_mae_c <= 0.55:
                weight += 0.10
            elif temp_mae_c <= 0.95:
                weight += 0.05
            elif temp_mae_c >= 2.4:
                weight -= 0.18
            elif temp_mae_c >= 1.7:
                weight -= 0.10
        if trend_bias_c is not None:
            abs_bias = abs(float(trend_bias_c))
            if abs_bias <= 0.35:
                weight += 0.08
            elif abs_bias <= 0.70:
                weight += 0.04
            elif abs_bias >= 1.6:
                weight -= 0.14
            elif abs_bias >= 1.1:
                weight -= 0.08
        weight = _clamp(weight, 0.04, 1.55)

        row_path = str(raw.get("path_label") or "")
        row_detail = str(raw.get("path_detail") or row_path or "")
        if not row_path:
            continue
        path_scores[row_path] = path_scores.get(row_path, 0.0) + float(weight)
        detail_scores[row_detail] = detail_scores.get(row_detail, 0.0) + float(weight)
        path_member_counts[row_path] = path_member_counts.get(row_path, 0) + 1
        weighted_alignment_by_path.setdefault(row_path, []).append((float(weight), float(alignment_score)))
        if temp_mae_c is not None:
            weighted_temp_mae_by_path.setdefault(row_path, []).append((float(weight), float(temp_mae_c)))
        if trend_bias_c is not None:
            weighted_trend_bias_by_path.setdefault(row_path, []).append((float(weight), float(trend_bias_c)))
        total_weight += float(weight)
        used_members += 1

    if total_weight <= 0.0 or not path_scores:
        return {
            "history_supported": False,
            "matched_time_count": matched_time_count,
        }

    dominant_path, dominant_weight = max(path_scores.items(), key=lambda item: item[1])
    dominant_path_detail = dominant_path
    dominant_detail_weight = -1.0
    for detail, value in detail_scores.items():
        if value <= 0.0:
            continue
        if not any(
            str(raw.get("path_label") or "") == dominant_path and str(raw.get("path_detail") or dominant_path) == detail
            for raw in rows
        ):
            continue
        if value > dominant_detail_weight:
            dominant_path_detail = detail
            dominant_detail_weight = value
    dominant_prob = round(dominant_weight / total_weight, 3)
    remaining = sorted((value for key, value in path_scores.items() if key != dominant_path), reverse=True)
    margin = dominant_weight - (remaining[0] if remaining else 0.0)
    dominant_margin_prob = round(margin / total_weight, 3)
    dominant_alignment_score = _weighted_mean_pairs(weighted_alignment_by_path.get(dominant_path, []))
    dominant_temp_mae_c = _weighted_mean_pairs(weighted_temp_mae_by_path.get(dominant_path, []))
    dominant_trend_bias_c = _weighted_mean_pairs(weighted_trend_bias_by_path.get(dominant_path, []))

    history_supported = bool(
        matched_time_count >= 2
        and used_members >= 4
        and dominant_prob >= 0.56
        and dominant_margin_prob >= 0.10
        and (dominant_alignment_score is None or dominant_alignment_score >= 0.56)
    )
    history_path_locked = bool(
        history_supported
        and dominant_prob >= 0.68
        and dominant_margin_prob >= 0.16
    )

    return {
        "history_supported": history_supported,
        "history_path_locked": history_path_locked,
        "matched_time_count": matched_time_count,
        "used_member_count": used_members,
        "dominant_path": dominant_path,
        "dominant_path_detail": dominant_path_detail,
        "dominant_prob": dominant_prob,
        "dominant_margin_prob": dominant_margin_prob,
        "dominant_alignment_score": dominant_alignment_score,
        "dominant_temp_mae_c": dominant_temp_mae_c,
        "dominant_trend_bias_c": dominant_trend_bias_c,
        "path_member_count": float(path_member_counts.get(dominant_path) or 0),
        "path_member_share": round(float(path_member_counts.get(dominant_path) or 0) / float(len(rows)), 3),
    }


def _member_matches_branch(raw: dict[str, Any], *, branch_path: str, branch_path_detail: str, branch_side: str) -> bool:
    row_path = str(raw.get("path_label") or "")
    row_detail = str(raw.get("path_detail") or row_path or "")
    if branch_path_detail and row_detail == branch_path_detail:
        return True
    if branch_path and row_path == branch_path:
        return True
    if branch_side and _path_side(row_path, path_detail=row_detail) == branch_side:
        return True
    return False


def _build_branch_circulation_signature(
    *,
    members: list[dict[str, Any]],
    branch_path: str,
    branch_path_detail: str,
    branch_side: str,
    transport_state: str,
    warm_landing_pending: bool,
    warm_landing_ready: bool,
    cold_landing_ready: bool,
    dry_mix_support: bool,
    cloud_locked: bool,
    precip_active: bool,
    cap_hold: bool,
) -> tuple[str, float]:
    rows = [dict(raw) for raw in members if isinstance(raw, dict)]
    if not rows:
        return "", 0.0

    branch_rows = [
        raw
        for raw in rows
        if _member_matches_branch(
            raw,
            branch_path=branch_path,
            branch_path_detail=branch_path_detail,
            branch_side=branch_side,
        )
    ]
    if len(branch_rows) < 2:
        return "", 0.0

    has_ens_vertical_detail = any(
        any(raw.get(key) not in (None, "") for key in ("z500_gpm", "rh700_pct", "t850_c", "t925_c", "wind_speed_925_kmh"))
        for raw in rows
    )
    if not has_ens_vertical_detail:
        return "", 0.0

    def _med(source: list[dict[str, Any]], key: str) -> float | None:
        return _median([
            _safe_float(item.get(key))
            for item in source
            if _safe_float(item.get(key)) is not None
        ])

    all_z500 = _med(rows, "z500_gpm")
    branch_z500 = _med(branch_rows, "z500_gpm")
    all_rh700 = _med(rows, "rh700_pct")
    branch_rh700 = _med(branch_rows, "rh700_pct")
    all_t850 = _med(rows, "t850_c")
    branch_t850 = _med(branch_rows, "t850_c")
    all_t925 = _med(rows, "t925_c")
    branch_t925 = _med(branch_rows, "t925_c")
    branch_wind925 = _med(branch_rows, "wind_speed_925_kmh")

    parts: list[str] = []
    score = 0.0

    if branch_z500 is not None and all_z500 is not None:
        dz500 = float(branch_z500) - float(all_z500)
        if dz500 >= 18.0:
            parts.append("500hPa 高度场偏高，脊性背景更占优")
            score += 0.18
        elif dz500 <= -18.0:
            parts.append("500hPa 高度场偏低，槽性背景更占优")
            score += 0.18

    if dry_mix_support or (
        branch_rh700 is not None and (
            float(branch_rh700) <= 45.0
            or (all_rh700 is not None and float(branch_rh700) <= float(all_rh700) - 8.0)
        )
    ):
        parts.append("700hPa 干层混合信号更明显")
        score += 0.22
    elif precip_active or (
        branch_rh700 is not None and float(branch_rh700) >= 72.0
    ):
        parts.append("700hPa 湿层偏厚")
        score += 0.16

    warm_low_level_signal = bool(
        transport_state == "warm"
        and (
            (branch_t925 is not None and all_t925 is not None and float(branch_t925) >= float(all_t925) + 0.35)
            or (branch_t850 is not None and all_t850 is not None and float(branch_t850) >= float(all_t850) + 0.45)
            or (branch_wind925 is not None and float(branch_wind925) >= 18.0)
        )
    )
    cold_low_level_signal = bool(
        transport_state == "cold"
        and (
            (branch_t925 is not None and all_t925 is not None and float(branch_t925) <= float(all_t925) - 0.35)
            or (branch_t850 is not None and all_t850 is not None and float(branch_t850) <= float(all_t850) - 0.45)
        )
    )

    if transport_state == "warm" and (warm_low_level_signal or warm_landing_pending or warm_landing_ready):
        if warm_landing_ready:
            parts.append("925-850hPa 偏暖输送已开始落到地面")
        elif warm_landing_pending:
            parts.append("925-850hPa 偏暖输送仍在，但地面抬温还没完全跟上")
        else:
            parts.append("925-850hPa 偏暖输送更占优")
        score += 0.26
    elif transport_state == "cold" and (cold_low_level_signal or cold_landing_ready):
        if cold_landing_ready:
            parts.append("925-850hPa 冷输送已开始落地")
        else:
            parts.append("925-850hPa 冷输送更占优")
        score += 0.24

    if cloud_locked and not precip_active:
        parts.append("云层约束还在压着地面增温")
        score += 0.18
    elif cap_hold and not precip_active:
        parts.append("低层稳层还在压着混合")
        score += 0.14

    if len(parts) < 2:
        return "", 0.0

    text = "当前更匹配的环流特征是" + "、".join(parts[:3])
    return text, round(_clamp(0.58 + score, 0.0, 0.92), 2)


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


def _branch_member_count(
    *,
    members: list[dict[str, Any]],
    branch_path: str,
    branch_path_detail: str,
) -> tuple[int | None, float | None]:
    rows = [dict(raw) for raw in members if str((raw or {}).get("path_label") or "").strip()]
    if not rows:
        return None, None

    path_label = str(branch_path or "").strip()
    path_detail = str(branch_path_detail or "").strip()
    if not path_label:
        return None, None

    matched_count = 0
    for raw in rows:
        row_path = str(raw.get("path_label") or "").strip()
        row_detail = str(raw.get("path_detail") or row_path or "").strip()
        if path_label == "transition":
            if row_path == "transition" and (not path_detail or row_detail == path_detail):
                matched_count += 1
            continue
        if row_path == path_label:
            matched_count += 1

    total_count = len(rows)
    if total_count <= 0 or matched_count <= 0:
        return 0, 0.0
    return matched_count, round(float(matched_count) / float(total_count), 3)


def _effective_member_count(weights: list[float]) -> float:
    valid = [max(0.0, float(weight)) for weight in weights if float(weight) > 0.0]
    if not valid:
        return 0.0
    total = sum(valid)
    sq_total = sum(weight * weight for weight in valid)
    if total <= 0.0 or sq_total <= 0.0:
        return 0.0
    return (total * total) / sq_total


def _trajectory_member_map(member_trajectory: dict[str, Any]) -> dict[int, dict[str, Any]]:
    out: dict[int, dict[str, Any]] = {}
    for raw in (member_trajectory or {}).get("members") or []:
        try:
            out[int(raw.get("number"))] = dict(raw)
        except Exception:
            continue
    return out


def _member_future_traits(
    *,
    row_path: str,
    row_detail: str,
    row_side: str,
    delta_t850_c: float | None,
    wind_speed_850_kmh: float | None,
    trajectory_row: dict[str, Any],
    branch_outlook_state: dict[str, Any],
    cloud_effective_cover: float | None,
    radiation_eff: float | None,
    precip_state: str,
) -> tuple[str, str, float, float, float]:
    delta = float(delta_t850_c) if delta_t850_c is not None else 0.0
    speed = float(wind_speed_850_kmh) if wind_speed_850_kmh is not None else 0.0
    branch = dict(branch_outlook_state or {})
    warm_pending = bool(branch.get("warm_landing_pending"))
    warm_ready = bool(branch.get("warm_landing_ready"))
    cold_ready = bool(branch.get("cold_landing_ready"))
    second_peak_retest_ready = bool(branch.get("second_peak_retest_ready"))
    precip_active = str(precip_state or "").strip().lower() not in {"", "none"}
    cloud_locked = bool(
        cloud_effective_cover is not None
        and radiation_eff is not None
        and float(cloud_effective_cover) >= 0.68
        and float(radiation_eff) <= 0.58
    )
    trajectory = dict(trajectory_row or {})
    prior_delta = _safe_float(trajectory.get("prior3h_t850_delta_c"))
    next_delta = _safe_float(trajectory.get("next3h_t850_delta_c"))
    prior_surface_delta = _safe_float(trajectory.get("prior3h_t2m_delta_c"))
    next_surface_delta = _safe_float(trajectory.get("next3h_t2m_delta_c"))
    trajectory_accel = _safe_float(trajectory.get("trajectory_accel_c"))
    future_room_c = _safe_float(trajectory.get("future_room_c"))
    future_cooling_c = _safe_float(trajectory.get("future_cooling_c"))
    next_wind10_delta = _safe_float(trajectory.get("next3h_wind10_delta_kmh"))
    next_msl_delta = _safe_float(trajectory.get("next3h_msl_delta_hpa"))
    trajectory_shape = str(trajectory.get("trajectory_shape") or "")
    next_signal_delta = next_surface_delta if next_surface_delta is not None else next_delta
    prior_signal_delta = prior_surface_delta if prior_surface_delta is not None else prior_delta

    if next_signal_delta is not None:
        if second_peak_retest_ready and row_side in {"warm", "neutral"} and next_signal_delta >= 0.08:
            future_family = "second_peak_retest"
        elif precip_active and row_side == "warm" and next_signal_delta <= 0.12:
            future_family = "convective_interrupt_risk"
        elif precip_active and row_side == "cold" and next_signal_delta <= -0.15:
            future_family = "cold_hold"
        elif row_path == "warm_support":
            if next_signal_delta >= 0.38 and (trajectory_accel is None or trajectory_accel >= -0.18):
                future_family = "warm_follow_through"
            elif next_signal_delta >= 0.12:
                future_family = "warm_landing_pending"
            elif future_cooling_c is not None and future_cooling_c >= 0.22:
                future_family = "volatile_transition"
            else:
                future_family = "neutral_plateau"
        elif row_detail == "weak_warm_transition":
            if next_signal_delta >= 0.22:
                future_family = "warm_landing_pending"
            elif next_signal_delta <= -0.18:
                future_family = "volatile_transition"
            else:
                future_family = "neutral_plateau"
        elif row_path == "cold_suppression":
            if next_signal_delta <= -0.25:
                future_family = "cold_hold"
            elif next_signal_delta <= -0.08:
                future_family = "cold_landing_pending"
            elif future_room_c is not None and future_room_c >= 0.20:
                future_family = "volatile_transition"
            else:
                future_family = "neutral_plateau"
        elif row_detail == "weak_cold_transition":
            if next_signal_delta <= -0.14:
                future_family = "cold_landing_pending"
            elif next_signal_delta >= 0.16:
                future_family = "volatile_transition"
            else:
                future_family = "neutral_plateau"
        elif abs(next_signal_delta) <= 0.10:
            future_family = "neutral_plateau"
        else:
            future_family = "volatile_transition"
    elif second_peak_retest_ready and row_side in {"warm", "neutral"}:
        future_family = "second_peak_retest"
    elif precip_active and row_side == "warm":
        future_family = "convective_interrupt_risk"
    elif precip_active and row_side == "cold":
        future_family = "cold_hold"
    elif row_path == "warm_support":
        future_family = "warm_follow_through" if warm_ready or delta >= 0.60 else "warm_landing_pending"
    elif row_detail == "weak_warm_transition":
        future_family = "warm_landing_pending"
    elif row_path == "cold_suppression":
        future_family = "cold_hold" if cold_ready or delta <= -0.60 else "cold_landing_pending"
    elif row_detail == "weak_cold_transition":
        future_family = "cold_landing_pending"
    elif row_detail == "neutral_stable":
        future_family = "neutral_plateau"
    else:
        future_family = "volatile_transition"

    if trajectory_shape in {"warming_follow_through", "reaccelerating"}:
        pace_state = "accelerating"
    elif trajectory_shape in {"warming_but_slowing"}:
        pace_state = "building"
    elif trajectory_shape in {"cooling_follow_through", "warm_reversal"}:
        pace_state = "cooling"
    elif trajectory_shape in {"plateau_after_warming", "flat_hold"}:
        pace_state = "flat"
    elif delta >= 0.90 and speed >= 18.0:
        pace_state = "accelerating"
    elif delta >= 0.25:
        pace_state = "building"
    elif delta <= -0.75:
        pace_state = "cooling"
    elif abs(delta) <= 0.20:
        pace_state = "flat"
    else:
        pace_state = "mixed"

    if next_signal_delta is not None:
        future_room_seed = max(0.0, future_room_c if future_room_c is not None else next_signal_delta)
        accel_bonus = max(0.0, trajectory_accel or 0.0)
        prior_support = max(0.0, prior_signal_delta or 0.0)
        room_factor = {
            "warm_follow_through": 0.86 + future_room_seed * 0.58 + accel_bonus * 0.18 + prior_support * 0.08,
            "warm_landing_pending": 0.68 + future_room_seed * 0.52 + prior_support * 0.06,
            "second_peak_retest": 0.72 + future_room_seed * 0.50,
            "neutral_plateau": 0.26 + future_room_seed * 0.22,
            "volatile_transition": 0.38 + future_room_seed * 0.28 + abs(trajectory_accel or 0.0) * 0.18,
            "cold_landing_pending": 0.18 + future_room_seed * 0.08,
            "cold_hold": 0.10 + future_room_seed * 0.04,
            "convective_interrupt_risk": 0.24 + future_room_seed * 0.16,
        }.get(future_family, 0.40)
        overshoot_factor = {
            "warm_follow_through": max(0.0, future_room_seed - 0.22) * 0.34 + accel_bonus * 0.10,
            "warm_landing_pending": max(0.0, future_room_seed - 0.28) * 0.20,
            "second_peak_retest": max(0.0, future_room_seed - 0.12) * 0.18,
            "volatile_transition": max(0.0, future_room_seed - 0.35) * 0.12,
        }.get(future_family, 0.0)
    else:
        room_factor = {
            "warm_follow_through": 1.06 + max(0.0, delta) * 0.24 + max(0.0, speed - 18.0) * 0.010,
            "warm_landing_pending": 0.82 + max(0.0, delta) * 0.20 + max(0.0, speed - 16.0) * 0.008,
            "second_peak_retest": 0.88 + max(0.0, delta) * 0.18,
            "neutral_plateau": 0.48 + delta * 0.10,
            "volatile_transition": 0.58 + delta * 0.12,
            "cold_landing_pending": 0.26 + max(0.0, delta) * 0.06,
            "cold_hold": 0.12 + max(0.0, delta) * 0.04,
            "convective_interrupt_risk": 0.54 + max(0.0, delta) * 0.10,
        }.get(future_family, 0.48)
        overshoot_factor = {
            "warm_follow_through": max(0.0, delta - 0.20) * 0.34 + max(0.0, speed - 22.0) * 0.010,
            "warm_landing_pending": max(0.0, delta - 0.30) * 0.20,
            "second_peak_retest": max(0.0, delta - 0.10) * 0.18,
            "volatile_transition": max(0.0, delta - 0.40) * 0.12,
        }.get(future_family, 0.0)

    stall_risk = 0.0
    if future_family in {"warm_follow_through", "warm_landing_pending", "second_peak_retest"}:
        if cloud_locked:
            stall_risk += 0.24
        if warm_pending and future_family == "warm_landing_pending":
            stall_risk += 0.16
        if next_signal_delta is not None and next_signal_delta <= 0.08:
            stall_risk += 0.16
        if trajectory_accel is not None and trajectory_accel <= -0.18:
            stall_risk += 0.12
        if next_wind10_delta is not None and next_wind10_delta <= -4.0:
            stall_risk += 0.08
    if future_family in {"convective_interrupt_risk", "cold_hold"}:
        stall_risk += 0.12
    if future_cooling_c is not None and future_cooling_c >= 0.22:
        stall_risk += 0.14
    if next_msl_delta is not None and next_msl_delta >= 1.2 and row_side == "warm":
        stall_risk += 0.08
    if future_family == "neutral_plateau":
        stall_risk += 0.18
    if precip_active and future_family in {"warm_follow_through", "warm_landing_pending", "convective_interrupt_risk"}:
        stall_risk += 0.18

    return (
        future_family,
        pace_state,
        round(_clamp(room_factor, 0.05, 1.75), 3),
        round(_clamp(overshoot_factor, 0.0, 0.65), 3),
        round(_clamp(stall_risk, 0.0, 0.65), 3),
    )


def _member_compatibility_weight(
    *,
    match_tier: str,
    row_path: str,
    row_detail: str,
    row_side: str,
    future_family: str,
    branch_outlook_state: dict[str, Any],
    matched_subset_active: bool,
    temp_trend_c: float | None,
    temp_bias_c: float | None,
    cloud_effective_cover: float | None,
    radiation_eff: float | None,
    precip_state: str,
    surface_temp_gap_c: float | None,
    surface_temp_bias_c: float | None,
    surface_alignment_score: float | None,
    history_alignment_score: float | None,
    history_temp_mae_c: float | None,
    history_trend_bias_c: float | None,
    history_match_count: float | None,
) -> float:
    branch = dict(branch_outlook_state or {})
    branch_family = str(branch.get("branch_family") or "")
    branch_path = str(branch.get("branch_path") or "")
    branch_path_detail = str(branch.get("branch_path_detail") or "")
    branch_side = str(branch.get("branch_side") or "")
    weight = {
        "exact": 0.74,
        "path": 0.54,
        "side": 0.30,
        "": 0.10,
    }.get(str(match_tier or ""), 0.10)

    if row_path == branch_path:
        weight += 0.10
    if row_detail and row_detail == branch_path_detail:
        weight += 0.08
    if row_side and row_side == branch_side:
        weight += 0.06

    if branch_family == "warm_landing_watch":
        if future_family == "warm_landing_pending":
            weight += 0.22
        elif future_family == "warm_follow_through":
            weight += 0.16
        elif future_family == "neutral_plateau":
            weight += 0.05
        else:
            weight -= 0.14
    elif branch_family == "warm_support_track":
        if future_family == "warm_follow_through":
            weight += 0.20
        elif future_family == "warm_landing_pending":
            weight += 0.08
        elif future_family in {"cold_hold", "cold_landing_pending"}:
            weight -= 0.16
    elif branch_family in {"cold_suppression_track", "convective_cold_hold"}:
        if future_family in {"cold_hold", "cold_landing_pending"}:
            weight += 0.18
        elif row_side == "warm":
            weight -= 0.16
    elif branch_family == "volatile_split":
        if future_family == "volatile_transition":
            weight += 0.12
    elif branch_family == "neutral_plateau":
        if future_family == "neutral_plateau":
            weight += 0.16
        elif row_side == "warm":
            weight += 0.04
    elif branch_family == "second_peak_retest":
        if future_family == "second_peak_retest":
            weight += 0.22

    positive_trend = max(0.0, _safe_float(temp_trend_c) or 0.0)
    negative_trend = max(0.0, -(_safe_float(temp_trend_c) or 0.0))
    positive_bias = max(0.0, _safe_float(temp_bias_c) or 0.0)
    negative_bias = max(0.0, -(_safe_float(temp_bias_c) or 0.0))

    if row_side == "warm":
        weight += positive_trend * 0.28 + positive_bias * 0.10
        weight -= negative_trend * 0.22 + negative_bias * 0.08
    elif row_side == "cold":
        weight += negative_trend * 0.24 + negative_bias * 0.08
        weight -= positive_trend * 0.22 + positive_bias * 0.08
    else:
        weight += min(0.06, abs(positive_trend - negative_trend) * 0.08)

    if surface_alignment_score is not None:
        weight += (_clamp(surface_alignment_score, 0.0, 1.0) - 0.55) * 0.28
    if history_alignment_score is not None:
        weight += (_clamp(history_alignment_score, 0.0, 1.0) - 0.55) * 0.34
    if history_match_count is not None:
        if history_match_count >= 5:
            weight += 0.08
        elif history_match_count >= 3:
            weight += 0.05
        elif history_match_count >= 2:
            weight += 0.03
    if surface_temp_gap_c is not None:
        gap = abs(float(surface_temp_gap_c))
        if gap <= 0.45:
            weight += 0.12
        elif gap <= 0.85:
            weight += 0.06
        elif gap >= 2.8:
            weight -= 0.24
        elif gap >= 1.8:
            weight -= 0.12
    if surface_temp_bias_c is not None:
        if row_side == "warm":
            if float(surface_temp_bias_c) <= -0.9:
                weight -= 0.12
            elif float(surface_temp_bias_c) >= 0.35:
                weight += 0.04
        elif row_side == "cold":
            if float(surface_temp_bias_c) >= 0.9:
                weight -= 0.12
            elif float(surface_temp_bias_c) <= -0.35:
                weight += 0.04
    if history_temp_mae_c is not None:
        if float(history_temp_mae_c) <= 0.60:
            weight += 0.10
        elif float(history_temp_mae_c) <= 1.00:
            weight += 0.05
        elif float(history_temp_mae_c) >= 2.40:
            weight -= 0.18
        elif float(history_temp_mae_c) >= 1.70:
            weight -= 0.10
    if history_trend_bias_c is not None:
        abs_trend_bias = abs(float(history_trend_bias_c))
        if abs_trend_bias <= 0.40:
            weight += 0.08
        elif abs_trend_bias <= 0.75:
            weight += 0.04
        elif abs_trend_bias >= 1.60:
            weight -= 0.14
        elif abs_trend_bias >= 1.10:
            weight -= 0.08
        if row_side == "warm":
            if float(history_trend_bias_c) >= 0.30:
                weight += 0.03
            elif float(history_trend_bias_c) <= -0.90:
                weight -= 0.06
        elif row_side == "cold":
            if float(history_trend_bias_c) <= -0.30:
                weight += 0.03
            elif float(history_trend_bias_c) >= 0.90:
                weight -= 0.06

    if (
        cloud_effective_cover is not None
        and radiation_eff is not None
        and float(cloud_effective_cover) >= 0.68
        and float(radiation_eff) <= 0.58
    ):
        if future_family in {"warm_follow_through", "warm_landing_pending"}:
            weight -= 0.12
        elif future_family == "neutral_plateau":
            weight += 0.06
    if str(precip_state or "").strip().lower() not in {"", "none"}:
        if row_side == "warm":
            weight -= 0.16
        elif row_side == "cold":
            weight += 0.08

    if matched_subset_active:
        if match_tier in {"exact", "path", "side"}:
            weight += 0.08
        else:
            weight *= 0.55

    return round(_clamp(weight, 0.02, 1.45), 3)


def _build_member_evolution_state(
    *,
    members: list[dict[str, Any]],
    member_trajectory: dict[str, Any],
    phase: str,
    observed_path: str,
    observed_path_detail: str,
    branch_outlook_state: dict[str, Any],
    matched_subset: dict[str, Any],
    latest_temp_c: float | None,
    latest_dewpoint_c: float | None,
    latest_rh_pct: float | None,
    latest_wspd_kmh: float | None,
    latest_wdir_deg: float | None,
    latest_pressure_hpa: float | None,
    temp_trend_c: float | None,
    temp_bias_c: float | None,
    cloud_effective_cover: float | None,
    radiation_eff: float | None,
    precip_state: str,
    member_history_alignment: dict[str, Any] | None = None,
) -> dict[str, Any]:
    rows = [dict(raw) for raw in members if str((raw or {}).get("path_label") or "").strip()]
    if not rows:
        return {}
    trajectory_by_number = _trajectory_member_map(member_trajectory)

    branch_source = str(branch_outlook_state.get("branch_source") or "")
    active_source = branch_source or (
        "matched_subset" if bool(matched_subset.get("matched_subset_active")) else (
            "observed_path" if observed_path else "full_ensemble"
        )
    )
    weighted_path_scores = {"warm_support": 0.0, "transition": 0.0, "cold_suppression": 0.0}
    weighted_family_scores: dict[str, float] = {}
    member_rows: list[dict[str, Any]] = []
    weights: list[float] = []
    history_map = _history_alignment_member_map(member_history_alignment)

    for raw in rows:
        row_path = str(raw.get("path_label") or "")
        row_detail = str(raw.get("path_detail") or row_path or "")
        row_side = _path_side(row_path, path_detail=row_detail)
        trajectory_row = trajectory_by_number.get(int(raw.get("number") or 0), {})
        t2m_current_c = _safe_float(trajectory_row.get("t2m_current_c"))
        td2m_current_c = _safe_float(trajectory_row.get("td2m_current_c"))
        rh2m_current_pct = _safe_float(raw.get("rh2m_pct"))
        wind_10m_current_kmh = _safe_float(trajectory_row.get("wind_10m_current_kmh"))
        wind_dir_10m_current_deg = _safe_float(raw.get("wind_direction_10m_deg"))
        msl_current_hpa = _safe_float(trajectory_row.get("msl_current_hpa"))
        surface_temp_gap_c = None if latest_temp_c is None or t2m_current_c is None else round(float(latest_temp_c) - float(t2m_current_c), 2)
        surface_temp_bias_c = None if latest_temp_c is None or t2m_current_c is None else round(float(t2m_current_c) - float(latest_temp_c), 2)
        surface_dewpoint_gap_c = None if latest_dewpoint_c is None or td2m_current_c is None else round(float(latest_dewpoint_c) - float(td2m_current_c), 2)
        surface_rh_gap_pct = None if latest_rh_pct is None or rh2m_current_pct is None else round(float(latest_rh_pct) - float(rh2m_current_pct), 1)
        wind_gap_kmh = None if latest_wspd_kmh is None or wind_10m_current_kmh is None else abs(float(latest_wspd_kmh) - float(wind_10m_current_kmh))
        wind_dir_gap_deg = _angular_gap_deg(latest_wdir_deg, wind_dir_10m_current_deg)
        pressure_gap_hpa = None if latest_pressure_hpa is None or msl_current_hpa is None else abs(float(latest_pressure_hpa) - float(msl_current_hpa))
        surface_alignment_score = None
        alignment_terms: list[float] = []
        alignment_weights: list[float] = []
        if surface_temp_gap_c is not None:
            alignment_terms.append(_clamp(1.0 - abs(float(surface_temp_gap_c)) / 3.2, 0.0, 1.0))
            alignment_weights.append(0.62)
        if surface_dewpoint_gap_c is not None:
            alignment_terms.append(_clamp(1.0 - abs(float(surface_dewpoint_gap_c)) / 4.0, 0.0, 1.0))
            alignment_weights.append(0.08)
        if surface_rh_gap_pct is not None:
            alignment_terms.append(_clamp(1.0 - abs(float(surface_rh_gap_pct)) / 28.0, 0.0, 1.0))
            alignment_weights.append(0.06)
        if wind_gap_kmh is not None:
            alignment_terms.append(_clamp(1.0 - float(wind_gap_kmh) / 18.0, 0.0, 1.0))
            alignment_weights.append(0.20)
        if wind_dir_gap_deg is not None:
            alignment_terms.append(_clamp(1.0 - float(wind_dir_gap_deg) / 90.0, 0.0, 1.0))
            alignment_weights.append(0.08)
        if pressure_gap_hpa is not None:
            alignment_terms.append(_clamp(1.0 - float(pressure_gap_hpa) / 3.5, 0.0, 1.0))
            alignment_weights.append(0.18)
        if alignment_terms and alignment_weights and sum(alignment_weights) > 0.0:
            surface_alignment_score = round(
                sum(term * weight for term, weight in zip(alignment_terms, alignment_weights))
                / sum(alignment_weights),
                3,
            )
        history_row = history_map.get(int(raw.get("number") or 0), {})
        history_alignment_score = _safe_float(history_row.get("history_alignment_score"))
        history_temp_mae_c = _safe_float(history_row.get("history_temp_mae_c"))
        history_trend_bias_c = _safe_float(history_row.get("history_trend_bias_c"))
        history_match_count = _safe_float(history_row.get("history_match_count"))
        match_tier = _member_match_tier(
            raw,
            observed_path=observed_path,
            observed_path_detail=observed_path_detail,
        )
        future_family, pace_state, room_factor, overshoot_factor, stall_risk = _member_future_traits(
            row_path=row_path,
            row_detail=row_detail,
            row_side=row_side,
            delta_t850_c=_safe_float(raw.get("delta_t850_c")),
            wind_speed_850_kmh=_safe_float(raw.get("wind_speed_850_kmh")),
            trajectory_row=trajectory_row,
            branch_outlook_state=branch_outlook_state,
            cloud_effective_cover=cloud_effective_cover,
            radiation_eff=radiation_eff,
            precip_state=precip_state,
        )
        compatibility_weight = _member_compatibility_weight(
            match_tier=match_tier,
            row_path=row_path,
            row_detail=row_detail,
            row_side=row_side,
            future_family=future_family,
            branch_outlook_state=branch_outlook_state,
            matched_subset_active=bool(matched_subset.get("matched_subset_active")),
            temp_trend_c=temp_trend_c,
            temp_bias_c=temp_bias_c,
            cloud_effective_cover=cloud_effective_cover,
            radiation_eff=radiation_eff,
            precip_state=precip_state,
            surface_temp_gap_c=surface_temp_gap_c,
            surface_temp_bias_c=surface_temp_bias_c,
            surface_alignment_score=surface_alignment_score,
            history_alignment_score=history_alignment_score,
            history_temp_mae_c=history_temp_mae_c,
            history_trend_bias_c=history_trend_bias_c,
            history_match_count=history_match_count,
        )
        weighted_path_scores[row_path] = weighted_path_scores.get(row_path, 0.0) + compatibility_weight
        weighted_family_scores[future_family] = weighted_family_scores.get(future_family, 0.0) + compatibility_weight
        weights.append(compatibility_weight)
        member_rows.append(
            {
                "number": int(raw.get("number") or 0),
                "path_label": row_path,
                "path_detail": row_detail,
                "path_side": row_side,
                "match_tier": match_tier,
                "future_family": future_family,
                "pace_state": pace_state,
                "compatibility_weight": compatibility_weight,
                "room_factor": room_factor,
                "overshoot_factor": overshoot_factor,
                "stall_risk": stall_risk,
                "delta_t850_c": _safe_float(raw.get("delta_t850_c")),
                "wind_speed_850_kmh": _safe_float(raw.get("wind_speed_850_kmh")),
                "trajectory_shape": str(trajectory_row.get("trajectory_shape") or ""),
                "t2m_current_c": t2m_current_c,
                "td2m_current_c": td2m_current_c,
                "rh2m_current_pct": rh2m_current_pct,
                "wind_10m_current_kmh": wind_10m_current_kmh,
                "wind_direction_10m_current_deg": wind_dir_10m_current_deg,
                "msl_current_hpa": msl_current_hpa,
                "surface_temp_gap_c": surface_temp_gap_c,
                "surface_temp_bias_c": surface_temp_bias_c,
                "surface_dewpoint_gap_c": surface_dewpoint_gap_c,
                "surface_rh_gap_pct": surface_rh_gap_pct,
                "surface_alignment_score": surface_alignment_score,
                "history_alignment_score": history_alignment_score,
                "history_temp_mae_c": history_temp_mae_c,
                "history_trend_bias_c": history_trend_bias_c,
                "history_match_count": history_match_count,
                "wind_gap_kmh": round(float(wind_gap_kmh), 2) if wind_gap_kmh is not None else None,
                "wind_dir_gap_deg": round(float(wind_dir_gap_deg), 1) if wind_dir_gap_deg is not None else None,
                "pressure_gap_hpa": round(float(pressure_gap_hpa), 2) if pressure_gap_hpa is not None else None,
                "prior3h_t850_delta_c": _safe_float(trajectory_row.get("prior3h_t850_delta_c")),
                "next3h_t850_delta_c": _safe_float(trajectory_row.get("next3h_t850_delta_c")),
                "prior3h_t2m_delta_c": _safe_float(trajectory_row.get("prior3h_t2m_delta_c")),
                "next3h_t2m_delta_c": _safe_float(trajectory_row.get("next3h_t2m_delta_c")),
                "next3h_td2m_delta_c": _safe_float(trajectory_row.get("next3h_td2m_delta_c")),
                "trajectory_accel_c": _safe_float(trajectory_row.get("trajectory_accel_c")),
                "future_room_c": _safe_float(trajectory_row.get("future_room_c")),
                "future_cooling_c": _safe_float(trajectory_row.get("future_cooling_c")),
                "next3h_wind10_delta_kmh": _safe_float(trajectory_row.get("next3h_wind10_delta_kmh")),
                "next3h_msl_delta_hpa": _safe_float(trajectory_row.get("next3h_msl_delta_hpa")),
            }
        )

    total_weight = sum(weights)
    if total_weight <= 0.0:
        return {}

    weighted_path_share = {
        key: round(value / total_weight, 3)
        for key, value in weighted_path_scores.items()
        if value > 0.0
    }
    dominant_weighted_path = max(weighted_path_scores.items(), key=lambda item: item[1])[0]
    dominant_future_family = max(weighted_family_scores.items(), key=lambda item: item[1])[0]
    branch_side = str(branch_outlook_state.get("branch_side") or "")
    branch_weight_share = round(
        sum(
            float(row.get("compatibility_weight") or 0.0)
            for row in member_rows
            if str(row.get("path_side") or "") == branch_side
        ) / total_weight,
        3,
    ) if branch_side else None

    return {
        "active_source": active_source,
        "dominant_weighted_path": dominant_weighted_path,
        "dominant_weighted_future_family": dominant_future_family,
        "effective_member_count": round(_effective_member_count(weights), 2),
        "branch_side_weight_share": branch_weight_share,
        "weighted_path_share": weighted_path_share,
        "weighted_future_family_share": {
            key: round(value / total_weight, 3)
            for key, value in weighted_family_scores.items()
            if value > 0.0
        },
        "members": member_rows,
    }


def _build_matched_branch_outlook(
    *,
    phase: str,
    hours_to_peak: float | None,
    hours_to_window_end: float | None,
    ensemble_member_count: float | None,
    members: list[dict[str, Any]],
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
    rebound_mode: str,
    should_discuss_second_peak: bool,
    future_candidate_role: str,
    latest_temp_c: float | None,
    observed_max_temp_c: float | None,
    temp_trend_c: float | None,
    history_alignment: dict[str, Any] | None = None,
) -> dict[str, Any]:
    history_state = dict(history_alignment or {})
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
    elif bool(history_state.get("history_supported")) and str(history_state.get("dominant_path") or ""):
        history_path = str(history_state.get("dominant_path") or "")
        history_detail = str(history_state.get("dominant_path_detail") or history_path)
        history_prob = _safe_float(history_state.get("dominant_prob"))
        history_margin = _safe_float(history_state.get("dominant_margin_prob"))
        history_override = bool(
            not observed_path
            or alignment_confidence not in {"high", "partial"}
            or history_path == observed_path
            or history_detail == observed_path_detail
            or bool(history_state.get("history_path_locked"))
            or (
                history_prob is not None
                and history_prob >= 0.70
                and history_margin is not None
                and history_margin >= 0.16
            )
        )
        if history_override:
            active_source = "history_surface_match"
            active_path = history_path
            active_path_detail = history_detail
            active_dominant_prob = history_prob
            if history_margin is not None:
                active_signal_dispersion_c = round(max(0.28, 2.1 * (1.0 - float(history_margin))), 2)
    elif alignment_confidence in {"high", "partial"} and observed_path:
        active_source = "observed_path"
        active_path = str(observed_path or "")
        active_path_detail = str(observed_path_detail or active_path or "")

    active_side = _path_side(active_path, path_detail=active_path_detail)
    branch_member_count, branch_member_share = _branch_member_count(
        members=members,
        branch_path=active_path,
        branch_path_detail=active_path_detail,
    )
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
    current_fresh_high = bool(
        latest_temp_c is not None
        and observed_max_temp_c is not None
        and float(latest_temp_c) >= float(observed_max_temp_c) - 0.05
    )
    fresh_high_still_rising = bool(
        current_fresh_high
        and (temp_trend_c is None or float(temp_trend_c) >= 0.08)
    )
    second_peak_retest_ready = bool(
        second_peak_watch
        and bool(should_discuss_second_peak)
        and str(rebound_mode or "").strip().lower() in {"second_peak", "retest"}
        and str(future_candidate_role or "").strip().lower() == "secondary_peak_candidate"
        and not fresh_high_still_rising
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

    if second_peak_retest_ready:
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
    circulation_signature_text, circulation_signature_score = _build_branch_circulation_signature(
        members=members,
        branch_path=active_path,
        branch_path_detail=active_path_detail,
        branch_side=active_side,
        transport_state=transport_state,
        warm_landing_pending=warm_landing_pending,
        warm_landing_ready=warm_landing_ready,
        cold_landing_ready=cold_landing_ready,
        dry_mix_support=dry_mix_support,
        cloud_locked=cloud_locked,
        precip_active=precip_active,
        cap_hold=cap_hold,
    )

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
        "second_peak_retest_ready": second_peak_retest_ready,
        "matched_subset_active": matched_subset_active,
        "ensemble_member_count": _safe_float(ensemble_member_count),
        "branch_member_count": _safe_float(branch_member_count),
        "branch_member_share": _safe_float(branch_member_share),
        "matched_member_count": _safe_float(matched_subset.get("matched_member_count")),
        "matched_member_share": _safe_float(matched_subset.get("matched_member_share")),
        "rejected_member_share": _safe_float(matched_subset.get("rejected_member_share")),
        "circulation_signature_text": circulation_signature_text,
        "circulation_signature_score": circulation_signature_score,
        "history_supported": bool(history_state.get("history_supported")),
        "history_path_locked": bool(history_state.get("history_path_locked")),
        "history_matched_time_count": _safe_float(history_state.get("matched_time_count")),
        "history_dominant_path": str(history_state.get("dominant_path") or ""),
        "history_dominant_path_detail": str(history_state.get("dominant_path_detail") or ""),
        "history_dominant_prob": _safe_float(history_state.get("dominant_prob")),
        "history_dominant_margin_prob": _safe_float(history_state.get("dominant_margin_prob")),
        "history_alignment_score": _safe_float(history_state.get("dominant_alignment_score")),
        "history_temp_mae_c": _safe_float(history_state.get("dominant_temp_mae_c")),
        "history_trend_bias_c": _safe_float(history_state.get("dominant_trend_bias_c")),
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
    member_history_alignment = dict(ensemble_factor.get("member_history_alignment") or {})
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
    history_alignment_summary = _history_alignment_summary(
        members=list(ensemble_factor.get("members") or []),
        member_history_alignment=member_history_alignment,
    )
    matched_branch_outlook = _build_matched_branch_outlook(
        phase=phase_name,
        hours_to_peak=hours_to_peak,
        hours_to_window_end=hours_to_window_end,
        ensemble_member_count=_safe_float(ensemble_factor.get("member_count")),
        members=list(ensemble_factor.get("members") or []),
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
        rebound_mode=str(phase.get("rebound_mode") or ""),
        should_discuss_second_peak=bool(phase.get("should_discuss_second_peak")),
        future_candidate_role=str((phase.get("shape") or {}).get("future_candidate_role") or ""),
        latest_temp_c=latest_temp_c,
        observed_max_temp_c=observed_max_temp_c,
        temp_trend_c=_safe_float(obs.get("temp_trend_effective_c"))
        if _safe_float(obs.get("temp_trend_effective_c")) is not None
        else _safe_float(obs.get("temp_trend_c")),
        history_alignment=history_alignment_summary,
    )
    member_evolution_state = _build_member_evolution_state(
        members=list(ensemble_factor.get("members") or []),
        member_trajectory=dict(ensemble_factor.get("member_trajectory") or {}),
        phase=phase_name,
        observed_path=str(ensemble_alignment.get("observed_path") or ""),
        observed_path_detail=str(ensemble_alignment.get("observed_path_detail") or ""),
        branch_outlook_state=matched_branch_outlook,
        matched_subset=matched_subset,
        latest_temp_c=latest_temp_c,
        latest_dewpoint_c=dewpoint_c,
        latest_rh_pct=_safe_float(obs.get("latest_rh")),
        latest_wspd_kmh=(
            _safe_float(obs.get("latest_wspd_kt")) * 1.852
            if _safe_float(obs.get("latest_wspd_kt")) is not None
            else None
        ),
        latest_wdir_deg=_safe_float(obs.get("latest_wdir_deg")),
        latest_pressure_hpa=_safe_float(obs.get("latest_pressure_hpa")),
        temp_trend_c=_safe_float(obs.get("temp_trend_effective_c"))
        if _safe_float(obs.get("temp_trend_effective_c")) is not None
        else _safe_float(obs.get("temp_trend_c")),
        temp_bias_c=_safe_float(obs.get("temp_bias_c")),
        cloud_effective_cover=cloud_effective_cover,
        radiation_eff=radiation_eff,
        precip_state=precip_state,
        member_history_alignment=member_history_alignment,
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
            "history_supported": bool(history_alignment_summary.get("history_supported")),
            "history_path_locked": bool(history_alignment_summary.get("history_path_locked")),
            "history_matched_time_count": _safe_float(history_alignment_summary.get("matched_time_count")),
            "history_used_member_count": _safe_float(history_alignment_summary.get("used_member_count")),
            "history_dominant_path": str(history_alignment_summary.get("dominant_path") or ""),
            "history_dominant_path_detail": str(history_alignment_summary.get("dominant_path_detail") or ""),
            "history_dominant_prob": _safe_float(history_alignment_summary.get("dominant_prob")),
            "history_dominant_margin_prob": _safe_float(history_alignment_summary.get("dominant_margin_prob")),
            "history_alignment_score": _safe_float(history_alignment_summary.get("dominant_alignment_score")),
            "history_temp_mae_c": _safe_float(history_alignment_summary.get("dominant_temp_mae_c")),
            "history_trend_bias_c": _safe_float(history_alignment_summary.get("dominant_trend_bias_c")),
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
            "second_peak_retest_ready": bool(matched_branch_outlook.get("second_peak_retest_ready")),
            "matched_subset_active": bool(matched_branch_outlook.get("matched_subset_active")),
            "ensemble_member_count": _safe_float(matched_branch_outlook.get("ensemble_member_count")),
            "branch_member_count": _safe_float(matched_branch_outlook.get("branch_member_count")),
            "branch_member_share": _safe_float(matched_branch_outlook.get("branch_member_share")),
            "matched_member_count": _safe_float(matched_branch_outlook.get("matched_member_count")),
            "matched_member_share": _safe_float(matched_branch_outlook.get("matched_member_share")),
            "rejected_member_share": _safe_float(matched_branch_outlook.get("rejected_member_share")),
            "circulation_signature_text": str(matched_branch_outlook.get("circulation_signature_text") or ""),
            "circulation_signature_score": _safe_float(matched_branch_outlook.get("circulation_signature_score")),
            "history_supported": bool(matched_branch_outlook.get("history_supported")),
            "history_path_locked": bool(matched_branch_outlook.get("history_path_locked")),
            "history_matched_time_count": _safe_float(matched_branch_outlook.get("history_matched_time_count")),
            "history_dominant_path": str(matched_branch_outlook.get("history_dominant_path") or ""),
            "history_dominant_path_detail": str(matched_branch_outlook.get("history_dominant_path_detail") or ""),
            "history_dominant_prob": _safe_float(matched_branch_outlook.get("history_dominant_prob")),
            "history_dominant_margin_prob": _safe_float(matched_branch_outlook.get("history_dominant_margin_prob")),
            "history_alignment_score": _safe_float(matched_branch_outlook.get("history_alignment_score")),
            "history_temp_mae_c": _safe_float(matched_branch_outlook.get("history_temp_mae_c")),
            "history_trend_bias_c": _safe_float(matched_branch_outlook.get("history_trend_bias_c")),
        },
        "member_evolution_state": {
            "active_source": str(member_evolution_state.get("active_source") or ""),
            "dominant_weighted_path": str(member_evolution_state.get("dominant_weighted_path") or ""),
            "dominant_weighted_future_family": str(member_evolution_state.get("dominant_weighted_future_family") or ""),
            "effective_member_count": _safe_float(member_evolution_state.get("effective_member_count")),
            "branch_side_weight_share": _safe_float(member_evolution_state.get("branch_side_weight_share")),
            "history_matched_time_count": _safe_float(history_alignment_summary.get("matched_time_count")),
            "history_supported": bool(history_alignment_summary.get("history_supported")),
            "weighted_path_share": dict(member_evolution_state.get("weighted_path_share") or {}),
            "weighted_future_family_share": dict(member_evolution_state.get("weighted_future_family_share") or {}),
            "members": list(member_evolution_state.get("members") or []),
        },
    }
