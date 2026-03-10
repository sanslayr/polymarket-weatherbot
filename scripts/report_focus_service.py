#!/usr/bin/env python3
"""Structured report-support bundle so render layer stays translation-only."""

from __future__ import annotations

from datetime import datetime
from typing import Any

from contracts import REPORT_FOCUS_SCHEMA_VERSION
from market_label_policy import build_market_label_policy
from param_store import load_tmax_learning_params


PHASE_LABELS = {
    "far": "远离窗口",
    "near_window": "接近窗口",
    "in_window": "窗口内",
    "post": "窗口后",
    "early_peak_watch": "早峰后观察",
    "unknown": "窗口状态未知",
}
DEFAULT_TRACK_LINE = "• 临窗前继续跟踪温度斜率与风向节奏，必要时再改判。"
FAR_TRACK_LINE = "• 当前先看环流、云量和低层风场配置是否继续维持；更细的实况校正留到临近目标窗再做。"


def _safe_float(value: Any) -> float | None:
    try:
        if value in (None, ""):
            return None
        return float(value)
    except Exception:
        return None


def _trend_horizon_phrase(metar_diag: dict[str, Any]) -> str:
    try:
        cad = float(metar_diag.get("metar_routine_cadence_min")) if metar_diag.get("metar_routine_cadence_min") is not None else None
    except Exception:
        cad = None
    try:
        recent = float(metar_diag.get("metar_recent_interval_min")) if metar_diag.get("metar_recent_interval_min") is not None else None
    except Exception:
        recent = None
    speci = bool(metar_diag.get("metar_speci_active"))
    speci_likely = bool(metar_diag.get("metar_speci_likely"))

    if recent is not None and 8.0 <= recent <= 90.0:
        base = recent
    elif cad is not None and 15.0 <= cad <= 90.0:
        base = cad
    else:
        base = 45.0

    if speci:
        lo = max(10, int(round(max(10.0, base * 0.45) / 5.0) * 5))
        hi = max(lo + 10, int(round(min(45.0, base * 1.10) / 5.0) * 5))
    elif speci_likely:
        lo = max(15, int(round(max(15.0, base * 0.55) / 5.0) * 5))
        hi = max(lo + 10, int(round(min(55.0, base * 1.20) / 5.0) * 5))
    elif base <= 35.0:
        lo, hi = 20, 40
    elif base <= 50.0:
        lo, hi = 25, 50
    else:
        lo, hi = 35, 70
    return f"未来{lo}-{hi}分钟"


def _cadence_line(metar_diag: dict[str, Any], phase: str) -> str:
    try:
        cadence = float(metar_diag.get("metar_routine_cadence_min")) if metar_diag.get("metar_routine_cadence_min") is not None else None
    except Exception:
        cadence = None
    if cadence is not None and 15.0 <= cadence <= 90.0 and phase == "far":
        cadence_round = int(round(cadence / 5.0) * 5)
        return f"• 该站常规约每{cadence_round}分钟一报；但离目标峰值窗仍远，当前更适合看后续预报是否继续维持这套形势。"
    return ""


def _quality_uncertainty_line(quality_snapshot: dict[str, Any], phase: str) -> str:
    q = dict(quality_snapshot or {})
    scores = dict(q.get("scores") or {})
    adjustments = dict(q.get("posterior_adjustments") or {})
    coverage = dict(q.get("coverage") or {})
    confidence_label = str(scores.get("confidence_label") or "")
    spread_multiplier = _safe_float(adjustments.get("spread_multiplier")) or 1.0
    sounding_density = str(coverage.get("sounding_density") or "")
    provider_fallback = bool(((q.get("source") or {}).get("synoptic_provider_fallback")))

    if confidence_label == "low":
        if provider_fallback:
            return "• 当前 3D 场已回退且整体不确定性偏高，区间与事件概率宜按保守口径理解。"
        if sounding_density in {"sparse", ""}:
            if phase == "far":
                return "• 探空/层结覆盖偏稀疏，当前更应把它理解为方向性情景，等临近目标窗再结合实况细化。"
            return "• 探空/层结覆盖偏稀疏，后验不确定性偏高，临窗需更依赖下一报实况。"
    if spread_multiplier >= 1.25:
        if phase == "far":
            return "• 当前后验不确定性仍偏大，更适合把区间理解成情景范围，待后续预报收敛后再细化。"
        return "• 当前后验不确定性仍偏大，重点看下一报是否继续支持当前方向。"
    return ""


def _push_focus_candidate(bucket: list[tuple[float, str]], score: float, text: str) -> None:
    txt = str(text or "").strip()
    if not txt:
        return
    bucket.append((float(score), txt if txt.startswith("•") else f"• {txt}"))


def build_report_focus_bundle(
    *,
    primary_window: dict[str, Any],
    metar_diag: dict[str, Any],
    analysis_snapshot: dict[str, Any],
) -> dict[str, Any]:
    snapshot = analysis_snapshot if isinstance(analysis_snapshot, dict) else {}
    temp_phase = dict(snapshot.get("temp_phase_decision") or {})
    weather_posterior = dict(snapshot.get("weather_posterior") or {})
    quality_snapshot = dict(snapshot.get("quality_snapshot") or {})
    peak_data = dict(snapshot.get("peak_data") or {})
    peak_summary = dict(peak_data.get("summary") or {})
    consistency = dict(peak_summary.get("consistency") or {})
    confidence = dict(peak_summary.get("confidence") or {})
    boundary_layer_regime = dict(snapshot.get("boundary_layer_regime") or {})
    thermo = dict(boundary_layer_regime.get("thermo") or {})
    state = dict(snapshot.get("condition_state") or {})

    display_phase = str(temp_phase.get("display_phase") or peak_summary.get("phase_now") or "unknown")
    daily_peak_state = str(temp_phase.get("daily_peak_state") or "open")
    short_term_state = str(temp_phase.get("short_term_state") or "holding")
    timing = dict(temp_phase.get("timing") or {})
    shape = dict(temp_phase.get("shape") or {})
    before_typical_peak = bool(timing.get("before_typical_peak"))
    overnight_carryover_high = bool(timing.get("overnight_carryover_high"))
    future_candidate_role = str(shape.get("future_candidate_role") or "")
    regime_key = str(boundary_layer_regime.get("regime_key") or "")
    vertical_regime = str(thermo.get("vertical_regime") or "")
    new_high_prob = _safe_float((weather_posterior.get("event_probs") or {}).get("new_high_next_60m")) or 0.0
    lock_prob = _safe_float((weather_posterior.get("event_probs") or {}).get("lock_by_window_end")) or 0.0
    trend_horizon = _trend_horizon_phrase(metar_diag)

    vars_block = [f"⚠️ **关注变量**（{PHASE_LABELS.get(display_phase, PHASE_LABELS['unknown'])}）"]
    focus_candidates: list[tuple[float, str]] = []

    cadence_line = _cadence_line(metar_diag, display_phase)

    tracking_line = str(boundary_layer_regime.get("tracking_line") or "").strip()
    if tracking_line:
        score = 0.95 if regime_key in {"boundary_layer_clearing", "static_stable", "mixing_depth", "advection"} else 0.65
        _push_focus_candidate(focus_candidates, score, tracking_line)

    if display_phase == "far":
        _push_focus_candidate(
            focus_candidates,
            0.82,
            "当前更该看环流、云量和低层风场配置会不会延续，而不是把眼前单报实况当成目标日结论。",
        )
        if before_typical_peak and future_candidate_role == "primary_remaining_peak":
            _push_focus_candidate(
                focus_candidates,
                0.74,
                "若后续预报仍维持日照/混合改善，目标日上沿才有继续抬升空间；若云量或风向配置转弱，则需回收区间。",
            )
    elif before_typical_peak and overnight_carryover_high and future_candidate_role == "primary_remaining_peak":
        early_peak_line = "白天主峰仍未到来，先看后段升温何时真正展开，再判断还能不能继续上冲。"
        if regime_key == "boundary_layer_clearing" or vertical_regime == "low_cloud_clearing":
            early_peak_line = "白天主峰仍未到来，先看低云/雾层何时真正松动，再判断后段升温能否顺利展开。"
        _push_focus_candidate(
            focus_candidates,
            0.9,
            early_peak_line,
        )
    elif new_high_prob >= 0.68 and display_phase not in {"far", "early_peak_watch"}:
        _push_focus_candidate(
            focus_candidates,
            0.72,
            f"{trend_horizon}仍有再创新高空间，优先看温度斜率是否继续维持正值。",
        )
    elif lock_prob >= 0.76:
        _push_focus_candidate(
            focus_candidates,
            0.72,
            f"{trend_horizon}更偏向高点锁定，重点看下一报是否继续横盘或回落。",
        )
    elif short_term_state == "reaccelerating":
        _push_focus_candidate(
            focus_candidates,
            0.62,
            f"{trend_horizon}若升温继续放大，最高温上沿仍可小幅上修。",
        )
    else:
        _push_focus_candidate(
            focus_candidates,
            0.45,
            FAR_TRACK_LINE if display_phase == "far" else "优先看下一报温度斜率、风向节奏和云量是否继续支持当前路径。",
        )

    quality_line = _quality_uncertainty_line(quality_snapshot, display_phase)
    if quality_line:
        _push_focus_candidate(focus_candidates, 0.35, quality_line)

    focus_lines: list[str] = []
    seen: set[str] = set()
    sorted_candidates = sorted(focus_candidates, key=lambda item: item[0], reverse=True)
    for _, line in sorted_candidates:
        if line in seen:
            continue
        seen.add(line)
        focus_lines.append(line)
        if len(focus_lines) >= 2:
            break

    if len(focus_lines) >= 2 and len(sorted_candidates) >= 2:
        top_score = sorted_candidates[0][0]
        second_score = sorted_candidates[1][0]
        if top_score >= 0.85 and second_score <= 0.55:
            focus_lines = focus_lines[:1]

    if not focus_lines and cadence_line:
        focus_lines.append(cadence_line if cadence_line.startswith("•") else f"• {cadence_line}")

    vars_block.extend(focus_lines[:2] if focus_lines else [FAR_TRACK_LINE if display_phase == "far" else DEFAULT_TRACK_LINE])
    if len(vars_block) == 1:
        vars_block.append(FAR_TRACK_LINE if display_phase == "far" else DEFAULT_TRACK_LINE)

    obs_analysis_lines: list[str] = []
    if bool(temp_phase.get("should_use_early_peak_wording")) and lock_prob < 0.70:
        obs_analysis_lines.append("• 当前更像已先出现早峰，短线动能转弱；全天是否锁定仍待后续实况确认。")
    elif daily_peak_state == "lean_locked":
        obs_analysis_lines.append("• 当前更偏向已接近全天高点，但仍需下一关键报确认是否真正锁定。")
    elif daily_peak_state == "locked":
        obs_analysis_lines.append("• 当前路径更接近高点已锁定，后续再创新高难度明显上升。")

    if bool(confidence.get("compact_settled_mode")) and peak_summary.get("observed"):
        observed = dict(peak_summary.get("observed") or {})
        obs_max = _safe_float(observed.get("max_temp_c"))
        if obs_max is not None:
            obs_analysis_lines.append("• 峰值窗已基本过去，实况高点附近的收敛信号更值得优先参考。")

    label_policy_cfg = {}
    try:
        label_policy_cfg = dict((load_tmax_learning_params().get("market_labels") or {}))
    except Exception:
        label_policy_cfg = {}
    label_policy = build_market_label_policy(
        quality=state.get("quality") or {},
        obj=state.get("obj") or {},
        low_conf_far=bool(confidence.get("low_conf_far")),
        phase_now=str(peak_summary.get("phase_now") or "unknown"),
        metar_diag=metar_diag,
        t_cons=float(consistency.get("temp_trend_consistency_c") or 0.0),
        b_cons=float(consistency.get("temp_bias_consistency_c") or 0.0),
        compact_settled_mode=bool(confidence.get("compact_settled_mode")),
        policy_params=label_policy_cfg,
    )

    return {
        "schema_version": REPORT_FOCUS_SCHEMA_VERSION,
        "vars_block": vars_block,
        "metar_analysis_lines": obs_analysis_lines,
        "market_label_policy": label_policy,
    }
