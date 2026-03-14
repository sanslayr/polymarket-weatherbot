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
GENERIC_FOCUS_KEYS = (
    "当前更该看环流、云量和低层风场配置会不会延续",
    "当前先看环流、云量和低层风场配置是否继续维持",
    "更细的实况校正留到临近目标窗再做",
    "优先看下一报温度斜率、风向节奏和云量是否继续支持当前路径",
    "优先看温度斜率和低层风向是否继续配合",
    "临窗前继续跟踪温度斜率与风向节奏",
    "若后续预报仍维持日照/混合改善",
    "若云量或风向配置转弱，则需回收区间",
    "仍有再创新高空间，优先看温度斜率是否继续维持正值",
    "更偏向高点锁定，重点看下一报是否继续横盘或回落",
    "若升温继续放大，最高温上沿仍可小幅上修",
    "当前区间不确定性仍偏大",
    "区间与事件概率宜按保守口径理解",
    "临窗需更依赖下一报实况",
    "重点看下一报是否继续支持当前方向",
)
GENERIC_FOCUS_FRAGMENTS = (
    ("优先看", "温度斜率"),
    ("优先看", "风向"),
    ("优先看", "云量"),
    ("继续支持当前路径",),
    ("继续维持", "当前方向"),
    ("最高温上沿", "小幅上修"),
    ("回收区间",),
)
DECISIVE_FOCUS_TOKENS = (
    "已观测高点",
    "前高",
    "锁定",
    "再创新高",
    "二峰",
    "晚峰",
    "系集主支",
    "低云",
    "雾层",
    "云底",
    "温露差",
    "混合层",
    "偏冷输送",
    "偏暖输送",
    "冷平流",
    "暖平流",
    "偏冷象限",
    "偏暖象限",
    "近地耦合",
    "实况是否重新转冷偏离同小时模式",
    "实况是否重新转暖偏离同小时模式",
    "贴近前高",
    "暖侧",
    "冷侧",
)


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


def _format_hour_label(hour_value: float | None) -> str:
    hour = _safe_float(hour_value)
    if hour is None:
        return ""
    hour = max(0.0, min(23.99, hour))
    hh = int(hour)
    mm = int(round((hour - hh) * 60.0))
    if mm >= 60:
        hh = min(23, hh + 1)
        mm = 0
    return f"{hh:02d}:{mm:02d}"


def _path_side(path_name: str, path_detail: str) -> str:
    key = str(path_detail or "").strip() if str(path_name or "").strip() == "transition" else str(path_name or "").strip()
    return {
        "warm_support": "warm",
        "weak_warm_transition": "warm",
        "cold_suppression": "cold",
        "weak_cold_transition": "cold",
        "neutral_stable": "neutral",
        "transition": "neutral",
    }.get(key, "neutral")


def _path_label(path_name: str, path_detail: str) -> str:
    key = str(path_detail or "").strip() if str(path_name or "").strip() == "transition" else str(path_name or "").strip()
    return {
        "warm_support": "暖侧路径",
        "weak_warm_transition": "暖侧试探",
        "cold_suppression": "冷侧压制路径",
        "weak_cold_transition": "冷侧回摆",
        "neutral_stable": "中性过渡",
        "transition": "中性过渡",
    }.get(key, "当前路径")


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
            return "• 探空/层结覆盖偏稀疏，当前区间不确定性偏高，临窗需更依赖下一报实况。"
    if spread_multiplier >= 1.25:
        if phase == "far":
            return "• 当前区间不确定性仍偏大，更适合把区间理解成情景范围，待后续预报收敛后再细化。"
        return "• 当前区间不确定性仍偏大，重点看下一报是否继续支持当前方向。"
    return ""


def _push_focus_candidate(bucket: list[tuple[float, str]], score: float, text: str) -> None:
    txt = str(text or "").strip()
    if not txt:
        return
    bucket.append((float(score), txt if txt.startswith("•") else f"• {txt}"))


def _is_generic_focus_text(text: str) -> bool:
    cleaned = str(text or "").strip().lstrip("•").strip()
    if not cleaned:
        return True
    if any(key in cleaned for key in GENERIC_FOCUS_KEYS):
        return True
    if any(token in cleaned for token in DECISIVE_FOCUS_TOKENS):
        return False
    return any(all(fragment in cleaned for fragment in fragments) for fragments in GENERIC_FOCUS_FRAGMENTS)


def _posterior_focus_line(
    *,
    display_phase: str,
    weather_posterior: dict[str, Any],
) -> tuple[float, str]:
    posterior = dict(weather_posterior or {})
    event_probs = dict(posterior.get("event_probs") or {})
    core = dict(posterior.get("core") or {})
    progress = dict(core.get("progress") or {})
    anchor = dict(posterior.get("anchor") or {})

    lock_prob = _safe_float(event_probs.get("lock_by_window_end"))
    new_high_prob = _safe_float(event_probs.get("new_high_next_60m"))
    upper_tail_cap_c = _safe_float((posterior.get("calibration") or {}).get("upper_tail_cap_c"))
    observed_anchor_c = _safe_float(progress.get("observed_anchor_c"))
    modeled_headroom_c = _safe_float(progress.get("modeled_headroom_c"))
    regime_shift_c = _safe_float(anchor.get("regime_median_shift_c"))

    if display_phase in {"near_window", "in_window", "post", "early_peak_watch"}:
        if (
            lock_prob is not None
            and new_high_prob is not None
            and lock_prob >= 0.78
            and new_high_prob <= 0.30
        ):
            if (
                observed_anchor_c is not None
                and upper_tail_cap_c is not None
                and (upper_tail_cap_c - observed_anchor_c) <= 0.35
            ):
                return 0.95, "综合判断已把高点基本压回已观测高点附近，重点只看下一报会不会重新贴近前高。"
            return 0.90, "综合判断已明显转向高点锁定，重点只看下一报是否继续横盘或回落。"
        if (
            new_high_prob is not None
            and new_high_prob >= 0.68
            and (lock_prob is None or lock_prob <= 0.45)
        ):
            if modeled_headroom_c is not None and modeled_headroom_c <= 0.55:
                return 0.88, "综合判断仍保留再创新高路径，但剩余上冲空间已经不大，重点看下一报温度斜率能否续正。"
            return 0.90, "综合判断仍保留再创新高路径，重点看下一报温度斜率能否续正。"

    if display_phase == "far" and regime_shift_c is not None and abs(regime_shift_c) >= 0.45:
        if regime_shift_c > 0.0:
            return 0.76, "站点特征和天气型修正仍把中值轻推到暖侧一档，重点看云量和低层风场能否继续同向配合。"
        return 0.76, "站点特征和天气型修正仍把中值压在冷侧一档，重点看压制信号会不会继续维持。"

    return 0.0, ""


def _phase_structure_focus_line(temp_phase: dict[str, Any], *, display_phase: str) -> tuple[float, str]:
    timing = dict(temp_phase.get("timing") or {})
    station = dict(temp_phase.get("station") or {})
    shape = dict(temp_phase.get("shape") or {})

    daily_peak_state = str(temp_phase.get("daily_peak_state") or "open")
    second_peak_potential = str(temp_phase.get("second_peak_potential") or "none")
    rebound_mode = str(temp_phase.get("rebound_mode") or "")
    should_discuss_second_peak = bool(temp_phase.get("should_discuss_second_peak"))
    before_typical_peak = bool(timing.get("before_typical_peak"))
    future_candidate_role = str(shape.get("future_candidate_role") or "")
    future_gap_vs_obs = _safe_float(shape.get("future_gap_vs_obs"))
    future_gap_vs_current = _safe_float(shape.get("future_gap_vs_current"))
    late_peak_share = _safe_float(station.get("late_peak_share")) or 0.0
    very_late_peak_share = _safe_float(station.get("very_late_peak_share")) or 0.0
    warm_peak_hour_median = _safe_float(station.get("warm_peak_hour_median"))

    if (
        display_phase in {"far", "near_window", "in_window", "post", "early_peak_watch"}
        and daily_peak_state == "open"
        and should_discuss_second_peak
        and second_peak_potential in {"moderate", "high"}
    ):
        if future_candidate_role == "secondary_peak_candidate" and future_gap_vs_obs is not None and future_gap_vs_obs >= 0.25:
            return 0.94, "后段二峰还开着，次峰仍有机会挑战前高，重点看温度会不会重新贴近并翻过前高。"
        if future_gap_vs_current is not None and future_gap_vs_current >= 0.45:
            return 0.90, "后段仍留着一段补涨窗口，重点看升温会不会重新提速回到前高附近。"
        if rebound_mode == "second_peak":
            return 0.88, "当前更像早峰后的二峰观察段，还不能按已经见顶处理。"

    if (
        display_phase in {"far", "near_window", "in_window", "post", "early_peak_watch"}
        and daily_peak_state == "open"
        and before_typical_peak
        and late_peak_share >= 0.55
        and second_peak_potential in {"none", "weak"}
    ):
        peak_clock = _format_hour_label(warm_peak_hour_median)
        if very_late_peak_share >= 0.35 and peak_clock:
            return 0.84, f"这个站常见拖到 {peak_clock} 前后才更像见顶，眼前高点别急着当终点。"
        if peak_clock:
            return 0.80, f"这个站常见高点偏晚，通常要到 {peak_clock} 前后才更像见顶，眼前高点别急着当终点。"
        return 0.78, "这个站常见偏晚见顶，眼前高点别急着当终点。"

    return 0.0, ""


def _ensemble_deviation_focus_line(
    posterior_feature_vector: dict[str, Any],
    *,
    display_phase: str,
) -> tuple[float, str]:
    if display_phase not in {"near_window", "in_window", "post", "early_peak_watch"}:
        return 0.0, ""

    ensemble_state = dict((posterior_feature_vector or {}).get("ensemble_path_state") or {})
    dominant_path = str(ensemble_state.get("dominant_path") or "")
    observed_path = str(ensemble_state.get("observed_path") or "")
    dominant_detail = str(ensemble_state.get("dominant_path_detail") or ensemble_state.get("transition_detail") or "")
    observed_detail = str(ensemble_state.get("observed_path_detail") or "")
    match_state = str(ensemble_state.get("observed_alignment_match_state") or "")
    confidence = str(ensemble_state.get("observed_alignment_confidence") or "")
    dominant_prob = _safe_float(ensemble_state.get("dominant_prob")) or 0.0

    if not dominant_path or not observed_path:
        return 0.0, ""
    if confidence not in {"high", "partial"}:
        return 0.0, ""
    if match_state in {"exact", "path"}:
        return 0.0, ""
    if dominant_prob < 0.48:
        return 0.0, ""

    dominant_side = _path_side(dominant_path, dominant_detail)
    observed_side = _path_side(observed_path, observed_detail)
    dominant_label = _path_label(dominant_path, dominant_detail)
    observed_label = _path_label(observed_path, observed_detail)
    if not dominant_label or not observed_label or dominant_label == observed_label:
        return 0.0, ""

    if observed_side == "warm" and dominant_side in {"cold", "neutral"}:
        return 0.92, f"当前实况正在偏离系集主支，开始往{observed_label}走；若下一报延续，上沿要按更暖一侧看。"
    if observed_side == "cold" and dominant_side in {"warm", "neutral"}:
        return 0.92, f"当前实况正在偏离系集主支，开始往{observed_label}走；若下一报延续，下沿要更优先。"
    if observed_side == "neutral" and dominant_side == "warm":
        return 0.84, f"当前实况没跟上系集主暖支，更像{observed_label}；若继续走平，上沿要回收。"
    if observed_side == "neutral" and dominant_side == "cold":
        return 0.84, f"当前实况没跟上系集主冷支，更像{observed_label}；若继续走平，下沿别压得太低。"
    return 0.0, ""


def build_report_focus_bundle(
    *,
    primary_window: dict[str, Any],
    metar_diag: dict[str, Any],
    analysis_snapshot: dict[str, Any],
) -> dict[str, Any]:
    snapshot = analysis_snapshot if isinstance(analysis_snapshot, dict) else {}
    temp_phase = dict(snapshot.get("temp_phase_decision") or {})
    weather_posterior = dict(snapshot.get("weather_posterior") or {})
    posterior_feature_vector = dict(snapshot.get("posterior_feature_vector") or {})
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
    temp_trend_step = _safe_float(metar_diag.get("temp_trend_1step_c")) or 0.0

    vars_block = [f"⚠️ **关注变量**（{PHASE_LABELS.get(display_phase, PHASE_LABELS['unknown'])}）"]
    focus_candidates: list[tuple[float, str]] = []

    tracking_line = str(boundary_layer_regime.get("tracking_line") or "").strip()
    if tracking_line:
        is_specific_tracking = any(token in tracking_line for token in DECISIVE_FOCUS_TOKENS)
        score = 0.95 if regime_key in {"boundary_layer_clearing", "static_stable", "mixing_depth", "advection"} else (0.84 if is_specific_tracking else 0.58)
        _push_focus_candidate(focus_candidates, score, tracking_line)

    posterior_focus_score, posterior_focus_line = _posterior_focus_line(
        display_phase=display_phase,
        weather_posterior=weather_posterior,
    )
    if posterior_focus_line:
        _push_focus_candidate(focus_candidates, posterior_focus_score, posterior_focus_line)

    phase_structure_score, phase_structure_line = _phase_structure_focus_line(
        temp_phase,
        display_phase=display_phase,
    )
    if phase_structure_line:
        _push_focus_candidate(focus_candidates, phase_structure_score, phase_structure_line)

    ensemble_focus_score, ensemble_focus_line = _ensemble_deviation_focus_line(
        posterior_feature_vector,
        display_phase=display_phase,
    )
    if ensemble_focus_line:
        _push_focus_candidate(focus_candidates, ensemble_focus_score, ensemble_focus_line)

    if before_typical_peak and overnight_carryover_high and future_candidate_role == "primary_remaining_peak":
        early_peak_line = "白天主峰仍未到来，先看后段升温何时真正展开，再判断还能不能继续上冲。"
        if regime_key == "boundary_layer_clearing" or vertical_regime == "low_cloud_clearing":
            early_peak_line = "白天主峰仍未到来，先看低云/雾层何时真正松动，再判断后段升温能否顺利展开。"
        _push_focus_candidate(
            focus_candidates,
            0.96,
            early_peak_line,
        )
    elif display_phase == "far":
        _push_focus_candidate(
            focus_candidates,
            0.48,
            "当前更该看环流、云量和低层风场配置会不会延续，而不是把眼前单报实况当成目标日结论。",
        )
        if before_typical_peak and future_candidate_role == "primary_remaining_peak":
            _push_focus_candidate(
                focus_candidates,
                0.74,
                "若后续预报仍维持日照/混合改善，目标日上沿才有继续抬升空间；若云量或风向配置转弱，则需回收区间。",
            )
    elif new_high_prob >= 0.82 and temp_trend_step >= 0.5 and display_phase not in {"far", "early_peak_watch"}:
        _push_focus_candidate(
            focus_candidates,
            0.86,
            "仍有再创新高空间，优先看温度斜率是否继续维持正值。",
        )
    elif lock_prob >= 0.84:
        _push_focus_candidate(
            focus_candidates,
            0.82,
            "更偏向高点锁定，重点看下一报是否继续横盘或回落。",
        )
    elif short_term_state == "reaccelerating":
        _push_focus_candidate(
            focus_candidates,
            0.62,
            "若升温继续放大，最高温上沿仍可小幅上修。",
        )
    else:
        _push_focus_candidate(
            focus_candidates,
            0.45,
            FAR_TRACK_LINE if display_phase == "far" else "优先看下一报温度斜率、风向节奏和云量是否继续支持当前路径。",
        )

    quality_line = _quality_uncertainty_line(quality_snapshot, display_phase)
    if quality_line:
        _push_focus_candidate(focus_candidates, 0.28, quality_line)

    focus_lines: list[str] = []
    seen: set[str] = set()
    sorted_candidates = sorted(focus_candidates, key=lambda item: item[0], reverse=True)
    for _, line in sorted_candidates:
        if _is_generic_focus_text(line):
            continue
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

    if focus_lines:
        vars_block.extend(focus_lines[:2])

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
