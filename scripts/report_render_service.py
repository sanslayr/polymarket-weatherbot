#!/usr/bin/env python3
"""Section rendering service for /look report."""

from __future__ import annotations

import re
from datetime import datetime
from typing import Any

from analysis_snapshot_view import (
    snapshot_canonical_raw_state,
    snapshot_path_context,
    snapshot_posterior_feature_vector,
    snapshot_temp_phase_decision,
    snapshot_weather_posterior_anchor,
    snapshot_weather_posterior_calibration,
    snapshot_weather_posterior_core,
    snapshot_weather_posterior_event_probs,
)
from analysis_snapshot_service import build_analysis_snapshot
from polymarket_render_service import _build_polymarket_section
from report_focus_service import GENERIC_FOCUS_KEYS, build_report_focus_bundle
from report_synoptic_service import (
    _background_compact_clause,
    _background_mechanism_text,
    _build_background_synoptic_line,
    _build_far_synoptic_block,
    _coastal_flow_mechanism,
    _far_directional_take,
    _far_future_setup_line,
    _far_line_overlap,
    _far_mechanism_focus,
    _far_profile_lead_phrase,
    _is_generic_far_text,
    _natural_flow_chain_line,
    _normalize_synoptic_text,
    _pick_background_basis,
    _pick_far_basis_text,
    _rephrase_far_basis_text,
    _short_mechanism_text,
    _summarize_impact_text,
)

PHASE_LABELS = {
    "far": "远离窗口",
    "near_window": "接近窗口",
    "in_window": "窗口内",
    "post": "窗口后",
    "early_peak_watch": "早峰后观察",
    "unknown": "窗口状态未知",
}
DEFAULT_TRACK_LINE = "• 临窗前继续跟踪温度斜率与风向节奏，必要时再改判。"
FAR_REPORT_HOURS_THRESHOLD = 10.0
NEAR_REPORT_HOURS_THRESHOLD = 6.0

GENERIC_REASON_KEYS = (
    "从当前链路看",
    "眼下更像是",
    "区间先放在",
    "最新报已经到",
    "最新报还在",
    "最新报冲到",
    "两边各留一点机动",
    "下沿留给偏冷回摆",
    "上沿留给临窗再抬",
    "上沿仍宜按受压情景处理",
    "上沿仍保留小幅上修空间",
    "短时没看到立刻冲顶的速度",
    "这股斜率能不能续上还要看下一报",
    "再大幅上冲要看下一报能不能重新提速",
    "只剩尾段机动",
    "仍是当前主导约束",
)
GENERIC_REASON_FRAGMENTS = (
    ("上沿", "受压情景"),
    ("上沿", "小幅上修空间"),
    ("当前主导约束",),
    ("还要看下一报",),
    ("机动",),
)
DECISIVE_REASON_TOKENS = (
    "锁定约",
    "再创新高约",
    "高点锁定（约",
    "综合判断仍保留再创新高路径",
    "综合判断仍把再创新高当主情景",
    "主路径约",
    "已观测高点",
    "二峰",
    "晚峰",
    "系集主路径",
    "系集主支",
    "500hPa",
    "850hPa",
    "低云",
    "混合层",
    "偏南风",
    "偏西风",
    "冷空气压制",
    "锋后",
    "偏冷输送",
    "偏暖输送",
    "暖侧",
    "冷侧",
    "峰值更像落在偏冷侧",
    "区间中心仍偏向暖侧",
    "站点特征和天气型修正",
)


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


def _hours_between(later: datetime | None, earlier: datetime | None) -> float | None:
    if later is None or earlier is None:
        return None
    try:
        if later.tzinfo is not None and earlier.tzinfo is None:
            earlier = earlier.replace(tzinfo=later.tzinfo)
        elif later.tzinfo is None and earlier.tzinfo is not None:
            later = later.replace(tzinfo=earlier.tzinfo)
    except Exception:
        pass
    try:
        return (later - earlier).total_seconds() / 3600.0
    except Exception:
        return None


def _normalized_temp_pace_signal(
    *,
    temp_trend_c: float | None,
    temp_accel_c: float | None,
    quality_state: dict[str, Any],
) -> dict[str, float | bool | None]:
    recent_interval_min = _safe_float(quality_state.get("metar_recent_interval_min"))
    routine_cadence_min = _safe_float(quality_state.get("metar_routine_cadence_min"))
    speci_active = bool(quality_state.get("metar_speci_active"))
    speci_likely = bool(quality_state.get("metar_speci_likely"))

    hourly_trend_c = temp_trend_c
    cadence_regular = False
    short_interval_active = False
    if recent_interval_min is not None and recent_interval_min > 0.0:
        hourly_trend_c = (temp_trend_c * 60.0 / recent_interval_min) if temp_trend_c is not None else None
        if routine_cadence_min is not None and routine_cadence_min > 0.0:
            cadence_regular = 0.80 * routine_cadence_min <= recent_interval_min <= 1.30 * routine_cadence_min
            short_interval_active = recent_interval_min <= max(15.0, 0.70 * routine_cadence_min)
        else:
            cadence_regular = 20.0 <= recent_interval_min <= 70.0
            short_interval_active = recent_interval_min <= 15.0

    accel_effective_c = temp_accel_c
    if speci_active or speci_likely or short_interval_active or not cadence_regular:
        accel_effective_c = None

    return {
        "hourly_trend_c": hourly_trend_c,
        "accel_effective_c": accel_effective_c,
        "recent_interval_min": recent_interval_min,
        "cadence_regular": cadence_regular,
        "short_interval_active": short_interval_active,
        "speci_active": speci_active,
        "speci_likely": speci_likely,
    }


def _format_local_clock(value: Any) -> str:
    dt = _parse_iso_dt(value)
    if not dt:
        return ""
    try:
        now_local = datetime.now(dt.tzinfo) if dt.tzinfo is not None else datetime.now()
        if dt.date() != now_local.date():
            return dt.strftime("%Y/%m/%d %H:%M Local")
        return dt.strftime("%H:%M Local")
    except Exception:
        return ""


def _format_local_window_range(start_value: Any, end_value: Any) -> str:
    start_dt = _parse_iso_dt(start_value)
    end_dt = _parse_iso_dt(end_value)
    if not start_dt or not end_dt:
        return ""
    try:
        return f"{start_dt.strftime('%H:%M')}~{end_dt.strftime('%H:%M')} Local"
    except Exception:
        return ""


def _pick_peak_local(snapshot: dict[str, Any], primary_window: dict[str, Any]) -> Any:
    canonical = dict(snapshot.get("canonical_raw_state") or {})
    window = dict(canonical.get("window") or {})
    calc_window = dict(window.get("calc") or {})
    primary_state = dict(window.get("primary") or {})
    return calc_window.get("peak_local") or primary_state.get("peak_local") or primary_window.get("peak_local")


def _classify_report_mode(
    snapshot: dict[str, Any],
    primary_window: dict[str, Any],
    metar_diag: dict[str, Any],
    phase_now: str,
) -> str:
    posterior = dict(snapshot.get("posterior_feature_vector") or {})
    time_phase = dict(posterior.get("time_phase") or {})
    hours_to_peak = _safe_float(time_phase.get("hours_to_peak"))
    peak_dt = _parse_iso_dt(_pick_peak_local(snapshot, primary_window))
    latest_dt = _parse_iso_dt(metar_diag.get("latest_report_local"))

    if hours_to_peak is None:
        hours_to_peak = _hours_between(peak_dt, latest_dt)

    peak_summary = dict(dict(snapshot.get("peak_data") or {}).get("summary") or {})
    ranges = dict(peak_summary.get("ranges") or {})
    core_range = dict(ranges.get("core") or {})
    core_lo = _safe_float(core_range.get("lo"))
    latest_temp = _safe_float(metar_diag.get("latest_temp"))
    observed_max = _safe_float(metar_diag.get("observed_max_temp_c"))
    temp_trend = _safe_float(metar_diag.get("temp_trend_1step_c"))

    cross_day = bool(peak_dt and latest_dt and peak_dt.date() != latest_dt.date())
    if cross_day:
        return "far_synoptic"
    if hours_to_peak is not None and hours_to_peak >= FAR_REPORT_HOURS_THRESHOLD:
        return "far_synoptic"

    near_trigger = False
    if hours_to_peak is not None and hours_to_peak < NEAR_REPORT_HOURS_THRESHOLD:
        near_trigger = True
    if latest_temp is not None and core_lo is not None and latest_temp >= core_lo - 1.0:
        near_trigger = True
    if temp_trend is not None and temp_trend >= 0.3:
        near_trigger = True
    if latest_temp is not None and observed_max is not None and latest_temp >= observed_max - 0.2:
        near_trigger = True

    if near_trigger:
        return "near_obs"
    if hours_to_peak is not None:
        return "transition"
    if phase_now in {"near_window", "in_window", "post", "early_peak_watch"}:
        return "near_obs"
    return "far_synoptic" if phase_now == "far" else "transition"


def _tighten_block_spacing(text: str) -> str:
    block = str(text or "").strip()
    if not block:
        return ""
    block = re.sub(r"\n{3,}", "\n\n", block)
    block = re.sub(r"\n\n(?=• 实况提醒：)", "\n", block)
    block = re.sub(r"\n\n(?=⚠️ 关注)", "\n", block)
    return block


def _join_report_parts(parts: list[str], *, compact_after: set[int] | None = None) -> str:
    items = [_tighten_block_spacing(part) for part in parts if str(part or "").strip()]
    if not items:
        return ""
    compact_after = compact_after or set()
    out = [items[0]]
    for idx, item in enumerate(items[1:], start=1):
        sep = "\n" if (idx - 1) in compact_after else "\n\n"
        out.append(sep + item)
    return "".join(out)


def _short_cloud_text(metar_diag: dict[str, Any]) -> str:
    raw_tokens = metar_diag.get("latest_cloud_tokens")
    tokens = [str(item).strip() for item in raw_tokens] if isinstance(raw_tokens, list) else []
    tokens = [item for item in tokens if item]
    if tokens:
        if len(tokens) > 2:
            return "/".join(tokens[:2]) + "…"
        return "/".join(tokens)
    code = str(metar_diag.get("latest_cloud_code") or "").strip().upper()
    if code:
        return code
    layers = str(metar_diag.get("latest_cloud_layers") or "").strip()
    if not layers:
        return ""
    return layers if len(layers) <= 24 else layers[:24].rstrip() + "…"


def _format_temp_delta(delta_c: Any, unit: str) -> str:
    value = _safe_float(delta_c)
    if value is None:
        return ""
    if unit == "F":
        value = value * 9.0 / 5.0
    if abs(value) < 0.05:
        return "持平"
    if abs(value - round(value)) < 0.05:
        return f"{value:+.0f}°{unit}"
    return f"{value:+.1f}°{unit}"


def _fmt_obs_temp(v_c: Any, unit: str) -> str:
    value = _safe_float(v_c)
    if value is None:
        return ""
    if unit == "F":
        value = value * 9.0 / 5.0 + 32.0
    if abs(value - round(value)) < 0.05:
        return f"{value:.0f}°{unit}"
    return f"{value:.1f}°{unit}"


def _observed_max_text(metar_diag: dict[str, Any], unit: str) -> str:
    observed_max = _safe_float(metar_diag.get("observed_max_temp_c"))
    if observed_max is None:
        return ""
    observed_text = _fmt_obs_temp(observed_max, unit)
    if not observed_text:
        return ""
    max_time = _format_local_clock(metar_diag.get("observed_max_time_local"))
    if max_time:
        return f"今日已观测最高温：{observed_text}（{max_time}）"
    return f"今日已观测最高温：{observed_text}"


def _build_obs_focus_metar_block(
    metar_diag: dict[str, Any],
    *,
    unit: str,
    fmt_temp,
    fallback_text: str,
    metar_analysis_lines: list[str] | None = None,
) -> str:
    latest_temp = _safe_float(metar_diag.get("latest_temp"))
    latest_time = _format_local_clock(metar_diag.get("latest_report_local"))
    observed_max_text = _observed_max_text(metar_diag, unit)

    if latest_temp is None and not observed_max_text:
        return "📡 实况：" + str(fallback_text or "METAR 实况摘要缺失。").strip()

    parts: list[str] = ["📡 实况："]
    if latest_time:
        parts.append(f"{latest_time} ")
    if latest_temp is not None:
        parts.append(_fmt_obs_temp(latest_temp, unit))
    if observed_max_text:
        parts.append(f"，{observed_max_text}")
    lead_line = "".join(parts).strip() + "。"

    detail_bits: list[str] = []
    wind_dir = metar_diag.get("latest_wdir")
    wind_spd = _safe_float(metar_diag.get("latest_wspd"))
    try:
        if wind_dir not in (None, "", "VRB") and wind_spd is not None:
            detail_bits.append(f"风 {float(wind_dir):.0f}° {wind_spd:.0f}kt")
        elif wind_spd is not None:
            detail_bits.append(f"风速 {wind_spd:.0f}kt")
    except Exception:
        pass

    cloud_text = _short_cloud_text(metar_diag)
    if cloud_text:
        detail_bits.append(f"云 {cloud_text}")

    temp_delta = _format_temp_delta(metar_diag.get("temp_trend_1step_c"), unit)
    if temp_delta:
        detail_bits.append(f"较上一报 {temp_delta}")

    wx_state = str(metar_diag.get("latest_precip_state") or "").strip().lower()
    if wx_state and wx_state not in {"none", "unknown"}:
        detail_bits.append(f"天气 {wx_state}")

    lines = [lead_line]
    if detail_bits:
        lines.append("• " + " | ".join(detail_bits[:4]))

    first_analysis = ""
    if metar_analysis_lines:
        for raw in metar_analysis_lines:
            cleaned = str(raw or "").strip()
            if cleaned:
                first_analysis = cleaned if cleaned.startswith("•") else f"• {cleaned}"
                break
    if first_analysis:
        lines.append(first_analysis)
    return "\n".join(lines)


def _build_far_obs_reference(
    metar_diag: dict[str, Any],
    *,
    unit: str,
    fallback_text: str,
) -> str:
    latest_temp = _safe_float(metar_diag.get("latest_temp"))
    latest_time = _format_local_clock(metar_diag.get("latest_report_local"))
    observed_max_text = _observed_max_text(metar_diag, unit)
    detail_bits: list[str] = []
    if latest_time:
        detail_bits.append(latest_time)
    if latest_temp is not None:
        detail_bits.append(_fmt_obs_temp(latest_temp, unit))
    wind_dir = metar_diag.get("latest_wdir")
    wind_spd = _safe_float(metar_diag.get("latest_wspd"))
    try:
        if wind_dir not in (None, "", "VRB") and wind_spd is not None:
            detail_bits.append(f"风 {float(wind_dir):.0f}° {wind_spd:.0f}kt")
        elif wind_spd is not None:
            detail_bits.append(f"风速 {wind_spd:.0f}kt")
    except Exception:
        pass
    cloud_text = _short_cloud_text(metar_diag)
    if cloud_text:
        detail_bits.append(f"云 {cloud_text}")
    if detail_bits:
        line = f"📡 当前实况：{' | '.join(detail_bits)}"
        if observed_max_text:
            line += f"；{observed_max_text}"
        line += "（仅作背景参考）。"
        return line
    if observed_max_text:
        return f"📡 当前实况：{observed_max_text}（仅作背景参考）。"
    fallback = str(fallback_text or "").strip()
    return f"📡 当前实况：{fallback}" if fallback else "📡 当前实况：METAR 实况摘要缺失。"


def _model_curve_shape_text(shape_type: str, plateau_state: str, day_range_c: float | None, unit: str) -> str:
    lead = ""
    if plateau_state == "broad" or shape_type == "broad_plateau":
        lead = "曲线偏平台型"
    elif plateau_state == "narrow":
        lead = "曲线接近窄平台"
    elif shape_type == "single_peak":
        lead = "曲线偏单峰"
    elif shape_type == "multi_peak":
        lead = "曲线存在多峰可能"

    if day_range_c is None:
        return lead

    day_range = float(day_range_c)
    if unit == "F":
        day_range = day_range * 9.0 / 5.0
    if abs(day_range - round(day_range)) < 0.05:
        range_txt = f"日较差约 {day_range:.0f}°{unit}"
    else:
        range_txt = f"日较差约 {day_range:.1f}°{unit}"

    tail = ""
    if day_range_c <= 3.0:
        tail = "模式自身没有给出明显冲高"
    elif day_range_c >= 6.0:
        tail = "模式保留了一定白天升温空间"

    parts = [part for part in (lead, range_txt, tail) if part]
    return "，".join(parts)


def _format_prob_pct(value: Any) -> str:
    prob = _safe_float(value)
    if prob is None:
        return ""
    return f"{round(max(0.0, min(1.0, prob)) * 100.0):.0f}%"


def _format_hour_of_day(hour_value: float | None) -> str:
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


def _ensemble_transition_detail_label(detail_label: str) -> str:
    return {
        "neutral_stable": "静稳维持",
        "weak_warm_transition": "暖侧试探",
        "weak_cold_transition": "冷侧回摆",
    }.get(str(detail_label or "").strip(), "")


def _ensemble_path_trigger_text(path_label: str, bottleneck_text: str, *, path_detail: str = "") -> str:
    bottleneck = str(bottleneck_text or "").strip()
    if path_label == "warm_support":
        if "混合层" in bottleneck or "低云" in bottleneck:
            return "若低云更早破碎、混合层做深更快，上沿才有再抬条件"
        if "风" in bottleneck:
            return "若低层暖输送更早落地、风向更快转到暖侧，上沿才有再抬条件"
        return "若低层暖输送更早落地，上沿才有再抬条件"
    if path_label == "cold_suppression":
        if "混合层" in bottleneck or "低云" in bottleneck:
            return "若稳层和低云维持更久，冲高空间仍会受压"
        if "风" in bottleneck:
            return "若偏冷输送维持更久，温度仍会走偏受压路径"
        return "若偏冷输送或稳定层维持更久，温度仍会偏受压"
    if path_detail == "neutral_stable":
        return "若低云和近地稳层变化不大，温度更易在中间区间窄幅摆动"
    if path_detail == "weak_warm_transition":
        return "若暖输送更早接地或低云提前松动，上沿有小幅上修空间"
    if path_detail == "weak_cold_transition":
        return "若偏冷输送维持更久或低云更慢破碎，区间更易贴近下沿"
    return "若低层改造幅度一般，温度更容易落在中间路径附近"


def _ensemble_path_label(path_label: str, *, summary: dict[str, Any] | None = None) -> str:
    key = str(path_label or "").strip()
    if key == "transition":
        summary = summary if isinstance(summary, dict) else {}
        transition_detail = str(summary.get("transition_detail") or summary.get("dominant_path_detail") or "").strip()
        detail_label = _ensemble_transition_detail_label(transition_detail)
        if detail_label:
            return detail_label
        return "中性过渡"
    return {
        "warm_support": "暖输送抬升",
        "cold_suppression": "冷抑制",
    }.get(key, key or "主路径")


def _ensemble_path_side(path_label: str, *, summary: dict[str, Any] | None = None) -> str:
    key = str(path_label or "").strip()
    summary = summary if isinstance(summary, dict) else {}
    if key == "transition":
        key = str(summary.get("transition_detail") or summary.get("dominant_path_detail") or "").strip() or "transition"
    return {
        "warm_support": "warm",
        "weak_warm_transition": "warm",
        "cold_suppression": "cold",
        "weak_cold_transition": "cold",
        "neutral_stable": "neutral",
        "transition": "neutral",
    }.get(key, "neutral")


def _format_signed_temp(value: float | None, *, unit: str = "C") -> str:
    temp = _safe_float(value)
    if temp is None:
        return ""
    return f"{temp:+.1f}°{unit}"


def _pick_first_float(*values: Any) -> float | None:
    for value in values:
        parsed = _safe_float(value)
        if parsed is not None:
            return parsed
    return None


def _format_lead_time(hours: float | None) -> str:
    value = _safe_float(hours)
    if value is None:
        return ""
    value = max(0.0, float(value))
    if value < 1.0:
        minutes = max(10, int(round(value * 60.0 / 10.0) * 10))
        return f"约{minutes}分钟"
    half_hour = round(value * 2.0) / 2.0
    if abs(half_hour - round(half_hour)) <= 0.01:
        return f"约{int(round(half_hour))}小时"
    return f"约{half_hour:.1f}小时"


def _live_window_progress_text(
    snapshot: dict[str, Any],
    metar_diag: dict[str, Any],
    *,
    unit: str,
) -> str:
    posterior = dict(snapshot.get("posterior_feature_vector") or {})
    time_phase = dict(posterior.get("time_phase") or {})
    observation_state = dict(posterior.get("observation_state") or {})
    peak_summary = dict(dict(snapshot.get("peak_data") or {}).get("summary") or {})
    ranges = dict(peak_summary.get("ranges") or {})
    core_range = dict(ranges.get("core") or {})
    display_range = dict(ranges.get("display") or {})

    latest_temp = _pick_first_float(
        metar_diag.get("latest_temp"),
        observation_state.get("latest_temp_c"),
    )
    if latest_temp is None:
        return ""

    core_lo = _pick_first_float(
        core_range.get("lo"),
        core_range.get("lo_c"),
        display_range.get("lo"),
        display_range.get("lo_c"),
    )
    core_hi = _pick_first_float(
        core_range.get("hi"),
        core_range.get("hi_c"),
        display_range.get("hi"),
        display_range.get("hi_c"),
    )
    hours_to_window_start = _safe_float(time_phase.get("hours_to_window_start"))
    hours_to_peak = _safe_float(time_phase.get("hours_to_peak"))

    if hours_to_window_start is not None and hours_to_window_start > 0.15:
        lead_text = _format_lead_time(hours_to_window_start)
        if core_lo is not None:
            gap_to_lo = core_lo - latest_temp
            if gap_to_lo >= 1.2:
                return f"距峰值窗开始{lead_text}，当前还比区间下沿低约 {gap_to_lo:.1f}°{unit}"
            if gap_to_lo >= 0.35:
                return f"距峰值窗开始{lead_text}，当前正在逼近区间下沿，还差约 {gap_to_lo:.1f}°{unit}"
            if core_hi is not None:
                gap_to_hi = core_hi - latest_temp
                if gap_to_hi >= 0.9:
                    return f"距峰值窗开始{lead_text}，当前已贴近区间下沿，接下来主要看能否继续往区间中上段推进"
                if gap_to_hi >= 0.2:
                    return f"距峰值窗开始{lead_text}，当前已进到区间中段，接下来主要看还能不能继续往上段摸"
                return f"距峰值窗开始{lead_text}，当前已贴近区间上沿，接下来主要看还能不能再抬一点"
        return f"距峰值窗开始{lead_text}，当前主要看实况能不能继续往窗口里推进"

    if hours_to_peak is not None:
        lead_text = _format_lead_time(hours_to_peak)
        if core_hi is not None:
            gap_to_hi = core_hi - latest_temp
            if gap_to_hi >= 1.0:
                return f"离峰值时点{lead_text}，当前仍在区间下沿附近，能否继续往上段推进更关键"
            if gap_to_hi >= 0.2:
                return f"离峰值时点{lead_text}，当前已进到区间中段，接下来主要看还能不能继续往上段摸"
            return f"离峰值时点{lead_text}，当前已贴近区间上沿，接下来主要看上沿还能不能再抬"
        return f"离峰值时点{lead_text}，当前主要看实况能不能继续往峰值窗里推进"

    return ""


def _ensemble_dominance_phrase(
    split_state: str,
    dominant_prob: float | None,
    dominant_margin_prob: float | None,
) -> str:
    split = str(split_state or "").strip()
    prob = _safe_float(dominant_prob) or 0.0
    margin = _safe_float(dominant_margin_prob) or 0.0
    if split == "clustered":
        if prob >= 0.85 or margin >= 0.60:
            return "几乎一边倒"
        return "主支很稳"
    if split == "mixed":
        if prob >= 0.58 or margin >= 0.18:
            return "主支占优"
        return "主次并存"
    return "主支优势有限"


def _ensemble_secondary_branch_text(
    *,
    dominant_path: str,
    split_state: str,
    transition_detail: str,
    transition_detail_prob: float | None,
    warm_support_prob: float | None,
    transition_prob: float | None,
    cold_suppression_prob: float | None,
) -> str:
    candidates: list[tuple[str, float]] = []
    if dominant_path != "warm_support":
        warm_prob = _safe_float(warm_support_prob)
        if warm_prob is not None and warm_prob > 0.0:
            candidates.append(("暖输送抬升", warm_prob))
    if dominant_path != "transition":
        trans_prob = _pick_first_float(transition_detail_prob, transition_prob)
        trans_label = _ensemble_transition_detail_label(transition_detail) or "中性过渡"
        if trans_prob is not None and trans_prob > 0.0:
            candidates.append((trans_label, trans_prob))
    if dominant_path != "cold_suppression":
        cold_prob = _safe_float(cold_suppression_prob)
        if cold_prob is not None and cold_prob > 0.0:
            candidates.append(("冷抑制", cold_prob))

    ordered = sorted(candidates, key=lambda item: item[1], reverse=True)
    ordered = [item for item in ordered if item[1] >= 0.04]
    if not ordered:
        return ""

    split = str(split_state or "").strip()
    top_label, top_prob = ordered[0]
    top_pct = _format_prob_pct(top_prob)
    if split == "clustered":
        return f"次支只剩{top_label} {top_pct}"
    if split == "mixed":
        extras = [f"{label} {_format_prob_pct(prob)}" for label, prob in ordered[:2]]
        return f"次支仍留着{' / '.join(extras)}"
    extras = [f"{label} {_format_prob_pct(prob)}" for label, prob in ordered[:3]]
    return f"分支仍散，{' / '.join(extras)}"


def _ensemble_amplitude_band_text(
    *,
    dominant_path: str,
    transition_detail: str,
    delta_t850_p10_c: float | None,
    delta_t850_p90_c: float | None,
) -> str:
    p10 = _safe_float(delta_t850_p10_c)
    p90 = _safe_float(delta_t850_p90_c)
    if p10 is None or p90 is None:
        return ""
    if dominant_path == "warm_support" and p10 > 0.2:
        return f"暖支振幅大多落在 {_format_signed_temp(p10)}~{_format_signed_temp(p90)}"
    if dominant_path == "cold_suppression" and p90 < -0.2:
        return f"冷支振幅大多落在 {_format_signed_temp(p10)}~{_format_signed_temp(p90)}"
    if dominant_path == "transition":
        detail_label = _ensemble_transition_detail_label(transition_detail) or "过渡支"
        return f"{detail_label}振幅大多落在 {_format_signed_temp(p10)}~{_format_signed_temp(p90)}"
    return ""


def _ensemble_convergence_text(
    split_state: str,
    dominant_prob: float | None,
    *,
    signal_dispersion_c: float | None = None,
) -> str:
    state = str(split_state or "").strip()
    prob = _safe_float(dominant_prob)
    dispersion = _safe_float(signal_dispersion_c)
    dispersion_text = ""
    if dispersion is not None:
        if dispersion <= 0.8:
            dispersion_text = "，路径振幅也偏集中"
        elif dispersion >= 2.2:
            dispersion_text = "，但振幅离散度仍偏大"
    if state == "clustered":
        if prob is not None:
            return f"系集已较收敛（主路径约 {_format_prob_pct(prob)}）{dispersion_text}"
        return "系集已较收敛"
    if state == "mixed":
        if prob is not None:
            return f"系集有主次之分但未完全收敛（主路径约 {_format_prob_pct(prob)}）{dispersion_text}"
        return "系集有主次之分但未完全收敛"
    if prob is not None:
        return f"系集分歧仍在（主路径仅约 {_format_prob_pct(prob)}）{dispersion_text}"
    return "系集分歧仍在"


def _build_far_single_run_reference_text(
    snapshot: dict[str, Any],
    primary_window: dict[str, Any],
    metar_diag: dict[str, Any],
    *,
    unit: str,
    fmt_temp,
) -> str:
    canonical = dict(snapshot.get("canonical_raw_state") or {})
    forecast = dict(canonical.get("forecast") or {})
    window = dict(canonical.get("window") or {})
    calc_window = dict(window.get("calc") or {})
    shape = dict(canonical.get("shape") or {})
    shape_forecast = dict(shape.get("forecast") or {})
    meta = dict(forecast.get("meta") or {})

    peak_temp = (
        _safe_float(calc_window.get("peak_temp_c"))
        or _safe_float(shape_forecast.get("global_peak_temp_c"))
        or _safe_float(primary_window.get("peak_temp_c"))
    )
    if peak_temp is None:
        return ""

    model_name = str(meta.get("model") or "").strip().upper()
    peak_local = calc_window.get("peak_local") or primary_window.get("peak_local")
    start_local = calc_window.get("start_local") or primary_window.get("start_local")
    end_local = calc_window.get("end_local") or primary_window.get("end_local")
    peak_clock = _format_local_clock(peak_local)
    line = f"{model_name} 数值预报峰值约 {fmt_temp(peak_temp)}" if model_name else f"数值预报峰值约 {fmt_temp(peak_temp)}"
    if peak_clock:
        line += f"（{peak_clock}前后）"
    window_range = _format_local_window_range(start_local, end_local)
    if window_range:
        line += f"；峰值窗 {window_range}"
    return line


def _strip_bullet_prefix(text: str) -> str:
    out = str(text or "").strip()
    if out.startswith("•"):
        out = out[1:].strip()
    return out.rstrip("。")


def _far_model_runtime_text(snapshot: dict[str, Any]) -> str:
    canonical = dict(snapshot.get("canonical_raw_state") or {})
    forecast = dict(canonical.get("forecast") or {})
    meta = dict(forecast.get("meta") or {})
    model_name = str(meta.get("model") or "").strip().upper()
    runtime = str(meta.get("runtime") or "").strip()
    label = " ".join(part for part in (model_name, runtime) if part)
    return label


def _build_far_deterministic_line(
    snapshot: dict[str, Any],
    syn_lines: list[str],
    metar_diag: dict[str, Any],
) -> str:
    signal_line = _strip_bullet_prefix(_build_far_signal_basis_line(snapshot))
    if signal_line:
        return signal_line

    synoptic_summary = dict(snapshot.get("synoptic_summary") or {})
    summary = dict(synoptic_summary.get("summary") or {})
    basis_text = _pick_far_basis_text(snapshot, syn_lines)
    mechanism = _rephrase_far_basis_text(basis_text) or _far_mechanism_focus(summary.get("pathway"))
    if _is_generic_far_text(mechanism):
        mechanism = _far_profile_lead_phrase(metar_diag)
    impact_text = _summarize_impact_text(summary.get("impact"))
    return _background_compact_clause(mechanism, _far_directional_take(mechanism, impact_text)).rstrip("。")


def _build_far_ensemble_line(snapshot: dict[str, Any]) -> str:
    canonical = dict(snapshot.get("canonical_raw_state") or {})
    forecast = dict(canonical.get("forecast") or {})
    ensemble = dict(forecast.get("ensemble_factor") or {})
    summary = dict(ensemble.get("summary") or {})
    probabilities = dict(ensemble.get("probabilities") or {})
    diagnostics = dict(ensemble.get("diagnostics") or {})
    context = dict(forecast.get("context") or {})
    if not probabilities:
        return ""

    ordered = sorted(
        ((key, _safe_float(value) or 0.0) for key, value in probabilities.items()),
        key=lambda item: item[1],
        reverse=True,
    )
    ordered = [item for item in ordered if item[1] > 0.0]
    if not ordered:
        return ""

    dominant_path = str(summary.get("dominant_path") or ordered[0][0])
    dominant_prob = _safe_float(summary.get("dominant_prob")) or ordered[0][1]
    split_state = str(summary.get("split_state") or "")
    dominant_detail = str(summary.get("dominant_path_detail") or summary.get("transition_detail") or "")
    transition_detail = str(summary.get("transition_detail") or "")
    dominant_label = _ensemble_path_label(dominant_path, summary=summary)
    trigger = _ensemble_path_trigger_text(
        dominant_path,
        str(context.get("bottleneck_text") or ""),
        path_detail=dominant_detail,
    )
    dominance_phrase = _ensemble_dominance_phrase(
        split_state,
        dominant_prob,
        summary.get("dominant_margin_prob"),
    )
    secondary_text = _ensemble_secondary_branch_text(
        dominant_path=dominant_path,
        split_state=split_state,
        transition_detail=transition_detail,
        transition_detail_prob=summary.get("transition_detail_prob"),
        warm_support_prob=probabilities.get("warm_support"),
        transition_prob=probabilities.get("transition"),
        cold_suppression_prob=probabilities.get("cold_suppression"),
    )
    amplitude_text = _ensemble_amplitude_band_text(
        dominant_path=dominant_path,
        transition_detail=transition_detail,
        delta_t850_p10_c=diagnostics.get("delta_t850_p10_c"),
        delta_t850_p90_c=diagnostics.get("delta_t850_p90_c"),
    )

    line = "ECMWF ENS"
    if split_state == "clustered":
        line += f" {dominance_phrase}收敛到{dominant_label}路径（主路径约 {_format_prob_pct(dominant_prob)}）"
    elif split_state == "mixed":
        line += f" {dominance_phrase}，主路径为{dominant_label}（主路径约 {_format_prob_pct(dominant_prob)}）"
    else:
        line += f" 仍有分歧，主路径为{dominant_label}（主路径约 {_format_prob_pct(dominant_prob)}）"
    if secondary_text:
        line += f"；{secondary_text}"
    if amplitude_text:
        line += f"；{amplitude_text}"
    if trigger:
        line += f"；{trigger}"
    return line


def _build_live_path_reference_line(
    snapshot: dict[str, Any],
    metar_diag: dict[str, Any],
    *,
    report_mode: str,
    unit: str,
) -> str:
    if report_mode not in {"transition", "near_obs"}:
        return ""
    posterior = dict(snapshot.get("posterior_feature_vector") or {})
    ensemble_state = dict(posterior.get("ensemble_path_state") or {})
    full_dominant_path = str(ensemble_state.get("dominant_path") or "")
    active_subset = bool(ensemble_state.get("matched_subset_active"))
    dominant_path = str(
        (
            ensemble_state.get("matched_dominant_path")
            if active_subset
            else ensemble_state.get("dominant_path")
        ) or ""
    )
    observed_path = str(ensemble_state.get("observed_path") or "")
    progress_text = _live_window_progress_text(snapshot, metar_diag, unit=unit)

    if dominant_path and observed_path:
        dominant_label = _ensemble_path_label(
            dominant_path,
            summary={
                "transition_detail": (
                    ensemble_state.get("matched_transition_detail")
                    if active_subset
                    else ensemble_state.get("transition_detail")
                ),
                "dominant_path_detail": (
                    ensemble_state.get("matched_dominant_path_detail")
                    if active_subset
                    else ensemble_state.get("dominant_path_detail")
                ),
            },
        )
        observed_label = _ensemble_path_label(
            observed_path,
            summary={
                "transition_detail": ensemble_state.get("observed_path_detail"),
                "dominant_path_detail": ensemble_state.get("observed_path_detail"),
            },
        )
        if dominant_label and observed_label:
            match_state = str(ensemble_state.get("observed_alignment_match_state") or "")
            confidence = str(ensemble_state.get("observed_alignment_confidence") or "")
            observed_locked = bool(ensemble_state.get("observed_path_locked"))
            dominant_prob = _format_prob_pct(
                ensemble_state.get("matched_dominant_prob") if active_subset else ensemble_state.get("dominant_prob")
            )
            secondary_text = _ensemble_secondary_branch_text(
                dominant_path=dominant_path,
                split_state=str(
                    (
                        ensemble_state.get("matched_split_state")
                        if active_subset
                        else ensemble_state.get("split_state")
                    ) or ""
                ),
                transition_detail=str(
                    (
                        ensemble_state.get("matched_transition_detail")
                        if active_subset
                        else ensemble_state.get("transition_detail")
                    ) or ""
                ),
                transition_detail_prob=(
                    ensemble_state.get("matched_transition_detail_prob")
                    if active_subset
                    else ensemble_state.get("transition_detail_prob")
                ),
                warm_support_prob=(
                    ensemble_state.get("matched_warm_support_prob")
                    if active_subset
                    else ensemble_state.get("warm_support_prob")
                ),
                transition_prob=(
                    ensemble_state.get("matched_transition_prob")
                    if active_subset
                    else ensemble_state.get("transition_prob")
                ),
                cold_suppression_prob=(
                    ensemble_state.get("matched_cold_suppression_prob")
                    if active_subset
                    else ensemble_state.get("cold_suppression_prob")
                ),
            )
            subset_intro = ""
            if active_subset:
                if full_dominant_path and full_dominant_path != dominant_path:
                    subset_intro = "当前实况已筛掉和实况不符的主支；"
                else:
                    subset_intro = "当前实况已筛掉明显不符路径；"
            if active_subset:
                line = f"{subset_intro}保留下来的系集里，{observed_label}"
                if dominant_prob:
                    line += f"约 {dominant_prob}"
                if progress_text:
                    line += f"，{progress_text}"
                if secondary_text:
                    line += f"；{secondary_text}"
                return line

            if match_state == "exact" and observed_locked:
                line = f"当前实况高度贴合系集主路径，{progress_text}" if progress_text else f"当前实况高度贴合系集主路径，正沿{observed_label}演进"
                if secondary_text:
                    line += f"；{secondary_text}"
                if dominant_prob:
                    line += f"（主路径约 {dominant_prob}）"
                return line
            if match_state in {"exact", "path"} and confidence in {"high", "partial"}:
                line = f"当前实况和系集主路径大体一致，{progress_text}" if progress_text else f"当前实况在走{observed_label}路径，与系集主判断大体一致"
                if secondary_text:
                    line += f"；{secondary_text}"
                if dominant_prob:
                    line += f"（主路径约 {dominant_prob}）"
                return line
            if confidence in {"high", "partial"} and observed_label != dominant_label:
                observed_side = _ensemble_path_side(
                    observed_path,
                    summary={
                        "transition_detail": ensemble_state.get("observed_path_detail"),
                        "dominant_path_detail": ensemble_state.get("observed_path_detail"),
                    },
                )
                dominant_side = _ensemble_path_side(
                    dominant_path,
                    summary={
                        "transition_detail": ensemble_state.get("transition_detail"),
                        "dominant_path_detail": ensemble_state.get("dominant_path_detail"),
                    },
                )
                if observed_side == "warm" and dominant_side in {"cold", "neutral"}:
                    line = f"当前实况正偏离系集主支，开始往{observed_label}演进，而主支仍偏{dominant_label}"
                elif observed_side == "cold" and dominant_side in {"warm", "neutral"}:
                    line = f"当前实况正偏离系集主支，开始往{observed_label}演进，而主支仍偏{dominant_label}"
                elif observed_side == "neutral" and dominant_side == "warm":
                    line = f"当前实况没跟上系集主暖支，已转到{observed_label}，和主支{dominant_label}已有偏离"
                elif observed_side == "neutral" and dominant_side == "cold":
                    line = f"当前实况没跟上系集主冷支，已转到{observed_label}，和主支{dominant_label}已有偏离"
                else:
                    line = f"当前实况在走{observed_label}路径，和系集主支{dominant_label}不一致"
                if progress_text:
                    line += f"；{progress_text}"
                if secondary_text:
                    line += f"；{secondary_text}"
                if dominant_prob:
                    line += f"（主路径约 {dominant_prob}）"
                return line

    observation_state = dict(posterior.get("observation_state") or {})
    cloud_state = dict(posterior.get("cloud_radiation_state") or {})
    transport_state = dict(posterior.get("transport_state") or {})
    quality_state = dict(posterior.get("quality_state") or {})
    temp_trend_c = _safe_float(observation_state.get("temp_trend_c"))
    temp_accel_c = _safe_float(observation_state.get("temp_accel_2step_c"))
    radiation_eff = _safe_float(cloud_state.get("radiation_eff"))
    cloud_trend = str(cloud_state.get("cloud_trend") or "").strip().lower()
    transport_key = str(transport_state.get("transport_state") or "").strip().lower()
    thermal_key = str(transport_state.get("thermal_advection_state") or "").strip().lower()
    surface_bias = str(transport_state.get("surface_bias") or "").strip().lower()
    pace_signal = _normalized_temp_pace_signal(
        temp_trend_c=temp_trend_c,
        temp_accel_c=temp_accel_c,
        quality_state=quality_state,
    )
    hourly_trend_c = _safe_float(pace_signal.get("hourly_trend_c"))
    accel_effective_c = _safe_float(pace_signal.get("accel_effective_c"))
    short_interval_active = bool(pace_signal.get("short_interval_active"))
    speci_active = bool(pace_signal.get("speci_active"))
    speci_likely = bool(pace_signal.get("speci_likely"))

    cloud_clearing = any(token in cloud_trend for token in {"clearing", "break", "散", "减", "少"})
    cloud_thickening = any(token in cloud_trend for token in {"thicken", "increase", "增", "厚", "多"})
    env_warm_support = bool((radiation_eff is not None and radiation_eff >= 0.62) or cloud_clearing)
    env_cold_support = bool((radiation_eff is not None and radiation_eff <= 0.50) or cloud_thickening)

    warm_rate_support = False
    cold_rate_support = False
    if accel_effective_c is not None:
        if accel_effective_c >= 0.10:
            warm_rate_support = True
        elif accel_effective_c <= -0.08:
            cold_rate_support = True
    trend_reference_c = hourly_trend_c if hourly_trend_c is not None else temp_trend_c
    trend_signal_allowed = not (short_interval_active or speci_active or speci_likely)
    if trend_reference_c is not None and trend_signal_allowed:
        if trend_reference_c >= 0.40 and (
            (accel_effective_c is not None and accel_effective_c >= 0.04)
            or env_warm_support
        ):
            warm_rate_support = True
        elif trend_reference_c <= -0.22 and (
            (accel_effective_c is not None and accel_effective_c <= -0.04)
            or env_cold_support
        ):
            cold_rate_support = True

    warm_transport = transport_key == "warm" or thermal_key in {"confirmed", "probable"} or "warm" in surface_bias
    cold_transport = transport_key == "cold" or "cold" in surface_bias

    if warm_transport and warm_rate_support and env_warm_support:
        if progress_text:
            return f"当前升温节奏仍在往窗口里推进，{progress_text}"
        if radiation_eff is not None and radiation_eff >= 0.62:
            return "当前升温节奏和辐射条件已提前兑现暖侧路径"
        return "当前升温节奏已提前兑现暖侧路径"
    if cold_transport and cold_rate_support and env_cold_support:
        if progress_text:
            return f"当前升温兑现偏慢，{progress_text}"
        if radiation_eff is not None and radiation_eff <= 0.50:
            return "当前升温节奏和辐射条件仍在兑现冷抑制路径"
        return "当前升温节奏仍在兑现冷抑制路径"
    if warm_rate_support and env_warm_support:
        if progress_text:
            return f"当前节奏略快于模式主路径，{progress_text}"
        return "当前升温节奏比模式主路径略快"
    if cold_rate_support and env_cold_support:
        if progress_text:
            return f"当前节奏略慢于模式主路径，{progress_text}"
        return "当前升温节奏比模式主路径偏慢"
    return ""


def _build_far_ensemble_path_block(snapshot: dict[str, Any]) -> str:
    canonical = dict(snapshot.get("canonical_raw_state") or {})
    forecast = dict(canonical.get("forecast") or {})
    ensemble = dict(forecast.get("ensemble_factor") or {})
    summary = dict(ensemble.get("summary") or {})
    probabilities = dict(ensemble.get("probabilities") or {})
    source = dict(ensemble.get("source") or {})
    context = dict(forecast.get("context") or {})

    if not probabilities:
        return ""

    ordered = sorted(
        ((key, _safe_float(value) or 0.0) for key, value in probabilities.items()),
        key=lambda item: item[1],
        reverse=True,
    )
    ordered = [item for item in ordered if item[1] > 0.0]
    if not ordered:
        return ""

    dominant_path = str(summary.get("dominant_path") or ordered[0][0])
    dominant_prob = _safe_float(summary.get("dominant_prob")) or ordered[0][1]
    split_state = str(summary.get("split_state") or "")
    runtime = str(source.get("runtime_used") or source.get("runtime_requested") or "").strip()

    lead = "主导路径"
    if split_state == "split":
        lead = "路径分歧较大"
    elif split_state == "mixed":
        lead = "存在多条竞争路径"

    header = "🔀 系集路径："
    first_label = f"{_ensemble_path_label(dominant_path, summary=summary)}路径"
    lines = [header]
    first_line = f"• {lead}偏向{first_label}（约 {_format_prob_pct(dominant_prob)}）"
    if runtime:
        first_line += f"，参考 ECMWF ENS {runtime}"
    first_line += "。"
    lines.append(first_line)

    bottleneck_text = str(context.get("bottleneck_text") or "")
    for key, prob in ordered[:2]:
        label = _ensemble_path_label(key, summary=summary)
        trigger = _ensemble_path_trigger_text(
            key,
            bottleneck_text,
            path_detail=str(summary.get("transition_detail") or ""),
        )
        lines.append(f"• {label}约 {_format_prob_pct(prob)}，{trigger}。")

    return "\n".join(lines)


def _build_far_outlook_block(
    snapshot: dict[str, Any],
    syn_lines: list[str],
    metar_diag: dict[str, Any],
    primary_window: dict[str, Any],
    *,
    unit: str,
    fmt_temp,
) -> str:
    runtime_text = _far_model_runtime_text(snapshot)
    header = "🧭 形势与路径："
    if runtime_text:
        header = f"🧭 形势与路径（{runtime_text}）："

    lines = [header]
    deterministic_line = _build_far_deterministic_line(snapshot, syn_lines, metar_diag)
    if deterministic_line:
        lines.append(f"• {deterministic_line}。")

    ensemble_line = _build_far_ensemble_line(snapshot)
    if ensemble_line:
        lines.append(f"• {ensemble_line}。")

    model_peak_line = _build_far_single_run_reference_text(
        snapshot,
        primary_window,
        metar_diag,
        unit=unit,
        fmt_temp=fmt_temp,
    )
    if model_peak_line:
        lines.append(f"• {model_peak_line}。")

    return "\n".join(lines) if len(lines) > 1 else ""


def _build_far_model_forecast_block(
    snapshot: dict[str, Any],
    primary_window: dict[str, Any],
    metar_diag: dict[str, Any],
    *,
    unit: str,
    fmt_temp,
) -> str:
    canonical = dict(snapshot.get("canonical_raw_state") or {})
    forecast = dict(canonical.get("forecast") or {})
    window = dict(canonical.get("window") or {})
    calc_window = dict(window.get("calc") or {})
    shape = dict(canonical.get("shape") or {})
    shape_forecast = dict(shape.get("forecast") or {})
    meta = dict(forecast.get("meta") or {})
    ensemble = dict(forecast.get("ensemble_factor") or {})

    peak_temp = (
        _safe_float(calc_window.get("peak_temp_c"))
        or _safe_float(shape_forecast.get("global_peak_temp_c"))
        or _safe_float(primary_window.get("peak_temp_c"))
    )
    peak_local = calc_window.get("peak_local") or primary_window.get("peak_local")
    start_local = calc_window.get("start_local") or primary_window.get("start_local")
    end_local = calc_window.get("end_local") or primary_window.get("end_local")
    model_name = str(meta.get("model") or "").strip().upper()
    runtime = str(meta.get("runtime") or "").strip()
    model_label = " ".join(part for part in (model_name, runtime) if part) or "当前模型"

    header = "📈 单跑参考：" if ensemble else "📈 模式温度："
    lines = [header]
    if peak_temp is not None:
        prefix = "当前单跑原始 2m 峰值约" if ensemble else f"{model_label} 原始 2m 峰值约"
        peak_line = f"{prefix} {fmt_temp(peak_temp)}"
        peak_clock = _format_local_clock(peak_local)
        if peak_clock:
            peak_line += f"，{peak_clock}前后见顶"
        window_range = _format_local_window_range(start_local, end_local)
        if window_range:
            peak_line += f"（峰值窗 {window_range}）"
        if ensemble:
            peak_line += "，仅作单跑参考"
        lines.append(f"• {peak_line}。")

    shape_line = _model_curve_shape_text(
        str(shape_forecast.get("shape_type") or ""),
        str(shape_forecast.get("plateau_state") or ""),
        _safe_float(shape_forecast.get("day_range_c")) or _safe_float(metar_diag.get("model_day_range_c")),
        unit,
    )
    if shape_line:
        lines.append(f"• {shape_line}。")

    return "\n".join(lines) if len(lines) > 1 else ""


def _build_far_ensemble_basis_line(snapshot: dict[str, Any]) -> str:
    canonical = dict(snapshot.get("canonical_raw_state") or {})
    forecast = dict(canonical.get("forecast") or {})
    ensemble = dict(forecast.get("ensemble_factor") or {})
    summary = dict(ensemble.get("summary") or {})
    probabilities = dict(ensemble.get("probabilities") or {})
    dominant_path = str(summary.get("dominant_path") or "")
    dominant_prob = _safe_float(summary.get("dominant_prob"))
    split_state = str(summary.get("split_state") or "")

    if split_state == "split":
        warm_prob = _format_prob_pct(probabilities.get("warm_support"))
        cold_prob = _format_prob_pct(probabilities.get("cold_suppression"))
        if warm_prob and cold_prob:
            return f"• ECMWF 系集没有收敛到单一路径，暖输送约 {warm_prob}、冷抑制约 {cold_prob}。"
        return "• ECMWF 系集没有收敛到单一路径，远期更该按多路径情景看待。"
    if dominant_path and dominant_prob is not None:
        return f"• ECMWF 系集当前更偏 {_ensemble_path_label(dominant_path, summary=summary)}路径（约 {_format_prob_pct(dominant_prob)}）。"
    return ""


def _build_far_signal_basis_line(snapshot: dict[str, Any]) -> str:
    canonical = dict(snapshot.get("canonical_raw_state") or {})
    forecast = dict(canonical.get("forecast") or {})
    h500 = dict(forecast.get("h500") or {})
    h850_review = dict(forecast.get("h850_review") or {})
    sounding = dict(forecast.get("sounding") or {})
    thermo = dict(sounding.get("thermo") or {})

    factors: list[str] = []

    thermal_role = str(h500.get("thermal_role") or "")
    tmax_bias_label = str(h500.get("tmax_bias_label") or "")
    if thermal_role == "cold_high_suppression" or "压温" in tmax_bias_label:
        factors.append("500hPa 冷高压/脊后下沉对上沿有压制")
    elif thermal_role == "warm_ridge_support" or "增温" in tmax_bias_label:
        factors.append("500hPa 脊前增温背景对上沿有托举")

    advection_type = str(h850_review.get("advection_type") or "")
    surface_role = str(h850_review.get("surface_role") or "")
    if advection_type == "cold":
        if surface_role == "background":
            factors.append("850hPa 偏冷输送更多作为背景约束存在")
        else:
            factors.append("850hPa 偏冷输送若继续落地，会继续压低上沿")
    elif advection_type == "warm":
        if surface_role == "background":
            factors.append("850hPa 偏暖输送提供了偏暖背景")
        else:
            factors.append("850hPa 偏暖输送若继续落地，会给上沿更多托举")

    layer_findings = [str(item).strip().rstrip("。") for item in (thermo.get("layer_findings") or sounding.get("layer_findings") or []) if str(item).strip()]
    if layer_findings:
        factors.append(layer_findings[0])

    deduped: list[str] = []
    seen: set[str] = set()
    for factor in factors:
        if factor in seen:
            continue
        seen.add(factor)
        deduped.append(factor)
        if len(deduped) >= 3:
            break

    if not deduped:
        return ""
    return f"• {'；'.join(deduped)}。"


def _obs_reasoning_line(latest_temp: float | None, trend: float | None, unit: str) -> str:
    if latest_temp is None:
        return ""
    temp_txt = _fmt_obs_temp(latest_temp, unit)
    if trend is None or abs(trend) < 0.15:
        return f"最新报还在 {temp_txt} 一带横着走，短时没看到立刻冲顶的速度"
    if trend >= 0.35:
        return f"最新报已经到 {temp_txt}，而且还在往上爬，但这股斜率能不能续上还要看下一报"
    return f"最新报冲到 {temp_txt} 后有点放缓，后面再大幅上冲要看下一报能不能重新提速"


def _impact_reasoning_text(impact: str) -> str:
    impact_txt = str(impact or "").strip()
    if any(key in impact_txt for key in ("偏下沿", "受压", "更容易被压住", "偏受限", "空间有限")):
        return "峰值更像落在偏冷侧"
    if "更可能比原先预报略高" in impact_txt:
        return "区间中心仍偏向暖侧"
    if "更可能比原先预报略低" in impact_txt:
        return "区间中心仍偏向冷侧"
    if any(key in impact_txt for key in ("偏上沿", "上修空间")):
        return "若后段继续配合，仍保留小幅补涨余地"
    return ""


def _mechanism_condition_text(mechanism: str) -> str:
    mechanism_txt = str(mechanism or "").strip()
    if not mechanism_txt:
        return ""
    replacements = (
        ("已开始接管", "继续接管"),
        ("正在增强", "继续增强"),
    )
    out = mechanism_txt
    for src, dst in replacements:
        if src in out:
            out = out.replace(src, dst)
            break
    return out


def _mechanism_basis_line(mechanism: str, impact: str) -> str:
    mechanism_txt = str(mechanism or "").strip()
    if not mechanism_txt:
        return ""
    impact_txt = _impact_reasoning_text(impact)
    condition = _mechanism_condition_text(mechanism_txt)
    if "锋后偏南气流" in mechanism_txt:
        return f"锋后偏南气流仍在托住主路径，{impact_txt}" if impact_txt else "锋后偏南气流仍在托住主路径"
    if "锋后偏北气流" in mechanism_txt or "冷空气压制" in mechanism_txt:
        return f"冷空气压制尚未解除，{impact_txt}" if impact_txt else "冷空气压制尚未解除，峰值更像落在偏冷侧"
    if "低云稳层" in mechanism_txt:
        return f"低云稳层若不松动，{impact_txt}" if impact_txt else "低云稳层若不松动，午后高点更容易停在偏低侧"
    if "混合层" in mechanism_txt:
        return "混合层能否继续做深，决定后段还能不能补涨"
    if "近地偏南风" in mechanism_txt or "偏南风已开始接管" in mechanism_txt:
        return f"偏南风仍在托住暖侧路径，{impact_txt}" if impact_txt else "偏南风仍在托住暖侧路径"
    if "近地偏西风" in mechanism_txt or "偏西风正在增强" in mechanism_txt:
        return "偏西风增强后，后段温度更看风向切换后的混合效率"
    if condition and impact_txt:
        return f"{condition}仍是主导约束，{impact_txt}"
    if condition:
        return f"{condition}仍是主导约束"
    return ""


def _matched_path_detail_reason_line(
    snapshot: dict[str, Any],
    metar_diag: dict[str, Any],
    *,
    report_mode: str,
) -> tuple[float, str]:
    if report_mode not in {"near_obs", "transition"}:
        return 0.0, ""

    path_context = snapshot_path_context(snapshot)
    significant_text = str(path_context.get("significant_forecast_detail_text") or "").strip()
    significant_score = _safe_float(path_context.get("significant_forecast_detail_score"))
    if significant_text and significant_score is not None and significant_score >= 0.78:
        return float(significant_score), significant_text

    posterior = snapshot_posterior_feature_vector(snapshot)
    ensemble_state = dict(posterior.get("ensemble_path_state") or {})
    match_state = str(ensemble_state.get("observed_alignment_match_state") or "")
    confidence = str(ensemble_state.get("observed_alignment_confidence") or "")
    if match_state not in {"exact", "path"} or confidence not in {"high", "partial"}:
        return 0.0, ""

    dominant_path = str(ensemble_state.get("dominant_path") or "")
    observed_path = str(ensemble_state.get("observed_path") or dominant_path or "")
    if not observed_path:
        return 0.0, ""

    path_summary = {
        "transition_detail": ensemble_state.get("observed_path_detail") or ensemble_state.get("transition_detail"),
        "dominant_path_detail": ensemble_state.get("observed_path_detail") or ensemble_state.get("dominant_path_detail"),
    }
    matched_side = _ensemble_path_side(observed_path, summary=path_summary)

    canonical = snapshot_canonical_raw_state(snapshot)
    forecast = dict(canonical.get("forecast") or {})
    observations = dict(canonical.get("observations") or {})
    context = dict(forecast.get("context") or {})
    h500 = dict(forecast.get("h500") or {})
    h850_review = dict(forecast.get("h850_review") or {})

    wx_state = str(metar_diag.get("latest_precip_state") or observations.get("precip_state") or "").strip().lower()
    wx_trend = str(metar_diag.get("precip_trend") or observations.get("precip_trend") or "").strip().lower()
    latest_wx = str(observations.get("latest_wx") or "").strip().upper()
    cloud_trend = str(observations.get("cloud_trend") or "").strip().lower()
    bottleneck_text = str(context.get("bottleneck_text") or "").strip()
    bottleneck_code = str(context.get("bottleneck_code") or "").strip()
    thermal_role = str(h500.get("thermal_role") or "").strip()
    tmax_bias_label = str(h500.get("tmax_bias_label") or "").strip()
    advection_type = str(h850_review.get("advection_type") or "").strip().lower()
    thermal_advection_state = str(h850_review.get("thermal_advection_state") or "").strip().lower()
    surface_role = str(h850_review.get("surface_role") or "").strip().lower()

    convective_now = (
        wx_state == "convective"
        or any(token in latest_wx for token in ("TS", "SHRA", "VCTS"))
    )
    precip_now = convective_now or wx_state in {"light", "moderate", "heavy", "rain", "showers"} or wx_trend in {"new", "steady", "intensify"}

    if convective_now:
        if matched_side == "warm":
            return 0.91, "当前仍在暖支里，但接下来主要看对流会不会提前冒出来压住上沿"
        return 0.92, "当前更该看对流和降水会不会继续压温，决定区间能否继续贴冷侧"

    if precip_now:
        if matched_side == "warm":
            return 0.88, "当前仍在暖支里，但接下来主要看降水和湿重置会不会拖慢后段升温"
        return 0.89, "当前更该看降水压温会不会继续，决定区间还能不能回到中上段"

    if "低云" in bottleneck_text or ("云" in bottleneck_text and any(token in bottleneck_text for token in ("松动", "破碎", "抬升"))):
        if any(token in cloud_trend for token in {"clearing", "break", "散", "减", "少"}):
            if matched_side == "cold":
                return 0.88, "当前更该看低云会不会继续松动；若松得动，冷侧压制才会开始退"
            return 0.88, "当前更该看低云能否继续松动，决定区间能不能从下沿继续往上推"
        if matched_side == "cold":
            return 0.89, "当前更该看低云稳层何时真正松动；不松，区间更容易继续贴下沿"
        return 0.89, "当前更该看低云何时真正松动，决定暖支还能不能继续兑现"

    if "混合层" in bottleneck_text or "耦合" in bottleneck_text or bottleneck_code in {"low_level_coupling_weak", "mixing_depth_limited"}:
        if matched_side == "warm":
            if advection_type == "warm" and (thermal_advection_state in {"weak", "probable"} or surface_role in {"background", "low_representativeness"}):
                return 0.89, "当前已在验证暖支，850偏暖输送还没完全接地，接下来主要看低层耦合能不能真正接上"
            return 0.89, "当前已在验证暖支，接下来主要看低层耦合和混合层能不能真正接上"
        if matched_side == "cold":
            if advection_type == "cold" and (thermal_advection_state in {"weak", "probable"} or surface_role in {"background", "low_representativeness"}):
                return 0.89, "当前已在验证压温支，850偏冷输送还没完全接地，接下来主要看冷抑制会不会继续落到站点"
            return 0.89, "当前已在验证压温支，混合层若起不来，区间更容易继续贴冷侧"
        return 0.86, "当前更该看混合层能否打穿稳层，这会决定中间路径会不会被打破"

    if thermal_role == "cold_high_suppression" or "压温" in tmax_bias_label:
        return 0.87, "当前更该看高空压温会不会继续落地，决定区间会不会继续贴冷侧"

    if thermal_role in {"warm_high_subsidence", "warm_ridge_support"} or "增温" in tmax_bias_label:
        if advection_type == "warm" and surface_role in {"background", "low_representativeness"}:
            return 0.85, "高空增温背景在托住暖支，但850偏暖输送代表性还弱，接下来主要看低层能不能真正接上"
        return 0.83, "高空增温背景还在托住暖支，接下来主要看这股暖势能不能继续兑现到地面"

    if advection_type == "warm":
        if thermal_advection_state in {"weak", "probable"} or surface_role in {"background", "low_representativeness"}:
            return 0.83, "850偏暖输送还没完全接地，接下来主要看暖支能不能真正落到站点"
        return 0.82, "接下来主要看850暖输送能否继续落地，决定能不能往区间上段推"

    if advection_type == "cold":
        if thermal_advection_state in {"weak", "probable"} or surface_role in {"background", "low_representativeness"}:
            return 0.83, "850偏冷输送还没完全接地，接下来主要看压温支会不会继续落到站点"
        return 0.82, "接下来主要看850冷输送能否继续落地，决定区间会不会继续贴冷侧"

    return 0.0, ""


def _push_reason_candidate(bucket: list[tuple[float, str]], score: float, text: str) -> None:
    cleaned = str(text or "").strip()
    if not cleaned:
        return
    bucket.append((float(score), cleaned.rstrip("。")))


def _is_generic_reason_text(text: str) -> bool:
    cleaned = str(text or "").strip()
    if not cleaned:
        return True
    if any(key in cleaned for key in GENERIC_REASON_KEYS):
        return True
    if any(token in cleaned for token in DECISIVE_REASON_TOKENS):
        return False
    return any(all(fragment in cleaned for fragment in fragments) for fragments in GENERIC_REASON_FRAGMENTS)


def _posterior_reasoning_line(snapshot: dict[str, Any], *, report_mode: str, unit: str) -> tuple[float, str]:
    event_probs = snapshot_weather_posterior_event_probs(snapshot)
    calibration = snapshot_weather_posterior_calibration(snapshot)
    anchor = snapshot_weather_posterior_anchor(snapshot)
    core = snapshot_weather_posterior_core(snapshot)
    progress = dict(core.get("progress") or {})
    temp_phase = snapshot_temp_phase_decision(snapshot)
    timing = dict(temp_phase.get("timing") or {})
    shape = dict(temp_phase.get("shape") or {})

    lock_prob = _safe_float(event_probs.get("lock_by_window_end"))
    new_high_prob = _safe_float(event_probs.get("new_high_next_60m"))
    upper_tail_cap_c = _safe_float(calibration.get("upper_tail_cap_c"))
    observed_anchor_c = _safe_float(progress.get("observed_anchor_c"))
    modeled_headroom_c = _safe_float(progress.get("modeled_headroom_c"))
    regime_shift_c = _safe_float(anchor.get("regime_median_shift_c"))
    display_phase = str(temp_phase.get("display_phase") or "")
    dominant_shape = str(temp_phase.get("dominant_shape") or "")
    second_peak_potential = str(temp_phase.get("second_peak_potential") or "none")
    should_discuss_second_peak = bool(temp_phase.get("should_discuss_second_peak"))
    future_candidate_role = str(shape.get("future_candidate_role") or "")
    before_typical_peak = bool(timing.get("before_typical_peak"))

    plain_single_peak_ramp = bool(
        report_mode in {"near_obs", "transition", "far_synoptic"}
        and dominant_shape == "single_peak"
        and future_candidate_role == "primary_remaining_peak"
        and second_peak_potential in {"none", "weak"}
        and (not should_discuss_second_peak)
        and before_typical_peak
    )

    if report_mode in {"near_obs", "transition"}:
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
                return (
                    0.95,
                    f"综合判断已把上沿压回已观测高点附近（锁定约 {_format_prob_pct(lock_prob)}，再创新高约 {_format_prob_pct(new_high_prob)}）",
                )
            return (
                0.90,
                f"综合判断已明显转向高点锁定（约 {_format_prob_pct(lock_prob)}），再创新高空间只剩尾段机动",
            )
        if (
            new_high_prob is not None
            and new_high_prob >= 0.68
            and (lock_prob is None or lock_prob <= 0.45)
            and (not plain_single_peak_ramp)
        ):
            if modeled_headroom_c is not None and modeled_headroom_c <= 0.55:
                return (
                    0.88,
                    f"综合判断仍保留再创新高路径（约 {_format_prob_pct(new_high_prob)}），但剩余上冲空间已不大",
                )
            return (
                0.90,
                f"综合判断仍把再创新高当主情景（约 {_format_prob_pct(new_high_prob)}），上沿不能过早压掉",
            )
    if report_mode != "near_obs" and regime_shift_c is not None and abs(regime_shift_c) >= 0.45:
        shift_abs = abs(regime_shift_c)
        shift_text = f"{shift_abs:.1f}°{unit}"
        if regime_shift_c > 0.0:
            return 0.76, f"站点特征和天气型修正把中值往暖侧推了一档（约 {shift_text}）"
        return 0.76, f"站点特征和天气型修正把中值往冷侧压了一档（约 {shift_text}）"

    return 0.0, ""


def _phase_structure_reasoning_line(snapshot: dict[str, Any], *, report_mode: str, unit: str) -> tuple[float, str]:
    if report_mode == "far_synoptic":
        return 0.0, ""

    temp_phase = dict(snapshot.get("temp_phase_decision") or {})
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

    if daily_peak_state == "open" and should_discuss_second_peak and second_peak_potential in {"moderate", "high"}:
        if future_candidate_role == "secondary_peak_candidate" and future_gap_vs_obs is not None and future_gap_vs_obs >= 0.25:
            return 0.91, f"后段仍留着可挑战前高的二峰窗口，次峰候选还高出已观测高点约 {future_gap_vs_obs:.1f}°{unit}"
        if future_gap_vs_current is not None and future_gap_vs_current >= 0.45:
            return 0.88, f"后段仍留着补涨段，次峰候选还高出当前约 {future_gap_vs_current:.1f}°{unit}"
        if rebound_mode == "second_peak":
            return 0.84, "当前处在早峰后二峰观察段，还不能按已锁定理解"

    if (
        daily_peak_state == "open"
        and before_typical_peak
        and late_peak_share >= 0.55
        and second_peak_potential in {"none", "weak"}
    ):
        peak_clock = _format_hour_of_day(warm_peak_hour_median)
        if very_late_peak_share >= 0.35 and peak_clock:
            return 0.79, f"这个站常见拖到 {peak_clock} 前后见顶，眼前高点不宜直接当终点"
        if peak_clock:
            return 0.76, f"这个站常见高点偏晚，常拖到 {peak_clock} 前后见顶"
        return 0.74, "这个站常见偏晚见顶，眼前高点不宜直接当终点"

    return 0.0, ""


def _parse_market_tagged_rows(poly_block: str) -> list[dict[str, str]]:
    tagged: list[dict[str, str]] = []
    for raw in str(poly_block or "").splitlines():
        match = re.search(
            r"\*\*(.+?)（(👍最有可能|😇潜在Alpha)）：Bid\s+([^|]+)\|\s+Ask\s+(.+?)\*\*",
            raw.strip(),
        )
        if not match:
            continue
        tagged.append(
            {
                "label": match.group(1).strip(),
                "tag": match.group(2).strip(),
                "bid": match.group(3).strip(),
                "ask": match.group(4).strip(),
            }
        )
    return tagged


def _build_range_rationale_block(
    snapshot: dict[str, Any],
    metar_diag: dict[str, Any],
    poly_block: str,
    *,
    report_mode: str,
    background_line: str = "",
    unit: str,
    fmt_temp,
    display_lo: float,
    display_hi: float,
    core_lo: float,
    core_hi: float,
) -> str:
    if not str(poly_block or "").strip():
        return ""
    if report_mode == "far_synoptic":
        return ""

    lines = ["**判断依据**"]
    posterior = dict(snapshot.get("posterior_feature_vector") or {})
    time_phase = dict(posterior.get("time_phase") or {})
    hours_to_peak = _safe_float(time_phase.get("hours_to_peak"))
    latest_temp = _safe_float(metar_diag.get("latest_temp"))
    trend = _safe_float(metar_diag.get("temp_trend_1step_c"))
    temp_bias = _safe_float(metar_diag.get("temp_bias_c"))

    synoptic_summary = dict(snapshot.get("synoptic_summary") or {})
    summary = dict(synoptic_summary.get("summary") or {})
    syn_lines = [str(item) for item in (synoptic_summary.get("lines") or []) if str(item).strip()]
    targeted_signal_basis = ""
    if report_mode != "near_obs" and hours_to_peak is not None and hours_to_peak > 3.0:
        targeted_signal_basis = _strip_bullet_prefix(_build_far_signal_basis_line(snapshot))
    mechanism_basis = (
        targeted_signal_basis
        or _short_mechanism_text(summary.get("pathway"))
        or _pick_background_basis(snapshot, include_lines=False)
        or _short_mechanism_text(dict(snapshot.get("boundary_layer_regime") or {}).get("headline"))
        or _pick_far_basis_text(snapshot, syn_lines)
    )
    mechanism = _background_mechanism_text(mechanism_basis, "", metar_diag) or _short_mechanism_text(mechanism_basis)
    coastal_mechanism = _coastal_flow_mechanism(snapshot, metar_diag)
    if coastal_mechanism and (not mechanism or mechanism in {"后面主要看混合层还能不能继续做深", "锋面附近风场仍在调整"}):
        mechanism = coastal_mechanism
    impact = _summarize_impact_text(summary.get("impact"))
    background_txt = _normalize_synoptic_text(str(background_line or "").replace("🧭 背景：", ""))
    if coastal_mechanism and background_txt and coastal_mechanism in background_txt:
        mechanism = coastal_mechanism

    live_path_line = _build_live_path_reference_line(snapshot, metar_diag, report_mode=report_mode, unit=unit)

    obs_line = _obs_reasoning_line(latest_temp, trend, unit)
    obs_signal_strong = bool(
        (trend is not None and abs(trend) >= 0.35)
        or (temp_bias is not None and abs(temp_bias) >= 0.8)
        or bool(metar_diag.get("metar_speci_active"))
        or bool(metar_diag.get("metar_speci_likely"))
    )
    has_targeted_basis = bool(mechanism or impact or live_path_line or targeted_signal_basis)
    should_use_obs_line = bool(
        obs_line
        and (
            (hours_to_peak is not None and hours_to_peak <= 1.75)
            or (
                obs_signal_strong
                and hours_to_peak is not None
                and hours_to_peak <= 2.0
            )
            or (
                report_mode == "near_obs"
                and not has_targeted_basis
                and (hours_to_peak is None or hours_to_peak <= 2.75)
            )
        )
    )

    concise_impact = _impact_reasoning_text(impact)

    mechanism_repeated = bool(mechanism and background_txt and mechanism in background_txt)
    impact_repeated = bool(concise_impact and background_txt and concise_impact in background_txt)
    reason_candidates: list[tuple[float, str]] = []

    matched_detail_score, matched_detail_line = _matched_path_detail_reason_line(
        snapshot,
        metar_diag,
        report_mode=report_mode,
    )
    posterior_score, posterior_line = _posterior_reasoning_line(
        snapshot,
        report_mode=report_mode,
        unit=unit,
    )
    suppress_positive_posterior = bool(
        matched_detail_line
        and matched_detail_score >= 0.82
        and any(token in posterior_line for token in ("再创新高路径", "再创新高当主情景"))
    )
    if posterior_line and not suppress_positive_posterior:
        _push_reason_candidate(reason_candidates, posterior_score, posterior_line)

    phase_structure_score, phase_structure_line = _phase_structure_reasoning_line(
        snapshot,
        report_mode=report_mode,
        unit=unit,
    )
    if phase_structure_line:
        _push_reason_candidate(reason_candidates, phase_structure_score, phase_structure_line)

    if live_path_line:
        _push_reason_candidate(reason_candidates, 0.97, live_path_line)

    if matched_detail_line:
        _push_reason_candidate(reason_candidates, matched_detail_score, matched_detail_line)

    if mechanism and not mechanism_repeated:
        mechanism_line = _mechanism_basis_line(mechanism, impact)
        if mechanism_line:
            _push_reason_candidate(reason_candidates, 0.78, mechanism_line)
    elif concise_impact and not impact_repeated:
        _push_reason_candidate(reason_candidates, 0.62, concise_impact)
    elif impact:
        _push_reason_candidate(reason_candidates, 0.52, impact)

    if should_use_obs_line and not live_path_line:
        _push_reason_candidate(reason_candidates, 0.72, obs_line)

    selected_lines: list[str] = []
    seen: set[str] = set()
    for _, line in sorted(reason_candidates, key=lambda item: item[0], reverse=True):
        if line in seen:
            continue
        if _is_generic_reason_text(line):
            continue
        seen.add(line)
        selected_lines.append(line)
        if len(selected_lines) >= 2:
            break

    if not selected_lines:
        return ""
    for line in selected_lines:
        lines.append(f"• {line}。")
    return "\n".join(lines)


def _compact_focus_block(lines: list[str], *, report_mode: str) -> str:
    if not lines:
        return ""
    header = str(lines[0] or "").strip()
    detail = ""
    for raw in lines[1:]:
        cleaned = str(raw or "").strip()
        if cleaned:
            detail = cleaned.lstrip("• ").strip()
            break
    if not detail:
        return ""
    if report_mode == "near_obs" and (
        "当前更该看环流" in detail or "当前先看环流" in detail or "更细的实况校正留到临近目标窗再做" in detail
    ):
        detail = "优先盯下一报温度斜率、风向节奏和云量是否继续支持当前路径"
    if any(key in detail for key in GENERIC_FOCUS_KEYS):
        return ""
    detail = detail.rstrip("。；，")
    match = re.search(r"关注变量[（(]([^）)]+)[）)]", header)
    if match:
        return f"⚠️ 关注（{match.group(1).strip()}）：{detail}。"
    return f"⚠️ 关注：{detail}。"


def _snapshot_for_report_mode(snapshot: dict[str, Any], report_mode: str) -> dict[str, Any]:
    if report_mode != "far_synoptic":
        return snapshot

    temp_phase = dict(snapshot.get("temp_phase_decision") or {})
    peak_data = dict(snapshot.get("peak_data") or {})
    peak_summary = dict(peak_data.get("summary") or {})
    if temp_phase.get("display_phase") == "far" and peak_summary.get("phase_now") == "far":
        return snapshot

    adjusted = dict(snapshot)
    temp_phase["display_phase"] = "far"
    peak_summary["phase_now"] = "far"
    peak_data["summary"] = peak_summary
    adjusted["temp_phase_decision"] = temp_phase
    adjusted["peak_data"] = peak_data
    return adjusted


def _build_metar_block(
    metar_diag: dict[str, Any],
    metar_text: str,
    unit: str,
    fmt_temp,
) -> str:
    metar_prefix: list[str] = []
    try:
        observed_max_text = _observed_max_text(metar_diag, unit)
        if observed_max_text:
            metar_prefix.append(f"• {observed_max_text}")
    except Exception:
        pass
    return "📡 **最新实况分析（METAR）**\n" + ("\n".join(metar_prefix + [metar_text]) if metar_prefix else metar_text)


def _build_far_metar_block(
    metar_diag: dict[str, Any],
    unit: str,
    fmt_temp,
) -> str:
    lines = ["📡 **当前实况参考（降级）**"]
    latest_local = str(metar_diag.get("latest_report_local") or "").strip()
    latest_time = ""
    if latest_local:
        try:
            latest_time = datetime.fromisoformat(latest_local).strftime("%H:%M Local")
        except Exception:
            latest_time = ""
    if latest_time:
        lines.append(f"• 最新报：{latest_time}")

    compact_bits: list[str] = []
    try:
        latest_temp = metar_diag.get("latest_temp")
        if latest_temp is not None:
            compact_bits.append(f"气温 {fmt_temp(float(latest_temp))}")
    except Exception:
        pass

    try:
        latest_wdir = metar_diag.get("latest_wdir")
        latest_wspd = metar_diag.get("latest_wspd")
        if latest_wdir not in (None, "") and latest_wspd not in (None, ""):
            compact_bits.append(f"风 {float(latest_wdir):.0f}° {float(latest_wspd):.0f}kt")
        elif latest_wspd not in (None, ""):
            compact_bits.append(f"风速 {float(latest_wspd):.0f}kt")
    except Exception:
        pass

    cloud_layers = str(metar_diag.get("latest_cloud_layers") or "").strip()
    if cloud_layers:
        compact_bits.append(f"云层 {cloud_layers}")

    if compact_bits:
        lines.append(f"• {' | '.join(compact_bits)}")

    lines.append("• 目标峰值窗仍远，当前实况只作背景参考；主判断以预报环流和后续演变为主。")
    return "\n".join(lines)


def choose_section_text(
    primary_window: dict[str, Any],
    metar_text: str,
    metar_diag: dict[str, Any],
    polymarket_event_url: str,
    forecast_decision: dict[str, Any] | None = None,
    compact_synoptic: bool = False,
    temp_unit: str = "C",
    synoptic_window: dict[str, Any] | None = None,
    polymarket_prefetched_event: tuple[bool, list[dict[str, Any]]] | None = None,
    temp_shape_analysis: dict[str, Any] | None = None,
    analysis_snapshot: dict[str, Any] | None = None,
) -> str:
    """Render-only section builder."""

    unit = "F" if str(temp_unit).upper() == "F" else "C"

    def _to_unit(c: float) -> float:
        return (c * 9.0 / 5.0 + 32.0) if unit == "F" else c

    def _fmt_temp(v_c: float) -> str:
        v = _to_unit(float(v_c))
        return f"{v:.1f}°{unit}"

    snapshot = analysis_snapshot if isinstance(analysis_snapshot, dict) else build_analysis_snapshot(
        primary_window=primary_window,
        metar_diag=metar_diag,
        forecast_decision=forecast_decision,
        temp_unit=unit,
        synoptic_window=synoptic_window,
        temp_shape_analysis=temp_shape_analysis,
    )

    synoptic_summary = dict(snapshot.get("synoptic_summary") or {})
    syn_lines = [str(item) for item in (synoptic_summary.get("lines") or []) if str(item).strip()]
    if not syn_lines:
        syn_lines = ["🧭 **环流形势对最高温影响**", "- 结构化环流摘要缺失，需回退到原始诊断。"]
    boundary_layer_regime = dict(snapshot.get("boundary_layer_regime") or {})
    regime_headline = str(boundary_layer_regime.get("headline") or "").strip()
    if regime_headline:
        mechanism_line = f"- **主导机制**：{regime_headline}"
        replaced = False
        for idx, line in enumerate(syn_lines):
            if "主导机制" in line:
                syn_lines[idx] = mechanism_line
                replaced = True
                break
        if not replaced:
            syn_lines = syn_lines[:1] + [mechanism_line] + syn_lines[1:]

    syn_lines = syn_lines[:5]

    temp_phase_decision = dict(snapshot.get("temp_phase_decision") or {})
    peak_data = dict(snapshot.get("peak_data") or {})
    weather_posterior = dict(snapshot.get("weather_posterior") or {})
    peak_summary = dict(peak_data.get("summary") or {})
    peak_range_block = [str(item) for item in (peak_data.get("block") or []) if str(item).strip()]
    peak_ranges = dict(peak_summary.get("ranges") or {})
    peak_display_range = dict(peak_ranges.get("display") or {})
    peak_core_range = dict(peak_ranges.get("core") or {})
    phase_now = str(peak_summary.get("phase_now") or "unknown")
    disp_lo = float(peak_display_range.get("lo"))
    disp_hi = float(peak_display_range.get("hi"))
    core_lo = float(peak_core_range.get("lo"))
    core_hi = float(peak_core_range.get("hi"))
    report_mode = _classify_report_mode(snapshot, primary_window, metar_diag, phase_now)
    focus_snapshot = _snapshot_for_report_mode(snapshot, report_mode)
    use_far_synoptic = report_mode == "far_synoptic"

    metar_block = _build_far_obs_reference(
        metar_diag=metar_diag,
        unit=unit,
        fallback_text=metar_text,
    ) if use_far_synoptic else _build_metar_block(
        metar_diag=metar_diag,
        metar_text=metar_text,
        unit=unit,
        fmt_temp=_fmt_temp,
    )

    report_focus = build_report_focus_bundle(
        primary_window=primary_window,
        metar_diag=metar_diag,
        analysis_snapshot=focus_snapshot,
    )
    vars_block = [str(item) for item in (report_focus.get("vars_block") or []) if str(item).strip()]
    if not vars_block:
        vars_block = [f"⚠️ **关注变量**（{PHASE_LABELS.get(phase_now, PHASE_LABELS['unknown'])}）", DEFAULT_TRACK_LINE]
    focus_block = _compact_focus_block(vars_block, report_mode=report_mode)

    metar_analysis_lines = [str(item) for item in (report_focus.get("metar_analysis_lines") or []) if str(item).strip()]
    if report_mode in {"near_obs", "transition"}:
        metar_block = _build_metar_block(
            metar_diag=metar_diag,
            metar_text=metar_text,
            unit=unit,
            fmt_temp=_fmt_temp,
        )
        if metar_analysis_lines:
            extra_lines: list[str] = []
            for raw in metar_analysis_lines:
                cleaned = str(raw or "").strip()
                if cleaned and cleaned not in metar_block:
                    extra_lines.append(cleaned)
            if extra_lines:
                metar_block = metar_block + "\n" + "\n".join(extra_lines)

    label_policy = dict(report_focus.get("market_label_policy") or {})
    range_hint = {
        # Keep the market ladder aligned with the same peak-range block shown above.
        # Using a broader posterior-only hint here can produce contradictory output
        # such as "likely capped" in the peak block while tagging a hotter tail bin
        # as "most likely" in the Polymarket block.
        "display_lo": float(disp_lo),
        "display_hi": float(disp_hi),
        "core_lo": float(core_lo),
        "core_hi": float(core_hi),
    }

    background_line = "" if report_mode == "far_synoptic" else _build_background_synoptic_line(snapshot, metar_diag)

    poly_block = ""
    range_rationale_block = ""
    market_weather_anchor = {
        "latest_temp_c": metar_diag.get("latest_temp"),
        "observed_max_temp_c": metar_diag.get("observed_max_temp_c"),
    }
    if str(polymarket_event_url or "").strip():
        try:
            poly_block = _build_polymarket_section(
                polymarket_event_url,
                primary_window,
                weather_anchor=market_weather_anchor,
                weather_posterior=snapshot.get("weather_posterior") or {},
                range_hint=range_hint,
                allow_best_label=bool(label_policy.get("allow_best_label", True)),
                allow_alpha_label=bool(label_policy.get("allow_alpha_label", True)),
                label_policy=label_policy,
                prefetched_event=polymarket_prefetched_event,
            )
            if str(poly_block).startswith("Polymarket："):
                poly_block = ""
            elif poly_block:
                range_rationale_block = _build_range_rationale_block(
                    snapshot,
                    metar_diag,
                    poly_block,
                    report_mode=report_mode,
                    background_line=background_line,
                    unit=unit,
                    fmt_temp=_fmt_temp,
                    display_lo=float(disp_lo),
                    display_hi=float(disp_hi),
                    core_lo=float(core_lo),
                    core_hi=float(core_hi),
                )
        except Exception:
            poly_block = ""
            range_rationale_block = ""

    compact_after: set[int] = set()
    if report_mode == "far_synoptic":
        synoptic_block = _build_far_outlook_block(
            snapshot,
            syn_lines,
            metar_diag,
            primary_window,
            unit=unit,
            fmt_temp=_fmt_temp,
        )
        parts = [
            synoptic_block,
        ]
        parts.append("\n".join(peak_range_block))
        if focus_block:
            parts.append(focus_block)
            compact_after.add(len(parts) - 2)
        if metar_block:
            parts.append(metar_block)
    else:
        parts = []
        if background_line:
            parts.append(background_line)
        parts.extend([
            metar_block,
            "\n".join(peak_range_block),
        ])
        if focus_block:
            parts.append(focus_block)
            compact_after.add(len(parts) - 2)
    if range_rationale_block:
        parts.append(range_rationale_block)
        compact_after.add(len(parts) - 2)
    if poly_block:
        parts.append(poly_block)
    return _join_report_parts(parts, compact_after=compact_after)
