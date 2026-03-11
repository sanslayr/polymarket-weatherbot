#!/usr/bin/env python3
"""Section rendering service for /look report."""

from __future__ import annotations

import re
from datetime import datetime
from typing import Any

from analysis_snapshot_service import build_analysis_snapshot
from polymarket_render_service import _build_polymarket_section
from report_focus_service import build_report_focus_bundle
from report_synoptic_service import (
    _background_compact_clause,
    _background_mechanism_text,
    _build_background_synoptic_line,
    _build_far_synoptic_block,
    _coastal_flow_mechanism,
    _natural_flow_chain_line,
    _normalize_synoptic_text,
    _pick_background_basis,
    _pick_far_basis_text,
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

GENERIC_FOCUS_KEYS = (
    "优先盯下一报温度斜率、风向节奏和云量是否继续支持当前路径",
    "优先看温度斜率和低层风向是否继续配合",
    "临窗前继续跟踪温度斜率与风向节奏，必要时再改判",
    "当前更该看环流、云量和低层风场配置会不会延续",
    "当前先看环流、云量和低层风场配置是否继续维持",
    "优先看云量、近地风场和温度斜率能否继续配合",
    "若三者同步走强，再考虑上修后段上沿",
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


def _format_local_clock(value: Any) -> str:
    dt = _parse_iso_dt(value)
    if not dt:
        return ""
    try:
        return dt.strftime("%H:%M Local")
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


def _build_obs_focus_metar_block(
    metar_diag: dict[str, Any],
    *,
    unit: str,
    fmt_temp,
    fallback_text: str,
    metar_analysis_lines: list[str] | None = None,
) -> str:
    latest_temp = _safe_float(metar_diag.get("latest_temp"))
    observed_max = _safe_float(metar_diag.get("observed_max_temp_c"))
    latest_time = _format_local_clock(metar_diag.get("latest_report_local"))
    max_time = _format_local_clock(metar_diag.get("observed_max_time_local"))

    if latest_temp is None and observed_max is None:
        return "📡 实况：" + str(fallback_text or "METAR 实况摘要缺失。").strip()

    parts: list[str] = ["📡 实况："]
    if latest_time:
        parts.append(f"{latest_time} ")
    if latest_temp is not None:
        parts.append(fmt_temp(latest_temp))
    if observed_max is not None:
        if max_time and max_time != latest_time:
            parts.append(f"，今日已到 {fmt_temp(observed_max)}（{max_time}）")
        else:
            parts.append(f"，今日已到 {fmt_temp(observed_max)}")
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
    fmt_temp,
    fallback_text: str,
) -> str:
    latest_temp = _safe_float(metar_diag.get("latest_temp"))
    latest_time = _format_local_clock(metar_diag.get("latest_report_local"))
    detail_bits: list[str] = []
    if latest_time:
        detail_bits.append(latest_time)
    if latest_temp is not None:
        detail_bits.append(fmt_temp(latest_temp))
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
        return f"📡 当前实况：{' | '.join(detail_bits)}（仅作背景参考）。"
    fallback = str(fallback_text or "").strip()
    return f"📡 当前实况：{fallback}" if fallback else "📡 当前实况：METAR 实况摘要缺失。"


def _range_target_text(
    *,
    unit: str,
    display_lo: float,
    display_hi: float,
    core_lo: float,
    core_hi: float,
) -> str:
    def _fmt_range(lo: float, hi: float) -> str:
        if unit == "F":
            lo_u = lo * 9.0 / 5.0 + 32.0
            hi_u = hi * 9.0 / 5.0 + 32.0
            return f"{lo_u:.1f}~{hi_u:.1f}°F"
        return f"{lo:.1f}~{hi:.1f}°C"

    if core_lo > display_lo and core_hi < display_hi:
        return f"区间先放在 {_fmt_range(core_lo, core_hi)}，两边各留一点机动"
    if core_lo > display_lo:
        return f"区间先放在 {_fmt_range(core_lo, core_hi)}，下沿留给偏冷回摆"
    if core_hi < display_hi:
        return f"区间先放在 {_fmt_range(core_lo, core_hi)}，上沿留给临窗再抬"
    return f"区间先放在 {_fmt_range(core_lo, core_hi)}"


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
        return "上沿仍宜按受压情景处理"
    if any(key in impact_txt for key in ("偏上沿", "上修空间")):
        return "上沿仍保留小幅上修空间"
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


def _mechanism_reasoning_line(mechanism: str, impact: str, range_target: str) -> str:
    mechanism_txt = str(mechanism or "").strip()
    if not mechanism_txt:
        return ""
    impact_txt = _impact_reasoning_text(impact)
    condition = _mechanism_condition_text(mechanism_txt)
    if "锋后偏南气流" in mechanism_txt:
        if impact_txt:
            return f"在锋后偏南气流持续维持的前提下，{range_target}，{impact_txt}"
        return f"在锋后偏南气流持续维持的前提下，{range_target}"
    if "锋后偏北气流" in mechanism_txt or "冷空气压制" in mechanism_txt:
        return f"若冷空气压制尚未解除，{range_target}"
    if "低云稳层" in mechanism_txt:
        return f"若低云稳层限制持续，{range_target}"
    if "混合层" in mechanism_txt:
        return f"若混合层加深幅度受限，冲高空间将受到约束，{range_target}"
    if "近地偏南风" in mechanism_txt or "偏南风已开始接管" in mechanism_txt:
        if impact_txt:
            return f"在偏南风持续维持的前提下，{range_target}，{impact_txt}"
        return f"在偏南风持续维持的前提下，{range_target}"
    if "近地偏西风" in mechanism_txt or "偏西风正在增强" in mechanism_txt:
        if impact_txt:
            return f"在偏西风继续增强的前提下，{range_target}，{impact_txt}"
        return f"在偏西风继续增强的前提下，{range_target}"
    if condition and impact_txt:
        return f"若{condition}继续维持，{range_target}，{impact_txt}"
    if condition:
        return f"若{condition}继续维持，{range_target}"
    return ""


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

    lines = ["**判断依据**"]
    latest_temp = _safe_float(metar_diag.get("latest_temp"))
    trend = _safe_float(metar_diag.get("temp_trend_1step_c"))
    obs_line = _obs_reasoning_line(latest_temp, trend, unit)
    if obs_line:
        lines.append(f"• {obs_line}。")

    synoptic_summary = dict(snapshot.get("synoptic_summary") or {})
    summary = dict(synoptic_summary.get("summary") or {})
    syn_lines = [str(item) for item in (synoptic_summary.get("lines") or []) if str(item).strip()]
    mechanism_basis = (
        _short_mechanism_text(summary.get("pathway"))
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

    range_target = _range_target_text(
        unit=unit,
        display_lo=display_lo,
        display_hi=display_hi,
        core_lo=core_lo,
        core_hi=core_hi,
    )

    concise_impact = _impact_reasoning_text(impact)

    mechanism_repeated = bool(mechanism and background_txt and mechanism in background_txt)
    impact_repeated = bool(concise_impact and background_txt and concise_impact in background_txt)

    if mechanism and not mechanism_repeated:
        detail_line = _mechanism_reasoning_line(mechanism, impact, range_target)
        if detail_line:
            lines.append(f"• {detail_line}。")
        else:
            lines.append(f"• {range_target}。")
    elif concise_impact and not impact_repeated:
        lines.append(f"• 眼下更像是{concise_impact}，所以{range_target}。")
    elif impact:
        lines.append(f"• 从当前链路看，{impact}，所以{range_target}。")
    else:
        lines.append(f"• {range_target}。")

    if len(lines) > 3:
        lines = lines[:3]
    if len(lines) == 1:
        return ""
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


def _build_metar_block(
    metar_diag: dict[str, Any],
    metar_text: str,
    unit: str,
    fmt_temp,
) -> str:
    metar_prefix: list[str] = []
    try:
        if metar_diag and metar_diag.get("observed_max_temp_c") is not None:
            mx = float(metar_diag.get("observed_max_temp_c"))
            if unit == "C":
                mx_txt = f"{int(round(mx))}°C" if abs(mx - round(mx)) < 0.05 else f"{mx:.1f}°C"
            else:
                mx_txt = fmt_temp(mx)
            tmax_local = str(metar_diag.get("observed_max_time_local") or "")
            tmax_txt = ""
            if tmax_local:
                try:
                    tmax_txt = datetime.fromisoformat(tmax_local).strftime("%H:%M Local")
                except Exception:
                    tmax_txt = ""
            if tmax_txt:
                metar_prefix.append(f"• 今日已观测最高温：{mx_txt}（{tmax_txt}）")
            else:
                metar_prefix.append(f"• 今日已观测最高温：{mx_txt}")
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
    far_from_window = phase_now == "far"
    report_mode = _classify_report_mode(snapshot, primary_window, metar_diag, phase_now)

    metar_block = _build_far_metar_block(
        metar_diag=metar_diag,
        unit=unit,
        fmt_temp=_fmt_temp,
    ) if far_from_window else _build_metar_block(
        metar_diag=metar_diag,
        metar_text=metar_text,
        unit=unit,
        fmt_temp=_fmt_temp,
    )

    report_focus = build_report_focus_bundle(
        primary_window=primary_window,
        metar_diag=metar_diag,
        analysis_snapshot=snapshot,
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
    elif far_from_window:
        metar_block = _build_far_obs_reference(
            metar_diag=metar_diag,
            unit=unit,
            fmt_temp=_fmt_temp,
            fallback_text=metar_text,
        )

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
        synoptic_block = _build_far_synoptic_block(snapshot, syn_lines, metar_diag)
        parts = [
            synoptic_block,
            "\n".join(peak_range_block),
        ]
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
