#!/usr/bin/env python3
"""Section rendering service for /look report."""

from __future__ import annotations

from datetime import datetime
from typing import Any

from analysis_snapshot_service import build_analysis_snapshot
from polymarket_render_service import _build_polymarket_section
from report_focus_service import build_report_focus_bundle

PHASE_LABELS = {
    "far": "远离窗口",
    "near_window": "接近窗口",
    "in_window": "窗口内",
    "post": "窗口后",
    "early_peak_watch": "早峰后观察",
    "unknown": "窗口状态未知",
}
DEFAULT_TRACK_LINE = "• 临窗前继续跟踪温度斜率与风向节奏，必要时再改判。"


def _clean_synoptic_line(line: str) -> str:
    txt = str(line or "").strip()
    if not txt:
        return ""
    if txt.startswith("🧭"):
        txt = txt[1:].strip()
    if txt.startswith("-"):
        txt = txt[1:].strip()
    txt = txt.replace("**", "").strip()
    if txt.startswith("环流形势对最高温影响"):
        txt = txt.removeprefix("环流形势对最高温影响").strip("：: ")
    if txt.startswith("主导机制："):
        txt = txt.removeprefix("主导机制：").strip()
    elif txt.startswith("主导机制:"):
        txt = txt.removeprefix("主导机制:").strip()
    return txt.strip()


def _compact_synoptic_block(lines: list[str]) -> str:
    compact_parts: list[str] = []
    seen: set[str] = set()
    for raw in lines:
        cleaned = _clean_synoptic_line(raw).rstrip("。；，")
        if not cleaned:
            continue
        if cleaned in seen:
            continue
        seen.add(cleaned)
        compact_parts.append(cleaned)
    if not compact_parts:
        return "🧭 环流形势：结构化环流摘要缺失，需回退到原始诊断。"
    return f"🧭 环流形势：{'；'.join(compact_parts)}。"


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

    metar_analysis_lines = [str(item) for item in (report_focus.get("metar_analysis_lines") or []) if str(item).strip()]
    if metar_analysis_lines and not far_from_window:
        metar_block = metar_block + "\n\n**实况分析**\n" + "\n".join(metar_analysis_lines)

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

    poly_block = ""
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
        except Exception:
            poly_block = ""

    synoptic_block = _compact_synoptic_block(syn_lines) if compact_synoptic else "\n".join(syn_lines)

    if far_from_window:
        parts = [
            synoptic_block,
            "\n".join(peak_range_block),
            "\n".join(vars_block),
            metar_block,
        ]
    else:
        parts = [
            synoptic_block,
            metar_block,
            "\n".join(peak_range_block),
            "\n".join(vars_block),
        ]
    if poly_block:
        parts.append(poly_block)
    return "\n\n".join(parts)
