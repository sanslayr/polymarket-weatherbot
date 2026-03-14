#!/usr/bin/env python3
"""Historical reference helpers for peak-range analysis."""

from __future__ import annotations

from typing import Any

from historical_context_provider import _analog_branch_label
from historical_payload import get_historical_payload, get_weighted_reference
from historical_strategy import blend_historical_range


def _date_span_text(dates: list[str]) -> str:
    cleaned = [str(item).strip() for item in dates if str(item).strip()]
    if not cleaned:
        return ""

    def _display_date(value: str) -> str:
        raw = str(value or "").strip()
        if len(raw) == 10 and raw[4] == "-" and raw[7] == "-":
            return raw.replace("-", "/")
        return raw

    try:
        ordered = sorted(cleaned)
    except Exception:
        ordered = cleaned
    if len(ordered) == 1:
        return _display_date(ordered[0])
    return f"{_display_date(ordered[0])} & {_display_date(ordered[-1])}"


def _reference_degree_text(text: str) -> str:
    s = str(text or "")
    if "强" in s:
        return "高"
    if "中" in s:
        return "中"
    if "弱" in s:
        return "弱"
    return s or "中"


def _branch_reference_text(
    label: str,
    rows: list[dict[str, Any]],
    degree_text: str,
    prefix: str | None = None,
) -> str:
    dates = [str(row.get("local_date") or "").strip() for row in rows if str(row.get("local_date") or "").strip()]
    date_text = _date_span_text(dates)
    parts: list[str] = []
    if prefix:
        parts.append(prefix)
    if date_text:
        parts.append(date_text)
    parts.append("日期邻近样本")
    parts.append(f"{label}背景")
    parts.append(f"参考度{degree_text}")
    return "，".join(parts)


def _shift_direction_text(shift_c: Any, fmt_delta_unit, unit: str) -> str:
    try:
        v = float(shift_c)
    except Exception:
        return "温度修正中性"
    if abs(v) < 0.05:
        return "温度修正中性"
    direction = "上修" if v > 0 else "下修"
    return f"温度{direction}{fmt_delta_unit(v, unit)}"


def apply_historical_reference(
    *,
    metar_diag: dict[str, Any],
    phase_now: str,
    compact_settled_mode: bool,
    core_lo: float,
    core_hi: float,
    disp_lo: float,
    disp_hi: float,
) -> tuple[float, float, float, float, dict[str, Any] | None]:
    _, _, _, _, historical_blend = blend_historical_range(
        metar_diag=metar_diag,
        phase_now=phase_now,
        compact_settled_mode=compact_settled_mode,
        core_lo=core_lo,
        core_hi=core_hi,
        disp_lo=disp_lo,
        disp_hi=disp_hi,
    )
    if isinstance(historical_blend, dict):
        historical_blend = {
            **historical_blend,
            "applied": False,
            "advisory_only": True,
        }
    # Final /look ranges now come from posterior quantiles. Historical analogs
    # remain available as advisory context only, not as direct range patches.
    return core_lo, core_hi, disp_lo, disp_hi, historical_blend


def build_peak_historical_reference(
    *,
    metar_diag: dict[str, Any],
    historical_blend: dict[str, Any] | None,
    unit: str,
    fmt_delta_unit,
) -> dict[str, Any] | None:
    if not historical_blend or (not historical_blend.get("applied") and not historical_blend.get("display")):
        return None

    strength_cn = {"weak": "弱参考", "medium": "中参考", "strong": "强参考"}.get(
        str(historical_blend.get("strength") or ""),
        str(historical_blend.get("strength") or ""),
    )
    weighted = get_weighted_reference(metar_diag)
    if isinstance(weighted, dict):
        selected_dates = [str(item) for item in (weighted.get("selected_dates") or []) if str(item).strip()]
    else:
        selected_dates = []

    historical_payload = get_historical_payload(metar_diag)
    historical_context = (historical_payload or {}).get("context") if isinstance(historical_payload, dict) else None
    if not isinstance(historical_context, dict):
        return None

    summary_lines = [str(item) for item in (historical_context.get("summary_lines") or []) if str(item).strip()]
    current_match = _pick_prefixed_line(summary_lines, "当前实况匹配：")
    analogs = historical_context.get("analogs") if isinstance(historical_context.get("analogs"), list) else []
    branch_assessment = (historical_payload or {}).get("branch_assessment") if isinstance(historical_payload, dict) else None
    branch_details = list((branch_assessment or {}).get("branch_details") or []) if isinstance(branch_assessment, dict) else []
    degree_text = _reference_degree_text(strength_cn)
    title = "- 历史参考："
    if bool(historical_blend.get("advisory_only")):
        title = "- 历史参考（旁证，不参与本次区间定标）："

    lines: list[str] = []
    if isinstance(branch_assessment, dict) and str(branch_assessment.get("branch_mode") or "") in {"split", "competitive"} and len(branch_details) >= 2:
        grouped: dict[str, list[dict[str, Any]]] = {}
        for row in analogs:
            if not isinstance(row, dict):
                continue
            label = _analog_branch_label(row)
            grouped.setdefault(label, []).append(row)
        for idx, item in enumerate(branch_details[:2], start=1):
            label = str(item.get("label") or "").strip()
            if not label:
                continue
            rows = sorted(
                grouped.get(label, []),
                key=lambda row: float(str(row.get("_similarity_score") or 0.0)),
                reverse=True,
            )
            fit_degree = _reference_degree_text(str(item.get("fit_label") or degree_text))
            lines.append(f"  - {_branch_reference_text(label, rows, fit_degree, prefix='主' if idx == 1 else '次')}")
    else:
        match_txt = current_match or "过渡型"
        date_text = _date_span_text(selected_dates)
        single_text = _branch_reference_text(match_txt, [{"local_date": date_text}] if date_text else [], degree_text)
        lines.append(f"  - {single_text}")

    shift_text = None
    if historical_blend.get("applied"):
        shift_text = f"  - {_shift_direction_text(historical_blend.get('shift_c'), fmt_delta_unit, unit)}"

    return {
        "title": title,
        "lines": lines,
        "shift_text": shift_text,
        "blend": historical_blend,
    }


def _pick_prefixed_line(lines: list[str], prefix: str) -> str | None:
    for line in lines:
        text = str(line or "").strip()
        if text.startswith(prefix):
            return text[len(prefix):].strip()
    return None
