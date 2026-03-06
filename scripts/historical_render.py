#!/usr/bin/env python3
"""Rendering helpers for archive-backed historical context."""

from __future__ import annotations

from typing import Any

from historical_context_provider import _fmt_c, _fmt_peak_value, _safe_float, regime_to_cn


def render_historical_context_block(context: dict[str, Any]) -> str:
    if not context.get("available"):
        return ""
    lines = ["🧠 **历史知识库参考**"]
    for line in context.get("summary_lines") or []:
        lines.append(f"- {line}")
    for line in context.get("analog_summary_lines") or []:
        lines.append(f"- {line}")
    for line in context.get("branch_lines") or []:
        lines.append(f"- {line}")
    analogs = context.get("analogs") or []
    if analogs:
        for index, row in enumerate(analogs, start=1):
            climate_window = str(row.get("_row_climate_window") or "")
            calendar_gap = str(row.get("_calendar_gap_days") or "")
            extra = []
            if climate_window:
                extra.append(climate_window)
            if calendar_gap:
                extra.append(f"距目标日历位差 {calendar_gap} 天")
            lines.append(
                f"- 相似历史日{index}："
                f"{row.get('local_date')} | {regime_to_cn(row.get('primary_regime'))} | "
                f"Tmax {_fmt_c(_safe_float(row.get('tmax_c')))} | "
                f"峰值 {_fmt_peak_value(row)} | "
                f"风向 {row.get('dominant_wind_sector') or 'n/a'} | "
                f"{' | '.join(extra) + ' | ' if extra else ''}"
                f"score {row.get('_similarity_score', 'n/a')}"
            )
            reasons = str(row.get("_similarity_reasons") or "").strip()
            if reasons:
                lines.append(f"  原因：{reasons}")
            impact = str(row.get("_impact_summary") or "").strip()
            if impact:
                lines.append(f"  表现：{impact}")
    return "\n".join(lines)
