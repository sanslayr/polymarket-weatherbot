#!/usr/bin/env python3
"""Rendering helpers for archive-backed historical context."""

from __future__ import annotations

from typing import Any

from historical_context_provider import _fmt_c, _fmt_peak_value, _safe_float


def _pick_line(lines: list[str], prefix: str) -> str | None:
    for line in lines:
        if str(line).startswith(prefix):
            return str(line)
    return None


def _summarize_reference_dates(analogs: list[dict[str, Any]], limit: int = 2) -> str | None:
    dates: list[str] = []
    for row in analogs:
        local_date = str(row.get("local_date") or "").strip()
        if not local_date or local_date in dates:
            continue
        dates.append(local_date)
        if len(dates) >= limit:
            break
    if not dates:
        return None
    return " / ".join(dates)


def _render_weighted_reference(context: dict[str, Any]) -> str | None:
    weighted = context.get("weighted_reference") or {}
    center = _safe_float(weighted.get("recommended_tmax_c"))
    low = _safe_float(weighted.get("recommended_tmax_p25_c"))
    high = _safe_float(weighted.get("recommended_tmax_p75_c"))
    branch = str(weighted.get("selected_branch") or "").strip()
    strength = str((context.get("branch_assessment") or {}).get("reference_strength_cn") or "").strip()
    peak_hour = _safe_float(weighted.get("peak_center_hour_local"))
    if center is None and low is None and high is None:
        return None
    parts = []
    if center is not None:
        parts.append(f"参考中心 {_fmt_c(center)}")
    if low is not None and high is not None:
        parts.append(f"参考区间 {_fmt_c(low)}~{_fmt_c(high)}")
    elif low is not None:
        parts.append(f"参考下沿 {_fmt_c(low)}")
    elif high is not None:
        parts.append(f"参考上沿 {_fmt_c(high)}")
    if peak_hour is not None:
        parts.append(f"峰值约 {_fmt_peak_value({'peak_hour_local': peak_hour})}")
    if branch:
        parts.append(f"主分支 `{branch}`")
    if strength:
        parts.append(f"强度 `{strength}`")
    return "加权参考：" + "，".join(parts)


def render_historical_context_block(context: dict[str, Any]) -> str:
    if not context.get("available"):
        return ""
    lines = ["🧠 **历史知识库参考**"]
    summary_lines = [str(line) for line in (context.get("summary_lines") or []) if str(line).strip()]
    analogs = list(context.get("analogs") or [])

    for prefix in ("站点历史画像：", "当前实况匹配："):
        line = _pick_line(summary_lines, prefix)
        if line:
            lines.append(f"- {line}")

    reference_dates = _summarize_reference_dates(analogs, limit=2)
    if reference_dates:
        lines.append(f"- 参考日期：{reference_dates}")

    weighted_line = _render_weighted_reference(context)
    if weighted_line:
        lines.append(f"- {weighted_line}")

    branch_assessment = context.get("branch_assessment") or {}
    rationale = str(branch_assessment.get("preferred_branch_rationale") or "").strip()
    if rationale:
        lines.append(f"- 分支判断：{rationale}")

    adjustment_hint = str(context.get("adjustment_hint") or "").strip()
    if adjustment_hint:
        lines.append(f"- 历史提示：{adjustment_hint}")
    return "\n".join(lines)
