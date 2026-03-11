#!/usr/bin/env python3
"""Peak-range text rendering from structured peak summary."""

from __future__ import annotations

from typing import Any


def render_peak_range_block(
    peak_summary: dict[str, Any],
    *,
    unit: str,
    fmt_range_fn,
) -> list[str]:
    summary = dict(peak_summary or {})
    ranges = dict(summary.get("ranges") or {})
    settled = dict(ranges.get("settled") or {})
    display_range = dict(ranges.get("display") or {})
    core_range = dict(ranges.get("core") or {})
    window = dict(ranges.get("window") or {})
    annotations = [str(item) for item in (summary.get("annotations") or []) if str(item).strip()]
    block = ["🌡️ **可能最高温区间（仅供参考）**"]
    if settled.get("active"):
        block.append(
            f"• **{fmt_range_fn(float(settled.get('lo')), float(settled.get('hi')))}**（{str(settled.get('reason') or '')}）"
        )
    else:
        disp_lo = float(display_range.get("lo"))
        disp_hi = float(display_range.get("hi"))
        core_lo = float(core_range.get("lo"))
        core_hi = float(core_range.get("hi"))
        window_label = str(window.get("label") or "峰值窗")
        window_text = str(window.get("text") or "")
        if core_lo != disp_lo or core_hi != disp_hi:
            block.append(f"• **{fmt_range_fn(disp_lo, disp_hi)}**（主看 {fmt_range_fn(core_lo, core_hi)}；{window_label} {window_text}）")
        else:
            block.append(f"• **{fmt_range_fn(disp_lo, disp_hi)}**（{window_label} {window_text}）")

    block.extend(annotations)
    return block
