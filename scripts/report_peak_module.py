#!/usr/bin/env python3
"""Compatibility wrapper for peak-range services.

Peak analysis and peak text rendering now live in `peak_range_service.py`.
Keep this module as a thin shim so older imports do not pull report logic
back into the rendering layer.
"""

from __future__ import annotations

from typing import Any

from peak_range_service import build_peak_range_summary, render_peak_range_block


def _build_peak_range_module(*args: Any, fmt_range_fn=None, unit: str = "C", **kwargs: Any) -> dict[str, Any]:
    summary = build_peak_range_summary(*args, unit=unit, **kwargs)
    return {
        "summary": summary,
        "block": render_peak_range_block(summary, unit=unit, fmt_range_fn=fmt_range_fn),
    }


__all__ = [
    "_build_peak_range_module",
    "build_peak_range_summary",
    "render_peak_range_block",
]
