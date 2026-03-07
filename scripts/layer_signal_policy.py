#!/usr/bin/env python3
"""Shared layer-signal weighting helpers for /look rendering."""

from __future__ import annotations

from typing import Any


def _text(value: Any) -> str:
    return str(value or "").strip()


def h700_is_moist_constraint(summary: Any) -> bool:
    text = _text(summary)
    return ("湿层" in text) or ("约束" in text)


def h700_dry_support_factor(summary: Any) -> float:
    text = _text(summary)
    if not text:
        return 0.0
    if h700_is_moist_constraint(text):
        return 0.0
    if ("近站" in text) and ("干层" in text or "偏干" in text):
        return 1.0
    if ("外围" in text) and ("干层" in text or "偏干" in text):
        return 0.45
    if ("偏远" in text) and ("干层" in text or "偏干" in text):
        return 0.12
    if ("代理" in text) and ("干层" in text or "偏干" in text):
        return 0.18
    if "干层" in text:
        return 0.60
    if "偏干" in text:
        return 0.25
    return 0.0


def _cloud_limited(low_cloud_pct: Any = None, cloud_code_now: Any = None) -> bool:
    try:
        low_cloud = float(low_cloud_pct) if low_cloud_pct is not None else None
    except Exception:
        low_cloud = None
    code = _text(cloud_code_now).upper()
    if low_cloud is not None and low_cloud >= 55.0:
        return True
    return code in {"BKN", "OVC", "VV"}


def h700_effective_dry_factor(
    summary: Any,
    *,
    low_cloud_pct: Any = None,
    cloud_code_now: Any = None,
) -> float:
    factor = h700_dry_support_factor(summary)
    if factor <= 0:
        return 0.0
    text = _text(summary)
    if _cloud_limited(low_cloud_pct=low_cloud_pct, cloud_code_now=cloud_code_now):
        if "近站" in text:
            factor *= 0.55
        else:
            factor *= 0.25
    return max(0.0, min(1.0, factor))


def h700_is_direct_dry_signal(summary: Any) -> bool:
    text = _text(summary)
    return ("近站" in text) and h700_dry_support_factor(text) >= 0.7


def h700_should_surface_in_evidence(
    summary: Any,
    *,
    low_cloud_pct: Any = None,
    cloud_code_now: Any = None,
) -> bool:
    text = _text(summary)
    if not text:
        return False
    if h700_is_moist_constraint(text):
        return True
    return h700_effective_dry_factor(
        text,
        low_cloud_pct=low_cloud_pct,
        cloud_code_now=cloud_code_now,
    ) >= 0.7
