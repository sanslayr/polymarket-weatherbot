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


def h700_is_direct_dry_signal(summary: Any) -> bool:
    return h700_dry_support_factor(summary) >= 0.6


def h700_should_surface_in_evidence(summary: Any) -> bool:
    text = _text(summary)
    if not text:
        return False
    if h700_is_moist_constraint(text):
        return True
    return h700_is_direct_dry_signal(text)
