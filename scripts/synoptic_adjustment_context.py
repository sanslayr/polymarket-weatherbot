#!/usr/bin/env python3
"""Normalized synoptic adjustment context for historical fusion.

Current source can be `forecast_decision`.
Future ERA5-derived background should emit the same shape, so historical fusion
does not need to change its call chain.
"""

from __future__ import annotations

from typing import Any


WARM_TOKENS = (
    "暖平流",
    "干空气",
    "干层",
    "云开",
    "升温加速",
    "高压暖脊",
    "高压脊",
    "下沉稳定",
    "地面高压配合",
)
COOL_TOKENS = (
    "冷平流",
    "湿层约束",
    "湿层",
    "低云更易维持",
    "压制",
    "封盖",
    "低云",
    "低压深槽",
    "低压槽",
    "动力抬升",
    "地面低压配合",
    "短波扰动嵌入",
    "槽加强",
    "PVA",
)


def _safe_text(value: Any) -> str:
    return str(value or "").strip()


def _token_hits(text: str, tokens: tuple[str, ...]) -> list[str]:
    return [token for token in tokens if token in text]


def _from_forecast_decision(forecast_decision: dict[str, Any] | None) -> dict[str, Any]:
    if not isinstance(forecast_decision, dict):
        return {
            "available": False,
            "source": "none",
            "line_500": "",
            "line_850": "",
            "extra": "",
            "synoptic_text": "",
            "warming_support_score": 0.0,
            "cooling_support_score": 0.0,
            "warm_tokens": [],
            "cool_tokens": [],
        }

    background = (((forecast_decision.get("decision") or {}).get("background")) or {})
    line_500 = _safe_text(background.get("line_500"))
    line_850 = _safe_text(background.get("line_850"))
    extra = _safe_text(background.get("extra") or ((forecast_decision.get("decision") or {}).get("bottleneck")))
    synoptic_text = " ".join(part for part in (line_500, line_850, extra) if part)
    warm_hits = _token_hits(synoptic_text, WARM_TOKENS)
    cool_hits = _token_hits(synoptic_text, COOL_TOKENS)
    return {
        "available": bool(synoptic_text),
        "source": "forecast_decision",
        "line_500": line_500,
        "line_850": line_850,
        "extra": extra,
        "synoptic_text": synoptic_text,
        "warming_support_score": float(len(warm_hits)),
        "cooling_support_score": float(len(cool_hits)),
        "warm_tokens": warm_hits,
        "cool_tokens": cool_hits,
    }


def merge_synoptic_adjustment_context(
    base_context: dict[str, Any] | None,
    external_context: dict[str, Any] | None,
) -> dict[str, Any]:
    base = dict(base_context or {})
    ext = dict(external_context or {})
    if not ext:
        return base
    if not base:
        return ext

    warm_tokens = list(dict.fromkeys([*base.get("warm_tokens", []), *ext.get("warm_tokens", [])]))
    cool_tokens = list(dict.fromkeys([*base.get("cool_tokens", []), *ext.get("cool_tokens", [])]))
    synoptic_text_parts = [_safe_text(base.get("synoptic_text")), _safe_text(ext.get("synoptic_text"))]
    source = _safe_text(ext.get("source")) or _safe_text(base.get("source")) or "hybrid"
    if base.get("available") and ext.get("available") and source not in {"forecast_decision", "era5"}:
        source = "hybrid"
    return {
        "available": bool(base.get("available") or ext.get("available")),
        "source": source if source else "hybrid",
        "line_500": _safe_text(ext.get("line_500")) or _safe_text(base.get("line_500")),
        "line_850": _safe_text(ext.get("line_850")) or _safe_text(base.get("line_850")),
        "extra": _safe_text(ext.get("extra")) or _safe_text(base.get("extra")),
        "synoptic_text": " ".join(part for part in synoptic_text_parts if part),
        "warming_support_score": max(float(base.get("warming_support_score") or 0.0), float(ext.get("warming_support_score") or 0.0)),
        "cooling_support_score": max(float(base.get("cooling_support_score") or 0.0), float(ext.get("cooling_support_score") or 0.0)),
        "warm_tokens": warm_tokens,
        "cool_tokens": cool_tokens,
    }


def build_synoptic_adjustment_context(
    *,
    forecast_decision: dict[str, Any] | None = None,
    external_context: dict[str, Any] | None = None,
) -> dict[str, Any]:
    base = _from_forecast_decision(forecast_decision)
    return merge_synoptic_adjustment_context(base, external_context)


def branch_alignment(label: str, synoptic_context: dict[str, Any] | None) -> dict[str, Any]:
    context = dict(synoptic_context or {})
    warming_score = float(context.get("warming_support_score") or 0.0)
    cooling_score = float(context.get("cooling_support_score") or 0.0)
    warm_tokens = list(context.get("warm_tokens") or [])
    cool_tokens = list(context.get("cool_tokens") or [])
    branch = str(label or "")
    branch_type = "neutral"
    if any(token in branch for token in ("末段冲高", "开窗反弹", "干混合", "晴空增温")):
        branch_type = "warming"
    elif any(token in branch for token in ("云压制", "降水重置", "湿热滞留")):
        branch_type = "cooling"

    alignment = "neutral"
    rationale = ""
    if branch_type == "warming":
        if warming_score > cooling_score and warming_score > 0:
            alignment = "supportive"
            rationale = "环流背景偏向放大增温路径"
        elif cooling_score > warming_score and cooling_score > 0:
            alignment = "conflicting"
            rationale = "环流背景偏向抑制增温路径"
    elif branch_type == "cooling":
        if cooling_score > warming_score and cooling_score > 0:
            alignment = "supportive"
            rationale = "环流背景偏向压制/滞留路径"
        elif warming_score > cooling_score and warming_score > 0:
            alignment = "conflicting"
            rationale = "环流背景偏向削弱压制路径"

    return {
        "branch_type": branch_type,
        "alignment": alignment,
        "rationale": rationale,
        "warm_tokens": warm_tokens,
        "cool_tokens": cool_tokens,
        "source": _safe_text(context.get("source")) or "none",
    }
