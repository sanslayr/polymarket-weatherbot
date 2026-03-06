#!/usr/bin/env python3
"""Structured payload helpers for historical analog context."""

from __future__ import annotations

from typing import Any

HISTORICAL_PAYLOAD_KEY = "historical"


def build_historical_payload(context: dict[str, Any] | None) -> dict[str, Any] | None:
    if not isinstance(context, dict):
        return None
    weighted_reference = context.get("weighted_reference") if isinstance(context.get("weighted_reference"), dict) else None
    return {
        "context": context,
        "adjustment_hint": context.get("adjustment_hint"),
        "weighted_reference": weighted_reference,
        "recommended_tmax_c": weighted_reference.get("recommended_tmax_c") if isinstance(weighted_reference, dict) else None,
        "synoptic_context": context.get("synoptic_context") if isinstance(context.get("synoptic_context"), dict) else None,
        "branch_assessment": context.get("branch_assessment") if isinstance(context.get("branch_assessment"), dict) else None,
    }


def attach_historical_payload(metar_diag: dict[str, Any], context: dict[str, Any] | None) -> dict[str, Any] | None:
    payload = build_historical_payload(context)
    if payload is None:
        return None
    metar_diag[HISTORICAL_PAYLOAD_KEY] = payload
    metar_diag["historical_context"] = payload.get("context")
    return payload


def get_historical_payload(metar_diag: dict[str, Any] | None) -> dict[str, Any] | None:
    if not isinstance(metar_diag, dict):
        return None
    payload = metar_diag.get(HISTORICAL_PAYLOAD_KEY)
    if isinstance(payload, dict):
        return payload
    context = metar_diag.get("historical_context")
    if not isinstance(context, dict):
        return None
    payload = build_historical_payload(context)
    if payload is not None:
        metar_diag[HISTORICAL_PAYLOAD_KEY] = payload
    return payload


def get_weighted_reference(metar_diag: dict[str, Any] | None) -> dict[str, Any] | None:
    payload = get_historical_payload(metar_diag)
    if not isinstance(payload, dict):
        return None
    weighted = payload.get("weighted_reference")
    return weighted if isinstance(weighted, dict) else None


def get_adjustment_hint(metar_diag: dict[str, Any] | None) -> str | None:
    payload = get_historical_payload(metar_diag)
    if not isinstance(payload, dict):
        return None
    hint = payload.get("adjustment_hint")
    return str(hint) if hint else None


def get_synoptic_context(metar_diag: dict[str, Any] | None) -> dict[str, Any] | None:
    payload = get_historical_payload(metar_diag)
    if not isinstance(payload, dict):
        return None
    context = payload.get("synoptic_context")
    return context if isinstance(context, dict) else None
