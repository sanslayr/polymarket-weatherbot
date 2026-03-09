#!/usr/bin/env python3
"""Weather posterior orchestration service."""

from __future__ import annotations

from typing import Any

from weather_posterior_calibration import apply_weather_posterior_calibration
from weather_posterior_core import build_weather_posterior_core


def build_weather_posterior(
    *,
    canonical_raw_state: dict[str, Any],
    posterior_feature_vector: dict[str, Any],
    quality_snapshot: dict[str, Any],
) -> dict[str, Any]:
    posterior_core = build_weather_posterior_core(
        canonical_raw_state=canonical_raw_state,
        posterior_feature_vector=posterior_feature_vector,
    )
    return apply_weather_posterior_calibration(
        posterior_core=posterior_core,
        quality_snapshot=quality_snapshot,
    )
