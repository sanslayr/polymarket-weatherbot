#!/usr/bin/env python3
"""Declarative station regime rule definitions."""

from __future__ import annotations

from typing import Any


STATION_REGIME_RULES: dict[str, dict[str, Any]] = {
    "LTAC": {
        "regimes": [
            {
                "id": "sunny_highland_dry_mix",
                "label": "晴空干混合高原态",
                "description": "晴空、干燥、弱风且仍有升温跑道时，高原内陆站点更易走出更强白天增温。",
                "gate": {
                    "cloud_codes": ["SKC", "CLR", "NSC", "NCD", "CAVOK", "FEW"],
                    "max_wind_kt": 8.0,
                    "min_dewpoint_dep_c": 10.0,
                    "max_rh_pct": 58.0,
                    "min_hours_to_peak": 0.8,
                    "min_non_fading_trend_c": -0.03,
                    "nonprecip_only": True,
                },
                "posterior_effect": {
                    "median_shift_c": {"base": 0.28, "per_strength": 0.16, "cap": 0.95},
                    "spread_scale": {"base": 0.92, "per_strength": -0.05, "floor": 0.72},
                    "warm_tail_bias": {"base": 0.05, "per_strength": 0.03, "cap": 0.22},
                    "floor_lift_c": {"base": 0.08, "per_strength": 0.08, "cap": 0.34},
                    "timing_shift_h": {"base": 0.08, "per_strength": 0.04, "cap": 0.35},
                },
            }
        ]
    }
}


def get_station_regime_rules(station_icao: str | None) -> list[dict[str, Any]]:
    station = str(station_icao or "").upper()
    node = STATION_REGIME_RULES.get(station)
    if not isinstance(node, dict):
        return []
    return [dict(item) for item in (node.get("regimes") or []) if isinstance(item, dict)]
