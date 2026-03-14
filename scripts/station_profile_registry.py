#!/usr/bin/env python3
"""Static station profiles for regime-aware posterior adjustments."""

from __future__ import annotations

from typing import Any


_STATION_PROFILES: dict[str, dict[str, Any]] = {
    "LTAC": {
        "station": "LTAC",
        "terrain_tags": ["inland", "plateau", "high_diurnal_range"],
        "traits": [
            "strong_sunny_day_radiative_warming_potential",
            "dry_mixing_capable",
            "not_overly_constrained_by_generic_weak_transport_when_sunny_dry",
        ],
        "regime_families": ["sunny_highland_dry_mix"],
        "failure_modes": [
            "generic_low_level_transport_weakness_can_understate_plateau_daytime_warming",
        ],
    },
}


def get_station_profile(station_icao: str | None) -> dict[str, Any]:
    station = str(station_icao or "").upper()
    profile = _STATION_PROFILES.get(station)
    if not isinstance(profile, dict):
        return {"station": station, "terrain_tags": [], "traits": [], "regime_families": []}
    return dict(profile)
