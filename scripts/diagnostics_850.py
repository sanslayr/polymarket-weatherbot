from __future__ import annotations

from datetime import datetime, timedelta
from typing import Any


def _eta_hours(distance_km: float | None, score: float, w850_kmh: float | None) -> tuple[float, float]:
    # Heuristic ETA range (hours): distance / effective transport speed, then confidence widening.
    if distance_km is None or distance_km <= 0:
        return (0.0, 0.0)
    base_speed = max(18.0, (w850_kmh or 30.0) * 0.55)
    center = distance_km / base_speed
    # Better score => tighter ETA band.
    spread = max(1.0, 4.0 - 2.5 * score)
    lo = max(0.0, center - spread)
    hi = max(lo + 0.5, center + spread)
    return (lo, hi)


def advection_eta_local(now_local: datetime, distance_km: float | None, score: float, w850_kmh: float | None) -> str:
    lo_h, hi_h = _eta_hours(distance_km, score, w850_kmh)
    start = now_local + timedelta(hours=lo_h)
    end = now_local + timedelta(hours=hi_h)
    return f"{start.strftime('%H:%M')}–{end.strftime('%H:%M')} Local"


def label_from_score(score: float) -> str:
    if score >= 0.70:
        return "高概率触站"
    if score >= 0.45:
        return "中概率触站"
    return "低概率触站"


def distance_km_from_system(system: dict[str, Any]) -> float | None:
    geo = system.get("geo_context", {}) if isinstance(system, dict) else {}
    dkm = geo.get("distance_km")
    try:
        return float(dkm)
    except Exception:
        return None
