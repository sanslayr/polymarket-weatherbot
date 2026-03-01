from __future__ import annotations

import math
from typing import Any


def _safe_float(v: Any, default: float = 0.0) -> float:
    try:
        return float(v)
    except Exception:
        return default


def _hour_of(ts_local: str) -> int:
    # expects YYYY-MM-DDTHH:MM
    try:
        return int(str(ts_local)[11:13])
    except Exception:
        return 12


def _gauss(x: float, mu: float, sigma: float) -> float:
    if sigma <= 0:
        return 0.0
    z = (x - mu) / sigma
    return math.exp(-0.5 * z * z)


def _solar_prior(hour_local: int) -> float:
    # Default local Tmax prior: usually 13-15 LT, with broad shoulder 12-16.
    # Keep soft prior (not hard rule) so dynamic weather can override.
    return max(_gauss(hour_local, 14.2, 1.8), 0.65 * _gauss(hour_local, 15.0, 2.3))


def _mixing_potential(w850_kmh: float) -> float:
    # weak wind -> poor mixing, too strong wind -> advective override/noisy local heating
    if w850_kmh <= 2:
        return 0.15
    if w850_kmh <= 8:
        return 0.45
    if w850_kmh <= 24:
        return 0.95
    if w850_kmh <= 38:
        return 0.65
    return 0.35


def _advection_hint(t850_now: float, t850_prev: float, wd850_deg: float) -> float:
    # simple model-side advection proxy: warming aloft trend + directional persistence
    dt = t850_now - t850_prev
    trend = max(-1.0, min(1.0, dt / 1.2))
    dir_term = 0.2 if wd850_deg >= 0 else 0.0
    return 0.65 * trend + dir_term


def hour_score(hourly: dict[str, Any], i: int) -> tuple[float, dict[str, float | bool | str]]:
    times = hourly.get("time") or []
    t2m = hourly.get("temperature_2m") or []
    clow = hourly.get("cloud_cover_low") or []
    t850 = hourly.get("temperature_850hPa") or []
    wspd850 = hourly.get("wind_speed_850hPa") or []
    wdir850 = hourly.get("wind_direction_850hPa") or []

    n = len(t2m)
    if n == 0:
        return 0.0, {}

    tmin = min(_safe_float(x) for x in t2m)
    tmax = max(_safe_float(x) for x in t2m)
    span = max(0.5, tmax - tmin)

    t = _safe_float(t2m[i])
    temp_norm = max(0.0, min(1.0, (t - tmin) / span))

    up1 = max(0.0, _safe_float(t2m[i + 1], t) - t) if i + 1 < n else 0.0
    up2 = max(0.0, _safe_float(t2m[i + 2], _safe_float(t2m[i + 1], t)) - _safe_float(t2m[i + 1], t)) if i + 2 < n else 0.0
    down1 = max(0.0, t - _safe_float(t2m[i + 1], t)) if i + 1 < n else 0.0

    traj_up = min(2.0, up1 + up2) / 2.0
    traj_dn = min(1.5, down1) / 1.5

    cloud_pct = max(0.0, min(100.0, _safe_float(clow[i], 50.0)))
    cloud_clear = 1.0 - cloud_pct / 100.0

    hh = _hour_of(times[i] if i < len(times) else "")
    solar = _solar_prior(hh)

    w850 = _safe_float(wspd850[i], 12.0)
    mix = _mixing_potential(w850)

    t850_now = _safe_float(t850[i], 0.0)
    t850_prev = _safe_float(t850[i - 1], t850_now) if i - 1 >= 0 else t850_now
    wd850 = _safe_float(wdir850[i], -1.0)
    adv = _advection_hint(t850_now, t850_prev, wd850)

    day_like = 10 <= hh <= 18
    # Night-time advection override: rare but possible (warm advection nocturnal rise / cold surge drop)
    nocturnal_override = (not day_like) and (abs(adv) >= 0.45) and (abs(_safe_float(t2m[i], 0.0) - _safe_float(t2m[max(0, i - 1)], 0.0)) >= 0.4)

    # Base score: model t2m remains core, but constrained by physical window prior.
    score = (
        0.48 * temp_norm
        + 0.15 * traj_up
        + 0.14 * solar
        + 0.10 * cloud_clear
        + 0.07 * mix
        + 0.06 * max(0.0, adv)
        - 0.08 * traj_dn
    )

    # Stable clear-sky guard against inertial over-shift late afternoon.
    if day_like and hh >= 16 and cloud_clear >= 0.75 and traj_up <= 0.10:
        score -= 0.10

    if nocturnal_override:
        score += 0.18 * (1.0 if adv > 0 else 0.7)

    score = max(0.0, min(1.5, score))

    info = {
        "temp_norm": round(temp_norm, 3),
        "traj_up": round(traj_up, 3),
        "traj_dn": round(traj_dn, 3),
        "solar_prior": round(solar, 3),
        "cloud_clear": round(cloud_clear, 3),
        "mix": round(mix, 3),
        "adv": round(adv, 3),
        "hour": hh,
        "nocturnal_override": nocturnal_override,
    }
    return float(score), info


def pick_peak_indices(hourly: dict[str, Any], limit: int = 4, min_separation_hours: int = 2) -> list[tuple[int, float, dict[str, Any]]]:
    t2m = hourly.get("temperature_2m") or []
    if not t2m:
        return []

    raw: list[tuple[int, float, dict[str, Any]]] = []
    for i in range(len(t2m)):
        s, info = hour_score(hourly, i)
        raw.append((i, s, info))

    raw.sort(key=lambda x: x[1], reverse=True)

    picked: list[tuple[int, float, dict[str, Any]]] = []
    for i, s, info in raw:
        if any(abs(i - j) <= min_separation_hours for j, _s, _info in picked):
            continue
        picked.append((i, s, info))
        if len(picked) >= max(1, limit):
            break
    return picked
