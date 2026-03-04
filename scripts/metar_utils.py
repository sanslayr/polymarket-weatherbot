from __future__ import annotations

import re
from datetime import datetime, timezone
from typing import Any

import requests


def fetch_metar_24h(icao: str) -> list[dict[str, Any]]:
    url = f"https://aviationweather.gov/api/data/metar?ids={icao}&format=json&hours=24"
    last_err: Exception | None = None
    for _ in range(1, 4):
        try:
            r = requests.get(url, timeout=40)
            r.raise_for_status()
            return r.json()
        except Exception as exc:
            last_err = exc
    assert last_err is not None
    raise last_err


def metar_obs_time_utc(metar: dict[str, Any]) -> datetime:
    raw = (metar.get("rawOb") or "").strip()
    m = re.search(r"\b(\d{2})(\d{2})(\d{2})Z\b", raw)
    if m:
        day, hh, mm = int(m.group(1)), int(m.group(2)), int(m.group(3))
        rt = datetime.fromisoformat(metar["reportTime"].replace("Z", "+00:00")).astimezone(timezone.utc)
        return datetime(rt.year, rt.month, day, hh, mm, tzinfo=timezone.utc)
    return datetime.fromisoformat(metar["reportTime"].replace("Z", "+00:00")).astimezone(timezone.utc)


def is_intish_value(v: Any) -> bool:
    try:
        return abs(float(v) - round(float(v))) < 0.05
    except Exception:
        return False


def observed_max_interval_c(
    obs_max_c: Any,
    display_unit: str,
    c_quantized: bool | None = None,
) -> tuple[float | None, float | None]:
    try:
        if obs_max_c is None:
            return None, None
        x = float(obs_max_c)
    except Exception:
        return None, None

    u = str(display_unit or "").upper()
    if u == "F":
        xf = x * 9.0 / 5.0 + 32.0
        lo_c = (xf - 0.50 - 32.0) * 5.0 / 9.0
        hi_c = (xf + 0.50 - 32.0) * 5.0 / 9.0
        return lo_c, hi_c

    q = is_intish_value(x) if c_quantized is None else bool(c_quantized)
    if q:
        return x - 0.50, x + 0.49
    return x, x
