from __future__ import annotations

import json
import re
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import requests

ROOT = Path(__file__).resolve().parent.parent
CACHE_DIR = ROOT / "cache" / "runtime"


def _cache_file(icao: str) -> Path:
    return CACHE_DIR / f"metar24_{str(icao).upper()}.json"


def _iso_utc(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def _parse_iso_utc(v: Any) -> datetime | None:
    try:
        s = str(v or "")
        if not s:
            return None
        return datetime.fromisoformat(s.replace("Z", "+00:00")).astimezone(timezone.utc)
    except Exception:
        return None


def _normalize_metar_payload(payload: Any) -> list[dict[str, Any]]:
    if not isinstance(payload, list):
        return []
    out: list[dict[str, Any]] = []
    for item in payload:
        if isinstance(item, dict):
            out.append(item)
    return out


def _read_cache(
    icao: str,
    now_utc: datetime,
    *,
    allow_stale: bool = False,
    stale_max_hours: float = 36.0,
) -> list[dict[str, Any]] | None:
    p = _cache_file(icao)
    if not p.exists():
        return None
    try:
        doc = json.loads(p.read_text(encoding="utf-8"))
        exp = _parse_iso_utc(doc.get("expires_at_utc"))
        updated = _parse_iso_utc(doc.get("updated_at_utc"))
        payload = _normalize_metar_payload(doc.get("payload"))
        if exp is None:
            return None
        if now_utc <= exp:
            return payload
        if allow_stale and updated is not None:
            age_h = (now_utc - updated).total_seconds() / 3600.0
            if age_h <= max(1.0, float(stale_max_hours)):
                return payload
    except Exception:
        return None
    return None


def _write_cache(icao: str, payload: list[dict[str, Any]], now_utc: datetime, ttl_minutes: int = 15) -> None:
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    doc = {
        "updated_at_utc": _iso_utc(now_utc),
        "expires_at_utc": _iso_utc(now_utc + timedelta(minutes=max(3, int(ttl_minutes)))),
        "payload": payload,
    }
    _cache_file(icao).write_text(json.dumps(doc, ensure_ascii=False), encoding="utf-8")


def fetch_metar_24h(icao: str) -> list[dict[str, Any]]:
    now_utc = datetime.now(timezone.utc)
    cached = _read_cache(icao, now_utc)
    if cached is not None:
        return cached

    url = f"https://aviationweather.gov/api/data/metar?ids={icao}&format=json&hours=24"
    for _ in range(1, 4):
        try:
            r = requests.get(url, timeout=40)
            r.raise_for_status()
            data = _normalize_metar_payload(r.json())
            _write_cache(icao, data, now_utc)
            return data
        except Exception:
            pass

    stale = _read_cache(icao, now_utc, allow_stale=True)
    if stale is not None:
        return stale
    # Negative-cache short outages to avoid repeated DNS/retry overhead every command.
    _write_cache(icao, [], now_utc, ttl_minutes=5)
    return []


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
