from __future__ import annotations

import json
import os
import re
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

import requests
from runtime_cache_policy import runtime_cache_enabled

ROOT = Path(__file__).resolve().parent.parent
CACHE_DIR = ROOT / "cache" / "runtime"
RAW_OB_TIME_RE = re.compile(r"\b(\d{2})(\d{2})(\d{2})Z\b")
METAR_FETCH_HOURS = 30
METAR_TIMEOUT_SECONDS = float(os.getenv("WEATHERBOT_METAR_TIMEOUT_SECONDS", "3") or "3")
METAR_FETCH_ATTEMPTS = max(1, int(os.getenv("WEATHERBOT_METAR_FETCH_ATTEMPTS", "1") or "1"))


def _cache_file(icao: str) -> Path:
    # Keep the existing filename stable so /look and alert share the same cache key.
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


def _reference_utc_for_metar(metar: dict[str, Any]) -> datetime | None:
    for key in ("reportTime", "receiptTime"):
        dt = _parse_iso_utc(metar.get(key))
        if dt is not None:
            return dt
    try:
        obs_time = metar.get("obsTime")
        if obs_time not in (None, ""):
            return datetime.fromtimestamp(float(obs_time), tz=timezone.utc)
    except Exception:
        pass
    return None


def metar_raw_prefix(metar: dict[str, Any]) -> str:
    raw = str(metar.get("rawOb") or "").strip().upper()
    if raw.startswith("SPECI "):
        return "SPECI"
    if raw.startswith("METAR "):
        return "METAR"
    return ""


def is_routine_metar_report(metar: dict[str, Any]) -> bool:
    return metar_raw_prefix(metar) == "METAR"


def metar_raw_ob_time_utc(metar: dict[str, Any]) -> datetime | None:
    raw = (metar.get("rawOb") or "").strip()
    match = RAW_OB_TIME_RE.search(raw)
    if not match:
        return None

    ref = _reference_utc_for_metar(metar)
    if ref is None:
        return None

    day, hh, mm = int(match.group(1)), int(match.group(2)), int(match.group(3))
    candidate = datetime(ref.year, ref.month, 1, tzinfo=timezone.utc) + timedelta(
        days=day - 1,
        hours=hh,
        minutes=mm,
    )

    # METAR raw issue times carry DDHHMMZ but not month/year. Anchor on the nearest
    # plausible month around the API timestamp so month boundaries do not drift.
    best = candidate
    best_delta = abs((candidate - ref).total_seconds())
    for month_shift in (-1, 1):
        shifted_anchor = (datetime(ref.year, ref.month, 15, tzinfo=timezone.utc) + timedelta(days=32 * month_shift))
        shifted_month_start = datetime(shifted_anchor.year, shifted_anchor.month, 1, tzinfo=timezone.utc)
        shifted = shifted_month_start + timedelta(days=day - 1, hours=hh, minutes=mm)
        delta = abs((shifted - ref).total_seconds())
        if delta < best_delta:
            best = shifted
            best_delta = delta
    return best


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
    if not runtime_cache_enabled():
        return None
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
    if not runtime_cache_enabled():
        return
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    doc = {
        "updated_at_utc": _iso_utc(now_utc),
        "expires_at_utc": _iso_utc(now_utc + timedelta(minutes=max(3, int(ttl_minutes)))),
        "payload": payload,
    }
    _cache_file(icao).write_text(json.dumps(doc, ensure_ascii=False), encoding="utf-8")


def fetch_metar_24h(icao: str, *, force_refresh: bool = False) -> list[dict[str, Any]]:
    now_utc = datetime.now(timezone.utc)
    if not force_refresh:
        cached = _read_cache(icao, now_utc)
        if cached is not None:
            return cached

    url = f"https://aviationweather.gov/api/data/metar?ids={icao}&format=json&hours={METAR_FETCH_HOURS}"
    for _ in range(METAR_FETCH_ATTEMPTS):
        try:
            r = requests.get(url, timeout=METAR_TIMEOUT_SECONDS)
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


def read_cached_metar_24h(
    icao: str,
    *,
    allow_stale: bool = False,
    stale_max_hours: float = 36.0,
) -> list[dict[str, Any]] | None:
    return _read_cache(
        icao,
        datetime.now(timezone.utc),
        allow_stale=allow_stale,
        stale_max_hours=stale_max_hours,
    )


def metar_obs_time_utc(metar: dict[str, Any]) -> datetime:
    raw_dt = metar_raw_ob_time_utc(metar)
    if raw_dt is not None:
        return raw_dt
    report_dt = _parse_iso_utc(metar.get("reportTime"))
    if report_dt is not None:
        return report_dt
    raise ValueError("METAR record missing rawOb issue time and reportTime")


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


def extract_observed_max_for_local_day(
    rows: list[dict[str, Any]] | None,
    tz_name: str,
    *,
    now_utc: datetime | None = None,
) -> dict[str, Any]:
    tz = ZoneInfo(str(tz_name or "UTC"))
    anchor_utc = now_utc.astimezone(timezone.utc) if isinstance(now_utc, datetime) else datetime.now(timezone.utc)
    target_local_date = anchor_utc.astimezone(tz).date()
    observed_max_temp_c = None
    observed_max_time_local = None

    for row in rows or []:
        try:
            report_utc = metar_obs_time_utc(row)
            local_dt = report_utc.astimezone(tz)
            temp = float(row.get("temp"))
        except Exception:
            continue
        if local_dt.date() != target_local_date:
            continue
        if observed_max_temp_c is None or temp > observed_max_temp_c:
            observed_max_temp_c = temp
            observed_max_time_local = local_dt
        elif (
            observed_max_temp_c is not None
            and abs(temp - observed_max_temp_c) < 1e-9
            and observed_max_time_local is not None
            and local_dt > observed_max_time_local
        ):
            observed_max_time_local = local_dt

    return {
        "target_local_date": target_local_date.isoformat(),
        "observed_max_temp_c": observed_max_temp_c,
        "observed_max_temp_quantized": bool(is_intish_value(observed_max_temp_c)) if observed_max_temp_c is not None else False,
        "observed_max_time_local": observed_max_time_local.isoformat() if observed_max_time_local is not None else None,
    }
