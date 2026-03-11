from __future__ import annotations

import json
import os
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

import requests

from runtime_cache_policy import runtime_cache_enabled
from station_catalog import Station


ROOT = Path(__file__).resolve().parent.parent
CACHE_DIR = ROOT / "cache" / "runtime"
MGM_CACHE_TTL_MINUTES = int(os.getenv("WEATHERBOT_MGM_CACHE_TTL_MINUTES", "10") or "10")
MGM_CACHE_STALE_HOURS = float(os.getenv("WEATHERBOT_MGM_CACHE_STALE_HOURS", "2") or "2")
MGM_TIMEOUT_SECONDS = float(os.getenv("WEATHERBOT_MGM_TIMEOUT_SECONDS", "4") or "4")


def _mgm_cache_path(icao: str) -> Path:
    return CACHE_DIR / f"mgm_reference_{str(icao).upper()}.json"


def _parse_cache_ts(value: Any) -> datetime | None:
    try:
        text = str(value or "").strip()
        if not text:
            return None
        return datetime.fromisoformat(text.replace("Z", "+00:00")).astimezone(timezone.utc)
    except Exception:
        return None


def _read_mgm_cache(icao: str, *, allow_stale: bool = False) -> dict[str, Any] | None:
    if not runtime_cache_enabled():
        return None
    path = _mgm_cache_path(icao)
    if not path.exists():
        return None
    try:
        doc = json.loads(path.read_text(encoding="utf-8"))
        payload = doc.get("payload")
        if not isinstance(payload, dict) or not payload:
            return None
        now_utc = datetime.now(timezone.utc)
        expires_at = _parse_cache_ts(doc.get("expires_at_utc"))
        if expires_at is not None and now_utc <= expires_at:
            return payload
        if allow_stale:
            updated_at = _parse_cache_ts(doc.get("updated_at_utc"))
            if updated_at is not None and (now_utc - updated_at) <= timedelta(hours=max(1.0, MGM_CACHE_STALE_HOURS)):
                return payload
    except Exception:
        return None
    return None


def _write_mgm_cache(icao: str, payload: dict[str, Any]) -> None:
    if not runtime_cache_enabled() or not payload:
        return
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    now_utc = datetime.now(timezone.utc)
    doc = {
        "updated_at_utc": now_utc.isoformat().replace("+00:00", "Z"),
        "expires_at_utc": (now_utc + timedelta(minutes=max(3, MGM_CACHE_TTL_MINUTES))).isoformat().replace("+00:00", "Z"),
        "payload": payload,
    }
    _mgm_cache_path(icao).write_text(json.dumps(doc, ensure_ascii=False), encoding="utf-8")


def _fetch_mgm_reference(station: Station) -> dict[str, Any] | None:
    if str(station.icao).upper() != "LTAC":
        return None
    cached = _read_mgm_cache(station.icao)
    if cached is not None:
        return cached

    headers = {
        "User-Agent": "Mozilla/5.0",
        "Referer": "https://www.mgm.gov.tr/",
        "Origin": "https://www.mgm.gov.tr",
        "Accept": "application/json, text/plain, */*",
    }
    try:
        query = requests.utils.quote("esenboga")
        candidates = requests.get(
            f"https://servis.mgm.gov.tr/web/merkezler?sorgu={query}",
            headers=headers,
            timeout=MGM_TIMEOUT_SECONDS,
        ).json()
        if not isinstance(candidates, list) or not candidates:
            return None
        center = candidates[0]
        center_id = center.get("merkezId")
        if center_id is None:
            return None
        observations = requests.get(
            f"https://servis.mgm.gov.tr/web/sondurumlar?merkezid={center_id}",
            headers=headers,
            timeout=MGM_TIMEOUT_SECONDS,
        ).json()
        if not isinstance(observations, list) or not observations:
            return None
        row = observations[0] or {}
        payload = {
            "source": "mgm",
            "merkez_id": center_id,
            "veri_zamani": row.get("veriZamani"),
            "temp_c": row.get("sicaklik"),
            "rh": row.get("nem"),
            "wind_kmh": row.get("ruzgarHiz"),
            "wind_dir": row.get("ruzgarYon"),
            "metar": row.get("rasatMetar"),
            "ilce": center.get("ilce"),
        }
        _write_mgm_cache(station.icao, payload)
        return payload
    except Exception:
        stale = _read_mgm_cache(station.icao, allow_stale=True)
        if stale is not None:
            return stale
        return None


def _wind_dir_text_cn(direction_deg: Any) -> str:
    try:
        deg = float(direction_deg) % 360.0
    except Exception:
        return "风向不定"
    dirs = [
        "北风", "东北偏北风", "东北风", "东北偏东风",
        "东风", "东南偏东风", "东南风", "东南偏南风",
        "南风", "西南偏南风", "西南风", "西南偏西风",
        "西风", "西北偏西风", "西北风", "西北偏北风",
    ]
    idx = int(((deg + 11.25) % 360.0) // 22.5)
    return dirs[idx]


def render_station_external_reference_line(reference: dict[str, Any], unit_pref: str) -> str:
    source = str((reference or {}).get("source") or "").strip().lower()
    if source != "mgm":
        return ""

    def _fmt_temp_ref(value_c: Any) -> str:
        try:
            numeric = float(value_c)
        except Exception:
            return str(value_c)
        if unit_pref == "F":
            return f"{(numeric * 9.0 / 5.0 + 32.0):.1f}°F"
        return f"{numeric:.1f}°C"

    fields = []
    value_time = str(reference.get("veri_zamani") or "")
    value_time_text = "--:-- Local"
    if value_time:
        try:
            parsed = datetime.fromisoformat(value_time.replace("Z", "+00:00"))
            if parsed.tzinfo is None:
                parsed = parsed.replace(tzinfo=timezone.utc)
            value_time_text = parsed.astimezone(ZoneInfo("Europe/Istanbul")).strftime("%H:%M Local")
        except Exception:
            if "T" in value_time and len(value_time) >= 16:
                value_time_text = value_time[11:16] + " Local"
    fields.append(f"- MGM参考（{value_time_text}）")

    try:
        temp_c = float(reference.get("temp_c")) if reference.get("temp_c") is not None else None
    except Exception:
        temp_c = None
    if temp_c is not None:
        fields.append(f"气温={_fmt_temp_ref(temp_c)}")

    rh = reference.get("rh")
    if rh is not None:
        fields.append(f"湿度={rh}%")

    wind_dir = reference.get("wind_dir")
    if wind_dir not in (None, ""):
        try:
            deg = float(wind_dir)
            fields.append(f"风向={_wind_dir_text_cn(deg)}（{deg:.0f}°）")
        except Exception:
            fields.append(f"风向={wind_dir}")

    wind_kmh = reference.get("wind_kmh")
    if wind_kmh is not None:
        try:
            fields.append(f"风速={float(wind_kmh):.1f}km/h")
        except Exception:
            fields.append(f"风速={wind_kmh}km/h")

    return "，".join(fields)


def fetch_station_external_reference(station: Station) -> dict[str, Any] | None:
    if str(station.icao).upper() == "LTAC":
        return _fetch_mgm_reference(station)
    return None

