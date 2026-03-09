from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

import requests

from hourly_data_service import model_cycle_tag
from metar_utils import metar_obs_time_utc

REPORT_RESULT_VERSION = "look-report-v2026-03-09-6"


def build_cached_result_meta(*, icao: str, model: str, metar24: list[dict[str, Any]] | None) -> dict[str, Any]:
    latest = _latest_metar_signature(metar24 or [])
    return {
        "icao": str(icao or "").upper(),
        "report_version": REPORT_RESULT_VERSION,
        "forecast_signature": _forecast_signature(model),
        "metar_signature": latest,
    }


def build_unchanged_notice(
    *,
    query_label: str,
    icao: str,
    model: str,
    cached_payload: dict[str, Any] | None,
) -> str | None:
    payload = cached_payload or {}
    meta = payload.get("result_meta") if isinstance(payload.get("result_meta"), dict) else {}
    if str(meta.get("report_version") or "").strip() != REPORT_RESULT_VERSION:
        return None
    cached_forecast = str(meta.get("forecast_signature") or "").strip()
    if not cached_forecast or cached_forecast != _forecast_signature(model):
        return None

    cached_metar = meta.get("metar_signature") if isinstance(meta.get("metar_signature"), dict) else {}
    cached_raw = str(cached_metar.get("raw_ob") or "").strip()
    cached_obs = str(cached_metar.get("obs_time_utc") or "").strip()
    if not cached_raw or not cached_obs:
        return None

    live_metar = fetch_live_latest_metar_signature(icao)
    if not live_metar:
        return None
    if str(live_metar.get("raw_ob") or "").strip() != cached_raw:
        return None
    if str(live_metar.get("obs_time_utc") or "").strip() != cached_obs:
        return None

    return (
        f"♻️ 已查询过 {query_label}；METAR 无更新，预报无需重拉。"
        f"请查看上一次该站点日期的 /look 结果（{_format_age_since_generated(payload)}前生成）。"
    )


def fetch_live_latest_metar_signature(icao: str) -> dict[str, str] | None:
    url = f"https://aviationweather.gov/api/data/metar?ids={str(icao or '').upper()}&format=json&hours=24"
    try:
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        payload = response.json()
    except Exception:
        return None
    if not isinstance(payload, list):
        return None
    latest = _latest_metar_signature([item for item in payload if isinstance(item, dict)])
    return latest if latest else None


def _forecast_signature(model: str) -> str:
    return model_cycle_tag(model, datetime.now(timezone.utc))


def _latest_metar_signature(metar24: list[dict[str, Any]]) -> dict[str, str] | None:
    latest: dict[str, Any] | None = None
    latest_dt: datetime | None = None
    for item in metar24:
        if not isinstance(item, dict):
            continue
        try:
            obs_dt = metar_obs_time_utc(item)
        except Exception:
            continue
        if latest_dt is None or obs_dt > latest_dt:
            latest = item
            latest_dt = obs_dt
    if latest is None or latest_dt is None:
        return None
    raw_ob = str(latest.get("rawOb") or "").strip()
    if not raw_ob:
        return None
    return {
        "raw_ob": raw_ob,
        "obs_time_utc": latest_dt.astimezone(timezone.utc).isoformat().replace("+00:00", "Z"),
    }


def _seconds_since_generated(payload: dict[str, Any]) -> int:
    try:
        updated_at = float(payload.get("updated_at"))
    except Exception:
        return 0
    return max(0, int(round(datetime.now(timezone.utc).timestamp() - updated_at)))


def _format_age_since_generated(payload: dict[str, Any]) -> str:
    total_seconds = _seconds_since_generated(payload)
    if total_seconds < 60:
        return f"{total_seconds}s"
    minutes, seconds = divmod(total_seconds, 60)
    return f"{minutes}m {seconds}s"
