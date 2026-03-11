from __future__ import annotations

import os
from datetime import datetime, timezone
from typing import Any

import requests

from hourly_data_service import model_cycle_tag
from metar_utils import metar_obs_time_utc, read_cached_metar_24h

REPORT_RESULT_VERSION = "look-report-v2026-03-11-27"
LOOK_RESULT_REUSE_WINDOW_SECONDS = int(os.getenv("LOOK_RESULT_REUSE_WINDOW_SECONDS", "120") or "120")
LOOK_METAR_SIGNATURE_TIMEOUT_SECONDS = float(
    os.getenv("LOOK_METAR_SIGNATURE_TIMEOUT_SECONDS", "3") or "3"
)
LOOK_FORCE_LIVE_METAR = str(os.getenv("LOOK_FORCE_LIVE_METAR", "1") or "1").strip().lower() in {"1", "true", "yes", "on"}
LOOK_FORCE_LIVE_POLYMARKET = str(os.getenv("LOOK_FORCE_LIVE_POLYMARKET", "1") or "1").strip().lower() in {"1", "true", "yes", "on"}


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
    if LOOK_FORCE_LIVE_METAR or LOOK_FORCE_LIVE_POLYMARKET:
        return None
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

    if LOOK_RESULT_REUSE_WINDOW_SECONDS > 0 and _seconds_since_generated(payload) <= LOOK_RESULT_REUSE_WINDOW_SECONDS:
        return (
            f"♻️ 已查询过 {query_label}；结果仍在 {LOOK_RESULT_REUSE_WINDOW_SECONDS}s 复用窗口内。"
            f"请查看上一次该站点日期的 /look 结果（{_format_age_since_generated(payload)}前生成）。"
        )

    live_metar = _latest_cached_metar_signature(icao)
    if live_metar is None:
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
        response = requests.get(url, timeout=LOOK_METAR_SIGNATURE_TIMEOUT_SECONDS)
        response.raise_for_status()
        payload = response.json()
    except Exception:
        return None
    if not isinstance(payload, list):
        return None
    latest = _latest_metar_signature([item for item in payload if isinstance(item, dict)])
    return latest if latest else None


def _latest_cached_metar_signature(icao: str) -> dict[str, str] | None:
    cached_rows = read_cached_metar_24h(icao, allow_stale=False)
    if not cached_rows:
        return None
    return _latest_metar_signature(cached_rows)


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
