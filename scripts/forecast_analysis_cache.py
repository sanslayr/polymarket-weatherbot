from __future__ import annotations

import hashlib
import json
import os
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from analysis_snapshot_service import build_analysis_snapshot
from cache_envelope import extract_payload, make_cache_doc
from ecmwf_ensemble_factor_service import build_ecmwf_ensemble_factor
from runtime_cache_policy import runtime_cache_enabled


ROOT = Path(__file__).resolve().parent.parent
CACHE_DIR = ROOT / "cache" / "runtime"
SCHEMA_VERSION = "forecast-analysis-cache.v1"
_SAME_DAY_MIN_HOURS = 3.0
_OBS_DRIVEN_MIN_HOURS = 1.0


def _compact_ensemble_factor(payload: dict[str, Any] | None) -> dict[str, Any]:
    data = dict(payload or {})
    if not data:
        return {}
    keep_keys = (
        "schema_version",
        "member_count",
        "summary",
        "probabilities",
        "detail_probabilities",
        "diagnostics",
        "source",
        "selection",
    )
    return {
        key: value
        for key, value in data.items()
        if key in keep_keys and value not in (None, "", [], {})
    }


def _compact_analysis_snapshot(snapshot: dict[str, Any] | None, ensemble_factor: dict[str, Any]) -> dict[str, Any]:
    data = dict(snapshot or {})
    if not data:
        return {}
    compact_ensemble = _compact_ensemble_factor(ensemble_factor)
    if compact_ensemble:
        data["ensemble_factor"] = compact_ensemble
    canonical = dict(data.get("canonical_raw_state") or {})
    forecast = dict(canonical.get("forecast") or {})
    if forecast:
        forecast["ensemble_factor"] = compact_ensemble
        canonical["forecast"] = forecast
        data["canonical_raw_state"] = canonical
    return data


def _safe_float(value: Any) -> float | None:
    try:
        if value in (None, ""):
            return None
        return float(value)
    except Exception:
        return None


def _parse_iso_dt(value: Any) -> datetime | None:
    try:
        text = str(value or "").strip()
        return datetime.fromisoformat(text) if text else None
    except Exception:
        return None


def _cache_key(*parts: str) -> str:
    raw = "|".join(parts)
    return hashlib.sha1(raw.encode("utf-8")).hexdigest()[:16]


def _cache_path(*parts: str) -> Path:
    return CACHE_DIR / f"forecast_analysis_{_cache_key(*parts)}.json"


def should_build_ecmwf_ensemble_factor(primary_window: dict[str, Any], metar_diag: dict[str, Any]) -> bool:
    peak_local = _parse_iso_dt(primary_window.get("peak_local"))
    latest_local = _parse_iso_dt(metar_diag.get("latest_report_local"))
    if not peak_local or not latest_local:
        return False
    try:
        if peak_local.tzinfo is not None and latest_local.tzinfo is None:
            latest_local = latest_local.replace(tzinfo=peak_local.tzinfo)
        elif peak_local.tzinfo is None and latest_local.tzinfo is not None:
            peak_local = peak_local.replace(tzinfo=latest_local.tzinfo)
    except Exception:
        pass

    if peak_local.date() != latest_local.date():
        return True
    hours_to_peak = (peak_local - latest_local).total_seconds() / 3600.0
    if hours_to_peak >= _SAME_DAY_MIN_HOURS:
        return True
    if hours_to_peak < _OBS_DRIVEN_MIN_HOURS:
        return False

    signal_score = 0
    temp_trend = _safe_float(
        metar_diag.get("temp_trend_effective_c")
        if metar_diag.get("temp_trend_effective_c") is not None
        else (
            metar_diag.get("temp_trend_smooth_c")
            if metar_diag.get("temp_trend_smooth_c") is not None
            else metar_diag.get("temp_trend_1step_c")
        )
    )
    temp_bias = _safe_float(
        metar_diag.get("temp_bias_smooth_c")
        if metar_diag.get("temp_bias_smooth_c") is not None
        else metar_diag.get("temp_bias_c")
    )
    cloud_cover = _safe_float(
        metar_diag.get("cloud_effective_cover_smooth")
        if metar_diag.get("cloud_effective_cover_smooth") is not None
        else metar_diag.get("cloud_effective_cover")
    )
    radiation_eff = _safe_float(
        metar_diag.get("radiation_eff_smooth")
        if metar_diag.get("radiation_eff_smooth") is not None
        else metar_diag.get("radiation_eff")
    )
    wind_dir_change_deg = _safe_float(metar_diag.get("wind_dir_change_deg"))
    cloud_trend = str(metar_diag.get("cloud_trend") or "").strip().lower()
    latest_temp = _safe_float(metar_diag.get("latest_temp"))
    observed_max_temp = _safe_float(metar_diag.get("observed_max_temp_c"))

    if temp_trend is not None and abs(temp_trend) >= 0.18:
        signal_score += 1
    if temp_bias is not None and abs(temp_bias) >= 0.22:
        signal_score += 1
    if cloud_cover is not None and (cloud_cover <= 0.35 or cloud_cover >= 0.65):
        signal_score += 1
    if radiation_eff is not None and (radiation_eff >= 0.72 or radiation_eff <= 0.50):
        signal_score += 1
    if wind_dir_change_deg is not None and abs(wind_dir_change_deg) >= 25.0:
        signal_score += 1
    if cloud_trend and all(token not in cloud_trend for token in {"steady", "stable", "little change", "无明显", "稳定"}):
        signal_score += 1
    if latest_temp is not None and observed_max_temp is not None and latest_temp >= observed_max_temp - 0.2:
        signal_score += 1
    if bool(metar_diag.get("metar_speci_active")) or bool(metar_diag.get("metar_speci_likely")):
        signal_score += 1
    return signal_score >= 2


def read_cached_forecast_analysis(
    *,
    station_icao: str,
    target_date: str,
    model: str,
    synoptic_provider: str,
    runtime_tag: str,
    latest_report_local: str | None = None,
    analysis_peak_local: str | None = None,
    ttl_hours: int = int(os.getenv("WEATHERBOT_FORECAST_ANALYSIS_CACHE_TTL_HOURS", "6") or "6"),
) -> dict[str, Any] | None:
    if not runtime_cache_enabled():
        return None
    path = _cache_path(station_icao, target_date, model.lower(), synoptic_provider, runtime_tag)
    if not path.exists():
        return None
    try:
        doc = json.loads(path.read_text(encoding="utf-8"))
        payload, updated_at, _env = extract_payload(doc)
        if not isinstance(payload, dict):
            return None
        if str(payload.get("schema_version") or "") != SCHEMA_VERSION:
            return None
        if updated_at:
            ts = datetime.fromisoformat(str(updated_at).replace("Z", "+00:00"))
            if datetime.now(timezone.utc) - ts > timedelta(hours=ttl_hours):
                return None
        snapshot_fresh = True
        if latest_report_local and str(payload.get("latest_report_local") or "") != str(latest_report_local):
            snapshot_fresh = False
        if analysis_peak_local and str(payload.get("analysis_peak_local") or "") != str(analysis_peak_local):
            return None
        cached = dict(payload)
        cached["analysis_snapshot_fresh"] = snapshot_fresh
        if not snapshot_fresh:
            cached["analysis_snapshot"] = {}
        return cached
    except Exception:
        return None


def write_cached_forecast_analysis(
    payload: dict[str, Any],
    *,
    station_icao: str,
    target_date: str,
    model: str,
    synoptic_provider: str,
    runtime_tag: str,
) -> None:
    if not runtime_cache_enabled():
        return
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    path = _cache_path(station_icao, target_date, model.lower(), synoptic_provider, runtime_tag)
    doc = make_cache_doc(
        payload,
        source_state="fresh",
        payload_schema_version=str(payload.get("schema_version")) if isinstance(payload, dict) else None,
        meta={"kind": "forecast_analysis"},
    )
    path.write_text(json.dumps(doc, ensure_ascii=False), encoding="utf-8")


def build_and_cache_forecast_analysis(
    *,
    station_icao: str,
    station_lat: float,
    station_lon: float,
    target_date: str,
    model: str,
    synoptic_provider: str,
    runtime_tag: str,
    primary_window: dict[str, Any],
    synoptic_window: dict[str, Any] | None,
    metar_diag: dict[str, Any],
    forecast_decision: dict[str, Any],
    temp_shape_analysis: dict[str, Any] | None,
    temp_unit: str,
    tz_name: str,
) -> dict[str, Any]:
    analysis_window = dict(synoptic_window or primary_window or {})
    analysis_peak_local = str(analysis_window.get("peak_local") or primary_window.get("peak_local") or "")
    ensemble_factor_raw: dict[str, Any] | None = None
    if model.lower() == "ecmwf" and should_build_ecmwf_ensemble_factor(analysis_window, metar_diag):
        try:
            ensemble_factor_raw = build_ecmwf_ensemble_factor(
                station_icao=station_icao,
                station_lat=float(station_lat),
                station_lon=float(station_lon),
                peak_local=analysis_peak_local,
                tz_name=tz_name,
                preferred_runtime_tag=runtime_tag,
                root=ROOT,
            )
        except Exception:
            ensemble_factor_raw = None

    analysis_snapshot = build_analysis_snapshot(
        primary_window=primary_window,
        metar_diag=metar_diag,
        forecast_decision=forecast_decision,
        ensemble_factor=ensemble_factor_raw,
        temp_unit=temp_unit,
        synoptic_window=analysis_window,
        temp_shape_analysis=temp_shape_analysis,
    )
    compact_ensemble_factor = _compact_ensemble_factor(ensemble_factor_raw)
    compact_snapshot = _compact_analysis_snapshot(analysis_snapshot, compact_ensemble_factor)
    payload = {
        "schema_version": SCHEMA_VERSION,
        "station": station_icao,
        "target_date": target_date,
        "model": model.lower(),
        "synoptic_provider": synoptic_provider,
        "runtime_tag": runtime_tag,
        "latest_report_local": str(metar_diag.get("latest_report_local") or ""),
        "analysis_peak_local": analysis_peak_local,
        "analysis_snapshot_fresh": True,
        "ensemble_factor": compact_ensemble_factor,
        "analysis_snapshot": compact_snapshot,
    }
    write_cached_forecast_analysis(
        payload,
        station_icao=station_icao,
        target_date=target_date,
        model=model,
        synoptic_provider=synoptic_provider,
        runtime_tag=runtime_tag,
    )
    return payload
