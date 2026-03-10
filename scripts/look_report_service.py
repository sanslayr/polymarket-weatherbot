#!/usr/bin/env python3
"""Primary /look orchestration service."""

from __future__ import annotations

import os
import time
from concurrent.futures import ThreadPoolExecutor, TimeoutError as FuturesTimeoutError
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Callable
from zoneinfo import ZoneInfo

import requests

import build_station_links as BSL
from analysis_snapshot_service import build_analysis_snapshot
from forecast_pipeline import load_or_build_forecast_decision
from historical_context_provider import build_historical_context, historical_context_enabled
from historical_payload import attach_historical_payload
from hourly_data_service import (
    build_post_eval_window as _build_post_eval_window,
    build_post_focus_window as _build_post_focus_window,
    detect_tmax_windows,
    fetch_hourly_router,
    slice_hourly_local_day,
)
from metar_analysis_service import metar_observation_block
from metar_utils import fetch_metar_24h
from polymarket_client import prefetch_polymarket_event as _prefetch_polymarket_event
from polymarket_render_service import _build_polymarket_section
from realtime_pipeline import classify_window_phase
from report_render_service import choose_section_text
from station_catalog import (
    Station,
    direction_factor_for as _direction_factor_for,
    factor_summary_for as _factor_summary_for,
    site_tag_for as _site_tag_for,
    terrain_tag_for as _terrain_tag_for,
)
from synoptic_provider_router import DEFAULT_SYNOPTIC_PROVIDER, normalize_synoptic_provider
from temperature_shape_analysis import analyze_temperature_shape
from temperature_window_resolver import resolve_temperature_window


ROOT = Path(__file__).resolve().parent.parent
STATION_CSV = ROOT / "station_links.csv"
SCRIPTS_DIR = ROOT / "scripts"
CACHE_DIR = ROOT / "cache" / "runtime"
_POLY_PREFETCH_POOL = ThreadPoolExecutor(max_workers=2)
_METAR_FETCH_POOL = ThreadPoolExecutor(max_workers=4)


def _env_flag(name: str, default: str = "1") -> bool:
    raw = str(os.getenv(name, default) or default).strip().lower()
    return raw in {"1", "true", "yes", "on"}


LOOK_FORCE_LIVE_METAR = _env_flag("LOOK_FORCE_LIVE_METAR")
LOOK_FORCE_LIVE_POLYMARKET = _env_flag("LOOK_FORCE_LIVE_POLYMARKET")


@dataclass
class LookReportBundle:
    mode: str
    model: str
    now_utc: datetime
    now_local: datetime
    body: str
    footer: str
    metar24: list[dict[str, Any]] | None
    runtime_utc: str = ""
    provider_used: str = ""
    synoptic_provider_used: str = ""
    synoptic_runtime_used: str = ""
    synoptic_stream_used: str = ""
    synoptic_previous_runtime_used: str = ""
    compact_synoptic: bool = False
    forecast_quality: dict[str, Any] = field(default_factory=dict)
    synoptic_error: str | None = None


def _noop_perf_log(stage: str, seconds: float) -> None:
    _ = (stage, seconds)


def _sounding_model_for_provider(provider: str | None) -> str:
    txt = str(provider or "").strip().lower()
    if txt == "gfs-grib2":
        return "gfs"
    return "ecmwf"


def _attach_historical_context(
    metar_diag: dict[str, Any],
    *,
    station_icao: str,
    target_date: str,
    forecast_decision: dict[str, Any] | None,
    synoptic_context: dict[str, Any] | None = None,
) -> dict[str, Any] | None:
    if not historical_context_enabled():
        return None
    historical_context = build_historical_context(
        station_icao,
        target_date,
        metar_diag,
        forecast_decision=forecast_decision,
        synoptic_context=synoptic_context,
        site_tag=_site_tag_for(station_icao),
        terrain_tag=_terrain_tag_for(station_icao),
        direction_factor=_direction_factor_for(station_icao),
        factor_summary=_factor_summary_for(station_icao),
    )
    attach_historical_payload(metar_diag, historical_context)
    return historical_context


def _run_synoptic_section(
    *,
    station: Station,
    target_date: str,
    peak_local: str,
    tz_name: str,
    model: str,
    runtime_tag: str,
    provider: str,
    pass_mode: str,
    perf_log: Callable[[str, float], None],
) -> dict[str, Any]:
    from synoptic_runner import run_synoptic_section as _run

    return _run(
        st=station,
        target_date=target_date,
        peak_local=peak_local,
        tz_name=tz_name,
        model=model,
        runtime_tag=runtime_tag,
        scripts_dir=SCRIPTS_DIR,
        cache_dir=CACHE_DIR,
        provider=provider,
        pass_mode=pass_mode,
        perf_log=perf_log,
    )


def _fetch_mgm_reference(st: Station) -> dict[str, Any] | None:
    if str(st.icao).upper() != "LTAC":
        return None

    headers = {
        "User-Agent": "Mozilla/5.0",
        "Referer": "https://www.mgm.gov.tr/",
        "Origin": "https://www.mgm.gov.tr",
        "Accept": "application/json, text/plain, */*",
    }
    try:
        query = requests.utils.quote("esenboga")
        cands = requests.get(
            f"https://servis.mgm.gov.tr/web/merkezler?sorgu={query}",
            headers=headers,
            timeout=12,
        ).json()
        if not isinstance(cands, list) or not cands:
            return None
        center = cands[0]
        mid = center.get("merkezId")
        if mid is None:
            return None
        obs = requests.get(
            f"https://servis.mgm.gov.tr/web/sondurumlar?merkezid={mid}",
            headers=headers,
            timeout=12,
        ).json()
        if not isinstance(obs, list) or not obs:
            return None
        row = obs[0] or {}
        return {
            "merkez_id": mid,
            "veri_zamani": row.get("veriZamani"),
            "temp_c": row.get("sicaklik"),
            "rh": row.get("nem"),
            "wind_kmh": row.get("ruzgarHiz"),
            "wind_dir": row.get("ruzgarYon"),
            "metar": row.get("rasatMetar"),
            "ilce": center.get("ilce"),
        }
    except Exception:
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


def _render_mgm_reference_line(mgm_ref: dict[str, Any], unit_pref: str) -> str:
    def _fmt_temp_ref(v_c: Any) -> str:
        try:
            v = float(v_c)
        except Exception:
            return str(v_c)
        if unit_pref == "F":
            return f"{(v * 9.0 / 5.0 + 32.0):.1f}°F"
        return f"{v:.1f}°C"

    fields = []
    vz = str(mgm_ref.get("veri_zamani") or "")
    vz_txt = "--:-- Local"
    if vz:
        try:
            parsed = datetime.fromisoformat(vz.replace("Z", "+00:00"))
            if parsed.tzinfo is None:
                parsed = parsed.replace(tzinfo=timezone.utc)
            vz_txt = parsed.astimezone(ZoneInfo("Europe/Istanbul")).strftime("%H:%M Local")
        except Exception:
            if "T" in vz and len(vz) >= 16:
                vz_txt = vz[11:16] + " Local"
    fields.append(f"- MGM参考（{vz_txt}）")

    try:
        temp_c = float(mgm_ref.get("temp_c")) if mgm_ref.get("temp_c") is not None else None
    except Exception:
        temp_c = None
    if temp_c is not None:
        fields.append(f"气温={_fmt_temp_ref(temp_c)}")

    rh = mgm_ref.get("rh")
    if rh is not None:
        fields.append(f"湿度={rh}%")

    wind_dir = mgm_ref.get("wind_dir")
    if wind_dir not in (None, ""):
        try:
            deg = float(wind_dir)
            fields.append(f"风向={_wind_dir_text_cn(deg)}（{deg:.0f}°）")
        except Exception:
            fields.append(f"风向={wind_dir}")

    wind_kmh = mgm_ref.get("wind_kmh")
    if wind_kmh is not None:
        try:
            fields.append(f"风速={float(wind_kmh):.1f}km/h")
        except Exception:
            fields.append(f"风速={wind_kmh}km/h")

    return "，".join(fields)


def _is_openmeteo_rate_limited_error(exc: Exception) -> bool:
    msg = str(exc)
    return ("429" in msg) or ("Too Many Requests" in msg) or ("open-meteo breaker active" in msg)


def _render_footer(links: dict[str, Any]) -> str:
    return (
        f"🔗[Polymarket]({links['polymarket_event']}) | "
        f"[METAR]({links['metar_24h']}) | "
        f"[Wunderground]({links['wunderground']}) | "
        f"[探空图（Tropicaltidbits）]({links['sounding_tropicaltidbits']})"
    )


def _build_metar_only_bundle(
    *,
    station: Station,
    links_payload: dict[str, Any],
    reason: str,
    tz_name: str,
    model: str,
    metar24_prefetched: list[dict[str, Any]] | None = None,
) -> LookReportBundle:
    unit_pref = "F" if str(station.icao).upper().startswith("K") else "C"
    metar24 = metar24_prefetched if metar24_prefetched is not None else fetch_metar_24h(
        station.icao,
        force_refresh=LOOK_FORCE_LIVE_METAR,
    )
    metar_text, metar_diag = metar_observation_block(
        metar24,
        {"time": [], "temperature_2m": [], "pressure_msl": []},
        tz_name,
        temp_unit=unit_pref,
    )
    mgm_ref = _fetch_mgm_reference(station)
    if mgm_ref:
        metar_text = metar_text + "\n" + _render_mgm_reference_line(mgm_ref, unit_pref)

    now_utc = datetime.now(timezone.utc)
    now_local = now_utc.astimezone(ZoneInfo(tz_name))

    _attach_historical_context(
        metar_diag,
        station_icao=station.icao,
        target_date=now_local.strftime("%Y-%m-%d"),
        forecast_decision=None,
    )

    poly_block = ""
    try:
        event_url = str((links_payload.get("links") or {}).get("polymarket_event") or "")
        if event_url:
            prefetched = _prefetch_polymarket_event(event_url, force_refresh=LOOK_FORCE_LIVE_POLYMARKET)
            poly_block = _build_polymarket_section(
                event_url,
                {
                    "peak_temp_c": float(metar_diag.get("latest_temp") or 0.0),
                    "start_local": now_utc.strftime("%Y-%m-%dT%H:%M"),
                    "end_local": now_utc.strftime("%Y-%m-%dT%H:%M"),
                },
                weather_anchor={
                    "latest_temp_c": metar_diag.get("latest_temp"),
                    "observed_max_temp_c": metar_diag.get("observed_max_temp_c"),
                },
                prefetched_event=prefetched,
            )
            if str(poly_block).startswith("Polymarket："):
                poly_block = ""
    except Exception:
        poly_block = ""

    body = (
        "📡 **最新实况分析（METAR-only 降级）**\n"
        f"- 触发原因：{reason}\n"
        "- 说明：Open-Meteo 当前不可用，已降级为实况-only 输出；背景/窗口判断暂不展开。\n\n"
        f"{metar_text}"
    )
    if poly_block:
        body = f"{body}\n\n{poly_block}"

    return LookReportBundle(
        mode="metar_only",
        model=model,
        now_utc=now_utc,
        now_local=now_local,
        body=body,
        footer=_render_footer(links_payload["links"]),
        metar24=metar24,
        runtime_utc=str(links_payload.get("runtime_utc") or ""),
    )


def build_look_report_bundle(
    *,
    station: Station,
    target_date: str,
    model: str,
    tz_name_station: str,
    perf_log: Callable[[str, float], None] | None = None,
) -> LookReportBundle:
    log = perf_log or _noop_perf_log
    provider = "auto"
    metar_future = _METAR_FETCH_POOL.submit(fetch_metar_24h, station.icao, force_refresh=LOOK_FORCE_LIVE_METAR)

    t0 = time.perf_counter()
    provider_used = "unknown"
    try:
        om, provider_used = fetch_hourly_router(station, target_date, model, provider=provider)
    except Exception as exc:
        if _is_openmeteo_rate_limited_error(exc) or "gfs" in str(exc).lower() or "429" in str(exc):
            fallback_valid_utc = datetime.strptime(target_date, "%Y-%m-%d").replace(tzinfo=timezone.utc) + timedelta(hours=12)
            degrade_model = "gfs" if provider in {"auto", "gfs", "gfs-grib2", "grib2"} else model
            links_payload = BSL.build_links(
                row=BSL.load_station(STATION_CSV, station.icao),
                model=degrade_model,
                now_utc=datetime.now(timezone.utc),
                target_valid_utc=fallback_valid_utc,
                target_date_utc=datetime.strptime(target_date, "%Y-%m-%d").replace(tzinfo=timezone.utc),
            )
            log("hourly_fetch", time.perf_counter() - t0)
            metar_prefetched: list[dict[str, Any]] | None = None
            try:
                metar_prefetched = metar_future.result(timeout=0.2)
            except Exception:
                metar_prefetched = None
            return _build_metar_only_bundle(
                station=station,
                links_payload=links_payload,
                reason=f"{provider} provider degraded: {exc}",
                tz_name=tz_name_station,
                model=degrade_model,
                metar24_prefetched=metar_prefetched,
            )
        raise
    log("hourly_fetch", time.perf_counter() - t0)

    synoptic_provider = normalize_synoptic_provider(os.getenv("FORECAST_3D_PROVIDER", DEFAULT_SYNOPTIC_PROVIDER))
    tz_name = tz_name_station or om.get("timezone", "UTC")
    hourly_day = slice_hourly_local_day(om["hourly"], target_date)
    tz = ZoneInfo(tz_name)
    unit_pref = "F" if str(station.icao).upper().startswith("K") else "C"

    t0 = time.perf_counter()
    metar24 = metar_future.result(timeout=45)
    metar_text, metar_diag = metar_observation_block(
        metar24,
        hourly_day,
        tz_name,
        target_date=target_date,
        temp_unit=unit_pref,
    )
    mgm_ref = _fetch_mgm_reference(station)
    if mgm_ref:
        metar_text = metar_text + "\n" + _render_mgm_reference_line(mgm_ref, unit_pref)
        metar_diag["mgm_reference"] = mgm_ref
    metar_diag["station_icao"] = str(station.icao).upper()
    try:
        metar_diag["station_lat"] = float(station.lat)
        metar_diag["station_lon"] = float(station.lon)
    except Exception:
        pass
    log("metar_fetch_parse", time.perf_counter() - t0)

    temp_shape_analysis = analyze_temperature_shape(
        hourly_day,
        metar_diag=metar_diag,
        station_icao=station.icao,
    )
    _windows, primary, _peak_candidates = detect_tmax_windows(
        hourly_day,
        temp_shape_analysis=temp_shape_analysis,
    )
    if not primary:
        raise RuntimeError("No Tmax window detected from forecast hourly data")

    window_resolution = resolve_temperature_window(
        primary,
        hourly_day,
        metar_diag,
        station_icao=station.icao,
        temp_shape_analysis=temp_shape_analysis,
    )
    primary = dict(window_resolution.get("resolved_window") or primary)
    metar_diag["analysis_window_override_active"] = bool(window_resolution.get("override_active"))
    metar_diag["analysis_window_mode"] = str(window_resolution.get("mode") or "forecast_primary")
    metar_diag["analysis_window_reason_codes"] = list(window_resolution.get("reason_codes") or [])
    if str(window_resolution.get("mode") or "") == "obs_peak_reanchor":
        metar_diag["obs_correction_applied"] = True

    analysis_window = dict(primary)
    try:
        gate_pre = classify_window_phase(primary, metar_diag)
        if str(gate_pre.get("phase") or "") == "post":
            post_focus = _build_post_focus_window(hourly_day, metar_diag)
            if isinstance(post_focus, dict) and post_focus.get("peak_local"):
                analysis_window = post_focus
                metar_diag["post_focus_window_active"] = True
                metar_diag["post_window_mode"] = "focus_rebreak"
                metar_diag["post_focus_peak_local"] = str(post_focus.get("peak_local") or "")
                try:
                    post_focus_peak = float(post_focus.get("peak_temp_c"))
                    metar_diag["post_focus_peak_temp_c"] = post_focus_peak
                    try:
                        obs_max = float(metar_diag.get("observed_max_temp_c")) if metar_diag.get("observed_max_temp_c") is not None else None
                    except Exception:
                        obs_max = None
                    if obs_max is not None and post_focus_peak <= obs_max - 0.4:
                        metar_diag["post_window_mode"] = "no_rebreak_eval"
                except Exception:
                    pass
            else:
                post_eval = _build_post_eval_window(hourly_day, metar_diag)
                if isinstance(post_eval, dict) and post_eval.get("peak_local"):
                    analysis_window = post_eval
                    metar_diag["post_focus_window_active"] = True
                    metar_diag["post_window_mode"] = "no_rebreak_eval"
    except Exception:
        analysis_window = dict(primary)

    peak_local_dt = datetime.strptime(analysis_window["peak_local"], "%Y-%m-%dT%H:%M").replace(tzinfo=tz)
    peak_utc = peak_local_dt.astimezone(timezone.utc)
    now_for_links_utc = datetime.now(timezone.utc)
    window_candidate_utc: list[datetime] = []
    for key in ("start_local", "peak_local", "end_local"):
        raw = str(analysis_window.get(key) or "").strip()
        if not raw:
            continue
        try:
            dt_local = datetime.strptime(raw, "%Y-%m-%dT%H:%M").replace(tzinfo=tz)
            window_candidate_utc.append(dt_local.astimezone(timezone.utc))
        except Exception:
            continue
    sounding_target_utc = (
        min(window_candidate_utc, key=lambda value: abs((value - now_for_links_utc).total_seconds()))
        if window_candidate_utc
        else peak_utc
    )

    link_model = "gfs" if synoptic_provider == "gfs-grib2" else model
    links_payload = BSL.build_links(
        row=BSL.load_station(STATION_CSV, station.icao),
        model=link_model,
        now_utc=now_for_links_utc,
        target_valid_utc=peak_utc,
        sounding_target_valid_utc=sounding_target_utc,
        target_date_utc=datetime.strptime(target_date, "%Y-%m-%d").replace(tzinfo=timezone.utc),
        sounding_model=_sounding_model_for_provider(synoptic_provider),
    )

    poly_prefetch_future = None
    try:
        event_url = str((links_payload.get("links") or {}).get("polymarket_event") or "")
        if event_url:
            poly_prefetch_future = _POLY_PREFETCH_POOL.submit(
                _prefetch_polymarket_event,
                event_url,
                force_refresh=LOOK_FORCE_LIVE_POLYMARKET,
            )
    except Exception:
        poly_prefetch_future = None

    now_utc = datetime.now(timezone.utc)
    now_local = now_utc.astimezone(tz)

    def _run_synoptic_fn(
        st: Station,
        date: str,
        peak_local: str,
        tz_name_value: str,
        model_value: str,
        runtime_tag: str,
        pass_mode: str = "full",
    ) -> dict[str, Any]:
        return _run_synoptic_section(
            station=st,
            target_date=date,
            peak_local=peak_local,
            tz_name=tz_name_value,
            model=model_value,
            runtime_tag=runtime_tag,
            provider=synoptic_provider,
            pass_mode=pass_mode,
            perf_log=log,
        )

    t0 = time.perf_counter()
    forecast_decision, _synoptic, synoptic_error = load_or_build_forecast_decision(
        station=station,
        target_date=target_date,
        model=model,
        synoptic_provider=synoptic_provider,
        now_utc=now_utc,
        now_local=now_local,
        station_lat=station.lat,
        station_lon=station.lon,
        primary_window=analysis_window,
        tz_name=tz_name,
        run_synoptic_fn=_run_synoptic_fn,
        perf_log=log,
    )
    log("forecast_pipeline", time.perf_counter() - t0)

    forecast_quality = (forecast_decision.get("quality") or {}) if isinstance(forecast_decision, dict) else {}
    synoptic_provider_used = str(forecast_quality.get("synoptic_provider_used") or synoptic_provider)
    synoptic_runtime_used = str(forecast_quality.get("synoptic_analysis_runtime_used") or "")
    synoptic_stream_used = str(forecast_quality.get("synoptic_analysis_stream_used") or "")
    synoptic_previous_runtime_used = str(forecast_quality.get("synoptic_previous_runtime_used") or "")
    sounding_model_used = _sounding_model_for_provider(synoptic_provider_used)
    if str(links_payload.get("sounding_model") or "") != sounding_model_used:
        links_payload = BSL.build_links(
            row=BSL.load_station(STATION_CSV, station.icao),
            model=link_model,
            now_utc=now_for_links_utc,
            target_valid_utc=peak_utc,
            sounding_target_valid_utc=sounding_target_utc,
            target_date_utc=datetime.strptime(target_date, "%Y-%m-%d").replace(tzinfo=timezone.utc),
            sounding_model=sounding_model_used,
        )

    gate_now = classify_window_phase(primary, metar_diag)
    compact_synoptic = str(gate_now.get("phase") or "unknown") in {"near_window", "in_window"}

    poly_prefetched_event = None
    if poly_prefetch_future is not None:
        try:
            poly_prefetched_event = poly_prefetch_future.result(timeout=0.05)
        except FuturesTimeoutError:
            poly_prefetched_event = None
        except Exception:
            poly_prefetched_event = None

    _attach_historical_context(
        metar_diag,
        station_icao=station.icao,
        target_date=target_date,
        forecast_decision=forecast_decision,
    )

    analysis_snapshot = build_analysis_snapshot(
        primary_window=primary,
        metar_diag=metar_diag,
        forecast_decision=forecast_decision,
        temp_unit=unit_pref,
        synoptic_window=analysis_window,
        temp_shape_analysis=temp_shape_analysis,
    )

    t0 = time.perf_counter()
    body = choose_section_text(
        primary,
        metar_text,
        metar_diag,
        links_payload["links"]["polymarket_event"],
        forecast_decision=forecast_decision,
        compact_synoptic=compact_synoptic,
        temp_unit=unit_pref,
        synoptic_window=analysis_window,
        polymarket_prefetched_event=poly_prefetched_event,
        temp_shape_analysis=temp_shape_analysis,
        analysis_snapshot=analysis_snapshot,
    )
    log("render_body", time.perf_counter() - t0)

    return LookReportBundle(
        mode="full",
        model=model,
        now_utc=now_utc,
        now_local=now_local,
        body=body,
        footer=_render_footer(links_payload["links"]),
        metar24=metar24,
        runtime_utc=str(links_payload.get("runtime_utc") or ""),
        provider_used=provider_used,
        synoptic_provider_used=synoptic_provider_used,
        synoptic_runtime_used=synoptic_runtime_used,
        synoptic_stream_used=synoptic_stream_used,
        synoptic_previous_runtime_used=synoptic_previous_runtime_used,
        compact_synoptic=compact_synoptic,
        forecast_quality=forecast_quality,
        synoptic_error=str(synoptic_error or "") or None,
    )
