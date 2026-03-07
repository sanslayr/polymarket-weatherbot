#!/usr/bin/env python3
"""Telegram command entrypoint for city Tmax report.

Examples:
  /look Ankara
  /look city=Ankara date=2026-03-03
  /look icao=LTAC model=ecmwf
  /look city=Toronto model=gfs
  /look Ankara modify model gfs date 2026-03-04
"""

from __future__ import annotations

import argparse
import os
import time
from concurrent.futures import ThreadPoolExecutor, TimeoutError as FuturesTimeoutError
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

import requests

PROCESS_T0 = time.perf_counter()

from forecast_pipeline import load_or_build_forecast_decision
from realtime_pipeline import classify_window_phase
from look_change_guard import build_cached_result_meta, build_unchanged_notice
from look_command import parse_telegram_command, render_look_help
from look_runtime_control import LookRuntimeContext, LookRuntimeController, build_request_key
import build_station_links as BSL
from hourly_data_service import (
    build_post_eval_window as _build_post_eval_window,
    build_post_focus_window as _build_post_focus_window,
    detect_tmax_windows,
    fetch_hourly_router,
    openmeteo_breaker_info as _openmeteo_breaker_info,
    prune_runtime_cache as _prune_runtime_cache,
    slice_hourly_local_day,
)
from metar_utils import fetch_metar_24h
from metar_analysis_service import metar_observation_block
from historical_context_provider import (
    build_historical_context,
    historical_context_enabled,
)
from historical_payload import attach_historical_payload
from polymarket_render_service import _build_polymarket_section
from report_render_service import choose_section_text
from polymarket_client import prefetch_polymarket_event as _prefetch_polymarket_event
from station_catalog import (
    Station,
    default_model_for_station,
    direction_factor_for as _direction_factor_for,
    factor_summary_for as _factor_summary_for,
    format_utc_offset,
    resolve_station,
    site_tag_for as _site_tag_for,
    station_timezone_name,
    terrain_tag_for as _terrain_tag_for,
)

ROOT = Path(__file__).resolve().parent.parent
STATION_CSV = ROOT / "station_links.csv"
SCRIPTS_DIR = ROOT / "scripts"

CACHE_DIR = ROOT / "cache" / "runtime"
PERF_LOG_ENABLED = os.getenv("LOOK_PERF_LOG", "0") == "1"
SYNOPTIC_PROVIDER = "gfs-grib2"
_POLY_PREFETCH_POOL = ThreadPoolExecutor(max_workers=2)
_METAR_FETCH_POOL = ThreadPoolExecutor(max_workers=4)
LOOK_FORCE_LIVE_METAR = str(os.getenv("LOOK_FORCE_LIVE_METAR", "1") or "1").strip().lower() in {"1", "true", "yes", "on"}
LOOK_FORCE_LIVE_POLYMARKET = str(os.getenv("LOOK_FORCE_LIVE_POLYMARKET", "1") or "1").strip().lower() in {"1", "true", "yes", "on"}


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


def _perf_log(stage: str, seconds: float) -> None:
    _ = (stage, seconds)
    return




def run_synoptic_section(
    st: Station,
    target_date: str,
    peak_local: str,
    tz_name: str,
    model: str,
    runtime_tag: str,
    pass_mode: str = "full",
) -> dict[str, Any]:
    from synoptic_runner import run_synoptic_section as _run

    return _run(
        st=st,
        target_date=target_date,
        peak_local=peak_local,
        tz_name=tz_name,
        model=model,
        runtime_tag=runtime_tag,
        scripts_dir=SCRIPTS_DIR,
        cache_dir=CACHE_DIR,
        provider=SYNOPTIC_PROVIDER,
        pass_mode=pass_mode,
        perf_log=_perf_log,
    )




def _fetch_mgm_reference(st: Station) -> dict[str, Any] | None:
    """Fetch MGM real-time reference for Ankara/Esenboğa (LTAC).

    MGM endpoint blocks requests without browser-like headers.
    """
    if str(st.icao).upper() != "LTAC":
        return None

    headers = {
        "User-Agent": "Mozilla/5.0",
        "Referer": "https://www.mgm.gov.tr/",
        "Origin": "https://www.mgm.gov.tr",
        "Accept": "application/json, text/plain, */*",
    }
    try:
        q = requests.utils.quote("esenboga")
        cands = requests.get(
            f"https://servis.mgm.gov.tr/web/merkezler?sorgu={q}",
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


def _is_openmeteo_rate_limited_error(exc: Exception) -> bool:
    msg = str(exc)
    return ("429" in msg) or ("Too Many Requests" in msg) or ("open-meteo breaker active" in msg)


def _render_metar_only_report(
    st: Station,
    model: str,
    links_payload: dict[str, Any],
    reason: str,
    tz_name: str,
    metar24_prefetched: list[dict[str, Any]] | None = None,
) -> str:
    unit_pref = "F" if str(st.icao).upper().startswith("K") else "C"
    metar24 = metar24_prefetched if metar24_prefetched is not None else fetch_metar_24h(st.icao, force_refresh=LOOK_FORCE_LIVE_METAR)
    metar_text, _metar_diag = metar_observation_block(
        metar24,
        {"time": [], "temperature_2m": [], "pressure_msl": []},
        tz_name,
        temp_unit=unit_pref,
    )
    mgm_ref = _fetch_mgm_reference(st) if str(st.icao).upper() == "LTAC" else None
    if mgm_ref:
        def _fmt_temp_ref(v_c: Any) -> str:
            try:
                v = float(v_c)
            except Exception:
                return str(v_c)
            if unit_pref == "F":
                return f"{(v * 9.0 / 5.0 + 32.0):.1f}°F"
            return f"{v:.1f}°C"
        try:
            t = float(mgm_ref.get("temp_c")) if mgm_ref.get("temp_c") is not None else None
        except Exception:
            t = None
        vz = str(mgm_ref.get("veri_zamani") or "")
        vz_txt = vz[11:16] + "Z" if ("T" in vz and len(vz) >= 16) else "--:--Z"
        bits = [f"- MGM参考（{vz_txt}）"]
        if t is not None:
            bits.append(f"T={_fmt_temp_ref(t)}")
        if mgm_ref.get("rh") is not None:
            bits.append(f"RH={mgm_ref.get('rh')}%")
        metar_text = metar_text + "\n" + "，".join(bits)

    lat_hemi = "N" if st.lat >= 0 else "S"
    lon_hemi = "E" if st.lon >= 0 else "W"
    rt = str(links_payload.get("runtime_utc") or "")
    rt_fmt = rt
    if len(rt) == 10 and rt.isdigit():
        rt_fmt = f"{rt[0:4]}/{rt[4:6]}/{rt[6:8]} {rt[8:10]}Z"

    now_utc = datetime.now(timezone.utc)
    tz = ZoneInfo(tz_name)
    now_local = now_utc.astimezone(tz)
    header = (
        f"📍 **{st.icao} ({st.city}) | {abs(st.lat):.4f}{lat_hemi}, {abs(st.lon):.4f}{lon_hemi}**\n"
        f"判断时间: {now_utc.strftime('%Y-%m-%d %H:%M')} UTC | {now_local.strftime('%Y-%m-%d %H:%M')} Local ({format_utc_offset(now_local)})\n"
        f"分析基准模型: {model.upper()}（运行时次: {rt_fmt}）\n"
        "**🦞龙虾学习中，不提供交易建议🦞**"
    )

    pseudo_peak = float(_metar_diag.get("latest_temp") or 0.0)
    pseudo_window = {
        "peak_temp_c": pseudo_peak,
        "start_local": now_utc.strftime("%Y-%m-%dT%H:%M"),
        "end_local": now_utc.strftime("%Y-%m-%dT%H:%M"),
    }
    poly_block = ""
    try:
        purl = links_payload["links"]["polymarket_event"]
        prefetched = _prefetch_polymarket_event(purl, force_refresh=LOOK_FORCE_LIVE_POLYMARKET)
        poly_block = _build_polymarket_section(
            purl,
            pseudo_window,
            weather_anchor={
                "latest_temp_c": _metar_diag.get("latest_temp"),
                "observed_max_temp_c": _metar_diag.get("observed_max_temp_c"),
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
    historical_context = _attach_historical_context(
        _metar_diag,
        station_icao=st.icao,
        target_date=datetime.now(timezone.utc).astimezone(tz).strftime("%Y-%m-%d"),
        forecast_decision=None,
    )
    if poly_block:
        body += f"\n\n{poly_block}"

    links = links_payload["links"]
    footer = (
        f"🔗[Polymarket]({links['polymarket_event']}) | "
        f"[METAR]({links['metar_24h']}) | "
        f"[Wunderground]({links['wunderground']}) | "
        f"[探空图（Tropicaltidbits）]({links['sounding_tropicaltidbits']})"
    )
    return f"{header}\n\n{body}\n{footer}"


def render_report(
    command_text: str,
    *,
    channel: str | None = None,
    peer_kind: str | None = None,
    peer_id: str | None = None,
    sender_id: str | None = None,
    session_key: str | None = None,
) -> str:
    req_start_utc = datetime.now(timezone.utc)
    t_e2e = time.perf_counter()
    bootstrap_elapsed = max(0.0, t_e2e - PROCESS_T0)
    perf_local: dict[str, float] = {}

    def _mark(stage: str, seconds: float) -> None:
        perf_local[stage] = float(seconds)
        _perf_log(stage, seconds)

    _prune_runtime_cache()
    params = parse_telegram_command(command_text)
    if params.get("cmd") != "look":
        raise ValueError("Unsupported command. Use /look")

    station_hint = params.get("station")
    if not station_hint:
        return render_look_help()
    if str(station_hint).strip().lower() in {"help", "帮助", "h"}:
        return render_look_help()

    try:
        st = resolve_station(station_hint)
    except ValueError as exc:
        return f"{exc}\n\n{render_look_help()}"
    tz_name_station = station_timezone_name(st)

    raw_target_date = params.get("date")
    if raw_target_date and len(raw_target_date) == 8 and raw_target_date.isdigit():
        raw_target_date = f"{raw_target_date[0:4]}-{raw_target_date[4:6]}-{raw_target_date[6:8]}"

    now_utc = datetime.now(timezone.utc)
    if raw_target_date:
        target_date = raw_target_date
    else:
        try:
            target_date = now_utc.astimezone(ZoneInfo(tz_name_station)).strftime("%Y-%m-%d")
        except Exception:
            target_date = now_utc.strftime("%Y-%m-%d")

    try:
        datetime.strptime(target_date, "%Y-%m-%d")
    except ValueError as exc:
        raise ValueError("date must be YYYY-MM-DD (or YYYYMMDD)") from exc

    runtime_context = LookRuntimeContext.from_runtime(
        channel=channel,
        peer_kind=peer_kind,
        peer_id=peer_id,
        sender_id=sender_id,
        session_key=session_key,
    )
    runtime_control = LookRuntimeController(
        context=runtime_context,
        compute_key=build_request_key(station_icao=st.icao, target_date=target_date),
        query_label=f"{st.city}({st.icao})-{target_date.replace('-', '')}",
    )
    cached_payload = runtime_control.peek_latest_result_payload()
    if cached_payload:
        unchanged_notice = build_unchanged_notice(
            query_label=f"{st.city}({st.icao})-{target_date.replace('-', '')}",
            icao=st.icao,
            model=default_model_for_station(st),
            cached_payload=cached_payload,
        )
        if unchanged_notice:
            return unchanged_notice
    preflight = runtime_control.preflight()
    if not preflight.proceed:
        return str(preflight.text or "")

    # 统一输出：固定走简版主报告（不再支持 mode/section/model/provider 参数）。
    try:
        model = default_model_for_station(st).lower()
        if model not in {"gfs", "ecmwf"}:
            model = "gfs"
        provider = "auto"
        metar_future = _METAR_FETCH_POOL.submit(fetch_metar_24h, st.icao, force_refresh=LOOK_FORCE_LIVE_METAR)

        t0 = time.perf_counter()
        provider_used = "unknown"
        try:
            om, provider_used = fetch_hourly_router(st, target_date, model, provider=provider)
        except Exception as exc:
            # open-meteo rate-limit OR gfs provider unavailable/rate-limited => degrade to METAR-only
            if _is_openmeteo_rate_limited_error(exc) or "gfs" in str(exc).lower() or "429" in str(exc):
                fallback_valid_utc = datetime.strptime(target_date, "%Y-%m-%d").replace(tzinfo=timezone.utc) + timedelta(hours=12)
                degrade_model = "gfs" if provider in {"auto", "gfs", "gfs-grib2", "grib2"} else model
                links_payload = BSL.build_links(
                    row=BSL.load_station(STATION_CSV, st.icao),
                    model=degrade_model,
                    now_utc=datetime.now(timezone.utc),
                    target_valid_utc=fallback_valid_utc,
                    target_date_utc=datetime.strptime(target_date, "%Y-%m-%d").replace(tzinfo=timezone.utc),
                )
                _mark("hourly_fetch", time.perf_counter() - t0)
                metar_prefetched: list[dict[str, Any]] | None = None
                try:
                    metar_prefetched = metar_future.result(timeout=0.2)
                except Exception:
                    metar_prefetched = None
                result_text = _render_metar_only_report(
                    st,
                    degrade_model,
                    links_payload,
                    reason=f"{provider} provider degraded: {exc}",
                    tz_name=tz_name_station,
                    metar24_prefetched=metar_prefetched,
                )
                runtime_control.success(
                    result_text,
                    result_meta=build_cached_result_meta(
                        icao=st.icao,
                        model=degrade_model,
                        metar24=metar_prefetched,
                    ),
                )
                return result_text
            raise
        _mark("hourly_fetch", time.perf_counter() - t0)

        global SYNOPTIC_PROVIDER
        # Strategy: hourly forecast prefers open-meteo; 3D field prefers gfs-grib2 by default.
        pref_3d = (os.getenv("FORECAST_3D_PROVIDER", "gfs-grib2") or "gfs-grib2").strip().lower()
        SYNOPTIC_PROVIDER = "gfs-grib2" if pref_3d in {"gfs", "gfs-grib2", "grib2"} else provider_used

        tz_name = tz_name_station or om.get("timezone", "UTC")
        hourly_day = slice_hourly_local_day(om["hourly"], target_date)
        windows, primary, peak_candidates = detect_tmax_windows(hourly_day)
        if not primary:
            raise RuntimeError("No Tmax window detected from forecast hourly data")

        tz = ZoneInfo(tz_name)
        unit_pref = "F" if str(st.icao).upper().startswith("K") else "C"

        t0 = time.perf_counter()
        metar24 = metar_future.result(timeout=45)
        metar_text, metar_diag = metar_observation_block(
            metar24,
            hourly_day,
            tz_name,
            target_date=target_date,
            temp_unit=unit_pref,
        )
        mgm_ref = _fetch_mgm_reference(st) if str(st.icao).upper() == "LTAC" else None
        if mgm_ref:
            def _fmt_temp_ref(v_c: Any) -> str:
                try:
                    v = float(v_c)
                except Exception:
                    return str(v_c)
                if unit_pref == "F":
                    return f"{(v * 9.0 / 5.0 + 32.0):.1f}°F"
                return f"{v:.1f}°C"
            try:
                t = float(mgm_ref.get("temp_c")) if mgm_ref.get("temp_c") is not None else None
            except Exception:
                t = None
            vz = str(mgm_ref.get("veri_zamani") or "")
            vz_txt = vz[11:16] + "Z" if ("T" in vz and len(vz) >= 16) else "--:--Z"
            extra = [f"- MGM参考（{vz_txt}）"]
            if t is not None:
                extra.append(f"T={_fmt_temp_ref(t)}")
            rh = mgm_ref.get("rh")
            if rh is not None:
                extra.append(f"RH={rh}%")
            w = mgm_ref.get("wind_kmh")
            if w is not None:
                try:
                    extra.append(f"Wind={float(w):.1f}km/h")
                except Exception:
                    pass
            metar_text = metar_text + "\n" + "，".join(extra)
            metar_diag["mgm_reference"] = mgm_ref
        try:
            metar_diag["station_lat"] = float(st.lat)
            metar_diag["station_lon"] = float(st.lon)
        except Exception:
            pass
        _mark("metar_fetch_parse", time.perf_counter() - t0)

        # 实况纠偏：当实况显著高于模型峰值时，优先以实况峰值时段重锚窗口。
        try:
            obs_max = float(metar_diag.get("observed_max_temp_c")) if metar_diag.get("observed_max_temp_c") is not None else None
        except Exception:
            obs_max = None
        try:
            model_peak = float(primary.get("peak_temp_c"))
        except Exception:
            model_peak = None

        if obs_max is not None and model_peak is not None and (obs_max - model_peak) >= 1.5:
            tmax_local_s = str(metar_diag.get("observed_max_time_local") or metar_diag.get("latest_report_local") or "")
            try:
                tmax_local = datetime.fromisoformat(tmax_local_s)
            except Exception:
                tmax_local = datetime.now(tz)
            sdt = tmax_local - timedelta(hours=1)
            edt = tmax_local + timedelta(hours=2)
            primary["start_local"] = sdt.strftime("%Y-%m-%dT%H:%M")
            primary["end_local"] = edt.strftime("%Y-%m-%dT%H:%M")
            primary["peak_local"] = tmax_local.strftime("%Y-%m-%dT%H:%M")
            primary["peak_temp_c"] = max(model_peak, obs_max)
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
                        pf = float(post_focus.get("peak_temp_c"))
                        metar_diag["post_focus_peak_temp_c"] = pf
                        try:
                            obs_mx = float(metar_diag.get("observed_max_temp_c")) if metar_diag.get("observed_max_temp_c") is not None else None
                        except Exception:
                            obs_mx = None
                        if obs_mx is not None and pf <= obs_mx - 0.4:
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
        for k in ("start_local", "peak_local", "end_local"):
            raw = str(analysis_window.get(k) or "").strip()
            if not raw:
                continue
            try:
                dt_local = datetime.strptime(raw, "%Y-%m-%dT%H:%M").replace(tzinfo=tz)
                window_candidate_utc.append(dt_local.astimezone(timezone.utc))
            except Exception:
                continue
        sounding_target_utc = (
            min(window_candidate_utc, key=lambda t: abs((t - now_for_links_utc).total_seconds()))
            if window_candidate_utc
            else peak_utc
        )

        analysis_model = model
        link_model = "gfs" if SYNOPTIC_PROVIDER == "gfs-grib2" else analysis_model
        links_payload = BSL.build_links(
            row=BSL.load_station(STATION_CSV, st.icao),
            model=link_model,
            now_utc=now_for_links_utc,
            target_valid_utc=peak_utc,
            sounding_target_valid_utc=sounding_target_utc,
            target_date_utc=datetime.strptime(target_date, "%Y-%m-%d").replace(tzinfo=timezone.utc),
        )

        poly_prefetch_future = None
        try:
            purl = str((links_payload.get("links") or {}).get("polymarket_event") or "")
            if purl:
                poly_prefetch_future = _POLY_PREFETCH_POOL.submit(
                    _prefetch_polymarket_event,
                    purl,
                    force_refresh=LOOK_FORCE_LIVE_POLYMARKET,
                )
        except Exception:
            poly_prefetch_future = None

        now_utc = datetime.now(timezone.utc)
        now_local = now_utc.astimezone(tz)

        t0 = time.perf_counter()
        forecast_decision, _synoptic, _synoptic_error = load_or_build_forecast_decision(
            station=st,
            target_date=target_date,
            model=model,
            synoptic_provider=SYNOPTIC_PROVIDER,
            now_utc=now_utc,
            now_local=now_local,
            station_lat=st.lat,
            station_lon=st.lon,
            primary_window=analysis_window,
            tz_name=tz_name,
            run_synoptic_fn=run_synoptic_section,
            perf_log=_mark,
        )
        forecast_elapsed = time.perf_counter() - t0
        _mark("forecast_pipeline", forecast_elapsed)
        lat_hemi = "N" if st.lat >= 0 else "S"
        lon_hemi = "E" if st.lon >= 0 else "W"
        rt = str(links_payload.get("runtime_utc") or "")
        rt_fmt = rt
        if len(rt) == 10 and rt.isdigit():
            rt_fmt = f"{rt[0:4]}/{rt[4:6]}/{rt[6:8]} {rt[8:10]}Z"

        terrain_tag = _terrain_tag_for(st.icao)
        site_tag = _site_tag_for(st.icao)
        direction_factor = _direction_factor_for(st.icao)
        head_geo = f"{abs(st.lat):.4f}{lat_hemi}, {abs(st.lon):.4f}{lon_hemi}"
        if site_tag:
            head_geo = f"{head_geo} ({site_tag})"
        elif terrain_tag:
            head_geo = f"{head_geo} ({terrain_tag})"

        gate_now = classify_window_phase(primary, metar_diag)
        phase_now = str(gate_now.get("phase") or "unknown")
        compact_synoptic = phase_now in {"near_window", "in_window"}

        header_lines = [
            f"📍 **{st.icao} ({st.city}) | {head_geo}**",
            f"生成时间: {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M')} UTC | {now_local.strftime('%H:%M')} Local (UTC{now_local.strftime('%z')[:3]})",
        ]
        if not compact_synoptic:
            header_lines.append(
                f"分析基准模型: {analysis_model.upper()}（运行时次: {rt_fmt}） | 小时预报源: {provider_used} | 3D场源: {SYNOPTIC_PROVIDER}"
            )
            if direction_factor:
                header_lines.append(f"方位因子: {direction_factor}")

        try:
            quality = (forecast_decision.get("quality") or {}) if isinstance(forecast_decision, dict) else {}
            missing = set(quality.get("missing_layers") or [])
            degraded = str(quality.get("source_state") or "") == "degraded"
            syn_fail = ("synoptic" in missing) or degraded
            err_txt = str(_synoptic_error or "")
            breaker_active, breaker_until, breaker_reason = _openmeteo_breaker_info()
            rate_limited = (
                ("429" in err_txt)
                or ("Too Many Requests" in err_txt)
                or ("breaker active" in err_txt)
                or breaker_active
                or (breaker_active and (("429" in str(breaker_reason)) or ("grid_429" in str(breaker_reason))))
            )
            if syn_fail and rate_limited:
                if breaker_until is not None:
                    header_lines.append(
                        f"⚠️ 数据提醒：Open-Meteo 请求过多（429），环流层已降级；预计 {breaker_until.strftime('%H:%M:%S')} UTC 后恢复"
                    )
                else:
                    header_lines.append("⚠️ 数据提醒：Open-Meteo 请求过多（429），环流层已降级。")
        except Exception:
            pass

        header_lines.append("**🦞龙虾学习中，不提供交易建议🦞**")

        header = "\n".join(header_lines)

        poly_prefetched_event = None
        if poly_prefetch_future is not None:
            try:
                poly_prefetched_event = poly_prefetch_future.result(timeout=0.05)
            except FuturesTimeoutError:
                poly_prefetched_event = None
            except Exception:
                poly_prefetched_event = None

        historical_context = _attach_historical_context(
            metar_diag,
            station_icao=st.icao,
            target_date=target_date,
            forecast_decision=forecast_decision,
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
        )
        _mark("render_body", time.perf_counter() - t0)
        total_elapsed = time.perf_counter() - t_e2e

        links = links_payload["links"]
        footer = (
            f"🔗[Polymarket]({links['polymarket_event']}) | "
            f"[METAR]({links['metar_24h']}) | "
            f"[Wunderground]({links['wunderground']}) | "
            f"[探空图（Tropicaltidbits）]({links['sounding_tropicaltidbits']})"
        )

        show_perf = str(os.getenv("LOOK_SHOW_PERF", "0") or "0").strip().lower() in {"1", "true", "yes", "on"}
        if show_perf:
            perf_line = (
                f"⏱️ 模块耗时: total {total_elapsed:.2f}s | bootstrap {bootstrap_elapsed:.2f}s | process {total_elapsed + bootstrap_elapsed:.2f}s"
                f" | hourly {float(perf_local.get('hourly_fetch', 0.0) or 0.0):.2f}s"
                f" | metar {float(perf_local.get('metar_fetch_parse', 0.0) or 0.0):.2f}s"
                f" | forecast {float(perf_local.get('forecast_pipeline', 0.0) or 0.0):.2f}s"
                f" (syn {float(perf_local.get('forecast.synoptic_build', 0.0) or 0.0):.2f}s"
                f", dec {float(perf_local.get('forecast.decision_build', 0.0) or 0.0):.2f}s"
                f", cacheR {float(perf_local.get('forecast.cache_read', 0.0) or 0.0):.2f}s"
                f", cacheW {float(perf_local.get('forecast.cache_write', 0.0) or 0.0):.2f}s)"
                f" | render {float(perf_local.get('render_body', 0.0) or 0.0):.2f}s"
            )
            header = f"{header}\n{perf_line}"

        result_text = f"{header}\n\n{body}\n{footer}"
        runtime_control.success(
            result_text,
            result_meta=build_cached_result_meta(
                icao=st.icao,
                model=model,
                metar24=metar24,
            ),
        )
        return result_text
    except Exception:
        runtime_control.failure()
        raise


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="Generate report text from Telegram-style command")
    p.add_argument("--command", required=True, help="Telegram command text, e.g. '/look Ankara model=ecmwf'")
    p.add_argument("--channel", help="Optional runtime channel, e.g. telegram")
    p.add_argument("--peer-kind", help="Optional runtime peer kind, e.g. group|direct")
    p.add_argument("--peer-id", help="Optional runtime peer/chat id")
    p.add_argument("--sender-id", help="Optional runtime sender id")
    p.add_argument("--session-key", help="Optional OpenClaw session key")
    return p


def main() -> None:
    args = build_parser().parse_args()
    try:
        print(
            render_report(
                args.command,
                channel=args.channel,
                peer_kind=args.peer_kind,
                peer_id=args.peer_id,
                sender_id=args.sender_id,
                session_key=args.session_key,
            )
        )
    except Exception as exc:
        print(f"❌ /look 执行失败: {exc}")


if __name__ == "__main__":
    main()
