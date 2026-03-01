from __future__ import annotations

import hashlib
import json
import os
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo
from pathlib import Path
from typing import Any, Callable

from diagnostics_500 import diagnose_500hpa
from diagnostics_700 import diagnose_700
from diagnostics_850 import advection_eta_local, distance_km_from_system
from diagnostics_925 import diagnose_925
from diagnostics_sounding import diagnose_sounding
from synoptic_regime import advection_reach_score, classify_large_scale_regime
from vertical_3d import build_3d_objects

ROOT = Path(__file__).resolve().parent.parent
CACHE_DIR = ROOT / "cache" / "runtime"
SCHEMA_VERSION = "forecast-decision.v2"


def _cache_key(*parts: str) -> str:
    raw = "|".join(parts)
    return hashlib.sha1(raw.encode("utf-8")).hexdigest()[:16]


def _cache_path(*parts: str) -> Path:
    return CACHE_DIR / f"forecast_decision_{_cache_key(*parts)}.json"


def _read_cache(*parts: str, ttl_hours: int = 6) -> dict[str, Any] | None:
    p = _cache_path(*parts)
    if not p.exists():
        return None
    try:
        doc = json.loads(p.read_text(encoding="utf-8"))
        ts = datetime.fromisoformat(str(doc.get("updated_at", "")).replace("Z", "+00:00"))
        if datetime.now(timezone.utc) - ts > timedelta(hours=ttl_hours):
            return None
        payload = doc.get("payload")
        if isinstance(payload, dict) and payload.get("schema_version") == SCHEMA_VERSION:
            return payload
    except Exception:
        return None
    return None


def _write_cache(payload: dict[str, Any], *parts: str) -> None:
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    p = _cache_path(*parts)
    doc = {
        "updated_at": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
        "payload": payload,
    }
    p.write_text(json.dumps(doc, ensure_ascii=False), encoding="utf-8")


def _write_3d_bundle(bundle: dict[str, Any], *parts: str) -> None:
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    k = _cache_key(*parts)
    p = CACHE_DIR / f"forecast_3d_bundle_{k}.json"
    p.write_text(json.dumps(bundle, ensure_ascii=False), encoding="utf-8")


def _runtime_tag(model: str, now_utc: datetime) -> str:
    cycle = 6
    hh = (now_utc.hour // cycle) * cycle
    return f"{now_utc.strftime('%Y%m%d')}{hh:02d}Z"


def build_forecast_decision(
    *,
    station: Any,
    target_date: str,
    model: str,
    now_local: datetime,
    station_lat: float,
    station_lon: float,
    primary_window: dict[str, Any],
    synoptic: dict[str, Any],
) -> dict[str, Any]:
    diag500 = diagnose_500hpa(synoptic) or {}
    diag700 = diagnose_700(primary_window) or {}
    diag925 = diagnose_925(primary_window, None) or {}
    snd = diagnose_sounding(primary_window, {}) or {}

    regimes = classify_large_scale_regime(synoptic, station_lat, primary_window.get("w850_kmh"))
    regime_txt = regimes[0] if regimes else "过渡背景"

    phase500 = str(diag500.get("phase") or regime_txt)
    phase_hint = str(diag500.get("phase_hint") or "")
    pva500 = str(diag500.get("pva_proxy") or "中性")

    syn_systems = ((synoptic.get("scale_summary", {}) if isinstance(synoptic, dict) else {}).get("synoptic", {}) or {}).get("systems", [])
    w850 = primary_window.get("w850_kmh")
    advec = [s for s in syn_systems if "advection" in str(s.get("system_type", ""))]
    advec_txt = "低层输送信号一般"
    if advec:
        a = advec[0]
        score, lvl = advection_reach_score(a, w850)
        eta_txt = advection_eta_local(now_local, distance_km_from_system(a), score, w850)
        advec_type = "暖平流" if "warm" in str(a.get("system_type", "")) else "冷平流"
        advec_txt = f"{advec_type}{lvl}（{score:.2f}，{eta_txt}）"

    try:
        start_dt = datetime.strptime(str(primary_window.get("start_local")), "%Y-%m-%dT%H:%M")
        now_naive = now_local.replace(tzinfo=None)
        hours_to_peak = (start_dt - now_naive).total_seconds() / 3600.0
    except Exception:
        hours_to_peak = -1

    if hours_to_peak > 4:
        phase_txt = "预报主导"
    elif 3 <= hours_to_peak <= 4:
        phase_txt = "预报-实况过渡"
    else:
        phase_txt = "实况主导"

    if "NVA" in pva500 or "下沉" in pva500 or "脊后" in phase_hint:
        p500_human = "高空下沉背景偏多，上冲动能容易被压制"
    else:
        p500_human = "高空仍有抬升触发条件，云层若放开更易再冲高"

    extra = None
    s700 = str(diag700.get("summary") or "")
    if s700 and "干层" in s700:
        extra = "700hPa 干空气偏明显：云开则升温加速，云不开则作用难落地"
    elif s700 and ("湿层" in s700 or "约束" in s700):
        extra = "700hPa 湿层约束偏强：低云更易维持，上沿更易受压"
    elif diag925.get("summary"):
        extra = f"925hPa：{diag925.get('summary')}"
    elif snd.get("path_bias"):
        pb = str(snd.get("path_bias"))
        extra = "探空显示高层约束相对弱" if "再试探" in pb else "探空显示高层约束偏强"

    objects3d = build_3d_objects(
        synoptic=synoptic,
        station_lat=station_lat,
        station_lon=station_lon,
        primary_window=primary_window,
        diag700=diag700,
        diag925=diag925,
    )

    return {
        "schema_version": SCHEMA_VERSION,
        "meta": {
            "station": str(getattr(station, "icao", "")),
            "date": target_date,
            "model": model,
            "runtime": _runtime_tag(model, datetime.now(timezone.utc)),
            "window": {
                "start_local": str(primary_window.get("start_local") or ""),
                "end_local": str(primary_window.get("end_local") or ""),
            },
        },
        "quality": {
            "source_state": "fresh",
            "missing_layers": [],
        },
        "features": {
            "objects_3d": objects3d,
            "h500": {
                "phase": phase500,
                "phase_hint": phase_hint,
                "pva_proxy": pva500,
            },
            "h850": {
                "advection": advec_txt,
            },
            "h700": {
                "summary": s700,
            },
            "h925": {
                "summary": str(diag925.get("summary") or ""),
            },
            "sounding": {
                "path_bias": str(snd.get("path_bias") or ""),
            },
        },
        "decision": {
            "main_path": phase_txt,
            "bottleneck": extra,
            "trigger": "临窗优先看云量开合与30-60分钟温度斜率",
            "object_3d_main": objects3d.get("main_object"),
            "override_risk": "high" if ((objects3d.get("main_object") or {}).get("impact_scope") == "possible_override" and (objects3d.get("main_object") or {}).get("vertical_coherence_score", 0) >= 0.6) else "low",
            "background": {
                "phase_mode": phase_txt,
                "phase500": phase500,
                "pva500": pva500,
                "phase_hint": phase_hint,
                "line_500": p500_human,
                "line_850": advec_txt,
                "extra": extra,
            },
        },
    }


def _model_step_hours(model: str) -> int:
    m = (model or "").strip().lower()
    # Use native-like temporal density for synoptic anchors.
    if m in {"gfs", "icon", "gem", "ukmo"}:
        return 3
    return 6


def _full_day_anchor_locals(target_date: str, tz_name: str, model: str) -> list[str]:
    """Return local-time anchors with envelope coverage.

    Rule: include one aligned step before local-day start and one aligned step after local-day end,
    then all aligned steps in between. This captures cross-boundary 3D evolution without extra far points.
    """
    tz = ZoneInfo(tz_name)
    d0_local = datetime.strptime(target_date, "%Y-%m-%d").replace(tzinfo=tz)
    d1_local = d0_local + timedelta(days=1)

    start_utc = d0_local.astimezone(timezone.utc)
    end_utc = d1_local.astimezone(timezone.utc)

    step_h = max(1, _model_step_hours(model))
    step_s = step_h * 3600

    start_ts = int(start_utc.timestamp())
    end_ts = int(end_utc.timestamp())

    first_ts = (start_ts // step_s) * step_s
    if first_ts > start_ts:
        first_ts -= step_s
    last_ts = ((end_ts + step_s - 1) // step_s) * step_s
    if last_ts < end_ts:
        last_ts += step_s

    anchors_utc: list[datetime] = []
    ts = first_ts
    while ts <= last_ts:
        anchors_utc.append(datetime.fromtimestamp(ts, tz=timezone.utc))
        ts += step_s

    return [a.astimezone(tz).strftime("%Y-%m-%dT%H:%M") for a in anchors_utc]


def _merge_synoptic_payloads(payloads: list[dict[str, Any]]) -> dict[str, Any]:
    merged = {"scale_summary": {"synoptic": {"systems": []}}}
    out = merged["scale_summary"]["synoptic"]["systems"]
    seen: set[tuple[str, str, int, int]] = set()
    for p in payloads:
        systems = ((p.get("scale_summary") or {}).get("synoptic") or {}).get("systems") or []
        for s in systems:
            key = (
                str(s.get("level") or ""),
                str(s.get("system_type") or ""),
                int(round(float(s.get("center_lat") or 0.0) * 10)),
                int(round(float(s.get("center_lon") or 0.0) * 10)),
            )
            if key in seen:
                continue
            seen.add(key)
            out.append(s)
    return merged


def load_or_build_forecast_decision(
    *,
    station: Any,
    target_date: str,
    model: str,
    now_utc: datetime,
    now_local: datetime,
    station_lat: float,
    station_lon: float,
    primary_window: dict[str, Any],
    tz_name: str,
    run_synoptic_fn: Callable[[Any, str, str, str, str, str], dict[str, Any]],
    perf_log: Callable[[str, float], None] | None = None,
) -> tuple[dict[str, Any], dict[str, Any], str | None]:
    def _log(stage: str, start: float) -> None:
        if perf_log:
            perf_log(stage, time.perf_counter() - start)

    runtime = _runtime_tag(model, now_utc)
    key_parts = (str(getattr(station, "icao", "")), target_date, model.lower(), runtime)

    t = time.perf_counter()
    cached = _read_cache(*key_parts)
    _log("forecast.cache_read", t)
    synoptic_error = None

    synoptic = {"scale_summary": {"synoptic": {"systems": []}}}
    if cached:
        try:
            cached.setdefault("quality", {})["source_state"] = "cache-hit"
        except Exception:
            pass
        # cache-hit 不再强制重跑 synoptic（此前这是主要耗时来源）
        return cached, synoptic, None

    t = time.perf_counter()
    syn_payloads: list[dict[str, Any]] = []
    anchor_locals = _full_day_anchor_locals(target_date, tz_name, model)
    # Default to full-day anchors; set FORECAST_ANCHOR_LIMIT>0 to cap for performance tests.
    anchor_limit = int(os.getenv("FORECAST_ANCHOR_LIMIT", "0") or "0")
    if anchor_limit > 0:
        anchor_locals = anchor_locals[:anchor_limit]

    def _pull_anchor(a_local: str) -> tuple[str, dict[str, Any] | None, str | None]:
        t_anchor = time.perf_counter()
        try:
            p = run_synoptic_fn(station, target_date, a_local, tz_name, model, runtime)
            if perf_log:
                perf_log(f"forecast.anchor.{a_local}", time.perf_counter() - t_anchor)
            return a_local, p, None
        except Exception as exc:
            if perf_log:
                perf_log(f"forecast.anchor.{a_local}.failed", time.perf_counter() - t_anchor)
            return a_local, None, str(exc)

    max_workers = 2
    with ThreadPoolExecutor(max_workers=max_workers) as ex:
        futs = [ex.submit(_pull_anchor, a) for a in anchor_locals]
        for fut in as_completed(futs):
            _a, payload, err = fut.result()
            if payload is not None:
                syn_payloads.append(payload)
            elif err:
                synoptic_error = err

    if syn_payloads:
        synoptic = _merge_synoptic_payloads(syn_payloads)
        try:
            _write_3d_bundle(
                {
                    "schema_version": "forecast-3d-bundle.v1",
                    "station": str(getattr(station, "icao", "")),
                    "date": target_date,
                    "model": model,
                    "runtime": runtime,
                    "anchors_local": anchor_locals,
                    "slices": syn_payloads,
                },
                str(getattr(station, "icao", "")), target_date, model.lower(), runtime,
            )
        except Exception:
            pass
    _log("forecast.synoptic_build", t)

    t = time.perf_counter()
    decision = build_forecast_decision(
        station=station,
        target_date=target_date,
        model=model,
        now_local=now_local,
        station_lat=station_lat,
        station_lon=station_lon,
        primary_window=primary_window,
        synoptic=synoptic,
    )
    _log("forecast.decision_build", t)

    if synoptic_error:
        decision.setdefault("quality", {})["source_state"] = "degraded"
        decision.setdefault("quality", {}).setdefault("missing_layers", []).append("synoptic")

    t = time.perf_counter()
    _write_cache(decision, *key_parts)
    _log("forecast.cache_write", t)
    return decision, synoptic, synoptic_error
