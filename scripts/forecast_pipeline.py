from __future__ import annotations

import glob
import hashlib
import json
import os
import random
import time
from collections import Counter
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
from contracts import (
    FORECAST_DECISION_SCHEMA_VERSION,
    FORECAST_3D_BUNDLE_SCHEMA_VERSION,
)
from cache_envelope import extract_payload, make_cache_doc
from runtime_cache_policy import runtime_cache_enabled

ROOT = Path(__file__).resolve().parent.parent
CACHE_DIR = ROOT / "cache" / "runtime"
SCHEMA_VERSION = FORECAST_DECISION_SCHEMA_VERSION


def _cache_key(*parts: str) -> str:
    raw = "|".join(parts)
    return hashlib.sha1(raw.encode("utf-8")).hexdigest()[:16]


def _cache_path(*parts: str) -> Path:
    return CACHE_DIR / f"forecast_decision_{_cache_key(*parts)}.json"


def _read_cache(*parts: str, ttl_hours: int = int(os.getenv("WEATHERBOT_FORECAST_DECISION_CACHE_TTL_HOURS", "2") or "2")) -> dict[str, Any] | None:
    if not runtime_cache_enabled():
        return None
    p = _cache_path(*parts)
    if not p.exists():
        return None
    try:
        doc = json.loads(p.read_text(encoding="utf-8"))
        payload, updated_at, _env = extract_payload(doc)
        if not isinstance(payload, dict):
            return None
        if updated_at:
            ts = datetime.fromisoformat(str(updated_at).replace("Z", "+00:00"))
            if datetime.now(timezone.utc) - ts > timedelta(hours=ttl_hours):
                return None
        if payload.get("schema_version") == SCHEMA_VERSION:
            return payload
    except Exception:
        return None
    return None


def _write_cache(payload: dict[str, Any], *parts: str) -> None:
    if not runtime_cache_enabled():
        return
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    p = _cache_path(*parts)
    doc = make_cache_doc(
        payload,
        source_state="fresh",
        payload_schema_version=str(payload.get("schema_version")) if isinstance(payload, dict) else None,
        meta={"kind": "forecast_decision"},
    )
    p.write_text(json.dumps(doc, ensure_ascii=False), encoding="utf-8")


def _write_3d_bundle(bundle: dict[str, Any], *parts: str) -> None:
    if not runtime_cache_enabled():
        return
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    k = _cache_key(*parts)
    p = CACHE_DIR / f"forecast_3d_bundle_{k}.json"
    p.write_text(json.dumps(bundle, ensure_ascii=False), encoding="utf-8")


def _read_recent_synoptic_bundle(*, station_icao: str, target_date: str, model: str, synoptic_provider: str, max_age_hours: int = 12) -> dict[str, Any] | None:
    """Fallback: read most recent 3D bundle for same station/date/model/provider across runtime tags."""
    if not runtime_cache_enabled():
        return None
    patt = str(CACHE_DIR / "forecast_3d_bundle_*.json")
    cands: list[tuple[datetime, dict[str, Any]]] = []
    for p in glob.glob(patt):
        try:
            doc = json.loads(Path(p).read_text(encoding="utf-8"))
        except Exception:
            continue
        if str(doc.get("station") or "") != station_icao:
            continue
        if str(doc.get("date") or "") != target_date:
            continue
        if str(doc.get("model") or "").lower() != model.lower():
            continue
        pdoc = str(doc.get("synoptic_provider") or "").lower()
        if pdoc and pdoc != str(synoptic_provider or "").lower():
            continue
        slices = doc.get("slices")
        if not isinstance(slices, list) or not slices:
            continue
        try:
            ts = datetime.fromtimestamp(Path(p).stat().st_mtime, tz=timezone.utc)
        except Exception:
            ts = datetime.now(timezone.utc)
        cands.append((ts, doc))

    if not cands:
        return None
    cands.sort(key=lambda x: x[0], reverse=True)
    ts, best = cands[0]
    if (datetime.now(timezone.utc) - ts) > timedelta(hours=max_age_hours):
        return None
    try:
        return _merge_synoptic_payloads(best.get("slices") or [])
    except Exception:
        return None


def _runtime_tag(model: str, now_utc: datetime) -> str:
    cycle = 6
    hh = (now_utc.hour // cycle) * cycle
    return f"{now_utc.strftime('%Y%m%d')}{hh:02d}Z"


def _classify_anchor_error(msg: str) -> str:
    s = str(msg or "").lower()
    if "429" in s or "too many requests" in s:
        return "rate_limit_429"
    if "404" in s or "not found" in s:
        return "not_found_404"
    if "timeout" in s or "timed out" in s:
        return "timeout"
    if "connection" in s or "ssl" in s or "dns" in s:
        return "network"
    if "parse" in s or "subprocess" in s:
        return "subprocess"
    return "unknown"


def _build_500_background_line(diag500: dict[str, Any] | None) -> str:
    diag = dict(diag500 or {})
    phase = str(diag.get("phase") or "").strip()
    hint = str(diag.get("phase_hint") or "").strip()
    pva = str(diag.get("pva_proxy") or "").strip()
    trend = str(diag.get("trend_12_24h") or "").strip()
    confidence = str(diag.get("confidence") or "").strip()

    if (not phase) or phase in {"中性", "弱信号背景"}:
        if ("上升" in pva) or ("下沉" in pva):
            return f"500hPa弱信号背景，{pva}。"
        return "高空背景信号有限。"

    parts: list[str] = [f"500hPa {phase}"]
    if hint and hint not in {"南北向过渡"}:
        parts[0] += f"（{hint}）"
    if pva and pva != "中性":
        parts.append(pva)
    if trend and trend not in {"不明确"}:
        parts.append(trend)
    if confidence and confidence not in {"低"}:
        parts.append(f"{confidence}置信")
    return "；".join(parts) + "。"


def build_forecast_decision(
    *,
    station: Any,
    target_date: str,
    model: str,
    synoptic_provider: str,
    now_local: datetime,
    station_lat: float,
    station_lon: float,
    primary_window: dict[str, Any],
    synoptic: dict[str, Any],
) -> dict[str, Any]:
    diag500 = diagnose_500hpa(synoptic) or {}
    diag700 = diagnose_700(
        primary_window,
        synoptic=synoptic,
        station_lat=station_lat,
        station_lon=station_lon,
    ) or {}
    diag925 = diagnose_925(primary_window, None) or {}
    temp_unit = "F" if str(getattr(station, "icao", "")).upper().startswith("K") else "C"
    snd = diagnose_sounding(primary_window, {}, temp_unit=temp_unit) or {}

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

    p500_human = _build_500_background_line(diag500)

    extra = None
    s700 = str(diag700.get("summary") or "")
    if s700 and ("干层" in s700 or "偏干" in s700):
        s700_scope = str(diag700.get("dry_intrusion_scope") or "")
        s700_impact = str(diag700.get("impact") or "")
        snd_q = str(((snd.get("thermo") or {}).get("quality")) or "") if isinstance(snd, dict) else ""

        if s700_impact:
            extra = s700_impact
        else:
            extra = "中层偏干信号存在，需配合低层开窗才容易转化为地面增温"

        if s700_scope in {"peripheral", "remote"}:
            extra = extra + "（距离较远，仅按背景弱加分处理）"
        if snd_q == "missing_profile" and s700_scope != "near":
            extra = extra + "（本站探空剖面缺测，未作本地湿干结构确认）"

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
            "synoptic_provider": synoptic_provider,
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
                "thermo": snd.get("thermo") if isinstance(snd, dict) else None,
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
    # Unified anchor cadence for all models.
    # Requirement: all anchors sampled at 6-hour granularity.
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


def _parse_anchor_local(anchor_local: str, tz: ZoneInfo) -> datetime | None:
    try:
        return datetime.strptime(anchor_local, "%Y-%m-%dT%H:%M").replace(tzinfo=tz)
    except Exception:
        return None


def _nearest_anchor_local(anchor_locals: list[str], target_local: datetime | None, tz_name: str) -> str | None:
    if target_local is None or not anchor_locals:
        return None
    tz = ZoneInfo(tz_name)
    best_anchor: str | None = None
    best_dist_s: float | None = None
    for a in anchor_locals:
        dt = _parse_anchor_local(a, tz)
        if dt is None:
            continue
        d = abs((dt - target_local).total_seconds())
        if best_dist_s is None or d < best_dist_s:
            best_dist_s = d
            best_anchor = a
    return best_anchor


def _select_outer500_anchor_locals(
    *,
    anchor_locals: list[str],
    primary_window: dict[str, Any],
    now_local: datetime,
    tz_name: str,
) -> list[str]:
    if not anchor_locals:
        return []
    tz = ZoneInfo(tz_name)
    max_count_env = int(os.getenv("FORECAST_OUTER500_ANCHOR_MAX", "0") or "0")
    if max_count_env > 0:
        max_count = max_count_env
    else:
        max_count = 4
        try:
            ws = datetime.strptime(str(primary_window.get("start_local") or ""), "%Y-%m-%dT%H:%M").replace(tzinfo=tz)
            we = datetime.strptime(str(primary_window.get("end_local") or ""), "%Y-%m-%dT%H:%M").replace(tzinfo=tz)
            if now_local < (ws - timedelta(hours=3)) or now_local > (we + timedelta(hours=2)):
                max_count = 3
        except Exception:
            max_count = 4
    if max_count <= 0:
        return []
    window_targets: list[datetime] = []
    for k in ("start_local", "peak_local", "end_local"):
        raw = str(primary_window.get(k) or "").strip()
        if not raw:
            continue
        try:
            window_targets.append(datetime.strptime(raw, "%Y-%m-%dT%H:%M").replace(tzinfo=tz))
        except Exception:
            continue

    picked: list[str] = []
    seen: set[str] = set()

    def _add(anchor_local: str | None) -> None:
        if not anchor_local or anchor_local in seen:
            return
        seen.add(anchor_local)
        picked.append(anchor_local)

    for t in window_targets:
        _add(_nearest_anchor_local(anchor_locals, t, tz_name))

    if len(picked) < max_count:
        _add(_nearest_anchor_local(anchor_locals, now_local.astimezone(tz), tz_name))

    if len(picked) < max_count:
        _add(anchor_locals[0])
    if len(picked) < max_count:
        _add(anchor_locals[-1])

    if len(picked) < max_count and len(window_targets) >= 2:
        sdt = min(window_targets)
        edt = max(window_targets)
        mid = sdt + (edt - sdt) / 2
        _add(_nearest_anchor_local(anchor_locals, mid, tz_name))

    if len(picked) < max_count:
        candidates = [
            anchor_locals[len(anchor_locals) // 4],
            anchor_locals[len(anchor_locals) // 2],
            anchor_locals[(len(anchor_locals) * 3) // 4],
        ]
        for c in candidates:
            if len(picked) >= max_count:
                break
            _add(c)

    return picked[:max_count]


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
    synoptic_provider: str,
    now_utc: datetime,
    now_local: datetime,
    station_lat: float,
    station_lon: float,
    primary_window: dict[str, Any],
    tz_name: str,
    run_synoptic_fn: Callable[..., dict[str, Any]],
    perf_log: Callable[[str, float], None] | None = None,
) -> tuple[dict[str, Any], dict[str, Any], str | None]:
    def _log(stage: str, start: float) -> None:
        if perf_log:
            perf_log(stage, time.perf_counter() - start)

    runtime = _runtime_tag(model, now_utc)
    key_parts = (
        str(getattr(station, "icao", "")),
        target_date,
        model.lower(),
        str(synoptic_provider or ""),
        runtime,
        str(primary_window.get("peak_local") or ""),
    )

    t = time.perf_counter()
    cached = _read_cache(*key_parts)
    _log("forecast.cache_read", t)
    synoptic_error = None

    force_rebuild = str(os.getenv("FORECAST_FORCE_REBUILD", "0") or "0") in {"1", "true", "yes", "on"}

    synoptic = {"scale_summary": {"synoptic": {"systems": []}}}
    if cached and (not force_rebuild):
        try:
            cached.setdefault("quality", {})["source_state"] = "cache-hit"
        except Exception:
            pass
        # cache-hit 不再强制重跑 synoptic（此前这是主要耗时来源）
        return cached, synoptic, None

    t = time.perf_counter()
    syn_payloads: list[dict[str, Any]] = []
    synoptic_from_fallback = False
    anchor_locals = _full_day_anchor_locals(target_date, tz_name, model)
    anchor_telemetry: list[dict[str, Any]] = []
    # Default to full-day anchors; set FORECAST_ANCHOR_LIMIT>0 to cap for performance tests.
    anchor_limit = int(os.getenv("FORECAST_ANCHOR_LIMIT", "0") or "0")
    if anchor_limit > 0:
        anchor_locals = anchor_locals[:anchor_limit]

    def _pull_anchor(a_local: str, pass_mode: str) -> tuple[str, dict[str, Any] | None, str | None, list[dict[str, Any]]]:
        t_anchor = time.perf_counter()
        try:
            # Optional jitter to smooth provider burst limits.
            jitter_ms = int(os.getenv("FORECAST_ANCHOR_JITTER_MS", "0") or "0")
            if jitter_ms > 0:
                time.sleep(random.uniform(0.0, max(0.0, jitter_ms / 1000.0)))
            p = run_synoptic_fn(station, target_date, a_local, tz_name, model, runtime, pass_mode=pass_mode)
            tele: list[dict[str, Any]] = []
            tnode = p.get("_telemetry") if isinstance(p, dict) else None
            if isinstance(tnode, dict):
                for ev in (tnode.get("passes") or []):
                    if not isinstance(ev, dict):
                        continue
                    row = dict(ev)
                    row.setdefault("anchor_local", a_local)
                    row.setdefault("pass_mode", pass_mode)
                    tele.append(row)
            if perf_log:
                perf_log(f"forecast.anchor.{a_local}.{pass_mode}", time.perf_counter() - t_anchor)
            return a_local, p, None, tele
        except Exception as exc:
            if perf_log:
                perf_log(f"forecast.anchor.{a_local}.{pass_mode}.failed", time.perf_counter() - t_anchor)
            return a_local, None, str(exc), []

    def _run_anchor_batch(anchors: list[str], pass_mode: str) -> tuple[list[dict[str, Any]], list[dict[str, Any]], int]:
        out_payloads: list[dict[str, Any]] = []
        out_telemetry: list[dict[str, Any]] = []
        ok = 0
        if not anchors:
            return out_payloads, out_telemetry, ok
        max_workers = max(1, int(os.getenv("FORECAST_MAX_WORKERS", "2") or "2"))
        with ThreadPoolExecutor(max_workers=max_workers) as ex:
            futs = [ex.submit(_pull_anchor, a, pass_mode) for a in anchors]
            for fut in as_completed(futs):
                _a, payload, err, tele = fut.result()
                if tele:
                    out_telemetry.extend(tele)
                if payload is not None:
                    out_payloads.append(payload)
                    ok += 1
                elif err:
                    out_telemetry.append({
                        "anchor_local": _a,
                        "pass_mode": pass_mode,
                        "status": "failed",
                        "stage": "runner",
                        "error_type": _classify_anchor_error(err),
                        "error": str(err)[:300],
                    })
        return out_payloads, out_telemetry, ok

    strategy = str(os.getenv("FORECAST_SYNOPTIC_PASS_STRATEGY", "split_outer500") or "split_outer500").strip().lower()
    inner_ok = 0
    outer500_anchors: list[str] = []
    outer500_ok = 0

    if strategy in {"full", "legacy"}:
        batch_payloads, batch_tele, batch_ok = _run_anchor_batch(anchor_locals, "full")
        syn_payloads.extend(batch_payloads)
        anchor_telemetry.extend(batch_tele)
        inner_ok = batch_ok
    else:
        inner_payloads, inner_tele, inner_ok = _run_anchor_batch(anchor_locals, "inner_only")
        syn_payloads.extend(inner_payloads)
        anchor_telemetry.extend(inner_tele)

        outer500_anchors = _select_outer500_anchor_locals(
            anchor_locals=anchor_locals,
            primary_window=primary_window,
            now_local=now_local,
            tz_name=tz_name,
        )
        outer_payloads, outer_tele, outer500_ok = _run_anchor_batch(outer500_anchors, "outer500_only")
        syn_payloads.extend(outer_payloads)
        anchor_telemetry.extend(outer_tele)

    if anchor_telemetry:
        for ev in anchor_telemetry:
            if str(ev.get("status")) == "failed":
                synoptic_error = str(ev.get("error") or synoptic_error or "")

    if syn_payloads:
        synoptic = _merge_synoptic_payloads(syn_payloads)
        try:
            _write_3d_bundle(
                {
                    "schema_version": FORECAST_3D_BUNDLE_SCHEMA_VERSION,
                    "station": str(getattr(station, "icao", "")),
                    "date": target_date,
                    "model": model,
                    "synoptic_provider": synoptic_provider,
                    "synoptic_pass_strategy": strategy,
                    "runtime": runtime,
                    "anchors_local": anchor_locals,
                    "outer500_anchors_local": outer500_anchors,
                    "slices": syn_payloads,
                },
                str(getattr(station, "icao", "")), target_date, model.lower(), str(synoptic_provider or ""), runtime,
            )
        except Exception:
            pass
    else:
        fallback_syn = _read_recent_synoptic_bundle(
            station_icao=str(getattr(station, "icao", "")),
            target_date=target_date,
            model=model,
            synoptic_provider=synoptic_provider,
            max_age_hours=int(os.getenv("FORECAST_SYNOPTIC_FALLBACK_HOURS", "12") or "12"),
        )
        if fallback_syn:
            synoptic = fallback_syn
            synoptic_from_fallback = True
            if perf_log:
                perf_log("forecast.synoptic_fallback_cache", 0.0)
    _log("forecast.synoptic_build", t)

    t = time.perf_counter()
    decision = build_forecast_decision(
        station=station,
        target_date=target_date,
        model=model,
        synoptic_provider=synoptic_provider,
        now_local=now_local,
        station_lat=station_lat,
        station_lon=station_lon,
        primary_window=primary_window,
        synoptic=synoptic,
    )
    _log("forecast.decision_build", t)

    q = decision.setdefault("quality", {})
    total_anchors = max(1, len(anchor_locals))
    ok_anchors = max(0, inner_ok)
    coverage = ok_anchors / total_anchors
    q["synoptic_anchors_total"] = total_anchors
    q["synoptic_anchors_ok"] = ok_anchors
    q["synoptic_coverage"] = round(coverage, 3)
    q["synoptic_pass_strategy"] = strategy

    if anchor_telemetry:
        q["synoptic_anchor_events"] = anchor_telemetry
        err_counter: Counter[str] = Counter()
        for ev in anchor_telemetry:
            if str(ev.get("status") or "") != "failed":
                continue
            stage = str(ev.get("stage") or "unknown")
            etype = str(ev.get("error_type") or "unknown")
            err_counter[f"{stage}:{etype}"] += 1
        if err_counter:
            q["synoptic_anchor_error_counts"] = dict(sorted(err_counter.items()))

    if synoptic_error and not synoptic_from_fallback:
        q["source_state"] = "degraded"
        q.setdefault("missing_layers", []).append("synoptic")
    elif synoptic_from_fallback:
        q["source_state"] = "fallback-cache"

    if strategy in {"full", "legacy"}:
        q["synoptic_outer500_anchors_total"] = len(anchor_locals)
        q["synoptic_outer500_anchors_ok"] = int(ok_anchors)
    else:
        q["synoptic_outer500_anchors_total"] = len(outer500_anchors)
        q["synoptic_outer500_anchors_ok"] = int(outer500_ok)
        if outer500_anchors and outer500_ok <= 0:
            q.setdefault("missing_layers", []).append("synoptic_outer500")
            role = str(primary_window.get("window_role") or "").lower()
            if role in {"near_window", "in_window", "post_window", "post_eval_no_rebreak"}:
                q["source_state"] = "degraded"

    # Coverage gate: partial slices are usable but should be flagged when too sparse.
    if coverage < float(os.getenv("FORECAST_SYNOPTIC_MIN_COVERAGE", "0.5") or "0.5"):
        q["source_state"] = "degraded"
        q.setdefault("missing_layers", []).append("synoptic")

    if isinstance(q.get("missing_layers"), list):
        q["missing_layers"] = sorted(set(str(x) for x in q.get("missing_layers") if x))

    t = time.perf_counter()
    _write_cache(decision, *key_parts)
    _log("forecast.cache_write", t)
    return decision, synoptic, synoptic_error
