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

from advection_review import build_850_advection_review
from diagnostics_500 import diagnose_500hpa
from diagnostics_700 import diagnose_700
from diagnostics_925 import diagnose_925
from diagnostics_sounding import diagnose_sounding
from sounding_obs_service import build_sounding_obs_context
from station_catalog import terrain_tag_for
from synoptic_regime import classify_large_scale_regime
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


def _iter_synoptic_bundle_docs(
    *,
    station_icao: str,
    target_date: str,
    model: str,
    synoptic_provider: str,
) -> list[tuple[datetime, dict[str, Any]]]:
    if not runtime_cache_enabled():
        return []
    patt = str(CACHE_DIR / "forecast_3d_bundle_*.json")
    cands: list[tuple[datetime, dict[str, Any]]] = []
    for p in glob.glob(patt):
        try:
            doc = json.loads(Path(p).read_text(encoding="utf-8"))
        except Exception:
            continue
        if str(doc.get("schema_version") or "") != FORECAST_3D_BUNDLE_SCHEMA_VERSION:
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
    cands.sort(key=lambda x: x[0], reverse=True)
    return cands


def _synoptic_from_bundle_doc(doc: dict[str, Any], *, synoptic_provider: str) -> dict[str, Any] | None:
    try:
        merged = _merge_synoptic_payloads(doc.get("slices") or [])
        merged["_provider_requested"] = str(doc.get("synoptic_provider") or synoptic_provider or "")
        merged["_provider_used"] = str(doc.get("synoptic_provider_used") or doc.get("synoptic_provider") or synoptic_provider or "")
        merged["_pass_strategy"] = str(doc.get("synoptic_pass_strategy") or "")
        merged["_anchors_local"] = [str(item).strip() for item in (doc.get("anchors_local") or []) if str(item).strip()]
        merged["_outer500_anchors_local"] = [
            str(item).strip() for item in (doc.get("outer500_anchors_local") or []) if str(item).strip()
        ]
        merged["_bundle_runtime"] = str(doc.get("runtime") or "")
        return merged
    except Exception:
        return None


def _read_runtime_synoptic_bundle(
    *,
    station_icao: str,
    target_date: str,
    model: str,
    synoptic_provider: str,
    runtime: str,
    max_age_hours: int = 12,
) -> dict[str, Any] | None:
    for ts, doc in _iter_synoptic_bundle_docs(
        station_icao=station_icao,
        target_date=target_date,
        model=model,
        synoptic_provider=synoptic_provider,
    ):
        if str(doc.get("runtime") or "") != runtime:
            continue
        if (datetime.now(timezone.utc) - ts) > timedelta(hours=max_age_hours):
            return None
        return _synoptic_from_bundle_doc(doc, synoptic_provider=synoptic_provider)
    return None


def _read_recent_synoptic_bundle(*, station_icao: str, target_date: str, model: str, synoptic_provider: str, max_age_hours: int = 12) -> dict[str, Any] | None:
    """Fallback: read most recent 3D bundle for same station/date/model/provider across runtime tags."""
    cands = _iter_synoptic_bundle_docs(
        station_icao=station_icao,
        target_date=target_date,
        model=model,
        synoptic_provider=synoptic_provider,
    )

    if not cands:
        return None
    for ts, best in cands:
        if (datetime.now(timezone.utc) - ts) > timedelta(hours=max_age_hours):
            return None
        merged = _synoptic_from_bundle_doc(best, synoptic_provider=synoptic_provider)
        if merged:
            return merged
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
    regime_label = str(diag.get("regime_label") or "").strip()
    hint = str(diag.get("phase_hint") or "").strip()
    pva_explained = str(diag.get("pva_explained") or "").strip()
    trend = str(diag.get("trend_12_24h") or "").strip()
    confidence = str(diag.get("confidence") or "").strip()
    height_text = str(diag.get("height_text") or "").strip()
    notable_params = [str(item).strip() for item in (diag.get("notable_params") or []) if str(item).strip()]

    if (not regime_label) or regime_label in {"中性", "高空弱信号背景", "弱信号背景"}:
        if pva_explained and pva_explained != "中性":
            return f"500hPa弱信号背景，{pva_explained}。"
        return "高空背景信号有限。"

    parts: list[str] = [f"500hPa {regime_label}"]
    if hint and hint not in {"南北向过渡"}:
        parts[0] += f"（{hint}）"
    if height_text:
        parts.append(height_text)
    if pva_explained and pva_explained != "中性":
        parts.append(pva_explained)
    if notable_params:
        parts.append("，".join(notable_params[:2]))
    if trend and trend not in {"不明确"}:
        parts.append(trend)
    if confidence and confidence not in {"低"}:
        parts.append(f"{confidence}置信")
    return "；".join(parts) + "。"


def _build_bottleneck_context(
    *,
    extra_text: str,
    diag700: dict[str, Any] | None,
    diag925: dict[str, Any] | None,
    sounding: dict[str, Any] | None,
) -> dict[str, Any]:
    text = str(extra_text or "").strip()
    d700 = dict(diag700 or {})
    d925 = dict(diag925 or {})
    snd = dict(sounding or {})
    path_bias = str(snd.get("path_bias") or "")
    code = "neutral"
    polarity = "neutral"
    source = "none"

    if str(d700.get("dry_intrusion_scope") or "") in {"near", "peripheral"} and str(d700.get("source") or "") == "synoptic-700":
        code = "midlevel_dry_support"
        polarity = "supportive"
        source = "h700"
    elif "湿层约束" in str(d700.get("summary") or ""):
        code = "midlevel_moist_constraint"
        polarity = "constraining"
        source = "h700"
    elif str(d925.get("coupling_state") or "") == "weak":
        code = "low_level_coupling_weak"
        polarity = "constraining"
        source = "h925"
    elif str(d925.get("coupling_state") or "") == "strong":
        code = "low_level_coupling_strong"
        polarity = "supportive"
        source = "h925"
    elif "再试探" in path_bias:
        code = "sounding_retest_support"
        polarity = "supportive"
        source = "sounding"
    elif path_bias:
        code = "sounding_constraint"
        polarity = "constraining"
        source = "sounding"

    return {
        "text": text,
        "code": code,
        "polarity": polarity,
        "source": source,
    }


def build_forecast_decision(
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
    sounding_obs = build_sounding_obs_context(station=station, now_utc=now_utc)
    snd = diagnose_sounding(
        primary_window,
        {},
        temp_unit=temp_unit,
        obs_context=sounding_obs,
        h700_summary=str(diag700.get("summary") or ""),
        h925_summary=str(diag925.get("summary") or ""),
    ) or {}

    regimes = classify_large_scale_regime(synoptic, station_lat, primary_window.get("w850_kmh"))
    regime_txt = regimes[0] if regimes else "过渡背景"

    phase500 = str(diag500.get("phase") or regime_txt)
    phase_hint = str(diag500.get("phase_hint") or "")
    pva500 = str(diag500.get("pva_proxy") or "中性")

    syn_systems = ((synoptic.get("scale_summary", {}) if isinstance(synoptic, dict) else {}).get("synoptic", {}) or {}).get("systems", [])
    advec = [s for s in syn_systems if "advection" in str(s.get("system_type", ""))]
    advection_review = build_850_advection_review(
        advec,
        now_local=now_local,
        primary_window=primary_window,
        h925_summary=str(diag925.get("summary") or ""),
        terrain_tag=str(terrain_tag_for(str(getattr(station, "icao", ""))) or ""),
    )
    advec_txt = str(advection_review.get("summary_line") or "低层输送信号一般。")

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
        try:
            s700_strength = float(diag700.get("dry_intrusion_strength") or 0.0)
        except Exception:
            s700_strength = 0.0
        try:
            low_cloud_pct = float(primary_window.get("low_cloud_pct")) if primary_window.get("low_cloud_pct") is not None else None
        except Exception:
            low_cloud_pct = None
        promote_dry_extra = s700_scope == "near"
        if (not promote_dry_extra) and s700_scope == "peripheral":
            promote_dry_extra = bool(s700_strength >= 12.5 and (low_cloud_pct is None or low_cloud_pct <= 25.0))

        if promote_dry_extra and s700_impact:
            extra = s700_impact
        elif promote_dry_extra:
            extra = "中层偏干信号存在，需配合低层开窗才容易转化为地面增温"

        if extra and s700_scope in {"peripheral", "remote"}:
            extra = extra + "（距离较远，仅按背景弱加分处理）"
        if extra and snd_q == "missing_profile" and s700_scope != "near":
            extra = extra + "（本站探空剖面缺测，未作本地湿干结构确认）"

    elif s700 and ("湿层" in s700 or "约束" in s700):
        extra = "700hPa 湿层约束偏强：低云更易维持，上沿更易受压"
    elif diag925.get("summary"):
        extra = f"925hPa：{diag925.get('summary')}"
    elif snd.get("path_bias"):
        pb = str(snd.get("path_bias"))
        extra = "探空显示高层约束相对弱" if "再试探" in pb else "探空显示高层约束偏强"

    bottleneck_context = _build_bottleneck_context(
        extra_text=str(extra or ""),
        diag700=diag700,
        diag925=diag925,
        sounding=snd,
    )

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
                "regime_label": str(diag500.get("regime_label") or ""),
                "proximity": str(diag500.get("proximity") or ""),
                "confidence": str(diag500.get("confidence") or ""),
                "forcing_text": str(diag500.get("forcing_text") or ""),
                "pva_explained": str(diag500.get("pva_explained") or ""),
                "impact_weight": str(diag500.get("impact_weight") or ""),
                "notable_params": list(diag500.get("notable_params") or []),
                "height_text": str(diag500.get("height_text") or ""),
                "thermal_role": str(diag500.get("thermal_role") or ""),
                "tmax_weight_score": float(diag500.get("tmax_weight_score") or 0.0),
                "tmax_bias_label": str(diag500.get("tmax_bias_label") or ""),
                "subtropical_high_detected": bool(diag500.get("subtropical_high_detected")),
                "subtropical_high_strength_gpm": diag500.get("subtropical_high_strength_gpm"),
                "subtropical_station_z500_gpm": diag500.get("subtropical_station_z500_gpm"),
                "subtropical_station_pct_in_band": diag500.get("subtropical_station_pct_in_band"),
                "subtropical_relation": diag500.get("subtropical_relation"),
                "subtropical_support_score": diag500.get("subtropical_support_score"),
                "subtropical_edge_586_margin_deg": diag500.get("subtropical_edge_586_margin_deg"),
                "subtropical_edge_588_margin_deg": diag500.get("subtropical_edge_588_margin_deg"),
                "westerly_belt_detected": bool(diag500.get("westerly_belt_detected")),
                "westerly_belt_intensity_ms": diag500.get("westerly_belt_intensity_ms"),
                "surface_coupling": str(diag500.get("surface_coupling") or ""),
            },
            "h850": {
                "advection": advec_txt,
                "review": advection_review,
            },
            "h700": dict(diag700),
            "h925": dict(diag925),
            "sounding": {
                "path_bias": str(snd.get("path_bias") or ""),
                "layer_findings": list(((snd.get("thermo") or {}).get("layer_findings") if isinstance(snd, dict) else []) or []),
                "actionable": str(((snd.get("thermo") or {}).get("actionable") if isinstance(snd, dict) else "") or ""),
                "profile_source": str(((snd.get("thermo") or {}).get("profile_source") if isinstance(snd, dict) else "") or ""),
                "confidence": str(((snd.get("thermo") or {}).get("sounding_confidence") if isinstance(snd, dict) else "") or ""),
                "thermo": snd.get("thermo") if isinstance(snd, dict) else None,
            },
        },
        "decision": {
            "main_path": phase_txt,
            "bottleneck": extra,
            "trigger": "临窗优先看云量开合与30-60分钟温度斜率",
            "object_3d_main": objects3d.get("main_object"),
            "override_risk": "high" if ((objects3d.get("main_object") or {}).get("impact_scope") == "possible_override" and (objects3d.get("main_object") or {}).get("vertical_coherence_score", 0) >= 0.6) else "low",
            "context": bottleneck_context,
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


def _select_inner_anchor_locals(
    *,
    anchor_locals: list[str],
    primary_window: dict[str, Any],
    now_local: datetime,
    tz_name: str,
) -> list[str]:
    if not anchor_locals:
        return []
    max_count_env = int(os.getenv("FORECAST_INNER_ANCHOR_MAX", "0") or "0")
    max_count = max_count_env if max_count_env > 0 else 5
    if len(anchor_locals) <= max_count:
        return list(anchor_locals)

    tz = ZoneInfo(tz_name)
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

    _add(_nearest_anchor_local(anchor_locals, now_local.astimezone(tz), tz_name))
    _add(anchor_locals[0])
    _add(anchor_locals[-1])

    if len(picked) < max_count and len(anchor_locals) >= 3:
        _add(anchor_locals[len(anchor_locals) // 2])

    return picked[:max_count]


def _merge_synoptic_payloads(payloads: list[dict[str, Any]]) -> dict[str, Any]:
    merged = {
        "scale_summary": {"synoptic": {"systems": []}},
        "anchor_slices": [],
        "anchor_count": 0,
        "_provider_requested": "",
        "_provider_used": "",
    }
    out = merged["scale_summary"]["synoptic"]["systems"]
    seen_global: set[tuple[str, str, int, int]] = set()
    slices_by_time: dict[str, dict[str, Any]] = {}
    legacy_slices: list[dict[str, Any]] = []
    provider_requested = ""
    provider_used = ""
    metadata_values: dict[str, list[Any]] = {}

    def _append_metadata(key: str, value: Any) -> None:
        if value in (None, "", [], {}):
            return
        bucket = metadata_values.setdefault(key, [])
        if value not in bucket:
            bucket.append(value)

    def _pick_runtime(values: list[Any]) -> Any:
        parsed: list[tuple[datetime, Any]] = []
        for value in values:
            text = str(value or "").strip()
            if len(text) != 11 or not text.endswith("Z") or not text[:10].isdigit():
                continue
            try:
                parsed.append((datetime.strptime(text, "%Y%m%d%HZ").replace(tzinfo=timezone.utc), value))
            except Exception:
                continue
        if parsed:
            parsed.sort(key=lambda item: item[0])
            return parsed[-1][1]
        return values[0]

    def _system_key(system: dict[str, Any]) -> tuple[str, str, int, int]:
        return (
            str(system.get("level") or ""),
            str(system.get("system_type") or ""),
            int(round(float(system.get("center_lat") or 0.0) * 10)),
            int(round(float(system.get("center_lon") or 0.0) * 10)),
        )

    def _append_global(system: dict[str, Any]) -> None:
        key = _system_key(system)
        if key in seen_global:
            return
        seen_global.add(key)
        out.append(system)

    for idx, payload in enumerate(payloads):
        if not provider_requested:
            provider_requested = str(payload.get("_provider_requested") or "")
        if not provider_used:
            provider_used = str(payload.get("_provider_used") or "")
        for key in (
            "analysis_runtime_used",
            "analysis_fh_used",
            "analysis_stream_used",
            "previous_runtime_used",
            "previous_fh_used",
            "previous_stream_used",
        ):
            _append_metadata(key, payload.get(key))
        systems = ((payload.get("scale_summary") or {}).get("synoptic") or {}).get("systems") or []
        analysis_time_utc = str(payload.get("analysis_time_utc") or "").strip()
        analysis_time_local = str(payload.get("analysis_time_local") or "").strip()
        slice_doc: dict[str, Any] | None = None
        if analysis_time_utc:
            slice_doc = slices_by_time.setdefault(
                analysis_time_utc,
                {
                    "analysis_time_utc": analysis_time_utc,
                    "analysis_time_local": analysis_time_local,
                    "systems": [],
                },
            )
            if analysis_time_local and not slice_doc.get("analysis_time_local"):
                slice_doc["analysis_time_local"] = analysis_time_local
        elif systems:
            slice_doc = {
                "analysis_time_utc": "",
                "analysis_time_local": analysis_time_local,
                "systems": [],
                "_legacy_index": idx,
            }
            legacy_slices.append(slice_doc)

        if slice_doc is not None:
            seen_local = {
                _system_key(system)
                for system in (slice_doc.get("systems") or [])
                if isinstance(system, dict)
            }
            for system in systems:
                if not isinstance(system, dict):
                    continue
                key = _system_key(system)
                if key not in seen_local:
                    slice_doc["systems"].append(system)
                    seen_local.add(key)
                _append_global(system)
        else:
            for system in systems:
                if isinstance(system, dict):
                    _append_global(system)

    ordered_slices = list(slices_by_time.values())
    try:
        ordered_slices.sort(key=lambda item: datetime.fromisoformat(str(item.get("analysis_time_utc") or "").replace("Z", "+00:00")))
    except Exception:
        ordered_slices.sort(key=lambda item: str(item.get("analysis_time_utc") or ""))
    ordered_slices.extend(sorted(legacy_slices, key=lambda item: int(item.get("_legacy_index") or 0)))

    for slice_doc in ordered_slices:
        if "_legacy_index" in slice_doc:
            slice_doc = dict(slice_doc)
            slice_doc.pop("_legacy_index", None)
        merged["anchor_slices"].append(slice_doc)

    merged["anchor_count"] = len(merged["anchor_slices"])
    merged["_provider_requested"] = provider_requested
    merged["_provider_used"] = provider_used or provider_requested
    for key, values in metadata_values.items():
        if not values:
            continue
        merged[key] = _pick_runtime(values) if key.endswith("_runtime_used") else values[0]
        if len(values) > 1:
            merged[f"{key}_values"] = list(values)
            merged[f"{key}_mixed"] = True
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
    prefer_cached_synoptic: bool = True,
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
    actual_synoptic_provider = str(synoptic_provider or "")
    if cached and (not force_rebuild):
        try:
            cached.setdefault("quality", {})["source_state"] = "cache-hit"
        except Exception:
            pass
        # cache-hit 不再强制重跑 synoptic（此前这是主要耗时来源）
        return cached, synoptic, None

    synoptic_from_fallback = False
    synoptic_from_bundle_cache = False
    strategy = str(os.getenv("FORECAST_SYNOPTIC_PASS_STRATEGY", "split_outer500") or "split_outer500").strip().lower()
    inner_anchor_locals: list[str] = []
    outer500_anchors: list[str] = []
    inner_ok = 0
    outer500_ok = 0
    anchor_telemetry: list[dict[str, Any]] = []

    if prefer_cached_synoptic and (not force_rebuild):
        t = time.perf_counter()
        cached_runtime_syn = _read_runtime_synoptic_bundle(
            station_icao=str(getattr(station, "icao", "")),
            target_date=target_date,
            model=model,
            synoptic_provider=synoptic_provider,
            runtime=runtime,
            max_age_hours=int(os.getenv("FORECAST_RUNTIME_BUNDLE_REUSE_HOURS", "12") or "12"),
        )
        _log("forecast.synoptic_runtime_bundle_cache", t)
        if cached_runtime_syn:
            synoptic = cached_runtime_syn
            actual_synoptic_provider = str(cached_runtime_syn.get("_provider_used") or synoptic_provider or "")
            strategy = str(cached_runtime_syn.get("_pass_strategy") or strategy)
            inner_anchor_locals = [str(item) for item in (cached_runtime_syn.get("_anchors_local") or []) if str(item)]
            outer500_anchors = [str(item) for item in (cached_runtime_syn.get("_outer500_anchors_local") or []) if str(item)]
            if not inner_anchor_locals:
                inner_anchor_locals = [
                    str(item.get("analysis_time_local") or item.get("analysis_time_utc") or "").strip()
                    for item in (cached_runtime_syn.get("anchor_slices") or [])
                    if isinstance(item, dict) and str(item.get("analysis_time_local") or item.get("analysis_time_utc") or "").strip()
                ]
            inner_ok = len(inner_anchor_locals)
            outer500_ok = len(outer500_anchors)
            synoptic_from_bundle_cache = True
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
                actual_synoptic_provider = str(fallback_syn.get("_provider_used") or synoptic_provider or "")
                strategy = str(fallback_syn.get("_pass_strategy") or strategy)
                inner_anchor_locals = [str(item) for item in (fallback_syn.get("_anchors_local") or []) if str(item)]
                outer500_anchors = [str(item) for item in (fallback_syn.get("_outer500_anchors_local") or []) if str(item)]
                if not inner_anchor_locals:
                    inner_anchor_locals = [
                        str(item.get("analysis_time_local") or item.get("analysis_time_utc") or "").strip()
                        for item in (fallback_syn.get("anchor_slices") or [])
                        if isinstance(item, dict) and str(item.get("analysis_time_local") or item.get("analysis_time_utc") or "").strip()
                    ]
                inner_ok = len(inner_anchor_locals)
                outer500_ok = len(outer500_anchors)
                synoptic_from_fallback = True
                if perf_log:
                    perf_log("forecast.synoptic_fallback_cache", 0.0)

    if not synoptic_from_bundle_cache and not synoptic_from_fallback:
        t = time.perf_counter()
        syn_payloads: list[dict[str, Any]] = []
        anchor_locals = _full_day_anchor_locals(target_date, tz_name, model)
        # Default to full-day anchors; set FORECAST_ANCHOR_LIMIT>0 to cap for performance tests.
        anchor_limit = int(os.getenv("FORECAST_ANCHOR_LIMIT", "0") or "0")
        if anchor_limit > 0:
            anchor_locals = anchor_locals[:anchor_limit]
        inner_anchor_locals = _select_inner_anchor_locals(
            anchor_locals=anchor_locals,
            primary_window=primary_window,
            now_local=now_local,
            tz_name=tz_name,
        )

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
            max_workers = max(1, int(os.getenv("FORECAST_MAX_WORKERS", "3") or "3"))
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

        if strategy in {"full", "legacy"}:
            batch_payloads, batch_tele, batch_ok = _run_anchor_batch(inner_anchor_locals, "full")
            syn_payloads.extend(batch_payloads)
            anchor_telemetry.extend(batch_tele)
            inner_ok = batch_ok
        else:
            inner_payloads, inner_tele, inner_ok = _run_anchor_batch(inner_anchor_locals, "inner_only")
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
            actual_synoptic_provider = str(synoptic.get("_provider_used") or synoptic_provider or "")
            actual_bundle_runtime = str(synoptic.get("analysis_runtime_used") or runtime)
            try:
                _write_3d_bundle(
                    {
                        "schema_version": FORECAST_3D_BUNDLE_SCHEMA_VERSION,
                        "station": str(getattr(station, "icao", "")),
                        "date": target_date,
                        "model": model,
                        "synoptic_provider": synoptic_provider,
                        "synoptic_provider_used": actual_synoptic_provider,
                        "synoptic_pass_strategy": strategy,
                        "runtime": actual_bundle_runtime,
                        "requested_runtime": runtime,
                        "anchors_local": inner_anchor_locals,
                        "outer500_anchors_local": outer500_anchors,
                        "slices": syn_payloads,
                    },
                    str(getattr(station, "icao", "")), target_date, model.lower(), str(synoptic_provider or ""), actual_bundle_runtime,
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
                actual_synoptic_provider = str(fallback_syn.get("_provider_used") or synoptic_provider or "")
                strategy = str(fallback_syn.get("_pass_strategy") or strategy)
                inner_anchor_locals = [str(item) for item in (fallback_syn.get("_anchors_local") or []) if str(item)]
                outer500_anchors = [str(item) for item in (fallback_syn.get("_outer500_anchors_local") or []) if str(item)]
                if not inner_anchor_locals:
                    inner_anchor_locals = [
                        str(item.get("analysis_time_local") or item.get("analysis_time_utc") or "").strip()
                        for item in (fallback_syn.get("anchor_slices") or [])
                        if isinstance(item, dict) and str(item.get("analysis_time_local") or item.get("analysis_time_utc") or "").strip()
                    ]
                inner_ok = len(inner_anchor_locals)
                outer500_ok = len(outer500_anchors)
                synoptic_from_fallback = True
                if perf_log:
                    perf_log("forecast.synoptic_fallback_cache", 0.0)
        _log("forecast.synoptic_build", t)

    t = time.perf_counter()
    decision = build_forecast_decision(
        station=station,
        target_date=target_date,
        model=model,
        synoptic_provider=actual_synoptic_provider,
        now_utc=now_utc,
        now_local=now_local,
        station_lat=station_lat,
        station_lon=station_lon,
        primary_window=primary_window,
        synoptic=synoptic,
    )
    _log("forecast.decision_build", t)

    q = decision.setdefault("quality", {})
    total_anchors = max(1, len(inner_anchor_locals))
    ok_anchors = max(0, inner_ok)
    coverage = ok_anchors / total_anchors
    q["synoptic_anchors_total"] = total_anchors
    q["synoptic_anchors_ok"] = ok_anchors
    q["synoptic_coverage"] = round(coverage, 3)
    q["synoptic_pass_strategy"] = strategy
    q["synoptic_provider_requested"] = str(synoptic_provider or "")
    q["synoptic_provider_used"] = str(actual_synoptic_provider or synoptic_provider or "")
    for key in (
        "analysis_runtime_used",
        "analysis_fh_used",
        "analysis_stream_used",
        "previous_runtime_used",
        "previous_fh_used",
        "previous_stream_used",
    ):
        value = synoptic.get(key)
        if value not in (None, "", [], {}):
            q[f"synoptic_{key}"] = value
        extra_values = synoptic.get(f"{key}_values")
        if extra_values not in (None, "", [], {}):
            q[f"synoptic_{key}_values"] = extra_values
        if synoptic.get(f"{key}_mixed"):
            q[f"synoptic_{key}_mixed"] = True
    if q["synoptic_provider_requested"] != q["synoptic_provider_used"]:
        q["synoptic_provider_fallback"] = True

    if anchor_telemetry:
        q["synoptic_anchor_events"] = anchor_telemetry
        err_counter: Counter[str] = Counter()
        provider_error_counter: Counter[str] = Counter()
        provider_failed: set[str] = set()
        provider_backoff_active: set[str] = set()
        provider_error_examples: dict[str, str] = {}
        for ev in anchor_telemetry:
            if str(ev.get("status") or "") != "failed":
                pass
            else:
                stage = str(ev.get("stage") or "unknown")
                etype = str(ev.get("error_type") or "unknown")
                err_counter[f"{stage}:{etype}"] += 1
            for pe in (ev.get("provider_errors") or []):
                if not isinstance(pe, dict):
                    continue
                provider_name = str(pe.get("provider") or "").strip()
                error_type = str(pe.get("error_type") or "unknown").strip() or "unknown"
                if provider_name:
                    provider_failed.add(provider_name)
                    provider_error_counter[f"{provider_name}:{error_type}"] += 1
                    if provider_name not in provider_error_examples and str(pe.get("error") or "").strip():
                        provider_error_examples[provider_name] = str(pe.get("error") or "").strip()
                    if pe.get("backoff_active"):
                        provider_backoff_active.add(provider_name)
        if err_counter:
            q["synoptic_anchor_error_counts"] = dict(sorted(err_counter.items()))
        if provider_error_counter:
            q["synoptic_provider_error_counts"] = dict(sorted(provider_error_counter.items()))
            q["synoptic_provider_failed"] = sorted(provider_failed)
            q["synoptic_provider_failed_reason"] = provider_error_counter.most_common(1)[0][0]
            if provider_error_examples:
                q["synoptic_provider_failed_examples"] = provider_error_examples
            if provider_backoff_active:
                q["synoptic_provider_backoff_active"] = sorted(provider_backoff_active)

    if synoptic_error and not synoptic_from_fallback:
        q["source_state"] = "degraded"
        q.setdefault("missing_layers", []).append("synoptic")
    elif synoptic_from_bundle_cache:
        q["source_state"] = "bundle-cache-hit"
    elif synoptic_from_fallback:
        q["source_state"] = "fallback-cache"

    if strategy in {"full", "legacy"}:
        q["synoptic_outer500_anchors_total"] = len(inner_anchor_locals)
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
    meta = decision.setdefault("meta", {})
    actual_runtime_used = str(q.get("synoptic_analysis_runtime_used") or "").strip()
    if actual_runtime_used:
        meta["runtime_requested"] = str(meta.get("runtime") or runtime)
        meta["runtime"] = actual_runtime_used

    t = time.perf_counter()
    _write_cache(decision, *key_parts)
    _log("forecast.cache_write", t)
    return decision, synoptic, synoptic_error
