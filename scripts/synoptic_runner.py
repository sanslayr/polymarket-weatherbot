from __future__ import annotations

import hashlib
import importlib
import json
import math
import os
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Callable
from zoneinfo import ZoneInfo

from contracts import SYNOPTIC_CACHE_SCHEMA_VERSION
from cache_envelope import extract_payload, make_cache_doc
from runtime_cache_policy import runtime_cache_enabled
from synoptic_provider_router import (
    DEFAULT_SYNOPTIC_PROVIDER,
    build_synoptic_grid_payload,
    provider_candidates,
)


def analyze_synoptic_2d(payload: dict[str, Any], mode: str = "full") -> dict[str, Any]:
    try:
        detector = importlib.import_module("synoptic_2d_detector")
    except ModuleNotFoundError as exc:
        missing = str(getattr(exc, "name", "") or "").strip() or "unknown"
        raise RuntimeError(f"synoptic detector dependency missing: {missing}") from exc
    except ImportError as exc:
        raise RuntimeError(f"synoptic detector import failed: {exc}") from exc

    analyze_fn = getattr(detector, "analyze", None)
    if not callable(analyze_fn):
        raise RuntimeError("synoptic detector import failed: analyze() missing")
    return dict(analyze_fn(payload, mode=mode) or {})


def _cache_key(*parts: str) -> str:
    raw = "|".join(parts)
    return hashlib.sha1(raw.encode("utf-8")).hexdigest()[:16]


def _cache_path(cache_dir: Path, kind: str, *parts: str) -> Path:
    return cache_dir / f"{kind}_{_cache_key(*parts)}.json"


def _read_cache(cache_dir: Path, kind: str, *parts: str) -> dict[str, Any] | None:
    if not runtime_cache_enabled():
        return None
    p = _cache_path(cache_dir, kind, *parts)
    if not p.exists():
        return None
    try:
        doc = json.loads(p.read_text(encoding="utf-8"))
        payload, _updated_at, env = extract_payload(doc)
        if isinstance(payload, dict) and isinstance(payload.get("scale_summary"), dict):
            return payload
        # ultra-legacy fallback
        if isinstance(doc, dict) and isinstance(doc.get("scale_summary"), dict):
            return doc
        # legacy wrapper using schema_version on top-level
        if isinstance(doc, dict) and doc.get("schema_version") == SYNOPTIC_CACHE_SCHEMA_VERSION and isinstance(doc.get("payload"), dict):
            return doc.get("payload")
        _ = env  # keep unpacked for forward-compatible extension
    except Exception:
        return None
    return None


def _write_cache(cache_dir: Path, kind: str, payload: dict[str, Any], *parts: str) -> None:
    if not runtime_cache_enabled():
        return
    cache_dir.mkdir(parents=True, exist_ok=True)
    p = _cache_path(cache_dir, kind, *parts)
    doc = make_cache_doc(
        payload,
        source_state="fresh",
        payload_schema_version=SYNOPTIC_CACHE_SCHEMA_VERSION,
        meta={"kind": kind},
    )
    p.write_text(json.dumps(doc, ensure_ascii=False), encoding="utf-8")


def _is_rate_limit_error(exc: Exception) -> bool:
    msg = str(exc)
    if "429" in msg or "Too Many Requests" in msg:
        return True
    stderr = getattr(exc, "stderr", None)
    if stderr and ("429" in str(stderr) or "Too Many Requests" in str(stderr)):
        return True
    return False


def _should_continue_to_next_provider(
    *,
    candidate_index: int,
    total_candidates: int,
    exc: Exception,
) -> bool:
    if not _is_rate_limit_error(exc):
        return True
    return candidate_index < max(0, total_candidates - 1)


def _classify_error_type(msg: str) -> str:
    s = str(msg or "").lower()
    if "package missing" in s or "modulenotfounderror" in s or "importerror" in s:
        return "dependency_missing"
    if "429" in s or "too many requests" in s or "rate_limit" in s:
        return "rate_limit_429"
    if "404" in s or "not found" in s:
        return "not_found_404"
    if "timed out" in s or "timeout" in s:
        return "timeout"
    if "connection" in s or "ssl" in s or "dns" in s:
        return "network"
    if "subprocess" in s or "parse failed" in s:
        return "subprocess"
    return "unknown"


def _short_err(exc: Exception | str, n: int = 280) -> str:
    t = str(exc)
    t = " ".join(t.split())
    return t[:n]


def _provider_backoff_enabled() -> bool:
    raw = str(os.getenv("WEATHERBOT_PROVIDER_BACKOFF", "1") or "1").strip().lower()
    return raw in {"1", "true", "yes", "on"}


def _provider_failure_scope(error_type: str, *, field_profile: str, runtime_tag: str) -> str:
    et = str(error_type or "unknown")
    if et in {"rate_limit_429", "timeout", "network", "dependency_missing"}:
        return "global"
    return f"{str(field_profile or 'full').strip().lower()}:{str(runtime_tag or '').strip()}"


def _provider_failure_ttl_seconds(error_type: str) -> int:
    et = str(error_type or "unknown")
    if et == "rate_limit_429":
        return 300
    if et in {"timeout", "network"}:
        return 120
    if et == "not_found_404":
        return 600
    if et == "dependency_missing":
        return 900
    return 180


def _provider_failure_path(cache_dir: Path, provider: str, scope: str) -> Path:
    key = _cache_key("provider-failure", str(provider or "").strip().lower(), str(scope or "").strip().lower())
    return cache_dir / f"provider_failure_{key}.json"


def _read_provider_failure_memo(cache_dir: Path, provider: str, scope: str) -> dict[str, Any] | None:
    if not _provider_backoff_enabled():
        return None
    p = _provider_failure_path(cache_dir, provider, scope)
    if not p.exists():
        return None
    try:
        doc = json.loads(p.read_text(encoding="utf-8"))
        expires_raw = str(doc.get("expires_at_utc") or "").strip()
        if not expires_raw:
            return None
        expires_at = datetime.fromisoformat(expires_raw.replace("Z", "+00:00"))
        if expires_at.tzinfo is None:
            expires_at = expires_at.replace(tzinfo=timezone.utc)
        if datetime.now(timezone.utc) >= expires_at.astimezone(timezone.utc):
            try:
                p.unlink()
            except Exception:
                pass
            return None
        return doc
    except Exception:
        return None


def _write_provider_failure_memo(
    cache_dir: Path,
    *,
    provider: str,
    scope: str,
    error_type: str,
    error: str,
) -> None:
    if not _provider_backoff_enabled():
        return
    cache_dir.mkdir(parents=True, exist_ok=True)
    now = datetime.now(timezone.utc)
    ttl_seconds = _provider_failure_ttl_seconds(error_type)
    payload = {
        "provider": str(provider or "").strip(),
        "scope": str(scope or "").strip(),
        "error_type": str(error_type or "unknown"),
        "error": _short_err(error, n=260),
        "updated_at_utc": now.isoformat().replace("+00:00", "Z"),
        "expires_at_utc": (now + timedelta(seconds=ttl_seconds)).isoformat().replace("+00:00", "Z"),
        "ttl_seconds": ttl_seconds,
    }
    _provider_failure_path(cache_dir, provider, scope).write_text(
        json.dumps(payload, ensure_ascii=False),
        encoding="utf-8",
    )


def _clear_provider_failure_memo(cache_dir: Path, provider: str, *scopes: str) -> None:
    if not _provider_backoff_enabled():
        return
    for scope in scopes:
        p = _provider_failure_path(cache_dir, provider, scope)
        try:
            if p.exists():
                p.unlink()
        except Exception:
            pass


def _shared_grid_enabled() -> bool:
    raw = str(os.getenv("FORECAST_SHARED_GRID_ENABLED", "1") or "1").strip().lower()
    return raw in {"1", "true", "yes", "on"}


def _shared_grid_quantize_ratio() -> float:
    try:
        return max(0.1, min(1.0, float(os.getenv("FORECAST_SHARED_GRID_QUANTIZE_RATIO", "0.5") or "0.5")))
    except Exception:
        return 0.5


def _shared_grid_axis_bounds(center: float, span: float, step: float) -> tuple[float, float]:
    if not _shared_grid_enabled():
        return float(center - span), float(center + span)

    ratio = _shared_grid_quantize_ratio()
    quantum = max(float(step), min(float(span), float(span) * ratio))
    min_bound = math.floor((float(center) - float(span)) / quantum) * quantum
    max_bound = min_bound + 2.0 * float(span)
    return round(min_bound, 6), round(max_bound, 6)


def _shared_request_bbox(st: Any, cfg: dict[str, Any]) -> tuple[float, float, float, float]:
    lat_span = float(cfg.get("lat_span") or 0.0)
    lon_span = float(cfg.get("lon_span") or 0.0)
    step = float(cfg.get("step") or 1.0)
    lat_min, lat_max = _shared_grid_axis_bounds(float(st.lat), lat_span, step)
    lon_min, lon_max = _shared_grid_axis_bounds(float(st.lon), lon_span, step)
    return lat_min, lat_max, lon_min, lon_max


def run_synoptic_section(
    *,
    st: Any,
    target_date: str,
    peak_local: str,
    tz_name: str,
    model: str,
    runtime_tag: str,
    scripts_dir: Path,
    cache_dir: Path,
    provider: str = DEFAULT_SYNOPTIC_PROVIDER,
    pass_mode: str = "full",
    perf_log: Callable[[str, float], None] | None = None,
) -> dict[str, Any]:
    mode = str(pass_mode or "full").strip().lower()
    if mode not in {"full", "inner_only", "outer500_only"}:
        mode = "full"

    cache_parts = (
        st.icao,
        target_date,
        model.lower(),
        str(provider or "").lower(),
        mode,
        runtime_tag,
        peak_local,
        tz_name,
        SYNOPTIC_CACHE_SCHEMA_VERSION,
    )
    cached = _read_cache(cache_dir, "synoptic", *cache_parts)
    if cached:
        return cached

    tz = ZoneInfo(tz_name)
    peak_local_dt = datetime.strptime(peak_local, "%Y-%m-%dT%H:%M").replace(tzinfo=tz)
    prev_local_dt = peak_local_dt - timedelta(hours=6)
    all_passes = [
        {
            "name": "inner",
            "lat_span": float(os.getenv("FORECAST_INNER_LAT_SPAN", "6.0") or "6.0"),
            "lon_span": float(os.getenv("FORECAST_INNER_LON_SPAN", "8.0") or "8.0"),
            "step": float(os.getenv("FORECAST_INNER_STEP_DEG", "1.0") or "1.0"),
            "batch": int(os.getenv("FORECAST_INNER_BATCH_SIZE", "80") or "80"),
            "field_profile": "full",
            "detector_mode": "full",
            "history": True,
        },
        {
            "name": "outer500",
            "lat_span": float(os.getenv("FORECAST_OUTER500_LAT_SPAN", "30.0") or "30.0"),
            "lon_span": float(os.getenv("FORECAST_OUTER500_LON_SPAN", "40.0") or "40.0"),
            "step": float(os.getenv("FORECAST_OUTER500_STEP_DEG", "2.0") or "2.0"),
            "batch": int(os.getenv("FORECAST_OUTER500_BATCH_SIZE", "120") or "120"),
            "field_profile": "outer500",
            "detector_mode": "outer500_only",
            "history": True,
        },
    ]
    if mode == "inner_only":
        passes = [all_passes[0]]
    elif mode == "outer500_only":
        passes = [all_passes[1]]
    else:
        passes = all_passes
    last_err: Exception | None = None
    collected: list[dict[str, Any]] = []
    pass_events: list[dict[str, Any]] = []

    for i, cfg in enumerate(passes, start=1):
        pass_t0 = time.perf_counter()
        ev: dict[str, Any] = {
            "pass": str(cfg.get("name") or i),
            "provider": str(provider or ""),
            "status": "running",
        }
        start_date = min(prev_local_dt.date(), peak_local_dt.date()).isoformat()
        end_date = max(prev_local_dt.date(), peak_local_dt.date()).isoformat()

        t_build = time.perf_counter()
        try:
            payload = None
            provider_errors: list[str] = []
            used_provider = str(provider or "")
            field_profile = str(cfg.get("field_profile") or "full")
            candidates = provider_candidates(provider)
            provider_error_details: list[dict[str, Any]] = []
            for idx, candidate_provider in enumerate(candidates):
                specific_scope = _provider_failure_scope(
                    "unknown",
                    field_profile=field_profile,
                    runtime_tag=runtime_tag,
                )
                active_memo = _read_provider_failure_memo(cache_dir, candidate_provider, "global") or _read_provider_failure_memo(
                    cache_dir,
                    candidate_provider,
                    specific_scope,
                )
                if active_memo:
                    provider_errors.append(
                        f"{candidate_provider}:backoff_active:{str(active_memo.get('error_type') or 'unknown')}"
                    )
                    provider_error_details.append(
                        {
                            "provider": candidate_provider,
                            "error_type": str(active_memo.get("error_type") or "unknown"),
                            "error": _short_err(str(active_memo.get("error") or "backoff_active"), n=220),
                            "backoff_active": True,
                            "backoff_scope": str(active_memo.get("scope") or ""),
                            "backoff_expires_at_utc": str(active_memo.get("expires_at_utc") or ""),
                        }
                    )
                    continue
                try:
                    lat_min, lat_max, lon_min, lon_max = _shared_request_bbox(st, cfg)
                    payload = build_synoptic_grid_payload(
                        candidate_provider,
                        station_icao=st.icao,
                        station_lat=float(st.lat),
                        station_lon=float(st.lon),
                        lat_min=lat_min,
                        lat_max=lat_max,
                        lon_min=lon_min,
                        lon_max=lon_max,
                        analysis_time_local=peak_local_dt.strftime("%Y-%m-%dT%H:%M"),
                        previous_time_local=prev_local_dt.strftime("%Y-%m-%dT%H:%M"),
                        tz_name=tz_name,
                        cycle_tag=runtime_tag,
                        field_profile=str(cfg.get("field_profile") or "full"),
                        root=scripts_dir.parents[2],
                    )
                    used_provider = candidate_provider
                    _clear_provider_failure_memo(
                        cache_dir,
                        candidate_provider,
                        "global",
                        specific_scope,
                    )
                    break
                except Exception as provider_exc:
                    error_type = _classify_error_type(str(provider_exc))
                    provider_errors.append(f"{candidate_provider}:{_short_err(provider_exc, n=180)}")
                    provider_error_details.append(
                        {
                            "provider": candidate_provider,
                            "error_type": error_type,
                            "error": _short_err(provider_exc, n=220),
                            "rate_limit": _is_rate_limit_error(provider_exc),
                        }
                    )
                    _write_provider_failure_memo(
                        cache_dir,
                        provider=candidate_provider,
                        scope=_provider_failure_scope(
                            error_type,
                            field_profile=field_profile,
                            runtime_tag=runtime_tag,
                        ),
                        error_type=error_type,
                        error=str(provider_exc),
                    )
                    last_err = provider_exc
                    if not _should_continue_to_next_provider(
                        candidate_index=idx,
                        total_candidates=len(candidates),
                        exc=provider_exc,
                    ):
                        break
            if payload is None:
                raise RuntimeError("; ".join(provider_errors) if provider_errors else "synoptic payload build failed")
            payload["_provider_used"] = used_provider
            payload["_provider_requested"] = str(provider or "")
            ev["build_s"] = round(time.perf_counter() - t_build, 3)
            ev["provider_used"] = used_provider
            if provider_error_details:
                ev["provider_errors"] = provider_error_details
            if used_provider != str(provider or ""):
                ev["provider_fallback"] = True
            if perf_log:
                perf_log(f"synoptic.{cfg['name']}.build", float(ev["build_s"]))
        except Exception as exc:
            last_err = exc
            ev.update({
                "status": "failed",
                "stage": "build",
                "error_type": _classify_error_type(str(exc)),
                "error": _short_err(exc),
                "elapsed_s": round(time.perf_counter() - pass_t0, 3),
            })
            if 'provider_error_details' in locals() and provider_error_details:
                ev["provider_errors"] = provider_error_details
            pass_events.append(ev)
            if perf_log:
                perf_log(f"synoptic.{cfg['name']}.failed", time.perf_counter() - pass_t0)
            if _is_rate_limit_error(exc):
                break
            continue

        try:
            t_detect = time.perf_counter()
            data = analyze_synoptic_2d(payload, mode=str(cfg.get("detector_mode") or "full"))
            data["_provider_used"] = str(payload.get("_provider_used") or used_provider)
            data["_provider_requested"] = str(payload.get("_provider_requested") or provider or "")
            for key in (
                "analysis_runtime_used",
                "analysis_fh_used",
                "analysis_stream_used",
                "previous_runtime_used",
                "previous_fh_used",
                "previous_stream_used",
            ):
                value = payload.get("grid_meta", {}).get(key)
                if value not in (None, "", [], {}):
                    data[key] = value
            ev["detect_s"] = round(time.perf_counter() - t_detect, 3)
            if perf_log:
                perf_log(f"synoptic.{cfg['name']}.detect", float(ev["detect_s"]))
        except Exception as exc:
            last_err = exc
            ev.update({
                "status": "failed",
                "stage": "detect",
                "error_type": _classify_error_type(str(exc)),
                "error": _short_err(exc),
                "elapsed_s": round(time.perf_counter() - pass_t0, 3),
            })
            pass_events.append(ev)
            if perf_log:
                perf_log(f"synoptic.{cfg['name']}.failed", time.perf_counter() - pass_t0)
            if _is_rate_limit_error(exc):
                break
            continue

        if cfg.get("name") == "outer500":
            cur = ((data.get("scale_summary") or {}).get("synoptic") or {}).get("systems", [])
            cur = [s for s in cur if str(s.get("level") or "") == "500"]
            data.setdefault("scale_summary", {}).setdefault("synoptic", {})["systems"] = cur

        systems_cnt = len(((data.get("scale_summary") or {}).get("synoptic") or {}).get("systems") or [])
        ev.update({
            "status": "ok",
            "stage": "done",
            "systems": int(systems_cnt),
            "elapsed_s": round(time.perf_counter() - pass_t0, 3),
        })
        pass_events.append(ev)
        if perf_log:
            perf_log(f"synoptic.{cfg['name']}.total", time.perf_counter() - pass_t0)
        collected.append(data)

    if collected:
        provider_used_values = [
            str((data.get("_provider_used") or provider or "")).strip()
            for data in collected
            if isinstance(data, dict)
        ]
        provider_used = provider_used_values[0] if provider_used_values else str(provider or "")
        provider_fallback = any(
            str((data.get("_provider_used") or provider or "")).strip() != str(provider or "").strip()
            for data in collected
            if isinstance(data, dict)
        )

        merged = {
            "analysis_time_utc": collected[0].get("analysis_time_utc") or peak_local_dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:00Z"),
            "analysis_time_local": peak_local_dt.strftime("%Y-%m-%dT%H:%M"),
            "station": {
                "icao": str(getattr(st, "icao", "")),
                "lat": float(getattr(st, "lat", 0.0)),
                "lon": float(getattr(st, "lon", 0.0)),
            },
            "scale_summary": {"synoptic": {"systems": []}},
            "_provider_requested": str(provider or ""),
            "_provider_used": provider_used,
        }
        for key in (
            "analysis_runtime_used",
            "analysis_fh_used",
            "analysis_stream_used",
            "previous_runtime_used",
            "previous_fh_used",
            "previous_stream_used",
        ):
            for data in collected:
                value = data.get(key)
                if value not in (None, "", [], {}):
                    merged[key] = value
                    break
        out = merged["scale_summary"]["synoptic"]["systems"]
        seen: set[tuple[str, str, int, int]] = set()
        for data in collected:
            systems = ((data.get("scale_summary") or {}).get("synoptic") or {}).get("systems", [])
            for s in systems:
                k = (
                    str(s.get("level") or ""),
                    str(s.get("system_type") or ""),
                    int(round(float(s.get("center_lat") or 0.0) * 10)),
                    int(round(float(s.get("center_lon") or 0.0) * 10)),
                )
                if k in seen:
                    continue
                seen.add(k)
                out.append(s)

        merged["_telemetry"] = {
            "provider_requested": str(provider or ""),
            "provider_used": provider_used,
            "provider_fallback": provider_fallback,
            "pass_mode": mode,
            "passes": pass_events,
            "degraded": any(str(e.get("status")) == "failed" for e in pass_events),
        }
        _write_cache(cache_dir, "synoptic", merged, *cache_parts)
        return merged

    stale = _read_cache(cache_dir, "synoptic", *cache_parts)
    if stale:
        stale.setdefault("_telemetry", {
            "provider": str(provider or ""),
            "pass_mode": mode,
            "passes": pass_events,
            "degraded": True,
            "from_cache": True,
        })
        return stale

    err_txt = _short_err(last_err or "synoptic_unknown_error")
    raise RuntimeError(f"synoptic section failed: {err_txt}")
