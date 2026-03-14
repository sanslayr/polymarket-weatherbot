from __future__ import annotations

import atexit
import csv
import fcntl
import json
import os
import tempfile
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import asdict, is_dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

from venv_utils import repo_venv_python


def _reexec_into_skill_venv() -> None:
    if str(os.getenv("WEATHERBOT_SKIP_VENV_REEXEC", "0") or "0").strip().lower() in {"1", "true", "yes", "on"}:
        return
    script_path = Path(__file__).resolve()
    venv_python = repo_venv_python(script_path.parent.parent)
    if not venv_python.exists():
        return
    current_python = Path(os.path.realpath(os.sys.executable)) if os.sys.executable else None
    try:
        target_python = venv_python.resolve()
    except FileNotFoundError:
        return
    if current_python == target_python:
        return
    env = dict(os.environ)
    env["WEATHERBOT_SKIP_VENV_REEXEC"] = "1"
    os.execvpe(str(target_python), [str(target_python), str(script_path), *os.sys.argv[1:]], env)


_reexec_into_skill_venv()

from hourly_data_service import (
    build_post_eval_window,
    build_post_focus_window,
    detect_tmax_windows,
    fetch_hourly_openmeteo,
    prune_runtime_cache,
    slice_hourly_local_day,
)
from metar_analysis_service import metar_observation_block
from metar_utils import fetch_metar_24h, metar_obs_time_utc
from realtime_pipeline import classify_window_phase
from forecast_analysis_cache import build_and_cache_forecast_analysis
from ecmwf_ensemble_factor_service import build_ecmwf_ensemble_factor_batch
from historical_context_provider import build_historical_context, historical_context_enabled
from historical_payload import attach_historical_payload
from station_catalog import (
    DEFAULT_STATION_CSV,
    Station,
    direction_factor_for,
    factor_summary_for,
    site_tag_for,
    station_timezone_name,
    terrain_tag_for,
)
from temperature_shape_analysis import analyze_temperature_shape
from temperature_window_resolver import resolve_temperature_window


ROOT = Path(__file__).resolve().parent.parent
STATE_DIR = ROOT / "cache" / "runtime" / "forecast_cache_worker"
STATE_DIR.mkdir(parents=True, exist_ok=True)
STATE_PATH = STATE_DIR / "state.json"
PID_PATH = STATE_DIR / "worker.pid"
LOG_PATH = STATE_DIR / "worker.log"
LOCK_PATH = STATE_DIR / "worker.lock"
CACHE_DIR = ROOT / "cache" / "runtime"
_LOCK_FD: int | None = None


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _json_safe(value: Any) -> Any:
    if is_dataclass(value):
        return _json_safe(asdict(value))
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, dict):
        return {str(k): _json_safe(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [_json_safe(item) for item in value]
    if isinstance(value, set):
        return [_json_safe(item) for item in sorted(value, key=str)]
    return value


def _write_json_atomic(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp_path = tempfile.mkstemp(prefix=f".{path.name}.", suffix=".tmp", dir=str(path.parent))
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as handle:
            handle.write(json.dumps(payload, ensure_ascii=False))
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(tmp_path, path)
    finally:
        try:
            os.unlink(tmp_path)
        except FileNotFoundError:
            pass


def _release_singleton_lock() -> None:
    global _LOCK_FD
    if _LOCK_FD is None:
        return
    try:
        fcntl.flock(_LOCK_FD, fcntl.LOCK_UN)
    except Exception:
        pass
    try:
        os.close(_LOCK_FD)
    except Exception:
        pass
    _LOCK_FD = None


def _acquire_singleton_lock() -> None:
    global _LOCK_FD
    fd = os.open(LOCK_PATH, os.O_CREAT | os.O_RDWR, 0o644)
    try:
        fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except BlockingIOError as exc:
        holder = ""
        try:
            with os.fdopen(os.dup(fd), "r", encoding="utf-8", errors="ignore") as handle:
                holder = handle.read().strip()
        finally:
            os.close(fd)
        raise RuntimeError(f"forecast_cache_worker already running ({holder or 'lock held'})") from exc
    os.ftruncate(fd, 0)
    os.write(fd, f"{os.getpid()}\n".encode("ascii", errors="ignore"))
    _LOCK_FD = fd
    atexit.register(_release_singleton_lock)


def _normalize_cycle_tag(value: Any) -> str:
    text = str(value or "").strip().upper()
    if len(text) == 11 and text.endswith("Z") and text[:10].isdigit():
        return text
    return ""


def _extract_synoptic_runtime_state(
    *,
    forecast_decision: dict[str, Any],
    synoptic: dict[str, Any] | None,
) -> dict[str, Any]:
    quality = dict(forecast_decision.get("quality") or {})
    syn = dict(synoptic or {})
    actual_runtime_tag = _normalize_cycle_tag(
        syn.get("analysis_runtime_used") or quality.get("synoptic_analysis_runtime_used")
    )
    previous_runtime_tag = _normalize_cycle_tag(
        syn.get("previous_runtime_used") or quality.get("synoptic_previous_runtime_used")
    )
    runtime_values = [
        _normalize_cycle_tag(item)
        for item in (syn.get("analysis_runtime_used_values") or quality.get("synoptic_analysis_runtime_used_values") or [])
    ]
    runtime_values = [item for item in runtime_values if item]
    runtime_mixed = bool(syn.get("analysis_runtime_used_mixed") or quality.get("synoptic_analysis_runtime_used_mixed"))
    if not runtime_mixed and len(set(runtime_values)) > 1:
        runtime_mixed = True
    source_state = str(quality.get("source_state") or "").strip().lower()
    missing_layers = sorted({str(item) for item in (quality.get("missing_layers") or []) if str(item)})
    synoptic_complete = bool(actual_runtime_tag) and not runtime_mixed and source_state != "degraded" and "synoptic" not in missing_layers
    return {
        "actual_runtime_tag": actual_runtime_tag,
        "previous_runtime_tag": previous_runtime_tag,
        "runtime_mixed": runtime_mixed,
        "runtime_values": runtime_values,
        "source_state": source_state,
        "missing_layers": missing_layers,
        "synoptic_complete": synoptic_complete,
    }


def _target_cycle_satisfied(payload: dict[str, Any], target_cycle: str) -> bool:
    return bool(payload.get("synoptic_complete")) and _normalize_cycle_tag(payload.get("actual_runtime_tag")) == _normalize_cycle_tag(target_cycle)


def _load_state() -> dict[str, Any]:
    if not STATE_PATH.exists():
        return {"last_runs": {}}
    try:
        return json.loads(STATE_PATH.read_text(encoding="utf-8"))
    except Exception:
        return {"last_runs": {}}


def _save_state(state: dict[str, Any]) -> None:
    _write_json_atomic(STATE_PATH, state)


def _load_station_rows() -> list[dict[str, str]]:
    with DEFAULT_STATION_CSV.open(newline="", encoding="utf-8") as f:
        return list(csv.DictReader(f))


def _station_from_row(row: dict[str, str]) -> Station:
    return Station(
        city=str(row.get("city") or "").strip(),
        icao=str(row.get("icao") or "").strip().upper(),
        lat=float(row.get("lat") or 0.0),
        lon=float(row.get("lon") or 0.0),
    )


def _target_dates_for_station(station: Station, *, now_utc: datetime, days_ahead: int) -> list[str]:
    tz = ZoneInfo(station_timezone_name(station))
    base_local = now_utc.astimezone(tz).date()
    return [(base_local + timedelta(days=offset)).isoformat() for offset in range(max(0, days_ahead) + 1)]


def _ecmwf_cycle_runtime_tag(now_utc: datetime) -> str:
    hh = (now_utc.hour // 6) * 6
    return f"{now_utc.strftime('%Y%m%d')}{hh:02d}Z"


def _cycle_base_time(cycle_tag: str) -> datetime:
    return datetime.strptime(cycle_tag, "%Y%m%d%HZ").replace(tzinfo=timezone.utc)


def _ecmwf_cycle_release_time(now_utc: datetime, lag_minutes: int) -> tuple[str, datetime]:
    tag = _ecmwf_cycle_runtime_tag(now_utc)
    base = _cycle_base_time(tag)
    return tag, base + timedelta(minutes=max(0, lag_minutes))


def _ecmwf_cycle_probe_start(now_utc: datetime, start_delay_hours: int) -> tuple[str, datetime]:
    tag = _ecmwf_cycle_runtime_tag(now_utc)
    base = _cycle_base_time(tag)
    return tag, base + timedelta(hours=max(0, start_delay_hours))


def _active_probe_cycle_tag(now_utc: datetime, start_delay_hours: int, stop_after_hours: int) -> str | None:
    current_tag, current_probe_start = _ecmwf_cycle_probe_start(now_utc, start_delay_hours)
    current_probe_end = _cycle_base_time(current_tag) + timedelta(hours=max(0, stop_after_hours))
    if current_probe_start <= now_utc < current_probe_end:
        return current_tag
    prev = now_utc - timedelta(hours=6)
    prev_tag, _prev_probe_start = _ecmwf_cycle_probe_start(prev, start_delay_hours)
    prev_probe_end = _cycle_base_time(prev_tag) + timedelta(hours=max(0, stop_after_hours))
    if _prev_probe_start <= now_utc < prev_probe_end:
        return prev_tag
    return None


def _should_probe_cycle(state: dict[str, Any], cycle_tag: str, now_utc: datetime, poll_minutes: int) -> bool:
    probe_state = dict((state.get("probe_state") or {}).get(cycle_tag) or {})
    raw = str(probe_state.get("last_probe_at_utc") or "").strip()
    if not raw:
        return True
    try:
        ts = datetime.fromisoformat(raw.replace("Z", "+00:00"))
        if ts.tzinfo is None:
            ts = ts.replace(tzinfo=timezone.utc)
    except Exception:
        return True
    return (now_utc - ts.astimezone(timezone.utc)).total_seconds() >= max(1, poll_minutes) * 60


def _cycle_tag_is_not_older(candidate: str, baseline: str) -> bool:
    return bool(candidate and baseline and candidate >= baseline)


def _run_synoptic_for_worker(
    st: Station,
    target_date: str,
    peak_local: str,
    tz_name: str,
    model: str,
    runtime_tag: str,
    pass_mode: str = "full",
) -> dict[str, Any]:
    from synoptic_provider_router import DEFAULT_SYNOPTIC_PROVIDER, normalize_synoptic_provider
    from synoptic_runner import run_synoptic_section as _run_synoptic_section

    return _run_synoptic_section(
        st=st,
        target_date=target_date,
        peak_local=peak_local,
        tz_name=tz_name,
        model=model,
        runtime_tag=runtime_tag,
        scripts_dir=ROOT / "scripts",
        cache_dir=ROOT / "cache" / "runtime",
        provider=normalize_synoptic_provider(os.getenv("FORECAST_3D_PROVIDER", DEFAULT_SYNOPTIC_PROVIDER)),
        pass_mode=pass_mode,
        perf_log=None,
    )


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
        site_tag=site_tag_for(station_icao),
        terrain_tag=terrain_tag_for(station_icao),
        direction_factor=direction_factor_for(station_icao),
        factor_summary=factor_summary_for(station_icao),
    )
    attach_historical_payload(metar_diag, historical_context)
    return historical_context


def _build_analysis_window(
    *,
    station: Station,
    target_date: str,
    tz_name: str,
) -> tuple[dict[str, Any], dict[str, Any], list[dict[str, Any]], dict[str, Any], dict[str, Any], dict[str, Any], dict[str, Any], str, str]:
    model = "ecmwf"
    hourly_payload = fetch_hourly_openmeteo(station, target_date, model)
    hourly_day = slice_hourly_local_day(hourly_payload["hourly"], target_date)
    unit_pref = "F" if str(station.icao).upper().startswith("K") else "C"
    metar24 = fetch_metar_24h(station.icao, force_refresh=False)
    _metar_text, metar_diag = metar_observation_block(
        metar24,
        hourly_day,
        tz_name,
        target_date=target_date,
        temp_unit=unit_pref,
    )
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
    analysis_window = dict(primary)
    try:
        gate_pre = classify_window_phase(primary, metar_diag)
        if str(gate_pre.get("phase") or "") == "post":
            post_focus = build_post_focus_window(hourly_day, metar_diag)
            if isinstance(post_focus, dict) and post_focus.get("peak_local"):
                analysis_window = post_focus
            else:
                post_eval = build_post_eval_window(hourly_day, metar_diag)
                if isinstance(post_eval, dict) and post_eval.get("peak_local"):
                    analysis_window = post_eval
    except Exception:
        analysis_window = dict(primary)

    return hourly_payload, hourly_day, metar24, metar_diag, primary, analysis_window, temp_shape_analysis, unit_pref, model


def _prepare_station_target_context(station: Station, target_date: str) -> dict[str, Any]:
    from forecast_pipeline import load_or_build_forecast_decision
    from synoptic_provider_router import DEFAULT_SYNOPTIC_PROVIDER, normalize_synoptic_provider

    tz_name = station_timezone_name(station)
    tz = ZoneInfo(tz_name)
    (
        hourly_payload,
        hourly_day,
        metar24,
        metar_diag,
        primary_window,
        analysis_window,
        temp_shape_analysis,
        unit_pref,
        model,
    ) = _build_analysis_window(
        station=station,
        target_date=target_date,
        tz_name=tz_name,
    )
    now_utc = _utc_now()
    now_local = now_utc.astimezone(tz)
    synoptic_provider = normalize_synoptic_provider(os.getenv("FORECAST_3D_PROVIDER", DEFAULT_SYNOPTIC_PROVIDER))
    forecast_decision, synoptic, synoptic_error = load_or_build_forecast_decision(
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
        run_synoptic_fn=_run_synoptic_for_worker,
        perf_log=None,
        prefer_cached_synoptic=False,
    )
    quality = dict(forecast_decision.get("quality") or {})
    runtime_state = _extract_synoptic_runtime_state(
        forecast_decision=forecast_decision,
        synoptic=synoptic,
    )
    historical_context = _attach_historical_context(
        metar_diag,
        station_icao=station.icao,
        target_date=target_date,
        forecast_decision=forecast_decision,
        synoptic_context=synoptic,
    )
    runtime_pref = (
        runtime_state.get("actual_runtime_tag")
        or str((forecast_decision.get("meta") or {}).get("runtime") or "")
        or _ecmwf_cycle_runtime_tag(now_utc)
    )
    return {
        "station": station,
        "target_date": target_date,
        "tz_name": tz_name,
        "hourly_payload": hourly_payload,
        "hourly_day": hourly_day,
        "metar24": metar24,
        "metar_diag": metar_diag,
        "primary_window": primary_window,
        "analysis_window": analysis_window,
        "temp_shape_analysis": temp_shape_analysis,
        "unit_pref": unit_pref,
        "model": model,
        "now_utc": now_utc,
        "now_local": now_local,
        "synoptic_provider": synoptic_provider,
        "forecast_decision": forecast_decision,
        "synoptic": synoptic,
        "synoptic_error": synoptic_error,
        "quality": quality,
        "runtime_state": runtime_state,
        "historical_context": historical_context,
        "runtime_pref": runtime_pref,
    }


def _finalize_station_target_context(
    prepared: dict[str, Any],
    *,
    prefetched_ensemble_factor: dict[str, Any] | None = None,
) -> dict[str, Any]:
    station: Station = prepared["station"]
    target_date = str(prepared["target_date"])
    metar_diag = dict(prepared["metar_diag"] or {})
    metar24 = list(prepared.get("metar24") or [])
    hourly_day = dict(prepared.get("hourly_day") or {})
    analysis_window = dict(prepared["analysis_window"] or {})
    primary_window = dict(prepared["primary_window"] or {})
    temp_shape_analysis = dict(prepared["temp_shape_analysis"] or {})
    forecast_decision = dict(prepared["forecast_decision"] or {})
    quality = dict(prepared["quality"] or {})
    runtime_state = dict(prepared["runtime_state"] or {})
    historical_context = dict(prepared["historical_context"] or {})
    runtime_pref = str(prepared.get("runtime_pref") or "")
    synoptic_provider = str(prepared.get("synoptic_provider") or "")
    unit_pref = str(prepared.get("unit_pref") or "C")
    model = str(prepared.get("model") or "ecmwf")
    tz_name = str(prepared.get("tz_name") or "")
    synoptic_error = prepared.get("synoptic_error")

    analysis_cache_payload: dict[str, Any] | None = None
    analysis_cache_error = ""
    try:
        analysis_cache_payload = build_and_cache_forecast_analysis(
            station_icao=station.icao,
            station_lat=float(station.lat),
            station_lon=float(station.lon),
            target_date=target_date,
            model=model,
            synoptic_provider=str(quality.get("synoptic_provider_used") or synoptic_provider),
            runtime_tag=runtime_pref,
            primary_window=primary_window,
            synoptic_window=analysis_window,
            metar_diag=metar_diag,
            forecast_decision=forecast_decision,
            temp_shape_analysis=temp_shape_analysis,
            temp_unit=unit_pref,
            tz_name=tz_name,
            metar24=metar24,
            prefetched_ensemble_factor=prefetched_ensemble_factor,
        )
    except Exception as exc:
        analysis_cache_error = str(exc)
    return {
        "station": station,
        "target_date": target_date,
        "requested_runtime_tag": forecast_decision.get("meta", {}).get("runtime_requested") or forecast_decision.get("meta", {}).get("runtime"),
        "actual_runtime_tag": runtime_state.get("actual_runtime_tag"),
        "previous_runtime_tag": runtime_state.get("previous_runtime_tag"),
        "runtime_mixed": runtime_state.get("runtime_mixed"),
        "runtime_values": runtime_state.get("runtime_values"),
        "hourly_provider": "openmeteo",
        "synoptic_provider": synoptic_provider,
        "analysis_peak_local": analysis_window.get("peak_local"),
        "hourly_points": len(hourly_day.get("time") or []),
        "observed_max_temp_c": metar_diag.get("observed_max_temp_c"),
        "latest_report_local": metar_diag.get("latest_report_local"),
        "historical_context_available": bool((historical_context or {}).get("available", historical_context is not None)),
        "forecast_analysis_cached": bool(analysis_cache_payload),
        "analysis_snapshot_cached": bool((analysis_cache_payload or {}).get("analysis_snapshot")),
        "ensemble_factor_cached": bool((analysis_cache_payload or {}).get("ensemble_factor")),
        "analysis_cache_runtime": runtime_pref,
        "analysis_cache_error": analysis_cache_error,
        "source_state": runtime_state.get("source_state") or quality.get("source_state"),
        "missing_layers": runtime_state.get("missing_layers"),
        "synoptic_complete": runtime_state.get("synoptic_complete"),
        "synoptic_provider_used": quality.get("synoptic_provider_used"),
        "synoptic_error": str(synoptic_error or ""),
        "success": bool(runtime_state.get("synoptic_complete")),
    }


def _prewarm_station_target(station: Station, target_date: str) -> dict[str, Any]:
    prepared = _prepare_station_target_context(station, target_date)
    return _finalize_station_target_context(prepared)


def _prefetch_surface_ens_for_prepared_tasks(
    prepared_tasks: list[dict[str, Any]],
    *,
    log_handle,
) -> dict[str, dict[str, Any]]:
    requests: list[dict[str, Any]] = []
    for prepared in prepared_tasks:
        station: Station = prepared["station"]
        analysis_window = dict(prepared.get("analysis_window") or {})
        metar_diag = dict(prepared.get("metar_diag") or {})
        model = str(prepared.get("model") or "")
        runtime_pref = str(prepared.get("runtime_pref") or "")
        target_date = str(prepared.get("target_date") or "")
        tz_name = str(prepared.get("tz_name") or "")
        peak_local = str(analysis_window.get("peak_local") or "").strip()
        if model.lower() != "ecmwf" or not peak_local or not runtime_pref:
            continue
        requests.append(
            {
                "request_id": f"{station.icao}|{target_date}",
                "station_icao": station.icao,
                "station_lat": float(station.lat),
                "station_lon": float(station.lon),
                "peak_local": peak_local,
                "analysis_local": str(metar_diag.get("latest_report_local") or ""),
                "tz_name": tz_name,
                "preferred_runtime_tag": runtime_pref,
                "metar24": list(prepared.get("metar24") or []),
            }
        )

    if not requests:
        return {}

    started_at = _utc_now().isoformat().replace("+00:00", "Z")
    try:
        payloads = build_ecmwf_ensemble_factor_batch(requests=requests, detail_stage="auto", root=ROOT)
        ready_count = sum(
            1
            for payload in payloads.values()
            if isinstance(payload, dict)
            and str(((payload.get("source") or {}).get("detail_level") or "")) in {"surface_anchor", "surface_trajectory"}
        )
        log_handle.write(
            f"{_utc_now().isoformat().replace('+00:00', 'Z')} ENS_SURFACE_BATCH "
            f"{json.dumps({'requested': len(requests), 'ready': ready_count, 'started_at': started_at}, ensure_ascii=False)}\n"
        )
        log_handle.flush()
        return payloads
    except Exception as exc:
        log_handle.write(
            f"{_utc_now().isoformat().replace('+00:00', 'Z')} ENS_SURFACE_BATCH_ERROR "
            f"{json.dumps({'requested': len(requests), 'error': str(exc), 'started_at': started_at}, ensure_ascii=False)}\n"
        )
        log_handle.flush()
        return {}


def _should_run(last_runs: dict[str, Any], run_key: str, interval_seconds: int) -> bool:
    last = dict(last_runs.get(run_key) or {})
    raw = str(last.get("started_at_utc") or "").strip()
    if not raw:
        return True
    try:
        ts = datetime.fromisoformat(raw.replace("Z", "+00:00"))
        if ts.tzinfo is None:
            ts = ts.replace(tzinfo=timezone.utc)
    except Exception:
        return True
    return (_utc_now() - ts.astimezone(timezone.utc)).total_seconds() >= interval_seconds


def _latest_metar_signature(station: Station) -> str:
    try:
        rows = fetch_metar_24h(station.icao, force_refresh=False)
    except Exception:
        return ""
    latest_dt = None
    for raw in rows or []:
        if not isinstance(raw, dict):
            continue
        try:
            dt = metar_obs_time_utc(raw)
        except Exception:
            continue
        if latest_dt is None or dt > latest_dt:
            latest_dt = dt
    if latest_dt is None:
        return ""
    return latest_dt.isoformat().replace("+00:00", "Z")


def _purge_old_forecast_cache(*, station_icao: str, target_date: str, keep_runtime_tag: str) -> dict[str, int]:
    removed = {"forecast_decision": 0, "forecast_3d_bundle": 0, "forecast_analysis": 0}
    for path in CACHE_DIR.glob("forecast_decision_*.json"):
        try:
            doc = json.loads(path.read_text(encoding="utf-8"))
            payload = doc.get("payload") or {}
            meta = payload.get("meta") or {}
            if str(meta.get("station") or "") != station_icao:
                continue
            if str(meta.get("date") or "") != target_date:
                continue
            runtime_tag = str(meta.get("runtime") or "")
            if not runtime_tag or runtime_tag == keep_runtime_tag:
                continue
            path.unlink()
            removed["forecast_decision"] += 1
        except Exception:
            continue
    for path in CACHE_DIR.glob("forecast_3d_bundle_*.json"):
        try:
            doc = json.loads(path.read_text(encoding="utf-8"))
            if str(doc.get("station") or "") != station_icao:
                continue
            if str(doc.get("date") or "") != target_date:
                continue
            runtime_tag = str(doc.get("runtime") or "")
            if not runtime_tag or runtime_tag == keep_runtime_tag:
                continue
            path.unlink()
            removed["forecast_3d_bundle"] += 1
        except Exception:
            continue
    for path in CACHE_DIR.glob("forecast_analysis_*.json"):
        try:
            doc = json.loads(path.read_text(encoding="utf-8"))
            payload = doc.get("payload") if isinstance(doc.get("payload"), dict) else doc
            if str(payload.get("station") or "") != station_icao:
                continue
            if str(payload.get("target_date") or "") != target_date:
                continue
            runtime_tag = str(payload.get("runtime_tag") or "")
            if not runtime_tag or runtime_tag == keep_runtime_tag:
                continue
            path.unlink()
            removed["forecast_analysis"] += 1
        except Exception:
            continue
    return removed


def _purge_stale_forecast_cache(*, max_age_hours: int) -> dict[str, int]:
    removed = {"forecast_decision": 0, "forecast_3d_bundle": 0, "forecast_analysis": 0}
    if max_age_hours <= 0:
        return removed
    cutoff = _utc_now() - timedelta(hours=max_age_hours)
    for prefix in ("forecast_decision", "forecast_3d_bundle", "forecast_analysis"):
        for path in CACHE_DIR.glob(f"{prefix}_*.json"):
            try:
                modified_at = datetime.fromtimestamp(path.stat().st_mtime, tz=timezone.utc)
            except Exception:
                continue
            if modified_at > cutoff:
                continue
            try:
                path.unlink()
                removed[prefix] += 1
            except FileNotFoundError:
                continue
    return removed


def _purge_legacy_surface_incompatible_cache() -> dict[str, int]:
    removed = {
        "forecast_analysis": 0,
        "ecmwf_ensemble_factor": 0,
    }
    for path in CACHE_DIR.glob("forecast_analysis_*.json"):
        try:
            raw = json.loads(path.read_text(encoding="utf-8"))
            payload = raw.get("payload") if isinstance(raw.get("payload"), dict) else raw
            if not isinstance(payload, dict):
                payload = {}
            schema_version = str(payload.get("schema_version") or "")
            ensemble_factor = dict(payload.get("ensemble_factor") or {})
            if schema_version == "forecast-analysis-cache.v8":
                detail_level = str(((ensemble_factor.get("source") or {}).get("detail_level") or "")).strip().lower()
                has_surface = bool(ensemble_factor.get("members") or ensemble_factor.get("member_trajectory") or ensemble_factor.get("daily_surface_timeline"))
                if detail_level in {"surface_anchor", "surface_trajectory"} and has_surface:
                    continue
            path.unlink()
            removed["forecast_analysis"] += 1
        except FileNotFoundError:
            continue
        except Exception:
            try:
                path.unlink()
                removed["forecast_analysis"] += 1
            except FileNotFoundError:
                continue
    for path in CACHE_DIR.glob("ecmwf_ensemble_factor_*.json"):
        try:
            raw = json.loads(path.read_text(encoding="utf-8"))
            payload = raw.get("payload") if isinstance(raw.get("payload"), dict) else raw
            if not isinstance(payload, dict):
                payload = {}
            schema_version = str(payload.get("schema_version") or "")
            detail_level = str(((payload.get("source") or {}).get("detail_level") or "")).strip().lower()
            has_surface = bool(payload.get("members") or payload.get("member_trajectory") or payload.get("daily_surface_timeline"))
            if schema_version == "ecmwf-ensemble-factor.v6" and detail_level in {"surface_anchor", "surface_trajectory"} and has_surface:
                continue
            path.unlink()
            removed["ecmwf_ensemble_factor"] += 1
        except FileNotFoundError:
            continue
        except Exception:
            try:
                path.unlink()
                removed["ecmwf_ensemble_factor"] += 1
            except FileNotFoundError:
                continue
    return removed


def main() -> None:
    _acquire_singleton_lock()
    PID_PATH.write_text(str(os.getpid()), encoding="utf-8")
    state = _load_state()
    loop_sleep_seconds = int(os.getenv("FORECAST_CACHE_PREWARM_POLL_SECONDS", "60") or "60")
    days_ahead = int(os.getenv("FORECAST_CACHE_PREWARM_DAYS_AHEAD", "1") or "1")
    max_workers = int(os.getenv("FORECAST_CACHE_PREWARM_MAX_WORKERS", "2") or "2")
    cycle_probe_start_hours = int(os.getenv("FORECAST_CACHE_PREWARM_CYCLE_START_HOURS", "3") or "3")
    cycle_poll_minutes = int(os.getenv("FORECAST_CACHE_PREWARM_CYCLE_POLL_MINUTES", "30") or "30")
    cycle_probe_stop_hours = int(os.getenv("FORECAST_CACHE_PREWARM_CYCLE_STOP_HOURS", "6") or "6")
    max_age_hours = int(os.getenv("FORECAST_CACHE_MAX_AGE_HOURS", "24") or "24")
    always_on = str(os.getenv("FORECAST_CACHE_PREWARM_ALWAYS_ON", "1") or "1").strip().lower() in {"1", "true", "yes", "on"}
    continuous_interval_seconds = int(os.getenv("FORECAST_CACHE_CONTINUOUS_INTERVAL_SECONDS", "900") or "900")

    with LOG_PATH.open("a", encoding="utf-8") as log:
        legacy_purged = _purge_legacy_surface_incompatible_cache()
        if any(legacy_purged.values()):
            log.write(
                f"{_utc_now().isoformat().replace('+00:00', 'Z')} PURGE_LEGACY "
                f"{json.dumps(legacy_purged, ensure_ascii=False)}\n"
            )
            log.flush()
        while True:
            now_utc = _utc_now()
            prune_runtime_cache(max_age_hours=max_age_hours)
            stale_purged = _purge_stale_forecast_cache(max_age_hours=max_age_hours)
            if any(stale_purged.values()):
                log.write(
                    f"{_utc_now().isoformat().replace('+00:00', 'Z')} PURGE_STALE "
                    f"{json.dumps(stale_purged, ensure_ascii=False)}\n"
                )
                log.flush()

            active_cycle = ""
            target_cycle_label = "continuous"
            if not always_on:
                active_cycle = _active_probe_cycle_tag(now_utc, cycle_probe_start_hours, cycle_probe_stop_hours) or ""
                if not active_cycle:
                    time.sleep(max(15, loop_sleep_seconds))
                    continue
                target_cycle_label = active_cycle
                last_cycle_tag = str((state.get("last_cycle") or {}).get("cycle_tag") or "")
                if _cycle_tag_is_not_older(last_cycle_tag, active_cycle):
                    time.sleep(max(15, loop_sleep_seconds))
                    continue
                if not _should_probe_cycle(state, active_cycle, now_utc, cycle_poll_minutes):
                    time.sleep(max(15, loop_sleep_seconds))
                    continue
                state.setdefault("probe_state", {})[active_cycle] = {
                    "last_probe_at_utc": now_utc.isoformat().replace("+00:00", "Z"),
                }
                _save_state(state)

            tasks: list[tuple[Station, str, str]] = []
            for row in _load_station_rows():
                station = _station_from_row(row)
                latest_metar_sig = _latest_metar_signature(station) if always_on else ""
                for target_date in _target_dates_for_station(station, now_utc=now_utc, days_ahead=days_ahead):
                    if always_on:
                        run_key = f"{station.icao}|{target_date}"
                        last_run = dict((state.get("last_runs") or {}).get(run_key) or {})
                        if (
                            latest_metar_sig
                            and str(last_run.get("latest_report_local") or "") != latest_metar_sig
                        ) or _should_run(state.get("last_runs") or {}, run_key, continuous_interval_seconds):
                            tasks.append((station, target_date, latest_metar_sig))
                    else:
                        tasks.append((station, target_date, latest_metar_sig))

            if not tasks:
                time.sleep(max(15, loop_sleep_seconds))
                continue

            cycle_success = True
            prepared_task_records: list[tuple[dict[str, Any], str]] = []
            with ThreadPoolExecutor(max_workers=max_workers) as pool:
                future_map = {
                    pool.submit(_prepare_station_target_context, station, target_date): (station, target_date, metar_sig)
                    for station, target_date, metar_sig in tasks
                }
                for future in as_completed(future_map):
                    station, target_date, metar_sig = future_map[future]
                    run_key = f"{station.icao}|{target_date}"
                    started_at = _utc_now().isoformat().replace("+00:00", "Z")
                    try:
                        prepared = future.result()
                        prepared_task_records.append((prepared, metar_sig))
                    except Exception as exc:
                        cycle_success = False
                        payload = {
                            "station": station,
                            "target_date": target_date,
                            "cycle_tag": target_cycle_label,
                            "task_success": False,
                            "success": False,
                            "stage": "prepare",
                            "error": str(exc),
                        }
                        state.setdefault("last_runs", {})[run_key] = {
                            "started_at_utc": started_at,
                            "completed_at_utc": _utc_now().isoformat().replace("+00:00", "Z"),
                            "success": False,
                            "task_success": False,
                            "target_cycle_ready": False,
                            "cycle_tag": target_cycle_label,
                            "error": str(exc),
                            "latest_report_local": str(metar_sig or ""),
                        }
                        log.write(
                            f"{_utc_now().isoformat().replace('+00:00', 'Z')} PREWARM_ERROR "
                            f"{json.dumps(_json_safe(payload), ensure_ascii=False)}\n"
                        )
                        log.flush()

            prefetched_ens = _prefetch_surface_ens_for_prepared_tasks(
                [prepared for prepared, _metar_sig in prepared_task_records],
                log_handle=log,
            )

            with ThreadPoolExecutor(max_workers=max_workers) as pool:
                future_map = {}
                for prepared, metar_sig in prepared_task_records:
                    station: Station = prepared["station"]
                    target_date = str(prepared["target_date"])
                    request_id = f"{station.icao}|{target_date}"
                    future = pool.submit(
                        _finalize_station_target_context,
                        prepared,
                        prefetched_ensemble_factor=dict(prefetched_ens.get(request_id) or {}),
                    )
                    future_map[future] = (prepared, metar_sig)

                for future in as_completed(future_map):
                    prepared, metar_sig = future_map[future]
                    station: Station = prepared["station"]
                    target_date = str(prepared["target_date"])
                    run_key = f"{station.icao}|{target_date}"
                    started_at = _utc_now().isoformat().replace("+00:00", "Z")
                    try:
                        payload = future.result()
                        actual_runtime_tag = _normalize_cycle_tag(payload.get("actual_runtime_tag"))
                        requested_runtime_tag = _normalize_cycle_tag(payload.get("requested_runtime_tag"))
                        payload["target_cycle"] = target_cycle_label
                        payload["runtime_match"] = bool(active_cycle) and actual_runtime_tag == active_cycle
                        payload["task_success"] = True
                        payload["target_cycle_ready"] = True if always_on else _target_cycle_satisfied(payload, active_cycle)
                        payload["success"] = bool(payload["task_success"])
                        if not payload["target_cycle_ready"]:
                            cycle_success = False
                        purged = _purge_old_forecast_cache(
                            station_icao=station.icao,
                            target_date=target_date,
                            keep_runtime_tag=actual_runtime_tag,
                        ) if payload["target_cycle_ready"] and actual_runtime_tag else {
                            "forecast_decision": 0,
                            "forecast_3d_bundle": 0,
                            "forecast_analysis": 0,
                        }
                        payload["purged_cache"] = purged
                        state.setdefault("last_runs", {})[run_key] = {
                            "started_at_utc": started_at,
                            "completed_at_utc": _utc_now().isoformat().replace("+00:00", "Z"),
                            "success": True,
                            "task_success": True,
                            "target_cycle_ready": bool(payload["target_cycle_ready"]),
                            "cycle_tag": target_cycle_label,
                            "requested_runtime_tag": requested_runtime_tag,
                            "runtime_tag": actual_runtime_tag,
                            "runtime_match": bool(active_cycle) and actual_runtime_tag == active_cycle,
                            "runtime_mixed": bool(payload.get("runtime_mixed")),
                            "synoptic_complete": bool(payload.get("synoptic_complete")),
                            "missing_layers": list(payload.get("missing_layers") or []),
                            "source_state": payload.get("source_state"),
                            "synoptic_provider_used": payload.get("synoptic_provider_used"),
                            "latest_report_local": str(payload.get("latest_report_local") or metar_sig or ""),
                        }
                        log.write(
                            f"{_utc_now().isoformat().replace('+00:00', 'Z')} PREWARM "
                            f"{json.dumps(_json_safe(payload), ensure_ascii=False)}\n"
                        )
                    except Exception as exc:
                        cycle_success = False
                        payload = {
                            "station": station,
                            "target_date": target_date,
                            "cycle_tag": target_cycle_label,
                            "task_success": False,
                            "success": False,
                            "stage": "finalize",
                            "error": str(exc),
                        }
                        state.setdefault("last_runs", {})[run_key] = {
                            "started_at_utc": started_at,
                            "completed_at_utc": _utc_now().isoformat().replace("+00:00", "Z"),
                            "success": False,
                            "task_success": False,
                            "target_cycle_ready": False,
                            "cycle_tag": target_cycle_label,
                            "error": str(exc),
                            "latest_report_local": str(metar_sig or ""),
                        }
                        log.write(
                            f"{_utc_now().isoformat().replace('+00:00', 'Z')} PREWARM_ERROR "
                            f"{json.dumps(_json_safe(payload), ensure_ascii=False)}\n"
                        )
                    finally:
                        log.flush()
            if cycle_success and not always_on:
                state["last_cycle"] = {
                    "cycle_tag": active_cycle,
                    "completed_at_utc": _utc_now().isoformat().replace("+00:00", "Z"),
                    "success": True,
                }
            elif not always_on:
                state["probe_state"][active_cycle] = {
                    "last_probe_at_utc": _utc_now().isoformat().replace("+00:00", "Z"),
                    "last_result": "pending",
                }
            _save_state(state)
            time.sleep(max(15, loop_sleep_seconds))


if __name__ == "__main__":
    main()
