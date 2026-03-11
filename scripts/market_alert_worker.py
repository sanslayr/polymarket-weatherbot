from __future__ import annotations

import atexit
import csv
import fcntl
import json
import os
import sys
import tempfile
import time
from collections import Counter
from concurrent.futures import Future, ThreadPoolExecutor
from dataclasses import asdict, is_dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo


def _reexec_into_skill_venv() -> None:
    if str(os.getenv("WEATHERBOT_SKIP_VENV_REEXEC", "0") or "0").strip().lower() in {"1", "true", "yes", "on"}:
        return
    script_path = Path(__file__).resolve()
    venv_python = script_path.parent.parent / ".venv_gfs" / "bin" / "python"
    if not venv_python.exists():
        return
    current_python = Path(os.path.realpath(sys.executable)) if sys.executable else None
    try:
        target_python = venv_python.resolve()
    except FileNotFoundError:
        return
    if current_python == target_python:
        return
    env = dict(os.environ)
    env["WEATHERBOT_SKIP_VENV_REEXEC"] = "1"
    os.execvpe(str(target_python), [str(target_python), str(script_path), *sys.argv[1:]], env)


_reexec_into_skill_venv()

from build_station_links import format_polymarket_date_slug
from market_signal_alert_service import format_market_signal_alert
from metar_utils import extract_observed_max_for_local_day, fetch_metar_24h, is_intish_value, is_routine_metar_report, metar_obs_time_utc
from station_catalog import DEFAULT_STATION_CSV, Station, station_timezone_name
from telegram_notifier import send_telegram_messages_report


ROOT = Path(__file__).resolve().parent.parent
SCHEDULE_CONFIG_PATH = ROOT / "config" / "market_alert_station_schedule.json"
STATE_DIR = ROOT / "cache" / "runtime" / "market_alert_worker"
STATE_DIR.mkdir(parents=True, exist_ok=True)
STATE_PATH = STATE_DIR / "state.json"
PID_PATH = STATE_DIR / "worker.pid"
LOG_PATH = STATE_DIR / "worker.log"
LOCK_PATH = STATE_DIR / "worker.lock"
_SCHEDULE_CACHE_MTIME_NS: int | None = None
_SCHEDULE_CACHE: dict[str, Any] = {}
_LOCK_FD: int | None = None


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _event_window_warmup_seconds() -> float:
    return 0.0


def _event_window_stream_seconds() -> float:
    configured = str(os.getenv("MARKET_EVENT_WINDOW_STREAM_SECONDS") or "").strip()
    if configured:
        try:
            return max(1.0, float(configured))
        except Exception:
            pass
    return 245.0


def _window_stream_seconds_remaining(window_end: datetime, now_utc: datetime) -> float | None:
    remaining_seconds = (window_end - now_utc).total_seconds()
    if remaining_seconds < 1.0:
        return None
    return max(1.0, min(_event_window_stream_seconds(), remaining_seconds))


def _loop_sleep_seconds(*, next_wake: datetime | None, now_utc: datetime, has_active_tasks: bool) -> float:
    if next_wake is None and not has_active_tasks:
        return 60.0
    if next_wake is None:
        return 1.0 if has_active_tasks else 60.0
    delay_seconds = (next_wake - now_utc).total_seconds()
    if has_active_tasks:
        return max(0.5, min(5.0, delay_seconds))
    return max(1.0, min(600.0, delay_seconds))


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
        raise RuntimeError(f"market_alert_worker already running ({holder or 'lock held'})") from exc
    os.ftruncate(fd, 0)
    os.write(fd, f"{os.getpid()}\n".encode("ascii", errors="ignore"))
    _LOCK_FD = fd
    atexit.register(_release_singleton_lock)


def _load_station_rows() -> list[dict[str, str]]:
    with DEFAULT_STATION_CSV.open(newline="", encoding="utf-8") as f:
        return list(csv.DictReader(f))


def _stale_cached_metar_rows(icao: str) -> list[dict[str, Any]]:
    cache_path = ROOT / "cache" / "runtime" / f"metar24_{str(icao).upper()}.json"
    if not cache_path.exists():
        return []
    try:
        doc = json.loads(cache_path.read_text(encoding="utf-8"))
    except Exception:
        return []
    payload = doc.get("payload")
    if not isinstance(payload, list):
        return []
    return [item for item in payload if isinstance(item, dict)]


def _station_from_row(row: dict[str, str]) -> Station:
    return Station(
        city=str(row.get("city") or "").strip(),
        icao=str(row.get("icao") or "").strip().upper(),
        lat=float(row.get("lat") or 0.0),
        lon=float(row.get("lon") or 0.0),
    )


def _parse_utcish(value: Any) -> datetime | None:
    try:
        text = str(value or "").strip()
        if not text:
            return None
        dt = datetime.fromisoformat(text.replace("Z", "+00:00"))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)
    except Exception:
        return None


def _parse_iso_dt_preserve_tz(value: Any) -> datetime | None:
    try:
        text = str(value or "").strip()
        if not text:
            return None
        dt = datetime.fromisoformat(text.replace("Z", "+00:00"))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt
    except Exception:
        return None


def _polymarket_event_url(
    row: dict[str, str],
    station: Station,
    *,
    scheduled_report_utc: str | datetime | None = None,
    now_utc: datetime | None = None,
) -> str:
    anchor_utc = _parse_utcish(scheduled_report_utc) or now_utc or _utc_now()
    tz = ZoneInfo(station_timezone_name(station))
    local_date = anchor_utc.astimezone(tz).date()
    date_slug = format_polymarket_date_slug(datetime(local_date.year, local_date.month, local_date.day, tzinfo=timezone.utc))
    return str(row.get("polymarket_event_url_format") or "").format(
        city_slug=str(row.get("polymarket_city_slug") or "").strip(),
        date_slug=date_slug,
    )


def _load_state() -> dict[str, Any]:
    if not STATE_PATH.exists():
        return {"last_alerts": {}, "last_window_runs": {}, "last_window_results": {}, "last_errors": {}}
    try:
        return json.loads(STATE_PATH.read_text(encoding="utf-8"))
    except Exception:
        return {"last_alerts": {}, "last_window_runs": {}, "last_window_results": {}, "last_errors": {}}


def _save_state(state: dict[str, Any]) -> None:
    _write_json_atomic(STATE_PATH, state)


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


def _estimate_routine_cadence_minutes(rows: list[dict[str, Any]]) -> float | None:
    obs_times = []
    for row in rows:
        if not is_routine_metar_report(row):
            continue
        try:
            obs_times.append(metar_obs_time_utc(row))
        except Exception:
            continue
    obs_times = sorted(set(obs_times))
    if len(obs_times) < 3:
        return None
    diffs = []
    for prev, cur in zip(obs_times[:-1], obs_times[1:]):
        diff = (cur - prev).total_seconds() / 60.0
        if diff >= 20.0:
            diffs.append(diff)
    if not diffs:
        return None
    diffs.sort()
    med = diffs[len(diffs) // 2]
    return float(int(round(med / 5.0)) * 5)


def _routine_minute_counts(rows: list[dict[str, Any]]) -> Counter[int]:
    minute_counts: Counter[int] = Counter()
    for row in rows:
        if not is_routine_metar_report(row):
            continue
        try:
            minute_counts[metar_obs_time_utc(row).minute] += 1
        except Exception:
            continue
    return minute_counts


def _infer_routine_minute_slots(rows: list[dict[str, Any]], cadence_min: float | None) -> list[int]:
    minute_counts = _routine_minute_counts(rows)
    if not minute_counts:
        return []

    if cadence_min is not None and cadence_min > 0:
        expected_slots_float = 60.0 / float(cadence_min)
        expected_slots = int(round(expected_slots_float))
        if expected_slots >= 1 and abs(expected_slots_float - expected_slots) <= 0.25:
            ranked = sorted(minute_counts.items(), key=lambda item: (-item[1], item[0]))
            return sorted(minute for minute, _count in ranked[:expected_slots])

    return sorted(minute_counts)


def _normalize_minute_slots(slots: list[Any] | None) -> list[int]:
    normalized: list[int] = []
    for item in slots or []:
        try:
            minute = int(item)
        except Exception:
            continue
        if 0 <= minute < 60:
            normalized.append(minute)
    return sorted(dict.fromkeys(normalized))


def _load_schedule_config() -> dict[str, Any]:
    global _SCHEDULE_CACHE_MTIME_NS, _SCHEDULE_CACHE
    try:
        stat = SCHEDULE_CONFIG_PATH.stat()
        mtime_ns = stat.st_mtime_ns
    except Exception:
        _SCHEDULE_CACHE_MTIME_NS = None
        _SCHEDULE_CACHE = {}
        return {}
    if _SCHEDULE_CACHE_MTIME_NS == mtime_ns:
        return _SCHEDULE_CACHE
    try:
        payload = json.loads(SCHEDULE_CONFIG_PATH.read_text(encoding="utf-8"))
        if not isinstance(payload, dict):
            payload = {}
    except Exception:
        payload = {}
    _SCHEDULE_CACHE_MTIME_NS = mtime_ns
    _SCHEDULE_CACHE = payload
    return payload


def _configured_schedule_for_station(station_icao: str) -> dict[str, Any]:
    payload = _load_schedule_config()
    stations = payload.get("stations") or {}
    if not isinstance(stations, dict):
        return {}
    item = stations.get(str(station_icao or "").upper()) or {}
    if not isinstance(item, dict):
        return {}
    cadence = item.get("cadence_min")
    try:
        cadence_value = float(cadence) if cadence is not None else None
    except Exception:
        cadence_value = None
    return {
        "cadence_min": cadence_value,
        "minute_slots": _normalize_minute_slots(item.get("minute_slots")),
    }


def _detect_schedule_drift(
    *,
    configured_cadence_min: float | None,
    configured_minute_slots: list[int],
    inferred_cadence_min: float | None,
    inferred_minute_slots: list[int],
    minute_counts: Counter[int],
) -> dict[str, Any] | None:
    total = sum(minute_counts.values())
    if total < 4 or not configured_minute_slots:
        return None

    configured_slots = _normalize_minute_slots(configured_minute_slots)
    inferred_slots = _normalize_minute_slots(inferred_minute_slots)
    off_slot_count = total - sum(minute_counts.get(slot, 0) for slot in configured_slots)
    cadence_changed = (
        configured_cadence_min is not None
        and inferred_cadence_min is not None
        and abs(float(configured_cadence_min) - float(inferred_cadence_min)) >= 5.0
    )
    slot_changed = bool(inferred_slots) and inferred_slots != configured_slots and off_slot_count >= max(2, int(total * 0.2))
    if not cadence_changed and not slot_changed:
        return None
    return {
        "configured_cadence_min": configured_cadence_min,
        "configured_minute_slots": configured_slots,
        "inferred_cadence_min": inferred_cadence_min,
        "inferred_minute_slots": inferred_slots,
        "minute_counts": dict(sorted(minute_counts.items())),
        "sample_count": total,
    }


def _latest_scheduled_report_from_slots(now_utc: datetime, tz: ZoneInfo, minute_slots: list[int]) -> datetime | None:
    normalized = sorted({int(minute) for minute in minute_slots if 0 <= int(minute) < 60})
    if not normalized:
        return None
    local_now = now_utc.astimezone(tz)
    anchors = [local_now.date(), (local_now - timedelta(days=1)).date()]
    candidates: list[datetime] = []
    for local_date in anchors:
        for hour in range(24):
            for minute in normalized:
                candidate_local = datetime(local_date.year, local_date.month, local_date.day, hour, minute, tzinfo=tz)
                candidate_utc = candidate_local.astimezone(timezone.utc)
                if candidate_utc <= now_utc:
                    candidates.append(candidate_utc)
    return max(candidates) if candidates else None


def _latest_metar_context(station: Station) -> dict[str, Any] | None:
    rows = fetch_metar_24h(station.icao, force_refresh=False)
    tz = ZoneInfo(station_timezone_name(station))
    today_local = _utc_now().astimezone(tz).date()
    valid_rows: list[dict[str, Any]] = []
    for row in rows or []:
        try:
            report_utc = metar_obs_time_utc(row)
        except Exception:
            continue
        if report_utc.astimezone(tz).date() == today_local:
            valid_rows.append(row)
    routine_rows = [row for row in valid_rows if is_routine_metar_report(row)]
    latest = max(routine_rows, key=metar_obs_time_utc) if routine_rows else None
    observed = extract_observed_max_for_local_day(valid_rows, station_timezone_name(station), now_utc=_utc_now())
    obs_max = observed.get("observed_max_temp_c")
    obs_max_time_local = _parse_iso_dt_preserve_tz(observed.get("observed_max_time_local"))
    inferred_cadence_min = _estimate_routine_cadence_minutes(routine_rows) if routine_rows else None
    minute_counts = _routine_minute_counts(routine_rows) if routine_rows else Counter()
    inferred_minute_slots = _infer_routine_minute_slots(routine_rows, inferred_cadence_min) if routine_rows else []
    configured_schedule = _configured_schedule_for_station(station.icao)
    cadence_min = configured_schedule.get("cadence_min") or inferred_cadence_min
    minute_slots = configured_schedule.get("minute_slots") or inferred_minute_slots
    schedule_drift = _detect_schedule_drift(
        configured_cadence_min=configured_schedule.get("cadence_min"),
        configured_minute_slots=configured_schedule.get("minute_slots") or [],
        inferred_cadence_min=inferred_cadence_min,
        inferred_minute_slots=inferred_minute_slots,
        minute_counts=minute_counts,
    )
    latest_report_utc = metar_obs_time_utc(latest) if latest is not None else None
    if latest_report_utc is None and minute_slots:
        latest_report_utc = _latest_scheduled_report_from_slots(_utc_now(), tz, minute_slots)
    if latest_report_utc is None or cadence_min is None:
        return None
    return {
        "latest_report_utc": latest_report_utc,
        "observed_max_temp_c": obs_max,
        "observed_max_temp_quantized": bool(observed.get("observed_max_temp_quantized")),
        "observed_max_time_local": obs_max_time_local.isoformat() if obs_max_time_local is not None else None,
        "routine_cadence_min": cadence_min,
        "routine_minute_slots": minute_slots,
        "inferred_routine_cadence_min": inferred_cadence_min,
        "inferred_routine_minute_slots": inferred_minute_slots,
        "schedule_source": "config" if configured_schedule.get("minute_slots") else "inferred",
        "schedule_drift": schedule_drift,
    }


def _scheduler_metar_context(station: Station) -> dict[str, Any] | None:
    tz = ZoneInfo(station_timezone_name(station))
    now_utc = _utc_now()
    rows = _stale_cached_metar_rows(station.icao)
    today_local = now_utc.astimezone(tz).date()
    valid_rows: list[dict[str, Any]] = []
    for row in rows:
        try:
            report_utc = metar_obs_time_utc(row)
        except Exception:
            continue
        if report_utc.astimezone(tz).date() == today_local:
            valid_rows.append(row)
    routine_rows = [row for row in valid_rows if is_routine_metar_report(row)]
    latest_observed_report_utc = None
    if routine_rows:
        try:
            latest_observed_report_utc = max(metar_obs_time_utc(row) for row in routine_rows)
        except Exception:
            latest_observed_report_utc = None
    observed = extract_observed_max_for_local_day(valid_rows, station_timezone_name(station), now_utc=now_utc)
    obs_max = observed.get("observed_max_temp_c")
    obs_max_time_local = _parse_iso_dt_preserve_tz(observed.get("observed_max_time_local"))
    inferred_cadence_min = _estimate_routine_cadence_minutes(routine_rows) if routine_rows else None
    minute_counts = _routine_minute_counts(routine_rows) if routine_rows else Counter()
    inferred_minute_slots = _infer_routine_minute_slots(routine_rows, inferred_cadence_min) if routine_rows else []
    configured_schedule = _configured_schedule_for_station(station.icao)
    cadence_min = configured_schedule.get("cadence_min") or inferred_cadence_min
    minute_slots = configured_schedule.get("minute_slots") or inferred_minute_slots
    schedule_drift = _detect_schedule_drift(
        configured_cadence_min=configured_schedule.get("cadence_min"),
        configured_minute_slots=configured_schedule.get("minute_slots") or [],
        inferred_cadence_min=inferred_cadence_min,
        inferred_minute_slots=inferred_minute_slots,
        minute_counts=minute_counts,
    )
    latest_scheduled_report_utc = _latest_scheduled_report_from_slots(now_utc, tz, minute_slots) if minute_slots else None
    latest_report_utc = latest_observed_report_utc
    if latest_scheduled_report_utc is not None and (latest_report_utc is None or latest_scheduled_report_utc > latest_report_utc):
        latest_report_utc = latest_scheduled_report_utc
    if latest_report_utc is None or cadence_min is None:
        return None
    return {
        "latest_report_utc": latest_report_utc,
        "observed_max_temp_c": obs_max,
        "observed_max_temp_quantized": bool(observed.get("observed_max_temp_quantized")),
        "observed_max_time_local": obs_max_time_local.isoformat() if obs_max_time_local is not None else None,
        "routine_cadence_min": cadence_min,
        "routine_minute_slots": minute_slots,
        "inferred_routine_cadence_min": inferred_cadence_min,
        "inferred_routine_minute_slots": inferred_minute_slots,
        "schedule_source": "config" if configured_schedule.get("minute_slots") else "inferred",
        "schedule_drift": schedule_drift,
    }


def _next_report_window_start(latest_report_utc: datetime, cadence_min: float, now_utc: datetime) -> datetime:
    cadence = max(20.0, float(cadence_min))
    next_report = latest_report_utc
    while next_report <= now_utc:
        next_report = next_report + timedelta(minutes=cadence)
    return next_report


def _next_scheduled_report_utc_from_slots(now_utc: datetime, minute_slots: list[int]) -> datetime | None:
    normalized = sorted({int(minute) for minute in minute_slots if 0 <= int(minute) < 60})
    if not normalized:
        return None
    base_hour = now_utc.replace(minute=0, second=0, microsecond=0)
    for hour_offset in range(0, 4):
        anchor = base_hour + timedelta(hours=hour_offset)
        for minute in normalized:
            candidate = anchor + timedelta(minutes=minute)
            if candidate > now_utc:
                return candidate
    return None


def _current_or_next_window(ctx: dict[str, Any], now_utc: datetime) -> tuple[datetime, datetime, str]:
    latest_report_utc = ctx["latest_report_utc"]
    cadence_min = float(ctx["routine_cadence_min"])
    minute_slots = [int(x) for x in (ctx.get("routine_minute_slots") or []) if str(x).strip() != ""]
    current_start = latest_report_utc
    current_end = latest_report_utc + timedelta(seconds=240)
    if latest_report_utc <= now_utc <= current_end:
        return current_start, current_end, latest_report_utc.isoformat().replace("+00:00", "Z")

    scheduled_report_dt = _next_scheduled_report_utc_from_slots(now_utc, minute_slots)
    if scheduled_report_dt is None:
        next_start = _next_report_window_start(latest_report_utc, cadence_min, now_utc)
        scheduled_report = next_start
    else:
        scheduled_report = scheduled_report_dt
        next_start = scheduled_report
    next_end = scheduled_report + timedelta(seconds=240)
    return next_start, next_end, scheduled_report.isoformat().replace("+00:00", "Z")


def _alert_key(station_icao: str, signal: dict[str, Any]) -> str:
    return "|".join(
        [
            station_icao,
            str(signal.get("signal_type") or ""),
            str(signal.get("scheduled_report_utc") or ""),
            str(signal.get("target_bucket_threshold_c") or ""),
            str((signal.get("evidence") or {}).get("first_live_bucket_label") or ""),
        ]
    )


def _should_send_alert(state: dict[str, Any], key: str, cooldown_seconds: int) -> bool:
    last = ((state.get("last_alerts") or {}).get(key) or {})
    try:
        ts = datetime.fromisoformat(str(last.get("sent_at_utc") or "").replace("Z", "+00:00"))
        if ts.tzinfo is None:
            ts = ts.replace(tzinfo=timezone.utc)
        return (_utc_now() - ts.astimezone(timezone.utc)).total_seconds() >= cooldown_seconds
    except Exception:
        return True


def _window_run_key(station_icao: str, scheduled_report_utc: str) -> str:
    return f"{station_icao}|{scheduled_report_utc}"


def _log_window_decision(
    *,
    state: dict[str, Any],
    log: Any,
    run_key: str,
    station: Station,
    scheduled_report_utc: str,
    window_start: datetime,
    reason: str,
) -> None:
    decisions = state.setdefault("last_window_decisions", {})
    if decisions.get(run_key) == reason:
        return
    decisions[run_key] = reason
    payload = {
        "station_icao": station.icao,
        "scheduled_report_utc": scheduled_report_utc,
        "window_start_utc": window_start.isoformat().replace("+00:00", "Z"),
        "reason": reason,
    }
    log.write(f"{_utc_now().isoformat().replace('+00:00', 'Z')} DECISION {json.dumps(payload, ensure_ascii=False)}\n")
    log.flush()


def _schedule_drift_key(station_icao: str, drift: dict[str, Any]) -> str:
    return "|".join(
        [
            station_icao,
            str(drift.get("configured_cadence_min") or ""),
            ",".join(str(x) for x in drift.get("configured_minute_slots") or []),
            str(drift.get("inferred_cadence_min") or ""),
            ",".join(str(x) for x in drift.get("inferred_minute_slots") or []),
        ]
    )


def _station_task(
    row: dict[str, str],
    metar_ctx: dict[str, Any],
    scheduled_report_utc: str,
    *,
    stream_seconds: float,
) -> dict[str, Any]:
    from market_monitor_service import run_market_monitor_event_window

    station = _station_from_row(row)
    event_url = _polymarket_event_url(row, station, scheduled_report_utc=scheduled_report_utc, now_utc=_utc_now())
    result = run_market_monitor_event_window(
        polymarket_event_url=event_url,
        observed_max_temp_c=metar_ctx.get("observed_max_temp_c"),
        scheduled_report_utc=scheduled_report_utc,
        daily_peak_state="open",
        stream_seconds=stream_seconds,
        baseline_seconds=float(os.getenv("MARKET_EVENT_WINDOW_BASELINE_SECONDS", "2") or "2"),
        core_only=False,
    )
    signal = dict(result.get("signal") or {})
    monitor_ok = bool(result.get("monitor_ok", True))
    monitor_status = str(result.get("monitor_status") or ("ok" if monitor_ok else "unknown"))
    monitor_diagnostics = dict(result.get("monitor_diagnostics") or {})
    if not signal.get("triggered"):
        return {
            "station": station,
            "event_url": event_url,
            "signal": signal,
            "monitor_ok": monitor_ok,
            "monitor_status": monitor_status,
            "monitor_diagnostics": monitor_diagnostics,
            "sent": False,
        }
    observed_utc = str(signal.get("observed_at_utc") or "")
    observed_local = None
    try:
        dt = datetime.fromisoformat(observed_utc.replace("Z", "+00:00")).astimezone(ZoneInfo(station_timezone_name(station)))
        observed_local = dt.isoformat()
    except Exception:
        observed_local = None
    scheduled_report_label = None
    try:
        scheduled_dt = datetime.fromisoformat(str(scheduled_report_utc).replace("Z", "+00:00")).astimezone(
            ZoneInfo(station_timezone_name(station))
        )
        scheduled_report_label = scheduled_dt.strftime("%Y/%m/%d")
    except Exception:
        scheduled_report_label = None
    text = format_market_signal_alert(
        city=station.city,
        station_icao=station.icao,
        signal=signal,
        observed_max_temp_c=metar_ctx.get("observed_max_temp_c"),
        observed_max_temp_quantized=bool(metar_ctx.get("observed_max_temp_quantized")),
        observed_max_time_local=metar_ctx.get("observed_max_time_local"),
        scheduled_report_label=scheduled_report_label,
        polymarket_event_url=event_url,
        observed_at_local=observed_local,
        local_tz_label="Local",
    )
    return {
        "station": station,
        "event_url": event_url,
        "signal": signal,
        "text": text,
        "monitor_ok": monitor_ok,
        "monitor_status": monitor_status,
        "monitor_diagnostics": monitor_diagnostics,
        "sent": False,
    }


def main() -> None:
    _acquire_singleton_lock()
    PID_PATH.write_text(str(os.getpid()), encoding="utf-8")
    max_workers = int(os.getenv("MARKET_ALERT_MAX_WORKERS", "12") or "12")
    cooldown_seconds = int(os.getenv("MARKET_ALERT_COOLDOWN_SECONDS", "900") or "900")
    alert_account = str(os.getenv("TELEGRAM_ALERT_ACCOUNT") or "weatherbot").strip() or "weatherbot"
    active_tasks: dict[str, Future] = {}
    state = _load_state()

    with ThreadPoolExecutor(max_workers=max_workers) as pool, LOG_PATH.open("a", encoding="utf-8") as log:
        while True:
            now_utc = _utc_now()
            rows = _load_station_rows()
            next_wake: datetime | None = None

            for task_key, future in list(active_tasks.items()):
                if not future.done():
                    continue
                try:
                    payload = future.result()
                    signal = dict(payload.get("signal") or {})
                    monitor_ok = bool(payload.get("monitor_ok", True))
                    monitor_status = str(payload.get("monitor_status") or ("ok" if monitor_ok else "unknown"))
                    monitor_diagnostics = dict(payload.get("monitor_diagnostics") or {})
                    window_result = {
                        "completed_at_utc": _utc_now().isoformat().replace("+00:00", "Z"),
                        "task_success": monitor_ok,
                        "monitor_status": monitor_status,
                        "monitor_diagnostics": monitor_diagnostics,
                        "triggered": bool(signal.get("triggered")),
                        "signal_type": str(signal.get("signal_type") or ""),
                        "observed_at_utc": str(signal.get("observed_at_utc") or ""),
                        "scheduled_report_utc": str(signal.get("scheduled_report_utc") or ""),
                        "within_report_window": bool(signal.get("within_report_window")),
                        "event_url": str(payload.get("event_url") or ""),
                        "sent": False,
                    }
                    if not monitor_ok:
                        state.setdefault("last_errors", {})[task_key] = {
                            "failed_at_utc": _utc_now().isoformat().replace("+00:00", "Z"),
                            "error": monitor_status,
                            "diagnostics": monitor_diagnostics,
                        }
                    if monitor_ok and signal.get("triggered"):
                        alert_key = _alert_key(payload["station"].icao, signal)
                        if _should_send_alert(state, alert_key, cooldown_seconds):
                            delivery_report = send_telegram_messages_report(
                                payload["text"],
                                account=alert_account,
                                disable_web_page_preview=False,
                            )
                            state.setdefault("last_alerts", {})[alert_key] = {
                                "sent_at_utc": _utc_now().isoformat().replace("+00:00", "Z"),
                                "account": alert_account,
                                "targets": list(delivery_report.get("targets") or []),
                                "delivered_target_count": len(delivery_report.get("successes") or []),
                            }
                            window_result["delivery"] = {
                                "account": alert_account,
                                "targets": list(delivery_report.get("targets") or []),
                                "success_count": len(delivery_report.get("successes") or []),
                                "error_count": len(delivery_report.get("errors") or []),
                            }
                            payload["sent"] = True
                            window_result["sent"] = True
                        else:
                            window_result["delivery"] = {
                                "account": alert_account,
                                "targets": [],
                                "success_count": 0,
                                "error_count": 0,
                                "cooldown_skipped": True,
                            }
                    state.setdefault("last_window_runs", {})[task_key] = _utc_now().isoformat().replace("+00:00", "Z")
                    state.setdefault("last_window_results", {})[task_key] = window_result
                    log.write(
                        f"{_utc_now().isoformat().replace('+00:00', 'Z')} WINDOW "
                        f"{json.dumps(_json_safe(payload), ensure_ascii=False)}\n"
                    )
                except Exception as exc:
                    state.setdefault("last_errors", {})[task_key] = {
                        "failed_at_utc": _utc_now().isoformat().replace("+00:00", "Z"),
                        "error": str(exc),
                    }
                    state.setdefault("last_window_results", {})[task_key] = {
                        "completed_at_utc": _utc_now().isoformat().replace("+00:00", "Z"),
                        "task_success": False,
                        "error": str(exc),
                    }
                    log.write(f"{_utc_now().isoformat().replace('+00:00', 'Z')} ERROR {str(exc)}\n")
                finally:
                    active_tasks.pop(task_key, None)
                    log.flush()

            for row in rows:
                station = _station_from_row(row)
                metar_ctx = _scheduler_metar_context(station)
                if not metar_ctx or metar_ctx.get("routine_cadence_min") is None:
                    continue
                row_now_utc = _utc_now()
                drift = metar_ctx.get("schedule_drift")
                if isinstance(drift, dict):
                    drift_key = _schedule_drift_key(station.icao, drift)
                    last_key = ((state.get("last_schedule_drifts") or {}).get(station.icao) or "")
                    if drift_key != last_key:
                        log.write(
                            f"{_utc_now().isoformat().replace('+00:00', 'Z')} SCHEDULE_DRIFT "
                            f"{json.dumps(_json_safe({'station': station, 'drift': drift}), ensure_ascii=False)}\n"
                        )
                        state.setdefault("last_schedule_drifts", {})[station.icao] = drift_key
                        log.flush()
                window_start, window_end, scheduled_report_utc = _current_or_next_window(metar_ctx, row_now_utc)
                run_key = _window_run_key(station.icao, scheduled_report_utc)
                if run_key in active_tasks:
                    if row_now_utc >= window_start:
                        _log_window_decision(
                            state=state,
                            log=log,
                            run_key=run_key,
                            station=station,
                            scheduled_report_utc=scheduled_report_utc,
                            window_start=window_start,
                            reason="already_running",
                        )
                    if next_wake is None or window_start < next_wake:
                        next_wake = window_start
                    continue
                if state.get("last_window_runs", {}).get(run_key):
                    if row_now_utc >= window_start:
                        _log_window_decision(
                            state=state,
                            log=log,
                            run_key=run_key,
                            station=station,
                            scheduled_report_utc=scheduled_report_utc,
                            window_start=window_start,
                            reason="already_completed",
                        )
                    if next_wake is None or window_start < next_wake:
                        next_wake = window_start
                    continue
                if row_now_utc >= window_start:
                    stream_seconds = _window_stream_seconds_remaining(window_end, row_now_utc)
                    if stream_seconds is None:
                        _log_window_decision(
                            state=state,
                            log=log,
                            run_key=run_key,
                            station=station,
                            scheduled_report_utc=scheduled_report_utc,
                            window_start=window_start,
                            reason="window_expired",
                        )
                        continue
                    _log_window_decision(
                        state=state,
                        log=log,
                        run_key=run_key,
                        station=station,
                        scheduled_report_utc=scheduled_report_utc,
                        window_start=window_start,
                            reason="submitted",
                        )
                    active_tasks[run_key] = pool.submit(
                        _station_task,
                        row,
                        metar_ctx,
                        scheduled_report_utc,
                        stream_seconds=stream_seconds,
                    )
                    continue
                if next_wake is None or window_start < next_wake:
                    next_wake = window_start

            _save_state(state)
            sleep_seconds = _loop_sleep_seconds(next_wake=next_wake, now_utc=_utc_now(), has_active_tasks=bool(active_tasks))
            time.sleep(sleep_seconds)


if __name__ == "__main__":
    main()
