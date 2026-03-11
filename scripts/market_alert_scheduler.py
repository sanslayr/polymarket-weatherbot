from __future__ import annotations

import csv
import json
import os
import time
from collections import Counter
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

from build_station_links import format_polymarket_date_slug
from metar_utils import extract_observed_max_for_local_day, fetch_metar_24h, is_routine_metar_report, metar_obs_time_utc
from station_catalog import DEFAULT_STATION_CSV, Station, station_timezone_name


ROOT = Path(__file__).resolve().parent.parent
SCHEDULE_CONFIG_PATH = ROOT / "config" / "market_alert_station_schedule.json"
_SCHEDULE_CACHE_MTIME_NS: int | None = None
_SCHEDULE_CACHE: dict[str, Any] = {}


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def event_window_stream_seconds() -> float:
    configured = str(os.getenv("MARKET_EVENT_WINDOW_STREAM_SECONDS") or "").strip()
    if configured:
        try:
            return max(1.0, float(configured))
        except Exception:
            pass
    return 245.0


def window_stream_seconds_remaining(window_end: datetime, now_utc: datetime) -> float | None:
    remaining_seconds = (window_end - now_utc).total_seconds()
    if remaining_seconds < 1.0:
        return None
    return max(1.0, min(event_window_stream_seconds(), remaining_seconds))


def loop_sleep_seconds(*, next_wake: datetime | None, now_utc: datetime, has_active_tasks: bool) -> float:
    if next_wake is None and not has_active_tasks:
        return 60.0
    if next_wake is None:
        return 1.0 if has_active_tasks else 60.0
    delay_seconds = (next_wake - now_utc).total_seconds()
    if has_active_tasks:
        return max(0.5, min(5.0, delay_seconds))
    return max(1.0, min(600.0, delay_seconds))


def load_station_rows() -> list[dict[str, str]]:
    with DEFAULT_STATION_CSV.open(newline="", encoding="utf-8") as f:
        return list(csv.DictReader(f))


def stale_cached_metar_rows(icao: str) -> list[dict[str, Any]]:
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


def station_from_row(row: dict[str, str]) -> Station:
    return Station(
        city=str(row.get("city") or "").strip(),
        icao=str(row.get("icao") or "").strip().upper(),
        lat=float(row.get("lat") or 0.0),
        lon=float(row.get("lon") or 0.0),
    )


def parse_utcish(value: Any) -> datetime | None:
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


def parse_iso_dt_preserve_tz(value: Any) -> datetime | None:
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


def polymarket_event_url(
    row: dict[str, str],
    station: Station,
    *,
    scheduled_report_utc: str | datetime | None = None,
    now_utc: datetime | None = None,
) -> str:
    anchor_utc = parse_utcish(scheduled_report_utc) or now_utc or utc_now()
    tz = ZoneInfo(station_timezone_name(station))
    local_date = anchor_utc.astimezone(tz).date()
    date_slug = format_polymarket_date_slug(datetime(local_date.year, local_date.month, local_date.day, tzinfo=timezone.utc))
    return str(row.get("polymarket_event_url_format") or "").format(
        city_slug=str(row.get("polymarket_city_slug") or "").strip(),
        date_slug=date_slug,
    )


def estimate_routine_cadence_minutes(rows: list[dict[str, Any]]) -> float | None:
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


def infer_routine_minute_slots(rows: list[dict[str, Any]], cadence_min: float | None) -> list[int]:
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


def configured_schedule_for_station(station_icao: str) -> dict[str, Any]:
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


def detect_schedule_drift(
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


def latest_metar_context(station: Station) -> dict[str, Any] | None:
    rows = fetch_metar_24h(station.icao, force_refresh=False)
    tz = ZoneInfo(station_timezone_name(station))
    today_local = utc_now().astimezone(tz).date()
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
    observed = extract_observed_max_for_local_day(valid_rows, station_timezone_name(station), now_utc=utc_now())
    obs_max = observed.get("observed_max_temp_c")
    obs_max_time_local = parse_iso_dt_preserve_tz(observed.get("observed_max_time_local"))
    inferred_cadence_min = estimate_routine_cadence_minutes(routine_rows) if routine_rows else None
    minute_counts = _routine_minute_counts(routine_rows) if routine_rows else Counter()
    inferred_minute_slots = infer_routine_minute_slots(routine_rows, inferred_cadence_min) if routine_rows else []
    configured_schedule = configured_schedule_for_station(station.icao)
    cadence_min = configured_schedule.get("cadence_min") or inferred_cadence_min
    minute_slots = configured_schedule.get("minute_slots") or inferred_minute_slots
    schedule_drift = detect_schedule_drift(
        configured_cadence_min=configured_schedule.get("cadence_min"),
        configured_minute_slots=configured_schedule.get("minute_slots") or [],
        inferred_cadence_min=inferred_cadence_min,
        inferred_minute_slots=inferred_minute_slots,
        minute_counts=minute_counts,
    )
    latest_report_utc = metar_obs_time_utc(latest) if latest is not None else None
    if latest_report_utc is None and minute_slots:
        latest_report_utc = _latest_scheduled_report_from_slots(utc_now(), tz, minute_slots)
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


def scheduler_metar_context(station: Station) -> dict[str, Any] | None:
    tz = ZoneInfo(station_timezone_name(station))
    now_utc = utc_now()
    rows = stale_cached_metar_rows(station.icao)
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
    obs_max_time_local = parse_iso_dt_preserve_tz(observed.get("observed_max_time_local"))
    inferred_cadence_min = estimate_routine_cadence_minutes(routine_rows) if routine_rows else None
    minute_counts = _routine_minute_counts(routine_rows) if routine_rows else Counter()
    inferred_minute_slots = infer_routine_minute_slots(routine_rows, inferred_cadence_min) if routine_rows else []
    configured_schedule = configured_schedule_for_station(station.icao)
    cadence_min = configured_schedule.get("cadence_min") or inferred_cadence_min
    minute_slots = configured_schedule.get("minute_slots") or inferred_minute_slots
    schedule_drift = detect_schedule_drift(
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


def next_scheduled_report_utc_from_slots(now_utc: datetime, minute_slots: list[int]) -> datetime | None:
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


def current_or_next_window(ctx: dict[str, Any], now_utc: datetime) -> tuple[datetime, datetime, str]:
    latest_report_utc = ctx["latest_report_utc"]
    cadence_min = float(ctx["routine_cadence_min"])
    minute_slots = [int(x) for x in (ctx.get("routine_minute_slots") or []) if str(x).strip() != ""]
    current_start = latest_report_utc
    current_end = latest_report_utc + timedelta(seconds=240)
    if latest_report_utc <= now_utc <= current_end:
        return current_start, current_end, latest_report_utc.isoformat().replace("+00:00", "Z")

    scheduled_report_dt = next_scheduled_report_utc_from_slots(now_utc, minute_slots)
    if scheduled_report_dt is None:
        next_start = _next_report_window_start(latest_report_utc, cadence_min, now_utc)
        scheduled_report = next_start
    else:
        scheduled_report = scheduled_report_dt
        next_start = scheduled_report
    next_end = scheduled_report + timedelta(seconds=240)
    return next_start, next_end, scheduled_report.isoformat().replace("+00:00", "Z")


def schedule_drift_key(station_icao: str, drift: dict[str, Any]) -> str:
    return "|".join(
        [
            station_icao,
            str(drift.get("configured_cadence_min") or ""),
            ",".join(str(x) for x in drift.get("configured_minute_slots") or []),
            str(drift.get("inferred_cadence_min") or ""),
            ",".join(str(x) for x in drift.get("inferred_minute_slots") or []),
        ]
    )
