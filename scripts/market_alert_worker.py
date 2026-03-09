from __future__ import annotations

import csv
import json
import os
import time
from concurrent.futures import Future, ThreadPoolExecutor
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

from build_station_links import format_polymarket_date_slug
from market_monitor_service import run_market_monitor_event_window
from market_signal_alert_service import format_market_signal_alert
from metar_utils import fetch_metar_24h, metar_obs_time_utc
from station_catalog import DEFAULT_STATION_CSV, Station, station_timezone_name
from telegram_notifier import send_telegram_messages


ROOT = Path(__file__).resolve().parent.parent
STATE_DIR = ROOT / "cache" / "runtime" / "market_alert_worker"
STATE_DIR.mkdir(parents=True, exist_ok=True)
STATE_PATH = STATE_DIR / "state.json"
PID_PATH = STATE_DIR / "worker.pid"
LOG_PATH = STATE_DIR / "worker.log"


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


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


def _polymarket_event_url(row: dict[str, str], now_utc: datetime) -> str:
    date_slug = format_polymarket_date_slug(now_utc)
    return str(row.get("polymarket_event_url_format") or "").format(
        city_slug=str(row.get("polymarket_city_slug") or "").strip(),
        date_slug=date_slug,
    )


def _load_state() -> dict[str, Any]:
    if not STATE_PATH.exists():
        return {"last_alerts": {}, "last_window_runs": {}}
    try:
        return json.loads(STATE_PATH.read_text(encoding="utf-8"))
    except Exception:
        return {"last_alerts": {}, "last_window_runs": {}}


def _save_state(state: dict[str, Any]) -> None:
    STATE_PATH.write_text(json.dumps(state, ensure_ascii=False), encoding="utf-8")


def _estimate_routine_cadence_minutes(rows: list[dict[str, Any]]) -> float | None:
    obs_times = []
    for row in rows:
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


def _latest_metar_context(station: Station) -> dict[str, Any] | None:
    rows = fetch_metar_24h(station.icao, force_refresh=False)
    if not rows:
        return None
    tz = ZoneInfo(station_timezone_name(station))
    today_local = _utc_now().astimezone(tz).date()
    valid_rows: list[dict[str, Any]] = []
    for row in rows:
        try:
            report_utc = metar_obs_time_utc(row)
        except Exception:
            continue
        if report_utc.astimezone(tz).date() == today_local:
            valid_rows.append(row)
    if not valid_rows:
        return None
    latest = max(valid_rows, key=metar_obs_time_utc)
    obs_max = None
    for row in valid_rows:
        try:
            temp = float(row.get("temp"))
        except Exception:
            continue
        obs_max = temp if obs_max is None else max(obs_max, temp)
    cadence_min = _estimate_routine_cadence_minutes(valid_rows)
    latest_report_utc = metar_obs_time_utc(latest)
    return {
        "latest_report_utc": latest_report_utc,
        "observed_max_temp_c": obs_max,
        "routine_cadence_min": cadence_min,
    }


def _next_report_window_start(latest_report_utc: datetime, cadence_min: float, now_utc: datetime) -> datetime:
    cadence = max(20.0, float(cadence_min))
    next_report = latest_report_utc
    while next_report <= now_utc:
        next_report = next_report + timedelta(minutes=cadence)
    return next_report + timedelta(seconds=60)


def _current_or_next_window(ctx: dict[str, Any], now_utc: datetime) -> tuple[datetime, datetime, str]:
    latest_report_utc = ctx["latest_report_utc"]
    cadence_min = float(ctx["routine_cadence_min"])
    current_start = latest_report_utc + timedelta(seconds=60)
    current_end = latest_report_utc + timedelta(seconds=180)
    if current_start <= now_utc <= current_end:
        return current_start, current_end, latest_report_utc.isoformat().replace("+00:00", "Z")
    next_start = _next_report_window_start(latest_report_utc, cadence_min, now_utc)
    scheduled_report = next_start - timedelta(seconds=60)
    next_end = scheduled_report + timedelta(seconds=180)
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


def _station_task(row: dict[str, str], metar_ctx: dict[str, Any], scheduled_report_utc: str) -> dict[str, Any]:
    station = _station_from_row(row)
    event_url = _polymarket_event_url(row, _utc_now())
    result = run_market_monitor_event_window(
        polymarket_event_url=event_url,
        observed_max_temp_c=metar_ctx.get("observed_max_temp_c"),
        scheduled_report_utc=scheduled_report_utc,
        daily_peak_state="open",
        stream_seconds=float(os.getenv("MARKET_EVENT_WINDOW_STREAM_SECONDS", "125") or "125"),
        baseline_seconds=float(os.getenv("MARKET_EVENT_WINDOW_BASELINE_SECONDS", "2") or "2"),
        core_only=True,
    )
    signal = dict(result.get("signal") or {})
    if not signal.get("triggered"):
        return {"station": station, "event_url": event_url, "signal": signal, "sent": False}
    observed_utc = str(signal.get("observed_at_utc") or "")
    observed_local = None
    try:
        dt = datetime.fromisoformat(observed_utc.replace("Z", "+00:00")).astimezone(ZoneInfo(station_timezone_name(station)))
        observed_local = dt.isoformat()
    except Exception:
        observed_local = None
    text = format_market_signal_alert(
        city=station.city,
        signal=signal,
        polymarket_event_url=event_url,
        observed_at_local=observed_local,
        local_tz_label="Local",
    )
    return {"station": station, "event_url": event_url, "signal": signal, "text": text, "sent": False}


def main() -> None:
    PID_PATH.write_text(str(os.getpid()), encoding="utf-8")
    max_workers = int(os.getenv("MARKET_ALERT_MAX_WORKERS", "6") or "6")
    cooldown_seconds = int(os.getenv("MARKET_ALERT_COOLDOWN_SECONDS", "900") or "900")
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
                    if signal.get("triggered"):
                        alert_key = _alert_key(payload["station"].icao, signal)
                        if _should_send_alert(state, alert_key, cooldown_seconds):
                            send_telegram_messages(payload["text"])
                            state.setdefault("last_alerts", {})[alert_key] = {
                                "sent_at_utc": _utc_now().isoformat().replace("+00:00", "Z")
                            }
                            payload["sent"] = True
                    log.write(f"{_utc_now().isoformat().replace('+00:00', 'Z')} WINDOW {json.dumps(payload, ensure_ascii=False)}\n")
                except Exception as exc:
                    log.write(f"{_utc_now().isoformat().replace('+00:00', 'Z')} ERROR {str(exc)}\n")
                finally:
                    active_tasks.pop(task_key, None)
                    log.flush()

            for row in rows:
                station = _station_from_row(row)
                metar_ctx = _latest_metar_context(station)
                if not metar_ctx or metar_ctx.get("routine_cadence_min") is None:
                    continue
                window_start, _window_end, scheduled_report_utc = _current_or_next_window(metar_ctx, now_utc)
                run_key = _window_run_key(station.icao, scheduled_report_utc)
                if run_key in active_tasks or state.get("last_window_runs", {}).get(run_key):
                    if next_wake is None or window_start < next_wake:
                        next_wake = window_start
                    continue
                if now_utc >= window_start:
                    active_tasks[run_key] = pool.submit(_station_task, row, metar_ctx, scheduled_report_utc)
                    state.setdefault("last_window_runs", {})[run_key] = _utc_now().isoformat().replace("+00:00", "Z")
                    continue
                if next_wake is None or window_start < next_wake:
                    next_wake = window_start

            _save_state(state)
            if next_wake is None and not active_tasks:
                time.sleep(60.0)
                continue
            sleep_seconds = 5.0 if active_tasks else max(1.0, min(600.0, (next_wake - _utc_now()).total_seconds()))
            time.sleep(sleep_seconds)


if __name__ == "__main__":
    main()
