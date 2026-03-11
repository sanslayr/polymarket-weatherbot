from __future__ import annotations

import json
import os
import sys
import time
from concurrent.futures import Future, ThreadPoolExecutor
from datetime import datetime
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

from market_alert_delivery_service import handle_completed_task  # noqa: E402
from market_alert_runtime_state import (  # noqa: E402
    LOG_PATH,
    PID_PATH,
    acquire_singleton_lock,
    json_safe,
    load_worker_state,
    save_worker_state,
)
from market_alert_scheduler import (  # noqa: E402
    current_or_next_window,
    load_station_rows,
    loop_sleep_seconds,
    polymarket_event_url,
    schedule_drift_key,
    scheduler_metar_context,
    station_from_row,
    utc_now,
    window_stream_seconds_remaining,
)
from market_signal_alert_service import format_market_signal_alert  # noqa: E402
from station_catalog import Station, station_timezone_name  # noqa: E402


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
    log.write(f"{utc_now().isoformat().replace('+00:00', 'Z')} DECISION {json.dumps(json_safe(payload), ensure_ascii=False)}\n")
    log.flush()


def _station_task(
    row: dict[str, str],
    metar_ctx: dict[str, Any],
    scheduled_report_utc: str,
    *,
    stream_seconds: float,
) -> dict[str, Any]:
    from market_monitor_service import run_market_monitor_event_window

    station = station_from_row(row)
    event_url = polymarket_event_url(row, station, scheduled_report_utc=scheduled_report_utc, now_utc=utc_now())
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
    acquire_singleton_lock()
    PID_PATH.write_text(str(os.getpid()), encoding="utf-8")
    max_workers = int(os.getenv("MARKET_ALERT_MAX_WORKERS", "12") or "12")
    cooldown_seconds = int(os.getenv("MARKET_ALERT_COOLDOWN_SECONDS", "900") or "900")
    alert_account = str(os.getenv("TELEGRAM_ALERT_ACCOUNT") or "weatherbot").strip() or "weatherbot"
    active_tasks: dict[str, Future] = {}
    state = load_worker_state()

    with ThreadPoolExecutor(max_workers=max_workers) as pool, LOG_PATH.open("a", encoding="utf-8") as log:
        while True:
            now_utc = utc_now()
            rows = load_station_rows()
            next_wake: datetime | None = None

            for task_key, future in list(active_tasks.items()):
                if not future.done():
                    continue
                try:
                    payload = future.result()
                    window_result = handle_completed_task(
                        payload=payload,
                        task_key=task_key,
                        state=state,
                        cooldown_seconds=cooldown_seconds,
                        alert_account=alert_account,
                    )
                    state.setdefault("last_window_runs", {})[task_key] = utc_now().isoformat().replace("+00:00", "Z")
                    state.setdefault("last_window_results", {})[task_key] = window_result
                    log.write(
                        f"{utc_now().isoformat().replace('+00:00', 'Z')} WINDOW "
                        f"{json.dumps(json_safe(payload), ensure_ascii=False)}\n"
                    )
                except Exception as exc:
                    state.setdefault("last_errors", {})[task_key] = {
                        "failed_at_utc": utc_now().isoformat().replace("+00:00", "Z"),
                        "error": str(exc),
                    }
                    state.setdefault("last_window_results", {})[task_key] = {
                        "completed_at_utc": utc_now().isoformat().replace("+00:00", "Z"),
                        "task_success": False,
                        "error": str(exc),
                    }
                    log.write(f"{utc_now().isoformat().replace('+00:00', 'Z')} ERROR {str(exc)}\n")
                finally:
                    active_tasks.pop(task_key, None)
                    log.flush()

            for row in rows:
                station = station_from_row(row)
                metar_ctx = scheduler_metar_context(station)
                if not metar_ctx or metar_ctx.get("routine_cadence_min") is None:
                    continue
                row_now_utc = utc_now()
                drift = metar_ctx.get("schedule_drift")
                if isinstance(drift, dict):
                    drift_key = schedule_drift_key(station.icao, drift)
                    last_key = ((state.get("last_schedule_drifts") or {}).get(station.icao) or "")
                    if drift_key != last_key:
                        log.write(
                            f"{utc_now().isoformat().replace('+00:00', 'Z')} SCHEDULE_DRIFT "
                            f"{json.dumps(json_safe({'station': station, 'drift': drift}), ensure_ascii=False)}\n"
                        )
                        state.setdefault("last_schedule_drifts", {})[station.icao] = drift_key
                        log.flush()
                window_start, window_end, scheduled_report_utc = current_or_next_window(metar_ctx, row_now_utc)
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
                    stream_seconds = window_stream_seconds_remaining(window_end, row_now_utc)
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

            save_worker_state(state)
            sleep_seconds = loop_sleep_seconds(next_wake=next_wake, now_utc=utc_now(), has_active_tasks=bool(active_tasks))
            time.sleep(sleep_seconds)


if __name__ == "__main__":
    main()
