from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from telegram_notifier import send_telegram_messages_report


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def alert_key(station_icao: str, signal: dict[str, Any]) -> str:
    return "|".join(
        [
            station_icao,
            str(signal.get("signal_type") or ""),
            str(signal.get("scheduled_report_utc") or ""),
            str(signal.get("target_bucket_threshold_c") or ""),
            str((signal.get("evidence") or {}).get("first_live_bucket_label") or ""),
        ]
    )


def should_send_alert(state: dict[str, Any], key: str, cooldown_seconds: int) -> bool:
    last = ((state.get("last_alerts") or {}).get(key) or {})
    try:
        ts = datetime.fromisoformat(str(last.get("sent_at_utc") or "").replace("Z", "+00:00"))
        if ts.tzinfo is None:
            ts = ts.replace(tzinfo=timezone.utc)
        return (_utc_now() - ts.astimezone(timezone.utc)).total_seconds() >= cooldown_seconds
    except Exception:
        return True


def build_window_result(payload: dict[str, Any]) -> dict[str, Any]:
    signal = dict(payload.get("signal") or {})
    return {
        "completed_at_utc": _utc_now().isoformat().replace("+00:00", "Z"),
        "task_success": bool(payload.get("monitor_ok", True)),
        "monitor_status": str(payload.get("monitor_status") or ("ok" if payload.get("monitor_ok", True) else "unknown")),
        "monitor_diagnostics": dict(payload.get("monitor_diagnostics") or {}),
        "triggered": bool(signal.get("triggered")),
        "signal_type": str(signal.get("signal_type") or ""),
        "observed_at_utc": str(signal.get("observed_at_utc") or ""),
        "scheduled_report_utc": str(signal.get("scheduled_report_utc") or ""),
        "within_report_window": bool(signal.get("within_report_window")),
        "event_url": str(payload.get("event_url") or ""),
        "sent": False,
    }


def handle_completed_task(
    *,
    payload: dict[str, Any],
    task_key: str,
    state: dict[str, Any],
    cooldown_seconds: int,
    alert_account: str,
) -> dict[str, Any]:
    signal = dict(payload.get("signal") or {})
    monitor_ok = bool(payload.get("monitor_ok", True))
    monitor_status = str(payload.get("monitor_status") or ("ok" if monitor_ok else "unknown"))
    monitor_diagnostics = dict(payload.get("monitor_diagnostics") or {})
    window_result = build_window_result(payload)

    if not monitor_ok:
        state.setdefault("last_errors", {})[task_key] = {
            "failed_at_utc": _utc_now().isoformat().replace("+00:00", "Z"),
            "error": monitor_status,
            "diagnostics": monitor_diagnostics,
        }
        return window_result

    if not signal.get("triggered"):
        return window_result

    dedupe_key = alert_key(payload["station"].icao, signal)
    if not should_send_alert(state, dedupe_key, cooldown_seconds):
        window_result["delivery"] = {
            "account": alert_account,
            "targets": [],
            "success_count": 0,
            "error_count": 0,
            "cooldown_skipped": True,
        }
        return window_result

    delivery_report = send_telegram_messages_report(
        payload["text"],
        account=alert_account,
        disable_web_page_preview=False,
    )
    state.setdefault("last_alerts", {})[dedupe_key] = {
        "sent_at_utc": _utc_now().isoformat().replace("+00:00", "Z"),
        "account": alert_account,
        "targets": list(delivery_report.get("targets") or []),
        "delivered_target_count": len(delivery_report.get("successes") or []),
    }
    payload["sent"] = True
    window_result["sent"] = True
    window_result["delivery"] = {
        "account": alert_account,
        "targets": list(delivery_report.get("targets") or []),
        "success_count": len(delivery_report.get("successes") or []),
        "error_count": len(delivery_report.get("errors") or []),
    }
    return window_result
