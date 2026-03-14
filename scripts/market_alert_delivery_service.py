from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from telegram_notifier import send_telegram_messages_report


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def alert_key(station_icao: str, signal: dict[str, Any], *, event_url: str = "") -> str:
    evidence = dict(signal.get("evidence") or {})
    target_bucket_label = str(signal.get("target_bucket_label") or "")
    target_bucket_threshold = str(signal.get("target_bucket_threshold_c") or signal.get("target_bucket_threshold_native") or "")
    first_live_bucket_label = str(evidence.get("first_live_bucket_label") or "")
    dead_bucket_label = str(evidence.get("dead_bucket_label") or "")
    return "|".join(
        [
            station_icao,
            str(signal.get("signal_type") or ""),
            str(event_url or ""),
            target_bucket_label or target_bucket_threshold,
            first_live_bucket_label,
            dead_bucket_label,
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
    signals = [dict(item) for item in (payload.get("signals") or []) if isinstance(item, dict)]
    active_signal = signals[-1] if signals else signal
    return {
        "completed_at_utc": _utc_now().isoformat().replace("+00:00", "Z"),
        "task_success": bool(payload.get("monitor_ok", True)),
        "monitor_status": str(payload.get("monitor_status") or ("ok" if payload.get("monitor_ok", True) else "unknown")),
        "monitor_diagnostics": dict(payload.get("monitor_diagnostics") or {}),
        "triggered": bool(signals) or bool(signal.get("triggered")),
        "signal_count": len(signals) if signals else (1 if signal.get("triggered") else 0),
        "signal_type": str(active_signal.get("signal_type") or ""),
        "observed_at_utc": str(active_signal.get("observed_at_utc") or ""),
        "scheduled_report_utc": str(active_signal.get("scheduled_report_utc") or ""),
        "within_report_window": bool(active_signal.get("within_report_window")),
        "event_url": str(payload.get("event_url") or ""),
        "resident_mode": bool(payload.get("resident_mode")),
        "resident_reason": str(payload.get("resident_reason") or ""),
        "sent": False,
    }


def deliver_alert_payload(
    *,
    payload: dict[str, Any],
    state: dict[str, Any],
    cooldown_seconds: int,
    alert_account: str,
) -> dict[str, Any]:
    signal = dict(payload.get("signal") or {})
    dedupe_key = alert_key(payload["station"].icao, signal, event_url=str(payload.get("event_url") or ""))
    if not should_send_alert(state, dedupe_key, cooldown_seconds):
        return {
            "key": dedupe_key,
            "sent": False,
            "cooldown_skipped": True,
            "delivery": {
                "account": alert_account,
                "targets": [],
                "success_count": 0,
                "error_count": 0,
                "cooldown_skipped": True,
            },
        }

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
    return {
        "key": dedupe_key,
        "sent": True,
        "cooldown_skipped": False,
        "delivery": {
            "account": alert_account,
            "targets": list(delivery_report.get("targets") or []),
            "success_count": len(delivery_report.get("successes") or []),
            "error_count": len(delivery_report.get("errors") or []),
        },
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
    emitted_alerts = [dict(item) for item in (payload.get("emitted_alerts") or []) if isinstance(item, dict)]

    if not monitor_ok:
        state.setdefault("last_errors", {})[task_key] = {
            "failed_at_utc": _utc_now().isoformat().replace("+00:00", "Z"),
            "error": monitor_status,
            "diagnostics": monitor_diagnostics,
        }
        return window_result

    if emitted_alerts:
        sent_count = len([item for item in emitted_alerts if item.get("sent")])
        skipped_count = len([item for item in emitted_alerts if item.get("cooldown_skipped")])
        window_result["sent"] = sent_count > 0
        window_result["delivery"] = {
            "account": alert_account,
            "targets": [target for item in emitted_alerts for target in ((item.get("delivery") or {}).get("targets") or [])],
            "success_count": sum(int(((item.get("delivery") or {}).get("success_count") or 0)) for item in emitted_alerts),
            "error_count": sum(int(((item.get("delivery") or {}).get("error_count") or 0)) for item in emitted_alerts),
            "cooldown_skipped_count": skipped_count,
        }
        return window_result

    if not signal.get("triggered"):
        return window_result

    delivery_result = deliver_alert_payload(
        payload=payload,
        state=state,
        cooldown_seconds=cooldown_seconds,
        alert_account=alert_account,
    )
    window_result["sent"] = bool(delivery_result.get("sent"))
    window_result["delivery"] = dict(delivery_result.get("delivery") or {})
    return window_result
