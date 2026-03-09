from __future__ import annotations

from datetime import datetime, timezone
from typing import Any


def _parse_dt(value: Any) -> datetime | None:
    text = str(value or "").strip()
    if not text:
        return None
    try:
        dt = datetime.fromisoformat(text.replace("Z", "+00:00"))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt
    except Exception:
        return None


def _format_signal_time(
    observed_at_utc: Any,
    *,
    observed_at_local: Any = None,
    local_tz_label: str | None = None,
) -> str:
    utc_dt = _parse_dt(observed_at_utc)
    if utc_dt is None:
        return str(observed_at_utc or "").strip()
    utc_text = utc_dt.astimezone(timezone.utc).strftime("%Y/%m/%d %H:%M:%S UTC")

    local_dt = _parse_dt(observed_at_local)
    if local_dt is None:
        return utc_text
    local_text = local_dt.strftime("%Y/%m/%d %H:%M:%S")
    local_label = str(local_tz_label or "Local").strip() or "Local"
    if (local_dt.utcoffset() or timezone.utc.utcoffset(None)).total_seconds() == 0:
        return utc_text
    return f"{utc_text} | {local_text} {local_label}"


def format_market_signal_alert(
    *,
    city: str,
    signal: dict[str, Any],
    scheduled_report_label: str | None = None,
    polymarket_event_url: str | None = None,
    observed_at_local: str | None = None,
    local_tz_label: str | None = None,
) -> str:
    signal_type = str(signal.get("signal_type") or "")
    confidence = str(signal.get("confidence") or "").strip()
    message = str(signal.get("message") or "").strip()
    lower_bound = signal.get("implied_report_temp_lower_bound_c")
    target_bucket = signal.get("target_bucket_threshold_c")
    evidence = dict(signal.get("evidence") or {})
    first_live_label = str(evidence.get("first_live_bucket_label") or "").strip()
    observed_at = _format_signal_time(
        signal.get("observed_at_utc"),
        observed_at_local=observed_at_local,
        local_tz_label=local_tz_label,
    )

    lines = [f"⚠️ **盘口异常提示 | {city}**"]
    if observed_at:
        lines.append(f"🕒 异动时间：{observed_at}")
    if signal_type == "report_temp_top_bucket_lock_in":
        if first_live_label:
            lines.append(f"🌡️ 推测最新报最高温：{first_live_label}")
        else:
            lines.append(f"🌡️ {message}")
    elif signal_type == "report_temp_scan_floor_stop":
        if first_live_label:
            lines.append(f"🌡️ 推测最新报最高温：{first_live_label}")
        elif target_bucket is not None:
            lines.append(f"🌡️ 推测最新报最高温：{int(float(target_bucket))}°C")
        elif lower_bound is not None:
            lines.append(f"🌡️ 推测最新报最高温：{int(float(lower_bound))}°C")
    elif lower_bound is not None:
        lines.append(f"📈 市场隐含最新报下界：>= {int(float(lower_bound))}°C")
    elif message:
        lines.append(f"📈 {message}")
    lines.append("📝 提示基于盘口异动，不代表官方实况。")
    if polymarket_event_url:
        lines.append(f"🔗 [查看 Polymarket 市场]({polymarket_event_url})")
    return "\n".join(lines)
