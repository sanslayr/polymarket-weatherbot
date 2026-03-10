from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from market_price_format import format_price_cents, infer_market_tick_cents


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
    local_text = local_dt.strftime("%H:%M:%S")
    local_label = str(local_tz_label or "Local").strip() or "Local"
    if (local_dt.utcoffset() or timezone.utc.utcoffset(None)).total_seconds() == 0:
        return utc_text
    if local_dt.date() != utc_dt.astimezone(local_dt.tzinfo).date():
        local_text = local_dt.strftime("%Y/%m/%d %H:%M:%S")
    return f"{utc_text} | {local_text} {local_label}"


def _format_temp_value(value: Any) -> str | None:
    try:
        if value in (None, ""):
            return None
        numeric = float(value)
    except Exception:
        return None
    rounded = round(numeric)
    if abs(numeric - rounded) < 0.02:
        return str(int(rounded))
    return f"{numeric:.1f}".rstrip("0").rstrip(".")


def _format_temp_label(value: Any, unit: str | None, *, prefix: str = "") -> str | None:
    temp_text = _format_temp_value(value)
    normalized_unit = str(unit or "").strip().upper() or "C"
    if temp_text is None:
        return None
    return f"{prefix}{temp_text}°{normalized_unit}"


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
    lower_bound_native = signal.get("implied_report_temp_lower_bound_native")
    target_bucket = signal.get("target_bucket_threshold_c")
    target_bucket_native = signal.get("target_bucket_threshold_native")
    temperature_unit = str(signal.get("temperature_unit") or "").strip().upper() or str(
        (signal.get("evidence") or {}).get("temperature_unit") or "C"
    ).strip().upper()
    evidence = dict(signal.get("evidence") or {})
    first_live_label = str(evidence.get("first_live_bucket_label") or "").strip()
    observed_at = _format_signal_time(
        signal.get("observed_at_utc"),
        observed_at_local=observed_at_local,
        local_tz_label=local_tz_label,
    )

    collapse_line = ""

    def _collapse_text(bucket_label: str, *, bid_now: Any, ask_now: Any) -> str:
        parts: list[str] = []
        bid_v = None
        ask_v = None
        try:
            bid_v = float(bid_now) if bid_now not in (None, "") else None
        except Exception:
            bid_v = None
        try:
            ask_v = float(ask_now) if ask_now not in (None, "") else None
        except Exception:
            ask_v = None
        if bid_v is not None and bid_v <= 0.001:
            parts.append("买盘接近归零")
        if ask_v is not None and ask_v <= 0.01:
            parts.append("卖盘压到 1¢ 或更低")
        if not parts:
            return ""
        return f"📉 观察盘口：{bucket_label} " + "，".join(parts) + "。"

    if signal_type == "report_temp_scan_floor_stop":
        collapsed = [str(x).strip() for x in (evidence.get("collapsed_prefix_labels") or []) if str(x).strip()]
        collapsed_prev_bids = dict(evidence.get("collapsed_prefix_prev_bids") or {})
        tick_cents = infer_market_tick_cents(
            evidence.get("first_live_bucket_bid"),
            *collapsed_prev_bids.values(),
        )
        first_live_bid = format_price_cents(evidence.get("first_live_bucket_bid"), tick_cents=tick_cents, none_text="N/A")
        if collapsed:
            collapsed_label = collapsed[0]
            collapsed_prev_bid = format_price_cents(collapsed_prev_bids.get(collapsed_label), tick_cents=tick_cents, none_text="N/A")
            collapse_line = (
                f"📉 观察盘口：{collapsed_label} Yes 由 {collapsed_prev_bid} 跌至接近归零，"
                f"{first_live_label} Yes 仍有 {first_live_bid} bid报价。"
            )
    elif signal_type == "report_temp_top_bucket_lock_in":
        collapsed = [str(x).strip() for x in (evidence.get("collapsed_lower_bucket_labels") or []) if str(x).strip()]
        top_label = str(evidence.get("top_bucket_label") or first_live_label).strip()
        if collapsed and top_label:
            collapse_line = f"📉 观察盘口：{'、'.join(collapsed)} 接近归零，仅 {top_label} 仍保留有效报价。"
    elif signal_type == "report_temp_lower_bound_jump":
        bucket_label = str(evidence.get("bucket_label") or "").strip()
        if bucket_label:
            collapse_line = _collapse_text(
                bucket_label,
                bid_now=evidence.get("best_bid"),
                ask_now=evidence.get("best_ask"),
            ) or f"📉 观察盘口：{bucket_label} 买盘接近归零或卖盘压到 1¢ 或更低。"

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
        elif _format_temp_label(target_bucket_native, temperature_unit):
            lines.append(f"🌡️ 推测最新报最高温：{_format_temp_label(target_bucket_native, temperature_unit)}")
        elif target_bucket is not None:
            lines.append(f"🌡️ 推测最新报最高温：{_format_temp_label(target_bucket, 'C')}")
        elif lower_bound is not None:
            lines.append(f"🌡️ 推测最新报最高温：{_format_temp_label(lower_bound_native or lower_bound, temperature_unit if lower_bound_native is not None else 'C')}")
    elif lower_bound_native is not None:
        lines.append(f"📈 市场隐含最新报下界：{_format_temp_label(lower_bound_native, temperature_unit, prefix='>= ')}")
    elif lower_bound is not None:
        lines.append(f"📈 市场隐含最新报下界：{_format_temp_label(lower_bound, 'C', prefix='>= ')}")
    elif message:
        lines.append(f"📈 {message}")
    if collapse_line:
        lines.append(collapse_line)
    lines.append("📝 提示基于盘口异动，不代表官方实况。")
    if polymarket_event_url:
        lines.append(f"🔗 [Polymarket 市场]({polymarket_event_url})")
    return "\n".join(lines)
