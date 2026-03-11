from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from market_price_format import format_price_cents, infer_market_tick_cents


def _to_float(value: Any) -> float | None:
    try:
        if value in (None, ""):
            return None
        return float(value)
    except Exception:
        return None


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


def _format_observed_temp_label(value_c: Any, display_unit: str | None, *, source_quantized: bool = False) -> str | None:
    try:
        if value_c in (None, ""):
            return None
        numeric_c = float(value_c)
    except Exception:
        return None

    normalized_unit = str(display_unit or "").strip().upper() or "C"
    if normalized_unit == "F":
        numeric_f = numeric_c * 9.0 / 5.0 + 32.0
        if source_quantized:
            return f"{int(round(numeric_f))}°F"
        return _format_temp_label(numeric_f, "F")
    if source_quantized:
        return f"{int(round(numeric_c))}°C"
    return _format_temp_label(numeric_c, "C")


def _join_bucket_labels(labels: list[str]) -> str:
    cleaned = [str(label).strip() for label in labels if str(label).strip()]
    return ", ".join(cleaned)


def _join_bucket_labels_with_slash(labels: list[str]) -> str:
    cleaned = [str(label).strip() for label in labels if str(label).strip()]
    return "/".join(cleaned)


def _format_ladder_row(bucket_label: str, *, bid_value: Any, ask_value: Any, tick_cents: float | None) -> str:
    bid_text = format_price_cents(bid_value, tick_cents=tick_cents, none_text="N/A")
    ask_text = format_price_cents(ask_value, tick_cents=tick_cents, none_text="N/A")
    return f"• {bucket_label}：Bid {bid_text} | Ask {ask_text}"


def _title_with_date(
    city: str,
    station_icao: str | None,
    scheduled_report_label: str | None,
    observed_at_local: str | None,
    signal: dict[str, Any],
) -> str:
    date_text = str(scheduled_report_label or "").strip()
    if not date_text:
        local_dt = _parse_dt(observed_at_local)
        if local_dt is not None:
            date_text = local_dt.strftime("%Y/%m/%d")
    if not date_text:
        scheduled_dt = _parse_dt(signal.get("scheduled_report_utc"))
        if scheduled_dt is not None:
            date_text = scheduled_dt.astimezone(timezone.utc).strftime("%Y/%m/%d")
    suffix = f" @ {date_text}" if date_text else ""
    return f"⚠️ *盘口归零异动 | {city}{suffix}*"


def _format_observed_max_note(
    observed_max_temp_c: float | None,
    *,
    observed_max_temp_quantized: bool,
    observed_max_time_local: str | None,
    display_unit: str | None,
) -> str | None:
    temp_label = _format_observed_temp_label(
        observed_max_temp_c,
        display_unit,
        source_quantized=bool(observed_max_temp_quantized),
    )
    if not temp_label:
        return None
    time_dt = _parse_dt(observed_max_time_local)
    if time_dt is None:
        return f"已记录METAR最高温：{temp_label}"
    return f"已记录METAR最高温：{temp_label} @ {time_dt.strftime('%H:%M')} Local"


def _format_bid_ask_clause(*, bid_value: Any, ask_value: Any, tick_cents: float | None) -> str:
    bid_text = format_price_cents(bid_value, tick_cents=tick_cents, none_text="N/A")
    ask_text = format_price_cents(ask_value, tick_cents=tick_cents, none_text="N/A")
    parts = [f"Bid {bid_text}"]
    if _to_float(ask_value) is not None:
        parts.append(f"Ask {ask_text}")
    return "｜".join(parts)


def format_market_signal_alert(
    *,
    city: str,
    station_icao: str | None = None,
    signal: dict[str, Any],
    observed_max_temp_c: float | None = None,
    observed_max_temp_quantized: bool = False,
    observed_max_time_local: str | None = None,
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
    target_bucket_label = str(signal.get("target_bucket_label") or "").strip()
    target_bucket = signal.get("target_bucket_threshold_c")
    target_bucket_native = signal.get("target_bucket_threshold_native")
    temperature_unit = str(signal.get("temperature_unit") or "").strip().upper() or str(
        (signal.get("evidence") or {}).get("temperature_unit") or "C"
    ).strip().upper()
    evidence = dict(signal.get("evidence") or {})
    first_live_label = str(evidence.get("first_live_bucket_label") or "").strip()
    top_bucket_label = str(evidence.get("top_bucket_label") or "").strip()
    live_ladder_rows = [row for row in (evidence.get("live_ladder_rows") or []) if isinstance(row, dict)]
    if not live_ladder_rows and first_live_label:
        live_ladder_rows = [
            {
                "bucket_label": first_live_label,
                "best_bid": evidence.get("first_live_bucket_bid"),
                "best_ask": evidence.get("first_live_bucket_ask"),
            }
        ]
    observed_at = _format_signal_time(
        signal.get("observed_at_utc"),
        observed_at_local=observed_at_local,
        local_tz_label=local_tz_label,
    )

    collapse_clauses: list[str] = []
    ladder_lines: list[str] = []

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
        return f"盘口观察：{bucket_label} " + "，".join(parts) + "。"

    if signal_type == "report_temp_scan_floor_stop":
        collapsed = [str(x).strip() for x in (evidence.get("collapsed_prefix_labels") or []) if str(x).strip()]
        collapsed_prev_bids = dict(evidence.get("collapsed_prefix_prev_bids") or {})
        collapsed_current_bids = dict(evidence.get("collapsed_prefix_current_bids") or {})
        collapsed_current_asks = dict(evidence.get("collapsed_prefix_current_asks") or {})
        price_floor = _to_float(evidence.get("price_floor")) or 0.02
        tick_cents = infer_market_tick_cents(
            evidence.get("first_live_bucket_bid"),
            evidence.get("first_live_bucket_ask"),
            *collapsed_prev_bids.values(),
            *collapsed_current_bids.values(),
            *collapsed_current_asks.values(),
            *[row.get("best_bid") for row in live_ladder_rows],
            *[row.get("best_ask") for row in live_ladder_rows],
        )
        for collapsed_label in collapsed:
            collapsed_label_text = f"{collapsed_label} Yes"
            collapsed_prev_bid_value = _to_float(collapsed_prev_bids.get(collapsed_label))
            collapsed_bid_now_value = _to_float(collapsed_current_bids.get(collapsed_label))
            collapsed_ask_now_value = _to_float(collapsed_current_asks.get(collapsed_label))
            if collapsed_prev_bid_value is not None and collapsed_prev_bid_value >= price_floor:
                collapse_clauses.append(
                    f"{collapsed_label_text} 由 "
                    f"{format_price_cents(collapsed_prev_bid_value, tick_cents=tick_cents, none_text='N/A')} "
                    "短时间跌至接近归零"
                )
                continue
            collapse_parts: list[str] = []
            if collapsed_bid_now_value is None or collapsed_bid_now_value < price_floor:
                collapse_parts.append("盘口短时归零")
            if collapsed_ask_now_value is not None and collapsed_ask_now_value <= 0.01:
                collapse_parts.append("卖盘压到 1¢ 或更低")
            collapse_body = "，".join(collapse_parts) if collapse_parts else "当前接近归零"
            if len(collapse_parts) >= 2 and collapse_parts[0] == "盘口短时归零":
                collapse_body = f"盘口短时归零（{collapse_parts[1]}）"
            collapse_clauses.append(f"{collapsed_label_text} {collapse_body}")
        for row in live_ladder_rows:
            label = str(row.get("bucket_label") or "").strip()
            if not label:
                continue
            ladder_lines.append(
                _format_ladder_row(
                    label,
                    bid_value=row.get("best_bid"),
                    ask_value=row.get("best_ask"),
                    tick_cents=tick_cents,
                )
            )
    elif signal_type == "report_temp_top_bucket_lock_in":
        collapsed = [str(x).strip() for x in (evidence.get("collapsed_lower_bucket_labels") or []) if str(x).strip()]
        top_label = str(evidence.get("top_bucket_label") or first_live_label).strip()
        if collapsed and top_label:
            for label in collapsed:
                collapse_clauses.append(f"{label} 接近归零")
            collapse_clauses.append(f"仅 {top_label} 仍保留有效报价")
        tick_cents = infer_market_tick_cents(
            *[row.get("best_bid") for row in live_ladder_rows],
            *[row.get("best_ask") for row in live_ladder_rows],
        )
        for row in live_ladder_rows:
            label = str(row.get("bucket_label") or "").strip()
            if not label:
                continue
            ladder_lines.append(
                _format_ladder_row(
                    label,
                    bid_value=row.get("best_bid"),
                    ask_value=row.get("best_ask"),
                    tick_cents=tick_cents,
                )
            )
    elif signal_type == "report_temp_lower_bound_jump":
        bucket_label = str(evidence.get("bucket_label") or "").strip()
        if bucket_label:
            collapse_text = _collapse_text(
                bucket_label,
                bid_now=evidence.get("best_bid"),
                ask_now=evidence.get("best_ask"),
            ) or f"盘口观察：{bucket_label} 买盘接近归零或卖盘压到 1¢ 或更低。"
            collapse_clauses.append(collapse_text.removeprefix("盘口观察：").removesuffix("。"))

    lines = [_title_with_date(city, station_icao, scheduled_report_label, observed_at_local, signal)]
    if observed_at:
        lines.append(observed_at)
    metar_peak_note = _format_observed_max_note(
        observed_max_temp_c,
        observed_max_temp_quantized=bool(observed_max_temp_quantized),
        observed_max_time_local=observed_max_time_local,
        display_unit=temperature_unit,
    )
    effective_bucket_label = first_live_label or top_bucket_label or target_bucket_label
    if signal_type == "report_temp_top_bucket_lock_in":
        if effective_bucket_label:
            lines.append(f"• *推测最新报最高温：{effective_bucket_label}*")
        else:
            lines.append(f"• {message}")
    elif signal_type == "report_temp_scan_floor_stop":
        if effective_bucket_label:
            lines.append(f"• *推测最新报最高温：{effective_bucket_label}*")
        elif _format_temp_label(target_bucket_native, temperature_unit):
            lines.append(f"• *推测最新报最高温：{_format_temp_label(target_bucket_native, temperature_unit)}*")
        elif target_bucket is not None:
            lines.append(f"• *推测最新报最高温：{_format_temp_label(target_bucket, 'C')}*")
        elif lower_bound is not None:
            lines.append(
                f"• *推测最新报最高温：{_format_temp_label(lower_bound_native or lower_bound, temperature_unit if lower_bound_native is not None else 'C')}*"
            )
    elif lower_bound_native is not None:
        lines.append(f"• 市场隐含最新报下界：{_format_temp_label(lower_bound_native, temperature_unit, prefix='>= ')}")
    elif lower_bound is not None:
        lines.append(f"• 市场隐含最新报下界：{_format_temp_label(lower_bound, 'C', prefix='>= ')}")
    elif message:
        lines.append(f"• {message}")
    if metar_peak_note:
        lines.append(f"• {metar_peak_note}")
    if collapse_clauses:
        lines.append(f"• 盘口观察：{'；'.join(collapse_clauses)}。")
    if ladder_lines:
        lines.append("")
        lines.append("*当前市场盘口价格：*")
    for ladder_line in ladder_lines:
        lines.append(ladder_line)
    if polymarket_event_url:
        lines.append(f"🔗 [Polymarket 市场]({polymarket_event_url})")
    lines.append("（基于盘口异动推测，不代表官方实况）")
    return "\n".join(lines)
