from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from contracts import MARKET_IMPLIED_WEATHER_SIGNAL_SCHEMA_VERSION


def _to_float(value: Any) -> float | None:
    try:
        if value in (None, ""):
            return None
        return float(value)
    except Exception:
        return None


def _to_dt(value: Any) -> datetime | None:
    try:
        if isinstance(value, datetime):
            dt = value
        else:
            text = str(value or "").strip()
            if not text:
                return None
            dt = datetime.fromisoformat(text.replace("Z", "+00:00"))
        if dt.tzinfo is None:
            return dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)
    except Exception:
        return None


def _format_temp_value(value: float | None) -> str | None:
    if value is None:
        return None
    rounded = round(float(value))
    if abs(float(value) - rounded) < 0.02:
        return str(int(rounded))
    return f"{float(value):.1f}".rstrip("0").rstrip(".")


def _bucket_temperature_unit(bucket: dict[str, Any]) -> str:
    unit = str(bucket.get("temperature_unit") or "").strip().upper()
    return unit if unit in {"C", "F"} else "C"


def _bucket_upper_bound_c(bucket: dict[str, Any]) -> float | None:
    upper = _to_float(bucket.get("upper_bound_c"))
    if upper is not None:
        return upper
    return _to_float(bucket.get("threshold_c"))


def _bucket_lower_bound_c(bucket: dict[str, Any]) -> float | None:
    lower = _to_float(bucket.get("lower_bound_c"))
    if lower is not None:
        return lower
    return _to_float(bucket.get("threshold_c"))


def _bucket_display_threshold(bucket: dict[str, Any]) -> tuple[float | None, str]:
    unit = _bucket_temperature_unit(bucket)
    native = _to_float(bucket.get("threshold_native"))
    if native is not None:
        return native, unit
    threshold_c = _to_float(bucket.get("threshold_c"))
    return threshold_c, "C"


def _increment_display_threshold(value: float | None, unit: str) -> float | None:
    if value is None:
        return None
    return float(value) + 1.0


def _window_seconds(now_utc: datetime | None, scheduled_report_utc: datetime | None) -> float | None:
    if now_utc is None or scheduled_report_utc is None:
        return None
    return (now_utc - scheduled_report_utc).total_seconds()


def _signal_confidence(
    *,
    within_trigger_window: bool,
    prev_bid: float | None,
    bid_now: float | None,
    ask_now: float | None,
    trade_price_now: float | None,
    trade_count_3m: float | None,
) -> str:
    score = 0.0
    if within_trigger_window:
        score += 1.0
    if prev_bid is not None and prev_bid >= 0.02:
        score += 0.9
    if bid_now is not None and bid_now <= 0.001:
        score += 0.8
    if ask_now is not None and ask_now <= 0.02:
        score += 0.6
    if trade_price_now is not None and trade_price_now <= 0.02:
        score += 0.6
    if trade_count_3m is not None and trade_count_3m >= 1:
        score += 0.4
    if score >= 3.0:
        return "high"
    if score >= 2.0:
        return "medium"
    return "low"


def _bucket_triggered(
    bucket: dict[str, Any],
    *,
    price_floor: float,
    ask_collapse_threshold: float,
) -> bool:
    prev_bid = _to_float(bucket.get("prev_best_bid"))
    bid_now = _to_float(bucket.get("best_bid"))
    ask_now = _to_float(bucket.get("best_ask"))
    no_bid = bool(bucket.get("no_bid")) or (bid_now is not None and bid_now <= 0.001)
    ask_collapsed = ask_now is not None and ask_now <= float(ask_collapse_threshold)
    return prev_bid is not None and prev_bid >= float(price_floor) and (no_bid or ask_collapsed)


def _bucket_dead_now(
    bucket: dict[str, Any],
    *,
    price_floor: float,
    ask_collapse_threshold: float,
) -> bool:
    prev_bid = _to_float(bucket.get("prev_best_bid"))
    bid_now = _to_float(bucket.get("best_bid"))
    ask_now = _to_float(bucket.get("best_ask"))
    trade_price_now = _to_float(bucket.get("last_trade_price"))
    if prev_bid is None or prev_bid < float(price_floor):
        return False
    exhausted_bid = bid_now is None or bid_now <= min(0.005, float(price_floor) / 4.0)
    ask_collapsed = ask_now is not None and ask_now <= float(ask_collapse_threshold)
    trade_collapsed = trade_price_now is not None and trade_price_now <= float(ask_collapse_threshold)
    return exhausted_bid and (ask_collapsed or trade_collapsed)


def _bucket_live_now(
    bucket: dict[str, Any],
    *,
    price_floor: float,
    ask_collapse_threshold: float,
) -> bool:
    bid_now = _to_float(bucket.get("best_bid"))
    return bid_now is not None and bid_now >= float(price_floor)


def _build_live_ladder_rows(buckets: list[dict[str, Any]]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for bucket in buckets:
        if not isinstance(bucket, dict):
            continue
        label = str(bucket.get("bucket_label") or "").strip()
        if not label:
            continue
        rows.append(
            {
                "bucket_label": label,
                "best_bid": _to_float(bucket.get("best_bid")),
                "best_ask": _to_float(bucket.get("best_ask")),
            }
        )
    return rows


def _build_ascending_scan_signal(
    *,
    first_live_bucket: dict[str, Any],
    triggered_prefix: list[dict[str, Any]],
    live_ladder_buckets: list[dict[str, Any]],
    scheduled_dt: datetime | None,
    now_dt: datetime,
    delta_seconds: float | None,
    within_trigger_window: bool,
    price_floor: float,
    ask_collapse_threshold: float,
    confidence: str,
    consistency_with_observed: str,
    trigger_mode: str,
) -> dict[str, Any]:
    first_live_threshold = _to_float(first_live_bucket.get("threshold_c"))
    first_live_label = str(first_live_bucket.get("bucket_label") or "")
    first_live_kind = str(first_live_bucket.get("bucket_kind") or "")
    first_live_display_value, display_unit = _bucket_display_threshold(first_live_bucket)
    collapsed_prefix_labels = [str(bucket.get("bucket_label") or "") for bucket in triggered_prefix]
    collapsed_prefix_prev_bids = {
        str(bucket.get("bucket_label") or ""): _to_float(bucket.get("prev_best_bid"))
        for bucket in triggered_prefix
        if str(bucket.get("bucket_label") or "").strip()
    }
    collapsed_prefix_current_bids = {
        str(bucket.get("bucket_label") or ""): _to_float(bucket.get("best_bid"))
        for bucket in triggered_prefix
        if str(bucket.get("bucket_label") or "").strip()
    }
    collapsed_prefix_current_asks = {
        str(bucket.get("bucket_label") or ""): _to_float(bucket.get("best_ask"))
        for bucket in triggered_prefix
        if str(bucket.get("bucket_label") or "").strip()
    }
    signal_type = "report_temp_top_bucket_lock_in" if first_live_kind == "at_or_above" else "report_temp_scan_floor_stop"
    return {
        "schema_version": MARKET_IMPLIED_WEATHER_SIGNAL_SCHEMA_VERSION,
        "signal_type": signal_type,
        "triggered": True,
        "implied_report_temp_lower_bound_c": first_live_threshold,
        "implied_report_temp_lower_bound_native": first_live_display_value,
        "bound_operator": ">=" if first_live_kind == "at_or_above" else "~=",
        "target_bucket_label": first_live_label,
        "target_bucket_threshold_c": first_live_threshold,
        "target_bucket_threshold_native": first_live_display_value,
        "temperature_unit": display_unit,
        "confidence": confidence,
        "scheduled_report_utc": scheduled_dt.isoformat().replace("+00:00", "Z") if scheduled_dt else None,
        "observed_at_utc": now_dt.isoformat().replace("+00:00", "Z"),
        "delta_from_report_seconds": delta_seconds,
        "within_report_window": within_trigger_window,
        "consistency_with_observed": consistency_with_observed,
        "evidence": {
            "first_live_bucket_label": first_live_label,
            "first_live_bucket_threshold_c": first_live_threshold,
            "first_live_bucket_threshold_native": first_live_display_value,
            "temperature_unit": display_unit,
            "first_live_bucket_kind": first_live_kind,
            "first_live_bucket_bid": _to_float(first_live_bucket.get("best_bid")),
            "first_live_bucket_ask": _to_float(first_live_bucket.get("best_ask")),
            "collapsed_prefix_count": len(triggered_prefix),
            "collapsed_prefix_labels": collapsed_prefix_labels,
            "collapsed_prefix_prev_bids": collapsed_prefix_prev_bids,
            "collapsed_prefix_current_bids": collapsed_prefix_current_bids,
            "collapsed_prefix_current_asks": collapsed_prefix_current_asks,
            "live_ladder_rows": _build_live_ladder_rows(live_ladder_buckets),
            "price_floor": float(price_floor),
            "ask_collapse_threshold": float(ask_collapse_threshold),
            "trigger_mode": trigger_mode,
        },
        "message": (
            f"盘口异常提示：向上正序扫描后，最低仍未被打死的档位是 {first_live_label}，"
            f"市场当前更像先按这一档附近交易。"
        ),
    }


def infer_market_implied_report_signal(
    *,
    bucket_snapshots: list[dict[str, Any]],
    scheduled_report_utc: str | datetime | None,
    now_utc: str | datetime | None = None,
    latest_observed_temp_c: float | None = None,
    price_floor: float = 0.02,
    trigger_window_start_seconds: int = 30,
    trigger_window_end_seconds: int = 300,
    ask_collapse_threshold: float = 0.01,
) -> dict[str, Any]:
    now_dt = _to_dt(now_utc) or datetime.now(timezone.utc)
    scheduled_dt = _to_dt(scheduled_report_utc)
    delta_seconds = _window_seconds(now_dt, scheduled_dt)
    within_trigger_window = (
        delta_seconds is not None
        and float(trigger_window_start_seconds) <= delta_seconds <= float(trigger_window_end_seconds)
    )

    best_signal: dict[str, Any] | None = None
    best_score = -1.0

    normalized_buckets = [dict(bucket) for bucket in (bucket_snapshots or []) if isinstance(bucket, dict)]

    scan_candidates = [
        bucket
        for bucket in normalized_buckets
        if str(bucket.get("bucket_kind") or "").strip().lower() in {"exact", "range", "at_or_above", "or_higher"}
        and _bucket_lower_bound_c(bucket) is not None
    ]
    if within_trigger_window and scan_candidates:
        scan_candidates.sort(key=lambda item: float(_bucket_lower_bound_c(item) or 0.0))
        if latest_observed_temp_c is not None:
            scan_candidates = [
                bucket
                for bucket in scan_candidates
                if (_bucket_upper_bound_c(bucket) is None or float(_bucket_upper_bound_c(bucket)) >= float(latest_observed_temp_c))
            ]
        triggered_prefix: list[dict[str, Any]] = []
        first_live_bucket: dict[str, Any] | None = None
        for bucket in scan_candidates:
            if _bucket_triggered(bucket, price_floor=price_floor, ask_collapse_threshold=ask_collapse_threshold):
                triggered_prefix.append(bucket)
                continue
            first_live_bucket = bucket
            break
        if (
            triggered_prefix
            and first_live_bucket is not None
            and _bucket_live_now(first_live_bucket, price_floor=price_floor, ask_collapse_threshold=ask_collapse_threshold)
        ):
            return _build_ascending_scan_signal(
                first_live_bucket=first_live_bucket,
                triggered_prefix=triggered_prefix,
                live_ladder_buckets=[
                    bucket
                    for bucket in scan_candidates
                    if float(_bucket_lower_bound_c(bucket) or -9999.0) >= float(_bucket_lower_bound_c(first_live_bucket) or -9999.0)
                ],
                scheduled_dt=scheduled_dt,
                now_dt=now_dt,
                delta_seconds=delta_seconds,
                within_trigger_window=within_trigger_window,
                price_floor=price_floor,
                ask_collapse_threshold=ask_collapse_threshold,
                confidence="high" if len(triggered_prefix) >= 2 else "medium",
                consistency_with_observed="ascending_scan_first_live_bucket",
                trigger_mode="ascending_scan_stop_on_first_live_bucket",
            )

        dead_prefix: list[dict[str, Any]] = []
        first_live_bucket = None
        for bucket in scan_candidates:
            if _bucket_dead_now(bucket, price_floor=price_floor, ask_collapse_threshold=ask_collapse_threshold):
                dead_prefix.append(bucket)
                continue
            first_live_bucket = bucket
            break
        if (
            dead_prefix
            and first_live_bucket is not None
            and _bucket_live_now(first_live_bucket, price_floor=price_floor, ask_collapse_threshold=ask_collapse_threshold)
        ):
            return _build_ascending_scan_signal(
                first_live_bucket=first_live_bucket,
                triggered_prefix=dead_prefix,
                live_ladder_buckets=[
                    bucket
                    for bucket in scan_candidates
                    if float(_bucket_lower_bound_c(bucket) or -9999.0) >= float(_bucket_lower_bound_c(first_live_bucket) or -9999.0)
                ],
                scheduled_dt=scheduled_dt,
                now_dt=now_dt,
                delta_seconds=delta_seconds,
                within_trigger_window=within_trigger_window,
                price_floor=price_floor,
                ask_collapse_threshold=ask_collapse_threshold,
                confidence="medium" if len(dead_prefix) >= 2 else "low",
                consistency_with_observed="ascending_scan_first_live_bucket_no_baseline",
                trigger_mode="ascending_scan_stop_on_first_live_bucket_no_baseline",
            )

    top_higher_candidates = [
        bucket
        for bucket in normalized_buckets
        if str(bucket.get("bucket_kind") or "").strip().lower() in {"at_or_above", "or_higher"}
        and _to_float(bucket.get("threshold_c")) is not None
    ]
    if within_trigger_window and top_higher_candidates:
        top_higher = max(top_higher_candidates, key=lambda item: float(item.get("threshold_c")))
        top_threshold = _to_float(top_higher.get("threshold_c"))
        if top_threshold is not None:
            lower_buckets = [
                bucket
                for bucket in normalized_buckets
                if _to_float(bucket.get("threshold_c")) is not None and float(bucket.get("threshold_c")) < top_threshold
            ]
            if lower_buckets:
                lower_dead = []
                for bucket in lower_buckets:
                    if _bucket_triggered(bucket, price_floor=price_floor, ask_collapse_threshold=ask_collapse_threshold):
                        lower_dead.append(bucket)
                if len(lower_dead) == len(lower_buckets):
                    return {
                        "schema_version": MARKET_IMPLIED_WEATHER_SIGNAL_SCHEMA_VERSION,
                        "signal_type": "report_temp_top_bucket_lock_in",
                        "triggered": True,
                        "implied_report_temp_lower_bound_c": top_threshold,
                        "implied_report_temp_lower_bound_native": _bucket_display_threshold(top_higher)[0],
                        "bound_operator": ">=",
                        "target_bucket_label": str(top_higher.get("bucket_label") or ""),
                        "target_bucket_threshold_c": top_threshold,
                        "target_bucket_threshold_native": _bucket_display_threshold(top_higher)[0],
                        "temperature_unit": _bucket_display_threshold(top_higher)[1],
                        "confidence": "high" if len(lower_dead) >= 2 else "medium",
                        "scheduled_report_utc": scheduled_dt.isoformat().replace("+00:00", "Z") if scheduled_dt else None,
                        "observed_at_utc": now_dt.isoformat().replace("+00:00", "Z"),
                        "delta_from_report_seconds": delta_seconds,
                        "within_report_window": within_trigger_window,
                        "consistency_with_observed": "top_bucket_market_lock_in",
                        "evidence": {
                            "top_bucket_label": str(top_higher.get("bucket_label") or ""),
                            "top_bucket_threshold_c": top_threshold,
                            "top_bucket_threshold_native": _bucket_display_threshold(top_higher)[0],
                            "temperature_unit": _bucket_display_threshold(top_higher)[1],
                            "collapsed_lower_bucket_count": len(lower_dead),
                            "collapsed_lower_bucket_labels": [str(bucket.get("bucket_label") or "") for bucket in lower_dead],
                            "live_ladder_rows": _build_live_ladder_rows(
                                [
                                    bucket
                                    for bucket in top_higher_candidates
                                    if float(_to_float(bucket.get("threshold_c")) or -9999.0) >= float(top_threshold)
                                ]
                            ),
                            "price_floor": float(price_floor),
                            "ask_collapse_threshold": float(ask_collapse_threshold),
                            "trigger_mode": "all_lower_buckets_dead_except_top_or_higher",
                        },
                        "message": f"盘口异常提示：除“{str(top_higher.get('bucket_label') or '')}”外，其余关键低档基本被打死，市场大概率已按最新报进入 {str(top_higher.get('bucket_label') or '')} 交易。",
                    }

    for bucket in normalized_buckets:
        if not isinstance(bucket, dict):
            continue
        category = str(bucket.get("bucket_kind") or "").strip().lower()
        threshold_c = _to_float(bucket.get("threshold_c"))
        if category not in {"at_or_below", "or_below"} or threshold_c is None:
            continue

        prev_bid = _to_float(bucket.get("prev_best_bid"))
        bid_now = _to_float(bucket.get("best_bid"))
        ask_now = _to_float(bucket.get("best_ask"))
        trade_price_now = _to_float(bucket.get("last_trade_price"))
        trade_count_3m = _to_float(bucket.get("trade_count_3m"))
        no_bid = bool(bucket.get("no_bid")) or (bid_now is not None and bid_now <= 0.001)
        ask_collapsed = ask_now is not None and ask_now <= float(ask_collapse_threshold)

        if prev_bid is None or prev_bid < float(price_floor):
            continue
        if not within_trigger_window:
            continue
        if not (no_bid or ask_collapsed):
            continue

        if latest_observed_temp_c is not None and float(latest_observed_temp_c) > threshold_c:
            consistency = "already_consistent_with_observed"
        else:
            consistency = "market_leads_observed_or_missing"

        display_threshold, display_unit = _bucket_display_threshold(bucket)
        implied_display_lower_bound = _increment_display_threshold(display_threshold, display_unit)
        confidence = _signal_confidence(
            within_trigger_window=within_trigger_window,
            prev_bid=prev_bid,
            bid_now=bid_now,
            ask_now=ask_now,
            trade_price_now=trade_price_now,
            trade_count_3m=trade_count_3m,
        )

        score = prev_bid
        score += 0.6
        if ask_collapsed:
            score += 0.3
        if trade_count_3m is not None:
            score += min(0.3, trade_count_3m * 0.1)

        signal = {
            "schema_version": MARKET_IMPLIED_WEATHER_SIGNAL_SCHEMA_VERSION,
            "signal_type": "report_temp_lower_bound_jump",
            "triggered": True,
            "implied_report_temp_lower_bound_c": threshold_c + 1.0,
            "implied_report_temp_lower_bound_native": implied_display_lower_bound,
            "bound_operator": ">=",
            "target_bucket_label": str(bucket.get("bucket_label") or ""),
            "target_bucket_threshold_c": threshold_c,
            "target_bucket_threshold_native": display_threshold,
            "temperature_unit": display_unit,
            "confidence": confidence,
            "scheduled_report_utc": scheduled_dt.isoformat().replace("+00:00", "Z") if scheduled_dt else None,
            "observed_at_utc": now_dt.isoformat().replace("+00:00", "Z"),
            "delta_from_report_seconds": delta_seconds,
            "within_report_window": within_trigger_window,
            "consistency_with_observed": consistency,
            "evidence": {
                "bucket_label": str(bucket.get("bucket_label") or ""),
                "prev_best_bid": prev_bid,
                "best_bid": bid_now,
                "best_ask": ask_now,
                "last_trade_price": trade_price_now,
                "trade_count_3m": trade_count_3m,
                "trade_volume_3m": _to_float(bucket.get("trade_volume_3m")),
                "price_floor": float(price_floor),
                "ask_collapse_threshold": float(ask_collapse_threshold),
                "temperature_unit": display_unit,
                "trigger_mode": "bid_swept_or_ask_collapsed",
            },
            "message": (
                f"盘口异常提示：市场大概率已按“最新报 > {_format_temp_value(display_threshold)}°{display_unit}”交易，"
                f"隐含最新报下界可先看 >= {_format_temp_value(implied_display_lower_bound)}°{display_unit}。"
            ),
        }
        if score > best_score:
            best_score = score
            best_signal = signal

    if best_signal:
        return best_signal

    return {
        "schema_version": MARKET_IMPLIED_WEATHER_SIGNAL_SCHEMA_VERSION,
        "signal_type": "report_temp_lower_bound_jump",
        "triggered": False,
        "implied_report_temp_lower_bound_c": None,
        "implied_report_temp_lower_bound_native": None,
        "bound_operator": None,
        "target_bucket_threshold_c": None,
        "target_bucket_threshold_native": None,
        "temperature_unit": None,
        "confidence": "none",
        "scheduled_report_utc": scheduled_dt.isoformat().replace("+00:00", "Z") if scheduled_dt else None,
        "observed_at_utc": now_dt.isoformat().replace("+00:00", "Z"),
        "delta_from_report_seconds": delta_seconds,
        "within_report_window": within_trigger_window,
        "consistency_with_observed": None,
        "evidence": {},
        "message": "",
    }
