from __future__ import annotations

import os
from datetime import datetime, timedelta, timezone
from typing import Any

from market_implied_weather_signal import infer_market_implied_report_signal
from market_metadata_service import build_market_catalog_snapshot
from market_stream_service import monitor_market_state, stream_market_state
from market_subscription_policy import build_market_subscription_plan


def _load_catalog(polymarket_event_url: str) -> dict[str, Any]:
    catalog = build_market_catalog_snapshot(polymarket_event_url, force_refresh=False)
    if catalog.get("event_found") and (catalog.get("markets") or []):
        return catalog
    return build_market_catalog_snapshot(polymarket_event_url, force_refresh=True)


def _to_dt(value: Any) -> datetime | None:
    try:
        text = str(value or "").strip()
        if not text:
            return None
        dt = datetime.fromisoformat(text.replace("Z", "+00:00"))
        if dt.tzinfo is None:
            return dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)
    except Exception:
        return None


def _clone_state_snapshot(state: dict[str, dict[str, Any]] | None) -> dict[str, dict[str, Any]]:
    out: dict[str, dict[str, Any]] = {}
    for asset_id, snapshot in (state or {}).items():
        if not isinstance(snapshot, dict):
            continue
        token = str(asset_id or "").strip()
        if not token:
            continue
        out[token] = dict(snapshot)
    return out


def _state_has_book_data(state: dict[str, dict[str, Any]] | None) -> bool:
    for snapshot in (state or {}).values():
        if not isinstance(snapshot, dict):
            continue
        if snapshot.get("best_bid") is not None or snapshot.get("best_ask") is not None or snapshot.get("last_trade_price") is not None:
            return True
    return False


def _signal_price_floor() -> float:
    try:
        return max(0.0, float(os.getenv("MARKET_SIGNAL_PRICE_FLOOR", "0.02") or "0.02"))
    except Exception:
        return 0.02


def _monitor_diagnostics(
    *,
    subscribed_asset_ids: list[str],
    stream_result: dict[str, Any],
) -> dict[str, Any]:
    baseline_state = _clone_state_snapshot(stream_result.get("baseline_state") or {})
    final_state = _clone_state_snapshot(stream_result.get("state") or {})
    baseline_has_book_data = _state_has_book_data(baseline_state)
    final_has_book_data = _state_has_book_data(final_state)
    diagnostics = {
        "subscribed_asset_count": len([item for item in subscribed_asset_ids if str(item).strip()]),
        "baseline_asset_count": len(baseline_state),
        "final_asset_count": len(final_state),
        "message_count": len(stream_result.get("messages") or []),
        "baseline_has_book_data": baseline_has_book_data,
        "final_has_book_data": final_has_book_data,
        "triggered_payload_present": bool(stream_result.get("triggered_payload")),
    }
    if not diagnostics["subscribed_asset_count"]:
        diagnostics["status"] = "no_subscriptions"
        diagnostics["ok"] = False
        return diagnostics
    if not (baseline_has_book_data or final_has_book_data):
        diagnostics["status"] = "no_market_data"
        diagnostics["ok"] = False
        return diagnostics
    diagnostics["status"] = "ok"
    diagnostics["ok"] = True
    return diagnostics


def _subscription_inputs(
    *,
    polymarket_event_url: str,
    observed_max_temp_c: float | None,
    report_window_active: bool,
    daily_peak_state: str,
    core_only: bool,
) -> tuple[dict[str, Any], dict[str, Any], list[str]]:
    catalog = _load_catalog(polymarket_event_url)
    plan = build_market_subscription_plan(
        market_catalog_snapshot=catalog,
        observed_max_temp_c=observed_max_temp_c,
        report_window_active=report_window_active,
        daily_peak_state=daily_peak_state,
        core_only=core_only,
    )
    subscribed = list(dict.fromkeys((plan.get("core_watch_asset_ids") or []) + (plan.get("upside_scan_asset_ids") or [])))
    return catalog, plan, subscribed


def build_bucket_snapshots(
    *,
    market_catalog_snapshot: dict[str, Any],
    current_state: dict[str, dict[str, Any]],
    previous_state: dict[str, dict[str, Any]] | None = None,
) -> list[dict[str, Any]]:
    prev_map = previous_state or {}
    out: list[dict[str, Any]] = []
    for market in market_catalog_snapshot.get("markets") or []:
        if not isinstance(market, dict):
            continue
        yes_token_id = str(market.get("yes_token_id") or "").strip()
        if not yes_token_id:
            continue
        cur = dict((current_state or {}).get(yes_token_id) or {})
        prev = dict((prev_map or {}).get(yes_token_id) or {})
        out.append(
            {
                "bucket_label": market.get("bucket_label"),
                "bucket_kind": market.get("bucket_kind"),
                "temperature_unit": market.get("temperature_unit"),
                "threshold_native": market.get("threshold_native"),
                "threshold_c": market.get("threshold_c"),
                "lower_bound_native": market.get("lower_bound_native"),
                "upper_bound_native": market.get("upper_bound_native"),
                "lower_bound_c": market.get("lower_bound_c"),
                "upper_bound_c": market.get("upper_bound_c"),
                "best_bid": cur.get("best_bid"),
                "best_ask": cur.get("best_ask"),
                "prev_best_bid": prev.get("best_bid"),
                "prev_best_ask": prev.get("best_ask"),
                "top_bid_size": cur.get("top_bid_size"),
                "top_ask_size": cur.get("top_ask_size"),
                "bid_depth_3": cur.get("bid_depth_3"),
                "ask_depth_3": cur.get("ask_depth_3"),
                "book_imbalance": cur.get("book_imbalance"),
                "mid_price": cur.get("mid_price"),
                "prev_mid_price": prev.get("mid_price"),
                "spread": cur.get("spread"),
                "staleness_ms": cur.get("staleness_ms"),
                "book_update_count_3m": cur.get("book_update_count_3m"),
                "last_trade_price": cur.get("last_trade_price"),
                "trade_count_3m": cur.get("trade_count_3m"),
                "trade_volume_3m": cur.get("trade_volume_3m"),
                "yes_token_id": yes_token_id,
            }
        )
    return out


def _infer_signal_payload(
    *,
    market_catalog_snapshot: dict[str, Any],
    current_state: dict[str, dict[str, Any]],
    previous_state: dict[str, dict[str, Any]] | None,
    scheduled_report_utc: str,
    observed_max_temp_c: float | None,
    observed_at_utc: str | None = None,
    continuous_mode: bool = False,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    bucket_snapshots = build_bucket_snapshots(
        market_catalog_snapshot=market_catalog_snapshot,
        current_state=current_state,
        previous_state=previous_state,
    )
    signal = infer_market_implied_report_signal(
        bucket_snapshots=bucket_snapshots,
        scheduled_report_utc=scheduled_report_utc,
        now_utc=observed_at_utc,
        latest_observed_temp_c=observed_max_temp_c,
        price_floor=_signal_price_floor(),
        continuous_mode=continuous_mode,
    )
    return bucket_snapshots, signal


def run_market_monitor_cycle(
    *,
    polymarket_event_url: str,
    observed_max_temp_c: float | None,
    scheduled_report_utc: str,
    report_window_active: bool = True,
    daily_peak_state: str = "open",
    previous_state: dict[str, dict[str, Any]] | None = None,
    stream_seconds: float = 4.0,
    core_only: bool = False,
) -> dict[str, Any]:
    catalog, plan, subscribed = _subscription_inputs(
        polymarket_event_url=polymarket_event_url,
        observed_max_temp_c=observed_max_temp_c,
        report_window_active=report_window_active,
        daily_peak_state=daily_peak_state,
        core_only=core_only,
    )
    stream_result = stream_market_state(asset_ids=subscribed, duration_seconds=stream_seconds) if subscribed else {"messages": [], "state": {}}
    monitor_diagnostics = _monitor_diagnostics(subscribed_asset_ids=subscribed, stream_result=stream_result)
    bucket_snapshots, signal = _infer_signal_payload(
        market_catalog_snapshot=catalog,
        current_state=stream_result.get("state") or {},
        previous_state=previous_state,
        scheduled_report_utc=scheduled_report_utc,
        observed_max_temp_c=observed_max_temp_c,
    )
    return {
        "catalog": catalog,
        "subscription_plan": plan,
        "stream_result": stream_result,
        "bucket_snapshots": bucket_snapshots,
        "monitor_ok": bool(monitor_diagnostics.get("ok")),
        "monitor_status": str(monitor_diagnostics.get("status") or "unknown"),
        "monitor_diagnostics": monitor_diagnostics,
        "signal": signal,
    }


def run_market_monitor_event_window(
    *,
    polymarket_event_url: str,
    observed_max_temp_c: float | None,
    scheduled_report_utc: str,
    daily_peak_state: str = "open",
    stream_seconds: float = 120.0,
    baseline_seconds: float = 2.0,
    core_only: bool = False,
    previous_state: dict[str, dict[str, Any]] | None = None,
    continuous_mode: bool = False,
) -> dict[str, Any]:
    catalog, plan, subscribed = _subscription_inputs(
        polymarket_event_url=polymarket_event_url,
        observed_max_temp_c=observed_max_temp_c,
        report_window_active=True,
        daily_peak_state=daily_peak_state,
        core_only=core_only,
    )
    scheduled_dt = _to_dt(scheduled_report_utc)
    trigger_window_starts_at = None if continuous_mode else (scheduled_dt + timedelta(seconds=30) if scheduled_dt is not None else None)
    reference_state: dict[str, dict[str, Any]] | None = None

    def _on_update(payload: dict[str, Any]) -> dict[str, Any] | None:
        nonlocal reference_state
        current_state = _clone_state_snapshot(payload.get("current_state") or {})
        observed_at_dt = _to_dt(payload.get("observed_at_utc"))
        if (
            not continuous_mode
            and previous_state is None
            and trigger_window_starts_at is not None
            and observed_at_dt is not None
            and observed_at_dt < trigger_window_starts_at
            and _state_has_book_data(current_state)
        ):
            # Keep the latest pre-report snapshot as the baseline for the first post-report move.
            reference_state = current_state
            return None
        effective_previous_state = _clone_state_snapshot(previous_state) or reference_state or _clone_state_snapshot(payload.get("baseline_state") or {})
        bucket_snapshots, signal = _infer_signal_payload(
            market_catalog_snapshot=catalog,
            current_state=current_state,
            previous_state=effective_previous_state,
            scheduled_report_utc=scheduled_report_utc,
            observed_max_temp_c=observed_max_temp_c,
            observed_at_utc=payload.get("observed_at_utc"),
            continuous_mode=continuous_mode,
        )
        if signal.get("triggered"):
            return {
                "signal": signal,
                "bucket_snapshots": bucket_snapshots,
            }
        return None

    stream_result = (
        monitor_market_state(
            asset_ids=subscribed,
            duration_seconds=stream_seconds,
            baseline_seconds=baseline_seconds,
            on_update=_on_update,
        )
        if subscribed
        else {"messages": [], "state": {}, "baseline_state": {}, "triggered_payload": None}
    )
    monitor_diagnostics = _monitor_diagnostics(subscribed_asset_ids=subscribed, stream_result=stream_result)
    triggered_payload = dict(stream_result.get("triggered_payload") or {})
    effective_previous_state = _clone_state_snapshot(previous_state) or reference_state or _clone_state_snapshot(stream_result.get("baseline_state") or {})
    if triggered_payload.get("bucket_snapshots") and triggered_payload.get("signal"):
        bucket_snapshots = triggered_payload.get("bucket_snapshots")
        signal = triggered_payload.get("signal")
    else:
        bucket_snapshots, signal = _infer_signal_payload(
            market_catalog_snapshot=catalog,
            current_state=stream_result.get("state") or {},
            previous_state=effective_previous_state,
            scheduled_report_utc=scheduled_report_utc,
            observed_max_temp_c=observed_max_temp_c,
            continuous_mode=continuous_mode,
        )
    return {
        "catalog": catalog,
        "subscription_plan": plan,
        "stream_result": stream_result,
        "bucket_snapshots": bucket_snapshots,
        "reference_state": effective_previous_state,
        "final_state": _clone_state_snapshot(stream_result.get("state") or {}),
        "monitor_ok": bool(monitor_diagnostics.get("ok")),
        "monitor_status": str(monitor_diagnostics.get("status") or "unknown"),
        "monitor_diagnostics": monitor_diagnostics,
        "signal": signal,
    }
