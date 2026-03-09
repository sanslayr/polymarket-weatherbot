from __future__ import annotations

from typing import Any

from market_implied_weather_signal import infer_market_implied_report_signal
from market_metadata_service import build_market_catalog_snapshot
from market_stream_service import stream_market_state
from market_subscription_policy import build_market_subscription_plan


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
                "threshold_c": market.get("threshold_c"),
                "best_bid": cur.get("best_bid"),
                "best_ask": cur.get("best_ask"),
                "prev_best_bid": prev.get("best_bid"),
                "prev_best_ask": prev.get("best_ask"),
                "last_trade_price": cur.get("last_trade_price"),
                "trade_count_3m": cur.get("trade_count_3m"),
                "yes_token_id": yes_token_id,
            }
        )
    return out


def run_market_monitor_cycle(
    *,
    polymarket_event_url: str,
    observed_max_temp_c: float | None,
    scheduled_report_utc: str,
    report_window_active: bool = True,
    daily_peak_state: str = "open",
    previous_state: dict[str, dict[str, Any]] | None = None,
    stream_seconds: float = 4.0,
) -> dict[str, Any]:
    catalog = build_market_catalog_snapshot(polymarket_event_url, force_refresh=True)
    plan = build_market_subscription_plan(
        market_catalog_snapshot=catalog,
        observed_max_temp_c=observed_max_temp_c,
        report_window_active=report_window_active,
        daily_peak_state=daily_peak_state,
    )
    subscribed = list(dict.fromkeys((plan.get("core_watch_asset_ids") or []) + (plan.get("upside_scan_asset_ids") or [])))
    stream_result = stream_market_state(asset_ids=subscribed, duration_seconds=stream_seconds) if subscribed else {"messages": [], "state": {}}
    bucket_snapshots = build_bucket_snapshots(
        market_catalog_snapshot=catalog,
        current_state=stream_result.get("state") or {},
        previous_state=previous_state,
    )
    signal = infer_market_implied_report_signal(
        bucket_snapshots=bucket_snapshots,
        scheduled_report_utc=scheduled_report_utc,
        latest_observed_temp_c=observed_max_temp_c,
    )
    return {
        "catalog": catalog,
        "subscription_plan": plan,
        "stream_result": stream_result,
        "bucket_snapshots": bucket_snapshots,
        "signal": signal,
    }
