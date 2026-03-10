from __future__ import annotations

from typing import Any


def _to_float(value: Any) -> float | None:
    try:
        if value in (None, ""):
            return None
        return float(value)
    except Exception:
        return None


def _market_lower_bound_c(market: dict[str, Any]) -> float | None:
    lower = _to_float(market.get("lower_bound_c"))
    if lower is not None:
        return lower
    threshold = _to_float(market.get("threshold_c"))
    return threshold


def _market_upper_bound_c(market: dict[str, Any]) -> float | None:
    upper = _to_float(market.get("upper_bound_c"))
    if upper is not None:
        return upper
    threshold = _to_float(market.get("threshold_c"))
    return threshold


def _market_sort_key(market: dict[str, Any]) -> tuple[float, float]:
    lower = _market_lower_bound_c(market)
    upper = _market_upper_bound_c(market)
    return (
        float(lower) if lower is not None else float("-inf"),
        float(upper) if upper is not None else float("inf"),
    )


def _market_contains_observed(market: dict[str, Any], observed: float) -> bool:
    lower = _market_lower_bound_c(market)
    upper = _market_upper_bound_c(market)
    if lower is not None and observed < float(lower):
        return False
    if upper is not None and observed > float(upper):
        return False
    return True


def build_market_subscription_plan(
    *,
    market_catalog_snapshot: dict[str, Any],
    observed_max_temp_c: float | None,
    report_window_active: bool = False,
    daily_peak_state: str = "open",
    core_only: bool = False,
) -> dict[str, Any]:
    markets = [dict(item) for item in (market_catalog_snapshot.get("markets") or []) if isinstance(item, dict)]
    observed = _to_float(observed_max_temp_c)

    top_or_higher = None
    if markets:
        higher_markets = [m for m in markets if str(m.get("bucket_kind") or "") == "at_or_above" and _to_float(m.get("threshold_c")) is not None]
        if higher_markets:
            top_or_higher = max(higher_markets, key=_market_sort_key)

    if top_or_higher is not None and observed is not None:
        top_threshold = float(top_or_higher.get("threshold_c"))
        if observed >= top_threshold:
            return {
                "monitor_mode": "idle",
                "reason_codes": ["top_or_higher_already_reached"],
                "core_watch_asset_ids": [],
                "upside_scan_asset_ids": [],
                "drop_asset_ids": [
                    str(m.get("yes_token_id") or "") for m in markets if str(m.get("yes_token_id") or "").strip()
                ],
            }

    live_markets = [m for m in markets if not bool(m.get("closed")) and bool(m.get("active"))]
    if observed is None:
        core_market = None
    else:
        containing_hits = [
            m
            for m in live_markets
            if str(m.get("bucket_kind") or "").strip().lower() in {"exact", "range", "at_or_below", "at_or_above"}
            and _market_contains_observed(m, observed)
        ]
        if containing_hits:
            core_market = min(containing_hits, key=_market_sort_key)
        else:
            lower_hits = [
                m
                for m in live_markets
                if _market_lower_bound_c(m) is not None and float(_market_lower_bound_c(m)) > observed
            ]
            core_market = min(lower_hits, key=_market_sort_key) if lower_hits else None

    core_watch_asset_ids: list[str] = []
    upside_scan_asset_ids: list[str] = []
    reason_codes: list[str] = []

    if core_market is not None:
        core_watch_asset_ids.append(str(core_market.get("yes_token_id") or ""))
        core_upper = _market_upper_bound_c(core_market)
        if core_upper is not None:
            for market in live_markets:
                lower = _market_lower_bound_c(market)
                token_id = str(market.get("yes_token_id") or "").strip()
                if lower is None or lower <= core_upper or not token_id:
                    continue
                upside_scan_asset_ids.append(token_id)
            reason_codes.append("core_market_selected")
            if upside_scan_asset_ids:
                reason_codes.append("upside_scan_enabled")

    if report_window_active and not core_only:
        reason_codes.append("report_window_active")
        lower_neighbor = None
        if core_market is not None:
            core_lower = _market_lower_bound_c(core_market)
            lower_candidates = [
                m
                for m in live_markets
                if _market_upper_bound_c(m) is not None
                and core_lower is not None
                and float(_market_upper_bound_c(m)) < core_lower
            ]
            if lower_candidates:
                lower_neighbor = max(lower_candidates, key=_market_sort_key)
        if lower_neighbor is not None:
            token_id = str(lower_neighbor.get("yes_token_id") or "").strip()
            if token_id and token_id not in core_watch_asset_ids:
                core_watch_asset_ids.append(token_id)

    if daily_peak_state == "locked":
        reason_codes.append("peak_locked")

    core_watch_asset_ids = [item for item in core_watch_asset_ids if item]
    upside_scan_asset_ids = [item for item in upside_scan_asset_ids if item and item not in core_watch_asset_ids]

    monitor_mode = "core" if core_only else ("report_window_expand" if report_window_active else ("core_plus_upside" if upside_scan_asset_ids else "core"))

    return {
        "monitor_mode": monitor_mode,
        "reason_codes": reason_codes,
        "core_watch_asset_ids": core_watch_asset_ids,
        "upside_scan_asset_ids": [] if core_only else upside_scan_asset_ids,
        "drop_asset_ids": [
            str(m.get("yes_token_id") or "")
            for m in live_markets
            if str(m.get("yes_token_id") or "").strip()
            and str(m.get("yes_token_id") or "") not in core_watch_asset_ids
            and str(m.get("yes_token_id") or "") not in ([] if core_only else upside_scan_asset_ids)
        ],
    }
