from __future__ import annotations

import json
import re
from typing import Any

from polymarket_client import fetch_polymarket_event_markets, poly_slug_from_url
from polymarket_range_match import f_to_c, parse_slug_interval, pretty_label_from_slug


def _parse_json_list(value: Any) -> list[Any]:
    if isinstance(value, list):
        return list(value)
    text = str(value or "").strip()
    if not text:
        return []
    try:
        data = json.loads(text)
        return list(data) if isinstance(data, list) else []
    except Exception:
        return []


def _bound_to_c(value: float | None, unit: str | None) -> float | None:
    if value is None:
        return None
    normalized = str(unit or "").strip().upper()
    if normalized == "F":
        return f_to_c(float(value))
    return float(value)


def _bucket_meta(
    *,
    bucket_label: str,
    bucket_kind: str,
    temperature_unit: str | None,
    threshold_native: float | None,
    lower_bound_native: float | None,
    upper_bound_native: float | None,
) -> dict[str, Any]:
    unit = str(temperature_unit or "").strip().upper() or None
    threshold_c = _bound_to_c(threshold_native, unit)
    lower_bound_c = _bound_to_c(lower_bound_native, unit)
    upper_bound_c = _bound_to_c(upper_bound_native, unit)
    return {
        "bucket_label": bucket_label,
        "bucket_kind": bucket_kind,
        "temperature_unit": unit,
        "threshold_native": threshold_native,
        "threshold_c": threshold_c,
        "lower_bound_native": lower_bound_native,
        "upper_bound_native": upper_bound_native,
        "lower_bound_c": lower_bound_c,
        "upper_bound_c": upper_bound_c,
    }


def _bucket_meta_from_slug(slug: str, question: str = "") -> dict[str, Any]:
    q = str(question or "")
    s = str(slug or "").lower()

    m = re.search(r"be\s+(\d+)[°º]([cf])\s+or\s+below", q, flags=re.IGNORECASE)
    if m:
        n = int(m.group(1))
        unit = str(m.group(2) or "").upper()
        return _bucket_meta(
            bucket_label=f"{n}°{unit} or below",
            bucket_kind="at_or_below",
            temperature_unit=unit,
            threshold_native=float(n),
            lower_bound_native=None,
            upper_bound_native=n + 0.49,
        )

    m = re.search(r"be\s+(\d+)[°º]([cf])\s+or\s+higher", q, flags=re.IGNORECASE)
    if m:
        n = int(m.group(1))
        unit = str(m.group(2) or "").upper()
        return _bucket_meta(
            bucket_label=f"{n}°{unit} or higher",
            bucket_kind="at_or_above",
            temperature_unit=unit,
            threshold_native=float(n),
            lower_bound_native=n - 0.5,
            upper_bound_native=None,
        )

    m = re.search(r"be\s+(\d+)[°º]([cf])\b", q, flags=re.IGNORECASE)
    if m:
        n = int(m.group(1))
        unit = str(m.group(2) or "").upper()
        return _bucket_meta(
            bucket_label=f"{n}°{unit}",
            bucket_kind="exact",
            temperature_unit=unit,
            threshold_native=float(n),
            lower_bound_native=n - 0.5,
            upper_bound_native=n + 0.49,
        )

    tail = re.search(r"-(\d+)forbelow$", s)
    if tail:
        n = int(tail.group(1))
        return _bucket_meta(
            bucket_label=f"{n}°F or below",
            bucket_kind="at_or_below",
            temperature_unit="F",
            threshold_native=float(n),
            lower_bound_native=None,
            upper_bound_native=n + 0.49,
        )

    tail = re.search(r"-(\d+)forhigher$", s)
    if tail:
        n = int(tail.group(1))
        return _bucket_meta(
            bucket_label=f"{n}°F or higher",
            bucket_kind="at_or_above",
            temperature_unit="F",
            threshold_native=float(n),
            lower_bound_native=n - 0.5,
            upper_bound_native=None,
        )

    tail = re.search(r"-(\d+)f$", s)
    if tail:
        n = int(tail.group(1))
        return _bucket_meta(
            bucket_label=f"{n}°F",
            bucket_kind="exact",
            temperature_unit="F",
            threshold_native=float(n),
            lower_bound_native=n - 0.5,
            upper_bound_native=n + 0.49,
        )

    tail = re.search(r"-(\d+)corbelow$", s)
    if tail:
        n = int(tail.group(1))
        return _bucket_meta(
            bucket_label=f"{n}°C or below",
            bucket_kind="at_or_below",
            temperature_unit="C",
            threshold_native=float(n),
            lower_bound_native=None,
            upper_bound_native=n + 0.49,
        )

    tail = re.search(r"-(\d+)corhigher$", s)
    if tail:
        n = int(tail.group(1))
        return _bucket_meta(
            bucket_label=f"{n}°C or higher",
            bucket_kind="at_or_above",
            temperature_unit="C",
            threshold_native=float(n),
            lower_bound_native=n - 0.5,
            upper_bound_native=None,
        )

    tail = re.search(r"-(\d+)c$", s)
    if tail:
        n = int(tail.group(1))
        return _bucket_meta(
            bucket_label=f"{n}°C",
            bucket_kind="exact",
            temperature_unit="C",
            threshold_native=float(n),
            lower_bound_native=n - 0.5,
            upper_bound_native=n + 0.49,
        )

    interval = parse_slug_interval(slug)
    if interval is None:
        return _bucket_meta(
            bucket_label=pretty_label_from_slug(slug),
            bucket_kind="unknown",
            temperature_unit=None,
            threshold_native=None,
            lower_bound_native=None,
            upper_bound_native=None,
        )
    if interval.lo == float("-inf"):
        threshold_native = float(int(interval.hi + 0.01))
        return _bucket_meta(
            bucket_label=pretty_label_from_slug(slug),
            bucket_kind="at_or_below",
            temperature_unit=interval.unit,
            threshold_native=threshold_native,
            lower_bound_native=None,
            upper_bound_native=interval.hi,
        )
    if interval.hi == float("inf"):
        threshold_native = float(int(interval.lo + 0.5))
        return _bucket_meta(
            bucket_label=pretty_label_from_slug(slug),
            bucket_kind="at_or_above",
            temperature_unit=interval.unit,
            threshold_native=threshold_native,
            lower_bound_native=interval.lo,
            upper_bound_native=None,
        )
    if abs(interval.hi - interval.lo - 0.99) < 0.02:
        threshold_native = float(int(interval.lo + 0.5))
        return _bucket_meta(
            bucket_label=pretty_label_from_slug(slug),
            bucket_kind="exact",
            temperature_unit=interval.unit,
            threshold_native=threshold_native,
            lower_bound_native=interval.lo,
            upper_bound_native=interval.hi,
        )
    return _bucket_meta(
        bucket_label=pretty_label_from_slug(slug),
        bucket_kind="range",
        temperature_unit=interval.unit,
        threshold_native=None,
        lower_bound_native=interval.lo,
        upper_bound_native=interval.hi,
    )


def build_market_catalog_snapshot(polymarket_event_url: str, *, force_refresh: bool = False) -> dict[str, Any]:
    slug = poly_slug_from_url(polymarket_event_url)
    event_found, markets = fetch_polymarket_event_markets(slug, force_refresh=force_refresh)
    catalog_markets: list[dict[str, Any]] = []
    asset_ids: list[str] = []

    for market in markets:
        if not isinstance(market, dict):
            continue
        market_slug = str(market.get("slug") or "").strip()
        clob_token_ids = [str(x) for x in _parse_json_list(market.get("clobTokenIds")) if str(x).strip()]
        outcomes = [str(x) for x in _parse_json_list(market.get("outcomes")) if str(x).strip()]
        outcome_prices = [str(x) for x in _parse_json_list(market.get("outcomePrices")) if str(x).strip()]
        outcome_map: dict[str, dict[str, Any]] = {}
        for idx, outcome in enumerate(outcomes):
            token_id = clob_token_ids[idx] if idx < len(clob_token_ids) else None
            price = outcome_prices[idx] if idx < len(outcome_prices) else None
            outcome_map[outcome] = {"token_id": token_id, "price": price}
        yes_token_id = str((outcome_map.get("Yes") or {}).get("token_id") or "").strip() or None
        no_token_id = str((outcome_map.get("No") or {}).get("token_id") or "").strip() or None
        if yes_token_id:
            asset_ids.append(yes_token_id)
        bucket_meta = _bucket_meta_from_slug(market_slug, str(market.get("question") or ""))
        catalog_markets.append(
            {
                "question": str(market.get("question") or ""),
                "slug": market_slug,
                "market_id": str(market.get("conditionId") or market.get("market") or ""),
                "yes_token_id": yes_token_id,
                "no_token_id": no_token_id,
                "active": bool(market.get("active")),
                "closed": bool(market.get("closed")),
                **bucket_meta,
            }
        )

    return {
        "event_slug": slug,
        "event_found": bool(event_found),
        "markets": catalog_markets,
        "yes_asset_ids": asset_ids,
    }
