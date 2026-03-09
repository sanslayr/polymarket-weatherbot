from __future__ import annotations

import json
import re
from typing import Any

from polymarket_client import fetch_polymarket_event_markets, poly_slug_from_url
from polymarket_range_match import parse_slug_interval, pretty_label_from_slug


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


def _bucket_meta_from_slug(slug: str, question: str = "") -> dict[str, Any]:
    q = str(question or "")
    s = str(slug or "").lower()

    m = re.search(r"be\s+(\d+)[°º]c\s+or\s+below", q, flags=re.IGNORECASE)
    if m:
        n = int(m.group(1))
        return {
            "bucket_label": f"{n}°C or below",
            "bucket_kind": "at_or_below",
            "threshold_c": n,
            "lower_bound_c": None,
            "upper_bound_c": n + 0.49,
        }

    m = re.search(r"be\s+(\d+)[°º]c\s+or\s+higher", q, flags=re.IGNORECASE)
    if m:
        n = int(m.group(1))
        return {
            "bucket_label": f"{n}°C or higher",
            "bucket_kind": "at_or_above",
            "threshold_c": n,
            "lower_bound_c": n - 0.5,
            "upper_bound_c": None,
        }

    m = re.search(r"be\s+(\d+)[°º]c\b", q, flags=re.IGNORECASE)
    if m:
        n = int(m.group(1))
        return {
            "bucket_label": f"{n}°C",
            "bucket_kind": "exact",
            "threshold_c": n,
            "lower_bound_c": n - 0.5,
            "upper_bound_c": n + 0.49,
        }

    tail = re.search(r"-(\d+)corbelow$", s)
    if tail:
        n = int(tail.group(1))
        return {
            "bucket_label": f"{n}°C or below",
            "bucket_kind": "at_or_below",
            "threshold_c": n,
            "lower_bound_c": None,
            "upper_bound_c": n + 0.49,
        }

    tail = re.search(r"-(\d+)corhigher$", s)
    if tail:
        n = int(tail.group(1))
        return {
            "bucket_label": f"{n}°C or higher",
            "bucket_kind": "at_or_above",
            "threshold_c": n,
            "lower_bound_c": n - 0.5,
            "upper_bound_c": None,
        }

    tail = re.search(r"-(\d+)c$", s)
    if tail:
        n = int(tail.group(1))
        return {
            "bucket_label": f"{n}°C",
            "bucket_kind": "exact",
            "threshold_c": n,
            "lower_bound_c": n - 0.5,
            "upper_bound_c": n + 0.49,
        }

    interval = parse_slug_interval(slug)
    if interval is None:
        return {
            "bucket_label": pretty_label_from_slug(slug),
            "bucket_kind": "unknown",
            "threshold_c": None,
            "lower_bound_c": None,
            "upper_bound_c": None,
        }
    if interval.lo == float("-inf"):
        return {
            "bucket_label": pretty_label_from_slug(slug),
            "bucket_kind": "at_or_below",
            "threshold_c": int(interval.hi + 0.01),
            "lower_bound_c": None,
            "upper_bound_c": interval.hi,
        }
    if interval.hi == float("inf"):
        return {
            "bucket_label": pretty_label_from_slug(slug),
            "bucket_kind": "at_or_above",
            "threshold_c": int(interval.lo + 0.5),
            "lower_bound_c": interval.lo,
            "upper_bound_c": None,
        }
    if abs(interval.hi - interval.lo - 0.99) < 0.02:
        return {
            "bucket_label": pretty_label_from_slug(slug),
            "bucket_kind": "exact",
            "threshold_c": int(interval.lo + 0.5),
            "lower_bound_c": interval.lo,
            "upper_bound_c": interval.hi,
        }
    return {
        "bucket_label": pretty_label_from_slug(slug),
        "bucket_kind": "range",
        "threshold_c": None,
        "lower_bound_c": interval.lo,
        "upper_bound_c": interval.hi,
    }


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
