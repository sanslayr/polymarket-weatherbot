from __future__ import annotations

import json
import os
import time
from threading import Lock
from typing import Any

import requests

POLYMARKET_TIMEOUT_SECONDS = float(os.getenv("POLYMARKET_TIMEOUT_SECONDS", "3") or "3")
POLYMARKET_EVENT_CACHE_TTL_SECONDS = int(os.getenv("POLYMARKET_EVENT_CACHE_TTL_SECONDS", "90") or "90")

_POLY_EVENT_CACHE: dict[str, tuple[float, bool, list[dict[str, Any]]]] = {}
_POLY_EVENT_CACHE_LOCK = Lock()


def poly_slug_from_url(polymarket_event_url: str) -> str:
    return str(polymarket_event_url or "").rstrip("/").split("/")[-1]


def fetch_polymarket_event_markets(
    slug: str,
    timeout: float | None = None,
    *,
    force_refresh: bool = False,
) -> tuple[bool, list[dict[str, Any]]]:
    now_ts = time.time()
    if not force_refresh:
        with _POLY_EVENT_CACHE_LOCK:
            hit = _POLY_EVENT_CACHE.get(slug)
            if hit and hit[0] > now_ts:
                return hit[1], list(hit[2])

    r = requests.get(
        "https://gamma-api.polymarket.com/events",
        params={"limit": 1, "slug": slug},
        timeout=(POLYMARKET_TIMEOUT_SECONDS if timeout is None else timeout),
    )
    r.raise_for_status()
    arr = r.json()
    if not arr:
        event_found = False
        markets: list[dict[str, Any]] = []
    else:
        event_found = True
        mk = arr[0].get("markets", [])
        markets = [m for m in mk if isinstance(m, dict)]

    with _POLY_EVENT_CACHE_LOCK:
        _POLY_EVENT_CACHE[slug] = (
            now_ts + POLYMARKET_EVENT_CACHE_TTL_SECONDS,
            event_found,
            list(markets),
        )

    return event_found, markets


def prefetch_polymarket_event(
    polymarket_event_url: str,
    *,
    force_refresh: bool = False,
) -> tuple[bool, list[dict[str, Any]]] | None:
    try:
        slug = poly_slug_from_url(polymarket_event_url)
        if not slug:
            return None
        return fetch_polymarket_event_markets(slug, force_refresh=force_refresh)
    except Exception:
        return None
