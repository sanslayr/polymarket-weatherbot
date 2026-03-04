#!/usr/bin/env python3
from __future__ import annotations

import argparse
import math
import re
from dataclasses import dataclass
from typing import Any

import requests

GAMMA_EVENTS = "https://gamma-api.polymarket.com/events"


@dataclass
class Interval:
    lo: float
    hi: float
    unit: str  # C|F


def c_to_f(x: float) -> float:
    return x * 9.0 / 5.0 + 32.0


def f_to_c(x: float) -> float:
    return (x - 32.0) * 5.0 / 9.0


def parse_slug_interval(slug: str) -> Interval | None:
    s = slug.lower()

    m = re.search(r"-(\d+)-(\d+)f$", s)
    if m:
        a, b = int(m.group(1)), int(m.group(2))
        return Interval(a - 0.5, b + 0.49, "F")

    m = re.search(r"-(\d+)-(\d+)c$", s)
    if m:
        a, b = int(m.group(1)), int(m.group(2))
        return Interval(a - 0.5, b + 0.49, "C")

    m = re.search(r"-(\d+)forbelow$", s)
    if m:
        n = int(m.group(1))
        return Interval(-math.inf, n + 0.49, "F")

    m = re.search(r"-(\d+)forhigher$", s)
    if m:
        n = int(m.group(1))
        return Interval(n - 0.5, math.inf, "F")

    m = re.search(r"-(\d+)corbelow$", s)
    if m:
        n = int(m.group(1))
        return Interval(-math.inf, n + 0.49, "C")

    m = re.search(r"-(\d+)corhigher$", s)
    if m:
        n = int(m.group(1))
        return Interval(n - 0.5, math.inf, "C")

    m = re.search(r"-(\d+)f$", s)
    if m:
        n = int(m.group(1))
        return Interval(n - 0.5, n + 0.49, "F")

    m = re.search(r"-(\d+)c$", s)
    if m:
        n = int(m.group(1))
        return Interval(n - 0.5, n + 0.49, "C")

    return None


def overlap_len(a: tuple[float, float], b: tuple[float, float]) -> float:
    lo = max(a[0], b[0])
    hi = min(a[1], b[1])
    return max(0.0, hi - lo)


REQUEST_TIMEOUT_SECONDS = 5


def fetch_event(slug: str) -> dict[str, Any] | None:
    # Gamma API behavior can vary by filters; try a few safe queries.
    attempts = [
        {"limit": 50, "slug": slug, "closed": "false", "active": "true"},
        {"limit": 5, "slug": slug},
        {"limit": 50, "closed": "false", "active": "true"},
    ]
    last_err: Exception | None = None
    for p in attempts:
        try:
            r = requests.get(GAMMA_EVENTS, params=p, timeout=REQUEST_TIMEOUT_SECONDS)
            r.raise_for_status()
            arr = r.json()
            if not arr:
                continue
            for ev in arr:
                if str(ev.get("slug", "")) == slug:
                    return ev
        except Exception as exc:
            last_err = exc
    # Timeout / API failure: silently skip polymarket section upstream.
    return None


def pretty_label_from_slug(slug: str) -> str:
    s = slug.lower()
    m = re.search(r"-(\d+)-(\d+)f$", s)
    if m:
        return f"{m.group(1)}–{m.group(2)}°F"
    m = re.search(r"-(\d+)-(\d+)c$", s)
    if m:
        return f"{m.group(1)}–{m.group(2)}°C"
    m = re.search(r"-(\d+)forbelow$", s)
    if m:
        return f"{m.group(1)}°F or below"
    m = re.search(r"-(\d+)forhigher$", s)
    if m:
        return f"{m.group(1)}°F or higher"
    m = re.search(r"-(\d+)corbelow$", s)
    if m:
        return f"{m.group(1)}°C or below"
    m = re.search(r"-(\d+)corhigher$", s)
    if m:
        return f"{m.group(1)}°C or higher"
    m = re.search(r"-(\d+)f$", s)
    if m:
        return f"{m.group(1)}°F"
    m = re.search(r"-(\d+)c$", s)
    if m:
        return f"{m.group(1)}°C"
    return slug


def sort_key_temp(iv: Interval, out_unit: str) -> float:
    lo = iv.lo
    if out_unit == iv.unit:
        return lo
    if out_unit == "C" and iv.unit == "F":
        return f_to_c(lo)
    if out_unit == "F" and iv.unit == "C":
        return c_to_f(lo)
    return lo


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--event-slug", required=True)
    ap.add_argument("--min", type=float, required=True)
    ap.add_argument("--max", type=float, required=True)
    ap.add_argument("--unit", choices=["C", "F"], required=True)
    ap.add_argument("--top", type=int, default=4)
    args = ap.parse_args()

    event = fetch_event(args.event_slug)
    if not event:
        return

    pred_lo, pred_hi = args.min, args.max

    rows: list[tuple[float, dict[str, Any], Interval]] = []
    for m in event.get("markets", []):
        iv = parse_slug_interval(str(m.get("slug", "")))
        if not iv:
            continue
        lo, hi = iv.lo, iv.hi
        if args.unit != iv.unit:
            if args.unit == "C" and iv.unit == "F":
                lo = f_to_c(lo) if lo != -math.inf else -math.inf
                hi = f_to_c(hi) if hi != math.inf else math.inf
            elif args.unit == "F" and iv.unit == "C":
                lo = c_to_f(lo) if lo != -math.inf else -math.inf
                hi = c_to_f(hi) if hi != math.inf else math.inf
        ov = overlap_len((pred_lo, pred_hi), (lo, hi))
        if ov <= 0:
            continue
        rows.append((ov, m, iv))

    # Keep only overlapping markets, then order by temperature ascending for display.
    rows.sort(key=lambda x: sort_key_temp(x[2], args.unit))

    print(f"Event: {event.get('title')} ({event.get('slug')})")
    print(f"Pred: {pred_lo:.2f}-{pred_hi:.2f}{args.unit}")
    shown = 0
    for ov, m, iv in rows:
        if shown >= args.top:
            break
        bid = m.get("bestBid")
        ask = m.get("bestAsk")
        label = pretty_label_from_slug(str(m.get("slug", "")))
        print(f"- {label} | Bid {bid} | Ask {ask} | overlap={ov:.2f}")
        shown += 1


if __name__ == "__main__":
    main()
