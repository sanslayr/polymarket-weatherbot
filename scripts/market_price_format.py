from __future__ import annotations

from typing import Any


def _to_float(value: Any) -> float | None:
    try:
        if value in (None, ""):
            return None
        return float(value)
    except Exception:
        return None


def infer_market_tick_cents(*values: Any) -> float:
    for value in values:
        text = str(value or "").strip()
        if not text:
            continue
        if "." not in text:
            continue
        decimals = text.split(".")[-1]
        if len(decimals) >= 3:
            try:
                numeric = float(text)
            except Exception:
                continue
            if abs(round(numeric * 100) - numeric * 100) > 1e-6:
                return 0.1
    return 1.0


def format_price_cents(value: Any, *, tick_cents: float | None = None, none_text: str = "None") -> str:
    numeric = _to_float(value)
    if numeric is None:
        return none_text
    cents = numeric * 100.0
    tick = float(tick_cents) if tick_cents is not None else infer_market_tick_cents(value)
    if tick <= 0.1 + 1e-9:
        return f"{cents:.1f}¢"
    return f"{int(round(cents))}¢"
