#!/usr/bin/env python3
"""Polymarket ladder rendering for /look report."""

from __future__ import annotations

import json
import math
import os
import re
from typing import Any

from market_price_format import format_price_cents
from polymarket_client import (
    fetch_polymarket_event_markets as _fetch_polymarket_event_markets,
    poly_slug_from_url as _poly_slug_from_url,
)

LOOK_FORCE_LIVE_POLYMARKET = str(os.getenv("LOOK_FORCE_LIVE_POLYMARKET", "1") or "1").strip().lower() in {"1", "true", "yes", "on"}


def _poly_num(tok: str) -> int:
    t = str(tok).lower()
    if t.startswith("neg-"):
        # formats like neg-9
        return int(t[3:])
    if t.startswith("neg"):
        # formats like neg9
        return -int(t[3:])
    return int(t)


def _poly_parse_interval(slug: str) -> tuple[float, float, str] | None:
    s = slug.lower()

    # explicit ranged bins first, e.g. "-42-43f" / "-12-13c"
    m = re.search(r"-(neg-?\d+|\d{1,3})-(neg-?\d+|\d{1,3})c$", s)
    if m:
        n1 = _poly_num(m.group(1))
        n2 = _poly_num(m.group(2))
        if abs(n1 - n2) <= 8:
            lo, hi = (n1, n2) if n1 <= n2 else (n2, n1)
            return (lo - 0.5, hi + 0.5, "C")
    m = re.search(r"-(neg-?\d+|\d{1,3})-(neg-?\d+|\d{1,3})f$", s)
    if m:
        n1 = _poly_num(m.group(1))
        n2 = _poly_num(m.group(2))
        if abs(n1 - n2) <= 8:
            lo, hi = (n1, n2) if n1 <= n2 else (n2, n1)
            return (lo - 0.5, hi + 0.5, "F")

    m = re.search(r"-(neg-?\d+|\d+)c$", s)
    if m:
        n = _poly_num(m.group(1))
        return (n - 0.5, n + 0.5, "C")
    m = re.search(r"-(neg-?\d+|\d+)corbelow$", s)
    if m:
        n = _poly_num(m.group(1))
        return (-math.inf, n + 0.5, "C")
    m = re.search(r"-(neg-?\d+|\d+)corhigher$", s)
    if m:
        n = _poly_num(m.group(1))
        return (n - 0.5, math.inf, "C")
    m = re.search(r"-(neg-?\d+|\d+)f$", s)
    if m:
        n = _poly_num(m.group(1))
        return (n - 0.5, n + 0.5, "F")
    m = re.search(r"-(neg-?\d+|\d+)forbelow$", s)
    if m:
        n = _poly_num(m.group(1))
        return (-math.inf, n + 0.5, "F")
    m = re.search(r"-(neg-?\d+|\d+)forhigher$", s)
    if m:
        n = _poly_num(m.group(1))
        return (n - 0.5, math.inf, "F")
    return None


def _poly_label(slug: str) -> str:
    s = slug.lower()
    for pat, fmt in [
        (r"-(neg-?\d+|\d{1,3})-(neg-?\d+|\d{1,3})c$", lambda a, b: f"{_poly_num(a)}-{_poly_num(b)}°C"),
        (r"-(neg-?\d+|\d{1,3})-(neg-?\d+|\d{1,3})f$", lambda a, b: f"{_poly_num(a)}-{_poly_num(b)}°F"),
        (r"-(neg-?\d+|\d+)c$", lambda n: f"{_poly_num(n)}°C"),
        (r"-(neg-?\d+|\d+)corbelow$", lambda n: f"{_poly_num(n)}°C or below"),
        (r"-(neg-?\d+|\d+)corhigher$", lambda n: f"{_poly_num(n)}°C or higher"),
        (r"-(neg-?\d+|\d+)f$", lambda n: f"{_poly_num(n)}°F"),
        (r"-(neg-?\d+|\d+)forbelow$", lambda n: f"{_poly_num(n)}°F or below"),
        (r"-(neg-?\d+|\d+)forhigher$", lambda n: f"{_poly_num(n)}°F or higher"),
    ]:
        m = re.search(pat, s)
        if m:
            if len(m.groups()) == 2:
                return fmt(m.group(1), m.group(2))
            return fmt(m.group(1))
    return slug


def _build_polymarket_section(
    polymarket_event_url: str,
    primary_window: dict[str, Any],
    weather_anchor: dict[str, Any] | None = None,
    range_hint: dict[str, float] | None = None,
    allow_best_label: bool = True,
    allow_alpha_label: bool = True,
    label_policy: dict[str, Any] | None = None,
    prefetched_event: tuple[bool, list[dict[str, Any]]] | None = None,
) -> str:
    slug = _poly_slug_from_url(polymarket_event_url)
    if not slug:
        return "Polymarket：未找到对应市场。"
    if prefetched_event is None:
        event_found, markets = _fetch_polymarket_event_markets(slug, force_refresh=LOOK_FORCE_LIVE_POLYMARKET)
    else:
        event_found, markets = prefetched_event

    if not event_found:
        return "Polymarket：未找到对应市场。"

    def _num(v: Any) -> float:
        try:
            return float(v)
        except Exception:
            return 0.0

    def _parse_poly_list(v: Any) -> list[Any]:
        if isinstance(v, list):
            return v
        if isinstance(v, str):
            try:
                x = json.loads(v)
                if isinstance(x, list):
                    return x
            except Exception:
                return []
        return []

    parsed: list[tuple[float, str, Any, Any, float, float]] = []
    resolved_rows: list[tuple[float, tuple[float, str, Any, Any, float, float]]] = []
    for m in markets:
        iv = _poly_parse_interval(str(m.get("slug", "")))
        if not iv:
            continue
        lo, hi, unit = iv
        if unit == "F":
            lo = (lo - 32) * 5 / 9 if lo != -math.inf else -math.inf
            hi = (hi - 32) * 5 / 9 if hi != math.inf else math.inf
        if math.isinf(lo) and not math.isinf(hi):
            center = hi - 0.5
        elif math.isinf(hi) and not math.isinf(lo):
            center = lo + 0.5
        elif math.isinf(lo) and math.isinf(hi):
            center = 999.0
        else:
            center = (lo + hi) / 2
        row = (center, _poly_label(str(m.get("slug", ""))), m.get("bestBid"), m.get("bestAsk"), lo, hi)
        parsed.append(row)

        closed_flag = bool(m.get("closed")) or (m.get("acceptingOrders") is False)
        outcomes = _parse_poly_list(m.get("outcomes"))
        prices = _parse_poly_list(m.get("outcomePrices"))
        yes_score = None
        try:
            yes_idx = next((i for i, o in enumerate(outcomes) if str(o).strip().lower() == "yes"), None)
            if yes_idx is not None and yes_idx < len(prices):
                yes_score = _num(prices[yes_idx])
        except Exception:
            yes_score = None

        # Only mark as resolved when explicit YES settlement is present.
        if closed_flag and yes_score is not None and yes_score >= 0.98:
            resolved_rows.append((yes_score, row))

    parsed.sort(key=lambda x: x[0])
    if not parsed:
        return "Polymarket：当前无可用盘口。"

    if resolved_rows:
        winner = sorted(resolved_rows, key=lambda x: x[0], reverse=True)[0][1]
        winner_label = str(winner[1])
        return "\n".join([
            "📈 **Polymarket 盘口与博弈**",
            f"🔒 已结算：{winner_label}",
        ])

    def _px(v: Any) -> float:
        try:
            return float(v)
        except Exception:
            return 0.0

    t_now = None if not weather_anchor else weather_anchor.get("latest_temp_c")
    try:
        t_now = float(t_now) if t_now is not None else None
    except Exception:
        t_now = None
    peak = float(primary_window.get("peak_temp_c") or 0.0)
    obs_max = None
    if weather_anchor:
        try:
            obs_max = float(weather_anchor.get("observed_max_temp_c")) if weather_anchor.get("observed_max_temp_c") is not None else None
        except Exception:
            obs_max = None

    # Market feasibility filtering uses observed realized value (not quantization interval).
    market_floor = obs_max

    filtered = []
    for center, label, bid, ask, lo, hi in parsed:
        # User policy: ignore bins that are fully below observed daily max.
        # For ranged buckets (e.g. 82-83F => 81.5~83.5F), keep if upper edge can still contain realized max.
        if market_floor is not None:
            try:
                hi_f = float(hi)
            except Exception:
                hi_f = None
            if hi_f is not None and math.isfinite(hi_f) and hi_f < float(market_floor):
                continue
        filtered.append((center, label, bid, ask, lo, hi))

    if not filtered:
        if market_floor is not None:
            tmp = []
            for c, l, b, a, lo_i, hi_i in parsed:
                try:
                    hi_f = float(hi_i)
                except Exception:
                    hi_f = None
                if hi_f is not None and math.isfinite(hi_f) and hi_f < float(market_floor):
                    continue
                tmp.append((c, l, b, a, lo_i, hi_i))
            filtered = tmp
        else:
            filtered = [(c, l, b, a, lo, hi) for c, l, b, a, lo, hi in parsed]
    if not filtered:
        return "Polymarket：当前无可用盘口。"

    lp = label_policy if isinstance(label_policy, dict) else {}

    def _lp_float(key: str, default: float) -> float:
        try:
            return float(lp.get(key)) if lp.get(key) is not None else float(default)
        except Exception:
            return float(default)

    best_lead_min = _lp_float("best_lead_min", 0.045)
    best_weather_min = _lp_float("best_weather_min", 0.28)
    alpha_cheap_ask_max = _lp_float("alpha_cheap_ask_max", 0.15)
    alpha_cheap_spread_max = _lp_float("alpha_cheap_spread_max", 0.10)
    alpha_cheap_weather_min = _lp_float("alpha_cheap_weather_min", 0.12)
    alpha_cheap_score_min = _lp_float("alpha_cheap_score_min", 0.22)
    alpha_mid_ask_max = _lp_float("alpha_mid_ask_max", 0.18)
    alpha_mid_spread_max = _lp_float("alpha_mid_spread_max", 0.06)
    alpha_mid_weather_min = _lp_float("alpha_mid_weather_min", 0.45)
    alpha_mid_score_min = _lp_float("alpha_mid_score_min", 0.30)

    likely_lo = peak - 0.8
    likely_hi = peak + 0.8

    hint = range_hint or {}
    try:
        hint_lo = float(hint.get("display_lo")) if hint.get("display_lo") is not None else None
    except Exception:
        hint_lo = None
    try:
        hint_hi = float(hint.get("display_hi")) if hint.get("display_hi") is not None else None
    except Exception:
        hint_hi = None

    target_lo = hint_lo if hint_lo is not None else likely_lo
    target_hi = hint_hi if hint_hi is not None else likely_hi

    try:
        core_lo = float(hint.get("core_lo")) if hint.get("core_lo") is not None else None
    except Exception:
        core_lo = None
    try:
        core_hi = float(hint.get("core_hi")) if hint.get("core_hi") is not None else None
    except Exception:
        core_hi = None
    if core_lo is None:
        core_lo = likely_lo
    if core_hi is None:
        core_hi = likely_hi

    def _ov_len(a0: float, a1: float, b0: float, b1: float) -> float:
        lo = max(a0, b0)
        hi = min(a1, b1)
        return max(0.0, hi - lo)

    def _weather_score(row: tuple[float, str, Any, Any, float, float]) -> float:
        c, _l, _b, _a, lo, hi = row
        core_w = max(0.4, float(core_hi - core_lo))
        disp_w = max(core_w, float(target_hi - target_lo), 0.8)
        ov_core = _ov_len(lo, hi, core_lo, core_hi) / core_w
        ov_disp = _ov_len(lo, hi, target_lo, target_hi) / disp_w
        mid = 0.5 * (core_lo + core_hi)
        c_term = max(0.0, 1.0 - abs(c - mid) / 2.0)
        return 0.55 * ov_core + 0.30 * ov_disp + 0.15 * c_term

    def _market_strength(row: tuple[float, str, Any, Any, float, float]) -> float:
        _c, _l, b, a, _lo, _hi = row
        bid = _px(b)
        ask = _px(a)
        if bid > 0 and ask > 0:
            mid = 0.5 * (bid + ask)
            spread = max(0.0, ask - bid)
        else:
            mid = max(bid, ask)
            spread = 0.25
        liquid = 0.08 if max(bid, ask) >= 0.02 else -0.05
        return mid - 0.35 * spread + liquid

    def _alpha_score(row: tuple[float, str, Any, Any, float, float]) -> float:
        _c, _l, b, a, _lo, hi = row
        bid = _px(b)
        ask = _px(a)
        spread = max(0.0, ask - bid)
        w = _weather_score(row)
        m = _market_strength(row)
        mispricing = max(0.0, w - m)

        cheap_bonus = 0.0
        if ask > 0 and ask <= 0.15:
            cheap_bonus = 0.14 + 0.10 * (0.15 - ask) / 0.15
        elif ask > 0.15 and ask <= 0.20 and spread <= 0.06 and w >= 0.45:
            cheap_bonus = 0.05 + 0.05 * (0.20 - ask) / 0.05

        tradable = 0.05 if max(bid, ask) >= 0.02 else -0.04
        stale_penalty = 0.0
        if t_now is not None and hi <= t_now + 0.3:
            stale_penalty = 0.35

        return 0.85 * mispricing + 0.25 * w + cheap_bonus + tradable - 0.25 * spread - stale_penalty

    ranked = sorted(filtered, key=lambda r: (0.75 * _weather_score(r) + 0.25 * _market_strength(r)), reverse=True)

    def _overlap_or_near(row: tuple[float, str, Any, Any, float, float]) -> bool:
        _c, _l, _b, _a, lo, hi = row
        if hi < target_lo - 0.5:
            return False
        if lo > target_hi + 0.6:
            return False
        return True

    near_pool = [r for r in filtered if _overlap_or_near(r)]
    mismatch = False
    if near_pool:
        seed = sorted(near_pool, key=_alpha_score, reverse=True)[:5]
    else:
        mismatch = True
        # 若主带无直接匹配，向 below/above 边缘档位寻找最近可交易区间
        below = [r for r in filtered if r[5] <= target_lo]
        above = [r for r in filtered if r[4] >= target_hi]
        seed = []
        if below:
            seed.append(sorted(below, key=lambda x: x[5], reverse=True)[0])
        if above:
            seed.append(sorted(above, key=lambda x: x[4])[0])
        # 再补一个流动性较好的档位，避免只剩单边
        for r in ranked:
            if all(r[1] != s[1] for s in seed):
                seed.append(r)
                break

    # Build a continuous temperature range around the most relevant bins.
    finite = sorted([r for r in filtered if not (math.isinf(r[4]) or math.isinf(r[5]))], key=lambda x: x[0])
    if seed and finite:
        min_c = min(r[0] for r in seed)
        max_c = max(r[0] for r in seed)
        continuous = [r for r in finite if (min_c - 0.01) <= r[0] <= (max_c + 0.01)]
        if not continuous:
            continuous = seed
    else:
        continuous = seed

    # keep continuity by temperature order; avoid dropping middle bins (e.g. 13/14/15) due hard cap
    focus = sorted(continuous, key=lambda x: x[0])

    # If too many bins, keep a compact contiguous slice around peak center instead of naive head truncation.
    if len(focus) > 8:
        target_c = peak
        k = min(8, len(focus))
        best_i = 0
        best_d = 1e9
        for i in range(0, len(focus) - k + 1):
            mid = focus[i + k // 2][0]
            d = abs(mid - target_c)
            if d < best_d:
                best_d = d
                best_i = i
        focus = focus[best_i: best_i + k]

    # Backfill interior finite bins to keep interval continuity (debug fix for missing middle labels).
    if focus:
        finite_all = sorted([r for r in filtered if not (math.isinf(r[4]) or math.isinf(r[5]))], key=lambda x: x[0])
        if finite_all:
            cmin = min(r[0] for r in focus if not (math.isinf(r[4]) or math.isinf(r[5]))) if any(not (math.isinf(r[4]) or math.isinf(r[5])) for r in focus) else None
            cmax = max(r[0] for r in focus if not (math.isinf(r[4]) or math.isinf(r[5]))) if any(not (math.isinf(r[4]) or math.isinf(r[5])) for r in focus) else None
            if cmin is not None and cmax is not None:
                filler = [r for r in finite_all if (cmin - 0.01) <= r[0] <= (cmax + 0.01)]
                merged = []
                seen = set()
                for r in (focus + filler):
                    if r[1] in seen:
                        continue
                    seen.add(r[1])
                    merged.append(r)
                focus = sorted(merged, key=lambda x: x[0])

    # include edge bins only when market-vs-weather mismatch fallback is active.
    if mismatch and focus:
        low_edge = focus[0][4]
        edge_bins = [r for r in filtered if math.isinf(r[4]) and not math.isinf(r[5]) and (r[5] >= low_edge - 1.1)]
        if edge_bins:
            edge = sorted(edge_bins, key=lambda x: x[5])[-1]
            if all(edge[1] != r[1] for r in focus):
                focus = [edge] + focus

    if mismatch and focus:
        high_edge = focus[-1][5]
        upper_bins = [r for r in filtered if not math.isinf(r[4]) and math.isinf(r[5]) and (r[4] <= high_edge + 1.1)]
        if upper_bins:
            edge = sorted(upper_bins, key=lambda x: x[4])[0]
            if all(edge[1] != r[1] for r in focus):
                focus = focus + [edge]

    # Bridge finite gap to upper edge (e.g. ensure 15°C appears before 16°C or higher when available).
    if focus:
        finite_all = sorted([r for r in filtered if not (math.isinf(r[4]) or math.isinf(r[5]))], key=lambda x: x[0])
        has_upper_edge = any((not math.isinf(r[4]) and math.isinf(r[5])) for r in focus)
        finite_focus = [r for r in focus if not (math.isinf(r[4]) or math.isinf(r[5]))]
        if has_upper_edge and finite_focus and finite_all:
            max_fin = max(r[0] for r in finite_focus)
            # edge lower bound (approx center of last finite bin before edge)
            upper_los = [r[4] for r in focus if (not math.isinf(r[4]) and math.isinf(r[5]))]
            edge_lo = min(upper_los) if upper_los else None
            if edge_lo is not None:
                bridge = [r for r in finite_all if (max_fin + 0.99) <= r[0] <= (edge_lo + 0.01)]
                if bridge:
                    seen = {r[1] for r in focus}
                    for r in bridge:
                        if r[1] not in seen:
                            focus.append(r)
                            seen.add(r[1])
                    focus = sorted(focus, key=lambda x: x[0])

    # Ensure one upper-tail bin is visible when merged upper bound approaches market upper buckets.
    # Handles both finite next-step bins and "X°C or higher" style edge bins.
    try:
        finite_all = sorted([r for r in filtered if not (math.isinf(r[4]) or math.isinf(r[5]))], key=lambda x: x[0])
        edge_ref_hi = max(likely_hi, target_hi)

        # 1) Prefer finite next-step bucket if close enough.
        target_center = round(edge_ref_hi)
        cand = [r for r in finite_all if abs(r[0] - target_center) <= 0.26 and r[0] >= (core_hi - 0.2)]
        if cand:
            up = sorted(cand, key=lambda x: abs(x[0] - edge_ref_hi))[0]
            if all(up[1] != r[1] for r in focus):
                focus.append(up)

        # 2) Add nearest upper-edge bucket when display upper range reaches it.
        upper_edges = sorted(
            [r for r in filtered if (not math.isinf(r[4]) and math.isinf(r[5]))],
            key=lambda x: x[4],
        )
        if upper_edges:
            # include first edge whose lower bound is not far above merged upper range
            edge_cands = [r for r in upper_edges if r[4] <= (target_hi + 0.4)]
            if edge_cands:
                up = edge_cands[0]
                if all(up[1] != r[1] for r in focus):
                    focus.append(up)

        focus = sorted(focus, key=lambda x: x[0])
    except Exception:
        pass

    # “最有可能”以天气预测一致性为主，市场强度仅作并列裁决。
    best_label = None
    if allow_best_label and filtered:
        core_bins = [r for r in filtered if (r[5] >= core_lo and r[4] <= core_hi)]
        pick_pool = core_bins if core_bins else filtered

        def _likely_score(row: tuple[float, str, Any, Any, float, float]) -> float:
            return 0.82 * _weather_score(row) + 0.18 * _market_strength(row)

        pick_sorted = sorted(pick_pool, key=_likely_score, reverse=True)
        s1 = _likely_score(pick_sorted[0])
        s2 = _likely_score(pick_sorted[1]) if len(pick_sorted) > 1 else -999.0
        # require clear lead to avoid over-tagging in tight distributions
        if (s1 - s2) >= best_lead_min and _weather_score(pick_sorted[0]) >= best_weather_min:
            best_label = pick_sorted[0][1]

    # Non-settled markets should show at least 2-3 bins (main + adjacent), avoid single-bin squeeze.
    if len(focus) == 1:
        only = focus[0]
        bid_only = _px(only[2])
        ask_only = _px(only[3])
        settled = (bid_only >= 0.98 or ask_only >= 0.98)
        if not settled:
            finite_all = sorted([r for r in filtered if not (math.isinf(r[4]) or math.isinf(r[5]))], key=lambda x: x[0])
            center = only[0]
            left = [r for r in finite_all if r[0] < center]
            right = [r for r in finite_all if r[0] > center]
            expanded = []
            if left:
                expanded.append(left[-1])
            expanded.append(only)
            if right:
                expanded.append(right[0])
            if len(expanded) < 2:
                edges = [r for r in filtered if (math.isinf(r[4]) or math.isinf(r[5]))]
                if edges:
                    expanded.append(edges[0])
            focus = sorted({r[1]: r for r in expanded}.values(), key=lambda x: x[0])

    # Final clipping by merged weather range to avoid displaying bins that are clearly too cold.
    if focus and hint:
        min_keep = target_lo - 0.4
        max_keep = target_hi + 0.9
        clipped = [r for r in focus if (r[5] >= min_keep and r[4] <= max_keep)]
        if clipped:
            focus = sorted(clipped, key=lambda x: x[0])
            if len(focus) < 3:
                finite_all = sorted([r for r in filtered if not (math.isinf(r[4]) or math.isinf(r[5]))], key=lambda x: x[0])
                if finite_all:
                    mid = 0.5 * (target_lo + target_hi)
                    nearest = sorted(finite_all, key=lambda x: abs(x[0] - mid))[:3]
                    merged = {r[1]: r for r in (focus + nearest)}
                    focus = sorted(merged.values(), key=lambda x: x[0])

    score_map = {(lbl, str(bid), str(ask)): _alpha_score((c, lbl, bid, ask, lo, hi)) for c, lbl, bid, ask, lo, hi in filtered}

    lines = ["📈 **Polymarket 盘口与博弈**"]

    def _row_tag(row: tuple[float, str, Any, Any, float, float]) -> str:
        _c, label, bid, ask, _lo, _hi = row
        bid_v = _px(bid)
        ask_v = _px(ask)
        spread_v = max(0.0, ask_v - bid_v)
        if allow_best_label and best_label and label == best_label:
            return "👍最有可能"
        if not allow_alpha_label:
            return ""
        s = score_map.get((label, str(bid), str(ask)), 0.0)
        w = _weather_score(row)
        if ask_v > 0 and ask_v <= alpha_cheap_ask_max and spread_v <= alpha_cheap_spread_max and w >= alpha_cheap_weather_min and s >= alpha_cheap_score_min:
            return "😇潜在Alpha"
        if ask_v > alpha_cheap_ask_max and ask_v <= alpha_mid_ask_max and spread_v <= alpha_mid_spread_max and w >= alpha_mid_weather_min and s >= alpha_mid_score_min:
            return "😇潜在Alpha"
        return ""

    # Display ladder (integrated contract baseline):
    # start from weather range-continuous bins; then expectation step will union with market range.
    display_rows = []
    try:
        finite_all = sorted([r for r in filtered if not (math.isinf(r[4]) or math.isinf(r[5]))], key=lambda x: x[0])
        if finite_all:
            base = [r for r in finite_all if (r[5] >= target_lo - 0.01 and r[4] <= target_hi + 0.01)]
            if base:
                cmin = min(r[0] for r in base)
                cmax = max(r[0] for r in base)
                display_rows = [r for r in finite_all if cmin - 0.01 <= r[0] <= cmax + 0.01]
            else:
                mid = 0.5 * (target_lo + target_hi)
                display_rows = sorted(finite_all, key=lambda x: abs(x[0] - mid))[:3]
                display_rows = sorted(display_rows, key=lambda x: x[0])

        if not display_rows:
            display_rows = sorted(list(filtered), key=lambda x: x[0])[:3]

        # include overlapping edge bins around weather range if present
        lower_edges = sorted([r for r in filtered if (math.isinf(r[4]) and not math.isinf(r[5]))], key=lambda x: x[5])
        if lower_edges:
            lo_cands = [r for r in lower_edges if r[5] >= target_lo - 0.6]
            if lo_cands and all(str(lo_cands[-1][1]) != str(x[1]) for x in display_rows):
                display_rows = [lo_cands[-1]] + display_rows

        upper_edges = sorted([r for r in filtered if (not math.isinf(r[4]) and math.isinf(r[5]))], key=lambda x: x[4])
        if upper_edges:
            up_cands = [r for r in upper_edges if r[4] <= target_hi + 0.6]
            if up_cands and all(str(up_cands[0][1]) != str(x[1]) for x in display_rows):
                display_rows = display_rows + [up_cands[0]]

        # de-dup + stable sort
        dedup = {str(r[1]): r for r in display_rows}
        display_rows = sorted(dedup.values(), key=lambda x: x[0])
    except Exception:
        display_rows = sorted(list(filtered), key=lambda x: x[0])[:3]

    expectation_lines: list[str] = []
    range_notes: list[str] = []
    market_cov_lo_c = None
    market_cov_hi_c = None

    settled_single = False
    settled_center = None
    if len(display_rows) == 1:
        only = display_rows[0]
        bid_only = _px(only[2])
        ask_only = _px(only[3])
        settled_single = (bid_only >= 0.98 or ask_only >= 0.98)
        if settled_single:
            range_notes.append("• ✅ 已定局：当前仅剩单一高置信可交易区间。")
            try:
                settled_center = float(only[0])
            except Exception:
                settled_center = None
    if mismatch:
        range_notes.append("• 注：市场档位与气象主带存在错位，已按最近 below/above 边缘区间回退展示。")

    # Market-vs-forecast distribution cues (use full filtered market set, not only displayed bins).
    try:
        unit_votes = ["F" if "°F" in str(r[1]) else ("C" if "°C" in str(r[1]) else "") for r in filtered]
        use_unit = "F" if unit_votes.count("F") > unit_votes.count("C") else "C"

        def _to_unit(v_c: float) -> float:
            return (v_c * 9.0 / 5.0 + 32.0) if use_unit == "F" else v_c

        sym = "°F" if use_unit == "F" else "°C"

        if settled_single and settled_center is not None:
            s_u = _to_unit(settled_center)
            expectation_lines.append(f"↳ {s_u:.1f}{sym}｜{s_u:.1f}~{s_u:.1f}{sym}（近似定局）")
            market_cov_lo_c = float(settled_center)
            market_cov_hi_c = float(settled_center)
        pts_full: list[tuple[float, str, float, float, float, bool]] = []
        for c, lbl, b, a, lo_i, hi_i in filtered:
            bidv = _px(b)
            askv = _px(a)
            if bidv > 0 and askv > 0:
                p = 0.5 * (bidv + askv)
            else:
                p = max(bidv, askv)
            if p > 0:
                is_edge = bool(math.isinf(lo_i) or math.isinf(hi_i))
                pts_full.append((float(c), str(lbl), float(p), float(lo_i), float(hi_i), is_edge))

        if (not settled_single) and len(pts_full) >= 2:
            tot = sum(p for _c, _l, p, _lo, _hi, _e in pts_full)
            if tot > 0:
                wpts = sorted([(c, l, p / tot, lo, hi, e) for c, l, p, lo, hi, e in pts_full], key=lambda z: z[0])

                def _wq(q: float) -> float:
                    acc = 0.0
                    for c, _l, p, _lo, _hi, _e in wpts:
                        acc += p
                        if acc >= q:
                            return c
                    return wpts[-1][0]

                emp_mu = sum(c * p for c, _l, p, _lo, _hi, _e in wpts)
                edge_share = sum(p for _c, _l, p, _lo, _hi, e in wpts if e)
                # Dynamic coverage target (avoid hard-coded 85%):
                # tighter books -> broader confidence statement; edge-heavy books -> narrower core range.
                cov_target = max(0.80, min(0.90, 0.90 - 0.18 * edge_share))
                q_lo = (1.0 - cov_target) / 2.0
                q_hi = 1.0 - q_lo
                emp_qlo = _wq(q_lo)
                emp_qhi = _wq(q_hi)

                # Normal-fit expectation/range (helps when higher/lower edge bins carry non-trivial mass).
                fit_ok = False
                fit_mu = emp_mu
                fit_sigma = max(0.35, math.sqrt(max(0.0, sum(((c - emp_mu) ** 2) * p for c, _l, p, _lo, _hi, _e in wpts))))

                try:
                    sqrt2 = math.sqrt(2.0)

                    def _ncdf(x: float, mu: float, sig: float) -> float:
                        if sig <= 0:
                            return 0.0
                        z = (x - mu) / sig
                        return 0.5 * (1.0 + math.erf(z / sqrt2))

                    def _pmass(lo: float, hi: float, mu: float, sig: float) -> float:
                        a = 0.0 if lo == -math.inf else _ncdf(lo, mu, sig)
                        b = 1.0 if hi == math.inf else _ncdf(hi, mu, sig)
                        return max(0.0, b - a)

                    cmin = min(c for c, _l, _p, _lo, _hi, _e in wpts)
                    cmax = max(c for c, _l, _p, _lo, _hi, _e in wpts)
                    base_sig = max(0.5, fit_sigma)
                    mu_lo = cmin - max(2.0, 2.0 * base_sig)
                    mu_hi = cmax + max(2.0, 2.0 * base_sig)

                    best_loss = 1e18
                    best = None
                    for i in range(41):
                        mu = mu_lo + (mu_hi - mu_lo) * (i / 40.0)
                        for j in range(36):
                            sig = 0.35 + (4.5 - 0.35) * (j / 35.0)
                            masses = [_pmass(lo, hi, mu, sig) for _c, _l, _p, lo, hi, _e in wpts]
                            msum = sum(masses)
                            if msum <= 1e-9:
                                continue
                            preds = [m / msum for m in masses]
                            loss = 0.0
                            for (_c, _l, p, _lo, _hi, _e), pr in zip(wpts, preds):
                                loss += ((pr - p) ** 2) / max(0.01, p)
                            if loss < best_loss:
                                best_loss = loss
                                best = (mu, sig)
                    if best is not None:
                        fit_mu, fit_sigma = best
                        fit_ok = True
                except Exception:
                    fit_ok = False

                cov_pct = int(round(cov_target * 100.0))
                feasible_floor = min(c for c, _l, _p, _lo, _hi, _e in wpts)
                if obs_max is not None:
                    feasible_floor = max(feasible_floor, float(obs_max))

                if fit_ok:
                    # convert target central coverage -> z via binary search (erf-based CDF)
                    z_lo_b, z_hi_b = 0.5, 2.2
                    for _ in range(28):
                        z_mid = 0.5 * (z_lo_b + z_hi_b)
                        c_mid = math.erf(z_mid / math.sqrt(2.0))
                        if c_mid < cov_target:
                            z_lo_b = z_mid
                        else:
                            z_hi_b = z_mid
                    z_cov = 0.5 * (z_lo_b + z_hi_b)

                    mu_c = max(fit_mu, feasible_floor)
                    lo_c = max(mu_c - z_cov * fit_sigma, feasible_floor)
                    hi_c = max(mu_c + z_cov * fit_sigma, lo_c)
                    mu_u = _to_unit(mu_c)
                    lo_u = _to_unit(lo_c)
                    hi_u = _to_unit(hi_c)
                    expectation_lines.append(f"↳ {mu_u:.1f}{sym}｜{lo_u:.1f}~{hi_u:.1f}{sym}（{cov_pct}%范围）")
                    market_cov_lo_c = float(lo_c)
                    market_cov_hi_c = float(hi_c)
                else:
                    mu_c = max(emp_mu, feasible_floor)
                    lo_c = max(emp_qlo, feasible_floor)
                    hi_c = max(emp_qhi, lo_c)
                    mu_u = _to_unit(mu_c)
                    lo_u = _to_unit(lo_c)
                    hi_u = _to_unit(hi_c)
                    expectation_lines.append(f"↳ {mu_u:.1f}{sym}｜{lo_u:.1f}~{hi_u:.1f}{sym}（{cov_pct}%范围）")
                    market_cov_lo_c = float(lo_c)
                    market_cov_hi_c = float(hi_c)

                if edge_share > 0.55:
                    edge_bins = sorted([(str(lbl), float(p)) for _c, lbl, p, _lo, _hi, e in wpts if e], key=lambda x: x[1], reverse=True)
                    edge_bins = [x for x in edge_bins if x[1] >= 0.02]
                    if edge_bins:
                        edge_txt = " / ".join([f"{lbl}({p*100:.0f}%)" for lbl, p in edge_bins[:2]])
                        expectation_lines.append(f"• 注：边缘占比较高（约 {edge_share*100:.0f}%，主要在 {edge_txt}），期望仅供参考。")
                    else:
                        expectation_lines.append(f"• 注：边缘占比较高（约 {edge_share*100:.0f}%），期望仅供参考。")

                # explicit hot-side cue above forecast core upper bound
                fc_hi = float(core_hi)
                hot_tail = sum(p for c, _l, p, _lo, _hi, _e in wpts if c >= (fc_hi + 0.5))
                if hot_tail >= 0.18:
                    expectation_lines.append(f"• 提示：市场对更高温档位定价不低（≥{_to_unit(fc_hi + 0.5):.1f}{sym} 合计约 {hot_tail*100:.0f}%）。")
    except Exception:
        pass

    # Final ladder alignment (contract):
    # include the UNION of weather Tmax range and market expected range, and keep bins continuous.
    try:
        union_lo = float(target_lo)
        union_hi = float(target_hi)
        if market_cov_lo_c is not None and market_cov_hi_c is not None:
            union_lo = min(union_lo, float(market_cov_lo_c))
            union_hi = max(union_hi, float(market_cov_hi_c))

        finite_all = sorted([r for r in filtered if not (math.isinf(r[4]) or math.isinf(r[5]))], key=lambda x: x[0])
        if finite_all:
            base = [r for r in finite_all if (r[5] >= union_lo - 0.01 and r[4] <= union_hi + 0.01)]
            if base:
                cmin = min(r[0] for r in base)
                cmax = max(r[0] for r in base)
                merged = [r for r in finite_all if cmin - 0.01 <= r[0] <= cmax + 0.01]
            else:
                merged = sorted(finite_all, key=lambda x: abs(x[0] - 0.5 * (union_lo + union_hi)))[:3]
                merged = sorted(merged, key=lambda x: x[0])
            display_rows = merged

        # include overlapping edge bins when union range touches bounds
        lower_edges = sorted([r for r in filtered if (math.isinf(r[4]) and not math.isinf(r[5]))], key=lambda x: x[5])
        if lower_edges:
            lo_cands = [r for r in lower_edges if r[5] >= union_lo - 0.6]
            if lo_cands:
                lo_pick = lo_cands[-1]
                if all(str(lo_pick[1]) != str(x[1]) for x in display_rows):
                    display_rows = [lo_pick] + display_rows

        upper_edges = sorted([r for r in filtered if (not math.isinf(r[4]) and math.isinf(r[5]))], key=lambda x: x[4])
        if upper_edges:
            up_cands = [r for r in upper_edges if r[4] <= union_hi + 0.6]
            if up_cands:
                up_pick = up_cands[0]
                if all(str(up_pick[1]) != str(x[1]) for x in display_rows):
                    display_rows = display_rows + [up_pick]

        # de-dup + stable sort
        dedup = {str(r[1]): r for r in display_rows}
        display_rows = sorted(dedup.values(), key=lambda x: x[0])
    except Exception:
        pass

    # Re-ensure dominant bucket after final alignment override.
    try:
        if filtered:
            top_row = None
            top_p = -1.0
            for r in filtered:
                _c, _lbl, b, a, _lo, _hi = r
                bidv = _px(b)
                askv = _px(a)
                p = (0.5 * (bidv + askv)) if (bidv > 0 and askv > 0) else max(bidv, askv)
                if p > top_p:
                    top_p = p
                    top_row = r
            if top_row is not None and all(str(top_row[1]) != str(x[1]) for x in display_rows):
                display_rows = sorted(display_rows + [top_row], key=lambda x: x[0])
    except Exception:
        pass

    if expectation_lines:
        lines.append("**市场定价**")
        lines.extend(expectation_lines)

    lines.append("**博弈区间**")
    if range_notes:
        lines.extend(range_notes)

    for _c, label, bid, ask, _lo, _hi in display_rows:
        bid_txt = format_price_cents(bid)
        ask_txt = format_price_cents(ask)
        tag = _row_tag((_c, label, bid, ask, _lo, _hi))

        if tag:
            lines.append(f"• **{label}（{tag}）：Bid {bid_txt} | Ask {ask_txt}**")
        else:
            lines.append(f"• {label}：Bid {bid_txt} | Ask {ask_txt}")

    return "\n".join(lines)
