from __future__ import annotations

from typing import Any


def classify_large_scale_regime(synoptic: dict[str, Any], station_lat: float, w850_kmh: float | None = None) -> list[str]:
    syn = synoptic.get("scale_summary", {}).get("synoptic", {}) if synoptic else {}
    systems = syn.get("systems", []) if isinstance(syn, dict) else []
    names = [str(s.get("system_type", "")) for s in systems]

    has_ridge = any("ridge" in n for n in names)
    has_trough = any("trough" in n for n in names)
    has_shortwave = any("shortwave" in n for n in names)
    has_sfc_high = any("surface_high" in n for n in names)
    has_sfc_low = any("surface_low" in n for n in names)
    has_warm_adv = any("warm_advection" in n for n in names)
    has_cold_adv = any("cold_advection" in n for n in names)

    main = ""
    if has_ridge and has_trough:
        main = "槽脊并存（过渡型）"
    elif has_trough:
        main = "500hPa 槽场偏主导"
    elif has_ridge:
        main = "500hPa 脊场偏主导"

    secondary: list[str] = []
    if has_ridge and has_sfc_high and 15 <= abs(station_lat) <= 40:
        secondary.append("疑似副热带高压控制（中等置信）")
    if has_trough and abs(station_lat) >= 30:
        secondary.append("疑似西风带槽影响（中等置信）")
    if has_sfc_low and not has_sfc_high and abs(station_lat) >= 25:
        secondary.append("深低压/气旋背景（弱-中等置信）")
    if has_warm_adv and has_trough:
        secondary.append("槽前暖区信号（弱-中等置信）")
    if has_cold_adv and has_trough:
        secondary.append("槽后冷区信号（弱-中等置信）")
    if w850_kmh is not None and w850_kmh >= 60:
        secondary.append("急流/强风带主导（低-中等置信）")
    if has_ridge and has_sfc_high and has_sfc_low and has_shortwave:
        secondary.append("阻塞高压候选（低置信）")

    out: list[str] = []
    if main:
        out.append(main)
    out.extend(secondary[:4])
    return out


def advection_reach_score(system: dict[str, Any], w850_kmh: float | None = None) -> tuple[float, str]:
    geo = system.get("geo_context", {}) if isinstance(system, dict) else {}
    band = str(geo.get("distance_band", ""))
    dkm = geo.get("distance_km")

    if "0-300" in band:
        score = 0.70
    elif "300-800" in band:
        score = 0.45
    elif "800" in band:
        score = 0.20
    else:
        score = 0.30

    try:
        d = float(dkm)
        if d <= 200:
            score += 0.10
        elif d >= 1000:
            score -= 0.05
    except Exception:
        pass

    if w850_kmh is not None:
        if w850_kmh >= 50:
            score += 0.20
        elif w850_kmh >= 30:
            score += 0.10

    score = max(0.0, min(1.0, score))
    if score >= 0.70:
        level = "高"
    elif score >= 0.45:
        level = "中"
    else:
        level = "低"
    return score, level


def advection_dominance_line(warm_scores: list[float], cold_scores: list[float]) -> str | None:
    if not warm_scores and not cold_scores:
        return None
    wmax = max(warm_scores) if warm_scores else 0.0
    cmax = max(cold_scores) if cold_scores else 0.0
    if wmax - cmax >= 0.15:
        return f"暖平流更可能有效触站（{wmax:.2f} vs {cmax:.2f}）"
    if cmax - wmax >= 0.15:
        return f"冷平流更可能有效触站（{cmax:.2f} vs {wmax:.2f}）"
    return f"冷暖平流对冲，短时以实况斜率判主导（暖{wmax:.2f}/冷{cmax:.2f}）"
