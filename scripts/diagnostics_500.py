from __future__ import annotations

from typing import Any


def _phase_hint_from_sector(system: dict[str, Any]) -> str | None:
    geo = system.get("geo_context", {}) if isinstance(system, dict) else {}
    ns = str(geo.get("sector_ns", "")).lower()
    ew = str(geo.get("sector_ew", "")).lower()
    stype = str(system.get("system_type", "")).lower()
    if "trough" in stype:
        if "west" in ew:
            return "槽前"
        if "east" in ew:
            return "槽后"
    if "ridge" in stype:
        if "west" in ew:
            return "脊前"
        if "east" in ew:
            return "脊后"
    if "south" in ns or "north" in ns:
        return "南北向过渡"
    return None


def diagnose_500hpa(synoptic: dict[str, Any]) -> dict[str, Any]:
    syn = synoptic.get("scale_summary", {}).get("synoptic", {}) if synoptic else {}
    systems = syn.get("systems", []) if isinstance(syn, dict) else []

    s500 = [s for s in systems if str(s.get("level", "")) == "500"]
    ridge = [s for s in s500 if "ridge" in str(s.get("system_type", "")).lower()]
    trough = [s for s in s500 if "trough" in str(s.get("system_type", "")).lower()]
    shortwave = [s for s in s500 if "shortwave" in str(s.get("system_type", "")).lower()]
    fallback_weak = [s for s in s500 if str(s.get("detection_mode", "")) == "fallback_weak"]

    phase = "中性"
    if ridge and trough:
        phase = "槽脊并存（过渡相位）"
    elif trough:
        phase = "槽相主导"
    elif ridge:
        phase = "脊相主导"

    trends = [str(s.get("trend") or "") for s in s500]
    deepening = any(t == "deepening" for t in trends)
    strengthening = any(t == "strengthening" for t in trends)
    continuity_flag = "不明确"
    if deepening and trough:
        continuity_flag = "槽加强"
    elif strengthening and ridge:
        continuity_flag = "脊加强"
    elif any(t in {"filling", "weakening"} for t in trends):
        continuity_flag = "系统减弱"

    # PVA/NVA proxy: trough+shortwave -> ascent tendency; ridge-only -> subsidence tendency.
    pva_proxy = "中性"
    if trough and shortwave:
        pva_proxy = "PVA代理偏强（上升背景）"
    elif trough:
        pva_proxy = "PVA代理偏弱（弱上升背景）"
        if deepening:
            pva_proxy = "PVA代理中等（上升背景）"
    elif ridge and not trough:
        pva_proxy = "NVA代理偏强（下沉背景）"
        if not strengthening:
            pva_proxy = "NVA代理偏弱（弱下沉背景）"

    vertical_motion_bg = "中性"
    if "上升" in pva_proxy:
        vertical_motion_bg = "上升倾向"
    elif "下沉" in pva_proxy:
        vertical_motion_bg = "下沉倾向"

    phase_hints = [h for h in (_phase_hint_from_sector(s) for s in (trough[:1] + ridge[:1])) if h]

    confidence = "低"
    signal_count = (
        int(bool(ridge))
        + int(bool(trough))
        + int(bool(shortwave))
        + int(bool(phase_hints))
        + int(continuity_flag != "不明确")
    )
    if signal_count >= 2:
        confidence = "中"
    if signal_count >= 4:
        confidence = "中-高"

    # weak fallback-only detection should not overstate confidence
    if s500 and len(fallback_weak) == len(s500):
        if confidence == "中-高":
            confidence = "中"
        phase = phase if phase != "中性" else "弱信号背景"

    return {
        "phase": phase,
        "phase_hint": phase_hints[0] if phase_hints else None,
        "pva_proxy": pva_proxy,
        "vertical_motion_bg": vertical_motion_bg,
        "trend_12_24h": continuity_flag,
        "confidence": confidence,
    }
