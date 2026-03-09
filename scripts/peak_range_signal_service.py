#!/usr/bin/env python3
"""Signal scoring helpers for peak-range analysis."""

from __future__ import annotations

from typing import Any

from layer_signal_policy import h700_effective_dry_factor, h700_is_moist_constraint
from realtime_pipeline import classify_window_phase


def _contains_any_text(text: str, keys: list[str]) -> bool:
    s = str(text or "")
    return any(k in s for k in keys)


def _f(v: Any) -> float | None:
    try:
        return float(v)
    except Exception:
        return None


def render_sounding_factor_pack(
    calc_window: dict[str, Any],
    metar_diag: dict[str, Any],
    snd_thermo: dict[str, Any],
    h700_summary: str,
    h925_summary: str,
    cloud_code_now: str,
) -> dict[str, Any]:
    low_cloud = _f(calc_window.get("low_cloud_pct"))
    w850 = _f(calc_window.get("w850_kmh"))
    wind_chg = _f(metar_diag.get("wind_dir_change_deg"))
    t_now = _f(metar_diag.get("latest_temp"))
    wx = str(metar_diag.get("latest_wx") or "").upper()
    sounding_source = str(snd_thermo.get("profile_source") or "")
    sounding_conf = str(snd_thermo.get("sounding_confidence") or "")
    sounding_cap = _f(snd_thermo.get("low_level_cap_score"))
    sounding_mix = _f(snd_thermo.get("mixing_support_score"))
    sounding_dry = _f(snd_thermo.get("midlevel_dry_score"))
    sounding_moist = _f(snd_thermo.get("midlevel_moist_score"))
    sounding_wind_mix = _f(snd_thermo.get("wind_profile_mix_score"))

    up_adj = 0.0
    down_adj = 0.0
    profile_score = 0.0
    tags: list[str] = []

    inv = 0.0
    if low_cloud is not None and low_cloud >= 70:
        inv += 0.35 if sounding_source == "obs" else 0.6
    if w850 is not None and w850 <= 15:
        inv += 0.18 if sounding_source == "obs" else 0.35
    if "耦合偏弱" in h925_summary:
        inv += 0.20 if sounding_source == "obs" else 0.35
    if sounding_cap is not None:
        inv = max(inv, 0.95 * sounding_cap)
    if inv >= 0.9:
        down_adj += 0.55
        profile_score += 0.35
        tags.append("探空稳定层/封盖约束偏强" if sounding_source == "obs" else "逆温/稳定约束偏强")
    elif inv >= 0.45:
        down_adj += 0.25
        profile_score += 0.25
        tags.append("探空显示低层稳定约束" if sounding_source == "obs" else "低层稳定约束")

    capev = snd_thermo.get("sbcape_jkg") or snd_thermo.get("mlcape_jkg") or snd_thermo.get("mucape_jkg")
    cinv = snd_thermo.get("sbcin_jkg") if snd_thermo.get("sbcin_jkg") is not None else snd_thermo.get("mlcin_jkg")
    if isinstance(capev, (int, float)):
        profile_score += 0.2
        if float(capev) >= 500 and (not isinstance(cinv, (int, float)) or float(cinv) > -75):
            down_adj += 0.2
            tags.append("对流可触发（云发展风险）")
        elif isinstance(cinv, (int, float)) and float(cinv) <= -125:
            up_adj += 0.15
            tags.append("抑制偏强（对流受限）")

    if any(k in wx for k in ["RA", "SN", "PL", "FZ", "DZ"]):
        if t_now is not None and -1.5 <= t_now <= 2.0:
            down_adj += 0.25
            profile_score += 0.2
            tags.append("近冰点相变/潜热冷却风险")

    dry_factor = h700_effective_dry_factor(
        h700_summary,
        low_cloud_pct=low_cloud,
        cloud_code_now=cloud_code_now,
    )
    if sounding_dry is not None:
        dry_factor = max(dry_factor * (0.65 if sounding_source == "obs" else 1.0), 0.95 * sounding_dry)
    if dry_factor > 0:
        profile_score += 0.18 * dry_factor
        if cloud_code_now in {"CLR", "CAVOK", "SKC", "FEW", "SCT"}:
            up_adj += 0.28 * dry_factor
        else:
            up_adj += 0.10 * dry_factor
        if dry_factor >= 0.75:
            tags.append("探空中层偏干+低层开窗（增温效率提升）" if sounding_source == "obs" else "中层偏干+低层开窗（增温效率提升）")
        elif dry_factor >= 0.30:
            tags.append("探空中层偏干背景（需配合低层开窗）" if sounding_source == "obs" else "中层偏干背景（需配合低层开窗）")
    moist_constraint = (sounding_moist is not None and sounding_moist >= 0.45) or h700_is_moist_constraint(h700_summary)
    if moist_constraint and dry_factor <= 0:
        profile_score += 0.35
        down_adj += 0.25 + (0.08 if sounding_moist is not None and sounding_moist >= 0.75 else 0.0)
        tags.append("探空中层湿层约束" if sounding_source == "obs" else "中层湿层约束")

    mix_signal = max(sounding_mix or 0.0, sounding_wind_mix or 0.0)
    if mix_signal >= 0.7:
        up_adj += 0.26
        profile_score += 0.24
        tags.append("探空显示混合下传条件较好")
    elif w850 is not None:
        if w850 >= 25 and (low_cloud is None or low_cloud <= 55):
            up_adj += 0.2
            profile_score += 0.2
            tags.append("混合条件较好")
        elif w850 <= 12 and low_cloud is not None and low_cloud >= 65:
            down_adj += 0.18
            profile_score += 0.15
            tags.append("混合偏弱")
    if wind_chg is not None and wind_chg >= 45:
        up_adj += 0.08
        down_adj += 0.08
        profile_score += 0.1
        tags.append("风切节奏扰动")
    if sounding_source == "obs":
        if sounding_conf == "H":
            profile_score += 0.20
        elif sounding_conf == "M":
            profile_score += 0.12

    return {
        "up_adj": up_adj,
        "down_adj": down_adj,
        "profile_score": profile_score,
        "tags": tags,
    }


def render_signal_scores(
    primary_window: dict[str, Any],
    metar_diag: dict[str, Any],
    line850: str,
    extra: str,
    precip_state: str,
    precip_trend: str,
    calc_window: dict[str, Any],
    snd_thermo: dict[str, Any],
    h700_summary: str,
    h925_summary: str,
    cloud_code_now: str,
) -> tuple[float, float, str]:
    up = 0.0
    down = 0.0

    if "暖平流" in line850:
        up += 1.0
    if "冷平流" in line850:
        down += 1.0

    if _contains_any_text(extra, ["封盖", "压制", "湿层", "低云"]):
        down += 1.0
    if h700_effective_dry_factor(
        h700_summary,
        low_cloud_pct=calc_window.get("low_cloud_pct"),
        cloud_code_now=cloud_code_now,
    ) <= 0 and _contains_any_text(extra, ["干层", "日照", "升温加速"]):
        up += 0.8

    try:
        bsrc = metar_diag.get("temp_bias_smooth_c") if metar_diag.get("temp_bias_smooth_c") is not None else metar_diag.get("temp_bias_c")
        b = float(bsrc) if bsrc is not None else 0.0
    except Exception:
        b = 0.0
    if b >= 0.8:
        up += 0.6
    elif b <= -0.8:
        down += 0.6

    ctrend = str(metar_diag.get("cloud_trend") or "")
    if ("增加" in ctrend) or ("回补" in ctrend):
        down += 0.5
    if ("开窗" in ctrend) or ("减弱" in ctrend):
        up += 0.5

    if precip_trend in {"new", "intensify"}:
        down += 0.75
    elif precip_trend in {"weaken", "end"}:
        up += 0.35
    elif precip_trend == "steady" and precip_state in {"moderate", "heavy", "convective"}:
        down += 0.45
    if precip_state == "convective":
        down += 0.25

    sf = render_sounding_factor_pack(
        calc_window=calc_window,
        metar_diag=metar_diag,
        snd_thermo=snd_thermo,
        h700_summary=h700_summary,
        h925_summary=h925_summary,
        cloud_code_now=cloud_code_now,
    )
    up += float(sf.get("up_adj") or 0.0)
    down += float(sf.get("down_adj") or 0.0)

    phase = str(classify_window_phase(primary_window, metar_diag).get("phase") or "unknown")
    return up, down, phase
