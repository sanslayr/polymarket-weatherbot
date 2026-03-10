#!/usr/bin/env python3
"""Boundary-layer regime and model-sounding proxy helpers for /look."""

from __future__ import annotations

from datetime import datetime
from typing import Any

from advection_review import thermal_advection_direction
from condition_state import build_live_condition_signals
from layer_signal_policy import h700_effective_dry_factor, h700_is_moist_constraint


BOUNDARY_LAYER_REGIME_SCHEMA_VERSION = "boundary-layer-regime.v1"


def _safe_float(value: Any) -> float | None:
    try:
        if value in (None, ""):
            return None
        return float(value)
    except Exception:
        return None


def _contains_any(text: Any, tokens: tuple[str, ...]) -> bool:
    raw = str(text or "")
    return any(token in raw for token in tokens)


def _clamp01(value: float) -> float:
    return max(0.0, min(1.0, float(value)))


def _hour_float(value: Any) -> float | None:
    try:
        dt = datetime.fromisoformat(str(value))
    except Exception:
        return None
    return float(dt.hour + dt.minute / 60.0)


def _as_text(value: Any) -> str:
    return str(value or "").strip()


def _humanize_layer_finding(text: Any) -> str:
    raw = _as_text(text).rstrip("。")
    if not raw:
        return ""
    replacements = (
        ("模式剖面指向低层浅稳层/弱逆温仍在，午前混合偏慢", "近地面这层空气还比较稳，上午不容易很快升温"),
        ("模式剖面显示低层存在一定稳定约束", "近地面这层空气还不够容易翻动，升温会偏慢"),
        ("近地层高湿接近饱和，低云/雾层消散更慢", "近地面又湿又闷，低云和雾不容易很快散开"),
        ("700hPa附近偏干，若开云可帮助侵蚀低云", "中层偏干，一旦见到日照，会更有利于云层减弱"),
        ("700hPa附近偏湿，云层维持条件偏强", "中层也偏湿，云层更容易维持"),
        ("925–850混合潜力尚可，一旦见光后段升温效率可改善", "一旦见到日照，低层空气有机会更快翻动，后段升温会顺一些"),
        ("925–850混合偏弱，升温更依赖低云何时真正破碎", "低层空气不太容易翻动，升温更要看低云何时真正散开"),
        ("925–850混合偏弱，午后升温更要看少云能否维持", "低层空气不太容易完全混匀，午后升温更要看升温势头能否维持"),
        ("模式剖面信号中性，优先跟踪下一报温度斜率与云量开合", "层结信号不突出，先看下一报温度和云量怎么走"),
        ("925–850hPa存在稳定层（封盖信号），冲高持续性受限", "低层上方还有一层稳定空气，升温往上冲会比较吃力"),
        ("低层湿层上接中层干层", "近地面偏湿，但再往上会转干"),
        ("925–700hPa层间耦合偏弱", "从近地面到更高一些的大气配合不够顺"),
        ("925–700hPa层间耦合较顺", "从近地面到更高一些的大气配合还算顺"),
    )
    out = raw
    for src, dst in replacements:
        if src in out:
            out = out.replace(src, dst)
    return out


def _vertical_regime_summary(vertical_regime: str) -> str:
    return {
        "low_cloud_clearing": "低层偏湿，低云和雾什么时候真正减弱更关键",
        "static_stable": "近地面这层空气偏稳，升温不容易一下子放大",
        "dry_clear_mixed": "低层空气不太容易完全混匀，后段升温更看风场和升温势头",
        "moist_capped": "低层偏湿偏稳，升温容易受压",
        "mixed_supportive": "低层一旦混合起来，后段升温会更顺",
        "neutral": "层结信号不算突出，先看实况升温节奏怎么走",
    }.get(str(vertical_regime or ""), "")


def build_model_sounding_proxy(
    primary_window: dict[str, Any],
    metar_diag: dict[str, Any],
    *,
    h700_summary: str = "",
    h925_summary: str = "",
    cloud_code_now: str = "",
) -> dict[str, Any]:
    signals = build_live_condition_signals(metar_diag or {})
    latest_rh = _safe_float(signals.get("latest_rh"))
    latest_wspd = _safe_float(signals.get("latest_wspd_kt"))
    latest_temp = _safe_float(signals.get("latest_temp_c"))
    dewpoint = _safe_float(signals.get("dewpoint_c"))
    temp_trend_c = _safe_float(signals.get("temp_trend_c"))
    local_hour = _hour_float(signals.get("latest_report_local"))
    latest_wx = _as_text((metar_diag or {}).get("latest_wx")).upper()
    low_cloud_pct = _safe_float(primary_window.get("low_cloud_pct"))
    w850_kmh = _safe_float(primary_window.get("w850_kmh"))
    cloud_base_ft = _safe_float((metar_diag or {}).get("latest_cloud_lowest_base_ft"))

    dewpoint_spread = None
    if latest_temp is not None and dewpoint is not None:
        dewpoint_spread = latest_temp - dewpoint

    low_level_cap_score = 0.0
    if low_cloud_pct is not None and low_cloud_pct >= 75.0:
        low_level_cap_score += 0.30
    if latest_rh is not None and latest_rh >= 92.0:
        low_level_cap_score += 0.24
    elif dewpoint_spread is not None and dewpoint_spread <= 1.5:
        low_level_cap_score += 0.22
    if cloud_code_now in {"BKN", "OVC", "VV"}:
        low_level_cap_score += 0.18
    if cloud_base_ft is not None and cloud_base_ft <= 800.0:
        low_level_cap_score += 0.14
    if _contains_any(latest_wx, ("FG", "BR", "BCFG", "DZ")):
        low_level_cap_score += 0.18
    if latest_wspd is not None and latest_wspd <= 5.0:
        low_level_cap_score += 0.14
    if w850_kmh is not None and w850_kmh <= 18.0:
        low_level_cap_score += 0.10
    if "耦合偏弱" in h925_summary:
        low_level_cap_score += 0.12
    if temp_trend_c is not None and abs(temp_trend_c) <= 0.12 and local_hour is not None and local_hour <= 12.5:
        low_level_cap_score += 0.08
    low_level_cap_score = _clamp01(low_level_cap_score)

    low_level_mix_score = 0.0
    if latest_wspd is not None:
        if latest_wspd >= 9.0:
            low_level_mix_score += 0.28
        elif latest_wspd >= 6.0:
            low_level_mix_score += 0.16
    if w850_kmh is not None:
        if w850_kmh >= 32.0:
            low_level_mix_score += 0.32
        elif w850_kmh >= 22.0:
            low_level_mix_score += 0.18
    if dewpoint_spread is not None:
        if dewpoint_spread >= 4.0:
            low_level_mix_score += 0.18
        elif dewpoint_spread >= 2.5:
            low_level_mix_score += 0.10
    if "耦合偏强" in h925_summary:
        low_level_mix_score += 0.18
    low_level_mix_score = _clamp01(low_level_mix_score)

    wind_profile_mix_score = 0.0
    if w850_kmh is not None:
        w850_kt = w850_kmh / 1.852
        if w850_kt >= 22.0:
            wind_profile_mix_score = 1.0
        elif w850_kt >= 16.0:
            wind_profile_mix_score = 0.65
        elif w850_kt >= 11.0:
            wind_profile_mix_score = 0.35
    else:
        w850_kt = None

    midlevel_dry_score = _clamp01(
        max(
            h700_effective_dry_factor(
                h700_summary,
                low_cloud_pct=low_cloud_pct,
                cloud_code_now=cloud_code_now,
            ),
            0.22 if (_contains_any(h700_summary, ("偏干", "干层")) and (low_cloud_pct is None or low_cloud_pct <= 35.0)) else 0.0,
        )
    )
    midlevel_moist_score = _clamp01(
        1.0 if h700_is_moist_constraint(h700_summary) else (
            0.55 if (low_cloud_pct is not None and low_cloud_pct >= 85.0 and latest_rh is not None and latest_rh >= 95.0) else 0.0
        )
    )

    mixing_support_score = _clamp01(
        max(
            low_level_mix_score,
            min(1.0, 0.55 * low_level_mix_score + 0.30 * wind_profile_mix_score + 0.25 * midlevel_dry_score),
        )
    )
    suppression_score = _clamp01(
        0.60 * low_level_cap_score
        + 0.25 * midlevel_moist_score
        + (0.15 if (latest_rh is not None and latest_rh >= 92.0) else 0.0)
    )

    layer_findings: list[str] = []
    if low_level_cap_score >= 0.75:
        layer_findings.append("模式剖面指向低层浅稳层/弱逆温仍在，午前混合偏慢。")
    elif low_level_cap_score >= 0.45:
        layer_findings.append("模式剖面显示低层存在一定稳定约束。")
    if (
        (latest_rh is not None and latest_rh >= 90.0)
        or (dewpoint_spread is not None and dewpoint_spread <= 1.5)
        or (low_cloud_pct is not None and low_cloud_pct >= 70.0)
    ):
        layer_findings.append("近地层高湿接近饱和，低云/雾层消散更慢。")
    if midlevel_dry_score >= 0.55:
        layer_findings.append("700hPa附近偏干，若开云可帮助侵蚀低云。")
    elif midlevel_moist_score >= 0.55:
        layer_findings.append("700hPa附近偏湿，云层维持条件偏强。")
    dry_clear_signature = bool(
        (low_cloud_pct is not None and low_cloud_pct <= 35.0)
        and (latest_rh is not None and latest_rh <= 75.0)
        and (dewpoint_spread is not None and dewpoint_spread >= 4.0)
        and cloud_code_now in {"CLR", "SKC", "FEW", "SCT", "CAVOK"}
    )

    if mixing_support_score >= 0.65:
        layer_findings.append("925–850混合潜力尚可，一旦见光后段升温效率可改善。")
    elif mixing_support_score <= 0.35:
        if dry_clear_signature:
            layer_findings.append("925–850混合偏弱，午后升温更要看少云能否维持。")
        else:
            layer_findings.append("925–850混合偏弱，升温更依赖低云何时真正破碎。")
    if not layer_findings:
        layer_findings.append("模式剖面信号中性，优先跟踪下一报温度斜率与云量开合。")

    if low_level_cap_score >= 0.65 or suppression_score >= 0.65:
        actionable = "若未来1-2小时仍未明显开云，上沿需下修；若温露差拉大、云底抬升、风速略增，则说明稳层开始松动。"
        path_bias = "高位收敛"
    elif midlevel_dry_score >= 0.55 or mixing_support_score >= 0.65:
        actionable = "若午前后见光并维持正斜率，后段仍有补涨空间。"
        path_bias = "高位再试探"
    else:
        actionable = "当前更偏边界层节奏控制，优先看云底、温露差与风速是否同步改善。"
        path_bias = "高位收敛"

    if low_level_cap_score >= 0.65 and (
        (latest_rh is not None and latest_rh >= 90.0)
        or (dewpoint_spread is not None and dewpoint_spread <= 1.5)
        or (low_cloud_pct is not None and low_cloud_pct >= 70.0)
    ):
        vertical_regime = "low_cloud_clearing"
    elif low_level_cap_score >= 0.55:
        vertical_regime = "static_stable"
    elif dry_clear_signature and low_level_cap_score < 0.45 and mixing_support_score < 0.65:
        vertical_regime = "dry_clear_mixed"
    elif midlevel_moist_score >= 0.55 and low_level_cap_score >= 0.35:
        vertical_regime = "moist_capped"
    elif mixing_support_score >= 0.65:
        vertical_regime = "mixed_supportive"
    else:
        vertical_regime = "neutral"

    return {
        "has_profile": True,
        "quality": "model_proxy",
        "profile_source": "model_proxy",
        "use_sounding_obs": False,
        "sounding_confidence": "M" if (low_level_cap_score >= 0.45 or mixing_support_score >= 0.45 or midlevel_dry_score >= 0.45 or midlevel_moist_score >= 0.45) else "L",
        "obs_age_hours": None,
        "rh925_pct": latest_rh,
        "rh850_pct": None,
        "rh700_pct": None,
        "t925_t850_c": None,
        "midlevel_rh_pct": None,
        "wind925_dir_deg": None,
        "wind850_dir_deg": None,
        "wind700_dir_deg": None,
        "wind925_kt": latest_wspd,
        "wind850_kt": round(float(w850_kt), 1) if w850_kt is not None else None,
        "wind700_kt": None,
        "low_level_cap_score": round(low_level_cap_score, 3),
        "low_level_mix_score": round(low_level_mix_score, 3),
        "midlevel_dry_score": round(midlevel_dry_score, 3),
        "midlevel_moist_score": round(midlevel_moist_score, 3),
        "wind_profile_mix_score": round(wind_profile_mix_score, 3),
        "mixing_support_score": round(mixing_support_score, 3),
        "suppression_score": round(suppression_score, 3),
        "layer_findings": layer_findings[:3],
        "vertical_regime": vertical_regime,
        "actionable": actionable,
        "path_bias": path_bias,
    }


def merge_sounding_thermo(
    snd_thermo: dict[str, Any] | None,
    proxy_thermo: dict[str, Any],
) -> dict[str, Any]:
    merged = dict(proxy_thermo or {})
    raw = dict(snd_thermo or {})
    for key, value in raw.items():
        if value not in (None, "", [], {}):
            merged[key] = value
    if bool(raw.get("has_profile")):
        merged["has_profile"] = True
        merged["quality"] = str(raw.get("quality") or merged.get("quality") or "ok")
        merged["profile_source"] = str(raw.get("profile_source") or merged.get("profile_source") or "model")
        merged["use_sounding_obs"] = bool(raw.get("use_sounding_obs"))
        merged["sounding_confidence"] = str(raw.get("sounding_confidence") or merged.get("sounding_confidence") or "L")
    return merged


def build_boundary_layer_regime(
    primary_window: dict[str, Any],
    metar_diag: dict[str, Any],
    *,
    snd_thermo: dict[str, Any] | None = None,
    advection_review: dict[str, Any] | None = None,
    h700_summary: str = "",
    h925_summary: str = "",
    line850: str = "",
    extra: str = "",
    h500_regime: str = "",
    object_type: str = "",
    cloud_code_now: str = "",
) -> dict[str, Any]:
    signals = build_live_condition_signals(metar_diag or {})
    proxy_thermo = build_model_sounding_proxy(
        primary_window,
        metar_diag,
        h700_summary=h700_summary,
        h925_summary=h925_summary,
        cloud_code_now=cloud_code_now,
    )
    thermo = merge_sounding_thermo(snd_thermo, proxy_thermo)

    latest_rh = _safe_float(signals.get("latest_rh"))
    latest_wspd = _safe_float(signals.get("latest_wspd_kt"))
    latest_temp = _safe_float(signals.get("latest_temp_c"))
    dewpoint = _safe_float(signals.get("dewpoint_c"))
    temp_trend_c = _safe_float(signals.get("temp_trend_c"))
    local_hour = _hour_float(signals.get("latest_report_local"))
    latest_wx = _as_text((metar_diag or {}).get("latest_wx")).upper()
    low_cloud_pct = _safe_float(primary_window.get("low_cloud_pct"))
    cloud_base_ft = _safe_float((metar_diag or {}).get("latest_cloud_lowest_base_ft"))
    dewpoint_spread = None if latest_temp is None or dewpoint is None else (latest_temp - dewpoint)

    cap_score = _safe_float(thermo.get("low_level_cap_score")) or 0.0
    mix_score = _safe_float(thermo.get("mixing_support_score")) or 0.0
    mid_dry_score = _safe_float(thermo.get("midlevel_dry_score")) or 0.0
    mid_moist_score = _safe_float(thermo.get("midlevel_moist_score")) or 0.0
    adv_review = advection_review if isinstance(advection_review, dict) else {}
    adv_role = str(adv_review.get("surface_role") or "")
    adv_bias = str(adv_review.get("transport_state") or adv_review.get("surface_bias") or "")
    adv_state = str(adv_review.get("thermal_advection_state") or "")
    adv_coupling_state = str(adv_review.get("surface_coupling_state") or "")
    adv_effect_weight = _safe_float(adv_review.get("surface_effect_weight")) or 0.0
    clearing_signature = bool(
        ((latest_rh is not None and latest_rh >= 90.0) or (dewpoint_spread is not None and dewpoint_spread <= 1.5) or _contains_any(latest_wx, ("FG", "BR", "BCFG", "DZ")))
        and ((cloud_code_now in {"BKN", "OVC", "VV"}) or (cloud_base_ft is not None and cloud_base_ft <= 900.0))
        and (latest_wspd is None or latest_wspd <= 5.0)
        and (local_hour is None or local_hour <= 14.0)
    )

    static_stable_score = 0.0
    if cap_score >= 0.75:
        static_stable_score += 0.90
    elif cap_score >= 0.45:
        static_stable_score += 0.55
    if latest_rh is not None and latest_rh >= 90.0:
        static_stable_score += 0.35
    elif dewpoint_spread is not None and dewpoint_spread <= 1.5:
        static_stable_score += 0.30
    if low_cloud_pct is not None and low_cloud_pct >= 70.0:
        static_stable_score += 0.35
    if cloud_code_now in {"BKN", "OVC", "VV"}:
        static_stable_score += 0.20
    if cloud_base_ft is not None and cloud_base_ft <= 800.0:
        static_stable_score += 0.18
    if _contains_any(latest_wx, ("FG", "BR", "BCFG", "DZ")):
        static_stable_score += 0.20
    if latest_wspd is not None and latest_wspd <= 5.0:
        static_stable_score += 0.18
    if temp_trend_c is not None and abs(temp_trend_c) <= 0.12 and local_hour is not None and local_hour <= 12.5:
        static_stable_score += 0.12
    if "耦合偏弱" in h925_summary:
        static_stable_score += 0.18
    if mid_moist_score >= 0.55:
        static_stable_score += 0.15
    if clearing_signature:
        static_stable_score += 0.28

    clearing_score = 0.0
    if static_stable_score >= 1.0:
        clearing_score += 0.45
    if low_cloud_pct is not None and low_cloud_pct >= 65.0:
        clearing_score += 0.28
    if _contains_any(latest_wx, ("FG", "BR", "BCFG")):
        clearing_score += 0.22
    if mid_dry_score >= 0.55:
        clearing_score += 0.28
    elif mix_score >= 0.55:
        clearing_score += 0.18
    if temp_trend_c is not None and temp_trend_c > 0.15:
        clearing_score += 0.10
    if clearing_signature:
        clearing_score += 0.38

    mixing_score = 0.0
    if mix_score >= 0.70:
        mixing_score += 0.85
    elif mix_score >= 0.45:
        mixing_score += 0.45
    if latest_wspd is not None and latest_wspd >= 8.0:
        mixing_score += 0.18
    if temp_trend_c is not None and temp_trend_c >= 0.25:
        mixing_score += 0.18
    if dewpoint_spread is not None and dewpoint_spread >= 3.0:
        mixing_score += 0.15

    advection_score = 0.0
    if adv_role == "dominant":
        advection_score += 1.05 + 0.18 * adv_effect_weight
    elif adv_role == "influence":
        advection_score += 0.68 + 0.15 * adv_effect_weight
    elif adv_role == "background":
        advection_score += 0.18
    elif adv_state in {"confirmed", "probable"}:
        advection_score += 0.62
    elif ("暖平流" in line850) or ("冷平流" in line850):
        advection_score += 0.70
    if adv_state == "confirmed":
        advection_score += 0.22
    elif adv_state == "probable":
        advection_score += 0.12
    if adv_role in {"dominant", "influence"} and adv_bias in {"warm", "cold"}:
        advection_score += 0.15
    elif ("暖平流" in line850 and "冷平流" not in line850) or ("冷平流" in line850 and "暖平流" not in line850):
        advection_score += 0.20
    if _contains_any(extra, ("平流", "输送")):
        advection_score += 0.15

    synoptic_score = 0.0
    if h500_regime in {"副热带高压控制", "副热带高压边缘", "高压暖脊", "高压脊", "低压深槽", "低压槽", "近区槽脊过渡", "冷高压稳定压温"}:
        synoptic_score += 0.65
    if _contains_any(object_type, ("dynamic", "subsidence", "baroclinic", "front", "trough", "ridge")):
        synoptic_score += 0.25
    if _contains_any(extra, ("封盖", "压制", "湿层", "低云")) and static_stable_score < 1.0:
        synoptic_score += 0.12

    scores = {
        "static_stable": round(static_stable_score, 2),
        "boundary_layer_clearing": round(clearing_score, 2),
        "mixing_depth": round(mixing_score, 2),
        "advection": round(advection_score, 2),
        "synoptic": round(synoptic_score, 2),
    }
    ranked = sorted(scores.items(), key=lambda item: item[1], reverse=True)
    regime_key = "synoptic"
    dominant_mechanism = "背景环流与输送"
    if static_stable_score >= 1.30 and clearing_score >= 0.80 and static_stable_score >= max(advection_score, synoptic_score):
        regime_key = "boundary_layer_clearing"
        dominant_mechanism = "低云清除"
    elif clearing_signature and static_stable_score >= max(advection_score, synoptic_score) - 0.10:
        regime_key = "boundary_layer_clearing"
        dominant_mechanism = "低云清除"
    elif static_stable_score >= 1.15 and static_stable_score >= max(mixing_score, advection_score, synoptic_score):
        regime_key = "static_stable"
        dominant_mechanism = "静稳约束"
    elif mixing_score >= max(static_stable_score, advection_score, synoptic_score) and mixing_score >= 0.85:
        regime_key = "mixing_depth"
        dominant_mechanism = "混合加深"
    elif advection_score >= max(static_stable_score, mixing_score, synoptic_score) and advection_score >= 0.80:
        regime_key = "advection"
        dominant_mechanism = "低层输送主导"

    confidence = "low"
    if ranked and len(ranked) >= 2 and (ranked[0][1] - ranked[1][1]) >= 0.45:
        confidence = "high"
    elif ranked and ranked[0][1] >= 0.90:
        confidence = "medium"

    dry_clear_signature = bool(
        (low_cloud_pct is not None and low_cloud_pct <= 35.0)
        and (latest_rh is not None and latest_rh <= 75.0)
        and (dewpoint_spread is not None and dewpoint_spread >= 4.0)
        and cloud_code_now in {"CLR", "SKC", "FEW", "SCT", "CAVOK"}
    )

    if regime_key == "boundary_layer_clearing":
        headline = "今天先看低云和雾何时散开；在它们真正减弱前，升温会比较慢，最高温也更容易被压住。"
        tracking_line = "优先看低云底是否抬升、温露差是否拉大、风速是否略增；若未来1-2报仍未明显开云，上沿需下修。"
    elif regime_key == "static_stable":
        headline = "今天低层空气偏稳，近地面不容易很快升温；如果这种状态不松动，白天最高温就更难抬高。"
        tracking_line = "重点看低层稳层是否松动；若温露差继续很小、云底维持偏低、风场仍弱，则全天上沿更难抬高。"
    elif regime_key == "mixing_depth":
        headline = "今天能不能升得更快，主要看近地面这层闷住的空气何时被打散；一旦混合起来，后段升温会顺很多。"
        tracking_line = "优先看风速与温度斜率是否同步放大；若见光后混合层迅速加深，后段仍可补涨。"
    elif regime_key == "advection":
        direction = thermal_advection_direction(adv_review, line850=line850)
        if direction == "cold":
            if adv_state == "confirmed":
                headline = "今日更偏低层冷平流主导，最高温更受冷平流在峰值窗前后能否持续落地影响。"
            elif adv_state == "probable":
                headline = "今日更偏低层偏冷输送背景主导，冷平流是否真正成为地面主导，仍要看峰值窗前后的落地程度。"
            else:
                headline = "今日低层偏冷输送背景存在，但是否上升到冷平流主导，仍要看后续落地链条。"
            if adv_coupling_state == "weak" or "耦合偏弱" in h925_summary:
                tracking_line = "优先看风向是否继续偏冷象限、风速是否增强，以及实况是否重新转冷偏离同小时模式；若近地耦合仍弱，偏冷背景未必能完整压到地面。"
            else:
                tracking_line = "优先看风向是否继续偏冷象限、风速是否增强，以及实况是否重新转冷偏离同小时模式；这将决定压温路径是否兑现。"
        elif direction == "warm":
            if adv_state == "confirmed":
                headline = "今日更偏低层暖平流主导，最高温更受暖平流在峰值窗前后能否持续落地影响。"
            elif adv_state == "probable":
                headline = "今日更偏低层偏暖输送背景主导，暖平流是否真正成为地面主导，仍要看峰值窗前后的落地程度。"
            else:
                headline = "今日低层偏暖输送背景存在，但是否上升到暖平流主导，仍要看后续落地链条。"
            if adv_coupling_state == "weak" or "耦合偏弱" in h925_summary:
                tracking_line = "优先看风向是否转入更有利增温的象限、风速是否增强，以及实况是否重新转暖偏离同小时模式；若近地耦合仍弱，偏暖背景落地会偏打折。"
            else:
                tracking_line = "优先看风向是否转入更有利增温的象限、风速是否增强，以及实况是否重新转暖偏离同小时模式；这将决定后段能否抬高上沿。"
        else:
            headline = "今天更要看低层空气本身是在变暖还是变冷，以及这种变化能不能真正传到地面。"
            tracking_line = "优先看风向风速是否发生持续重排，以及实况与同小时模式的温度偏差是否同向扩大，确认低层温度输送是否开始主导地面温度。"
    else:
        background_bits: list[str] = []
        if low_cloud_pct is not None and low_cloud_pct >= 50.0:
            background_bits.append("云量会不会重新增多")
        elif dry_clear_signature:
            background_bits.append("午后升温效率能否继续维持")
        if adv_state in {"confirmed", "probable"} or adv_role in {"dominant", "influence"}:
            direction = thermal_advection_direction(adv_review, line850=line850)
            if direction == "cold":
                background_bits.append("偏冷输送是否继续落地")
            elif direction == "warm":
                background_bits.append("偏暖输送是否继续落地")
            else:
                background_bits.append("低层输送是否继续重排")
        if latest_wspd is not None and latest_wspd >= 8.0:
            background_bits.append("低层风场能否继续带动升温")
        if not background_bits:
            background_bits.append("云量变化")
            background_bits.append("低层风场")

        if len(background_bits) >= 2:
            headline = (
                "后段更要看"
                + background_bits[0]
                + "，以及"
                + background_bits[1]
                + "；这会一起决定后段升温还能不能延续。"
            )
        else:
            headline = (
                "后段更要看"
                + background_bits[0]
                + "；这会决定后段升温还能不能延续。"
            )
        tracking_line = "优先看云量、近地风场和温度斜率能否继续配合；若三者同步走强，再考虑上修后段上沿。"

    layer_bits: list[str] = []
    for finding in list(thermo.get("layer_findings") or []):
        txt = _humanize_layer_finding(finding)
        if txt and txt not in layer_bits:
            layer_bits.append(txt)
    for finding in list(thermo.get("relationship_findings") or []):
        txt = _humanize_layer_finding(finding)
        if txt and txt not in layer_bits:
            layer_bits.append(txt)
    vertical_regime = str(thermo.get("vertical_regime") or "")
    if dry_clear_signature:
        layer_bits = [
            "低层空气不太容易完全混匀，午后升温更要看升温势头能否维持"
            if ("低云何时真正散开" in txt or "低云何时真正破碎" in txt)
            else txt
            for txt in layer_bits
        ]
    primary_layer_summary = _vertical_regime_summary(vertical_regime)
    if primary_layer_summary:
        layer_bits = [txt for txt in layer_bits if txt != primary_layer_summary]
        layer_bits.insert(0, primary_layer_summary)
    if not layer_bits:
        if cap_score >= 0.65:
            layer_bits.append("近地面这层空气还比较稳")
        if mid_dry_score >= 0.55:
            layer_bits.append("中层偏干")
        elif mid_moist_score >= 0.55:
            layer_bits.append("中层偏湿")
    layer_summary = "；".join(layer_bits[:2]) + "。" if layer_bits else ""

    reason_codes: list[str] = []
    if regime_key in {"boundary_layer_clearing", "static_stable"}:
        reason_codes.append("static_stable_boundary_layer")
    if adv_role == "dominant":
        reason_codes.append("advection_surface_dominant")
    elif adv_role == "influence":
        reason_codes.append("advection_surface_influence")
    elif adv_role == "background":
        reason_codes.append("advection_background_only")
    if _contains_any(latest_wx, ("FG", "BR", "BCFG", "DZ")):
        reason_codes.append("fog_or_mist_present")
    if low_cloud_pct is not None and low_cloud_pct >= 70.0:
        reason_codes.append("low_cloud_persistent")
    if cap_score >= 0.65:
        reason_codes.append("low_level_cap")
    if mid_dry_score >= 0.55:
        reason_codes.append("midlevel_dry_support")
    if mid_moist_score >= 0.55:
        reason_codes.append("midlevel_moist_constraint")

    return {
        "schema_version": BOUNDARY_LAYER_REGIME_SCHEMA_VERSION,
        "regime_key": regime_key,
        "dominant_mechanism": dominant_mechanism,
        "confidence": confidence,
        "sounding_mode": str(thermo.get("profile_source") or "model_proxy"),
        "scores": scores,
        "headline": headline,
        "layer_summary": layer_summary,
        "tracking_line": tracking_line,
        "reason_codes": reason_codes,
        "advection_role": adv_role,
        "thermo": thermo,
    }
