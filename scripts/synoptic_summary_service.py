#!/usr/bin/env python3
"""Structured synoptic summary builder for /look analysis snapshot."""

from __future__ import annotations

import re
from typing import Any

from advection_review import thermal_advection_direction
from layer_signal_policy import h700_dry_support_factor, h700_effective_dry_factor, h700_is_moist_constraint, h700_should_surface_in_evidence


def _h500_weight_score(feature: dict[str, Any] | None) -> float:
    if not isinstance(feature, dict):
        return 0.0
    try:
        return max(-1.0, min(1.0, float(feature.get("tmax_weight_score") or 0.0)))
    except Exception:
        return 0.0


def _h500_regime_label(feature: dict[str, Any] | None) -> str:
    if not isinstance(feature, dict):
        return ""
    return str(feature.get("regime_label") or "").strip()


def _h500_thermal_role(feature: dict[str, Any] | None) -> str:
    if not isinstance(feature, dict):
        return ""
    return str(feature.get("thermal_role") or "").strip()


def _h500_has_key_signal(feature: dict[str, Any] | None) -> bool:
    if not isinstance(feature, dict):
        return False
    if str(feature.get("impact_weight") or "") in {"medium", "high"}:
        return True
    role = _h500_thermal_role(feature)
    score = abs(_h500_weight_score(feature))
    if role in {"warm_high_subsidence", "cold_high_suppression", "trough_lift"} and score >= 0.18:
        return True
    return abs(_h500_weight_score(feature)) >= 0.22


def build_synoptic_summary(
    *,
    primary_window: dict[str, Any],
    metar_diag: dict[str, Any],
    syn_w: dict[str, Any],
    calc_window: dict[str, Any],
    obj: dict[str, Any],
    candidates: list[dict[str, Any]],
    cov: float | None,
    line500: str,
    h500_feature: dict[str, Any] | None,
    line850: str,
    advection_review: dict[str, Any] | None,
    extra: str,
    h700_summary: str,
    h925_summary: str,
    snd_thermo: dict[str, Any],
    cloud_code_now: str,
    boundary_layer_regime: dict[str, Any] | None = None,
    compact_synoptic: bool = False,
) -> dict[str, Any]:
    syn_lines = ["🧭 **环流形势对最高温影响**"]
    h500_regime = _h500_regime_label(h500_feature)
    h500_score = _h500_weight_score(h500_feature)
    h500_key_signal = _h500_has_key_signal(h500_feature)
    adv_review = advection_review if isinstance(advection_review, dict) else {}
    regime = boundary_layer_regime if isinstance(boundary_layer_regime, dict) else {}
    regime_key = str(regime.get("regime_key") or "")
    regime_headline = str(regime.get("headline") or "").strip()
    layer_summary = str(regime.get("layer_summary") or "").strip()
    sounding_mode = str(regime.get("sounding_mode") or "")
    adv_state = str(adv_review.get("thermal_advection_state") or "")
    adv_direction = thermal_advection_direction(adv_review, line850=line850)
    adv_is_confirmed = adv_state == "confirmed"
    adv_is_foreground = adv_state in {"confirmed", "probable"}

    def _fallback_regime_headline(key: str) -> str:
        return {
            "boundary_layer_clearing": "今天先看低云和雾何时散开；在它们真正减弱前，升温会比较慢，最高温也更容易被压住",
            "static_stable": "今天低层空气偏稳，近地面不容易很快升温；如果这种状态不松动，白天最高温就更难抬高",
            "mixing_depth": "今天能不能升得更快，主要看近地面这层闷住的空气何时被打散；一旦混合起来，后段升温会顺很多",
            "advection": "今天更要看低层空气本身是在变暖还是变冷，以及这种变化能不能真正传到地面",
        }.get(key, "")

    def _contains_any(text: str, keys: list[str]) -> bool:
        s = str(text or "")
        return any(k in s for k in keys)

    def _regime_scores() -> dict[str, float]:
        s = {"advection": 0.0, "dynamic": 0.0, "stability": 0.0, "baroclinic": 0.0, "shear": 0.0}
        txt850 = str(line850)
        txt500 = str(line500)
        txtx = str(extra)
        if adv_is_foreground:
            s["advection"] += 0.95
        if h500_key_signal:
            s["dynamic"] += 0.85
        elif _contains_any(txt500, ["副热带高压", "冷高压", "深槽", "低压槽", "暖脊", "高压脊", "抬升", "PVA", "NVA", "涡度", "下沉稳定"]):
            s["dynamic"] += 0.55
        if _contains_any(txtx, ["封盖", "压制", "湿层", "低云", "耦合偏弱"]):
            s["stability"] += 0.9
        if _contains_any(txtx + txt850, ["锋", "锋生", "斜压"]):
            s["baroclinic"] += 0.55
            if adv_is_foreground:
                s["baroclinic"] += 0.15
        if _contains_any(txtx + txt850, ["风切", "切换"]):
            s["shear"] += 0.7
        o = dict(obj) if isinstance(obj, dict) else {}
        if o:
            t = str(o.get("type") or "").lower()
            conf_boost = {"high": 1.2, "medium": 0.8, "low": 0.3}.get(str(o.get("confidence") or ""), 0.3)
            if "advection" in t:
                s["advection"] += conf_boost
            if "dynamic" in t or "trough" in t:
                s["dynamic"] += conf_boost
            if "dry_intrusion" in t or "subsidence" in t:
                s["stability"] += conf_boost
            if "baroclinic" in t or "front" in t:
                s["baroclinic"] += conf_boost
            if "shear" in t:
                s["shear"] += conf_boost
        for c in candidates[:4]:
            if not isinstance(c, dict):
                continue
            t = str(c.get("type") or "").lower()
            w = {"high": 0.45, "medium": 0.35, "low": 0.12}.get(str(c.get("confidence") or ""), 0.1)
            if "advection" in t:
                s["advection"] += w
            if "dynamic" in t or "trough" in t:
                s["dynamic"] += w
            if "dry_intrusion" in t or "subsidence" in t:
                s["stability"] += w
            if "baroclinic" in t or "front" in t:
                s["baroclinic"] += w
            if "shear" in t:
                s["shear"] += w
        try:
            if cov is not None and float(cov) < 0.65:
                s["baroclinic"] *= 0.86
                s["shear"] *= 0.9
        except Exception:
            pass
        return s

    def _regime_label(key: str) -> str:
        return {
            "advection": "平流输送",
            "dynamic": "高空动力触发",
            "stability": "低层稳定约束",
            "baroclinic": "锋面/斜压调整",
            "shear": "风切节奏扰动",
        }.get(key, key)

    def _dir_cn_from_deg(deg: float) -> str:
        dirs = ["北", "东北", "东", "东南", "南", "西南", "西", "西北"]
        idx = int(((deg % 360) + 22.5) // 45) % 8
        return dirs[idx]

    def _front_plain_desc(otype: str) -> str | None:
        is_front = ("front" in otype) or ("baroclinic" in otype) or _contains_any(str(line850) + str(extra), ["锋", "锋生", "斜压"])
        if not is_front:
            return None
        warm = adv_direction == "warm" and adv_is_foreground
        cold = adv_direction == "cold" and adv_is_foreground
        if warm and not cold:
            nature = "偏暖锋"
        elif cold and not warm:
            nature = "偏冷锋"
        elif warm and cold:
            nature = "冷暖交汇（近静止锋）"
        else:
            nature = "锋性过渡"
        wdir = metar_diag.get("latest_wdir")
        wspd = metar_diag.get("latest_wspd")
        try:
            wspd_v = float(wspd)
        except Exception:
            wspd_v = None
        if wdir in (None, "", "VRB") or wspd_v is None or wspd_v <= 4:
            move = "移动偏慢，接近准静止"
        else:
            try:
                to_deg = (float(wdir) + 180.0) % 360.0
                move = f"可能向{_dir_cn_from_deg(to_deg)}方向缓慢推进（低置信）"
            except Exception:
                move = "移动方向暂不稳定"
        return f"{nature}；{move}"

    def _system_plain_desc(otype: str) -> str | None:
        fd = _front_plain_desc(otype)
        if fd:
            return fd
        txtx = str(extra)
        if regime_key == "boundary_layer_clearing":
            return "低云雾何时抬升和破碎更关键，白天升温先看边界层清除节奏"
        if regime_key == "static_stable":
            return "低层稳层仍在，午前升温效率更容易受压"
        if regime_key == "mixing_depth":
            return "能否尽快打穿低层稳层，比高空背景本身更关键"
        if ("advection" in otype) or adv_direction in {"warm", "cold"}:
            if adv_direction == "warm" and adv_is_confirmed:
                return "低层偏暖输送（850-925hPa）更明确，若云量放开，升温会更顺"
            if adv_direction == "warm":
                return "低层偏暖输送（850-925hPa）为主，云量若放开，升温会更顺"
            if adv_direction == "cold" and adv_is_confirmed:
                return "低层偏冷输送（850-925hPa）更明确，对升温有抑制"
            if adv_direction == "cold":
                return "低层偏冷输送（850-925hPa）偏强，对升温有压制"
            return "冷暖输送并存，短时更容易出现重排"
        if ("dry_intrusion" in otype) or _contains_any(txtx, ["封盖", "湿层", "低云", "压制", "干层"]):
            return "低层受封盖约束，短时升温不容易放大"
        if ("dynamic" in otype) or h500_regime in {"低压深槽", "低压槽", "近区槽脊过渡"} or _contains_any(str(line500), ["抬升", "涡度", "PVA"]):
            return "高空有触发信号，但是否落地还要看近地风云配合"
        return None

    rs = _regime_scores()
    r_sorted = sorted(rs.items(), key=lambda x: x[1], reverse=True)
    r1, s1 = r_sorted[0]
    has_primary_regime = s1 >= 0.9
    otype = str((obj or {}).get("type") or "").lower()
    impact = str((obj or {}).get("impact_scope") or "background_only")

    if obj:
        background_driver = _regime_label(r1) if has_primary_regime else "混合背景"
        system_plain_desc = _system_plain_desc(otype) or background_driver
        if impact == "station_relevant":
            scope_txt = "影响会直接落在峰值窗"
        elif impact == "possible_override":
            scope_txt = "主要改写峰值时段"
        else:
            scope_txt = "短时改写幅度有限"
    else:
        background_driver = _regime_label(r1) if has_primary_regime else "背景场"
        system_plain_desc = _system_plain_desc("") or "暂未识别到单独可追踪的近站系统"
        scope_txt = "更偏实况触发"

    def _impact_direction_and_trigger() -> tuple[str, str]:
        if adv_direction == "cold":
            direction = "更可能比原先预报略低"
        elif adv_direction == "warm":
            direction = "更可能比原先预报略高"
        else:
            direction = "暂时看不出明显偏高或偏低"
        trigger = "先看升温是否能连续走强"
        return direction, trigger

    direction_txt, trigger_txt = _impact_direction_and_trigger()
    impact_line = f"{direction_txt}；{scope_txt}"

    def _humanize_850(s: str) -> str:
        txt = str(s or "")
        m = re.search(r"(暖平流|冷平流)([^（]*)（([0-9.]+)，([^）]+)）", txt)
        if not m:
            if "暖平流" in txt:
                return "低层偏暖输送（850-925hPa）较明显"
            if "冷平流" in txt:
                return "低层偏冷输送（850-925hPa）较明显"
            return txt
        kind = m.group(1)
        qual = str(m.group(2) or "").strip()
        conf_raw = float(m.group(3))
        conf = "高" if conf_raw >= 0.67 else "中" if conf_raw >= 0.34 else "低"
        eta = m.group(4)
        qual_txt = f"{qual}，" if qual else ""
        direction_txt = "低层偏暖输送（850-925hPa）" if kind == "暖平流" else "低层偏冷输送（850-925hPa）"
        return f"{direction_txt}（{qual_txt}置信度{conf}，可能影响时间{eta}）"

    evidence_bits: list[str] = []
    line850_h = _humanize_850(line850)
    if line500 and "高空背景" not in str(line500):
        evidence_bits.append(f"500hPa: {line500}")
    if line850_h and "信号一般" not in line850_h:
        evidence_bits.append(f"850hPa: {line850_h}")
    low_cloud_pct = calc_window.get("low_cloud_pct")
    if h700_summary and h700_should_surface_in_evidence(h700_summary, low_cloud_pct=low_cloud_pct, cloud_code_now=cloud_code_now):
        if h700_is_moist_constraint(h700_summary) or h700_effective_dry_factor(h700_summary, low_cloud_pct=low_cloud_pct, cloud_code_now=cloud_code_now) >= 0.85:
            evidence_bits.append(f"700hPa: {h700_summary}")
    if h925_summary and "信号一般" not in h925_summary:
        evidence_bits.append(f"925hPa: {h925_summary}")
    key_evidence = evidence_bits[0] if evidence_bits else ""

    def _has_named_system(text: str) -> bool:
        return _contains_any(text, ["副热带高压", "季风槽", "冷锋", "暖锋", "静止锋", "锋性", "高压脊", "低压槽", "深槽", "切变"])

    def _clean_line_text(text: str) -> str:
        return str(text or "").strip().rstrip("。；，")

    def _line_priority(label: str, text: str) -> float:
        txt = str(text or "")
        if not txt:
            return 0.0
        if label == "主导机制":
            return 1.0
        if label == "层结/模式剖面":
            if _contains_any(txt, ["逆温", "稳", "湿", "干", "低云", "雾", "混合", "升温"]):
                return 0.82
            return 0.62
        if label == "相关链路":
            if _has_named_system(txt):
                return 0.92
            if _contains_any(txt, ["850-925hPa", "输送", "抬升", "锋", "边界层清除"]):
                return 0.8
            return 0.58
        if label == "影响":
            if "暂时看不出明显偏高或偏低" not in txt:
                return 0.78
            if _contains_any(txt, ["峰值时段", "直接落在峰值窗"]):
                return 0.64
            return 0.5
        return 0.0

    detail_candidates: list[tuple[float, str, str]] = []
    if layer_summary and layer_summary != regime_headline:
        layer_label = "探空层结" if sounding_mode == "obs" else "层结/模式剖面"
        clean_layer_summary = _clean_line_text(layer_summary)
        detail_candidates.append((_line_priority("层结/模式剖面", clean_layer_summary), layer_label, clean_layer_summary))
    if system_plain_desc:
        clean_system_plain_desc = _clean_line_text(system_plain_desc)
        detail_candidates.append((_line_priority("相关链路", clean_system_plain_desc), "相关链路", clean_system_plain_desc))
    if impact_line:
        clean_impact_line = _clean_line_text(impact_line)
        detail_candidates.append((_line_priority("影响", clean_impact_line), "影响", clean_impact_line))

    if not regime_headline:
        regime_headline = _fallback_regime_headline(regime_key)

    if regime_headline:
        syn_lines.append(f"- **主导机制**：{_clean_line_text(regime_headline)}。")

    detail_candidates.sort(key=lambda item: item[0], reverse=True)
    selected_details: list[tuple[str, str]] = []
    for priority, label, text in detail_candidates:
        if priority < 0.64:
            continue
        selected_details.append((label, text))
        if len(selected_details) >= 2:
            break

    if not selected_details and detail_candidates:
        _priority, label, text = detail_candidates[0]
        selected_details.append((label, text))

    strongest_priority = detail_candidates[0][0] if detail_candidates else 0.0
    if len(detail_candidates) >= 3 and strongest_priority >= 0.9 and detail_candidates[2][0] >= 0.78:
        label, text = detail_candidates[2][1], detail_candidates[2][2]
        if (label, text) not in selected_details:
            selected_details.append((label, text))

    for label, text in selected_details:
        syn_lines.append(f"- **{label}**：{text}。")

    return {
        "schema_version": "synoptic-summary.v1",
        "lines": syn_lines,
        "summary": {
            "background_driver": background_driver,
            "pathway": system_plain_desc,
            "impact": impact_line,
            "key_evidence": key_evidence,
            "trigger_hint": trigger_txt,
        },
        "primary_regime_key": r1,
        "primary_regime_label": _regime_label(r1),
        "has_primary_regime": has_primary_regime,
        "advection_direction": adv_direction,
        "advection_state": adv_state,
        "object_type": otype,
        "impact_scope": impact,
        "compact_synoptic": bool(compact_synoptic),
    }
