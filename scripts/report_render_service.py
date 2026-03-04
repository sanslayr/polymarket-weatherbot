#!/usr/bin/env python3
"""Section rendering service for /look report."""

from __future__ import annotations

import math
import re
from datetime import datetime, timedelta
from typing import Any

from market_label_policy import build_market_label_policy
from param_store import load_tmax_learning_params
from polymarket_render_service import _build_polymarket_section
from realtime_pipeline import classify_window_phase, select_realtime_triggers
from report_peak_module import _build_peak_range_module

PHASE_LABELS = {
    "far": "远离窗口",
    "near_window": "接近窗口",
    "in_window": "窗口内",
    "post": "窗口后",
    "unknown": "窗口状态未知",
}
DEFAULT_TRACK_LINE = "• 临窗前继续跟踪温度斜率与风向节奏，必要时再改判。"


def _parse_iso_dt(v: Any) -> datetime | None:
    try:
        s = str(v or "")
        return datetime.fromisoformat(s) if s else None
    except Exception:
        return None


def _coerce_same_tz(a: datetime | None, b: datetime | None) -> tuple[datetime | None, datetime | None]:
    if a is None or b is None:
        return a, b
    try:
        if a.tzinfo is not None and b.tzinfo is None:
            b = b.replace(tzinfo=a.tzinfo)
        elif a.tzinfo is None and b.tzinfo is not None:
            a = a.replace(tzinfo=b.tzinfo)
    except Exception:
        pass
    return a, b


def _hours_between(later: datetime | None, earlier: datetime | None, nonneg: bool = False) -> float | None:
    later, earlier = _coerce_same_tz(later, earlier)
    if later is None or earlier is None:
        return None
    try:
        h = (later - earlier).total_seconds() / 3600.0
    except Exception:
        return None
    if nonneg:
        return max(0.0, h)
    return h


def _hours_between_iso(later_iso: Any, earlier_iso: Any, nonneg: bool = False) -> float | None:
    return _hours_between(_parse_iso_dt(later_iso), _parse_iso_dt(earlier_iso), nonneg=nonneg)


def _hm(s: Any) -> str:
    try:
        dt = datetime.strptime(str(s), "%Y-%m-%dT%H:%M")
        return dt.strftime("%H:%M")
    except Exception:
        return str(s)



def _build_synoptic_lines(
    primary_window: dict[str, Any],
    metar_diag: dict[str, Any],
    compact_synoptic: bool,
    syn_w: dict[str, Any],
    calc_window: dict[str, Any],
    d: dict[str, Any],
    quality: dict[str, Any],
    obj: dict[str, Any],
    candidates: list[dict[str, Any]],
    cov: float | None,
    line500: str,
    line850: str,
    extra: str,
    h700_summary: str,
    h925_summary: str,
    snd_thermo: dict[str, Any],
    cloud_code_now: str,
    precip_state: str,
    precip_trend: str,
) -> list[str]:
    syn_lines = ["🧭 **环流形势对最高温影响**"]

    def _contains_any(text: str, keys: list[str]) -> bool:
        s = str(text or "")
        return any(k in s for k in keys)

    def _infer_regime_and_desc(otype: str, impact: str) -> tuple[str, str]:
        if ("front" in otype) or ("baroclinic" in otype) or _contains_any(extra + line850, ["锋", "锋生", "斜压"]):
            return "锋面活动主导", "锋区调整"
        if "dry_intrusion" in otype or _contains_any(extra, ["湿层", "低云", "封盖", "压制"]):
            return "稳定层约束主导", "低层受限"
        if _contains_any(line850, ["暖平流"]):
            return "平流主导", "暖平流抬升"
        if _contains_any(line850, ["冷平流"]):
            return "平流主导", "冷平流切入"
        if _contains_any(line500, ["槽", "抬升", "PVA", "涡度"]):
            return "动力抬升主导", "槽前触发"
        if impact == "background_only":
            return "弱信号背景", "背景噪声"
        return "混合主导", "混合扰动"

    def _candidate_groups() -> set[str]:
        gs: set[str] = set()
        for c in candidates[:4]:
            t = str((c or {}).get("type") or "").lower()
            if "advection" in t:
                gs.add("advection")
            elif "baroclinic" in t or "frontal" in t:
                gs.add("baroclinic")
            elif "dry_intrusion" in t or "subsidence" in t:
                gs.add("stability")
            elif "dynamic" in t or "trough" in t:
                gs.add("dynamic")
            elif "shear" in t:
                gs.add("shear")
        return gs

    def _interaction_note(gs: set[str]) -> str | None:
        if {"advection", "stability"}.issubset(gs):
            return "暖平流与稳定层约束并存，强度取决于云层能否持续开窗"
        if {"advection", "baroclinic"}.issubset(gs):
            return "输送与锋生叠加，主要影响峰值时段重排"
        if {"dynamic", "stability"}.issubset(gs):
            return "高空触发存在，但低层落地受约束"
        if {"baroclinic", "shear"}.issubset(gs):
            return "斜压与风切并行，局地变化节奏可能加快"
        return None

    def _regime_scores() -> dict[str, float]:
        s = {
            "advection": 0.0,
            "dynamic": 0.0,
            "stability": 0.0,
            "baroclinic": 0.0,
            "shear": 0.0,
        }

        txt850 = str(line850)
        txt500 = str(line500)
        txtx = str(extra)

        if _contains_any(txt850, ["暖平流", "冷平流", "平流"]):
            s["advection"] += 0.95
        if _contains_any(txt500, ["槽", "抬升", "PVA", "涡度"]):
            s["dynamic"] += 0.85
        if _contains_any(txtx, ["封盖", "压制", "湿层", "低云", "耦合偏弱"]):
            s["stability"] += 0.9
        if _contains_any(txtx + txt850, ["锋", "锋生", "斜压"]):
            # text-only frontal cues are useful but should not dominate without object support
            s["baroclinic"] += 0.55
            if _contains_any(txt850, ["暖平流", "冷平流"]):
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
                b_boost = conf_boost
                try:
                    dmin = float(o.get("distance_km_min") or 0.0)
                    if dmin >= 700:
                        b_boost *= 0.75
                except Exception:
                    pass
                if str(o.get("confidence") or "").lower() == "low":
                    b_boost *= 0.85
                s["baroclinic"] += b_boost
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
                wb = w
                try:
                    dmin = float(c.get("distance_km_min") or 0.0)
                    if dmin >= 700:
                        wb *= 0.8
                except Exception:
                    pass
                s["baroclinic"] += wb
            if "shear" in t:
                s["shear"] += w

        # low synoptic coverage: damp baroclinic/shear textual dominance
        try:
            if cov is not None and float(cov) < 0.65:
                s["baroclinic"] *= 0.86
                s["shear"] *= 0.9
        except Exception:
            pass

        return s

    def _regime_label(k: str) -> str:
        return {
            "advection": "平流输送",
            "dynamic": "高空动力触发",
            "stability": "低层稳定约束",
            "baroclinic": "锋面/斜压调整",
            "shear": "风切节奏扰动",
        }.get(k, k)

    def _dir_cn_from_deg(deg: float) -> str:
        dirs = ["北", "东北", "东", "东南", "南", "西南", "西", "西北"]
        idx = int(((deg % 360) + 22.5) // 45) % 8
        return dirs[idx]

    def _front_plain_desc(otype: str) -> str | None:
        is_front = ("front" in otype) or ("baroclinic" in otype) or _contains_any(str(line850) + str(extra), ["锋", "锋生", "斜压"])
        if not is_front:
            return None

        warm = "暖平流" in str(line850)
        cold = "冷平流" in str(line850)
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

        txt850 = str(line850)
        txtx = str(extra)

        if ("advection" in otype) or ("暖平流" in txt850) or ("冷平流" in txt850):
            if "暖平流" in txt850 and "冷平流" not in txt850:
                return "暖空气输送为主，云量若放开，升温会更顺"
            if "冷平流" in txt850 and "暖平流" not in txt850:
                return "冷空气输送偏强，对升温有压制"
            return "冷暖输送并存，短时更容易出现重排"

        if ("dry_intrusion" in otype) or _contains_any(txtx, ["封盖", "湿层", "低云", "压制", "干层"]):
            if _contains_any(txtx, ["干层", "日照", "升温加速"]):
                return "高层偏干，若日照打开，升温会突然加速"
            return "低层受封盖约束，短时升温不容易放大"

        if ("dynamic" in otype) or _contains_any(str(line500), ["槽", "抬升", "涡度", "PVA"]):
            return "高空有触发信号，但是否落地还要看近地风云配合"

        if ("shear" in otype):
            return "风场切换型系统，节奏变化快，峰值时段易前后摆动"

        if ("subsidence" in otype):
            return "下沉背景偏强，整体更偏稳态"

        return None

    def _sounding_factor_pack() -> dict[str, Any]:
        def _f(v: Any) -> float | None:
            try:
                return float(v)
            except Exception:
                return None

        low_cloud = _f(calc_window.get("low_cloud_pct"))
        w850 = _f(calc_window.get("w850_kmh"))
        wind_chg = _f(metar_diag.get("wind_dir_change_deg"))
        t_now = _f(metar_diag.get("latest_temp"))
        td_now = _f(metar_diag.get("latest_dewpoint"))
        wx = str(metar_diag.get("latest_wx") or "").upper()

        up_adj = 0.0
        down_adj = 0.0
        profile_score = 0.0
        tags: list[str] = []

        # 1) stability / inversion
        inv = 0.0
        if low_cloud is not None and low_cloud >= 70:
            inv += 0.6
        if w850 is not None and w850 <= 15:
            inv += 0.35
        if "耦合偏弱" in h925_summary:
            inv += 0.35
        if inv >= 0.9:
            down_adj += 0.55
            profile_score += 0.35
            tags.append("逆温/稳定约束偏强")
        elif inv >= 0.45:
            down_adj += 0.25
            profile_score += 0.25
            tags.append("低层稳定约束")

        # 2) convection
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

        # 3) phase-change / latent-cooling risk
        if any(k in wx for k in ["RA", "SN", "PL", "FZ", "DZ"]):
            if t_now is not None and -1.5 <= t_now <= 2.0:
                down_adj += 0.25
                profile_score += 0.2
                tags.append("近冰点相变/潜热冷却风险")

        # 4) moisture structure (mid-dry / upper moist hints)
        mid_dry = "干层" in h700_summary
        if mid_dry:
            profile_score += 0.45
            if cloud_code_now in {"CLR", "CAVOK", "SKC", "FEW", "SCT"}:
                up_adj += 0.45
                tags.append("中层偏干+云开（增温效率高）")
            else:
                up_adj += 0.15
                tags.append("中层偏干（但低层云仍有限制）")
        elif ("湿层" in h700_summary) or ("约束" in h700_summary):
            profile_score += 0.35
            down_adj += 0.25
            tags.append("中层湿层约束")

        # 5) shear / mixing
        if w850 is not None:
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

        return {
            "up_adj": up_adj,
            "down_adj": down_adj,
            "profile_score": profile_score,
            "tags": tags,
        }

    def _signal_scores() -> tuple[float, float, str]:
        up = 0.0
        down = 0.0

        if "暖平流" in line850:
            up += 1.0
        if "冷平流" in line850:
            down += 1.0

        if _contains_any(extra, ["封盖", "压制", "湿层", "低云"]):
            down += 1.0
        if _contains_any(extra, ["干层", "日照", "升温加速"]):
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

        # precipitation evolution effect (change > state)
        if precip_trend in {"new", "intensify"}:
            down += 0.75
        elif precip_trend in {"weaken", "end"}:
            up += 0.35
        elif precip_trend == "steady" and precip_state in {"moderate", "heavy", "convective"}:
            down += 0.45
        if precip_state == "convective":
            down += 0.25

        sf = _sounding_factor_pack()
        up += float(sf.get("up_adj") or 0.0)
        down += float(sf.get("down_adj") or 0.0)

        phase = str(classify_window_phase(primary_window, metar_diag).get("phase") or "unknown")
        return up, down, phase

    def _evidence_routes() -> tuple[str, str]:
        # system route
        sys_score = 0.0
        if obj:
            sys_score += {"high": 1.1, "medium": 0.8, "low": 0.35}.get(str(obj.get("confidence") or ""), 0.2)
            sys_score += {"station_relevant": 0.8, "possible_override": 0.45, "background_only": 0.2}.get(str(obj.get("impact_scope") or ""), 0.2)
        else:
            try:
                rmax = max(_regime_scores().values())
            except Exception:
                rmax = 0.0
            if rmax >= 0.9:
                sys_score += 0.45

        # profile route
        profile_score = 0.0
        if h700_summary:
            profile_score += 0.7
            if "近站" in h700_summary:
                profile_score += 0.45
            elif "外围" in h700_summary:
                profile_score += 0.2
        if h925_summary:
            profile_score += 0.35
            if "偏弱" in h925_summary:
                profile_score -= 0.15
        if snd_thermo.get("has_profile"):
            profile_score += 0.35
        sf = _sounding_factor_pack()
        profile_score += float(sf.get("profile_score") or 0.0)

        # obs route
        obs_score = 0.0
        try:
            tb_src = metar_diag.get("temp_bias_smooth_c") if metar_diag.get("temp_bias_smooth_c") is not None else metar_diag.get("temp_bias_c")
            tb = abs(float(tb_src or 0.0))
        except Exception:
            tb = 0.0
        try:
            ts_src = metar_diag.get("temp_trend_smooth_c") if metar_diag.get("temp_trend_smooth_c") is not None else metar_diag.get("temp_trend_1step_c")
            ts = abs(float(ts_src or 0.0))
        except Exception:
            ts = 0.0
        if tb >= 1.5:
            obs_score += 0.9
        elif tb >= 0.8:
            obs_score += 0.5
        if ts >= 0.8:
            obs_score += 0.55
        elif ts >= 0.4:
            obs_score += 0.3

        routes = [
            ("系统路由", "system", sys_score),
            ("剖面路由", "profile", profile_score),
            ("实况路由", "obs", obs_score),
        ]
        routes.sort(key=lambda x: x[2], reverse=True)
        main = f"{routes[0][0]}({routes[0][1]})"
        aux = f"{routes[1][0]}({routes[1][1]})" if routes[1][2] >= 0.45 else "无明显次级路由"
        return main, aux

    def _impact_direction_and_trigger() -> tuple[str, str]:
        up, down, phase = _signal_scores()

        if abs(up - down) < 0.55:
            direction = "暂时看不出明显偏高或偏低"
        elif up > down:
            direction = "更可能比原先预报略高"
        else:
            direction = "更可能比原先预报略低"

        if phase in {"near_window", "in_window"}:
            trigger = "临窗重点看云量开合和风向变化"
        elif phase == "far":
            trigger = "先看升温是否能连续走强"
        else:
            trigger = "重点看温度斜率与云量是否突变"

        return direction, trigger

    direction_txt, trigger_txt = _impact_direction_and_trigger()
    cgroups = _candidate_groups()
    inter_note = _interaction_note(cgroups)

    def _dominant_nature_text(rkey: str, otype: str, fallback_desc: str) -> str:
        fd = _front_plain_desc(otype)
        if rkey == "baroclinic" and fd:
            return fd
        if rkey == "advection":
            if "暖平流" in str(line850) and "冷平流" not in str(line850):
                return "暖输送主导"
            if "冷平流" in str(line850) and "暖平流" not in str(line850):
                return "冷输送主导"
            return "冷暖输送交替"
        if rkey == "stability":
            return "低层稳定约束"
        if rkey == "dynamic":
            return "高空触发主导"
        if rkey == "shear":
            return "风切重排主导"
        return fallback_desc or "背景过渡"

    rs = _regime_scores()
    r_sorted = sorted(rs.items(), key=lambda x: x[1], reverse=True)
    r1, s1 = r_sorted[0]
    r2, s2 = r_sorted[1]
    has_primary_regime = s1 >= 0.9

    if obj:
        otype = str(obj.get("type") or "").lower()
        impact = str(obj.get("impact_scope") or "background_only")
        regime, desc = _infer_regime_and_desc(otype, impact)

        # 1) 主导系统（一句话，含性质）
        if has_primary_regime:
            nature_txt = _dominant_nature_text(r1, otype, regime)
            syn_lines.append(f"- **主导系统**：{_regime_label(r1)}（{nature_txt}）。")
        else:
            nature_txt = _dominant_nature_text("baroclinic" if ("baroclinic" in otype or "front" in otype) else "mixed", otype, desc)
            syn_lines.append(f"- **主导系统**：{regime}（{nature_txt}）。")

        # 2) 落地影响（方向 + 触发 + 交互）
        if impact == "station_relevant":
            scope_txt = "系统近站，影响将直接落在峰值窗"
        elif impact == "possible_override":
            scope_txt = "系统在外围，主要改写峰值时段"
        else:
            scope_txt = "当前以背景场为主，短时改写概率有限"

        impact_line = f"{direction_txt}；{scope_txt}"
        if inter_note:
            impact_line += f"。当前组合关系：{inter_note}"
        impact_line += f"。建议：{trigger_txt}。"
        syn_lines.append(f"- **落地影响**：{impact_line}")

    else:
        if has_primary_regime:
            nature_txt = _dominant_nature_text(r1, "", "结构未闭合")
            syn_lines.append(f"- **主导系统**：{_regime_label(r1)}（{nature_txt}；结构未闭合，暂不立3D主系统）。")
        else:
            syn_lines.append("- **主导系统**：当前未识别到可稳定追踪的同一套分层系统。")

        tail = f"。当前组合关系：{inter_note}" if inter_note else ""
        syn_lines.append(f"- **落地影响**：{direction_txt}；短时以实况触发为主。建议：{trigger_txt}{tail}。")

    # concise evidence line (avoid spreading full layer-by-layer by default)
    def _humanize_850(s: str) -> str:
        txt = str(s or "")
        m = re.search(r"(暖平流|冷平流)([^（]*)（([0-9.]+)，([^）]+)）", txt)
        if m:
            kind = m.group(1)
            conf_raw = float(m.group(3))
            if conf_raw >= 0.67:
                conf = "高"
            elif conf_raw >= 0.34:
                conf = "中"
            else:
                conf = "低"
            eta = m.group(4)
            return f"{kind}（置信度{conf}，可能影响时间{eta}）"
        return txt

    line850_h = _humanize_850(line850)

    def _is_weak_evidence(s: str) -> bool:
        t = str(s or "")
        weak_tokens = ["信号一般", "信号有限", "中性", "背景", "不明", "弱"]
        return any(k in t for k in weak_tokens)

    def _h700_dist_km(s: str) -> float | None:
        t = str(s or "")
        m = re.search(r"约\s*([0-9]+(?:\.[0-9]+)?)\s*km", t)
        if not m:
            return None
        try:
            return float(m.group(1))
        except Exception:
            return None

    def _is_generic_500(s: str) -> bool:
        t = str(s or "")
        generic_tokens = [
            "高空仍有抬升触发条件",
            "云层若放开更易再冲高",
            "高空背景信号有限",
            "高空背景一般",
        ]
        return any(k in t for k in generic_tokens)

    evidence_bits: list[str] = []
    if line850_h and not _is_weak_evidence(line850_h):
        evidence_bits.append(f"850hPa: {line850_h}")

    if h700_summary and not _is_weak_evidence(h700_summary):
        d700 = _h700_dist_km(h700_summary)
        h700_key = ("近站" in h700_summary) or ((d700 is not None) and (d700 <= 360)) or ("湿层" in h700_summary) or ("约束" in h700_summary)
        if h700_key:
            evidence_bits.append(f"700hPa: {h700_summary}")

    if line500 and (not _is_weak_evidence(line500)) and (not _is_generic_500(line500)):
        strong500 = any(k in str(line500) for k in ["槽", "短波", "涡度", "PVA", "急流", "冷涡"])
        if strong500:
            evidence_bits.append(f"500hPa: {line500}")

    if h925_summary and not _is_weak_evidence(h925_summary):
        evidence_bits.append(f"925hPa: {h925_summary}")

    if extra and not _is_weak_evidence(extra):
        evidence_bits.append(f"约束: {extra}")

    if evidence_bits:
        if len(evidence_bits) == 1:
            syn_lines.append(f"- **关键证据**：{evidence_bits[0]}。")
        else:
            syn_lines.append("- **关键证据**：")
            for e in evidence_bits[:3]:
                syn_lines.append(f"  • {e}")

    def _sounding_layer_note() -> str | None:
        bits: list[str] = []
        sf = _sounding_factor_pack()

        if h700_summary:
            if "干层" in h700_summary:
                bits.append("中层(600-700hPa)偏干")
            elif ("湿层" in h700_summary) or ("约束" in h700_summary):
                bits.append("中层(700hPa)湿层约束")

        if snd_thermo.get("has_profile"):
            capev = snd_thermo.get("sbcape_jkg") or snd_thermo.get("mlcape_jkg") or snd_thermo.get("mucape_jkg")
            cinv = snd_thermo.get("sbcin_jkg") if snd_thermo.get("sbcin_jkg") is not None else snd_thermo.get("mlcin_jkg")
            if isinstance(capev, (int, float)):
                bits.append(f"对流能量 CAPE≈{float(capev):.0f}J/kg")
            if isinstance(cinv, (int, float)):
                bits.append(f"抑制 CIN≈{float(cinv):.0f}J/kg")

        tags = [str(x) for x in (sf.get("tags") or [])]
        for t in tags:
            if t not in bits:
                bits.append(t)

        if cloud_code_now in {"BKN", "OVC", "VV"}:
            bits.append("当前低层云量偏多（地面辐射受限）")

        if not bits:
            return None
        return "；".join(bits[:3]) + "。"

    if str(d.get("override_risk") or "low") == "high":
        syn_lines.append("- **改写风险**：中到高，窗口前后需盯实况触发。")

    if str(quality.get("source_state") or "") == "degraded":
        cov_txt = ""
        try:
            if quality.get("synoptic_coverage") is not None:
                cov_txt = f"；coverage={float(quality.get('synoptic_coverage')):.2f}"
        except Exception:
            cov_txt = ""
        syn_lines.append(f"- **数据状态**：环流链路降级（结论偏保守{cov_txt}）。")

    phase_for_syn = str(classify_window_phase(primary_window, metar_diag).get("phase") or "unknown")
    post_mode = str(metar_diag.get("post_window_mode") or "")
    syn_win_label = "峰值窗口"
    if phase_for_syn == "post" and bool(metar_diag.get("post_focus_window_active")):
        syn_win_label = "潜在反超窗口" if post_mode != "no_rebreak_eval" else "后段验证窗口"
    syn_lines.append(
        f"- **{syn_win_label}**：{_hm(syn_w.get('start_local'))}~{_hm(syn_w.get('end_local'))} Local。"
    )

    if compact_synoptic:
        short_cue = "以实况触发为主"
        if "暖平流" in line850 and "冷平流" not in line850:
            short_cue = "暖平流对上沿仍有支撑"
        elif "冷平流" in line850:
            short_cue = "冷平流对上沿有抑制"
        elif "干层" in h700_summary:
            short_cue = "中层偏干有利白天增温"

        # expose thermal-balance/window-prior constraints in human wording
        thermal_txt = ""
        try:
            ph = int(str(primary_window.get("peak_local") or "")[11:13])
        except Exception:
            ph = -1
        try:
            lowc = float(calc_window.get("low_cloud_pct") or 0.0)
        except Exception:
            lowc = 0.0
        try:
            w850 = float(calc_window.get("w850_kmh") or 0.0)
        except Exception:
            w850 = 0.0

        if 13 <= ph <= 15:
            thermal_txt = "热力节律仍指向午后峰值"
        elif ph >= 16:
            thermal_txt = "峰值相位偏后，需看风场/云量是否继续支撑"
        elif 0 <= ph <= 11:
            thermal_txt = "峰值相位偏早，需警惕平流主导改写"

        if lowc >= 75:
            thermal_txt = (thermal_txt + "，低云压制仍在") if thermal_txt else "低云压制仍在"
        elif lowc <= 25 and thermal_txt:
            thermal_txt = thermal_txt + "，辐射效率相对较高"

        if w850 >= 38 and thermal_txt:
            thermal_txt = thermal_txt + "，强风混合使节奏更易重排"

        precip_tail = ""
        if precip_trend in {"new", "intensify"}:
            precip_tail = "；降水正在增强，短时压温风险抬升"
        elif precip_state in {"moderate", "heavy", "convective"}:
            precip_tail = "；降水仍在，白天增温效率受抑"

        tail = f"；{thermal_txt}" if thermal_txt else ""
        syn_lines = [
            "🧭 **环流形势对最高温影响**",
            f"- {direction_txt}；{short_cue}，{trigger_txt}{tail}{precip_tail}。",
        ]

    return syn_lines


def _build_vars_and_market_blocks(
    primary_window: dict[str, Any],
    polymarket_event_url: str,
    metar_diag: dict[str, Any],
    metar_block: str,
    quality: dict[str, Any],
    obj: dict[str, Any],
    low_conf_far: bool,
    phase_now: str,
    obs_max: float | None,
    obs_floor: float | None,
    obs_ceil: float | None,
    compact_settled_mode: bool,
    cloud_code: str,
    line850: str,
    snd_thermo: dict[str, Any],
    precip_state: str,
    precip_trend: str,
    rt_rad_low: float,
    rt_rad_recover: float,
    rt_rad_recover_tr: float,
    t_cons: float,
    b_cons: float,
    t_tr: Any,
    t_bias: Any,
    gate: dict[str, Any],
    disp_lo: float,
    disp_hi: float,
    core_lo: float,
    core_hi: float,
    fmt_range,
    fmt_temp,
    polymarket_prefetched_event: tuple[bool, list[dict[str, Any]]] | None,
) -> tuple[list[str], str, str]:
    vars_block = [f"⚠️ **关注变量**（{PHASE_LABELS.get(phase_now, PHASE_LABELS['unknown'])}）"]
    obs_analysis_lines: list[str] = []

    t_bias = metar_diag.get("temp_bias_smooth_c") if metar_diag.get("temp_bias_smooth_c") is not None else metar_diag.get("temp_bias_c")
    t_tr = metar_diag.get("temp_trend_smooth_c") if metar_diag.get("temp_trend_smooth_c") is not None else metar_diag.get("temp_trend_1step_c")
    cloud_tr = str(metar_diag.get("cloud_trend") or "")
    focus: list[tuple[float, str]] = []

    def _fallback_vars(exc: Exception | None = None) -> list[str]:
        if exc is not None and str(os.getenv("LOOK_DEBUG_ERRORS", "0") or "0").lower() in {"1", "true", "yes", "on"}:
            return [f"⚠️ **关注变量**（{PHASE_LABELS.get(phase_now, PHASE_LABELS['unknown'])}）", f"• 变量块调试：{type(exc).__name__}: {exc}"]
        return [f"⚠️ **关注变量**（{PHASE_LABELS.get(phase_now, PHASE_LABELS['unknown'])}）", DEFAULT_TRACK_LINE]

    def _trend_horizon_phrase() -> str:
        try:
            cad = float(metar_diag.get("metar_routine_cadence_min")) if metar_diag.get("metar_routine_cadence_min") is not None else None
        except Exception:
            cad = None
        try:
            recent = float(metar_diag.get("metar_recent_interval_min")) if metar_diag.get("metar_recent_interval_min") is not None else None
        except Exception:
            recent = None
        speci = bool(metar_diag.get("metar_speci_active"))
        speci_likely = bool(metar_diag.get("metar_speci_likely"))

        base = None
        if recent is not None and 8.0 <= recent <= 90.0:
            base = recent
        elif cad is not None and 15.0 <= cad <= 90.0:
            base = cad
        else:
            base = 45.0

        if speci:
            lo = max(10, int(round(max(10.0, base * 0.45) / 5.0) * 5))
            hi = max(lo + 10, int(round(min(45.0, base * 1.10) / 5.0) * 5))
        elif speci_likely:
            lo = max(15, int(round(max(15.0, base * 0.55) / 5.0) * 5))
            hi = max(lo + 10, int(round(min(55.0, base * 1.20) / 5.0) * 5))
        elif base <= 35.0:
            lo, hi = 20, 40
        elif base <= 50.0:
            lo, hi = 25, 50
        else:
            lo, hi = 35, 70
        return f"未来{lo}-{hi}分钟"

    trend_horizon = _trend_horizon_phrase()

    cadence_line = None
    try:
        cad_show = float(metar_diag.get("metar_routine_cadence_min")) if metar_diag.get("metar_routine_cadence_min") is not None else None
    except Exception:
        cad_show = None
    if cad_show is not None and 15.0 <= cad_show <= 90.0 and phase_now == "far":
        cad_round = int(round(cad_show / 5.0) * 5)
        cadence_line = f"• 该站常规约每{cad_round}分钟一报，窗口前优先看连续两报斜率是否同向。"

    next_key_report_txt = None
    try:
        cad_min = float(metar_diag.get("metar_routine_cadence_min")) if metar_diag.get("metar_routine_cadence_min") is not None else None
    except Exception:
        cad_min = None
    try:
        latest_local_txt = str(metar_diag.get("latest_report_local") or "")
        latest_local_dt2 = datetime.fromisoformat(latest_local_txt) if latest_local_txt else None
    except Exception:
        latest_local_dt2 = None
    if cad_min is not None and cad_min >= 50 and latest_local_dt2 is not None:
        try:
            nxt = latest_local_dt2 + timedelta(minutes=int(round(cad_min)))
            next_key_report_txt = nxt.strftime("%H:%M")
        except Exception:
            next_key_report_txt = None

    key_report_slots: list[str] = []
    try:
        win_start_dt = datetime.fromisoformat(str(primary_window.get("start_local") or "")) if primary_window.get("start_local") else None
        win_end_dt = datetime.fromisoformat(str(primary_window.get("end_local") or "")) if primary_window.get("end_local") else None
    except Exception:
        win_start_dt = None
        win_end_dt = None
    if (
        cad_min is not None
        and 20.0 <= cad_min <= 90.0
        and latest_local_dt2 is not None
        and phase_now in {"near_window", "in_window", "post"}
        and (not bool(metar_diag.get("metar_speci_active")))
    ):
        try:
            step = int(round(cad_min))
            dt = latest_local_dt2 + timedelta(minutes=step)
            horizon_end = win_end_dt if win_end_dt is not None else (latest_local_dt2 + timedelta(hours=3))
            while dt <= (horizon_end + timedelta(minutes=5)) and len(key_report_slots) < 3:
                if win_start_dt is None or dt >= (win_start_dt - timedelta(minutes=20)):
                    key_report_slots.append(dt.strftime("%H:%M"))
                dt += timedelta(minutes=step)
        except Exception:
            key_report_slots = []

    # 温度趋势（优先）
    try:
        tv = float(t_tr or 0.0)
        if tv >= 0.6:
            if phase_now == "far":
                focus.append((1.0, "• 窗口前若后续1-2次常规报仍维持正斜率 → 最高温上沿保留上修空间。"))
            else:
                focus.append((1.0, f"• {trend_horizon}升温斜率若继续维持正值 → 最高温上沿仍可上修。"))
        elif tv <= -0.6:
            if phase_now == "far":
                focus.append((1.0, "• 窗口前若后续1-2次常规报持续转负 → 峰值可能提前锁定并压低上沿。"))
            else:
                focus.append((1.0, f"• {trend_horizon}斜率若持续转负 → 峰值可能提前锁定并压低上沿。"))
        else:
            focus.append((0.55, "• 先盯温度斜率是否重新放大，这是临窗改判的最快信号。"))
    except Exception:
        focus.append((0.45, "• 先盯温度斜率是否重新放大，这是临窗改判的最快信号。"))

    if cadence_line:
        focus.append((0.86, cadence_line))

    if key_report_slots:
        slot_txt = " / ".join(key_report_slots[:2])
        if phase_now == "post" and obs_max is not None:
            focus.append((0.95, f"• 关键发报点（{slot_txt} Local）：若连续报维持横盘/回落，高点基本锁定；仅在斜率再放大并伴随风云重排时才重开上修。"))
        else:
            focus.append((0.95, f"• 关键发报点（{slot_txt} Local）：这些报点对“是否封顶/是否上修”影响最大，建议优先盯。"))
    elif next_key_report_txt and phase_now in {"near_window", "in_window", "post"} and (not bool(metar_diag.get("metar_speci_active"))):
        if phase_now == "post" and obs_max is not None:
            focus.append((0.93, f"• 重点看 {next_key_report_txt} Local：若继续横盘/回落，高点基本锁定；仅当温度重新抬升并伴随风云再配合，才可能改写前高。"))
        else:
            focus.append((0.93, f"• 下一关键报约 {next_key_report_txt} Local（按该站常规发报节律推算；该报点对是否封顶更关键）。"))

    try:
        tv_s = float(t_tr or 0.0)
    except Exception:
        tv_s = 0.0
    try:
        bv_s = float(t_bias or 0.0)
    except Exception:
        bv_s = 0.0
    try:
        wd_s = float(metar_diag.get("wind_dir_change_deg") or 0.0)
    except Exception:
        wd_s = 0.0
    quiet_baseline = bool(
        abs(tv_s) <= 0.35
        and abs(bv_s) <= 0.9
        and wd_s <= 20.0
        and cloud_tr not in {"回补", "开窗", "增加", "减弱"}
        and precip_trend not in {"new", "intensify", "weaken", "end"}
        and precip_state in {"none", "light"}
    )

    if bool(metar_diag.get("metar_speci_active")) and phase_now in {"near_window", "in_window", "post"}:
        focus.append((0.98, "• 当前已进入 SPECI 加密报阶段：窗口判断以最新加密报为准，不再按常规发报节律等待。"))
    elif bool(metar_diag.get("metar_speci_likely")):
        if phase_now in {"near_window", "in_window", "post"}:
            if quiet_baseline:
                focus.append((0.92, "• 平稳背景下若突发 SPECI，通常对应温度/风云结构突变，可能直接改写窗口期封顶判断。"))
            else:
                focus.append((0.9, "• 窗口期异常信号增多，下一报可能转为 SPECI 加密更新；一旦触发，优先按加密报重估上沿。"))
        else:
            focus.append((0.78, "• 异常信号增多，临窗前需防 SPECI 加密报触发；若触发再提高改判频率。"))

    if bool(metar_diag.get("diurnal_uplift_applied")) and phase_now == "far":
        focus.append((0.84, "• 在当前环流仍偏辐射主导的前提下，晴空日振幅常高于模式，白天升温可能强于当前曲线。"))

    # 偏差驱动
    if isinstance(t_bias, (int, float)):
        if t_bias >= 1.5:
            focus.append((0.95, "• 实况持续高于同小时模式（偏暖延续） → 最高温更偏上沿。"))
        elif t_bias <= -1.5:
            focus.append((0.95, "• 实况持续低于同小时模式（偏冷延续） → 最高温更偏下沿。"))

    # 晴空辐射日圆弧顶信号（防惯性高估）
    if bool(metar_diag.get("rounded_top_cap_applied")):
        focus.append((1.02, "• 实况斜率已走平/转弱（圆弧顶特征）→ 上沿再上修空间有限，优先防高估。"))

    # 窗口已过后的惯性上冲抑制信号
    if bool(metar_diag.get("late_end_cap_applied")):
        focus.append((1.01, "• 峰值窗已过且斜率未再放大 → 继续上冲概率偏低。"))

    # 夜间增温复核：暖平流+混合/云被/露点/气压组合触发时，保留小幅回升可能
    if bool(metar_diag.get("nocturnal_reheat_signal")):
        rs = str(metar_diag.get("nocturnal_reheat_reasons") or "暖平流与边界层信号")
        focus.append((0.90, f"• 夜间增温触发（{rs}）→ 后段仍有小幅回升可能。"))

    # 基于METAR多层云量 + 天气现象的有效辐射因子（0~1）
    if phase_now in {"near_window", "in_window"}:
        try:
            rad_eff_focus = float(metar_diag.get("radiation_eff_smooth")) if metar_diag.get("radiation_eff_smooth") is not None else None
        except Exception:
            rad_eff_focus = None
        try:
            rad_tr_focus = float(metar_diag.get("radiation_eff_trend_1step")) if metar_diag.get("radiation_eff_trend_1step") is not None else None
        except Exception:
            rad_tr_focus = None

        low_cut = max(0.45, rt_rad_low - 0.05)
        low_tr_cut = max(0.008, 0.4 * rt_rad_recover_tr)
        if rad_eff_focus is not None and rad_eff_focus <= low_cut and (rad_tr_focus is None or rad_tr_focus <= low_tr_cut):
            focus.append((0.98, "• 有效辐射偏弱且未回升（云层/天气现象综合）→ 末段冲高空间受限。"))
        elif rad_eff_focus is not None and rad_eff_focus >= rt_rad_recover and (rad_tr_focus is not None and rad_tr_focus >= max(0.02, rt_rad_recover_tr * 0.8)):
            focus.append((0.72, "• 有效辐射仍在回升（云层开窗占优）→ 尾段仍保留小幅冲高可能。"))

    # 风场重排（给出具体方向场景）
    try:
        wdir = metar_diag.get("latest_wdir")
        wspd = metar_diag.get("latest_wspd")
        wdchg = float(metar_diag.get("wind_dir_change_deg") or 0.0)
        st_lat = float(metar_diag.get("station_lat") or 0.0)
        nh = st_lat >= 0
        warm_sector = "偏南到西南" if nh else "偏北到西北"
        cool_sector = "偏北到东北" if nh else "偏南到东南"

        if wdir not in (None, "", "VRB") and wspd is not None:
            try:
                ws = float(wspd)
                wind_gate = int(max(14.0, round(ws + 3.0)))
            except Exception:
                ws = None
                wind_gate = 15

            sc = 0.95 if wdchg >= 35 else 0.72
            if "冷平流" in line850:
                txt = (
                    f"• 风场改判阈值：若转{cool_sector}并增至≈{wind_gate}kt以上，冷输送压温会更明显；"
                    f"若回摆到{warm_sector}且风速回落，才可能释放小幅反超空间（当前{wdir}° {wspd}kt）。"
                )
            elif "暖平流" in line850:
                txt = (
                    f"• 风场改判阈值：若转{warm_sector}并增至≈{wind_gate}kt以上，暖输送更易落地；"
                    f"若转{cool_sector}并增强，后段上沿会被压住（当前{wdir}° {wspd}kt）。"
                )
            else:
                txt = (
                    f"• 风场改判阈值：若转{cool_sector}并增至≈{wind_gate}kt以上，多偏压温；"
                    f"若转{warm_sector}且维持正斜率，才有后段上修空间（当前{wdir}° {wspd}kt）。"
                )
            focus.append((sc, txt))
        else:
            focus.append((0.45, "• 近地风场若由不定转为稳定单一来流：偏冷象限通常压温，偏暖象限才支持后段反超。"))
    except Exception:
        focus.append((0.45, "• 近地风向/风速若突变 → 峰值出现时段与幅度可能改写。"))

    # 云量只在“有信号”时上提，不再每次默认主重点
    if cloud_code in {"BKN", "OVC", "VV"}:
        focus.append((0.95, "• 低云维持/继续增厚 → 最高温上沿下压，峰值可能提前结束。"))
    elif ("回补" in cloud_tr) or ("增加" in cloud_tr):
        focus.append((0.85, "• 云量回补迹象增强 → 临窗压制风险抬升。"))
    elif ("开窗" in cloud_tr) or ("减弱" in cloud_tr):
        focus.append((0.7, "• 云量转疏在延续 → 地面增温效率仍有支撑。"))

    # precipitation evolution: prioritize change signal
    if bool(metar_diag.get("precip_warm_relief_day")) and precip_trend in {"new", "intensify", "steady"}:
        focus.append((0.74, "• 轻降水叠加暖平流信号 → 压温效应可能弱于常规降水情景，仍以小幅扰动为主。"))
    elif precip_trend in {"new", "intensify"}:
        focus.append((1.08, "• 降水出现/增强 → 上沿偏下修，且短时不确定性增大（对降水强度/相态变化敏感）。"))
    elif precip_trend in {"weaken", "end"}:
        focus.append((0.82, "• 降水减弱/结束 → 压温约束减轻，若云层不回补上沿可恢复。"))
    elif precip_state in {"moderate", "heavy", "convective"}:
        focus.append((0.9, "• 降水持续 → 白天增温效率受抑，最高温更偏下沿；短时波动可能放大。"))


    try:
        if snd_thermo.get("has_profile"):
            capev = snd_thermo.get("sbcape_jkg") or snd_thermo.get("mlcape_jkg") or snd_thermo.get("mucape_jkg")
            cinv = snd_thermo.get("sbcin_jkg") if snd_thermo.get("sbcin_jkg") is not None else snd_thermo.get("mlcin_jkg")
            if isinstance(capev, (int, float)) and capev >= 300 and (not isinstance(cinv, (int, float)) or cinv > -75):
                focus.append((0.8, "• 探空显示可用对流能量且抑制偏弱 → 午后云量/阵性扰动上升，峰值波动风险增加。"))
            elif isinstance(cinv, (int, float)) and cinv <= -125:
                focus.append((0.6, "• 探空抑制偏强（CIN较大） → 对流触发受限，升温路径更看近地风场。"))
    except Exception:
        pass

    # P1 short-term triggers (window-gated)
    try:
        rt_triggers = select_realtime_triggers(primary_window, metar_diag)
        if next_key_report_txt and phase_now in {"near_window", "in_window", "post"} and (not bool(metar_diag.get("metar_speci_active"))):
            if cad_min is not None and 20.0 <= cad_min <= 90.0:
                cad_txt = f"约每{int(round(cad_min / 5.0) * 5)}分钟"
            else:
                cad_txt = "按本站常规节律"
            key_line = f"• 该站常规发报{cad_txt}，下一关键报约 {next_key_report_txt} Local（对是否封顶更关键）。"
            if key_line not in rt_triggers:
                rt_triggers = [key_line] + list(rt_triggers)
        phase_now = str(gate.get("phase") or "unknown")
        focus_sorted = [txt for _s, txt in sorted(focus, key=lambda x: x[0], reverse=True)]
        # 去重保序
        uniq_focus: list[str] = []
        for t in focus_sorted:
            if t not in uniq_focus:
                uniq_focus.append(t)

        def _focus_category(text: str) -> str:
            s = str(text or "")
            if ("下一关键报" in s) or ("下一报" in s) or ("SPECI" in s) or ("加密更新" in s):
                return "window"
            if ("偏暖延续" in s) or ("偏冷延续" in s) or ("斜率" in s) or ("锁定" in s) or ("上沿" in s):
                return "temp"
            if ("风场" in s) or ("风向" in s) or ("风速" in s) or ("来流" in s):
                return "wind"
            if ("辐射" in s) or ("云" in s):
                return "cloud"
            if "降水" in s:
                return "precip"
            if ("夜间增温" in s) or ("反超" in s):
                return "rebreak"
            return "other"

        def _pick_by_phase(lines: list[str], phase: str, limit: int) -> list[str]:
            if not lines:
                return []
            phase_pref = {
                "far": ["temp", "wind", "cloud", "precip", "window", "other"],
                "near_window": ["temp", "wind", "cloud", "precip", "window", "other"],
                "in_window": ["temp", "cloud", "wind", "precip", "window", "other"],
                "post": ["window", "temp", "wind", "cloud", "rebreak", "precip", "other"],
            }
            pref = phase_pref.get(phase, ["temp", "wind", "cloud", "precip", "window", "other"])
            picked: list[str] = []
            used = set()
            for cat in pref:
                for line in lines:
                    if line in used:
                        continue
                    if _focus_category(line) == cat:
                        picked.append(line)
                        used.add(line)
                        break
                if len(picked) >= limit:
                    return picked
            for line in lines:
                if line in used:
                    continue
                picked.append(line)
                used.add(line)
                if len(picked) >= limit:
                    break
            return picked

        merged_all: list[str] = []
        for t in (list(rt_triggers) + list(uniq_focus)):
            if t not in merged_all:
                merged_all.append(t)

        if phase_now == "far":
            merged_all = [x for x in merged_all if ("远离峰值窗口" not in x) and ("临窗前继续跟踪" not in x)]
            try:
                tv_far = float(t_tr or 0.0)
            except Exception:
                tv_far = 0.0
            try:
                bv_far = float(t_bias or 0.0)
            except Exception:
                bv_far = 0.0
            try:
                wd_far = float(metar_diag.get("wind_dir_change_deg") or 0.0)
            except Exception:
                wd_far = 0.0
            active_far = (
                abs(tv_far) >= 0.6
                or abs(bv_far) >= 1.2
                or wd_far >= 35
                or ("回补" in cloud_tr)
                or ("开窗" in cloud_tr)
                or precip_trend in {"new", "intensify", "weaken", "end"}
            )
            vars_lines = _pick_by_phase(merged_all, phase_now, 2 if active_far else 1)
        else:
            vars_lines = _pick_by_phase(merged_all, phase_now, 3)

        vars_block = vars_block[:1] + vars_lines
        if len(vars_block) == 1:
            vars_block.append(DEFAULT_TRACK_LINE)
    except Exception as _e:
        vars_block = _fallback_vars(_e)

    if compact_settled_mode and obs_max is not None:
        if obs_floor is not None and obs_ceil is not None and (obs_ceil - obs_floor) >= 0.30:
            anchor_txt = fmt_range(float(obs_floor), float(obs_ceil))
        else:
            anchor_txt = fmt_temp(float(obs_max))

        # Move realized-state interpretation into METAR analysis block (not variable block).
        obs_analysis_lines.append(f"• 峰值窗基本已过，最高温大概率在已观测高点附近收敛（当前锚点 {anchor_txt}）。")
        if bool(metar_diag.get("decisive_hourly_report")):
            try:
                key_dt = datetime.fromisoformat(str(metar_diag.get("latest_report_local") or ""))
                key_txt = key_dt.strftime("%H:%M")
            except Exception:
                key_txt = "本轮"
            obs_analysis_lines.append(f"• 该站小时关键报（{key_txt} Local）已给出平稳信号，后续再创新高难度上升。")

        vars_block = [
            "⚠️ **关注变量**（窗口后）",
        ]
        if next_key_report_txt:
            if (not bool(metar_diag.get("metar_speci_active"))) and (not bool(metar_diag.get("metar_speci_likely"))):
                vars_block.append(f"• 重点看 {next_key_report_txt} Local：温度是否维持横盘/回落（若是，则高点基本锁定）。")
            else:
                vars_block.append(f"• 重点看 {next_key_report_txt} Local：若斜率再放大并伴随风云重排，才可能重开新高窗口。")
        else:
            vars_block.append("• 下一报重点看温度斜率是否再转正，以及风向/云量是否出现相变。")
        vars_block.append("• 关注风向风速是否转入更有利增温的象限并增强。")
        vars_block.append("• 关注云量是否继续转疏或再回补（将直接影响末段上冲空间）。")

    label_policy_cfg = {}
    try:
        label_policy_cfg = dict((load_tmax_learning_params().get("market_labels") or {}))
    except Exception:
        label_policy_cfg = {}
    label_policy = build_market_label_policy(
        quality=quality,
        obj=obj,
        low_conf_far=low_conf_far,
        phase_now=phase_now,
        metar_diag=metar_diag,
        t_cons=float(t_cons),
        b_cons=float(b_cons),
        compact_settled_mode=compact_settled_mode,
        policy_params=label_policy_cfg,
    )

    if obs_analysis_lines:
        metar_block = metar_block + "\n\n**实况分析**\n" + "\n".join(obs_analysis_lines)

    poly_block = ""
    market_weather_anchor = {
        "latest_temp_c": metar_diag.get("latest_temp"),
        "observed_max_temp_c": metar_diag.get("observed_max_temp_c"),
    }
    try:
        poly_block = _build_polymarket_section(
            polymarket_event_url,
            primary_window,
            weather_anchor=market_weather_anchor,
            range_hint={
                "display_lo": float(disp_lo),
                "display_hi": float(disp_hi),
                "core_lo": float(core_lo),
                "core_hi": float(core_hi),
            },
            allow_best_label=bool(label_policy.get("allow_best_label", True)),
            allow_alpha_label=bool(label_policy.get("allow_alpha_label", True)),
            label_policy=label_policy,
            prefetched_event=polymarket_prefetched_event,
        )
        # if market is unavailable (no event / no tradable ladder), omit whole section
        if str(poly_block).startswith("Polymarket："):
            poly_block = ""
    except Exception:
        poly_block = ""

    return vars_block, metar_block, poly_block


def _build_condition_state(
    primary_window: dict[str, Any],
    metar_diag: dict[str, Any],
    forecast_decision: dict[str, Any] | None,
    synoptic_window: dict[str, Any] | None,
) -> dict[str, Any]:
    fdec = forecast_decision if isinstance(forecast_decision, dict) else {}
    d = (fdec.get("decision") or {}) if isinstance(fdec, dict) else {}
    bg = (d.get("background") or fdec.get("background") or {}) if isinstance(fdec, dict) else {}
    quality = (fdec.get("quality") or {}) if isinstance(fdec, dict) else {}

    meta_window = (((fdec.get("meta") or {}).get("window")) if isinstance(fdec, dict) else {}) or {}
    if isinstance(synoptic_window, dict) and synoptic_window.get("start_local") and synoptic_window.get("end_local"):
        syn_w = synoptic_window
    elif isinstance(meta_window, dict) and meta_window.get("start_local") and meta_window.get("end_local"):
        syn_w = meta_window
    else:
        syn_w = primary_window

    post_focus_active = bool(metar_diag.get("post_focus_window_active"))
    calc_window = syn_w if post_focus_active else primary_window

    line500 = str(bg.get("line_500") or "高空背景信号有限。")
    line850 = str(bg.get("line_850") or "低层输送信号一般。")
    extra = str(bg.get("extra") or "")
    h700_summary = str((((fdec.get("features") or {}).get("h700") or {}).get("summary") if isinstance(fdec, dict) else "") or "")
    h925_summary = str((((fdec.get("features") or {}).get("h925") or {}).get("summary") if isinstance(fdec, dict) else "") or "")
    snd_thermo = ((((fdec.get("features") or {}).get("sounding") or {}).get("thermo") if isinstance(fdec, dict) else None) or {})
    cloud_code_now = str(metar_diag.get("latest_cloud_code") or "").upper()
    precip_state = str(metar_diag.get("latest_precip_state") or "none").lower()
    precip_trend = str(metar_diag.get("precip_trend") or "none").lower()
    candidates = (((fdec.get("features") or {}).get("objects_3d") or {}).get("candidates") or []) if isinstance(fdec, dict) else []

    try:
        cov = float((quality or {}).get("synoptic_coverage")) if (quality or {}).get("synoptic_coverage") is not None else None
    except Exception:
        cov = None

    def _conf_ord(x: str) -> int:
        return {"high": 3, "medium": 2, "low": 1}.get(str(x or "").lower(), 0)

    raw_obj = (d.get("object_3d_main") or {}) if isinstance(d, dict) else {}
    obj = dict(raw_obj) if isinstance(raw_obj, dict) else {}
    if cov is not None and cov < 0.5:
        obj = {}
    elif obj and _conf_ord(obj.get("confidence")) <= 1:
        alt = None
        for c in candidates:
            if not isinstance(c, dict):
                continue
            if _conf_ord(c.get("confidence")) >= 2:
                alt = c
                break
        if alt is not None:
            obj = dict(alt)
            obj["_promoted_from_candidate"] = True
        else:
            obj = {}

    return {
        "fdec": fdec,
        "d": d,
        "quality": quality,
        "syn_w": syn_w,
        "calc_window": calc_window,
        "line500": line500,
        "line850": line850,
        "extra": extra,
        "h700_summary": h700_summary,
        "h925_summary": h925_summary,
        "snd_thermo": snd_thermo,
        "cloud_code_now": cloud_code_now,
        "precip_state": precip_state,
        "precip_trend": precip_trend,
        "candidates": candidates,
        "cov": cov,
        "obj": obj,
    }


def _build_metar_block(
    metar_diag: dict[str, Any],
    metar_text: str,
    unit: str,
    fmt_temp,
) -> str:
    metar_prefix: list[str] = []
    try:
        if metar_diag and metar_diag.get("observed_max_temp_c") is not None:
            mx = float(metar_diag.get("observed_max_temp_c"))
            if unit == "C":
                mx_txt = f"{int(round(mx))}°C" if abs(mx - round(mx)) < 0.05 else f"{mx:.1f}°C"
            else:
                mx_txt = fmt_temp(mx)
            tmax_local = str(metar_diag.get("observed_max_time_local") or "")
            tmax_txt = ""
            if tmax_local:
                try:
                    tmax_txt = datetime.fromisoformat(tmax_local).strftime("%H:%M Local")
                except Exception:
                    tmax_txt = ""
            if tmax_txt:
                metar_prefix.append(f"• 今日已观测最高温：{mx_txt}（{tmax_txt}）")
            else:
                metar_prefix.append(f"• 今日已观测最高温：{mx_txt}")
    except Exception:
        pass
    return "📡 **最新实况分析（METAR）**\n" + ("\n".join(metar_prefix + [metar_text]) if metar_prefix else metar_text)



def choose_section_text(
    primary_window: dict[str, Any],
    metar_text: str,
    metar_diag: dict[str, Any],
    polymarket_event_url: str,
    forecast_decision: dict[str, Any] | None = None,
    compact_synoptic: bool = False,
    temp_unit: str = "C",
    synoptic_window: dict[str, Any] | None = None,
    polymarket_prefetched_event: tuple[bool, list[dict[str, Any]]] | None = None,
) -> str:
    """Render-only section builder.

    Decision/diagnostics should come from forecast_pipeline; this function only translates
    structured outputs into report text.
    """

    unit = "F" if str(temp_unit).upper() == "F" else "C"

    lp = load_tmax_learning_params() or {}
    lp_rt = (lp.get("rounded_top") or {}) if isinstance(lp, dict) else {}

    rt_accel_neg = float(lp_rt.get("temp_accel_neg_threshold", -0.25))
    rt_flat = float(lp_rt.get("flat_trend_threshold", 0.12))
    rt_weak = float(lp_rt.get("weak_trend_threshold", 0.22))
    rt_near_peak_h = float(lp_rt.get("near_peak_hours", 1.8))
    rt_near_end_h = float(lp_rt.get("near_end_hours", 1.0))
    rt_solar_stall = float(lp_rt.get("solar_stalling_slope", 0.012))
    rt_solar_rise = float(lp_rt.get("solar_strong_rise_slope", 0.030))
    rt_rad_low = float(lp_rt.get("rad_low_threshold", 0.55))
    rt_rad_recover = float(lp_rt.get("rad_recover_threshold", 0.72))
    rt_rad_recover_tr = float(lp_rt.get("rad_recover_trend", 0.025))

    lp_night = (lp.get("nocturnal_rewarm") or {}) if isinstance(lp, dict) else {}
    rt_night_solar = float(lp_night.get("night_solar_max", 0.08))
    rt_night_hour_start = float(lp_night.get("night_hour_start", 17.5))
    rt_night_hour_end = float(lp_night.get("night_hour_end", 7.0))
    rt_night_warm_bias = float(lp_night.get("warm_advection_bias_min", 0.45))
    rt_night_wind_jump = float(lp_night.get("wind_speed_jump_kt", 3.0))
    rt_night_wind_mix_min = float(lp_night.get("wind_speed_mix_min_kt", 7.0))
    rt_night_dp_rise = float(lp_night.get("dewpoint_rise_min_c", 0.8))
    rt_night_pres_fall = float(lp_night.get("pressure_fall_min_hpa", -0.6))
    rt_night_score_min = float(lp_night.get("score_min", 1.5))

    def _to_unit(c: float) -> float:
        return (c * 9.0 / 5.0 + 32.0) if unit == "F" else c

    def _fmt_temp(v_c: float) -> str:
        v = _to_unit(float(v_c))
        return f"{v:.1f}°{unit}"

    def fmt_range_fn(lo_c: float, hi_c: float) -> str:
        lo_u = _to_unit(float(lo_c))
        hi_u = _to_unit(float(hi_c))
        return f"{lo_u:.1f}~{hi_u:.1f}°{unit}"

    def solar_clear_score_fn(lat_deg: float, lon_deg: float, dt_local: datetime) -> float:
        """Return simplified clear-sky radiation score in [0,1] from solar geometry.
        Uses NOAA-like equation-of-time/declination and local solar time correction by longitude.
        """
        tz_off_h = 0.0
        try:
            if dt_local.tzinfo is not None and dt_local.utcoffset() is not None:
                tz_off_h = float(dt_local.utcoffset().total_seconds() / 3600.0)
        except Exception:
            tz_off_h = 0.0

        doy = int(dt_local.timetuple().tm_yday)
        hour = float(dt_local.hour + dt_local.minute / 60.0 + dt_local.second / 3600.0)
        gamma = 2.0 * math.pi / 365.0 * (doy - 1 + (hour - 12.0) / 24.0)

        decl = (
            0.006918
            - 0.399912 * math.cos(gamma)
            + 0.070257 * math.sin(gamma)
            - 0.006758 * math.cos(2 * gamma)
            + 0.000907 * math.sin(2 * gamma)
            - 0.002697 * math.cos(3 * gamma)
            + 0.00148 * math.sin(3 * gamma)
        )
        eqtime = 229.18 * (
            0.000075
            + 0.001868 * math.cos(gamma)
            - 0.032077 * math.sin(gamma)
            - 0.014615 * math.cos(2 * gamma)
            - 0.040849 * math.sin(2 * gamma)
        )

        tst_min = hour * 60.0 + eqtime + 4.0 * float(lon_deg) - 60.0 * tz_off_h
        tst_min = tst_min % 1440.0
        ha_deg = tst_min / 4.0 - 180.0

        lat_rad = math.radians(float(lat_deg))
        ha_rad = math.radians(ha_deg)
        cosz = (
            math.sin(lat_rad) * math.sin(decl)
            + math.cos(lat_rad) * math.cos(decl) * math.cos(ha_rad)
        )
        cosz = max(-1.0, min(1.0, cosz))
        if cosz <= 0.0:
            return 0.0

        # Relative clear-sky radiation shape (slightly convex to represent midday dominance)
        return max(0.0, min(1.0, cosz ** 1.15))

    state = _build_condition_state(
        primary_window=primary_window,
        metar_diag=metar_diag,
        forecast_decision=forecast_decision,
        synoptic_window=synoptic_window,
    )

    fdec = state["fdec"]
    d = state["d"]
    quality = state["quality"]
    syn_w = state["syn_w"]
    calc_window = state["calc_window"]
    line500 = state["line500"]
    line850 = state["line850"]
    extra = state["extra"]
    h700_summary = state["h700_summary"]
    h925_summary = state["h925_summary"]
    snd_thermo = state["snd_thermo"]
    cloud_code_now = state["cloud_code_now"]
    precip_state = state["precip_state"]
    precip_trend = state["precip_trend"]
    candidates = state["candidates"]
    cov = state["cov"]
    obj = state["obj"]

    syn_lines = _build_synoptic_lines(
        primary_window=primary_window,
        metar_diag=metar_diag,
        compact_synoptic=compact_synoptic,
        syn_w=syn_w,
        calc_window=calc_window,
        d=d,
        quality=quality,
        obj=obj,
        candidates=candidates,
        cov=cov,
        line500=line500,
        line850=line850,
        extra=extra,
        h700_summary=h700_summary,
        h925_summary=h925_summary,
        snd_thermo=snd_thermo,
        cloud_code_now=cloud_code_now,
        precip_state=precip_state,
        precip_trend=precip_trend,
    )

    metar_block = _build_metar_block(
        metar_diag=metar_diag,
        metar_text=metar_text,
        unit=unit,
        fmt_temp=_fmt_temp,
    )

    peak_data = _build_peak_range_module(
        primary_window=primary_window,
        syn_w=syn_w,
        calc_window=calc_window,
        metar_diag=metar_diag,
        quality=quality,
        obj=obj,
        line500=line500,
        line850=line850,
        extra=extra,
        h700_summary=h700_summary,
        h925_summary=h925_summary,
        snd_thermo=snd_thermo,
        cloud_code_now=cloud_code_now,
        precip_state=precip_state,
        precip_trend=precip_trend,
        unit=unit,
        rt_accel_neg=rt_accel_neg,
        rt_flat=rt_flat,
        rt_weak=rt_weak,
        rt_near_peak_h=rt_near_peak_h,
        rt_near_end_h=rt_near_end_h,
        rt_solar_stall=rt_solar_stall,
        rt_solar_rise=rt_solar_rise,
        rt_rad_low=rt_rad_low,
        rt_rad_recover=rt_rad_recover,
        rt_rad_recover_tr=rt_rad_recover_tr,
        rt_night_solar=rt_night_solar,
        rt_night_hour_start=rt_night_hour_start,
        rt_night_hour_end=rt_night_hour_end,
        rt_night_warm_bias=rt_night_warm_bias,
        rt_night_wind_jump=rt_night_wind_jump,
        rt_night_wind_mix_min=rt_night_wind_mix_min,
        rt_night_dp_rise=rt_night_dp_rise,
        rt_night_pres_fall=rt_night_pres_fall,
        rt_night_score_min=rt_night_score_min,
        solar_clear_score_fn=solar_clear_score_fn,
        fmt_range_fn=fmt_range_fn,
    )
    peak_range_block = peak_data["peak_range_block"]
    obs_max = peak_data["obs_max"]
    obs_floor = peak_data["obs_floor"]
    obs_ceil = peak_data["obs_ceil"]
    gate = peak_data["gate"]
    phase_now = peak_data["phase_now"]
    low_conf_far = peak_data["low_conf_far"]
    compact_settled_mode = peak_data["compact_settled_mode"]
    cloud_code = peak_data["cloud_code"]
    t_cons = peak_data["t_cons"]
    b_cons = peak_data["b_cons"]
    disp_lo = peak_data["disp_lo"]
    disp_hi = peak_data["disp_hi"]
    core_lo = peak_data["core_lo"]
    core_hi = peak_data["core_hi"]

    t_bias = metar_diag.get("temp_bias_smooth_c") if metar_diag.get("temp_bias_smooth_c") is not None else metar_diag.get("temp_bias_c")
    t_tr = metar_diag.get("temp_trend_smooth_c") if metar_diag.get("temp_trend_smooth_c") is not None else metar_diag.get("temp_trend_1step_c")

    vars_block, metar_block, poly_block = _build_vars_and_market_blocks(
        primary_window=primary_window,
        polymarket_event_url=polymarket_event_url,
        metar_diag=metar_diag,
        metar_block=metar_block,
        quality=quality,
        obj=obj,
        low_conf_far=low_conf_far,
        phase_now=phase_now,
        obs_max=obs_max,
        obs_floor=obs_floor,
        obs_ceil=obs_ceil,
        compact_settled_mode=compact_settled_mode,
        cloud_code=cloud_code,
        line850=line850,
        snd_thermo=snd_thermo,
        precip_state=precip_state,
        precip_trend=precip_trend,
        rt_rad_low=rt_rad_low,
        rt_rad_recover=rt_rad_recover,
        rt_rad_recover_tr=rt_rad_recover_tr,
        t_cons=t_cons,
        b_cons=b_cons,
        t_tr=t_tr,
        t_bias=t_bias,
        gate=gate,
        disp_lo=float(disp_lo),
        disp_hi=float(disp_hi),
        core_lo=float(core_lo),
        core_hi=float(core_hi),
        fmt_range=fmt_range_fn,
        fmt_temp=_fmt_temp,
        polymarket_prefetched_event=polymarket_prefetched_event,
    )

    parts = [
        "\n".join(syn_lines),
        metar_block,
        "\n".join(peak_range_block),
        "\n".join(vars_block),
    ]
    if poly_block:
        parts.append(poly_block)
    return "\n\n".join(parts)
