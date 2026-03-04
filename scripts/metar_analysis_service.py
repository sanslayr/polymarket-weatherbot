#!/usr/bin/env python3
"""METAR observation analysis service for /look rendering."""

from __future__ import annotations

import math
import re
from datetime import datetime, timedelta
from typing import Any
from zoneinfo import ZoneInfo

from param_store import load_tmax_learning_params
from metar_utils import (
    metar_obs_time_utc as _metar_obs_time_utc,
    observed_max_interval_c as _observed_max_interval_c,
)


def metar_observation_block(
    metar24: list[dict[str, Any]],
    hourly_local: dict[str, Any],
    tz_name: str,
    target_date: str | None = None,
    temp_unit: str = "C",
) -> tuple[str, dict[str, Any]]:
    if not metar24:
        return "无可用METAR数据。", {}

    series = sorted(metar24, key=lambda x: x.get("reportTime", ""))
    latest = series[-1]
    prev = series[-2] if len(series) > 1 else None
    prev2 = series[-3] if len(series) > 2 else None
    prev3 = series[-4] if len(series) > 3 else None

    tz = ZoneInfo(tz_name)
    latest_dt_local = _metar_obs_time_utc(latest).astimezone(tz)
    hour_key = latest_dt_local.strftime("%Y-%m-%dT%H:00")

    tmap = {t: v for t, v in zip(hourly_local["time"], hourly_local["temperature_2m"])}
    pmap = {t: v for t, v in zip(hourly_local["time"], hourly_local["pressure_msl"])}
    fc_t = tmap.get(hour_key)
    fc_p = pmap.get(hour_key)

    def _collect_cloud_pairs(obs: dict[str, Any]) -> list[tuple[str, int | None]]:
        raw_ob = (obs.get("rawOb") or "")
        if " CAVOK" in raw_ob:
            return [("CAVOK", None)]
        if " CLR" in raw_ob:
            return [("CLR", None)]
        if " SKC" in raw_ob:
            return [("SKC", None)]

        pairs: list[tuple[str, int | None]] = []
        seen: set[tuple[str, int | None]] = set()

        for code, h in re.findall(r"\b(FEW|SCT|BKN|OVC|VV)(\d{3})\b", raw_ob):
            ft = int(h) * 100
            key = (code, ft)
            if key not in seen:
                seen.add(key)
                pairs.append((code, ft))

        clouds = obs.get("clouds") if isinstance(obs.get("clouds"), list) else []
        for c in clouds:
            if not isinstance(c, dict):
                continue
            code = str(c.get("cover") or "").upper()
            if code not in {"FEW", "SCT", "BKN", "OVC", "VV"}:
                continue
            base = c.get("base")
            ft = None
            try:
                ft = int(float(base)) if base is not None else None
            except Exception:
                ft = None
            key = (code, ft)
            if key not in seen:
                seen.add(key)
                pairs.append((code, ft))

        if not pairs:
            fallback_cover = str(obs.get("cover") or "").upper()
            if fallback_cover in {"CAVOK", "CLR", "SKC"}:
                return [(fallback_cover, None)]
            if fallback_cover in {"FEW", "SCT", "BKN", "OVC", "VV"}:
                return [(fallback_cover, None)]
        return pairs

    def _cloud_compact(obs: dict[str, Any]) -> str:
        pairs = _collect_cloud_pairs(obs)
        if not pairs:
            return str(obs.get("cover") or "N/A")
        if len(pairs) == 1 and pairs[0][0] in {"CAVOK", "CLR", "SKC"}:
            return f"{pairs[0][0]}(晴天)"
        toks: list[str] = []
        for code, ft in pairs:
            if ft is None:
                toks.append(code)
            else:
                toks.append(f"{code}{int(round(ft/100.0)):03d}")
        return " ".join(toks)

    def _cloud_tokens(obs: dict[str, Any]) -> list[str]:
        pairs = _collect_cloud_pairs(obs)
        if not pairs:
            return []
        if len(pairs) == 1 and pairs[0][0] in {"CAVOK", "CLR", "SKC"}:
            return [pairs[0][0]]
        toks: list[str] = []
        for code, ft in pairs:
            if ft is None:
                toks.append(code)
            else:
                toks.append(f"{code}{int(round(ft/100.0)):03d}")
        return toks

    def parse_cloud_layers(obs: dict[str, Any]) -> str:
        pairs = _collect_cloud_pairs(obs)
        if not pairs:
            return str(obs.get("cover") or "N/A")
        if len(pairs) == 1 and pairs[0][0] in {"CAVOK", "CLR", "SKC"}:
            return f"{pairs[0][0]}(晴天)"

        code_meaning = {
            "FEW": "少云",
            "SCT": "疏云",
            "BKN": "多云",
            "OVC": "阴天",
            "VV": "垂直能见度",
            "CAVOK": "能见良好",
            "CLR": "净空",
            "SKC": "净空",
        }
        out = []
        for code, ft in pairs:
            meaning = code_meaning.get(code, code)
            if ft is None:
                out.append(f"{code}({meaning})")
            else:
                m = int(round(ft * 0.3048))
                h = int(round(ft / 100.0))
                out.append(f"{code}{h:03d}({meaning}{ft}ft/{m}m)")
        return ", ".join(out)

    def _wind_dir_text(d: Any) -> str:
        try:
            deg = float(d)
        except Exception:
            return "风向不定"
        deg = deg % 360
        dirs = [
            "北风", "东北偏北风", "东北风", "东北偏东风",
            "东风", "东南偏东风", "东南风", "东南偏南风",
            "南风", "西南偏南风", "西南风", "西南偏西风",
            "西风", "西北偏西风", "西北风", "西北偏北风",
        ]
        idx = int(((deg + 11.25) % 360) // 22.5)
        return dirs[idx]

    def fmt_wind(x: dict[str, Any]) -> str:
        d = x.get("wdir")
        s = x.get("wspd")
        if d in (None, "", "VRB"):
            return f"风向不定（VRB） {s}kt"
        return f"{_wind_dir_text(d)}（{d}°） {s}kt"

    time_label = "UTC" if str(tz_name).upper() == "UTC" else "Local"

    unit = "F" if str(temp_unit).upper() == "F" else "C"

    lp = load_tmax_learning_params() or {}
    lp_rad = (lp.get("metar_radiation") or {}) if isinstance(lp, dict) else {}
    cloud_cover_map = (lp_rad.get("cloud_cover_map") or {}) if isinstance(lp_rad, dict) else {}
    cloud_base_weight = (lp_rad.get("cloud_base_weight") or {}) if isinstance(lp_rad, dict) else {}
    layer_gamma = list(lp_rad.get("layer_gamma") or [1.0, 0.55, 0.30, 0.20])
    trans_cfg = (lp_rad.get("transmittance") or {}) if isinstance(lp_rad, dict) else {}
    wx_cfg = (lp_rad.get("wx_transmittance") or {}) if isinstance(lp_rad, dict) else {}

    def _to_temp_unit(v_c: float) -> float:
        return (v_c * 9.0 / 5.0 + 32.0) if unit == "F" else v_c

    def _fmt_temp_value(v_c: Any) -> str:
        try:
            if unit == "C":
                v = float(v_c)
                if abs(v - round(v)) < 0.05:
                    return f"{int(round(v))}°C"
                return f"{v:.1f}°C"
            v = _to_temp_unit(float(v_c))
            if abs(v - round(v)) < 0.05:
                return f"{int(round(v))}°{unit}"
            return f"{v:.1f}°{unit}"
        except Exception:
            return f"{v_c}°{unit}"

    def _delta_text(v: float, unit_txt: str) -> str:
        if abs(v) < 0.05:
            return "较上一报持平"
        return f"较上一报 {v:+.1f}{unit_txt}"

    def _delta_temp_text(v_c: float) -> str:
        if unit == "C":
            dv = float(v_c)
            if abs(dv) < 0.05:
                return "较上一报持平"
            if abs(dv - round(dv)) < 0.05:
                return f"较上一报 {int(round(dv)):+d}°C"
            return f"较上一报 {dv:+.1f}°C"
        return _delta_text(_to_temp_unit(float(v_c)) - _to_temp_unit(0.0), f"°{unit}")

    def _cloud_cov_code(code: str) -> float:
        c = str(code or "").upper()
        try:
            v = float(cloud_cover_map.get(c, cloud_cover_map.get("UNKNOWN", 0.45)))
        except Exception:
            v = 0.45
        return max(0.0, min(1.0, v))

    def _cloud_base_weight(ft: int | None) -> float:
        try:
            w_unknown = float(cloud_base_weight.get("unknown", 0.65))
            w1 = float(cloud_base_weight.get("lt_2500", 1.0))
            w2 = float(cloud_base_weight.get("lt_7000", 0.75))
            w3 = float(cloud_base_weight.get("lt_15000", 0.45))
            w4 = float(cloud_base_weight.get("ge_15000", 0.25))
        except Exception:
            w_unknown, w1, w2, w3, w4 = 0.65, 1.0, 0.75, 0.45, 0.25
        if ft is None:
            return w_unknown
        x = float(ft)
        if x < 2500:
            return w1
        if x < 7000:
            return w2
        if x < 15000:
            return w3
        return w4

    def _metar_cloud_effective_cover(obs: dict[str, Any]) -> float:
        pairs = _collect_cloud_pairs(obs)
        if not pairs:
            return 0.35
        if len(pairs) == 1 and str(pairs[0][0]).upper() in {"CLR", "CAVOK", "SKC"}:
            return 0.0

        layers: list[tuple[float, float]] = []
        for code, ft in pairs:
            cov = _cloud_cov_code(str(code))
            wt = _cloud_base_weight(ft)
            score = max(0.0, min(0.98, cov * wt))
            order = float(ft) if ft is not None else 99999.0
            layers.append((order, score))
        layers.sort(key=lambda z: z[0])

        gammas = layer_gamma if layer_gamma else [1.0, 0.55, 0.30, 0.20]
        prod = 1.0
        for i, (_ord, base_score) in enumerate(layers):
            g = float(gammas[i]) if i < len(gammas) else 0.15
            x = max(0.0, min(0.98, g * base_score))
            prod *= (1.0 - x)
        c_eff = 1.0 - prod
        return max(0.0, min(1.0, c_eff))

    def _cloud_transmittance(obs: dict[str, Any]) -> float:
        c_eff = _metar_cloud_effective_cover(obs)
        try:
            factor = float(trans_cfg.get("factor", 0.85))
            power = float(trans_cfg.get("power", 1.2))
            floor = float(trans_cfg.get("floor", 0.12))
        except Exception:
            factor, power, floor = 0.85, 1.2, 0.12
        t = 1.0 - factor * (c_eff ** power)
        return max(floor, min(1.0, t))

    def _wx_transmittance(wx_raw: Any) -> float:
        s = str(wx_raw or "").upper().replace(" ", "")
        if not s:
            return float(wx_cfg.get("DEFAULT", 1.0))
        if "TS" in s:
            return float(wx_cfg.get("TS", 0.45))
        if "FG" in s:
            return float(wx_cfg.get("FG", 0.60))
        if any(k in s for k in ["BR", "HZ", "FU", "DU", "SA", "VA"]):
            return float(wx_cfg.get("OBSCURATION", 0.90))
        if any(k in s for k in ["+RA", "+DZ", "+SN"]):
            return float(wx_cfg.get("HEAVY_PRECIP", 0.55))
        if any(k in s for k in ["-RA", "-DZ", "-SN"]):
            return float(wx_cfg.get("LIGHT_PRECIP", 0.75))
        if any(k in s for k in ["RA", "DZ", "SN", "PL", "GR", "GS"]):
            return float(wx_cfg.get("PRECIP", 0.60))
        return float(wx_cfg.get("DEFAULT", 1.0))

    def _wx_human_desc(wx_raw: Any) -> str:
        t = str(wx_raw or "").upper().strip().replace(" ", "")
        if not t or t == "无降水天气现象":
            return ""

        parts: list[str] = []

        intensity = ""
        if t.startswith("+"):
            intensity = "强"
        elif t.startswith("-"):
            intensity = "小"

        def _i(base: str) -> str:
            return f"{intensity}{base}" if intensity else base

        # vicinity / descriptors
        if "VC" in t:
            parts.append("附近")
        if "MI" in t:
            parts.append("浅层")
        if "BC" in t:
            parts.append("片状")
        if "PR" in t:
            parts.append("部分")

        # convective / severe
        if "TS" in t:
            parts.append("雷暴")
        if "SQ" in t:
            parts.append("飑")
        if "FC" in t:
            parts.append("漏斗云/龙卷")
        if "DS" in t:
            parts.append("沙尘暴")
        elif "SS" in t:
            parts.append("沙暴")
        if "PO" in t:
            parts.append("尘旋/沙旋")

        # precipitation (long tokens first)
        if "FZRA" in t:
            parts.append(_i("冻雨"))
        elif "FZDZ" in t:
            parts.append(_i("冻毛毛雨"))
        else:
            if "DZ" in t:
                parts.append(_i("毛毛雨"))
            if "RA" in t:
                parts.append(_i("雨"))
            if "SN" in t:
                parts.append(_i("雪"))
            if "SG" in t:
                parts.append(_i("米雪"))
            if "PL" in t:
                parts.append(_i("冰粒"))
            if "GR" in t:
                parts.append(_i("冰雹"))
            elif "GS" in t:
                parts.append(_i("小冰雹"))
            if "UP" in t:
                parts.append("未知降水")

        if "SH" in t and any(k in t for k in ["RA", "SN", "GS", "GR", "PL", "DZ"]):
            parts.append("阵性")
        if "FZ" in t and ("FZRA" not in t) and ("FZDZ" not in t):
            parts.append("冻性")
        if "BL" in t:
            parts.append("吹雪/吹尘")
        if "DR" in t:
            parts.append("低吹")

        # obscuration / particulates
        if "FG" in t:
            parts.append("雾")
        if "BR" in t:
            parts.append("轻雾")
        if "HZ" in t:
            parts.append("霾")
        if "FU" in t:
            parts.append("烟")
        if "DU" in t:
            parts.append("扬尘")
        if "SA" in t:
            parts.append("沙")
        if "VA" in t:
            parts.append("火山灰")

        if not parts:
            return ""
        uniq = list(dict.fromkeys(parts))
        return "、".join(uniq)

    def _wind_change_text(cur: dict[str, Any], prev_x: dict[str, Any] | None) -> str:
        if not prev_x:
            return ""

        cur_d = cur.get("wdir")
        cur_s = cur.get("wspd")
        prv_d = prev_x.get("wdir")
        prv_s = prev_x.get("wspd")

        def _f(v: Any) -> float | None:
            try:
                if v in (None, "", "VRB"):
                    return None
                return float(v)
            except Exception:
                return None

        cd = _f(cur_d)
        pd = _f(prv_d)
        try:
            cs = float(cur_s)
        except Exception:
            cs = None
        try:
            ps = float(prv_s)
        except Exception:
            ps = None

        prev_wind_txt = f"{prv_d}° {prv_s}kt" if (prv_d not in (None, "", "VRB") and prv_s not in (None, "")) else f"{prv_d or 'VRB'} {prv_s or '?'}kt"

        if cd is None and pd is None:
            dir_msg = "风向信息不足"
        elif cd is None and pd is not None:
            dir_msg = "转为风向不定"
        elif cd is not None and pd is None:
            dir_msg = "风向由不定转为可判定"
        else:
            d = abs((cd - pd + 180.0) % 360.0 - 180.0)
            if d >= 60:
                dir_msg = "风向明显转向"
            elif d >= 25:
                dir_msg = "风向小幅转向"
            else:
                dir_msg = "风向基本稳定"

        if cs is None or ps is None:
            spd_msg = "风速变化待确认"
        else:
            ds = cs - ps
            if ds >= 3.0:
                spd_msg = f"风速增强{ds:.0f}kt"
            elif ds <= -3.0:
                spd_msg = f"风速减弱{abs(ds):.0f}kt"
            else:
                spd_msg = "风速变化不大"

        return f"较上一报{prev_wind_txt}，{dir_msg}，{spd_msg}"

    def _cloud_change_parts(cur: dict[str, Any], prev_x: dict[str, Any] | None) -> dict[str, str]:
        if not prev_x:
            return {}
        prev_compact = _cloud_compact(prev_x)
        cur_tokens = _cloud_tokens(cur)
        prev_tokens = _cloud_tokens(prev_x)
        tr = _cloud_trend(cur, prev_x)

        if cur_tokens == prev_tokens:
            tr_txt = "云层稳定无变化"
            return {
                "prev": prev_compact,
                "trend": tr_txt,
                "inline": f"（上一报{prev_compact}，{tr_txt}）",
            }

        cur_set = set(cur_tokens)
        prev_set = set(prev_tokens)
        added = [t for t in cur_tokens if t not in prev_set]
        removed = [t for t in prev_tokens if t not in cur_set]

        if added and removed:
            rank = {"CLR": 0, "CAVOK": 0, "SKC": 0, "FEW": 1, "SCT": 2, "BKN": 3, "OVC": 4, "VV": 5}

            def _parse_tok(tok: str) -> tuple[str, int | None]:
                s = str(tok or "").upper()
                m = re.match(r"^(FEW|SCT|BKN|OVC|VV)(\d{3})$", s)
                if m:
                    return m.group(1), int(m.group(2)) * 100
                if s in {"CLR", "CAVOK", "SKC", "FEW", "SCT", "BKN", "OVC", "VV"}:
                    return s, None
                return s, None

            add_p = [_parse_tok(t) for t in added]
            rem_p = [_parse_tok(t) for t in removed]

            # try layer-evolution matching by nearest cloud-base height
            # tuple: (removed_tok, added_tok, base_diff_ft, rank_delta, removed_base_ft, added_base_ft)
            pairs: list[tuple[str, str, float | None, int, int | None, int | None]] = []
            used_add: set[int] = set()
            for ri, (rc, rh) in enumerate(rem_p):
                best_j = None
                best_d = None
                for aj, (ac, ah) in enumerate(add_p):
                    if aj in used_add:
                        continue
                    if rh is not None and ah is not None:
                        d = abs(float(ah - rh))
                    else:
                        d = 99999.0
                    if best_d is None or d < best_d:
                        best_d = d
                        best_j = aj
                if best_j is not None:
                    used_add.add(best_j)
                    ac, ah = add_p[best_j]
                    dr = rank.get(ac, 2) - rank.get(rc, 2)
                    pairs.append((removed[ri], added[best_j], best_d, dr, rh, ah))

            layer_pairs = [p for p in pairs if p[2] is not None and p[2] <= 4000.0]
            if len(layer_pairs) == len(pairs) and len(pairs) >= 1:
                dir_all = [p[3] for p in layer_pairs]
                arrows = "/".join([f"{p[0]}→{p[1]}" for p in layer_pairs[:3]])

                hvals = [h for p in layer_pairs for h in (p[4], p[5]) if h is not None]
                if hvals:
                    h_lo = int(min(hvals))
                    h_hi = int(max(hvals))
                    if abs(h_hi - h_lo) <= 1500:
                        h_mid = int(round((h_lo + h_hi) / 200.0) * 100)
                        h_txt = f"约{h_mid}ft高度层"
                    else:
                        lo_r = int(round(h_lo / 100.0) * 100)
                        hi_r = int(round(h_hi / 100.0) * 100)
                        h_txt = f"约{lo_r}-{hi_r}ft高度层"
                else:
                    h_txt = "该高度层"

                if all(d < 0 for d in dir_all):
                    tr_txt = f"{h_txt}云量减弱（{arrows}）"
                elif all(d > 0 for d in dir_all):
                    tr_txt = f"{h_txt}云量增强（{arrows}）"
                else:
                    tr_txt = f"{h_txt}云层调整（{arrows}）"
            else:
                tr_txt = f"云层重排（新增{'/'.join(added)}；消退{'/'.join(removed)}）"
        elif added:
            tr_txt = f"云量增加（新增{'/'.join(added)}）"
        elif removed:
            tr_txt = f"云量减少（消退{'/'.join(removed)}）"
        else:
            tr_txt = tr if tr else "云层结构有调整"

        return {
            "prev": prev_compact,
            "trend": tr_txt,
            "inline": f"（上一报{prev_compact}，{tr_txt}）",
        }

    def _calc_rh_pct(t_c: Any, td_c: Any) -> float | None:
        try:
            t = float(t_c)
            td = float(td_c)
        except Exception:
            return None
        try:
            # Magnus approximation (°C)
            rh = 100.0 * math.exp((17.625 * td) / (243.04 + td) - (17.625 * t) / (243.04 + t))
            return max(0.0, min(100.0, rh))
        except Exception:
            return None

    def fmt_latest_obs(x: dict[str, Any], prev_x: dict[str, Any] | None) -> list[str]:
        local = _metar_obs_time_utc(x).astimezone(tz)
        wx = x.get("wxString") or x.get("wx") or "无降水天气现象"
        wx_desc = _wx_human_desc(wx)
        cloud = parse_cloud_layers(x)

        dt = dp = dpres = 0.0
        if prev_x:
            try:
                dt = float(x.get("temp", 0)) - float(prev_x.get("temp", 0))
                dp = float(x.get("dewp", 0)) - float(prev_x.get("dewp", 0))
                dpres = float(x.get("altim", 0)) - float(prev_x.get("altim", 0))
            except Exception:
                pass

        latest_hdr = f"**最新报：{local.strftime('%H:%M')} {time_label}**"
        if prev_x:
            prev_local = _metar_obs_time_utc(prev_x).astimezone(tz)
            latest_hdr = f"**最新报：{local.strftime('%H:%M')} {time_label}**（上一报 {prev_local.strftime('%H:%M')}）"

        wind_line = fmt_wind(x)
        wind_cmp = _wind_change_text(x, prev_x)
        if wind_cmp:
            wind_line = f"{wind_line}（{wind_cmp}）"

        cloud_line = cloud
        cloud_cmp = _cloud_change_parts(x, prev_x)
        if cloud_cmp:
            cur_tokens_n = len(_cloud_tokens(x))
            prev_tokens_n = len(_cloud_tokens(prev_x)) if prev_x else 0
            multiline_cmp = (cur_tokens_n >= 3) or (prev_tokens_n >= 3) or (cloud.count(",") >= 2)
            if multiline_cmp:
                cloud_line = (
                    f"{cloud}\n"
                    f"  ↳ 上一报：{cloud_cmp.get('prev', '')}\n"
                    f"  ↳ 变化：{cloud_cmp.get('trend', '')}"
                )
            else:
                cloud_line = f"{cloud}{cloud_cmp.get('inline', '')}"

        rh_now = _calc_rh_pct(x.get("temp"), x.get("dewp"))
        rh_prev = _calc_rh_pct(prev_x.get("temp"), prev_x.get("dewp")) if prev_x else None
        rh_line = "缺测"
        if rh_now is not None:
            if rh_prev is not None:
                rh_delta = rh_now - rh_prev
                rh_line = f"{rh_now:.0f}%（{_delta_text(rh_delta, '%')}）"
            else:
                rh_line = f"{rh_now:.0f}%"

        lines = [
            latest_hdr,
            f"• **🌡️ 气温**：{_fmt_temp_value(x.get('temp'))}（{_delta_temp_text(dt)}）",
            f"• **💧 露点**：{_fmt_temp_value(x.get('dewp'))}（{_delta_temp_text(dp)}）",
            f"• **💦 湿度**：{rh_line}",
            f"• **📊 气压**：{x.get('altim')} hPa（{_delta_text(dpres, ' hPa')}）",
            f"• **💨 风**：{wind_line}",
            f"• **☁️ 云层**：{cloud_line}",
        ]
        if wx and wx != "无降水天气现象":
            if wx_desc:
                lines.append(f"• **🌦️ 天气现象**：{wx}（{wx_desc}）")
            else:
                lines.append(f"• **🌦️ 天气现象**：{wx}")
        return lines

    def _cloud_code(x: dict[str, Any] | None) -> str:
        if not x:
            return ""
        raw = (x.get("rawOb") or "")
        if " CAVOK" in raw:
            return "CAVOK"
        if " CLR" in raw:
            return "CLR"
        if " SKC" in raw:
            return "SKC"

        rank = {"CLR": 0, "CAVOK": 0, "SKC": 0, "FEW": 1, "SCT": 2, "BKN": 3, "OVC": 4, "VV": 5}
        codes: list[str] = []

        for c, _h in re.findall(r"\b(FEW|SCT|BKN|OVC|VV)(\d{3})\b", raw):
            codes.append(c)

        clouds = x.get("clouds") if isinstance(x.get("clouds"), list) else []
        for c in clouds:
            if isinstance(c, dict):
                cv = str(c.get("cover") or "").upper()
                if cv:
                    codes.append(cv)

        cover = str(x.get("cover") or "").upper()
        if cover:
            codes.append(cover)

        codes = [c for c in codes if c in rank]
        if not codes:
            return ""
        return sorted(codes, key=lambda c: rank.get(c, 0), reverse=True)[0]

    def _cloud_token(x: dict[str, Any] | None) -> str:
        if not x:
            return ""
        raw = (x.get("rawOb") or "")
        if " CAVOK" in raw:
            return "CAVOK"
        if " CLR" in raw:
            return "CLR"
        if " SKC" in raw:
            return "SKC"

        rank = {"CLR": 0, "CAVOK": 0, "SKC": 0, "FEW": 1, "SCT": 2, "BKN": 3, "OVC": 4, "VV": 5}
        toks: list[tuple[str, int | None]] = []

        for c, h in re.findall(r"\b(FEW|SCT|BKN|OVC|VV)(\d{3})\b", raw):
            toks.append((c, int(h) * 100))

        clouds = x.get("clouds") if isinstance(x.get("clouds"), list) else []
        for c in clouds:
            if not isinstance(c, dict):
                continue
            cv = str(c.get("cover") or "").upper()
            if cv not in rank:
                continue
            base = c.get("base")
            ft = None
            try:
                ft = int(float(base)) if base is not None else None
            except Exception:
                ft = None
            toks.append((cv, ft))

        if not toks:
            c = _cloud_code(x)
            return c

        # choose strongest cover; if tie, lower base first (more restrictive)
        toks = sorted(toks, key=lambda z: (rank.get(z[0], 0), -(z[1] if z[1] is not None else 10**9)), reverse=True)
        c, ft = toks[0]
        if ft is None:
            return c
        return f"{c}{int(round(ft/100.0)):03d}"

    def _cloud_trend(cur: dict[str, Any], prev_x: dict[str, Any] | None) -> str:
        rank = {"CLR": 0, "CAVOK": 0, "SKC": 0, "FEW": 1, "SCT": 2, "BKN": 3, "OVC": 4, "VV": 5}
        c1 = _cloud_code(cur)
        c0 = _cloud_code(prev_x)
        if not c0 or not c1:
            return "云层趋势不明确"
        r0 = rank.get(c0, 2)
        r1 = rank.get(c1, 2)
        if r1 >= r0 + 2:
            return f"云层快速回补（{c0}→{c1}）"
        if r1 > r0:
            return f"云量增加（{c0}→{c1}）"
        if r1 <= r0 - 2:
            return f"云层明显开窗（{c0}→{c1}）"
        if r1 < r0:
            return f"云量减弱（{c0}→{c1}）"
        return f"云层级别稳定（{c1}）"

    def _hour_bias(obs: dict[str, Any]) -> float | None:
        try:
            k = _metar_obs_time_utc(obs).astimezone(tz).strftime("%Y-%m-%dT%H:00")
            fv = tmap.get(k)
            if fv is None:
                return None
            return round(float(obs.get("temp", 0.0)) - float(fv), 2)
        except Exception:
            return None

    lines = []
    lines.extend(fmt_latest_obs(latest, prev))

    bias = None if fc_t is None else round(float(latest.get("temp", 0)) - float(fc_t), 2)
    p_bias = None if fc_p is None else round(float(latest.get("altim", 0)) - float(fc_p), 2)
    if bias is not None and p_bias is not None:
        b_disp = (_to_temp_unit(float(bias)) - _to_temp_unit(0.0))
        b_txt = f"{b_disp:+.1f}°{unit}"
        lines.append(f"同小时模式偏差：温度 {b_txt}；气压 {p_bias:+.1f}hPa")

    t_trend = None
    if prev is not None:
        try:
            t_trend = float(latest.get("temp", 0)) - float(prev.get("temp", 0))
        except Exception:
            t_trend = None

    def _is_intish(v: Any) -> bool:
        try:
            return abs(float(v) - round(float(v))) < 0.05
        except Exception:
            return False

    # Quantization-aware smoothing: avoid overreacting to single METAR step (often integer-quantized).
    trend_steps: list[float] = []
    for a, b in ((latest, prev), (prev, prev2), (prev2, prev3)):
        if a is None or b is None:
            continue
        try:
            trend_steps.append(float(a.get("temp", 0)) - float(b.get("temp", 0)))
        except Exception:
            continue

    temp_accel_2step = None
    if len(trend_steps) >= 2:
        try:
            # positive: warming acceleration; negative: warming deceleration / rounded-top tendency
            temp_accel_2step = float(trend_steps[0] - trend_steps[1])
        except Exception:
            temp_accel_2step = None

    t_trend_smooth = None
    if trend_steps:
        ws = [0.55, 0.30, 0.15]
        use_n = min(len(trend_steps), len(ws))
        num = sum(trend_steps[i] * ws[i] for i in range(use_n))
        den = sum(ws[:use_n])
        t_trend_smooth = num / den if den > 0 else None

        # integer METAR deadband
        int_q = _is_intish(latest.get("temp")) and (prev is None or _is_intish(prev.get("temp")))
        deadband = 0.55 if int_q else 0.35
        if t_trend_smooth is not None and abs(t_trend_smooth) < deadband:
            t_trend_smooth = 0.0

    latest_cloud_code = _cloud_code(latest)

    if str(latest_cloud_code or "").upper() in {"CLR", "CAVOK", "SKC", "FEW", "SCT"}:
        cloud_hint = "云量约束偏弱"
    elif str(latest_cloud_code or "").upper() in {"BKN", "OVC", "VV"}:
        cloud_hint = "低云约束仍在"
    else:
        cloud_hint = "云量约束不确定"

    trend_ref = t_trend_smooth if isinstance(t_trend_smooth, (int, float)) else t_trend
    if isinstance(trend_ref, (int, float)):
        if trend_ref >= 0.5:
            trend_hint = "短时升温仍在延续"
        elif trend_ref <= -0.5:
            trend_hint = "短时升温动能转弱"
        else:
            trend_hint = "短时温度基本横盘"
    else:
        trend_hint = "短时温度节奏待确认"

    def _wx_state(wx: str) -> str:
        s = str(wx or "").upper()
        if not s:
            return "none"
        if "TS" in s:
            return "convective"
        if any(k in s for k in ["RA", "DZ", "SN", "PL", "GR", "GS"]):
            if "+" in s:
                return "heavy"
            if "-" in s:
                return "light"
            return "moderate"
        return "none"

    def _wx_hint(wx: str) -> str:
        s = str(wx or "").upper()
        if not s:
            return ""
        if "TS" in s:
            return "对流降水干扰在场"
        if any(k in s for k in ["RA", "DZ"]):
            if "-RA" in s or "-DZ" in s:
                return "弱降雨干扰在场"
            if "+RA" in s or "+DZ" in s:
                return "较强降雨干扰在场"
            return "降雨干扰在场"
        if any(k in s for k in ["SN", "PL", "GR", "GS"]):
            return "降水相态干扰在场"
        return ""

    wx_now = str(latest.get("wxString") or latest.get("wx") or "").upper()
    wx_prev = str((prev or {}).get("wxString") or (prev or {}).get("wx") or "").upper() if prev else ""
    wx_state_now = _wx_state(wx_now)
    wx_state_prev = _wx_state(wx_prev)

    rank = {"none": 0, "light": 1, "moderate": 2, "heavy": 3, "convective": 4}
    if rank.get(wx_state_now, 0) > 0 and rank.get(wx_state_prev, 0) == 0:
        wx_trend = "new"
    elif rank.get(wx_state_now, 0) == 0 and rank.get(wx_state_prev, 0) > 0:
        wx_trend = "end"
    elif rank.get(wx_state_now, 0) > rank.get(wx_state_prev, 0):
        wx_trend = "intensify"
    elif rank.get(wx_state_now, 0) < rank.get(wx_state_prev, 0):
        wx_trend = "weaken"
    elif rank.get(wx_state_now, 0) > 0:
        wx_trend = "steady"
    else:
        wx_trend = "none"

    wx_hint = _wx_hint(wx_now)
    wx_change_hint = ""
    if wx_trend == "new":
        wx_change_hint = "降水新出现"
    elif wx_trend == "intensify":
        wx_change_hint = "降水在增强"
    elif wx_trend == "weaken":
        wx_change_hint = "降水在减弱"
    elif wx_trend == "end":
        wx_change_hint = "降水已结束"

    # METAR-based cloud/radiation quantification (multi-layer aware)
    c_eff_now = _metar_cloud_effective_cover(latest)
    c_eff_prev = _metar_cloud_effective_cover(prev) if prev else None
    c_eff_prev2 = _metar_cloud_effective_cover(prev2) if prev2 else None
    t_cloud_now = _cloud_transmittance(latest)
    t_cloud_prev = _cloud_transmittance(prev) if prev else None
    t_cloud_prev2 = _cloud_transmittance(prev2) if prev2 else None

    t_wx_now = _wx_transmittance(wx_now)
    t_wx_prev = _wx_transmittance(wx_prev) if prev else None
    wx_prev2 = str((prev2 or {}).get("wxString") or (prev2 or {}).get("wx") or "").upper() if prev2 else ""
    t_wx_prev2 = _wx_transmittance(wx_prev2) if prev2 else None

    rad_eff_now = max(0.0, min(1.0, t_cloud_now * t_wx_now))
    rad_eff_prev = (max(0.0, min(1.0, t_cloud_prev * t_wx_prev)) if (t_cloud_prev is not None and t_wx_prev is not None) else None)
    rad_eff_prev2 = (max(0.0, min(1.0, t_cloud_prev2 * t_wx_prev2)) if (t_cloud_prev2 is not None and t_wx_prev2 is not None) else None)

    rad_eff_smooth = rad_eff_now
    ws_rad = [0.6, 0.3, 0.1]
    rad_series = [rad_eff_now, rad_eff_prev, rad_eff_prev2]
    num = 0.0
    den = 0.0
    for i, v in enumerate(rad_series):
        if isinstance(v, (int, float)):
            num += float(v) * ws_rad[i]
            den += ws_rad[i]
    if den > 0:
        rad_eff_smooth = num / den

    rad_eff_trend = None
    if rad_eff_prev is not None:
        try:
            rad_eff_trend = float(rad_eff_now - rad_eff_prev)
        except Exception:
            rad_eff_trend = None

    summary_bits = [trend_hint, cloud_hint]
    if wx_hint:
        summary_bits.append(wx_hint)
    if wx_change_hint:
        summary_bits.append(wx_change_hint)

    # 合并“简评 + 近两小时节奏”为单行趋势描述
    rhythm_chunks: list[str] = []
    try:
        if prev is not None and prev2 is not None:
            t0 = float(latest.get("temp", 0.0))
            t1 = float(prev.get("temp", 0.0))
            t2 = float(prev2.get("temp", 0.0))
            dt_now = t0 - t1
            dt_prev = t1 - t2

            p0 = float(latest.get("altim", 0.0))
            p2 = float(prev2.get("altim", 0.0))
            dp2h = p0 - p2

            w0 = latest.get("wdir")
            w1 = prev.get("wdir")
            dchg = 0.0
            if w0 not in (None, "", "VRB") and w1 not in (None, "", "VRB"):
                a = abs(float(w0) - float(w1)) % 360.0
                dchg = min(a, 360.0 - a)

            cloud_txt = _cloud_trend(latest, prev)

            temp_signal = (abs(dt_now) >= 0.5) or (abs(dt_prev) >= 0.5)
            press_signal = abs(dp2h) >= 1.2
            wind_signal = dchg >= 35
            cloud_signal = ("回补" in cloud_txt) or ("开窗" in cloud_txt) or ("减少" in cloud_txt) or ("增加" in cloud_txt)

            if temp_signal or press_signal or wind_signal or cloud_signal:
                if dt_now >= 0.5 and dt_prev >= 0.2:
                    rhythm_chunks.append("温度仍在上行")
                elif dt_now <= -0.5 and dt_prev <= -0.2:
                    rhythm_chunks.append("温度有回落迹象")
                elif temp_signal:
                    rhythm_chunks.append("温度在窄幅震荡")

                if press_signal:
                    if dp2h <= -1.2:
                        rhythm_chunks.append("气压继续走低")
                    elif dp2h >= 1.2:
                        rhythm_chunks.append("气压明显回升")

                if wind_signal:
                    rhythm_chunks.append("风向正在重排")

                if cloud_signal:
                    if "回补" in cloud_txt or "增加" in cloud_txt:
                        rhythm_chunks.append("云量有回补")
                    elif "开窗" in cloud_txt or "减少" in cloud_txt:
                        rhythm_chunks.append("云量在转疏")
    except Exception:
        rhythm_chunks = []

    merged_bits = list(summary_bits)
    for c in rhythm_chunks:
        if c not in merged_bits:
            merged_bits.append(c)

    lines.append("")
    lines.append(f"• 最近两小时实况趋势：{'，'.join(merged_bits)}。")

    wind_dir_change = None
    wind_speed_step = None
    dewpoint_step = None
    pressure_step = None
    if prev is not None:
        try:
            d1 = latest.get("wdir")
            d0 = prev.get("wdir")
            if d1 not in (None, "", "VRB") and d0 not in (None, "", "VRB"):
                a = abs(float(d1) - float(d0)) % 360.0
                wind_dir_change = min(a, 360.0 - a)
        except Exception:
            wind_dir_change = None
        try:
            if latest.get("wspd") not in (None, "") and prev.get("wspd") not in (None, ""):
                wind_speed_step = float(latest.get("wspd", 0)) - float(prev.get("wspd", 0))
        except Exception:
            wind_speed_step = None
        try:
            dewpoint_step = float(latest.get("dewp", 0)) - float(prev.get("dewp", 0))
        except Exception:
            dewpoint_step = None
        try:
            pressure_step = float(latest.get("altim", 0)) - float(prev.get("altim", 0))
        except Exception:
            pressure_step = None

    # 3) 同小时模式偏差轨迹（最近2-3报）
    b0 = _hour_bias(latest)
    b1 = _hour_bias(prev) if prev else None
    b2 = _hour_bias(prev2) if prev2 else None
    bvals = [v for v in [b0, b1, b2] if isinstance(v, (int, float))]
    bias_smooth = None
    if bvals:
        ws = [0.55, 0.30, 0.15]
        num = 0.0
        den = 0.0
        for i, v in enumerate([b0, b1, b2]):
            if isinstance(v, (int, float)):
                w = ws[i]
                num += float(v) * w
                den += w
        if den > 0:
            bias_smooth = round(num / den, 2)
    # Bias trajectory line removed by operator preference: keep report concise.

    # 5) 高影响触发告警（仅触发时显示）
    alert = None
    ctrend = _cloud_trend(latest, prev)
    if wx_trend in {"new", "intensify"}:
        alert = "⚠️ 实况触发：降水出现/增强，短时压温风险上升。"
    elif isinstance(t_trend, (int, float)) and t_trend >= 0.5 and ("开窗" in ctrend or "减弱" in ctrend):
        alert = "⚠️ 实况触发：短时升温 + 云层转疏，窗口上沿风险上调。"
    elif isinstance(t_trend, (int, float)) and t_trend <= -0.5 and ("回补" in ctrend or "增加" in ctrend):
        alert = "⚠️ 实况触发：降温 + 云层增厚，峰值窗口可能提前结束。"
    if alert:
        lines.append("")
        lines.append(alert)


    observed_points: list[tuple[float, datetime]] = []
    temps_by_local_date: dict[date, list[float]] = {}
    # Use local target-date max (not rolling 24h max), to avoid cross-day contamination.
    target_local_date = None
    if target_date:
        try:
            target_local_date = datetime.strptime(target_date, "%Y-%m-%d").date()
        except Exception:
            target_local_date = None
    if target_local_date is None:
        target_local_date = latest_dt_local.date()

    for x in series:
        try:
            x_local_dt = _metar_obs_time_utc(x).astimezone(tz)
            t_val = float(x.get("temp"))
            temps_by_local_date.setdefault(x_local_dt.date(), []).append(t_val)
            if x_local_dt.date() != target_local_date:
                continue
            observed_points.append((t_val, x_local_dt))
        except Exception:
            pass
    obs_max_temp = max((p[0] for p in observed_points), default=None)
    obs_max_time_local = None
    if obs_max_temp is not None:
        cands = [dt for t, dt in observed_points if abs(t - obs_max_temp) < 1e-9]
        if cands:
            obs_max_time_local = max(cands)

    obs_max_interval_lo, obs_max_interval_hi = _observed_max_interval_c(
        obs_max_temp,
        unit,
        c_quantized=_is_intish(obs_max_temp) if obs_max_temp is not None else None,
    )

    obs_today_range_c = None
    obs_prev_day_range_c = None
    try:
        today_vals = temps_by_local_date.get(target_local_date, [])
        if len(today_vals) >= 2:
            obs_today_range_c = float(max(today_vals) - min(today_vals))
        prev_vals = temps_by_local_date.get(target_local_date - timedelta(days=1), [])
        if len(prev_vals) >= 4:
            obs_prev_day_range_c = float(max(prev_vals) - min(prev_vals))
    except Exception:
        obs_today_range_c = None
        obs_prev_day_range_c = None

    model_day_range_c = None
    try:
        t2m: list[float] = []
        for v in (hourly_local.get("temperature_2m") or []):
            try:
                fv = float(v)
                if math.isfinite(fv):
                    t2m.append(fv)
            except Exception:
                continue
        if len(t2m) >= 2:
            model_day_range_c = float(max(t2m) - min(t2m))
    except Exception:
        model_day_range_c = None

    routine_cadence_min = None
    recent_interval_min = None
    speci_active = False
    speci_count_24h = 0
    report_count_24h = 0
    speci_ratio_24h = 0.0
    rapid_temp_jump_count_24h = 0
    rapid_wind_jump_count_24h = 0
    wx_transition_count_24h = 0
    try:
        obs_local: list[tuple[datetime, str]] = []
        for x in series:
            obs_local.append((_metar_obs_time_utc(x).astimezone(tz), str(x.get("rawOb") or "")))

        report_count_24h = len(obs_local)
        speci_count_24h = sum(1 for _dt, raw in obs_local if str(raw).upper().startswith("SPECI "))
        speci_ratio_24h = round(float(speci_count_24h) / float(max(1, report_count_24h)), 3)

        for i in range(1, len(series)):
            cur = series[i]
            pre = series[i - 1]
            try:
                if abs(float(cur.get("temp", 0)) - float(pre.get("temp", 0))) >= 1.2:
                    rapid_temp_jump_count_24h += 1
            except Exception:
                pass

            try:
                ws_cur = float(cur.get("wspd", 0)) if cur.get("wspd") not in (None, "") else None
                ws_pre = float(pre.get("wspd", 0)) if pre.get("wspd") not in (None, "") else None
                wd_cur = float(cur.get("wdir", 0)) if cur.get("wdir") not in (None, "", "VRB") else None
                wd_pre = float(pre.get("wdir", 0)) if pre.get("wdir") not in (None, "", "VRB") else None
                ws_jump = abs(ws_cur - ws_pre) if (ws_cur is not None and ws_pre is not None) else 0.0
                wd_jump = 0.0
                if wd_cur is not None and wd_pre is not None:
                    a = abs(wd_cur - wd_pre) % 360.0
                    wd_jump = min(a, 360.0 - a)
                if ws_jump >= 6.0 or wd_jump >= 50.0:
                    rapid_wind_jump_count_24h += 1
            except Exception:
                pass

            try:
                wx_cur = _wx_state(cur.get("wxString") or cur.get("wx") or "")
                wx_pre = _wx_state(pre.get("wxString") or pre.get("wx") or "")
                if wx_cur != wx_pre and not (wx_cur == "none" and wx_pre == "none"):
                    wx_transition_count_24h += 1
            except Exception:
                pass

        diffs_min: list[float] = []
        for i in range(1, len(obs_local)):
            dmin = (obs_local[i][0] - obs_local[i - 1][0]).total_seconds() / 60.0
            if 8.0 <= dmin <= 130.0:
                diffs_min.append(dmin)

        routine_pool = [d for d in diffs_min if d >= 20.0]
        if routine_pool:
            srt = sorted(routine_pool)
            m = len(srt) // 2
            med = srt[m] if len(srt) % 2 == 1 else 0.5 * (srt[m - 1] + srt[m])
            routine_cadence_min = float(int(round(med / 5.0)) * 5)

        if len(obs_local) >= 2:
            recent_interval_min = round((obs_local[-1][0] - obs_local[-2][0]).total_seconds() / 60.0, 1)

        raw_speci = any(str(raw).upper().startswith("SPECI ") for _dt, raw in obs_local[-3:])
        short_interval = False
        if recent_interval_min is not None:
            if routine_cadence_min is not None:
                short_interval = bool(recent_interval_min <= max(15.0, 0.70 * routine_cadence_min))
            else:
                short_interval = bool(recent_interval_min <= 20.0)
        speci_active = bool(raw_speci or short_interval)
    except Exception:
        routine_cadence_min = None
        recent_interval_min = None
        speci_active = False
        speci_count_24h = 0
        report_count_24h = 0
        speci_ratio_24h = 0.0
        rapid_temp_jump_count_24h = 0
        rapid_wind_jump_count_24h = 0
        wx_transition_count_24h = 0

    cloud_tr = _cloud_trend(latest, prev) if prev else ""
    peak_lock_confirmed = False
    try:
        if prev is not None and prev2 is not None:
            t1 = float(latest.get("temp", 0)) - float(prev.get("temp", 0))
            t0 = float(prev.get("temp", 0)) - float(prev2.get("temp", 0))
            peak_lock_confirmed = (t1 <= -0.2 and t0 <= -0.2)
    except Exception:
        peak_lock_confirmed = False

    # Predictive SPECI likelihood (before an actual SPECI arrives):
    # detect abnormal short-term changes that often trigger denser updates.
    speci_likely_score = 0.0
    try:
        if prev is not None:
            dt1 = float(latest.get("temp", 0)) - float(prev.get("temp", 0))
            if abs(dt1) >= 1.2:
                speci_likely_score += 0.90
            if abs(dt1) >= 2.0:
                speci_likely_score += 0.50

            if dewpoint_step is not None and abs(float(dewpoint_step)) >= 1.5:
                speci_likely_score += 0.35
            if wind_speed_step is not None and abs(float(wind_speed_step)) >= 6.0:
                speci_likely_score += 0.60
            if wind_dir_change is not None and float(wind_dir_change) >= 50.0:
                speci_likely_score += 0.45
            if pressure_step is not None and abs(float(pressure_step)) >= 1.2:
                speci_likely_score += 0.25

        if wx_trend in {"new", "intensify"}:
            speci_likely_score += 1.00
        elif wx_trend in {"weaken", "end"}:
            speci_likely_score += 0.20
        if wx_state_now in {"heavy", "convective"}:
            speci_likely_score += 0.90

        if "快速回补" in cloud_tr or "明显开窗" in cloud_tr:
            speci_likely_score += 0.45

        if routine_cadence_min is not None:
            if routine_cadence_min >= 50:
                speci_likely_score += 0.25
            elif routine_cadence_min <= 25:
                speci_likely_score -= 0.15

        if recent_interval_min is not None and routine_cadence_min is not None and recent_interval_min < 0.75 * routine_cadence_min:
            # already denser than routine cadence, likely in active weather mode
            speci_likely_score += 0.35
    except Exception:
        speci_likely_score = 0.0

    speci_likely_threshold = 1.35
    try:
        if speci_ratio_24h >= 0.10:
            speci_likely_threshold -= 0.12
        elif speci_ratio_24h >= 0.05:
            speci_likely_threshold -= 0.07
        elif report_count_24h >= 20 and speci_ratio_24h <= 0.01:
            speci_likely_threshold += 0.05

        if rapid_temp_jump_count_24h >= 4 and speci_count_24h == 0:
            speci_likely_threshold += 0.08
        if wx_transition_count_24h >= 3:
            speci_likely_threshold -= 0.05
        if routine_cadence_min is not None and routine_cadence_min >= 50 and speci_count_24h >= 1:
            speci_likely_threshold -= 0.05
    except Exception:
        pass
    speci_likely_threshold = max(1.10, min(1.55, float(speci_likely_threshold)))

    speci_likely = bool((not speci_active) and (speci_likely_score >= speci_likely_threshold))

    return "\n".join(lines), {
        "latest_report_utc": _metar_obs_time_utc(latest).isoformat().replace('+00:00', 'Z'),
        "latest_report_local": latest_dt_local.isoformat(),
        "temp_bias_c": bias,
        "temp_bias_smooth_c": bias_smooth,
        "pressure_bias_hpa": p_bias,
        "latest_wdir": latest.get("wdir"),
        "latest_wspd": latest.get("wspd"),
        "latest_temp": latest.get("temp"),
        "latest_dewpoint": latest.get("dewp"),
        "latest_rh": _calc_rh_pct(latest.get("temp"), latest.get("dewp")),
        "dewpoint_trend_1step_c": dewpoint_step,
        "latest_cloud_code": latest_cloud_code,
        "latest_wx": latest.get("wxString") or latest.get("wx") or "",
        "latest_precip_state": wx_state_now,
        "precip_trend": wx_trend,
        "cloud_trend": cloud_tr,
        "cloud_effective_cover": c_eff_now,
        "cloud_effective_cover_smooth": (0.6 * c_eff_now + 0.3 * c_eff_prev + 0.1 * c_eff_prev2) if (c_eff_prev is not None and c_eff_prev2 is not None) else c_eff_now,
        "cloud_transmittance": t_cloud_now,
        "wx_transmittance": t_wx_now,
        "radiation_eff": rad_eff_now,
        "radiation_eff_smooth": rad_eff_smooth,
        "radiation_eff_trend_1step": rad_eff_trend,
        "temp_trend_1step_c": t_trend,
        "temp_trend_smooth_c": t_trend_smooth,
        "temp_accel_2step_c": temp_accel_2step,
        "metar_temp_quantized": _is_intish(latest.get("temp")) and (prev is None or _is_intish(prev.get("temp"))),
        "pressure_trend_1step_hpa": pressure_step,
        "wind_dir_change_deg": wind_dir_change,
        "wind_speed_trend_1step_kt": wind_speed_step,
        "metar_routine_cadence_min": routine_cadence_min,
        "metar_recent_interval_min": recent_interval_min,
        "metar_speci_active": speci_active,
        "metar_speci_likely": speci_likely,
        "metar_speci_likely_score": round(speci_likely_score, 2),
        "metar_speci_likely_threshold": round(speci_likely_threshold, 2),
        "metar_speci_count_24h": int(speci_count_24h),
        "metar_speci_ratio_24h": float(speci_ratio_24h),
        "metar_report_count_24h": int(report_count_24h),
        "metar_rapid_temp_jump_count_24h": int(rapid_temp_jump_count_24h),
        "metar_rapid_wind_jump_count_24h": int(rapid_wind_jump_count_24h),
        "metar_wx_transition_count_24h": int(wx_transition_count_24h),
        "peak_lock_confirmed": peak_lock_confirmed,
        "observed_max_temp_c": obs_max_temp,
        "observed_max_interval_lo_c": obs_max_interval_lo,
        "observed_max_interval_hi_c": obs_max_interval_hi,
        "observed_today_range_c": obs_today_range_c,
        "observed_prev_day_range_c": obs_prev_day_range_c,
        "model_day_range_c": model_day_range_c,
        "observed_max_time_local": obs_max_time_local.isoformat() if obs_max_time_local else None,
    }
