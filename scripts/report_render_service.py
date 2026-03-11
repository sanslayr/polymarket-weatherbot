#!/usr/bin/env python3
"""Section rendering service for /look report."""

from __future__ import annotations

import re
from datetime import datetime
from typing import Any

from analysis_snapshot_service import build_analysis_snapshot
from polymarket_render_service import _build_polymarket_section
from report_focus_service import build_report_focus_bundle
from station_catalog import resolve_station, station_meta_for

PHASE_LABELS = {
    "far": "远离窗口",
    "near_window": "接近窗口",
    "in_window": "窗口内",
    "post": "窗口后",
    "early_peak_watch": "早峰后观察",
    "unknown": "窗口状态未知",
}
DEFAULT_TRACK_LINE = "• 临窗前继续跟踪温度斜率与风向节奏，必要时再改判。"
FAR_REPORT_HOURS_THRESHOLD = 10.0
NEAR_REPORT_HOURS_THRESHOLD = 6.0

STRONG_BACKGROUND_KEYS = (
    "暖输送",
    "冷输送",
    "暖平流",
    "冷平流",
    "副高",
    "副热带高压",
    "高压脊",
    "低压槽",
    "深槽",
    "槽前",
    "槽后",
    "锋",
    "斜压",
    "切变",
    "低云",
    "云量",
    "雾",
    "封盖",
    "干层",
    "湿层",
    "混合",
    "逆温",
    "稳层",
    "边界层",
    "海风",
    "湖风",
    "偏南风",
    "偏北风",
    "偏东风",
    "偏西风",
    "下沉",
    "抬升",
)

GENERIC_BACKGROUND_KEYS = (
    "短时改写幅度有限",
    "更偏实况触发",
    "暂时看不出明显偏高或偏低",
    "暂未识别到单独可追踪的近站系统",
    "低层风场和午后升温效率共同作用",
    "后段升温能否继续维持",
    "午后升温效率",
)

WEAK_BACKGROUND_MECHANISMS = {
    "低层气流配置仍在主导升温节奏",
    "混合层加深幅度仍是关键约束",
    "云量演变仍是关键变量",
    "近地风向切换时点仍是关键变量",
    "锋面附近风场仍在调整",
}

GENERIC_FOCUS_KEYS = (
    "优先盯下一报温度斜率、风向节奏和云量是否继续支持当前路径",
    "优先看温度斜率和低层风向是否继续配合",
    "临窗前继续跟踪温度斜率与风向节奏，必要时再改判",
    "当前更该看环流、云量和低层风场配置会不会延续",
    "当前先看环流、云量和低层风场配置是否继续维持",
    "优先看云量、近地风场和温度斜率能否继续配合",
    "若三者同步走强，再考虑上修后段上沿",
)


def _safe_float(value: Any) -> float | None:
    try:
        if value in (None, ""):
            return None
        return float(value)
    except Exception:
        return None


def _parse_iso_dt(value: Any) -> datetime | None:
    try:
        text = str(value or "").strip()
        return datetime.fromisoformat(text) if text else None
    except Exception:
        return None


def _hours_between(later: datetime | None, earlier: datetime | None) -> float | None:
    if later is None or earlier is None:
        return None
    try:
        if later.tzinfo is not None and earlier.tzinfo is None:
            earlier = earlier.replace(tzinfo=later.tzinfo)
        elif later.tzinfo is None and earlier.tzinfo is not None:
            later = later.replace(tzinfo=earlier.tzinfo)
    except Exception:
        pass
    try:
        return (later - earlier).total_seconds() / 3600.0
    except Exception:
        return None


def _format_local_clock(value: Any) -> str:
    dt = _parse_iso_dt(value)
    if not dt:
        return ""
    try:
        return dt.strftime("%H:%M Local")
    except Exception:
        return ""


def _clean_synoptic_line(line: str) -> str:
    txt = str(line or "").strip()
    if not txt:
        return ""
    if txt.startswith("🧭"):
        txt = txt[1:].strip()
    if txt.startswith("-"):
        txt = txt[1:].strip()
    txt = txt.replace("**", "").strip()
    if txt.startswith("环流形势对最高温影响"):
        txt = txt.removeprefix("环流形势对最高温影响").strip("：: ")
    if txt.startswith("主导机制："):
        txt = txt.removeprefix("主导机制：").strip()
    elif txt.startswith("主导机制:"):
        txt = txt.removeprefix("主导机制:").strip()
    return txt.strip()


def _compact_synoptic_block(lines: list[str]) -> str:
    compact_parts: list[str] = []
    seen: set[str] = set()
    for raw in lines:
        cleaned = _clean_synoptic_line(raw).rstrip("。；，")
        if not cleaned:
            continue
        if cleaned in seen:
            continue
        seen.add(cleaned)
        compact_parts.append(cleaned)
    if not compact_parts:
        return "🧭 环流形势：结构化环流摘要缺失，需回退到原始诊断。"
    return f"🧭 环流形势：{'；'.join(compact_parts)}。"


def _normalize_synoptic_text(text: str) -> str:
    cleaned = _clean_synoptic_line(text)
    if not cleaned:
        return ""
    for prefix in (
        "今天没有特别单一的主导因素，",
        "今天更要看",
        "今天先看",
        "重点看",
        "重点盯",
        "当前更像",
        "更像",
    ):
        if cleaned.startswith(prefix):
            cleaned = cleaned[len(prefix):].strip()
    cleaned = cleaned.replace("后段更要看", "重点看")
    cleaned = cleaned.replace("先看", "重点看", 1) if cleaned.startswith("先看") else cleaned
    cleaned = cleaned.replace("；这会一起决定后段升温还能不能延续", "")
    cleaned = cleaned.replace("；这会决定后段升温还能不能延续", "")
    cleaned = cleaned.replace("；这会一起决定午后还能不能继续升温", "")
    cleaned = cleaned.replace("；这会决定午后还能不能继续升温", "")
    cleaned = re.sub(r"\s+", " ", cleaned)
    return cleaned.strip().rstrip("。；，")


def _has_strong_background_signal(text: str) -> bool:
    txt = _normalize_synoptic_text(text)
    if not txt:
        return False
    if any(key in txt for key in GENERIC_BACKGROUND_KEYS):
        return False
    return any(key in txt for key in STRONG_BACKGROUND_KEYS)


def _summarize_impact_text(text: str) -> str:
    cleaned = _normalize_synoptic_text(text)
    if not cleaned or any(key in cleaned for key in GENERIC_BACKGROUND_KEYS):
        return ""
    head = cleaned.split("；", 1)[0].strip()
    mapping = (
        ("更可能比原先预报略高", "最高温倾向略偏上沿"),
        ("更可能比原先预报略低", "最高温倾向略偏下沿"),
        ("上沿仍有一点上修空间", "最高温上沿仍有小幅上修空间"),
        ("上沿有一点上修空间", "最高温上沿仍有小幅上修空间"),
        ("上沿有一点受压风险", "最高温上沿仍有受压风险"),
        ("更容易被压住", "最高温更容易被压住"),
    )
    for src, dst in mapping:
        if src in head:
            return dst
    return head


def _station_label(snapshot: dict[str, Any], metar_diag: dict[str, Any]) -> str:
    station_hint = str(metar_diag.get("station_icao") or "").strip().upper()
    if not station_hint:
        posterior = dict(snapshot.get("posterior_feature_vector") or {})
        meta = dict(posterior.get("meta") or {})
        station_hint = str(meta.get("station") or "").strip()
    if not station_hint:
        canonical = dict(snapshot.get("canonical_raw_state") or {})
        forecast = dict(canonical.get("forecast") or {})
        meta = dict(forecast.get("meta") or {})
        station_hint = str(meta.get("station") or "").strip()
    if not station_hint:
        return ""
    try:
        return str(resolve_station(station_hint).city or station_hint).strip()
    except Exception:
        return station_hint


def _station_icao(snapshot: dict[str, Any], metar_diag: dict[str, Any]) -> str:
    station_hint = str(metar_diag.get("station_icao") or "").strip().upper()
    if station_hint:
        return station_hint
    posterior = dict(snapshot.get("posterior_feature_vector") or {})
    meta = dict(posterior.get("meta") or {})
    station_hint = str(meta.get("station") or "").strip().upper()
    if station_hint:
        return station_hint
    canonical = dict(snapshot.get("canonical_raw_state") or {})
    forecast = dict(canonical.get("forecast") or {})
    meta = dict(forecast.get("meta") or {})
    return str(meta.get("station") or "").strip().upper()


def _append_unique_text(bucket: list[str], text: str) -> None:
    candidate = _normalize_synoptic_text(text)
    if not candidate:
        return
    for existing in bucket:
        if candidate in existing or existing in candidate:
            return
    bucket.append(candidate)


def _short_mechanism_text(text: str) -> str:
    cleaned = _normalize_synoptic_text(text)
    if not cleaned:
        return ""
    if "，" in cleaned:
        head = cleaned.split("，", 1)[0].strip()
        if _has_strong_background_signal(head):
            return head
    return cleaned


def _station_context_label(snapshot: dict[str, Any], metar_diag: dict[str, Any]) -> str:
    city = _station_label(snapshot, metar_diag)
    icao = _station_icao(snapshot, metar_diag)
    meta = station_meta_for(icao) if icao else {}
    site_tag = str(meta.get("site_tag") or "").strip()
    terrain = str(meta.get("terrain") or "").strip()
    water_factor = str(meta.get("water_factor") or "").strip()
    urban_position = str(meta.get("urban_position") or "").strip()

    if site_tag:
        if site_tag.endswith("机场"):
            return f"{city}{site_tag}"
        if any(key in site_tag for key in ("海", "湾", "湖")):
            return f"{city}这类{site_tag}"
        return f"{city}{site_tag}"
    if water_factor in {"近水体影响", "沿海影响", "河口影响"}:
        return f"{city}这类近水体站"
    if urban_position:
        if "城中" in urban_position or "主城" in urban_position:
            return f"{city}{urban_position.replace('(', '（').replace(')', '）')}"
        if "郊" in urban_position:
            return f"{city}这类{urban_position.replace('(', '（').replace(')', '）')}"
    if terrain:
        return f"{city}这类{terrain}"
    return city


def _far_mechanism_focus(text: str) -> str:
    normalized = _short_mechanism_text(text)
    if not normalized:
        return ""
    mapping = (
        (("锋", "斜压"), "锋面/斜压带怎么摆动"),
        (("冷平流", "冷输送"), "冷空气压制能否持续"),
        (("暖平流", "暖输送"), "暖空气输送能否持续落地"),
        (("低云", "雾", "封盖", "稳层", "逆温"), "低云稳层何时松动"),
        (("混合", "干层", "下沉"), "混合层能否顺利做深"),
        (("海风", "湖风", "偏南风", "偏北风", "偏东风", "偏西风"), "近地风向切换能否真正落地"),
    )
    for keys, phrase in mapping:
        if any(key in normalized for key in keys):
            return phrase
    return normalized


def _historical_profile_summary_lines(metar_diag: dict[str, Any]) -> list[str]:
    context = metar_diag.get("historical_context")
    if not isinstance(context, dict):
        return []
    summary_lines = context.get("summary_lines")
    if not isinstance(summary_lines, list):
        return []
    out: list[str] = []
    for raw in summary_lines:
        line = str(raw or "").strip()
        if not line.startswith("站点背景摘要："):
            continue
        item = line.split("：", 1)[1].strip() if "：" in line else ""
        if item:
            out.append(item.rstrip("。"))
    return out


def _best_far_clause(text: str) -> str:
    normalized = _normalize_synoptic_text(text)
    if not normalized:
        return ""
    clauses = [part.strip(" ，。；") for part in normalized.split("；") if part.strip(" ，。；")]
    if not clauses:
        return normalized
    best = ""
    best_score = -999
    for clause in clauses:
        score = _background_score(clause)
        if any(key in clause for key in ("峰值", "尾段", "风向", "低云", "混合", "暖平流", "冷平流", "近水体")):
            score += 2
        if score > best_score:
            best = clause
            best_score = score
    return best or normalized


def _pick_far_basis_text(snapshot: dict[str, Any], syn_lines: list[str]) -> str:
    synoptic_summary = dict(snapshot.get("synoptic_summary") or {})
    summary = dict(synoptic_summary.get("summary") or {})
    regime = dict(snapshot.get("boundary_layer_regime") or {})
    thermo = dict(regime.get("thermo") or {})

    ranked_candidates: list[tuple[int, str]] = []

    def _add_candidate(text: Any, base_score: int) -> None:
        clause = _best_far_clause(str(text or ""))
        if not clause:
            return
        score = _background_score(clause)
        if any(key in clause for key in ("层结信号不算突出", "模式剖面信号中性", "先看实况升温节奏怎么走")):
            score -= 5
        if score < 1:
            return
        ranked_candidates.append((score + base_score, clause))

    _add_candidate(regime.get("layer_summary"), 6)
    _add_candidate(regime.get("headline"), 5)
    for idx, finding in enumerate(list(thermo.get("layer_findings") or [])[:2]):
        _add_candidate(finding, 4 - idx)
    _add_candidate(summary.get("pathway"), 3)
    for idx, raw in enumerate(syn_lines[:4]):
        _add_candidate(raw, max(0, 2 - idx))

    if not ranked_candidates:
        return ""
    ranked_candidates.sort(key=lambda item: item[0], reverse=True)
    return ranked_candidates[0][1]


def _rephrase_far_basis_text(text: str) -> str:
    clause = _best_far_clause(text).rstrip("。；，")
    if not clause:
        return ""
    replacements = (
        ("今天更要看", "更看"),
        ("今天先看", "更看"),
        ("当前更要看", "更看"),
        ("当前主看", "更看"),
        ("当前更像", "更像"),
        ("重点看", "更看"),
        ("后段更要看", "后段更看"),
        ("先看", "更看"),
    )
    out = clause
    out = out.replace("925–850混合偏弱", "低层混合偏弱")
    out = out.replace("925-850混合偏弱", "低层混合偏弱")
    out = out.replace("925–850混合潜力尚可", "低层混合条件还可以")
    out = out.replace("925-850混合潜力尚可", "低层混合条件还可以")
    for src, dst in replacements:
        if out.startswith(src):
            out = dst + out[len(src):]
            break
    out = out.strip(" ，")
    if out.startswith("今天"):
        out = out[2:].strip(" ，")
    if out.startswith("当前"):
        out = out[2:].strip(" ，")
    return out


def _far_profile_outlook_line(metar_diag: dict[str, Any]) -> str:
    for note in _historical_profile_summary_lines(metar_diag):
        txt = note.rstrip("。")
        if "峰值偏早" in txt or "锁温" in txt or "回落" in txt:
            return "站点历史上更常见的是偏早见顶，后段锁温或回落，不太会一路冲高"
        if "晚峰站" in txt or "尾段升温" in txt or "16-18L" in txt:
            return "站点历史上更该防的是 16-18L 这段尾段升温，不是太早封顶"
        if "偏暖风向" in txt and "偏冷风向" in txt:
            return "站点历史上暖冷两类风向能拉开明显温差，关键还是看低层风向最后站哪边"
        if "风向切换敏感" in txt:
            return "站点历史上风向一旦切过去，区间上下沿都会被很快改写"
        if "日较差偏小" in txt:
            return "站点历史上日较差不算大，云层或近水体更容易压住上沿"
    return ""


def _is_generic_far_text(text: str) -> bool:
    txt = _normalize_synoptic_text(text)
    if not txt:
        return True
    return any(key in txt for key in GENERIC_BACKGROUND_KEYS) or "暂未识别到单独可追踪的近站系统" in txt


def _far_profile_lead_phrase(metar_diag: dict[str, Any]) -> str:
    for note in _historical_profile_summary_lines(metar_diag):
        txt = note.rstrip("。")
        if "峰值偏早" in txt or "锁温" in txt or "回落" in txt:
            return "偏早见顶，后段更常见锁温或回落"
        if "晚峰站" in txt or "尾段升温" in txt or "16-18L" in txt:
            return "晚峰结构更明显，真正的变数在 16-18L 这段尾段升温"
        if "偏暖风向" in txt and "偏冷风向" in txt:
            return "低层风向主导性很强，暖冷两类路径会把区间明显拉开"
        if "日较差偏小" in txt:
            return "整体更像窄振幅结构，上沿不容易被轻易打开"
    return ""


def _far_future_setup_line(mechanism: str, impact: str, metar_diag: dict[str, Any]) -> str:
    profile_line = _far_profile_outlook_line(metar_diag)
    if profile_line:
        return profile_line
    mechanism_txt = str(mechanism or "").strip()
    impact_txt = str(impact or "").strip()
    if any(key in impact_txt for key in ("偏下沿", "受压", "更容易被压住", "偏受限", "空间有限")):
        return "到峰值窗前后，若这套形势未明显减弱，温度更容易贴着区间下半段运行"
    if any(key in impact_txt for key in ("偏上沿", "上修空间")):
        return "到峰值窗前后，若地面继续配合，温度还有摸上区间上沿的机会"
    if any(key in mechanism_txt for key in ("低云", "稳层", "逆温")):
        return "到峰值窗前后，更看低云稳层能否解除；若消散偏慢，上沿就需按保守情景处理"
    if any(key in mechanism_txt for key in ("混合", "干层", "边界层")):
        return "到峰值窗前后，更看混合层能否顺利加深；若加深不足，冲高幅度将受到限制"
    if any(key in mechanism_txt for key in ("风向", "偏南风", "偏北风", "偏东风", "偏西风")):
        return "到峰值窗前后，更看低层风向切换能否落地；一旦切过去，区间上下沿都可能被改写"
    return ""


def _far_line_overlap(lead: str, follow: str) -> bool:
    lead_txt = _normalize_synoptic_text(lead)
    follow_txt = _normalize_synoptic_text(follow)
    if not lead_txt or not follow_txt:
        return False
    keys = ("尾段升温", "锁温", "回落", "风向", "低云", "混合", "晚峰", "近水体")
    shared = [key for key in keys if key in lead_txt and key in follow_txt]
    return len(shared) >= 1


def _wind_sector_cn(deg: float | None) -> str:
    if deg is None:
        return ""
    value = float(deg) % 360.0
    if value < 45 or value >= 315:
        return "偏北风"
    if value < 135:
        return "偏东风"
    if value < 225:
        return "偏南风"
    return "偏西风"


def _coastal_flow_mechanism(snapshot: dict[str, Any], metar_diag: dict[str, Any]) -> str:
    icao = _station_icao(snapshot, metar_diag)
    if not icao:
        return ""
    meta = station_meta_for(icao) if icao else {}
    site_tag = str(meta.get("site_tag") or "").strip()
    water_factor = str(meta.get("water_factor") or "").strip()
    if not site_tag.endswith("机场"):
        return ""
    if not any(key in site_tag for key in ("海", "湾", "填海")) and water_factor not in {"近水体影响", "沿海影响", "河口影响"}:
        return ""
    sector = _wind_sector_cn(_safe_float(metar_diag.get("latest_wdir")))
    if not sector:
        return ""
    if sector in {"偏北风", "偏东风"}:
        return f"{sector}和近水体影响仍在"
    if sector == "偏南风":
        return "近地偏南风已开始接管"
    return "近地偏西风正在增强"


def _far_directional_take(mechanism: str, impact: str) -> str:
    impact_txt = str(impact or "").strip()
    mechanism_txt = str(mechanism or "").strip()
    if "近水体影响仍在" in mechanism_txt:
        return "午后上沿更看这股压制何时松动"
    if "偏南风已开始接管" in mechanism_txt or "偏西风正在增强" in mechanism_txt:
        return "若午后继续维持，升温上沿还有打开空间"
    if any(key in impact_txt for key in ("偏下沿", "受压", "更容易被压住")):
        return "这套形势若维持，午后上沿更容易受压"
    if any(key in impact_txt for key in ("偏上沿", "上修空间")):
        return "这套形势若维持，午后上沿还有打开空间"
    if any(key in mechanism_txt for key in ("锋面/斜压", "冷空气压制", "低云稳层")):
        return "若它迟迟不松动，白天冲高幅度就会受限"
    if any(key in mechanism_txt for key in ("暖空气输送", "混合层")):
        return "若后续继续配合，白天仍有再抬一截的可能"
    if "风向切换" in mechanism_txt:
        return "风向一旦提前切换，区间上沿和下沿都会跟着改写"
    return impact_txt or ""


def _pick_peak_local(snapshot: dict[str, Any], primary_window: dict[str, Any]) -> Any:
    canonical = dict(snapshot.get("canonical_raw_state") or {})
    window = dict(canonical.get("window") or {})
    calc_window = dict(window.get("calc") or {})
    primary_state = dict(window.get("primary") or {})
    return calc_window.get("peak_local") or primary_state.get("peak_local") or primary_window.get("peak_local")


def _classify_report_mode(
    snapshot: dict[str, Any],
    primary_window: dict[str, Any],
    metar_diag: dict[str, Any],
    phase_now: str,
) -> str:
    posterior = dict(snapshot.get("posterior_feature_vector") or {})
    time_phase = dict(posterior.get("time_phase") or {})
    hours_to_peak = _safe_float(time_phase.get("hours_to_peak"))
    peak_dt = _parse_iso_dt(_pick_peak_local(snapshot, primary_window))
    latest_dt = _parse_iso_dt(metar_diag.get("latest_report_local"))

    if hours_to_peak is None:
        hours_to_peak = _hours_between(peak_dt, latest_dt)

    peak_summary = dict(dict(snapshot.get("peak_data") or {}).get("summary") or {})
    ranges = dict(peak_summary.get("ranges") or {})
    core_range = dict(ranges.get("core") or {})
    core_lo = _safe_float(core_range.get("lo"))
    latest_temp = _safe_float(metar_diag.get("latest_temp"))
    observed_max = _safe_float(metar_diag.get("observed_max_temp_c"))
    temp_trend = _safe_float(metar_diag.get("temp_trend_1step_c"))

    cross_day = bool(peak_dt and latest_dt and peak_dt.date() != latest_dt.date())
    if cross_day:
        return "far_synoptic"
    if hours_to_peak is not None and hours_to_peak >= FAR_REPORT_HOURS_THRESHOLD:
        return "far_synoptic"

    near_trigger = False
    if hours_to_peak is not None and hours_to_peak < NEAR_REPORT_HOURS_THRESHOLD:
        near_trigger = True
    if latest_temp is not None and core_lo is not None and latest_temp >= core_lo - 1.0:
        near_trigger = True
    if temp_trend is not None and temp_trend >= 0.3:
        near_trigger = True
    if latest_temp is not None and observed_max is not None and latest_temp >= observed_max - 0.2:
        near_trigger = True

    if near_trigger:
        return "near_obs"
    if hours_to_peak is not None:
        return "transition"
    if phase_now in {"near_window", "in_window", "post", "early_peak_watch"}:
        return "near_obs"
    return "far_synoptic" if phase_now == "far" else "transition"


def _tighten_block_spacing(text: str) -> str:
    block = str(text or "").strip()
    if not block:
        return ""
    block = re.sub(r"\n{3,}", "\n\n", block)
    block = re.sub(r"\n\n(?=• 实况提醒：)", "\n", block)
    block = re.sub(r"\n\n(?=⚠️ 关注)", "\n", block)
    return block


def _join_report_parts(parts: list[str], *, compact_after: set[int] | None = None) -> str:
    items = [_tighten_block_spacing(part) for part in parts if str(part or "").strip()]
    if not items:
        return ""
    compact_after = compact_after or set()
    out = [items[0]]
    for idx, item in enumerate(items[1:], start=1):
        sep = "\n" if (idx - 1) in compact_after else "\n\n"
        out.append(sep + item)
    return "".join(out)


def _short_cloud_text(metar_diag: dict[str, Any]) -> str:
    raw_tokens = metar_diag.get("latest_cloud_tokens")
    tokens = [str(item).strip() for item in raw_tokens] if isinstance(raw_tokens, list) else []
    tokens = [item for item in tokens if item]
    if tokens:
        if len(tokens) > 2:
            return "/".join(tokens[:2]) + "…"
        return "/".join(tokens)
    code = str(metar_diag.get("latest_cloud_code") or "").strip().upper()
    if code:
        return code
    layers = str(metar_diag.get("latest_cloud_layers") or "").strip()
    if not layers:
        return ""
    return layers if len(layers) <= 24 else layers[:24].rstrip() + "…"


def _format_temp_delta(delta_c: Any, unit: str) -> str:
    value = _safe_float(delta_c)
    if value is None:
        return ""
    if unit == "F":
        value = value * 9.0 / 5.0
    if abs(value) < 0.05:
        return "持平"
    if abs(value - round(value)) < 0.05:
        return f"{value:+.0f}°{unit}"
    return f"{value:+.1f}°{unit}"


def _fmt_obs_temp(v_c: Any, unit: str) -> str:
    value = _safe_float(v_c)
    if value is None:
        return ""
    if unit == "F":
        value = value * 9.0 / 5.0 + 32.0
    if abs(value - round(value)) < 0.05:
        return f"{value:.0f}°{unit}"
    return f"{value:.1f}°{unit}"


def _build_obs_focus_metar_block(
    metar_diag: dict[str, Any],
    *,
    unit: str,
    fmt_temp,
    fallback_text: str,
    metar_analysis_lines: list[str] | None = None,
) -> str:
    latest_temp = _safe_float(metar_diag.get("latest_temp"))
    observed_max = _safe_float(metar_diag.get("observed_max_temp_c"))
    latest_time = _format_local_clock(metar_diag.get("latest_report_local"))
    max_time = _format_local_clock(metar_diag.get("observed_max_time_local"))

    if latest_temp is None and observed_max is None:
        return "📡 实况：" + str(fallback_text or "METAR 实况摘要缺失。").strip()

    parts: list[str] = ["📡 实况："]
    if latest_time:
        parts.append(f"{latest_time} ")
    if latest_temp is not None:
        parts.append(fmt_temp(latest_temp))
    if observed_max is not None:
        if max_time and max_time != latest_time:
            parts.append(f"，今日已到 {fmt_temp(observed_max)}（{max_time}）")
        else:
            parts.append(f"，今日已到 {fmt_temp(observed_max)}")
    lead_line = "".join(parts).strip() + "。"

    detail_bits: list[str] = []
    wind_dir = metar_diag.get("latest_wdir")
    wind_spd = _safe_float(metar_diag.get("latest_wspd"))
    try:
        if wind_dir not in (None, "", "VRB") and wind_spd is not None:
            detail_bits.append(f"风 {float(wind_dir):.0f}° {wind_spd:.0f}kt")
        elif wind_spd is not None:
            detail_bits.append(f"风速 {wind_spd:.0f}kt")
    except Exception:
        pass

    cloud_text = _short_cloud_text(metar_diag)
    if cloud_text:
        detail_bits.append(f"云 {cloud_text}")

    temp_delta = _format_temp_delta(metar_diag.get("temp_trend_1step_c"), unit)
    if temp_delta:
        detail_bits.append(f"较上一报 {temp_delta}")

    wx_state = str(metar_diag.get("latest_precip_state") or "").strip().lower()
    if wx_state and wx_state not in {"none", "unknown"}:
        detail_bits.append(f"天气 {wx_state}")

    lines = [lead_line]
    if detail_bits:
        lines.append("• " + " | ".join(detail_bits[:4]))

    first_analysis = ""
    if metar_analysis_lines:
        for raw in metar_analysis_lines:
            cleaned = str(raw or "").strip()
            if cleaned:
                first_analysis = cleaned if cleaned.startswith("•") else f"• {cleaned}"
                break
    if first_analysis:
        lines.append(first_analysis)
    return "\n".join(lines)


def _build_far_obs_reference(
    metar_diag: dict[str, Any],
    *,
    unit: str,
    fmt_temp,
    fallback_text: str,
) -> str:
    latest_temp = _safe_float(metar_diag.get("latest_temp"))
    latest_time = _format_local_clock(metar_diag.get("latest_report_local"))
    detail_bits: list[str] = []
    if latest_time:
        detail_bits.append(latest_time)
    if latest_temp is not None:
        detail_bits.append(fmt_temp(latest_temp))
    wind_dir = metar_diag.get("latest_wdir")
    wind_spd = _safe_float(metar_diag.get("latest_wspd"))
    try:
        if wind_dir not in (None, "", "VRB") and wind_spd is not None:
            detail_bits.append(f"风 {float(wind_dir):.0f}° {wind_spd:.0f}kt")
        elif wind_spd is not None:
            detail_bits.append(f"风速 {wind_spd:.0f}kt")
    except Exception:
        pass
    cloud_text = _short_cloud_text(metar_diag)
    if cloud_text:
        detail_bits.append(f"云 {cloud_text}")
    if detail_bits:
        return f"📡 当前实况：{' | '.join(detail_bits)}（仅作背景参考）。"
    fallback = str(fallback_text or "").strip()
    return f"📡 当前实况：{fallback}" if fallback else "📡 当前实况：METAR 实况摘要缺失。"


def _build_far_synoptic_block(snapshot: dict[str, Any], syn_lines: list[str], metar_diag: dict[str, Any]) -> str:
    synoptic_summary = dict(snapshot.get("synoptic_summary") or {})
    summary = dict(synoptic_summary.get("summary") or {})
    basis_text = _pick_far_basis_text(snapshot, syn_lines)
    mechanism = _rephrase_far_basis_text(basis_text) or _far_mechanism_focus(summary.get("pathway"))
    if _is_generic_far_text(mechanism):
        mechanism = _far_profile_lead_phrase(metar_diag)
    impact_text = _summarize_impact_text(summary.get("impact"))
    future_line = _far_future_setup_line(mechanism, impact_text, metar_diag)
    lead_clause = _background_compact_clause(mechanism, _far_directional_take(mechanism, impact_text))
    lead = f"• {lead_clause}。" if lead_clause else ""

    if lead and future_line and _far_line_overlap(lead, future_line):
        future_line = ""

    if lead and future_line:
        return "🧭 环流：\n" + lead + "\n" + f"• {future_line}。"
    if lead:
        return "🧭 环流：\n" + lead
    directional = _far_directional_take(mechanism, impact_text)
    if mechanism and directional:
        return f"🧭 环流：当前主看{mechanism}，{directional}。"
    if mechanism:
        return f"🧭 环流：当前主看{mechanism}。"
    fallback = _compact_synoptic_block(syn_lines)
    return fallback.replace("环流形势", "环流", 1)


def _natural_flow_chain_line(city: str, mechanism: str, impact: str) -> str:
    mechanism_txt = str(mechanism or "").strip()
    impact_txt = str(impact or "").strip()
    place = city or "这里"
    if not mechanism_txt:
        return ""
    if mechanism_txt.endswith("仍是关键变量"):
        return f"• {place}当前{mechanism_txt}。"
    if mechanism_txt.endswith("仍是关键约束"):
        return f"• {place}当前{mechanism_txt}。"
    if "锋后偏南气流" in mechanism_txt:
        tail = "午后升温能否延续，取决于这股南风能否继续维持"
        if any(key in impact_txt for key in ("偏下沿", "受压", "更容易被压住", "偏受限", "空间有限")):
            tail = "但上沿仍应按受压情景处理"
        return f"• {place}当前仍受锋后偏南气流主导，{tail}。"
    if "锋后偏北气流" in mechanism_txt or "冷空气压制" in mechanism_txt:
        return f"• {place}当前仍处在冷空气压制之下，后续上探空间取决于这道压制何时解除。"
    if "低云稳层" in mechanism_txt:
        return f"• {place}当前主要受低云稳层限制，云层消散时点将直接影响午后上沿。"
    if "混合层" in mechanism_txt or "混匀" in mechanism_txt:
        return f"• {place}当前更接近混合尚未充分建立的结构，后续升温幅度主要取决于混合层能否继续加深。"
    if "近地偏南风" in mechanism_txt or "偏南风已开始接管" in mechanism_txt:
        return f"• {place}近地偏南风已开始主导，午后上沿能否进一步打开，取决于该风场能否继续维持。"
    if "近地偏西风" in mechanism_txt or "偏西风正在增强" in mechanism_txt:
        return f"• {place}近地偏西风正在增强，后续升温幅度取决于这股西风能否继续维持。"
    if "风向" in mechanism_txt or any(key in mechanism_txt for key in ("偏南风", "偏北风", "偏东风", "偏西风")):
        return f"• {place}当前主要矛盾在于低层风向切换时点，区间上下沿都会随之调整。"
    if mechanism_txt.startswith(("更看", "更像", "后段更看", "偏早见顶", "晚峰结构", "低层风向主导性很强", "整体更像")):
        return f"• {place}{mechanism_txt}。"
    return f"• {place}当前主要关注{mechanism_txt}。"


def _background_candidates(snapshot: dict[str, Any], *, include_lines: bool = True) -> list[str]:
    synoptic_summary = dict(snapshot.get("synoptic_summary") or {})
    summary = dict(synoptic_summary.get("summary") or {})
    regime = dict(snapshot.get("boundary_layer_regime") or {})
    candidates: list[str] = []
    for candidate in (
        summary.get("pathway"),
        regime.get("headline"),
    ):
        text = _normalize_synoptic_text(str(candidate or ""))
        if text:
            candidates.append(text)
    if include_lines:
        for raw in (synoptic_summary.get("lines") or []):
            text = _normalize_synoptic_text(str(raw or ""))
            if text and "结构化环流摘要缺失" not in text:
                candidates.append(text)
    return candidates


def _background_score(text: str) -> int:
    txt = _normalize_synoptic_text(text)
    if not txt or any(key in txt for key in GENERIC_BACKGROUND_KEYS):
        return -999
    score = 0
    weighted_keys = (
        ("冷平流", 7),
        ("冷输送", 7),
        ("暖平流", 7),
        ("暖输送", 7),
        ("海风", 6),
        ("湖风", 6),
        ("偏南风", 6),
        ("偏北风", 6),
        ("偏东风", 6),
        ("偏西风", 6),
        ("偏南气流", 6),
        ("偏北气流", 6),
        ("偏东气流", 6),
        ("偏西气流", 6),
        ("低云", 6),
        ("雾", 6),
        ("封盖", 6),
        ("逆温", 6),
        ("稳层", 6),
        ("混合", 6),
        ("干层", 5),
        ("湿层", 5),
        ("边界层", 5),
        ("下沉", 5),
        ("抬升", 5),
        ("切变", 4),
        ("锋后", 4),
        ("槽后", 4),
        ("锋前", 3),
        ("槽前", 3),
        ("锋", 1),
        ("斜压", 1),
        ("高压脊", 2),
        ("低压槽", 2),
    )
    for key, weight in weighted_keys:
        if key in txt:
            score += weight
    if "相关链路" in txt:
        score -= 6
    if "锋面/斜压调整" in txt or txt in {"锋面/斜压调整", "锋面/斜压"}:
        score -= 5
    if "主导机制" in txt:
        score += 1
    if any(key in txt for key in ("压制", "受限", "上探", "冲高", "抬升", "落地", "松动", "切换")):
        score += 2
    score += min(len(txt) // 14, 3)
    return score


def _pick_background_basis(snapshot: dict[str, Any], *, include_lines: bool = True) -> str:
    best = ""
    best_score = -999
    for candidate in _background_candidates(snapshot, include_lines=include_lines):
        score = _background_score(candidate)
        if score > best_score:
            best = candidate
            best_score = score
    return best if best_score >= 1 else ""


def _background_mechanism_text(text: str, impact: str, metar_diag: dict[str, Any]) -> str:
    normalized = _short_mechanism_text(text)
    if not normalized:
        return ""
    impact_text = str(impact or "").strip()
    latest_wdir = _safe_float(metar_diag.get("latest_wdir"))
    south_sector = latest_wdir is not None and 120.0 <= latest_wdir <= 240.0
    north_sector = latest_wdir is not None and (latest_wdir <= 60.0 or latest_wdir >= 300.0)

    if any(key in normalized for key in ("冷平流", "冷输送")):
        return "冷空气压制尚未解除"
    if any(key in normalized for key in ("暖平流", "暖输送")):
        return "暖空气输送仍在建立"
    if "锋后偏南气流" in normalized:
        return "锋后偏南气流仍在主导"
    if "锋后偏北气流" in normalized:
        return "锋后偏北气流压制仍在维持"
    if any(key in normalized for key in ("偏南气流", "偏北气流", "偏东气流", "偏西气流")):
        return "低层气流配置仍在主导升温节奏"
    if any(key in normalized for key in ("低云", "雾", "封盖", "稳层", "逆温")):
        return "低云稳层限制尚未解除"
    if any(key in normalized for key in ("混合", "干层", "边界层", "下沉")):
        return "混合层加深幅度仍是关键约束"
    if "云量" in normalized:
        return "云量演变仍是关键变量"
    if any(key in normalized for key in ("海风", "湖风", "偏南风", "偏北风", "偏东风", "偏西风")):
        return "近地风向切换时点仍是关键变量"
    if any(key in normalized for key in ("锋", "斜压")):
        if south_sector:
            return "锋后偏南气流仍在主导"
        if north_sector:
            return "锋后偏北气流压制仍在维持"
        if any(key in impact_text for key in ("偏下沿", "受压", "更容易被压住")):
            return "冷空气压制尚未解除"
        return "锋面附近风场仍在调整"
    return normalized


def _background_directional_take(mechanism: str, impact: str) -> str:
    mechanism_txt = str(mechanism or "").strip()
    impact_txt = str(impact or "").strip()
    if "近水体影响仍在" in mechanism_txt:
        return "午后上沿取决于这股近水体压制何时减弱"
    if "云量变化" in mechanism_txt:
        if any(key in impact_txt for key in ("偏下沿", "受压", "更容易被压住")):
            return "若云层继续回补，午后上沿将继续受压"
        return "午后最高温仍取决于云层是否继续回补"
    if "偏南风已开始接管" in mechanism_txt or "偏西风正在增强" in mechanism_txt:
        return "若该风场继续维持，升温上沿仍保留打开空间"
    if any(key in impact_txt for key in ("偏下沿", "受压", "更容易被压住", "偏受限", "空间有限")):
        if any(key in mechanism_txt for key in ("低云稳层", "混合层")):
            return "午后上沿仍应按偏保守情景处理"
        return "午后进一步上探空间有限"
    if any(key in impact_txt for key in ("偏上沿", "上修空间")):
        return "若地面条件继续配合，上沿仍保留小幅上修空间"
    if "风向" in mechanism_txt:
        return "最终高点更取决于风向切换时点"
    if "混合层" in mechanism_txt:
        return "混合层加深程度将直接决定上沿还能否继续上修"
    if "低云稳层" in mechanism_txt:
        return "若云层消散偏慢，午后冲高幅度需按保守情景处理"
    return impact_txt


def _should_emit_background_line(
    basis: str,
    mechanism: str,
    directional: str,
    impact: str,
    coastal_mechanism: str,
) -> bool:
    basis_txt = _normalize_synoptic_text(basis)
    mechanism_txt = str(mechanism or "").strip()
    directional_txt = str(directional or "").strip()
    impact_txt = str(impact or "").strip()

    if not mechanism_txt and not directional_txt:
        return False

    clear_impact = any(key in impact_txt for key in ("偏上沿", "偏下沿", "上修空间", "受压", "更容易被压住"))
    basis_score = _background_score(basis_txt)
    strong_basis = basis_score >= 8 or _has_strong_background_signal(basis_txt)
    strong_mechanism = mechanism_txt not in WEAK_BACKGROUND_MECHANISMS and not _is_generic_far_text(mechanism_txt)
    coastal_focus = bool(coastal_mechanism and mechanism_txt == coastal_mechanism)
    hard_signal = mechanism_txt in {
        "冷空气压制尚未解除",
        "暖空气输送仍在建立",
        "锋后偏南气流仍在主导",
        "锋后偏北气流压制仍在维持",
        "低云稳层限制尚未解除",
    }

    if coastal_focus:
        return True
    if clear_impact and (strong_basis or strong_mechanism):
        return True
    if hard_signal and strong_basis:
        return True
    if strong_mechanism and strong_basis and directional_txt:
        return True
    return False


def _background_compact_clause(mechanism: str, directional: str) -> str:
    mechanism_txt = str(mechanism or "").strip().rstrip("。；，")
    directional_txt = str(directional or "").strip().rstrip("。；，")
    if mechanism_txt and directional_txt:
        if directional_txt in mechanism_txt:
            return mechanism_txt
        if mechanism_txt in directional_txt:
            return directional_txt
        return f"{mechanism_txt}；{directional_txt}"
    return mechanism_txt or directional_txt


def _build_background_synoptic_line(snapshot: dict[str, Any], metar_diag: dict[str, Any]) -> str:
    synoptic_summary = dict(snapshot.get("synoptic_summary") or {})
    summary = dict(synoptic_summary.get("summary") or {})

    impact = _summarize_impact_text(summary.get("impact"))
    basis = _pick_background_basis(snapshot, include_lines=False)
    mechanism = _background_mechanism_text(basis, impact, metar_diag)
    coastal_mechanism = _coastal_flow_mechanism(snapshot, metar_diag)
    if coastal_mechanism and (not mechanism or mechanism in {"后面主要看混合层还能不能继续做深", "锋面附近风场仍在调整"}):
        mechanism = coastal_mechanism
    combined = f"{basis} {impact}".strip()
    if not mechanism and not impact:
        return ""

    directional = _background_directional_take(mechanism, impact)
    lower_basis = combined.lower()
    if not directional and ("front" in lower_basis or "baroclinic" in lower_basis):
        directional = "午后再往上冲的空间会小一些"
    if not _should_emit_background_line(basis, mechanism, directional, impact, coastal_mechanism):
        return ""

    compact_clause = _background_compact_clause(mechanism, directional)
    return f"🧭 背景：{compact_clause}。"


def _range_target_text(
    *,
    unit: str,
    display_lo: float,
    display_hi: float,
    core_lo: float,
    core_hi: float,
) -> str:
    def _fmt_range(lo: float, hi: float) -> str:
        if unit == "F":
            lo_u = lo * 9.0 / 5.0 + 32.0
            hi_u = hi * 9.0 / 5.0 + 32.0
            return f"{lo_u:.1f}~{hi_u:.1f}°F"
        return f"{lo:.1f}~{hi:.1f}°C"

    if core_lo > display_lo and core_hi < display_hi:
        return f"区间先放在 {_fmt_range(core_lo, core_hi)}，两边各留一点机动"
    if core_lo > display_lo:
        return f"区间先放在 {_fmt_range(core_lo, core_hi)}，下沿留给偏冷回摆"
    if core_hi < display_hi:
        return f"区间先放在 {_fmt_range(core_lo, core_hi)}，上沿留给临窗再抬"
    return f"区间先放在 {_fmt_range(core_lo, core_hi)}"


def _obs_reasoning_line(latest_temp: float | None, trend: float | None, unit: str) -> str:
    if latest_temp is None:
        return ""
    temp_txt = _fmt_obs_temp(latest_temp, unit)
    if trend is None or abs(trend) < 0.15:
        return f"最新报还在 {temp_txt} 一带横着走，短时没看到立刻冲顶的速度"
    if trend >= 0.35:
        return f"最新报已经到 {temp_txt}，而且还在往上爬，但这股斜率能不能续上还要看下一报"
    return f"最新报冲到 {temp_txt} 后有点放缓，后面再大幅上冲要看下一报能不能重新提速"


def _impact_reasoning_text(impact: str) -> str:
    impact_txt = str(impact or "").strip()
    if any(key in impact_txt for key in ("偏下沿", "受压", "更容易被压住", "偏受限", "空间有限")):
        return "上沿仍宜按受压情景处理"
    if any(key in impact_txt for key in ("偏上沿", "上修空间")):
        return "上沿仍保留小幅上修空间"
    return ""


def _mechanism_condition_text(mechanism: str) -> str:
    mechanism_txt = str(mechanism or "").strip()
    if not mechanism_txt:
        return ""
    replacements = (
        ("已开始接管", "继续接管"),
        ("正在增强", "继续增强"),
    )
    out = mechanism_txt
    for src, dst in replacements:
        if src in out:
            out = out.replace(src, dst)
            break
    return out


def _mechanism_reasoning_line(mechanism: str, impact: str, range_target: str) -> str:
    mechanism_txt = str(mechanism or "").strip()
    if not mechanism_txt:
        return ""
    impact_txt = _impact_reasoning_text(impact)
    condition = _mechanism_condition_text(mechanism_txt)
    if "锋后偏南气流" in mechanism_txt:
        if impact_txt:
            return f"在锋后偏南气流持续维持的前提下，{range_target}，{impact_txt}"
        return f"在锋后偏南气流持续维持的前提下，{range_target}"
    if "锋后偏北气流" in mechanism_txt or "冷空气压制" in mechanism_txt:
        return f"若冷空气压制尚未解除，{range_target}"
    if "低云稳层" in mechanism_txt:
        return f"若低云稳层限制持续，{range_target}"
    if "混合层" in mechanism_txt:
        return f"若混合层加深幅度受限，冲高空间将受到约束，{range_target}"
    if "近地偏南风" in mechanism_txt or "偏南风已开始接管" in mechanism_txt:
        if impact_txt:
            return f"在偏南风持续维持的前提下，{range_target}，{impact_txt}"
        return f"在偏南风持续维持的前提下，{range_target}"
    if "近地偏西风" in mechanism_txt or "偏西风正在增强" in mechanism_txt:
        if impact_txt:
            return f"在偏西风继续增强的前提下，{range_target}，{impact_txt}"
        return f"在偏西风继续增强的前提下，{range_target}"
    if condition and impact_txt:
        return f"若{condition}继续维持，{range_target}，{impact_txt}"
    if condition:
        return f"若{condition}继续维持，{range_target}"
    return ""


def _parse_market_tagged_rows(poly_block: str) -> list[dict[str, str]]:
    tagged: list[dict[str, str]] = []
    for raw in str(poly_block or "").splitlines():
        match = re.search(
            r"\*\*(.+?)（(👍最有可能|😇潜在Alpha)）：Bid\s+([^|]+)\|\s+Ask\s+(.+?)\*\*",
            raw.strip(),
        )
        if not match:
            continue
        tagged.append(
            {
                "label": match.group(1).strip(),
                "tag": match.group(2).strip(),
                "bid": match.group(3).strip(),
                "ask": match.group(4).strip(),
            }
        )
    return tagged


def _build_range_rationale_block(
    snapshot: dict[str, Any],
    metar_diag: dict[str, Any],
    poly_block: str,
    *,
    background_line: str = "",
    unit: str,
    fmt_temp,
    display_lo: float,
    display_hi: float,
    core_lo: float,
    core_hi: float,
) -> str:
    if not str(poly_block or "").strip():
        return ""

    lines = ["**判断依据**"]
    latest_temp = _safe_float(metar_diag.get("latest_temp"))
    trend = _safe_float(metar_diag.get("temp_trend_1step_c"))
    obs_line = _obs_reasoning_line(latest_temp, trend, unit)
    if obs_line:
        lines.append(f"• {obs_line}。")

    synoptic_summary = dict(snapshot.get("synoptic_summary") or {})
    summary = dict(synoptic_summary.get("summary") or {})
    syn_lines = [str(item) for item in (synoptic_summary.get("lines") or []) if str(item).strip()]
    mechanism_basis = (
        _short_mechanism_text(summary.get("pathway"))
        or _pick_background_basis(snapshot, include_lines=False)
        or _short_mechanism_text(dict(snapshot.get("boundary_layer_regime") or {}).get("headline"))
        or _pick_far_basis_text(snapshot, syn_lines)
    )
    mechanism = _background_mechanism_text(mechanism_basis, "", metar_diag) or _short_mechanism_text(mechanism_basis)
    coastal_mechanism = _coastal_flow_mechanism(snapshot, metar_diag)
    if coastal_mechanism and (not mechanism or mechanism in {"后面主要看混合层还能不能继续做深", "锋面附近风场仍在调整"}):
        mechanism = coastal_mechanism
    impact = _summarize_impact_text(summary.get("impact"))
    background_txt = _normalize_synoptic_text(str(background_line or "").replace("🧭 背景：", ""))
    if coastal_mechanism and background_txt and coastal_mechanism in background_txt:
        mechanism = coastal_mechanism

    range_target = _range_target_text(
        unit=unit,
        display_lo=display_lo,
        display_hi=display_hi,
        core_lo=core_lo,
        core_hi=core_hi,
    )

    concise_impact = _impact_reasoning_text(impact)

    mechanism_repeated = bool(mechanism and background_txt and mechanism in background_txt)
    impact_repeated = bool(concise_impact and background_txt and concise_impact in background_txt)

    if mechanism and not mechanism_repeated:
        detail_line = _mechanism_reasoning_line(mechanism, impact, range_target)
        if detail_line:
            lines.append(f"• {detail_line}。")
        else:
            lines.append(f"• {range_target}。")
    elif concise_impact and not impact_repeated:
        lines.append(f"• 眼下更像是{concise_impact}，所以{range_target}。")
    elif impact:
        lines.append(f"• 从当前链路看，{impact}，所以{range_target}。")
    else:
        lines.append(f"• {range_target}。")

    if len(lines) > 3:
        lines = lines[:3]
    if len(lines) == 1:
        return ""
    return "\n".join(lines)


def _compact_focus_block(lines: list[str], *, report_mode: str) -> str:
    if not lines:
        return ""
    header = str(lines[0] or "").strip()
    detail = ""
    for raw in lines[1:]:
        cleaned = str(raw or "").strip()
        if cleaned:
            detail = cleaned.lstrip("• ").strip()
            break
    if not detail:
        return ""
    if report_mode == "near_obs" and (
        "当前更该看环流" in detail or "当前先看环流" in detail or "更细的实况校正留到临近目标窗再做" in detail
    ):
        detail = "优先盯下一报温度斜率、风向节奏和云量是否继续支持当前路径"
    if any(key in detail for key in GENERIC_FOCUS_KEYS):
        return ""
    detail = detail.rstrip("。；，")
    match = re.search(r"关注变量[（(]([^）)]+)[）)]", header)
    if match:
        return f"⚠️ 关注（{match.group(1).strip()}）：{detail}。"
    return f"⚠️ 关注：{detail}。"


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


def _build_far_metar_block(
    metar_diag: dict[str, Any],
    unit: str,
    fmt_temp,
) -> str:
    lines = ["📡 **当前实况参考（降级）**"]
    latest_local = str(metar_diag.get("latest_report_local") or "").strip()
    latest_time = ""
    if latest_local:
        try:
            latest_time = datetime.fromisoformat(latest_local).strftime("%H:%M Local")
        except Exception:
            latest_time = ""
    if latest_time:
        lines.append(f"• 最新报：{latest_time}")

    compact_bits: list[str] = []
    try:
        latest_temp = metar_diag.get("latest_temp")
        if latest_temp is not None:
            compact_bits.append(f"气温 {fmt_temp(float(latest_temp))}")
    except Exception:
        pass

    try:
        latest_wdir = metar_diag.get("latest_wdir")
        latest_wspd = metar_diag.get("latest_wspd")
        if latest_wdir not in (None, "") and latest_wspd not in (None, ""):
            compact_bits.append(f"风 {float(latest_wdir):.0f}° {float(latest_wspd):.0f}kt")
        elif latest_wspd not in (None, ""):
            compact_bits.append(f"风速 {float(latest_wspd):.0f}kt")
    except Exception:
        pass

    cloud_layers = str(metar_diag.get("latest_cloud_layers") or "").strip()
    if cloud_layers:
        compact_bits.append(f"云层 {cloud_layers}")

    if compact_bits:
        lines.append(f"• {' | '.join(compact_bits)}")

    lines.append("• 目标峰值窗仍远，当前实况只作背景参考；主判断以预报环流和后续演变为主。")
    return "\n".join(lines)


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
    temp_shape_analysis: dict[str, Any] | None = None,
    analysis_snapshot: dict[str, Any] | None = None,
) -> str:
    """Render-only section builder."""

    unit = "F" if str(temp_unit).upper() == "F" else "C"

    def _to_unit(c: float) -> float:
        return (c * 9.0 / 5.0 + 32.0) if unit == "F" else c

    def _fmt_temp(v_c: float) -> str:
        v = _to_unit(float(v_c))
        return f"{v:.1f}°{unit}"

    snapshot = analysis_snapshot if isinstance(analysis_snapshot, dict) else build_analysis_snapshot(
        primary_window=primary_window,
        metar_diag=metar_diag,
        forecast_decision=forecast_decision,
        temp_unit=unit,
        synoptic_window=synoptic_window,
        temp_shape_analysis=temp_shape_analysis,
    )

    synoptic_summary = dict(snapshot.get("synoptic_summary") or {})
    syn_lines = [str(item) for item in (synoptic_summary.get("lines") or []) if str(item).strip()]
    if not syn_lines:
        syn_lines = ["🧭 **环流形势对最高温影响**", "- 结构化环流摘要缺失，需回退到原始诊断。"]
    boundary_layer_regime = dict(snapshot.get("boundary_layer_regime") or {})
    regime_headline = str(boundary_layer_regime.get("headline") or "").strip()
    if regime_headline:
        mechanism_line = f"- **主导机制**：{regime_headline}"
        replaced = False
        for idx, line in enumerate(syn_lines):
            if "主导机制" in line:
                syn_lines[idx] = mechanism_line
                replaced = True
                break
        if not replaced:
            syn_lines = syn_lines[:1] + [mechanism_line] + syn_lines[1:]

    syn_lines = syn_lines[:5]

    temp_phase_decision = dict(snapshot.get("temp_phase_decision") or {})
    peak_data = dict(snapshot.get("peak_data") or {})
    weather_posterior = dict(snapshot.get("weather_posterior") or {})
    peak_summary = dict(peak_data.get("summary") or {})
    peak_range_block = [str(item) for item in (peak_data.get("block") or []) if str(item).strip()]
    peak_ranges = dict(peak_summary.get("ranges") or {})
    peak_display_range = dict(peak_ranges.get("display") or {})
    peak_core_range = dict(peak_ranges.get("core") or {})
    phase_now = str(peak_summary.get("phase_now") or "unknown")
    disp_lo = float(peak_display_range.get("lo"))
    disp_hi = float(peak_display_range.get("hi"))
    core_lo = float(peak_core_range.get("lo"))
    core_hi = float(peak_core_range.get("hi"))
    far_from_window = phase_now == "far"
    report_mode = _classify_report_mode(snapshot, primary_window, metar_diag, phase_now)

    metar_block = _build_far_metar_block(
        metar_diag=metar_diag,
        unit=unit,
        fmt_temp=_fmt_temp,
    ) if far_from_window else _build_metar_block(
        metar_diag=metar_diag,
        metar_text=metar_text,
        unit=unit,
        fmt_temp=_fmt_temp,
    )

    report_focus = build_report_focus_bundle(
        primary_window=primary_window,
        metar_diag=metar_diag,
        analysis_snapshot=snapshot,
    )
    vars_block = [str(item) for item in (report_focus.get("vars_block") or []) if str(item).strip()]
    if not vars_block:
        vars_block = [f"⚠️ **关注变量**（{PHASE_LABELS.get(phase_now, PHASE_LABELS['unknown'])}）", DEFAULT_TRACK_LINE]
    focus_block = _compact_focus_block(vars_block, report_mode=report_mode)

    metar_analysis_lines = [str(item) for item in (report_focus.get("metar_analysis_lines") or []) if str(item).strip()]
    if report_mode in {"near_obs", "transition"}:
        metar_block = _build_metar_block(
            metar_diag=metar_diag,
            metar_text=metar_text,
            unit=unit,
            fmt_temp=_fmt_temp,
        )
        if metar_analysis_lines:
            extra_lines: list[str] = []
            for raw in metar_analysis_lines:
                cleaned = str(raw or "").strip()
                if cleaned and cleaned not in metar_block:
                    extra_lines.append(cleaned)
            if extra_lines:
                metar_block = metar_block + "\n" + "\n".join(extra_lines)
    elif far_from_window:
        metar_block = _build_far_obs_reference(
            metar_diag=metar_diag,
            unit=unit,
            fmt_temp=_fmt_temp,
            fallback_text=metar_text,
        )

    label_policy = dict(report_focus.get("market_label_policy") or {})
    range_hint = {
        # Keep the market ladder aligned with the same peak-range block shown above.
        # Using a broader posterior-only hint here can produce contradictory output
        # such as "likely capped" in the peak block while tagging a hotter tail bin
        # as "most likely" in the Polymarket block.
        "display_lo": float(disp_lo),
        "display_hi": float(disp_hi),
        "core_lo": float(core_lo),
        "core_hi": float(core_hi),
    }

    background_line = "" if report_mode == "far_synoptic" else _build_background_synoptic_line(snapshot, metar_diag)

    poly_block = ""
    range_rationale_block = ""
    market_weather_anchor = {
        "latest_temp_c": metar_diag.get("latest_temp"),
        "observed_max_temp_c": metar_diag.get("observed_max_temp_c"),
    }
    if str(polymarket_event_url or "").strip():
        try:
            poly_block = _build_polymarket_section(
                polymarket_event_url,
                primary_window,
                weather_anchor=market_weather_anchor,
                range_hint=range_hint,
                allow_best_label=bool(label_policy.get("allow_best_label", True)),
                allow_alpha_label=bool(label_policy.get("allow_alpha_label", True)),
                label_policy=label_policy,
                prefetched_event=polymarket_prefetched_event,
            )
            if str(poly_block).startswith("Polymarket："):
                poly_block = ""
            elif poly_block:
                range_rationale_block = _build_range_rationale_block(
                    snapshot,
                    metar_diag,
                    poly_block,
                    background_line=background_line,
                    unit=unit,
                    fmt_temp=_fmt_temp,
                    display_lo=float(disp_lo),
                    display_hi=float(disp_hi),
                    core_lo=float(core_lo),
                    core_hi=float(core_hi),
                )
        except Exception:
            poly_block = ""
            range_rationale_block = ""

    compact_after: set[int] = set()
    if report_mode == "far_synoptic":
        synoptic_block = _build_far_synoptic_block(snapshot, syn_lines, metar_diag)
        parts = [
            synoptic_block,
            "\n".join(peak_range_block),
        ]
        if focus_block:
            parts.append(focus_block)
            compact_after.add(len(parts) - 2)
        if metar_block:
            parts.append(metar_block)
    else:
        parts = []
        if background_line:
            parts.append(background_line)
        parts.extend([
            metar_block,
            "\n".join(peak_range_block),
        ])
        if focus_block:
            parts.append(focus_block)
            compact_after.add(len(parts) - 2)
    if range_rationale_block:
        parts.append(range_rationale_block)
        compact_after.add(len(parts) - 2)
    if poly_block:
        parts.append(poly_block)
    return _join_report_parts(parts, compact_after=compact_after)
