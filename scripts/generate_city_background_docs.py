#!/usr/bin/env python3
"""Generate per-city METAR historical background markdown docs."""

from __future__ import annotations

import csv
import gzip
import os
import re
import statistics
from collections import Counter, defaultdict
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

from city_profile_overrides import CITY_CLIMATE_WINDOWS, CITY_PROFILE_OVERRIDES
from historical_context_provider import regime_to_cn, translate_special_features

ROOT = Path(__file__).resolve().parent.parent
REFERENCE_DIR = ROOT / "cache" / "historical_reference"
PRIOR_CSV = REFERENCE_DIR / "weatherbot_station_priors.csv"
MONTHLY_CSV = REFERENCE_DIR / "weatherbot_monthly_climatology.csv"
DAILY_CSV = REFERENCE_DIR / "weatherbot_daily_local_regimes.csv"
STATION_CSV = ROOT / "station_links.csv"
DOCS_DIR = ROOT / "docs" / "operations" / "city-background"
PROFILES_DIR = DOCS_DIR / "profiles"
RAW_ISD_DIR = Path(
    os.getenv(
        "WEATHER_ARCHIVE_RAW_ISD_DIR",
        str(ROOT.parent / "polymarket-weather-archive" / "data" / "raw" / "metar_isd"),
    )
)
HOURLY_CACHE: dict[tuple[str, str], list[dict[str, object]]] = {}

MONTH_NAMES = {
    1: "Jan",
    2: "Feb",
    3: "Mar",
    4: "Apr",
    5: "May",
    6: "Jun",
    7: "Jul",
    8: "Aug",
    9: "Sep",
    10: "Oct",
    11: "Nov",
    12: "Dec",
}

BACKGROUND_SPECS: list[dict[str, object]] = [
    {
        "key": "cloud_suppressed",
        "label": "云压制背景",
        "match": lambda row: _is_true(row.get("cloud_suppressed_flag")),
        "note": "优先盯低云持续性和午前升温恢复，不要把暂时偏冷直接当全天失败。",
    },
    {
        "key": "cloud_break_rebound",
        "label": "开窗反弹背景",
        "match": lambda row: _is_true(row.get("cloud_break_rebound_flag")),
        "note": "一旦云量退场且斜率恢复，后段补涨常比当前温度更重要。",
    },
    {
        "key": "rain_reset",
        "label": "降水重置背景",
        "match": lambda row: _is_true(row.get("rain_reset_flag")),
        "note": "先防高估，除非午前明显放晴且地面升温重新建立。",
    },
    {
        "key": "humid_sticky",
        "label": "湿热滞留背景",
        "match": lambda row: _is_true(row.get("humid_sticky_flag")),
        "note": "露点和湿层厚度权重应抬高，单看气温斜率容易误判。",
    },
    {
        "key": "dry_mixing",
        "label": "干混合背景",
        "match": lambda row: _is_true(row.get("dry_mixing_flag")),
        "note": "晴空低云少时要防保守低估，尤其在高日较差站点。",
    },
    {
        "key": "wind_shift",
        "label": "风向切换背景",
        "match": lambda row: _is_true(row.get("wind_shift_transition_flag")),
        "note": "转向应被视作 live pivot，常比同小时模型误差更早提示路径变化。",
    },
    {
        "key": "low_visibility_stagnation",
        "label": "低能见 / 静稳背景",
        "match": lambda row: _is_true(row.get("low_visibility_day_flag")) or _is_true(row.get("light_wind_stagnation_flag")),
        "note": "先看雾霾/湿层散除时点，再判断是否真正进入有效混合。",
    },
    {
        "key": "late_surge",
        "label": "末段冲高背景",
        "match": lambda row: _is_true(row.get("late_surge_flag")),
        "note": "15L 后仍有斜率时不要过早封顶，需保留尾段上冲空间。",
    },
    {
        "key": "clean_solar_ramp",
        "label": "晴空增温背景",
        "match": lambda row: _is_true(row.get("clean_solar_ramp_flag")),
        "note": "若无低云和降水残余干扰，可沿上沿路径处理。",
    },
]

SEASONAL_SITUATION_SPECS: list[tuple[str, str]] = [
    ("midday_low_ceiling_flag", "午间低云压制"),
    ("rain_reset_flag", "降水重置"),
    ("low_visibility_day_flag", "低能见 / 静稳"),
    ("dry_mixing_flag", "干混合"),
    ("clean_solar_ramp_flag", "晴空增温"),
    ("late_surge_flag", "末段冲高"),
    ("wind_shift_transition_flag", "风向切换"),
    ("humid_sticky_flag", "湿热滞留"),
]


@dataclass(frozen=True)
class ClimateWindow:
    label: str
    months: frozenset[int]


def main() -> None:
    priors = _load_csv(PRIOR_CSV)
    monthly_rows = _load_csv(MONTHLY_CSV)
    daily_rows = _load_csv(DAILY_CSV)
    stations = {row["icao"].upper(): row for row in _load_csv(STATION_CSV)}

    PROFILES_DIR.mkdir(parents=True, exist_ok=True)
    generated_paths: list[Path] = []
    for prior in sorted(priors, key=lambda row: row["station_id"]):
        station_id = str(prior["station_id"]).upper()
        station = stations.get(station_id)
        if station is None:
            continue
        monthly = [row for row in monthly_rows if str(row["station_id"]).upper() == station_id]
        daily = [row for row in daily_rows if str(row["station_id"]).upper() == station_id]
        content = _render_city_background(prior, station, monthly, daily)
        output_path = PROFILES_DIR / f"{station_id}_{_slug(station['city'])}.md"
        output_path.write_text(content, encoding="utf-8")
        generated_paths.append(output_path)

    _write_index(generated_paths)
    print(f"generated_profiles={len(generated_paths)}")
    print(f"output_dir={PROFILES_DIR}")


def _render_city_background(
    prior: dict[str, str],
    station: dict[str, str],
    monthly_rows: list[dict[str, str]],
    daily_rows: list[dict[str, str]],
) -> str:
    station_id = str(prior["station_id"]).upper()
    city = station["city"]
    override = CITY_PROFILE_OVERRIDES.get(station_id, {})
    climate_windows = _get_climate_windows(station_id)
    daily_rows_by_window = _group_rows_by_climate_window(daily_rows, climate_windows)
    hourly_rows = _load_hourly_station_rows(station_id, str(prior.get("timezone") or station.get("timezone") or "UTC"))
    site_tag = station.get("site_tag") or station.get("terrain_tag") or "n/a"
    factor_summary = station.get("factor_summary") or "n/a"
    terrain_tag = station.get("terrain_tag") or "n/a"
    terrain_tag2 = station.get("terrain_tag2") or "n/a"
    water_sector = station.get("water_sector") or "n/a"
    urban_position = station.get("urban_position") or "n/a"
    water_factor = station.get("water_factor") or "n/a"
    city_sector = station.get("city_sector") or "n/a"
    related_docs = _related_manual_docs(station_id, city)

    hottest = sorted(monthly_rows, key=lambda row: _safe_float(row.get("tmax_median_c")) or -999, reverse=True)[:3]
    cloudiest = sorted(monthly_rows, key=lambda row: _safe_float(row.get("midday_low_ceiling_share")) or -1, reverse=True)[:3]
    rainiest = sorted(monthly_rows, key=lambda row: _safe_float(row.get("precip_day_share")) or -1, reverse=True)[:3]
    late_surge_months = sorted(monthly_rows, key=lambda row: _safe_float(row.get("late_surge_share")) or -1, reverse=True)[:2]
    humid_months = sorted(monthly_rows, key=lambda row: _safe_float(row.get("humid_sticky_share")) or -1, reverse=True)[:2]
    dry_months = sorted(monthly_rows, key=lambda row: _safe_float(row.get("dry_mixing_share")) or -1, reverse=True)[:2]

    lines: list[str] = []
    lines.append(f"# {station_id} ({city}) 城市背景 L2 画像")
    lines.append("")
    lines.append(f"- 站点：**{station_id} / {city}**")
    lines.append(_format_coords(station["lat"], station["lon"]))
    lines.append(f"- 固定站点标签：**{site_tag}**")
    lines.append(f"- 地形 / 水体标签：{terrain_tag} / {terrain_tag2}")
    lines.append(f"- 站点固定因子：{factor_summary}")
    lines.append(f"- 水体因子：{water_factor}；城市方位：{city_sector}；城市相对位置：{urban_position}")
    lines.append(f"- 历史样本覆盖：`{prior.get('years_covered', 'n/a')}`（METAR/ISD）")
    lines.append("")
    lines.append("---")
    lines.append("")
    lines.append("## 1) 结论摘要（历史 METAR + 站点背景）")
    lines.append("")
    for idx, bullet in enumerate(_summary_bullets(prior, monthly_rows, daily_rows, override, climate_windows, daily_rows_by_window), start=1):
        lines.append(f"{idx}. {bullet}")
    lines.append("")
    lines.append("## 2) L2 核心识别框架")
    lines.append("")
    core_identity = str(override.get("core_identity") or "该站暂无额外人工 override，当前以历史 METAR 条件统计为主。").strip()
    lines.append(f"- 核心定位：{core_identity}")
    decisive_factors = [str(item) for item in override.get("decisive_factors", [])]
    if decisive_factors:
        for item in decisive_factors:
            lines.append(f"- {item}")
    else:
        lines.append("- 当前无额外站点级经验备注，后续可补充人工研究。")
    lines.append("")
    lines.append("## 3) 站点固定背景")
    lines.append("")
    lines.append(f"- `site_tag`: {site_tag}")
    lines.append(f"- `factor_summary`: {factor_summary}")
    lines.append(f"- `dominant_wind_regimes`: {prior.get('dominant_wind_regimes', 'n/a')}")
    lines.append(f"- `terrain_sector`: {station.get('terrain_sector') or 'n/a'}；`water_sector`: {water_sector}；`city_distance_km`: {station.get('city_distance_km') or 'n/a'}")
    lines.append(f"- `climate_windows`: {_format_climate_windows(climate_windows)}")
    lines.append("")
    lines.append("## 4) 历史 METAR 关键特征")
    lines.append("")
    lines.append(f"- 站点历史画像：{translate_special_features(prior.get('special_features'))}")
    lines.append(
        f"- 暖日峰值时刻：中位 `{_fmt_hour(_safe_float(prior.get('warm_peak_hour_median')))}`；"
        f"P75 `{_fmt_hour(_safe_float(prior.get('warm_peak_hour_p75')))}`；"
        f"晚峰占比 `{_fmt_pct(_safe_float(prior.get('late_peak_share')))}`；"
        f"17点后峰值占比 `{_fmt_pct(_safe_float(prior.get('very_late_peak_share')))}`"
    )
    lines.append(
        f"- 日较差：中位 `{_fmt_c(_safe_float(prior.get('daily_range_median_c')))}`；"
        f"P90 `{_fmt_c(_safe_float(prior.get('daily_range_p90_c')))}`；"
        f"早晨/午前/午后升温中位 `{_fmt_c(_safe_float(prior.get('morning_warmup_median_c')))}` / "
        f"`{_fmt_c(_safe_float(prior.get('noon_warmup_median_c')))}` / "
        f"`{_fmt_c(_safe_float(prior.get('late_ramp_median_c')))}`"
    )
    lines.append(
        f"- 云与压制：晨间/午间/午后低云占比 `{_fmt_pct(_safe_float(prior.get('morning_low_ceiling_share')))}` / "
        f"`{_fmt_pct(_safe_float(prior.get('midday_low_ceiling_share')))}` / "
        f"`{_fmt_pct(_safe_float(prior.get('afternoon_low_ceiling_share')))}`"
    )
    lines.append(
        f"- 湿度与重置：热日午后露点中位 `{_fmt_c(_safe_float(prior.get('hot_day_dewpoint_median_c')))}`；"
        f"降水重置占比 `{_fmt_pct(_safe_float(prior.get('rain_reset_day_share')))}`；"
        f"湿热滞留占比 `{_fmt_pct(_safe_float(prior.get('humid_sticky_day_share')))}`；"
        f"干混合占比 `{_fmt_pct(_safe_float(prior.get('dry_mixing_day_share')))}`"
    )
    lines.append(
        f"- 风与能见度：轻风占比 `{_fmt_pct(_safe_float(prior.get('light_wind_share')))}`；"
        f"风向切换日占比 `{_fmt_pct(_safe_float(prior.get('wind_shift_day_share')))}`；"
        f"低能见度占比 `{_fmt_pct(_safe_float(prior.get('reduced_visibility_share')))}`"
    )
    _append_bullet_section(lines, "## 5) 风向 / 日内转换图谱", _wind_diurnal_lines(hourly_rows))
    _append_bullet_section(lines, "## 6) 云层结构 / 压制图谱", _cloud_structure_lines(daily_rows, hourly_rows))
    _append_bullet_section(
        lines,
        "## 7) 季节 / 气候情形分层判断",
        _seasonal_situation_lines(daily_rows, climate_windows, daily_rows_by_window),
    )
    _append_bullet_section(
        lines,
        "## 8) 条件化响应拆解",
        _conditional_response_lines(daily_rows, climate_windows, daily_rows_by_window),
    )
    _append_bullet_section(lines, "## 9) 全年背景频率概览", _weather_background_lines(daily_rows))
    lines.append("## 10) 季节性窗口")
    lines.append("")
    lines.append(f"- 最热月份：{_fmt_month_rank(hottest, 'tmax_median_c')}")
    lines.append(f"- 午间低云最重月份：{_fmt_month_rank(cloudiest, 'midday_low_ceiling_share', is_pct=True)}")
    lines.append(f"- 降水/重置风险更重月份：{_fmt_month_rank(rainiest, 'precip_day_share', is_pct=True)}")
    lines.append(f"- 末段冲高更常见月份：{_fmt_month_rank(late_surge_months, 'late_surge_share', is_pct=True)}")
    lines.append(f"- 湿热滞留更常见月份：{_fmt_month_rank(humid_months, 'humid_sticky_share', is_pct=True)}")
    lines.append(f"- 干混合上冲更常见月份：{_fmt_month_rank(dry_months, 'dry_mixing_share', is_pct=True)}")
    lines.append("")
    lines.append("## 11) Local Regime 图谱")
    lines.append("")
    for item in _regime_profile_lines(daily_rows):
        lines.append(f"- {item}")
    lines.append("")
    focus_lines = _focus_slice_lines(daily_rows, override)
    section_no = 12
    if focus_lines:
        lines.append(f"## {section_no}) 特征性天气 / 气候专题讨论")
        lines.append("")
        for item in focus_lines:
            lines.append(f"- {item}")
        lines.append("")
        section_no += 1
    lines.append(f"## {section_no}) 旧版易误判场景")
    lines.append("")
    for idx, item in enumerate(_failure_modes(prior, override), start=1):
        lines.append(f"{idx}. {item}")
    lines.append("")
    section_no += 1
    lines.append(f"## {section_no}) 实况优先观察顺序")
    lines.append("")
    for idx, item in enumerate(_watch_order(prior, monthly_rows, override), start=1):
        lines.append(f"{idx}. {item}")
    lines.append("")

    repo_notes = [str(item) for item in override.get("repo_notes", [])]
    section_no += 1
    if repo_notes:
        lines.append(f"## {section_no}) 仓库既有特殊规则 / 研究备注")
        lines.append("")
        for item in repo_notes:
            lines.append(f"- {item}")
        lines.append("")
        section_no += 1
    if related_docs:
        lines.append(f"## {section_no}) 相关既有文档")
        lines.append("")
        for rel in related_docs:
            lines.append(f"- `{rel}`")
        lines.append("")
        section_no += 1
    lines.append(f"## {section_no}) 备注")
    lines.append("")
    lines.append("- 本文由 archive 导出的历史 METAR 特征与站点固定背景联合生成，定位为更细的 L2 城市画像。")
    lines.append("- 用途：补足旧版零碎单点城市经验，形成统一、可回放、可更新的城市背景参考层。")
    lines.append("- 云量部分已开始显式利用原始 METAR / ISD 的多层云字段（如 `GA1/GA2/GA3/CIG`），避免把多层云结构过度简化成单一低云标签。")
    lines.append("- 这仍是局地 METAR 行为画像，不替代后续 ERA5 环流背景判断。")
    lines.append("")
    return "\n".join(lines)


def _write_index(generated_paths: list[Path]) -> None:
    manual_docs = sorted(
        path.relative_to(ROOT).as_posix()
        for path in DOCS_DIR.glob("*.md")
        if path.name != "README.md"
    )
    lines: list[str] = []
    lines.append("# City Background Docs")
    lines.append("")
    lines.append("本目录同时保留两类文档：")
    lines.append("")
    lines.append("- 人工研究归档：历史单城市调研、临时研究结论")
    lines.append("- 自动生成 L2 画像：基于 archive 的 2022-2025 METAR/ISD 历史特征 + 站点级特殊备注")
    lines.append("")
    lines.append("## 自动生成 L2 画像")
    lines.append("")
    for path in sorted(generated_paths):
        lines.append(f"- `{path.relative_to(ROOT).as_posix()}`")
    lines.append("")
    lines.append("## 既有人工研究")
    lines.append("")
    for rel in manual_docs:
        lines.append(f"- `{rel}`")
    lines.append("")
    lines.append("## 更新方式")
    lines.append("")
    lines.append("```bash")
    lines.append("python3 scripts/sync_historical_reference.py")
    lines.append("PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=scripts python3 scripts/generate_city_background_docs.py")
    lines.append("```")
    lines.append("")
    (DOCS_DIR / "README.md").write_text("\n".join(lines), encoding="utf-8")


def _append_bullet_section(lines: list[str], title: str, items: list[str]) -> None:
    lines.append(title)
    lines.append("")
    for item in items:
        lines.append(f"- {item}")
    lines.append("")


def _summary_bullets(
    prior: dict[str, str],
    monthly_rows: list[dict[str, str]],
    daily_rows: list[dict[str, str]],
    override: dict[str, object],
    climate_windows: list[ClimateWindow],
    daily_rows_by_window: dict[str, list[dict[str, str]]],
) -> list[str]:
    bullets: list[str] = []
    core_identity = str(override.get("core_identity") or "").strip()
    if core_identity:
        bullets.append(core_identity)

    late_peak_share = _safe_float(prior.get("late_peak_share")) or 0.0
    humid_share = _safe_float(prior.get("humid_sticky_day_share")) or 0.0
    cloud_break_share = _safe_float(prior.get("cloud_break_day_share")) or 0.0
    rain_reset_share = _safe_float(prior.get("rain_reset_day_share")) or 0.0
    wind_shift_share = _safe_float(prior.get("wind_shift_day_share")) or 0.0
    daily_range = _safe_float(prior.get("daily_range_median_c")) or 0.0
    midday_cloud = _safe_float(prior.get("midday_low_ceiling_share")) or 0.0
    dominant_wind = prior.get("dominant_wind_regimes") or "n/a"

    seasonal_wind_lines = _seasonal_wind_condition_lines(daily_rows, climate_windows, daily_rows_by_window)
    if seasonal_wind_lines:
        bullets.append(seasonal_wind_lines[0])
    if late_peak_share >= 0.55:
        bullets.append("晚峰占比高，若 15L 后仍保有斜率，不宜提前按内陆早峰模板封顶。")
    elif late_peak_share <= 0.20:
        bullets.append("峰值偏早，后段常见的是锁温或回落，而不是持续上冲。")
    if humid_share >= 0.18:
        bullets.append("湿热滞留占比不低，露点背景会显著改写升温效率，不能只看气温斜率。")
    if cloud_break_share >= 0.10:
        bullets.append("存在稳定的“上午压制、午后开窗”样本，实时云层变化应高权重处理。")
    if rain_reset_share >= 0.10:
        bullets.append("降水重置日占比不低，午前启动迟滞和地面湿润残留要单独处理。")
    if wind_shift_share >= 0.16:
        bullets.append(f"风向切换敏感，主导风向组合为 {dominant_wind}，转向常常比当前温度更早发出路径变化信号。")
    if daily_range >= 11.0:
        bullets.append("日较差偏大，晴空干混合日的上冲空间明显高于站点日常印象。")
    if daily_range <= 7.0:
        bullets.append("日较差偏小，近水体、云层或高湿对升温上限约束较强。")
    if midday_cloud >= 0.30:
        bullets.append("午间低云占比高，峰值窗云量持续性通常比单报温度更关键。")

    hottest = sorted(monthly_rows, key=lambda row: _safe_float(row.get("tmax_median_c")) or -999, reverse=True)[:1]
    if hottest:
        bullets.append(
            f"同月最热窗口通常出现在 {MONTH_NAMES.get(_safe_int(hottest[0].get('month')) or 0, 'n/a')}，"
            f"对应月度 Tmax 中位约 {_fmt_c(_safe_float(hottest[0].get('tmax_median_c')))}。"
        )
    regime_line = _top_regime_bullet(daily_rows)
    if regime_line:
        bullets.append(regime_line)
    return bullets[:6]


def _top_regime_bullet(daily_rows: list[dict[str, str]]) -> str | None:
    regime_counts = Counter(str(row.get("primary_regime") or "") for row in daily_rows if row.get("primary_regime"))
    if not regime_counts:
        return None
    selected = None
    for regime, count in regime_counts.most_common():
        if regime != "mixed_transitional":
            selected = (regime, count)
            break
    if selected is None:
        selected = regime_counts.most_common(1)[0]
    regime, count = selected
    return f"历史最有代表性的局地日型为“{regime_to_cn(regime)}”({count} 天)，这比旧版零碎经验更稳定。"


def _conditional_response_lines(
    daily_rows: list[dict[str, str]],
    climate_windows: list[ClimateWindow],
    daily_rows_by_window: dict[str, list[dict[str, str]]],
) -> list[str]:
    lines: list[str] = []
    lines.extend(_seasonal_wind_condition_lines(daily_rows, climate_windows, daily_rows_by_window))
    for flag, label in [
        ("midday_low_ceiling_flag", "午间低云日"),
        ("rain_reset_flag", "降水重置日"),
        ("low_visibility_day_flag", "低能见度日"),
        ("wind_shift_transition_flag", "风向切换日"),
    ]:
        line = _flag_effect_line(daily_rows, flag, label)
        if line:
            lines.append(line)
    late_surge_line = _late_surge_line(daily_rows)
    if late_surge_line:
        lines.append(late_surge_line)
    if not lines:
        lines.append("当前站点尚未提炼出稳定的条件化差异，说明实况路径的重要性高于单一静态标签。")
    return lines[:6]


def _wind_diurnal_lines(hourly_rows: list[dict[str, object]]) -> list[str]:
    if not hourly_rows:
        return ["未找到原始小时级 ISD/METAR 数据，暂无法生成更细的风向时段图谱。"]

    lines: list[str] = []
    sector_counter = Counter(
        str(row["wind_sector"])
        for row in hourly_rows
        if row.get("wind_sector")
    )
    total = max(sum(sector_counter.values()), 1)
    for sector, count in sector_counter.most_common(3):
        if count / total < 0.05:
            continue
        sector_rows = [row for row in hourly_rows if row.get("wind_sector") == sector]
        high_months = _top_value_counts(sector_rows, "local_month", MONTH_NAMES, unit="报")
        high_hours = _hour_window_text(sector_rows)
        detail = (
            f"{sector} 风：小时样本占比 `{_fmt_pct(count / total)}`；"
            f"高发月份 {high_months}；高发时段 {high_hours}"
        )
        lines.append(detail + "。")

    transition_lines = _dominant_transition_lines(hourly_rows)
    lines.extend(transition_lines)
    if not lines:
        lines.append("当前站点尚未提炼出稳定的风向/日内转换模式。")
    return lines[:5]


def _load_hourly_station_rows(station_id: str, timezone_name: str) -> list[dict[str, object]]:
    cache_key = (station_id, timezone_name)
    cached = HOURLY_CACHE.get(cache_key)
    if cached is not None:
        return cached

    station_dir = RAW_ISD_DIR / station_id
    if not station_dir.exists():
        HOURLY_CACHE[cache_key] = []
        return []

    try:
        zone = ZoneInfo(timezone_name)
    except Exception:  # noqa: BLE001
        zone = ZoneInfo("UTC")

    rows: list[dict[str, object]] = []
    for path in sorted(station_dir.glob("*.csv.gz")):
        with gzip.open(path, "rt", encoding="utf-8", newline="") as handle:
            reader = csv.DictReader(handle)
            for raw in reader:
                try:
                    utc_dt = datetime.fromisoformat(raw["DATE"]).replace(tzinfo=timezone.utc)
                except Exception:  # noqa: BLE001
                    continue
                local_dt = utc_dt.astimezone(zone)
                rows.append(
                    {
                        "local_date": str(local_dt.date()),
                        "local_month": local_dt.month,
                        "local_hour": local_dt.hour,
                        "wind_sector": _raw_wind_sector(raw.get("WND")),
                        "cloud_layer_count": _raw_cloud_layer_count(raw),
                        "low_ceiling_ft": _raw_cig_ft(raw.get("CIG")),
                    }
                )

    HOURLY_CACHE[cache_key] = rows
    return rows


def _dominant_transition_lines(hourly_rows: list[dict[str, object]]) -> list[str]:
    by_date: dict[str, list[dict[str, object]]] = defaultdict(list)
    for row in hourly_rows:
        if row.get("local_date"):
            by_date[str(row["local_date"])].append(row)

    transition_events: list[dict[str, object]] = []
    for local_date, rows in by_date.items():
        morning = [str(row["wind_sector"]) for row in rows if row.get("wind_sector") and 6 <= int(row["local_hour"]) <= 11]
        afternoon = [str(row["wind_sector"]) for row in rows if row.get("wind_sector") and 12 <= int(row["local_hour"]) <= 18]
        if not morning or not afternoon:
            continue
        morning_sector = Counter(morning).most_common(1)[0][0]
        afternoon_sector = Counter(afternoon).most_common(1)[0][0]
        if morning_sector == afternoon_sector:
            continue
        switch_hour = None
        for row in sorted(rows, key=lambda item: int(item["local_hour"])):
            if int(row["local_hour"]) >= 10 and row.get("wind_sector") == afternoon_sector:
                switch_hour = int(row["local_hour"])
                break
        transition_events.append(
            {
                "local_date": local_date,
                "pair": (morning_sector, afternoon_sector),
                "switch_hour": switch_hour,
                "month": rows[0]["local_month"],
            }
        )

    if not transition_events:
        return []

    transition_counter = Counter(tuple(event["pair"]) for event in transition_events)
    lines: list[str] = []
    for pair, count in transition_counter.most_common(2):
        if count < 15:
            continue
        pair_events = [event for event in transition_events if tuple(event["pair"]) == pair]
        switch_hours = [int(event["switch_hour"]) for event in pair_events if event.get("switch_hour") is not None]
        months = Counter(int(event["month"]) for event in pair_events if event.get("month") is not None)
        line = (
            f"高频上午→下午转换 `{pair[0]}→{pair[1]}`：`{count}` 天；"
            f"高发月份 {_top_counter_text(months, MONTH_NAMES, unit='天')}"
        )
        if switch_hours:
            line += f"；常见转换时段 `{_format_switch_window(switch_hours)}`"
        lines.append(line + "。")
    return lines


def _cloud_structure_lines(
    daily_rows: list[dict[str, str]],
    hourly_rows: list[dict[str, object]],
) -> list[str]:
    if not hourly_rows:
        return ["未找到原始小时级 ISD/METAR 数据，暂无法生成更细的云层结构图谱。"]

    lines: list[str] = []
    layer_rows = [row for row in hourly_rows if (row.get("cloud_layer_count") or 0) >= 2]
    if layer_rows:
        layer_share = len(layer_rows) / len(hourly_rows)
        months = _top_value_counts(layer_rows, "local_month", MONTH_NAMES, unit="报")
        hours = _hour_window_text(layer_rows)
        lines.append(
            f"多层云小时样本占比 `{_fmt_pct(layer_share)}`；高发月份 {months}；高发时段 {hours}。"
        )

    daily_index = {str(row.get('local_date')): row for row in daily_rows if row.get('local_date')}
    by_date: dict[str, list[dict[str, object]]] = defaultdict(list)
    for row in hourly_rows:
        local_date = row.get("local_date")
        if local_date:
            by_date[str(local_date)].append(row)

    multilayer_days: list[dict[str, str]] = []
    clearer_days: list[dict[str, str]] = []
    low_multilayer_days: list[dict[str, str]] = []
    for local_date, rows in by_date.items():
        midday = [row for row in rows if row.get("local_hour") is not None and 10 <= int(row["local_hour"]) <= 16]
        if not midday or local_date not in daily_index:
            continue
        layer_counts = [int(row.get("cloud_layer_count") or 0) for row in midday]
        low_ceils = [int(row["low_ceiling_ft"]) for row in midday if row.get("low_ceiling_ft") is not None]
        multilayer_share = sum(1 for value in layer_counts if value >= 2) / len(midday)
        any_cloud_share = sum(1 for value in layer_counts if value >= 1) / len(midday)
        low_ceiling_share = sum(1 for value in low_ceils if value <= 6500) / len(low_ceils) if low_ceils else 0.0
        matched = daily_index[local_date]
        if multilayer_share >= 0.5:
            multilayer_days.append(matched)
        if any_cloud_share <= 0.25 and low_ceiling_share <= 0.25:
            clearer_days.append(matched)
        if multilayer_share >= 0.5 and low_ceiling_share >= 0.5:
            low_multilayer_days.append(matched)

    compare_line = _cloud_compare_line(multilayer_days, clearer_days)
    if compare_line:
        lines.append(compare_line)
    low_line = _cloud_lowline(low_multilayer_days)
    if low_line:
        lines.append(low_line)

    if not lines:
        lines.append("当前站点尚未提炼出稳定的多层云 / 低顶云结构差异。")
    return lines[:4]


def _focus_slice_lines(daily_rows: list[dict[str, str]], override: dict[str, object]) -> list[str]:
    specs = override.get("focus_slices")
    if not isinstance(specs, list):
        return []

    lines: list[str] = []
    for spec in specs:
        if not isinstance(spec, dict):
            continue
        filtered = _filter_focus_rows(daily_rows, spec)
        if len(filtered) < int(spec.get("min_days", 25)):
            continue

        label = str(spec.get("label") or "专题切片")
        note = _clean_sentence(str(spec.get("note") or "").strip())
        sectors = spec.get("sectors")
        top_n = int(spec.get("top_n", 4))
        groups: list[tuple[str, list[dict[str, str]]]] = []
        grouped: dict[str, list[dict[str, str]]] = defaultdict(list)
        for row in filtered:
            sector = str(row.get("dominant_wind_sector") or "").strip()
            if sector:
                grouped[sector].append(row)

        if isinstance(sectors, list):
            for sector in sectors:
                sector_text = str(sector)
                if sector_text in grouped and len(grouped[sector_text]) >= int(spec.get("min_sector_days", 12)):
                    groups.append((sector_text, grouped[sector_text]))
        else:
            ranked = sorted(grouped.items(), key=lambda item: len(item[1]), reverse=True)
            for sector, rows in ranked[:top_n]:
                if len(rows) >= int(spec.get("min_sector_days", 12)):
                    groups.append((sector, rows))

        if not groups:
            continue

        parts: list[str] = [f"{label}：样本 `{len(filtered)}` 天"]
        for sector, rows in groups:
            parts.append(
                f"{sector} 风 `{len(rows)}` 天 -> `Tmax` `{_fmt_c(_median(rows, 'tmax_c'))}` / "
                f"`Tmin` `{_fmt_c(_median(rows, 'tmin_c'))}` / "
                f"日较差 `{_fmt_c(_median(rows, 'daily_range_c'))}` / "
                f"峰值 `{_fmt_hour(_median(rows, 'peak_hour_local'))}`"
            )
        if note:
            parts.append(f"注意：{note}")
        lines.append("；".join(parts) + "。")
    return lines


def _filter_focus_rows(daily_rows: list[dict[str, str]], spec: dict[str, object]) -> list[dict[str, str]]:
    months = {int(month) for month in spec.get("months", []) if isinstance(month, int)}
    conditions = spec.get("conditions", {})
    filtered: list[dict[str, str]] = []
    for row in daily_rows:
        month = _safe_int(row.get("month"))
        if months and month not in months:
            continue
        ok = True
        if isinstance(conditions, dict):
            for key, expected in conditions.items():
                current = row.get(str(key))
                if isinstance(expected, bool):
                    if _is_true(current) != expected:
                        ok = False
                        break
                else:
                    if str(current) != str(expected):
                        ok = False
                        break
        if ok:
            filtered.append(row)
    return filtered


def _weather_background_lines(daily_rows: list[dict[str, str]]) -> list[str]:
    total = max(len(daily_rows), 1)
    min_days = max(20, int(total * 0.04))
    ranked: list[tuple[int, str]] = []
    cached_rows: dict[str, list[dict[str, str]]] = {}
    fallback_rows: dict[str, list[dict[str, str]]] = {}

    for spec in BACKGROUND_SPECS:
        key = str(spec["key"])
        matcher = spec["match"]
        rows = [row for row in daily_rows if matcher(row)]
        if len(rows) < min_days:
            continue
        fallback_rows[key] = rows
        if len(rows) / total > 0.90:
            continue
        cached_rows[key] = rows
        ranked.append((len(rows), key))

    if not ranked and fallback_rows:
        key, rows = max(fallback_rows.items(), key=lambda item: len(item[1]))
        cached_rows[key] = rows
        ranked = [(len(rows), key)]

    if not ranked:
        return ["当前站点尚未提炼出稳定的天气背景类别，说明日常路径更依赖实时状态。"]

    lines: list[str] = []
    for _, key in sorted(ranked, reverse=True)[:5]:
        spec = next(item for item in BACKGROUND_SPECS if item["key"] == key)
        rows = cached_rows[key]
        share = len(rows) / total
        top_months = _top_months_for_rows(rows)
        tmax = _median(rows, "tmax_c")
        peak = _median(rows, "peak_hour_local")
        daily_range = _median(rows, "daily_range_c")
        noon = _median(rows, "warmup_noon_c")
        desc = (
            f"{spec['label']}：{len(rows)} 天（{_fmt_pct(share)}）；"
            f"高发月份 {top_months}；"
            f"Tmax 中位 `{_fmt_c(tmax)}`；峰值 `{_fmt_hour(peak)}`；"
            f"日较差 `{_fmt_c(daily_range)}`"
        )
        if noon is not None:
            desc += f"；午前升温 `{_fmt_c(noon)}`"
        desc += f"；注意：{spec['note']}"
        lines.append(desc)
    return lines


def _cloud_compare_line(multilayer_days: list[dict[str, str]], clearer_days: list[dict[str, str]]) -> str | None:
    if len(multilayer_days) < 20 or len(clearer_days) < 20:
        return None
    lead = f"午间多层云日（{len(multilayer_days)} 天）对比午间较开阔日（{len(clearer_days)} 天）"
    parts: list[str] = []
    tmax_multi = _median(multilayer_days, "tmax_c")
    tmax_clear = _median(clearer_days, "tmax_c")
    if tmax_multi is not None and tmax_clear is not None and abs(tmax_clear - tmax_multi) >= 1.0:
        direction = "压低到" if tmax_clear > tmax_multi else "抬高到"
        parts.append(f"`Tmax` 中位由 `{_fmt_c(tmax_clear)}` {direction} `{_fmt_c(tmax_multi)}`")
    range_multi = _median(multilayer_days, "daily_range_c")
    range_clear = _median(clearer_days, "daily_range_c")
    if range_multi is not None and range_clear is not None and abs(range_clear - range_multi) >= 1.0:
        direction = "压到" if range_clear > range_multi else "抬到"
        parts.append(f"日较差由 `{_fmt_c(range_clear)}` {direction} `{_fmt_c(range_multi)}`")
    peak_multi = _median(multilayer_days, "peak_hour_local")
    peak_clear = _median(clearer_days, "peak_hour_local")
    if peak_multi is not None and peak_clear is not None and abs(peak_multi - peak_clear) >= 1.0:
        if peak_multi > peak_clear:
            parts.append(f"峰值由 `{_fmt_hour(peak_clear)}` 推迟到 `{_fmt_hour(peak_multi)}`")
        else:
            parts.append(f"峰值由 `{_fmt_hour(peak_clear)}` 提前到 `{_fmt_hour(peak_multi)}`")
    if not parts:
        return None
    return lead + "：" + "；".join(parts) + "。"


def _cloud_lowline(low_multilayer_days: list[dict[str, str]]) -> str | None:
    if len(low_multilayer_days) < 15:
        return None
    return (
        f"午间低顶多层云共存日 `{len(low_multilayer_days)}` 天；"
        f"`Tmax` 中位 `{_fmt_c(_median(low_multilayer_days, 'tmax_c'))}`；"
        f"峰值 `{_fmt_hour(_median(low_multilayer_days, 'peak_hour_local'))}`；"
        f"这类样本更接近“被层云/多层云持续压制”，不应只按单层低云处理。"
    )


def _seasonal_wind_condition_lines(
    daily_rows: list[dict[str, str]],
    climate_windows: list[ClimateWindow],
    daily_rows_by_window: dict[str, list[dict[str, str]]],
) -> list[str]:
    candidates: list[tuple[float, int, str]] = []
    rendered: dict[str, str] = {}
    for window in climate_windows:
        season = window.label
        rows = daily_rows_by_window.get(season, [])
        ranked = _wind_rankings(rows)
        if len(ranked) < 4:
            continue
        warm_group = ranked[:2]
        cool_group = ranked[-2:]
        warm_median = statistics.mean(float(item["tmax_median"]) for item in warm_group if item["tmax_median"] is not None)
        cool_median = statistics.mean(float(item["tmax_median"]) for item in cool_group if item["tmax_median"] is not None)
        diff = warm_median - cool_median
        if diff < 1.5:
            continue
        warm_text = " / ".join(f"{item['sector']}({_fmt_c(item['tmax_median'])})" for item in warm_group)
        cool_text = " / ".join(f"{item['sector']}({_fmt_c(item['tmax_median'])})" for item in cool_group)
        parts = [f"{season}偏暖风向 `{warm_text}`；偏冷风向 `{cool_text}`；温差约 `{_fmt_c(diff)}`"]
        warm_peak_values = [float(item["peak_median"]) for item in warm_group if item.get("peak_median") is not None]
        cool_peak_values = [float(item["peak_median"]) for item in cool_group if item.get("peak_median") is not None]
        if warm_peak_values and cool_peak_values:
            parts.append(
                f"峰值时刻约 `{_fmt_hour(statistics.mean(warm_peak_values))}` vs `{_fmt_hour(statistics.mean(cool_peak_values))}`"
            )
        rendered[season] = "；".join(parts) + "。"
        candidates.append((diff, len(rows), season))
    candidates.sort(reverse=True)
    return [rendered[season] for _, _, season in candidates[:2]]


def _seasonal_situation_lines(
    daily_rows: list[dict[str, str]],
    climate_windows: list[ClimateWindow],
    daily_rows_by_window: dict[str, list[dict[str, str]]],
) -> list[str]:
    lines: list[str] = []
    for window in climate_windows:
        season = window.label
        rows = daily_rows_by_window.get(season, [])
        if len(rows) < 45:
            continue
        top_situations = _top_seasonal_situations(rows)
        warm_cool = _seasonal_wind_groups(rows)
        suppressor = _top_seasonal_suppressor(rows)
        parts: list[str] = []
        if top_situations:
            parts.append(f"主情形 `{top_situations}`")
        if warm_cool:
            parts.append(warm_cool)
        if suppressor:
            parts.append(suppressor)
        if parts:
            lines.append(f"{season}：" + "；".join(parts) + "。")
    if not lines:
        return ["当前站点尚未提炼出稳定的季节 / 气候情形差异。"]
    return lines[:4]


def _flag_effect_line(daily_rows: list[dict[str, str]], flag: str, label: str) -> str | None:
    flagged = [row for row in daily_rows if _is_true(row.get(flag))]
    base = [row for row in daily_rows if not _is_true(row.get(flag))]
    if len(flagged) < max(30, int(len(daily_rows) * 0.06)) or len(base) < 30:
        return None

    range_flag = _median(flagged, "daily_range_c")
    range_base = _median(base, "daily_range_c")
    noon_flag = _median(flagged, "warmup_noon_c")
    noon_base = _median(base, "warmup_noon_c")
    peak_flag = _median(flagged, "peak_hour_local")
    peak_base = _median(base, "peak_hour_local")
    late_ramp_flag = _median(flagged, "late_ramp_c")
    late_ramp_base = _median(base, "late_ramp_c")

    effects: list[str] = []
    if range_flag is not None and range_base is not None and abs(range_base - range_flag) >= 1.0:
        direction = "压到" if range_base > range_flag else "抬到"
        effects.append(f"日较差中位从 `{_fmt_c(range_base)}` {direction} `{_fmt_c(range_flag)}`")
    if noon_flag is not None and noon_base is not None and abs(noon_base - noon_flag) >= 0.5:
        direction = "降到" if noon_base > noon_flag else "抬到"
        effects.append(f"午前升温从 `{_fmt_c(noon_base)}` {direction} `{_fmt_c(noon_flag)}`")
    if peak_flag is not None and peak_base is not None and abs(peak_flag - peak_base) >= 1.0:
        if peak_flag > peak_base:
            effects.append(f"峰值时刻约由 `{_fmt_hour(peak_base)}` 推迟到 `{_fmt_hour(peak_flag)}`")
        else:
            effects.append(f"峰值时刻约由 `{_fmt_hour(peak_base)}` 提前到 `{_fmt_hour(peak_flag)}`")
    if late_ramp_flag is not None and late_ramp_base is not None and abs(late_ramp_flag - late_ramp_base) >= 0.75:
        effects.append(f"尾段斜率约 `{_fmt_c(late_ramp_base)}` vs `{_fmt_c(late_ramp_flag)}`")
    if not effects:
        return None
    return f"{label}（{len(flagged)} 天）：" + "；".join(effects) + "。"


def _late_surge_line(daily_rows: list[dict[str, str]]) -> str | None:
    surged = [row for row in daily_rows if _is_true(row.get("late_surge_flag"))]
    base = [row for row in daily_rows if not _is_true(row.get("late_surge_flag"))]
    if len(surged) < max(25, int(len(daily_rows) * 0.05)) or len(base) < 30:
        return None
    peak_s = _median(surged, "peak_hour_local")
    peak_b = _median(base, "peak_hour_local")
    ramp_s = _median(surged, "late_ramp_c")
    ramp_b = _median(base, "late_ramp_c")
    if peak_s is None or peak_b is None or abs(peak_s - peak_b) < 1.0:
        return None
    line = (
        f"末段冲高日（{len(surged)} 天）：峰值时刻中位约由 `{_fmt_hour(peak_b)}` 推迟到 `{_fmt_hour(peak_s)}`"
    )
    if ramp_s is not None and ramp_b is not None:
        line += f"；尾段斜率约 `{_fmt_c(ramp_b)}` vs `{_fmt_c(ramp_s)}`"
    return line + "。"


def _regime_profile_lines(daily_rows: list[dict[str, str]]) -> list[str]:
    total = max(len(daily_rows), 1)
    by_regime: dict[str, list[dict[str, str]]] = defaultdict(list)
    for row in daily_rows:
        regime = str(row.get("primary_regime") or "").strip()
        if regime:
            by_regime[regime].append(row)

    lines: list[str] = []
    mixed = by_regime.get("mixed_transitional")
    if mixed:
        lines.append(
            f"过渡型：{len(mixed)} 天（{_fmt_pct(len(mixed) / total)}）；说明站点对实时路径较敏感，不宜机械套单一模板。"
        )

    meaningful = [
        (regime, rows)
        for regime, rows in sorted(by_regime.items(), key=lambda item: len(item[1]), reverse=True)
        if regime != "mixed_transitional"
    ][:4]
    for regime, rows in meaningful:
        lines.append(
            f"{regime_to_cn(regime)}：{len(rows)} 天（{_fmt_pct(len(rows) / total)}）；"
            f"Tmax 中位 `{_fmt_c(_median(rows, 'tmax_c'))}`；"
            f"峰值 `{_fmt_hour(_median(rows, 'peak_hour_local'))}`；"
            f"日较差 `{_fmt_c(_median(rows, 'daily_range_c'))}`"
        )
    if not lines:
        lines.append("n/a")
    return lines


def _top_months_for_rows(rows: list[dict[str, str]]) -> str:
    counter = Counter(_safe_int(row.get("month")) for row in rows if _safe_int(row.get("month")) is not None)
    if not counter:
        return "n/a"
    parts: list[str] = []
    for month, count in counter.most_common(2):
        parts.append(f"{MONTH_NAMES.get(month, 'n/a')} ({count}天)")
    return " / ".join(parts)


def _get_climate_windows(station_id: str) -> list[ClimateWindow]:
    configured = CITY_CLIMATE_WINDOWS.get(station_id)
    if configured:
        return _normalize_climate_windows(configured)
    return [
        ClimateWindow("冬季(12-2)", frozenset({12, 1, 2})),
        ClimateWindow("春季(3-5)", frozenset({3, 4, 5})),
        ClimateWindow("夏季(6-8)", frozenset({6, 7, 8})),
        ClimateWindow("秋季(9-11)", frozenset({9, 10, 11})),
    ]


def _normalize_climate_windows(raw_windows: list[dict[str, object]]) -> list[ClimateWindow]:
    windows: list[ClimateWindow] = []
    for raw_window in raw_windows:
        label = str(raw_window.get("label") or "n/a")
        months = frozenset(int(item) for item in raw_window.get("months", []) if isinstance(item, int))
        if not months:
            continue
        windows.append(ClimateWindow(label=label, months=months))
    return windows


def _group_rows_by_climate_window(
    daily_rows: list[dict[str, str]],
    climate_windows: list[ClimateWindow],
) -> dict[str, list[dict[str, str]]]:
    grouped: dict[str, list[dict[str, str]]] = defaultdict(list)
    for row in daily_rows:
        month = _safe_int(row.get("month"))
        if month is None:
            continue
        for window in climate_windows:
            if month in window.months:
                grouped[window.label].append(row)
                break
    return grouped


def _format_climate_windows(climate_windows: list[ClimateWindow]) -> str:
    return " / ".join(window.label for window in climate_windows)


def _top_seasonal_situations(rows: list[dict[str, str]]) -> str | None:
    total = max(len(rows), 1)
    items: list[tuple[float, str]] = []
    for flag, label in SEASONAL_SITUATION_SPECS:
        share = sum(1 for row in rows if _is_true(row.get(flag))) / total
        if share < 0.08:
            continue
        items.append((share, label))
    if not items:
        return None
    top = sorted(items, reverse=True)[:2]
    return " / ".join(f"{label}({_fmt_pct(share)})" for share, label in top)


def _seasonal_wind_groups(rows: list[dict[str, str]]) -> str | None:
    ranked = _wind_rankings(rows)
    if len(ranked) < 4:
        return None
    warm = "/".join(str(item["sector"]) for item in ranked[:2])
    cool = "/".join(str(item["sector"]) for item in ranked[-2:])
    return f"偏暖风向 `{warm}`；偏冷风向 `{cool}`"


def _top_seasonal_suppressor(rows: list[dict[str, str]]) -> str | None:
    candidates = [
        ("midday_low_ceiling_flag", "午间低云"),
        ("rain_reset_flag", "降水重置"),
        ("low_visibility_day_flag", "低能见 / 静稳"),
    ]
    best: tuple[float, str, float] | None = None
    for flag, label in candidates:
        flagged = [row for row in rows if _is_true(row.get(flag))]
        base = [row for row in rows if not _is_true(row.get(flag))]
        if len(flagged) < 12 or len(base) < 12:
            continue
        tmax_flag = _median(flagged, "tmax_c")
        tmax_base = _median(base, "tmax_c")
        if tmax_flag is None or tmax_base is None:
            continue
        drop = tmax_base - tmax_flag
        if drop < 1.0:
            continue
        score = drop * (len(flagged) / len(rows))
        if best is None or score > best[0]:
            best = (score, label, drop)
    if best is None:
        return None
    _, label, drop = best
    return f"主要压制因子 `{label}`（`Tmax` 中位约压低 `{_fmt_c(drop)}`）"


def _failure_modes(prior: dict[str, str], override: dict[str, object]) -> list[str]:
    modes: list[str] = [str(item) for item in override.get("failure_modes", [])]
    if (_safe_float(prior.get("late_peak_share")) or 0.0) >= 0.55:
        modes.append("若在午后前段就提前封顶，最容易低估真正的晚峰或尾段冲高。")
    if (_safe_float(prior.get("midday_low_ceiling_share")) or 0.0) >= 0.30:
        modes.append("若忽略峰值窗低云持续性，只按当前温度线性外推，容易把云压制误当成模式冷偏差。")
    if (_safe_float(prior.get("rain_reset_day_share")) or 0.0) >= 0.10:
        modes.append("若不单独处理雨后重置日，暖季上限会被系统性高估。")
    if (_safe_float(prior.get("reduced_visibility_share")) or 0.0) >= 0.15:
        modes.append("晨间低能见/湿层较重时，启动偏慢更常见，不宜直接套 clean-ramp 日模板。")
    return _dedupe(modes)[:5]


def _watch_order(
    prior: dict[str, str],
    monthly_rows: list[dict[str, str]],
    override: dict[str, object],
) -> list[str]:
    hooks: list[str] = [str(item) for item in override.get("watch_order", [])]
    if (_safe_float(prior.get("cloud_break_day_share")) or 0.0) >= 0.10:
        hooks.append("上午若低云逐步减弱，不要只看当前偏冷；优先判断是否进入开窗反弹阶段。")
    if (_safe_float(prior.get("late_peak_share")) or 0.0) >= 0.55:
        hooks.append("接近窗口后段仍有斜率时，应继续保留上冲空间，避免过早封顶。")
    if (_safe_float(prior.get("humid_sticky_day_share")) or 0.0) >= 0.18:
        hooks.append("露点和湿层厚度的解释优先级要提高，单看温度斜率容易误判。")
    if (_safe_float(prior.get("wind_shift_day_share")) or 0.0) >= 0.16:
        hooks.append("风向切换应视为 live pivot，必要时比同小时模式偏差更值得信任。")
    if (_safe_float(prior.get("rain_reset_day_share")) or 0.0) >= 0.10:
        hooks.append("有降水或地面湿润残留时，优先防高估；除非午前快速放晴且斜率恢复。")
    clean_months = sorted(monthly_rows, key=lambda row: _safe_float(row.get("dry_mixing_share")) or -1, reverse=True)[:1]
    if clean_months and (_safe_float(clean_months[0].get("dry_mixing_share")) or 0.0) >= 0.15:
        hooks.append(
            f"{MONTH_NAMES.get(_safe_int(clean_months[0].get('month')) or 0, '该月')} 常见干混合上冲，晴空低云少的日子要防旧版保守低估。"
        )
    if not hooks:
        hooks.append("当前站点更适合维持旧版主逻辑，只在明显 regime 信号出现时做温和修正。")
    return _dedupe(hooks)[:5]


def _wind_extremes(daily_rows: list[dict[str, str]]) -> tuple[dict[str, float | str] | None, dict[str, float | str] | None]:
    ranked = _wind_rankings(daily_rows)
    if len(ranked) < 2:
        return None, None
    return ranked[0], ranked[-1]


def _wind_rankings(daily_rows: list[dict[str, str]]) -> list[dict[str, float | str]]:
    by_sector: dict[str, list[dict[str, str]]] = defaultdict(list)
    for row in daily_rows:
        sector = str(row.get("dominant_wind_sector") or "").strip()
        if sector:
            by_sector[sector].append(row)
    threshold = max(40, int(len(daily_rows) * 0.03))
    stats: list[dict[str, float | str]] = []
    for sector, rows in by_sector.items():
        if len(rows) < threshold:
            continue
        tmax = _median(rows, "tmax_c")
        if tmax is None:
            continue
        stats.append(
            {
                "sector": sector,
                "count": float(len(rows)),
                "share": len(rows) / max(len(daily_rows), 1),
                "tmax_median": tmax,
                "peak_median": _median(rows, "peak_hour_local"),
            }
        )
    stats.sort(key=lambda item: float(item["tmax_median"]), reverse=True)
    return stats


def _related_manual_docs(station_id: str, city: str) -> list[str]:
    tokens = {station_id.lower(), _slug(city)}
    docs: list[str] = []
    for path in DOCS_DIR.glob("*.md"):
        if path.name == "README.md":
            continue
        name = path.stem.lower()
        if any(token in name for token in tokens):
            docs.append(path.relative_to(ROOT).as_posix())
    return sorted(docs)


def _dedupe(items: list[str]) -> list[str]:
    seen: set[str] = set()
    result: list[str] = []
    for item in items:
        normalized = item.strip()
        if not normalized or normalized in seen:
            continue
        seen.add(normalized)
        result.append(normalized)
    return result


def _load_csv(path: Path) -> list[dict[str, str]]:
    with path.open(newline="", encoding="utf-8") as handle:
        return list(csv.DictReader(handle))


def _slug(text: str) -> str:
    return re.sub(r"[^a-z0-9]+", "_", text.strip().lower()).strip("_")


def _safe_float(value: str | None) -> float | None:
    if value in (None, "", "n/a"):
        return None
    try:
        return float(value)
    except ValueError:
        return None


def _safe_int(value: str | None) -> int | None:
    if value in (None, "", "n/a"):
        return None
    try:
        return int(float(value))
    except ValueError:
        return None


def _is_true(value: str | None) -> bool:
    return str(value or "").strip().lower() in {"1", "true", "yes"}


def _raw_wind_sector(raw: str | None) -> str | None:
    if not isinstance(raw, str) or not raw:
        return None
    code = raw.split(",")[0]
    if code in {"999", "VRB"}:
        return None
    try:
        direction = int(code)
    except ValueError:
        return None
    if direction > 360:
        return None
    sectors = ["N", "NE", "E", "SE", "S", "SW", "W", "NW"]
    return sectors[int(((direction + 22.5) % 360) // 45)]


def _raw_cloud_layer_count(raw: dict[str, str]) -> int:
    count = 0
    for key in ("GA1", "GA2", "GA3"):
        value = raw.get(key)
        if isinstance(value, str) and value.strip():
            count += 1
    return count


def _raw_cig_ft(raw: str | None) -> int | None:
    if not isinstance(raw, str) or not raw.strip():
        return None
    code = raw.split(",")[0].strip()
    if code in {"99999", "22000"}:
        return None
    try:
        return int(code)
    except ValueError:
        return None


def _median(rows: list[dict[str, str]], key: str) -> float | None:
    values = [_safe_float(row.get(key)) for row in rows]
    clean = [value for value in values if value is not None]
    if not clean:
        return None
    return statistics.median(clean)


def _fmt_pct(value: float | None) -> str:
    if value is None:
        return "n/a"
    return f"{value * 100:.1f}%"


def _fmt_c(value: float | None) -> str:
    if value is None:
        return "n/a"
    return f"{value:.1f}°C"


def _fmt_hour(value: float | None) -> str:
    if value is None:
        return "n/a"
    total_minutes = int(round(float(value) * 60.0))
    total_minutes = max(0, min(total_minutes, 23 * 60 + 59))
    hour, minute = divmod(total_minutes, 60)
    return f"{hour:02d}:{minute:02d}"


def _top_value_counts(
    rows: list[dict[str, object]],
    key: str,
    labels: dict[int, str] | None = None,
    top_n: int = 2,
    unit: str = "天",
) -> str:
    counter = Counter(int(row[key]) for row in rows if row.get(key) is not None)
    return _top_counter_text(counter, labels, top_n=top_n, unit=unit)


def _top_counter_text(
    counter: Counter,
    labels: dict[int, str] | None = None,
    top_n: int = 2,
    unit: str = "天",
) -> str:
    if not counter:
        return "n/a"
    parts: list[str] = []
    for value, count in counter.most_common(top_n):
        label = labels.get(value, str(value)) if labels else str(value)
        suffix = unit if unit else ""
        parts.append(f"{label} ({count}{suffix})")
    return " / ".join(parts)


def _hour_window_text(rows: list[dict[str, object]]) -> str:
    counter = Counter(int(row["local_hour"]) for row in rows if row.get("local_hour") is not None)
    if not counter:
        return "n/a"
    max_count = max(counter.values())
    selected = sorted(hour for hour, count in counter.items() if count >= max_count * 0.8)
    if not selected:
        selected = sorted(hour for hour, _ in counter.most_common(2))
    if len(selected) >= 10 or (selected[-1] - selected[0]) >= 18:
        return "全天偏多"
    if len(selected) == 1:
        return f"`{selected[0]:02d}:00`"
    return f"`{selected[0]:02d}:00-{selected[-1]:02d}:00`"


def _format_switch_window(hours: list[int]) -> str:
    if not hours:
        return "n/a"
    counter = Counter(hours)
    selected = sorted(hour for hour, count in counter.items() if count >= max(counter.values()) * 0.6)
    if not selected:
        selected = sorted(hour for hour, _ in counter.most_common(2))
    if len(selected) == 1:
        return f"{selected[0]:02d}:00"
    return f"{selected[0]:02d}:00-{selected[-1]:02d}:00"


def _clean_sentence(text: str) -> str:
    cleaned = text.strip()
    while cleaned.endswith(("。", ".", "；", ";", "!", "！")):
        cleaned = cleaned[:-1].rstrip()
    return cleaned


def _fmt_month_rank(rows: list[dict[str, str]], key: str, is_pct: bool = False) -> str:
    parts: list[str] = []
    for row in rows:
        month = MONTH_NAMES.get(_safe_int(row.get("month")) or 0, "n/a")
        value = _safe_float(row.get(key))
        if is_pct:
            parts.append(f"{month} ({_fmt_pct(value)})")
        else:
            parts.append(f"{month} ({_fmt_c(value)})")
    return "；".join(parts) if parts else "n/a"


def _format_coords(lat_raw: str, lon_raw: str) -> str:
    lat = float(lat_raw)
    lon = float(lon_raw)
    lat_hemi = "N" if lat >= 0 else "S"
    lon_hemi = "E" if lon >= 0 else "W"
    return f"- 坐标：{abs(lat):.4f}{lat_hemi}, {abs(lon):.4f}{lon_hemi}"


if __name__ == "__main__":
    main()
