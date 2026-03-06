#!/usr/bin/env python3
"""Archive-backed METAR historical context for weatherbot."""

from __future__ import annotations

import csv
import gzip
import math
import os
from collections import defaultdict
from datetime import datetime, timezone
from functools import lru_cache
from pathlib import Path
import statistics
from typing import Any
from zoneinfo import ZoneInfo

from city_profile_overrides import CITY_CLIMATE_WINDOWS
from condition_state import build_live_condition_signals, extract_hour, extract_minute, kt_to_ms
from synoptic_adjustment_context import branch_alignment, build_synoptic_adjustment_context

ROOT = Path(__file__).resolve().parent.parent
DEFAULT_REFERENCE_DIR = ROOT / "data" / "historical_reference"
LEGACY_CACHE_REFERENCE_DIR = ROOT / "cache" / "historical_reference"
ARCHIVE_REFERENCE_CANDIDATES = (
    DEFAULT_REFERENCE_DIR,
    LEGACY_CACHE_REFERENCE_DIR,
    ROOT.parent / "polymarket-weather-archive" / "reports",
    Path("/Users/ham/polymarket-weather-archive/reports"),
)

PRIOR_FILE = "weatherbot_station_priors.csv"
DAILY_FILE = "weatherbot_daily_local_regimes.csv"
MONTHLY_FILE = "weatherbot_monthly_climatology.csv"
REFERENCE_MD = "weatherbot_metar_reference.md"
DEFAULT_RAW_DIR = DEFAULT_REFERENCE_DIR / "raw_metar_isd"
ARCHIVE_RAW_CANDIDATES = (
    DEFAULT_RAW_DIR,
    ROOT.parent / "polymarket-weather-archive" / "data" / "raw" / "metar_isd",
    Path("/Users/ham/polymarket-weather-archive/data/raw/metar_isd"),
)

RAW_CLOUD_COVER_MAP = {
    "00": 0.00,
    "01": 0.10,
    "02": 0.25,
    "03": 0.40,
    "04": 0.50,
    "05": 0.60,
    "06": 0.75,
    "07": 0.95,
    "08": 1.00,
    "09": 0.95,
    "10": 0.50,
}

FEATURE_CN_MAP = {
    "late-day surge risk": "末段冲高倾向",
    "large diurnal swing": "日较差偏大",
    "muted diurnal range": "日较差偏小",
    "humid-heat persistence": "湿热持续性强",
    "midday cloud suppression risk": "午间云压制明显",
    "frequent visibility restrictions": "低能见度偏多",
    "frequent light-wind stagnation": "轻风滞留偏多",
    "frequent precip resets": "降水重置频繁",
    "frequent cloud-break rebounds": "开窗反弹较常见",
    "dry-mixing upside": "干混合上冲潜力",
    "wind-shift sensitive": "风向切换敏感",
    "balanced baseline station": "基线相对平衡",
}


def historical_context_enabled() -> bool:
    raw = str(os.getenv("LOOK_ENABLE_HISTORICAL_CONTEXT", "1")).strip().lower()
    return raw not in {"0", "false", "off", "no"}


def reference_dir() -> Path | None:
    env_dir = str(os.getenv("WEATHERBOT_HISTORICAL_DIR") or "").strip()
    candidates = []
    if env_dir:
        candidates.append(Path(env_dir))
    candidates.extend(ARCHIVE_REFERENCE_CANDIDATES)
    for candidate in candidates:
        if (candidate / PRIOR_FILE).exists() and (candidate / DAILY_FILE).exists() and (candidate / MONTHLY_FILE).exists():
            return candidate
    return None


@lru_cache(maxsize=1)
def _load_station_priors() -> dict[str, dict[str, str]]:
    path = _required_path(PRIOR_FILE)
    with path.open(newline="", encoding="utf-8") as handle:
        return {str(row.get("station_id") or "").upper(): row for row in csv.DictReader(handle)}


@lru_cache(maxsize=1)
def _load_monthly_rows() -> list[dict[str, str]]:
    path = _required_path(MONTHLY_FILE)
    with path.open(newline="", encoding="utf-8") as handle:
        return list(csv.DictReader(handle))


@lru_cache(maxsize=1)
def _load_daily_rows() -> list[dict[str, str]]:
    path = _required_path(DAILY_FILE)
    with path.open(newline="", encoding="utf-8") as handle:
        return list(csv.DictReader(handle))


def archive_raw_dir() -> Path | None:
    env_dir = str(os.getenv("WEATHERBOT_HISTORICAL_RAW_DIR") or "").strip()
    candidates = []
    if env_dir:
        candidates.append(Path(env_dir))
    candidates.extend(ARCHIVE_RAW_CANDIDATES)
    for candidate in candidates:
        if candidate.exists():
            return candidate
    return None


@lru_cache(maxsize=64)
def _load_station_daily_rows(station_id: str) -> tuple[dict[str, str], ...]:
    station_key = str(station_id or "").upper()
    rows = [row for row in _load_daily_rows() if str(row.get("station_id") or "").upper() == station_key]
    return tuple(rows)


@lru_cache(maxsize=128)
def _load_candidate_daily_rows(station_id: str) -> tuple[dict[str, str], ...]:
    station_rows = list(_load_station_daily_rows(station_id))
    if not station_rows:
        return tuple()
    return tuple(station_rows)


@lru_cache(maxsize=32)
def _station_timezone(station_id: str) -> str:
    priors = _load_station_priors()
    station_key = str(station_id or "").upper()
    if station_key in priors:
        timezone_name = str(priors[station_key].get("timezone") or "").strip()
        if timezone_name:
            return timezone_name
    station_rows = _load_station_daily_rows(station_key)
    for row in station_rows:
        timezone_name = str(row.get("timezone") or "").strip()
        if timezone_name:
            return timezone_name
    return "UTC"


@lru_cache(maxsize=32)
def _load_station_hourly_index(station_id: str) -> dict[tuple[str, int], tuple[dict[str, Any], ...]]:
    base_dir = archive_raw_dir()
    if base_dir is None:
        return {}
    station_dir = base_dir / str(station_id or "").upper()
    if not station_dir.exists():
        return {}
    try:
        zone = ZoneInfo(_station_timezone(station_id))
    except Exception:
        zone = ZoneInfo("UTC")

    grouped: dict[tuple[str, int], list[dict[str, Any]]] = defaultdict(list)
    for path in sorted(station_dir.glob("*.csv.gz")):
        with gzip.open(path, "rt", encoding="utf-8", newline="") as handle:
            reader = csv.DictReader(handle)
            for raw in reader:
                state = _parse_raw_hourly_state(raw, zone)
                if not state:
                    continue
                key = (str(state["local_date"]), int(state["local_hour"]))
                grouped[key].append(state)

    return {
        key: tuple(sorted(value, key=lambda item: int(item.get("local_minute") or 0)))
        for key, value in grouped.items()
    }


def build_historical_context(
    station_id: str,
    target_date: str,
    metar_diag: dict[str, Any],
    *,
    forecast_decision: dict[str, Any] | None = None,
    synoptic_context: dict[str, Any] | None = None,
    site_tag: str | None = None,
    terrain_tag: str | None = None,
    direction_factor: str | None = None,
    factor_summary: str | None = None,
    analog_limit: int = 5,
) -> dict[str, Any]:
    if not historical_context_enabled():
        return {"available": False, "reason": "historical context disabled"}
    ref_dir = reference_dir()
    if ref_dir is None:
        return {"available": False, "reason": "historical reference files not found"}

    station_key = str(station_id or "").upper()
    priors = _load_station_priors()
    station_prior = priors.get(station_key)
    if not station_prior:
        return {"available": False, "reason": f"no historical prior for {station_key}", "reference_dir": str(ref_dir)}

    month = _target_month(target_date)
    monthly_row = _select_monthly_row(station_key, month)
    live_regime = infer_live_regime(metar_diag)
    current_state = _live_state_vector(metar_diag, live_regime)
    normalized_synoptic_context = build_synoptic_adjustment_context(
        forecast_decision=forecast_decision,
        external_context=synoptic_context,
    )
    analogs = find_similar_days(station_key, target_date, metar_diag, live_regime, limit=analog_limit)

    analog_tmax_values = [_safe_float(row.get("tmax_c")) for row in analogs]
    analog_tmax_values = [value for value in analog_tmax_values if value is not None]
    analog_peak_values = [_safe_float(row.get("peak_hour_local")) for row in analogs]
    analog_peak_values = [value for value in analog_peak_values if value is not None]

    summary_lines: list[str] = []
    if factor_summary:
        summary_lines.append(f"站点固定因子：{factor_summary}")
    elif site_tag or terrain_tag:
        summary_lines.append(f"站点固定因子：{site_tag or terrain_tag}")
    summary_lines.append(f"站点历史画像：{translate_special_features(station_prior.get('special_features'))}")
    if monthly_row:
        summary_lines.append(
            "同月基线："
            f"Tmax中位 {_fmt_c(_safe_float(monthly_row.get('tmax_median_c')))}，"
            f"峰值时刻 {_fmt_hour(_safe_float(monthly_row.get('peak_hour_median')))}，"
            f"午间低云 {_fmt_pct(_safe_float(monthly_row.get('midday_low_ceiling_share')))}"
        )
    summary_lines.append(
        "当前实况匹配："
        f"{live_regime['primary_regime_cn']}（标签：{', '.join(live_regime['tags_cn']) or '过渡'}）"
    )
    summary_lines.append("检索口径：按日历日期邻近度加权，不预设季节窗口")
    if analog_tmax_values:
        analog_tmax_mean = sum(analog_tmax_values) / len(analog_tmax_values)
        analog_peak_mean = (sum(analog_peak_values) / len(analog_peak_values)) if analog_peak_values else None
        summary_lines.append(
            "相似日均值："
            f"Tmax {_fmt_c(analog_tmax_mean)}，"
            f"峰值时刻 {_fmt_hour(analog_peak_mean)}"
        )
    branch_assessment = assess_analog_branches(
        analogs,
        metar_diag,
        live_regime,
        synoptic_context=normalized_synoptic_context,
    )
    weighted_reference = build_weighted_reference(
        analogs,
        current_state,
        live_regime,
        branch_assessment,
        synoptic_context=normalized_synoptic_context,
    )
    preferred_branch = str(branch_assessment.get("preferred_branch") or "")
    reference_strength = str(branch_assessment.get("reference_strength_cn") or "")
    branch_rationale = str(branch_assessment.get("preferred_branch_rationale") or "")
    if preferred_branch and reference_strength:
        summary_lines.append(f"分支判定：当前走势更贴近 `{preferred_branch}`，历史参考强度 `{reference_strength}`")
    if branch_rationale:
        summary_lines.append(f"判定依据：{branch_rationale}")
    adjustment_hint = build_adjustment_hint(
        station_prior,
        monthly_row,
        live_regime,
        analogs,
        weighted_reference=weighted_reference,
        branch_assessment=branch_assessment,
    )
    if adjustment_hint:
        summary_lines.append(f"历史优化提示：{adjustment_hint}")
    analog_summary_lines = summarize_analog_group(analogs, weighted_reference)
    branch_lines = summarize_analog_branches(analogs, branch_assessment)

    return {
        "available": True,
        "reference_dir": str(ref_dir),
        "station_id": station_key,
        "station_prior": station_prior,
        "monthly_row": monthly_row,
        "live_regime": live_regime,
        "analogs": analogs,
        "current_state": current_state,
        "summary_lines": summary_lines,
        "analog_summary_lines": analog_summary_lines,
        "branch_lines": branch_lines,
        "branch_assessment": branch_assessment,
        "weighted_reference": weighted_reference,
        "adjustment_hint": adjustment_hint,
        "synoptic_context": normalized_synoptic_context,
    }

def infer_live_regime(metar_diag: dict[str, Any]) -> dict[str, Any]:
    signals = build_live_condition_signals(metar_diag)
    cloud_cover = _safe_float(signals.get("cloud_effective_cover"))
    radiation_eff = _safe_float(signals.get("radiation_eff"))
    temp_trend = _safe_float(signals.get("temp_trend_c"))
    bias = _safe_float(signals.get("temp_bias_c"))
    dewpoint = _safe_float(signals.get("dewpoint_c"))
    latest_temp = _safe_float(signals.get("latest_temp_c"))
    latest_rh = _safe_float(signals.get("latest_rh"))
    wind_change = _safe_float(signals.get("wind_dir_change_deg"))
    latest_wspd = _safe_float(signals.get("latest_wspd_kt"))
    cloud_trend = str(signals.get("cloud_trend") or "")
    precip_state = str(signals.get("precip_state") or "none").lower()
    precip_trend = str(signals.get("precip_trend") or "none").lower()
    local_hour = extract_hour(signals.get("latest_report_local"))

    tags: list[str] = []
    if precip_state not in {"", "none"} or precip_trend in {"new", "intensify"}:
        tags.append("rain_reset")
    if ("开窗" in cloud_trend or "减弱" in cloud_trend) and (temp_trend is not None and temp_trend >= 0.25):
        tags.append("cloud_break_rebound")
    if cloud_cover is not None and radiation_eff is not None and cloud_cover >= 0.70 and radiation_eff <= 0.45:
        tags.append("cloud_suppressed")
    if local_hour is not None and local_hour >= 14 and temp_trend is not None and temp_trend >= 0.30:
        tags.append("late_surge")
    if dewpoint is not None and dewpoint >= 18.0:
        tags.append("humid_sticky")
    elif latest_rh is not None and latest_rh >= 75.0:
        tags.append("humid_sticky")
    if latest_temp is not None and dewpoint is not None and (latest_temp - dewpoint) >= 8.0 and (radiation_eff is not None and radiation_eff >= 0.65):
        tags.append("dry_mixing")
    if latest_wspd is not None and latest_wspd <= 4.0 and latest_rh is not None and latest_rh >= 70.0:
        tags.append("light_wind_stagnation")
    if wind_change is not None and wind_change >= 50.0:
        tags.append("wind_shift_transition")
    if radiation_eff is not None and radiation_eff >= 0.80 and cloud_cover is not None and cloud_cover <= 0.15 and precip_state in {"", "none"}:
        tags.append("clean_solar_ramp")

    primary = "mixed_transitional"
    for candidate in (
        "rain_reset",
        "cloud_break_rebound",
        "cloud_suppressed",
        "late_surge",
        "humid_sticky",
        "dry_mixing",
        "wind_shift_transition",
        "light_wind_stagnation",
        "clean_solar_ramp",
    ):
        if candidate in tags:
            primary = candidate
            break
    if not tags:
        tags = ["mixed_transitional"]
    return {
        "primary_regime": primary,
        "primary_regime_cn": regime_to_cn(primary),
        "tags": tags,
        "tags_cn": [regime_to_cn(tag) for tag in tags],
        "wind_sector": wind_sector_from_diag(metar_diag.get("latest_wdir")),
        "cloud_signature": _live_cloud_signature(metar_diag),
    }


def find_similar_days(
    station_id: str,
    target_date: str,
    metar_diag: dict[str, Any],
    live_regime: dict[str, Any],
    *,
    limit: int = 3,
) -> list[dict[str, str]]:
    signals = build_live_condition_signals(metar_diag)
    current_hour = extract_hour(signals.get("latest_report_local"))
    current_minute = extract_minute(signals.get("latest_report_local"))
    target_day_of_year = _target_day_of_year(target_date)
    current_state = _live_state_vector(metar_diag, live_regime)
    rows: list[dict[str, str]] = []
    for row in _load_candidate_daily_rows(station_id):
        score = 0.0
        reasons: list[tuple[float, str]] = []
        row_date = str(row.get("local_date") or "")

        row_day_of_year = _date_to_day_of_year(row_date)
        if target_day_of_year is not None and row_day_of_year is not None:
            calendar_gap = _circular_day_distance(target_day_of_year, row_day_of_year)
            bonus = max(0.0, 4.0 - (calendar_gap / 9.0))
            score += bonus
            if bonus >= 0.8:
                reasons.append((bonus, f"日历位置接近（差 `{calendar_gap}` 天）"))
        else:
            calendar_gap = None

        row_regime = str(row.get("primary_regime") or "")
        if row_regime == live_regime.get("primary_regime"):
            bonus = 2.8
            score += bonus
            reasons.append((bonus, f"局地日型一致（{regime_to_cn(row_regime)}）"))
        row_tags = {item.strip() for item in str(row.get("regime_tags") or "").split(";") if item.strip()}
        live_tags = set(live_regime.get("tags") or [])
        tag_bonus = 0.7 * len(row_tags & live_tags)
        score += tag_bonus
        if tag_bonus >= 0.7:
            reasons.append((tag_bonus, f"次级标签重合（{len(row_tags & live_tags)} 项）"))

        hist_state = _historical_state_for_row(station_id, row, current_hour, current_minute)
        hist_wind_sector = str((hist_state or {}).get("wind_sector") or row.get("dominant_wind_sector") or "")
        if current_state.get("wind_sector") and hist_wind_sector == current_state.get("wind_sector"):
            bonus = 1.2
            score += bonus
            reasons.append((bonus, f"当前时刻风向一致（{hist_wind_sector}）"))
        elif current_state.get("wind_sector") and _wind_family(hist_wind_sector) == _wind_family(str(current_state.get("wind_sector"))):
            bonus = 0.6
            score += bonus
            reasons.append((bonus, f"当前时刻风向同属 {_wind_family(hist_wind_sector)} 象限"))

        if current_state.get("temp_c") is not None and hist_state and hist_state.get("temp_c") is not None:
            diff = abs(float(hist_state["temp_c"]) - float(current_state["temp_c"]))
            bonus = 1.6 * max(0.0, 1.0 - diff / 6.5)
            score += bonus
            if bonus >= 0.45:
                reasons.append((bonus, f"当前时刻温度接近（差 `{diff:.1f}°C`）"))

        if current_state.get("dew_c") is not None and hist_state and hist_state.get("dew_c") is not None:
            diff = abs(float(hist_state["dew_c"]) - float(current_state["dew_c"]))
            bonus = 1.1 * max(0.0, 1.0 - diff / 5.5)
            score += bonus
            if bonus >= 0.35:
                reasons.append((bonus, f"当前时刻露点接近（差 `{diff:.1f}°C`）"))

        if current_state.get("cloud_signature") and hist_state and hist_state.get("cloud_signature"):
            live_sig = str(current_state["cloud_signature"])
            hist_sig = str(hist_state["cloud_signature"])
            if live_sig == hist_sig:
                bonus = 1.4
                score += bonus
                reasons.append((bonus, f"云结构接近（{cloud_signature_to_cn(hist_sig)}）"))
            elif _cloud_signature_family(live_sig) == _cloud_signature_family(hist_sig):
                bonus = 0.7
                score += bonus
                reasons.append((bonus, f"云结构同类（{cloud_signature_to_cn(live_sig)} / {cloud_signature_to_cn(hist_sig)}）"))

        if current_state.get("cloud_effective_cover") is not None and hist_state and hist_state.get("cloud_effective_cover") is not None:
            diff = abs(float(hist_state["cloud_effective_cover"]) - float(current_state["cloud_effective_cover"]))
            bonus = 0.8 * max(0.0, 1.0 - diff)
            score += bonus
            if bonus >= 0.35:
                reasons.append((bonus, f"云量强度接近（差 `{diff:.2f}`）"))

        if current_state.get("wind_speed_ms") is not None and hist_state and hist_state.get("wind_speed_ms") is not None:
            diff = abs(float(hist_state["wind_speed_ms"]) - float(current_state["wind_speed_ms"]))
            bonus = 0.7 * max(0.0, 1.0 - diff / 4.5)
            score += bonus
            if bonus >= 0.2:
                reasons.append((bonus, f"风速接近（差 `{diff:.1f}m/s`）"))

        current_precip = str(current_state.get("precip_state") or "none")
        hist_precip = str((hist_state or {}).get("precip_state") or "")
        if current_precip not in {"", "none"} and hist_precip not in {"", "none"}:
            bonus = 0.8
            score += bonus
            reasons.append((bonus, "实况与历史都带降水/湿重置"))
        elif current_precip in {"", "none"} and row.get("clean_solar_ramp_flag", "").lower() == "true":
            bonus = 0.4
            score += bonus
            reasons.append((bonus, "都更接近晴空增温路径"))

        if hist_state and hist_state.get("_state_source") == "raw_hourly":
            hour_gap = abs(int(hist_state.get("_hour_gap") or 0))
            bonus = 0.6 if hour_gap == 0 else 0.2
            score += bonus
            if bonus >= 0.5:
                reasons.append((bonus, "使用到同小时历史实况切片"))
        if score <= 0.5:
            continue
        enriched = dict(row)
        enriched["_similarity_score"] = f"{score:.2f}"
        if calendar_gap is not None:
            enriched["_calendar_gap_days"] = str(calendar_gap)
        if hist_state:
            if hist_state.get("cloud_signature"):
                enriched["_historical_cloud_signature"] = str(hist_state.get("cloud_signature"))
            if hist_state.get("_state_source"):
                enriched["_historical_state_source"] = str(hist_state.get("_state_source"))
            if hist_state.get("temp_c") is not None:
                enriched["_historical_hour_temp_c"] = f"{float(hist_state.get('temp_c')):.1f}"
            if hist_state.get("dew_c") is not None:
                enriched["_historical_hour_dew_c"] = f"{float(hist_state.get('dew_c')):.1f}"
            if hist_state.get("wind_sector"):
                enriched["_historical_hour_wind_sector"] = str(hist_state.get("wind_sector"))
        enriched["_similarity_reasons"] = "；".join(
            text for _weight, text in sorted(reasons, key=lambda item: item[0], reverse=True)[:4]
        )
        enriched["_impact_summary"] = _analog_impact_summary(enriched)
        rows.append(enriched)
    rows.sort(key=lambda item: float(item["_similarity_score"]), reverse=True)
    return rows[:limit]


def _live_state_vector(metar_diag: dict[str, Any], live_regime: dict[str, Any]) -> dict[str, Any]:
    signals = build_live_condition_signals(metar_diag)
    return {
        "temp_c": _safe_float(signals.get("latest_temp_c")),
        "dew_c": _safe_float(signals.get("dewpoint_c")),
        "wind_sector": live_regime.get("wind_sector") or wind_sector_from_diag(signals.get("latest_wdir_deg")),
        "wind_speed_ms": kt_to_ms(_safe_float(signals.get("latest_wspd_kt"))),
        "cloud_effective_cover": _safe_float(signals.get("cloud_effective_cover")),
        "cloud_signature": live_regime.get("cloud_signature") or _live_cloud_signature(metar_diag),
        "precip_state": str(signals.get("precip_state") or "none").lower(),
    }


def summarize_analog_group(
    analogs: list[dict[str, str]],
    weighted_reference: dict[str, Any] | None = None,
) -> list[str]:
    if not analogs:
        return []
    focus = _high_confidence_analogs(analogs)
    if not focus:
        return []

    focus_dates = " / ".join(str(row.get("local_date") or "") for row in focus[:4] if row.get("local_date"))
    tmax_values = [_safe_float(row.get("tmax_c")) for row in focus]
    tmax_values = [value for value in tmax_values if value is not None]
    peak_values = [_safe_float(row.get("peak_hour_local")) for row in focus]
    peak_values = [value for value in peak_values if value is not None]

    lines: list[str] = []
    if focus_dates:
        lines.append(f"高度相似参考日：{focus_dates}")

    consensus_parts: list[str] = []
    if tmax_values:
        consensus_parts.append(
            f"Tmax 共识带 `{min(tmax_values):.1f}-{max(tmax_values):.1f}°C`，中位 `{statistics.median(tmax_values):.1f}°C`"
        )
    if peak_values:
        consensus_parts.append(
            f"峰值多落在 `{_fmt_hour(min(peak_values))}-{_fmt_hour(max(peak_values))}`"
        )
    if consensus_parts:
        lines.append("相似日共识：" + "；".join(consensus_parts))

    shared_windows = _top_common_text([str(row.get("_row_climate_window") or "") for row in focus], minimum=2)
    shared_regimes = _top_common_text([regime_to_cn(str(row.get("primary_regime") or "")) for row in focus], minimum=2)
    shared_cloud_values = [
        cloud_signature_to_cn(str(row.get("_historical_cloud_signature")))
        for row in focus
        if str(row.get("_historical_cloud_signature") or "").strip()
    ]
    shared_clouds = _top_common_text(shared_cloud_values, minimum=2)
    shared_winds = _top_common_text([str(row.get("dominant_wind_sector") or "") for row in focus], minimum=2)

    signal_parts: list[str] = []
    if shared_windows:
        signal_parts.append(f"同属 `{shared_windows}`")
    if shared_regimes:
        signal_parts.append(f"主日型集中在 `{shared_regimes}`")
    if shared_clouds:
        signal_parts.append(f"云结构多为 `{shared_clouds}`")
    if shared_winds:
        signal_parts.append(f"主导风向偏 `{shared_winds}`")
    if signal_parts:
        lines.append("共同特征：" + "；".join(signal_parts))

    driver_text = _shared_driver_summary(focus)
    if driver_text:
        lines.append(f"对最高温最相关的共同因子：{driver_text}")

    driver_balance = _driver_balance_summary(focus)
    if driver_balance:
        lines.extend(driver_balance)

    spread_text = _analog_spread_summary(focus, tmax_values, peak_values)
    if spread_text:
        lines.append(spread_text)
    if weighted_reference:
        weighted_text = _weighted_reference_text(weighted_reference)
        if weighted_text:
            lines.append(weighted_text)
    return lines


def summarize_analog_branches(
    analogs: list[dict[str, str]],
    branch_assessment: dict[str, Any] | None = None,
) -> list[str]:
    if not analogs:
        return []
    focus = _high_confidence_analogs(analogs)
    if len(focus) < 2:
        return []

    grouped: dict[str, list[dict[str, str]]] = defaultdict(list)
    for row in focus:
        grouped[_analog_branch_label(row)].append(row)

    ranked = sorted(grouped.items(), key=lambda item: (-len(item[1]), item[0]))
    detail_map = {
        str(item.get("label") or ""): item
        for item in ((branch_assessment or {}).get("branch_details") or [])
        if str(item.get("label") or "").strip()
    }
    if len(ranked) <= 1:
        label, rows = ranked[0]
        summary = _branch_metrics_text(rows)
        detail = detail_map.get(label) or {}
        fit = _safe_float(detail.get("fit_score"))
        strength = str(detail.get("fit_label") or "")
        extra = f"；贴合度 `{fit:.2f}`（{strength}）" if fit is not None and strength else ""
        return [f"路径分支：高相似样本基本收敛在 `{label}`，{summary}{extra}"]

    lines: list[str] = []
    for index, (label, rows) in enumerate(ranked[:2], start=1):
        summary = _branch_metrics_text(rows)
        detail = detail_map.get(label) or {}
        fit = _safe_float(detail.get("fit_score"))
        strength = str(detail.get("fit_label") or "")
        rationale = str(detail.get("rationale") or "")
        extra = []
        if fit is not None and strength:
            extra.append(f"贴合度 `{fit:.2f}`（{strength}）")
        if rationale:
            extra.append(f"依据：{rationale}")
        suffix = "；".join(extra)
        lines.append(f"路径分支{index}：`{label}`，{summary}" + (f"；{suffix}" if suffix else ""))
    return lines


def _historical_state_for_row(
    station_id: str,
    row: dict[str, Any],
    target_hour: int | None,
    target_minute: int | None,
) -> dict[str, Any] | None:
    local_date = str(row.get("local_date") or "")
    if not local_date:
        return None
    raw_state = _select_hourly_state(station_id, local_date, target_hour, target_minute)
    if raw_state:
        return raw_state
    cloud_cover = _historical_cloud_for_hour(row, target_hour)
    return {
        "temp_c": _historical_temp_for_hour(row, target_hour),
        "dew_c": _historical_dew_for_hour(row, target_hour),
        "wind_sector": str(row.get("dominant_wind_sector") or ""),
        "wind_speed_ms": _safe_float(row.get("wind_speed_mean_ms")),
        "cloud_effective_cover": cloud_cover,
        "cloud_signature": _daily_cloud_signature(row, target_hour),
        "precip_state": "precip" if str(row.get("precip_day_flag") or "").lower() == "true" else "none",
        "_state_source": "daily_fallback",
    }


def _select_hourly_state(
    station_id: str,
    local_date: str,
    target_hour: int | None,
    target_minute: int | None,
) -> dict[str, Any] | None:
    if target_hour is None:
        return None
    hourly_index = _load_station_hourly_index(station_id)
    if not hourly_index:
        return None

    current_minute = 30 if target_minute is None else target_minute
    candidates: list[tuple[int, dict[str, Any], int, int]] = []
    for hour_gap in (0, -1, 1):
        bucket = hourly_index.get((local_date, target_hour + hour_gap))
        if not bucket:
            continue
        for state in bucket:
            minute_gap = abs(int(state.get("local_minute") or 0) - current_minute)
            total_gap = abs(hour_gap) * 60 + minute_gap
            candidates.append((total_gap, state, abs(hour_gap), minute_gap))
    if not candidates:
        return None

    _total_gap, picked, hour_gap, minute_gap = min(candidates, key=lambda item: item[0])
    enriched = dict(picked)
    enriched["_hour_gap"] = hour_gap
    enriched["_minute_gap"] = minute_gap
    enriched["_state_source"] = "raw_hourly"
    return enriched


def _parse_raw_hourly_state(raw: dict[str, str], zone: ZoneInfo) -> dict[str, Any] | None:
    raw_date = str(raw.get("DATE") or "").strip()
    if not raw_date:
        return None
    try:
        utc_dt = datetime.fromisoformat(raw_date)
    except Exception:
        return None
    if utc_dt.tzinfo is None:
        utc_dt = utc_dt.replace(tzinfo=timezone.utc)
    else:
        utc_dt = utc_dt.astimezone(timezone.utc)

    local_dt = utc_dt.astimezone(zone)
    cloud_layers = _parse_raw_cloud_layers(raw)
    ceiling_m = _parse_distance_code(raw.get("CIG"))
    lowest_layer_m = min(
        [float(layer["base_m"]) for layer in cloud_layers if layer.get("base_m") is not None],
        default=None,
    )
    cloud_effective_cover = _raw_cloud_effective_cover(cloud_layers)
    return {
        "local_date": str(local_dt.date()),
        "local_hour": local_dt.hour,
        "local_minute": local_dt.minute,
        "temp_c": _parse_signed_tenths(raw.get("TMP")),
        "dew_c": _parse_signed_tenths(raw.get("DEW")),
        "wind_dir_deg": _parse_wind_direction(raw.get("WND")),
        "wind_sector": wind_sector_from_diag(_parse_wind_direction(raw.get("WND"))),
        "wind_speed_ms": _parse_wind_speed(raw.get("WND")),
        "visibility_m": _parse_distance_code(raw.get("VIS")),
        "ceiling_m": ceiling_m,
        "cloud_layer_count": len(cloud_layers),
        "cloud_effective_cover": cloud_effective_cover,
        "cloud_signature": _raw_cloud_signature(len(cloud_layers), lowest_layer_m, ceiling_m, cloud_effective_cover),
        "precip_state": "precip" if _raw_precip_flag(raw) else "none",
    }


def _parse_raw_cloud_layers(raw: dict[str, str]) -> list[dict[str, float | str | None]]:
    layers: list[dict[str, float | str | None]] = []
    for key in ("GA1", "GA2", "GA3"):
        value = str(raw.get(key) or "").strip()
        if not value:
            continue
        parts = value.split(",")
        if len(parts) < 3:
            continue
        cover_code = parts[0].strip()
        base_code = parts[2].strip()
        if cover_code in {"00", "99"}:
            continue
        cover_fraction = RAW_CLOUD_COVER_MAP.get(cover_code)
        if base_code in {"99999", "999999", "+99999", ""}:
            base_m = None
        else:
            base_m = _safe_float(base_code.replace("+", ""))
        if cover_fraction is None:
            continue
        layers.append({"cover_code": cover_code, "cover_fraction": cover_fraction, "base_m": base_m})
    return layers


def _raw_cloud_effective_cover(layers: list[dict[str, float | str | None]]) -> float | None:
    if not layers:
        return 0.0
    scored: list[tuple[float, float]] = []
    for layer in layers:
        base_m = _safe_float(layer.get("base_m"))
        cover_fraction = _safe_float(layer.get("cover_fraction"))
        if cover_fraction is None:
            continue
        order = base_m if base_m is not None else 99999.0
        scored.append((order, max(0.0, min(1.0, cover_fraction * _raw_cloud_base_weight_m(base_m)))))
    if not scored:
        return None
    scored.sort(key=lambda item: item[0])
    gammas = [1.0, 0.55, 0.30, 0.20]
    prod = 1.0
    for index, (_order, base_score) in enumerate(scored):
        gamma = gammas[index] if index < len(gammas) else 0.15
        prod *= (1.0 - max(0.0, min(0.98, gamma * base_score)))
    return max(0.0, min(1.0, 1.0 - prod))


def _raw_cloud_base_weight_m(base_m: float | None) -> float:
    if base_m is None:
        return 0.65
    if base_m < 760:
        return 1.00
    if base_m < 2100:
        return 0.75
    if base_m < 4500:
        return 0.45
    return 0.25


def _raw_cloud_signature(
    layer_count: int,
    lowest_layer_m: float | None,
    ceiling_m: float | None,
    cloud_effective_cover: float | None,
) -> str:
    low_ref = min([value for value in (lowest_layer_m, ceiling_m) if value is not None], default=None)
    eff = 0.0 if cloud_effective_cover is None else float(cloud_effective_cover)
    if layer_count == 0 and eff <= 0.12:
        return "clear_open"
    if low_ref is not None and low_ref <= 1200 and layer_count >= 2:
        return "low_multilayer"
    if low_ref is not None and low_ref <= 1200:
        return "low_single_layer"
    if layer_count >= 2 and low_ref is not None and low_ref > 2000 and eff <= 0.55:
        return "high_cloud_open_low"
    if layer_count >= 2:
        return "multilayer_cloud"
    if eff >= 0.75:
        return "solid_cloud"
    if eff <= 0.20:
        return "open_sky"
    return "mixed_cloud"


def _daily_cloud_signature(row: dict[str, Any], hour: int | None) -> str | None:
    share = _historical_cloud_for_hour(row, hour)
    if share is None:
        return None
    if share >= 0.65:
        return "solid_cloud"
    if share >= 0.45:
        return "low_single_layer"
    if share <= 0.15:
        return "open_sky"
    return "mixed_cloud"


def _live_cloud_signature(metar_diag: dict[str, Any]) -> str | None:
    tokens = metar_diag.get("latest_cloud_tokens")
    if isinstance(tokens, str):
        live_tokens = [item.strip() for item in tokens.split(";") if item.strip()]
    elif isinstance(tokens, list):
        live_tokens = [str(item).strip() for item in tokens if str(item).strip()]
    else:
        live_tokens = []

    layer_count = len([token for token in live_tokens if token.upper() not in {"CLR", "SKC", "CAVOK"}])
    lowest_base_ft = _safe_float(metar_diag.get("latest_cloud_lowest_base_ft"))
    lowest_base_m = (lowest_base_ft * 0.3048) if lowest_base_ft is not None else None
    cloud_cover = _safe_float(metar_diag.get("cloud_effective_cover_smooth"))
    if cloud_cover is None:
        cloud_cover = _safe_float(metar_diag.get("cloud_effective_cover"))
    cloud_code = str(metar_diag.get("latest_cloud_code") or "").upper().strip()

    if layer_count == 0 and cloud_code in {"CLR", "SKC", "CAVOK"} and (cloud_cover is None or cloud_cover <= 0.15):
        return "clear_open"
    if lowest_base_m is not None and lowest_base_m <= 1200 and layer_count >= 2:
        return "low_multilayer"
    if lowest_base_m is not None and lowest_base_m <= 1200 and cloud_code in {"BKN", "OVC", "VV", "SCT", "FEW"}:
        return "low_single_layer"
    if layer_count >= 2 and lowest_base_m is not None and lowest_base_m > 2000 and (cloud_cover is None or cloud_cover <= 0.55):
        return "high_cloud_open_low"
    if layer_count >= 2:
        return "multilayer_cloud"
    if cloud_cover is not None and cloud_cover >= 0.75:
        return "solid_cloud"
    if cloud_cover is not None and cloud_cover <= 0.20:
        return "open_sky"
    if cloud_code in {"CLR", "SKC", "CAVOK"}:
        return "clear_open"
    return "mixed_cloud"


def cloud_signature_to_cn(signature: str | None) -> str:
    return {
        "clear_open": "晴空开阔",
        "open_sky": "较开阔天空",
        "low_single_layer": "低云单层",
        "low_multilayer": "低顶多层云",
        "high_cloud_open_low": "高云主导但低层开窗",
        "multilayer_cloud": "多层云主导",
        "solid_cloud": "厚云覆盖",
        "mixed_cloud": "混合云场",
    }.get(str(signature or ""), str(signature or "混合云场"))


def _cloud_signature_family(signature: str | None) -> str | None:
    signature = str(signature or "")
    if signature in {"clear_open", "open_sky", "high_cloud_open_low"}:
        return "open"
    if signature in {"low_single_layer", "low_multilayer"}:
        return "low"
    if signature in {"multilayer_cloud", "solid_cloud", "mixed_cloud"}:
        return "cloud"
    return None


def _high_confidence_analogs(analogs: list[dict[str, str]]) -> list[dict[str, str]]:
    if not analogs:
        return []
    best = _safe_float(analogs[0].get("_similarity_score"))
    if best is None:
        return analogs[:3]
    rows = [row for row in analogs if (_safe_float(row.get("_similarity_score")) or 0.0) >= (best - 1.25)]
    if len(rows) < min(2, len(analogs)):
        rows = analogs[: min(2, len(analogs))]
    return rows[:4]


def _top_common_text(values: list[str], *, minimum: int = 2) -> str:
    cleaned = [value.strip() for value in values if str(value or "").strip()]
    if not cleaned:
        return ""
    counts: dict[str, int] = defaultdict(int)
    for value in cleaned:
        counts[value] += 1
    ranked = sorted(counts.items(), key=lambda item: (-item[1], item[0]))
    selected = [value for value, count in ranked if count >= minimum][:2]
    return " / ".join(selected)


def _shared_driver_summary(rows: list[dict[str, str]]) -> str:
    driver_labels = {
        "midday_low_ceiling_flag": "午间低云压制",
        "rain_reset_flag": "降水/湿重置",
        "cloud_break_rebound_flag": "午前压制后开窗反弹",
        "late_surge_flag": "末段仍有上冲",
        "humid_sticky_flag": "高露点拖慢混合",
        "dry_mixing_flag": "干混合抬高上限",
        "wind_shift_transition_flag": "风向切换改变路径",
        "light_wind_stagnation_flag": "轻风滞留抑制混合",
        "clean_solar_ramp_flag": "晴空增温效率高",
    }
    hits: list[str] = []
    threshold = max(2, len(rows) // 2 + 1)
    for key, label in driver_labels.items():
        count = sum(1 for row in rows if str(row.get(key) or "").lower() == "true")
        if count >= threshold:
            hits.append(label)
    return "；".join(hits[:3])


def _driver_balance_summary(rows: list[dict[str, str]]) -> list[str]:
    boost_labels = {
        "cloud_break_rebound_flag": "开窗反弹",
        "late_surge_flag": "末段上冲",
        "dry_mixing_flag": "干混合",
        "clean_solar_ramp_flag": "晴空增温",
    }
    suppress_labels = {
        "midday_low_ceiling_flag": "午间低云",
        "rain_reset_flag": "降水/湿重置",
        "humid_sticky_flag": "高露点滞留",
        "light_wind_stagnation_flag": "轻风滞留",
        "low_visibility_day_flag": "低能见静稳",
    }
    threshold = max(2, len(rows) // 2 + 1)
    boosts = [label for key, label in boost_labels.items() if sum(_flag_true(row.get(key)) for row in rows) >= threshold]
    suppress = [label for key, label in suppress_labels.items() if sum(_flag_true(row.get(key)) for row in rows) >= threshold]
    lines: list[str] = []
    if boosts:
        lines.append("共同抬温因子：" + "；".join(boosts[:3]))
    if suppress:
        lines.append("共同压温因子：" + "；".join(suppress[:3]))
    return lines


def _analog_spread_summary(
    rows: list[dict[str, str]],
    tmax_values: list[float],
    peak_values: list[float],
) -> str:
    if len(rows) < 2 or not tmax_values:
        return ""
    tmax_spread = max(tmax_values) - min(tmax_values)
    peak_spread = (max(peak_values) - min(peak_values)) if peak_values else None
    if tmax_spread <= 1.5 and (peak_spread is None or peak_spread <= 1.0):
        return "这些参照日对最高温路径较一致，可把它们视作同一类参考。"
    if tmax_spread >= 3.0 or (peak_spread is not None and peak_spread >= 2.0):
        return "这些参照日虽然都相似，但最高温路径已有分叉，需重点盯云量演变和午后斜率来决定落在哪一支。"
    return "这些参照日整体同向，但最高温上限和峰值时刻仍有中等分散。"


def _analog_impact_summary(row: dict[str, str]) -> str:
    effects: list[str] = []
    tmax = _safe_float(row.get("tmax_c"))
    if tmax is not None:
        effects.append(f"Tmax `{tmax:.1f}°C`")
    peak_text = _fmt_peak_value(row)
    if peak_text != "n/a":
        effects.append(f"峰值 `{peak_text}`")

    impact_bits: list[str] = []
    if str(row.get("midday_low_ceiling_flag") or "").lower() == "true":
        share = _safe_float(row.get("midday_low_ceiling_share"))
        if share is not None:
            impact_bits.append(f"午间低云偏重（占比 `{share * 100:.0f}%`）")
        else:
            impact_bits.append("午间低云压制明显")
    if str(row.get("rain_reset_flag") or "").lower() == "true":
        impact_bits.append("有降水/湿重置，午前启动偏慢")
    if str(row.get("cloud_break_rebound_flag") or "").lower() == "true":
        impact_bits.append("午前压制后有开窗反弹")
    if str(row.get("late_surge_flag") or "").lower() == "true":
        late_ramp = _safe_float(row.get("late_ramp_c"))
        if late_ramp is not None:
            impact_bits.append(f"尾段仍有上冲（late ramp `{late_ramp:.1f}°C`）")
        else:
            impact_bits.append("尾段仍有上冲")
    if str(row.get("dry_mixing_flag") or "").lower() == "true":
        impact_bits.append("干混合抬高上限")
    if str(row.get("humid_sticky_flag") or "").lower() == "true":
        impact_bits.append("高露点拖慢混合")
    if str(row.get("wind_shift_transition_flag") or "").lower() == "true":
        impact_bits.append("风向切换改变升温路径")

    wind_sector = str(row.get("_historical_hour_wind_sector") or row.get("dominant_wind_sector") or "").strip()
    raw_cloud_signature = str(row.get("_historical_cloud_signature") or "").strip()
    cloud_signature = cloud_signature_to_cn(raw_cloud_signature) if raw_cloud_signature else ""
    if wind_sector:
        impact_bits.append(f"对应风向 `{wind_sector}`")
    if cloud_signature:
        impact_bits.append(f"云结构 `{cloud_signature}`")

    if impact_bits:
        effects.append("；".join(impact_bits[:4]))
    return "；".join(effects)


def build_weighted_reference(
    analogs: list[dict[str, str]],
    current_state: dict[str, Any],
    live_regime: dict[str, Any],
    branch_assessment: dict[str, Any] | None = None,
    synoptic_context: dict[str, Any] | None = None,
) -> dict[str, Any] | None:
    if not analogs:
        return None
    selected_rows = list((branch_assessment or {}).get("selected_rows") or [])
    focus = selected_rows or _high_confidence_analogs(analogs)
    weighted_rows: list[tuple[float, dict[str, str]]] = []
    for row in focus:
        score = _safe_float(row.get("_similarity_score"))
        tmax_c = _safe_float(row.get("tmax_c"))
        if score is None or tmax_c is None:
            continue
        weighted_rows.append((score, row))
    if not weighted_rows:
        return None

    scores = [score for score, _row in weighted_rows]
    max_score = max(scores)
    weights = [math.exp((score - max_score) / 2.0) for score in scores]
    tmax_pairs = [(weight, _safe_float(row.get("tmax_c"))) for weight, (_score, row) in zip(weights, weighted_rows)]
    tmax_pairs = [(weight, value) for weight, value in tmax_pairs if value is not None]
    if not tmax_pairs:
        return None

    center = _weighted_mean(tmax_pairs)
    low = _weighted_quantile(tmax_pairs, 0.25)
    high = _weighted_quantile(tmax_pairs, 0.75)
    peak_pairs = [(weight, _safe_float(row.get("peak_hour_local"))) for weight, (_score, row) in zip(weights, weighted_rows)]
    peak_pairs = [(weight, value) for weight, value in peak_pairs if value is not None]
    hist_hour_pairs = [(weight, _safe_float(row.get("_historical_hour_temp_c"))) for weight, (_score, row) in zip(weights, weighted_rows)]
    hist_hour_pairs = [(weight, value) for weight, value in hist_hour_pairs if value is not None]

    hist_hour_center = _weighted_mean(hist_hour_pairs) if hist_hour_pairs else None
    live_temp = _safe_float(current_state.get("temp_c"))
    live_temp_delta = None if live_temp is None or hist_hour_center is None else live_temp - hist_hour_center

    primary = str(live_regime.get("primary_regime") or "")
    strength = str((branch_assessment or {}).get("reference_strength") or "weak")
    selected_branch = str((branch_assessment or {}).get("preferred_branch") or "")
    alignment = branch_alignment(selected_branch, synoptic_context)
    factor = 0.55
    if primary in {"late_surge", "cloud_break_rebound", "dry_mixing", "clean_solar_ramp"}:
        factor = 0.65
    elif primary in {"rain_reset", "cloud_suppressed", "humid_sticky", "light_wind_stagnation"}:
        factor = 0.50
    delta_cap = 1.2
    if strength == "medium":
        factor += 0.05
        delta_cap = 1.7
    elif strength == "strong":
        factor += 0.12
        delta_cap = 2.2
    if alignment.get("alignment") == "supportive":
        factor += 0.03
        delta_cap += 0.15
    elif alignment.get("alignment") == "conflicting":
        factor = max(0.35, factor - 0.05)
        delta_cap = max(0.8, delta_cap - 0.2)
    recommended_delta = 0.0 if live_temp_delta is None else _clamp(live_temp_delta * factor, -delta_cap, delta_cap)
    recommended_center = None if center is None else center + recommended_delta
    recommended_low = None if low is None else low + recommended_delta
    recommended_high = None if high is None else high + recommended_delta
    branch_names = sorted({_analog_branch_label(row) for _score, row in weighted_rows})
    selected_dates = [
        str(row.get("local_date") or "")
        for _score, row in weighted_rows
        if str(row.get("local_date") or "").strip()
    ]

    return {
        "analog_count": len(weighted_rows),
        "tmax_center_c": center,
        "tmax_p25_c": low,
        "tmax_p75_c": high,
        "peak_center_hour_local": _weighted_mean(peak_pairs) if peak_pairs else None,
        "historical_same_hour_temp_c": hist_hour_center,
        "live_temp_c": live_temp,
        "live_temp_delta_c": live_temp_delta,
        "recommended_delta_c": recommended_delta,
        "recommended_tmax_c": recommended_center,
        "recommended_tmax_p25_c": recommended_low,
        "recommended_tmax_p75_c": recommended_high,
        "branch_names": branch_names,
        "selected_dates": selected_dates[:4],
        "reference_strength": strength,
        "selected_branch": selected_branch,
        "synoptic_alignment": str(alignment.get("alignment") or "neutral"),
        "synoptic_alignment_rationale": str(alignment.get("rationale") or ""),
        "synoptic_context_source": str((synoptic_context or {}).get("source") or ""),
    }


def _weighted_reference_text(weighted_reference: dict[str, Any]) -> str:
    center = _safe_float(weighted_reference.get("tmax_center_c"))
    low = _safe_float(weighted_reference.get("tmax_p25_c"))
    high = _safe_float(weighted_reference.get("tmax_p75_c"))
    recommended = _safe_float(weighted_reference.get("recommended_tmax_c"))
    rec_low = _safe_float(weighted_reference.get("recommended_tmax_p25_c"))
    rec_high = _safe_float(weighted_reference.get("recommended_tmax_p75_c"))
    same_hour_hist = _safe_float(weighted_reference.get("historical_same_hour_temp_c"))
    live_temp = _safe_float(weighted_reference.get("live_temp_c"))
    live_delta = _safe_float(weighted_reference.get("live_temp_delta_c"))
    peak = _safe_float(weighted_reference.get("peak_center_hour_local"))
    strength = str(weighted_reference.get("reference_strength") or "")
    selected_branch = str(weighted_reference.get("selected_branch") or "")
    parts: list[str] = []
    if center is not None:
        if low is not None and high is not None:
            parts.append(f"analog 加权参考 `{center:.1f}°C`，参考带 `{low:.1f}-{high:.1f}°C`")
        else:
            parts.append(f"analog 加权参考 `{center:.1f}°C`")
    if selected_branch or strength:
        text = "分支"
        if selected_branch:
            text += f" `{selected_branch}`"
        if strength:
            strength_cn = {"weak": "弱参考", "medium": "中参考", "strong": "强参考"}.get(strength, strength)
            text += f"，权重 `{strength_cn}`"
        parts.append(text)
    if peak is not None:
        parts.append(f"加权峰值时刻约 `{_fmt_hour(peak)}`")
    if same_hour_hist is not None and live_temp is not None and live_delta is not None:
        parts.append(
            f"同小时历史参考 `{same_hour_hist:.1f}°C`，当前实况 `{live_temp:.1f}°C`，偏差 `{live_delta:+.1f}°C`"
        )
    if recommended is not None:
        if rec_low is not None and rec_high is not None:
            parts.append(f"据此修正后的温度参考中心 `{recommended:.1f}°C`，区间 `{rec_low:.1f}-{rec_high:.1f}°C`")
        else:
            parts.append(f"据此修正后的温度参考中心 `{recommended:.1f}°C`")
    return "加权参考：" + "；".join(parts) if parts else ""


def _analog_branch_label(row: dict[str, str]) -> str:
    if _flag_true(row.get("cloud_break_rebound_flag")):
        return "开窗反弹支"
    if _flag_true(row.get("cloud_suppressed_flag")):
        return "云压制支"
    if _flag_true(row.get("rain_reset_flag")):
        return "降水重置支"
    if _flag_true(row.get("late_surge_flag")):
        return "末段冲高支"
    if _flag_true(row.get("dry_mixing_flag")) or _flag_true(row.get("clean_solar_ramp_flag")):
        return "干混合/晴空增温支"
    if _flag_true(row.get("humid_sticky_flag")):
        return "湿热滞留支"
    if _flag_true(row.get("wind_shift_transition_flag")):
        return "风向切换支"
    signature = cloud_signature_to_cn(str(row.get("_historical_cloud_signature") or ""))
    if signature and signature != "混合云场":
        return f"{signature}支"
    return f"{regime_to_cn(str(row.get('primary_regime') or 'mixed_transitional'))}支"


def _branch_metrics_text(rows: list[dict[str, str]]) -> str:
    dates = " / ".join(str(row.get("local_date") or "") for row in rows[:3] if row.get("local_date"))
    tmax_values = [_safe_float(row.get("tmax_c")) for row in rows]
    tmax_values = [value for value in tmax_values if value is not None]
    peak_values = [_safe_float(row.get("peak_hour_local")) for row in rows]
    peak_values = [value for value in peak_values if value is not None]
    pieces: list[str] = []
    if dates:
        pieces.append(f"日期 `{dates}`")
    if tmax_values:
        pieces.append(
            f"Tmax `{min(tmax_values):.1f}-{max(tmax_values):.1f}°C`，中位 `{statistics.median(tmax_values):.1f}°C`"
        )
    if peak_values:
        pieces.append(f"峰值 `{_fmt_hour(min(peak_values))}-{_fmt_hour(max(peak_values))}`")
    drivers = _shared_driver_summary(rows)
    if drivers:
        pieces.append(f"关键驱动 `{drivers}`")
    return "；".join(pieces)


def assess_analog_branches(
    analogs: list[dict[str, str]],
    metar_diag: dict[str, Any],
    live_regime: dict[str, Any],
    *,
    synoptic_context: dict[str, Any] | None = None,
) -> dict[str, Any]:
    if not analogs:
        return {}
    focus = _high_confidence_analogs(analogs)
    if not focus:
        return {}

    grouped: dict[str, list[dict[str, str]]] = defaultdict(list)
    for row in focus:
        grouped[_analog_branch_label(row)].append(row)

    details: list[dict[str, Any]] = []
    for label, rows in grouped.items():
        fit_score, rationale = _branch_fit_score(label, rows, metar_diag, live_regime, synoptic_context=synoptic_context)
        tmax_values = [_safe_float(row.get("tmax_c")) for row in rows]
        tmax_values = [value for value in tmax_values if value is not None]
        peak_values = [_safe_float(row.get("peak_hour_local")) for row in rows]
        peak_values = [value for value in peak_values if value is not None]
        convergence_bonus = 0.0
        if tmax_values:
            spread = max(tmax_values) - min(tmax_values)
            if spread <= 1.2:
                convergence_bonus += 0.5
            elif spread <= 2.0:
                convergence_bonus += 0.2
        if peak_values:
            peak_spread = max(peak_values) - min(peak_values)
            if peak_spread <= 0.75:
                convergence_bonus += 0.3
            elif peak_spread <= 1.5:
                convergence_bonus += 0.1
        fit_score += convergence_bonus
        fit_label = "高"
        if fit_score < 4.8:
            fit_label = "中"
        if fit_score < 3.6:
            fit_label = "低"
        details.append(
            {
                "label": label,
                "fit_score": round(fit_score, 2),
                "fit_label": fit_label,
                "rationale": rationale,
                "row_count": len(rows),
                "tmax_spread_c": round(max(tmax_values) - min(tmax_values), 2) if len(tmax_values) >= 2 else 0.0,
                "tmax_center_c": round(statistics.median(tmax_values), 2) if tmax_values else None,
                "peak_center_hour_local": round(statistics.median(peak_values), 2) if peak_values else None,
                "rows": rows,
            }
        )

    if not details:
        return {}
    details.sort(key=lambda item: (-float(item.get("fit_score") or 0.0), -int(item.get("row_count") or 0), str(item.get("label") or "")))
    top = details[0]
    second = details[1] if len(details) > 1 else None
    top_score = float(top.get("fit_score") or 0.0)
    second_score = float(second.get("fit_score") or 0.0) if second else None
    margin = top_score - second_score if second_score is not None else top_score
    top_center = _safe_float(top.get("tmax_center_c"))
    second_center = _safe_float(second.get("tmax_center_c")) if second else None
    center_gap = abs(top_center - second_center) if top_center is not None and second_center is not None else None

    branch_mode = "converged"
    if second is None:
        branch_mode = "converged"
    elif margin >= 0.9 and top_score >= 5.0:
        branch_mode = "preferred"
    elif margin < 0.35 or (center_gap is not None and center_gap >= 1.2 and margin < 0.7):
        branch_mode = "split"
    else:
        branch_mode = "competitive"

    strength = "weak"
    strength_cn = "弱参考"
    if top_score >= 5.2 and margin >= 0.8 and int(top.get("row_count") or 0) >= 2:
        strength = "strong"
        strength_cn = "强参考"
    elif top_score >= 4.2 and margin >= 0.35:
        strength = "medium"
        strength_cn = "中参考"

    selected_rows = top.get("rows") if strength in {"strong", "medium"} else focus
    return {
        "preferred_branch": str(top.get("label") or ""),
        "preferred_branch_rationale": str(top.get("rationale") or ""),
        "reference_strength": strength,
        "reference_strength_cn": strength_cn,
        "synoptic_context_source": str((synoptic_context or {}).get("source") or ""),
        "top_branch_score": round(top_score, 2),
        "second_branch_score": round(second_score, 2) if second_score is not None else None,
        "score_margin": round(margin, 2),
        "branch_mode": branch_mode,
        "branch_center_gap_c": round(center_gap, 2) if center_gap is not None else None,
        "selected_rows": selected_rows,
        "branch_details": [{k: v for k, v in item.items() if k != "rows"} for item in details],
    }


def _branch_fit_score(
    label: str,
    rows: list[dict[str, str]],
    metar_diag: dict[str, Any],
    live_regime: dict[str, Any],
    *,
    synoptic_context: dict[str, Any] | None = None,
) -> tuple[float, str]:
    similarities = [_safe_float(row.get("_similarity_score")) for row in rows]
    similarities = [value for value in similarities if value is not None]
    score = statistics.mean(similarities) / 3.0 if similarities else 0.0
    reasons: list[str] = []

    signals = build_live_condition_signals(metar_diag)
    temp_trend = _safe_float(signals.get("temp_trend_c"))
    radiation_eff = _safe_float(signals.get("radiation_eff"))
    cloud_cover = _safe_float(signals.get("cloud_effective_cover"))
    precip_state = str(signals.get("precip_state") or "none").lower()
    cloud_trend = str(signals.get("cloud_trend") or "")
    wind_change = _safe_float(signals.get("wind_dir_change_deg"))
    live_primary = str(live_regime.get("primary_regime") or "")

    if "末段冲高" in label:
        if temp_trend is not None and temp_trend >= 0.25:
            score += 0.9
            reasons.append("当前升温斜率仍偏强")
        if live_primary == "late_surge":
            score += 0.8
            reasons.append("live regime 指向末段冲高")
    if "开窗反弹" in label:
        if "开窗" in cloud_trend or "减弱" in cloud_trend:
            score += 0.9
            reasons.append("云量有开窗/减弱迹象")
        if temp_trend is not None and temp_trend >= 0.15:
            score += 0.5
            reasons.append("当前温度已开始回升")
        if live_primary == "cloud_break_rebound":
            score += 0.8
            reasons.append("live regime 指向开窗反弹")
    if "云压制" in label:
        if cloud_cover is not None and cloud_cover >= 0.65:
            score += 0.9
            reasons.append("当前云量仍偏重")
        if radiation_eff is not None and radiation_eff <= 0.45:
            score += 0.6
            reasons.append("辐射效率偏低")
        if live_primary == "cloud_suppressed":
            score += 0.8
            reasons.append("live regime 指向云压制")
    if "降水重置" in label:
        if precip_state not in {"", "none"}:
            score += 1.0
            reasons.append("当前仍有降水/湿重置")
        if live_primary == "rain_reset":
            score += 0.8
            reasons.append("live regime 指向降水重置")
    if "干混合" in label or "晴空增温" in label:
        if radiation_eff is not None and radiation_eff >= 0.75:
            score += 0.8
            reasons.append("辐射效率较高")
        if cloud_cover is not None and cloud_cover <= 0.20:
            score += 0.6
            reasons.append("低云约束较弱")
        if live_primary in {"dry_mixing", "clean_solar_ramp"}:
            score += 0.8
            reasons.append("live regime 偏干混合/晴空增温")
    if "湿热滞留" in label:
        dew_c = _safe_float(signals.get("dewpoint_c"))
        rh = _safe_float(signals.get("latest_rh"))
        if (dew_c is not None and dew_c >= 18.0) or (rh is not None and rh >= 75.0):
            score += 0.8
            reasons.append("当前湿度背景偏高")
        if live_primary == "humid_sticky":
            score += 0.8
            reasons.append("live regime 指向湿热滞留")
    if "风向切换" in label:
        if wind_change is not None and wind_change >= 45.0:
            score += 0.9
            reasons.append("当前风向变化较大")
        if live_primary == "wind_shift_transition":
            score += 0.8
            reasons.append("live regime 指向风向切换")

    alignment = branch_alignment(label, synoptic_context)
    if alignment.get("alignment") == "supportive":
        score += 0.6
        reasons.append(str(alignment.get("rationale") or "环流背景与当前分支同向"))
    elif alignment.get("alignment") == "conflicting":
        score -= 0.2

    return score, "；".join(reasons[:3])


def build_adjustment_hint(
    station_prior: dict[str, str],
    monthly_row: dict[str, str] | None,
    live_regime: dict[str, Any],
    analogs: list[dict[str, str]],
    *,
    weighted_reference: dict[str, Any] | None = None,
    branch_assessment: dict[str, Any] | None = None,
) -> str:
    baseline = _safe_float((monthly_row or {}).get("tmax_median_c"))
    recommended = _safe_float((weighted_reference or {}).get("recommended_tmax_c"))
    strength = str((branch_assessment or {}).get("reference_strength") or (weighted_reference or {}).get("reference_strength") or "")
    preferred_branch = str((branch_assessment or {}).get("preferred_branch") or (weighted_reference or {}).get("selected_branch") or "")
    if recommended is not None and baseline is not None:
        delta = recommended - baseline
        if delta >= 1.0 and live_regime.get("primary_regime") in {"late_surge", "cloud_break_rebound", "dry_mixing", "clean_solar_ramp"}:
            strength_text = {"strong": "，且历史拟合度高", "medium": "，历史参考偏强"}.get(strength, "")
            branch_text = f"（{preferred_branch}）" if preferred_branch else ""
            return f"加权历史参考{branch_text}高于同月基线，末段上修风险偏高{strength_text}"
        if delta <= -1.0 and live_regime.get("primary_regime") in {"rain_reset", "cloud_suppressed", "humid_sticky", "light_wind_stagnation"}:
            strength_text = {"strong": "，且历史拟合度高", "medium": "，历史参考偏强"}.get(strength, "")
            branch_text = f"（{preferred_branch}）" if preferred_branch else ""
            return f"加权历史参考{branch_text}低于同月基线，需防高估{strength_text}"
    elif baseline is not None:
        analog_tmax_values = [_safe_float(row.get("tmax_c")) for row in analogs]
        analog_tmax_values = [value for value in analog_tmax_values if value is not None]
        if analog_tmax_values:
            analog_mean = sum(analog_tmax_values) / len(analog_tmax_values)
            if analog_mean >= baseline + 1.0 and live_regime.get("primary_regime") in {"late_surge", "cloud_break_rebound", "dry_mixing", "clean_solar_ramp"}:
                return "类似历史日常高于同月基线，末段上修风险偏高"
            if analog_mean <= baseline - 1.0 and live_regime.get("primary_regime") in {"rain_reset", "cloud_suppressed", "humid_sticky", "light_wind_stagnation"}:
                return "类似历史日常低于同月基线，需防高估"

    late_peak_share = _safe_float(station_prior.get("late_peak_share"))
    if late_peak_share is not None and late_peak_share >= 0.55 and live_regime.get("primary_regime") == "late_surge":
        return "该站历史晚峰倾向强，临窗前不宜过早压顶"
    if _safe_float(station_prior.get("cloud_break_day_share")) is not None and _safe_float(station_prior.get("cloud_break_day_share")) >= 0.10 and live_regime.get("primary_regime") == "cloud_break_rebound":
        return "该站历史存在明显开窗反弹，需盯紧云层减弱时段"
    if _safe_float(station_prior.get("humid_sticky_day_share")) is not None and _safe_float(station_prior.get("humid_sticky_day_share")) >= 0.18 and live_regime.get("primary_regime") == "humid_sticky":
        return "该站高湿持续型占比不低，夜间回落和午后上冲都容易被拖慢"
    if _safe_float(station_prior.get("wind_shift_day_share")) is not None and _safe_float(station_prior.get("wind_shift_day_share")) >= 0.16 and live_regime.get("primary_regime") == "wind_shift_transition":
        return "该站对风向切换敏感，实况风转向可视为即时修正信号"
    return ""


def regime_to_cn(regime: str) -> str:
    return {
        "late_surge": "末段冲高",
        "cloud_suppressed": "云压制",
        "cloud_break_rebound": "开窗反弹",
        "humid_sticky": "湿热滞留",
        "dry_mixing": "干混合增温",
        "rain_reset": "降水重置",
        "light_wind_stagnation": "轻风滞留",
        "wind_shift_transition": "风向切换",
        "low_visibility_day": "低能见度",
        "clean_solar_ramp": "晴空增温",
        "mixed_transitional": "过渡型",
    }.get(str(regime or ""), str(regime or "过渡型"))


def translate_special_features(raw: Any) -> str:
    items = [item.strip() for item in str(raw or "").split(";") if item.strip()]
    if not items:
        return "n/a"
    translated = [FEATURE_CN_MAP.get(item, item) for item in items]
    return "；".join(translated)


def wind_sector_from_diag(wdir: Any) -> str | None:
    deg = _safe_float(wdir)
    if deg is None:
        return None
    dirs = ["N", "NE", "E", "SE", "S", "SW", "W", "NW"]
    idx = int(((deg + 22.5) % 360) // 45)
    return dirs[idx]


def _required_path(filename: str) -> Path:
    ref_dir = reference_dir()
    if ref_dir is None:
        raise FileNotFoundError("historical reference directory not found")
    path = ref_dir / filename
    if not path.exists():
        raise FileNotFoundError(path)
    return path


def _select_monthly_row(station_id: str, month: int | None) -> dict[str, str] | None:
    if month is None:
        return None
    exact = None
    fallback = None
    for row in _load_monthly_rows():
        if str(row.get("station_id") or "").upper() != station_id:
            continue
        row_month = _safe_int(row.get("month"))
        if row_month == month:
            exact = row
            break
        if fallback is None and row_month is not None:
            fallback = row
    return exact or fallback


def _target_month(target_date: str | None) -> int | None:
    if not target_date:
        return None
    try:
        return datetime.strptime(str(target_date), "%Y-%m-%d").month
    except Exception:
        return None


def _boundary_adjacent_months(target_date: str | None, edge_days: int = 10) -> set[int]:
    if not target_date:
        return set()
    try:
        dt = datetime.strptime(str(target_date), "%Y-%m-%d")
    except Exception:
        return set()
    out: set[int] = set()
    if dt.day <= edge_days:
        out.add((dt.replace(day=1) - timedelta(days=1)).month)
    next_month_start = (dt.replace(day=28) + timedelta(days=4)).replace(day=1)
    last_day = (next_month_start - timedelta(days=1)).day
    if (last_day - dt.day) < edge_days:
        out.add(next_month_start.month)
    return out

def _target_day_of_year(target_date: str | None) -> int | None:
    if not target_date:
        return None
    try:
        return datetime.strptime(str(target_date), "%Y-%m-%d").timetuple().tm_yday
    except Exception:
        return None


def _date_to_day_of_year(value: str | None) -> int | None:
    if not value:
        return None
    try:
        return datetime.strptime(str(value), "%Y-%m-%d").timetuple().tm_yday
    except Exception:
        return None


def _circular_day_distance(left: int, right: int) -> int:
    direct = abs(left - right)
    return min(direct, 366 - direct)


def _circular_month_distance(left: int, right: int) -> int:
    direct = abs(left - right)
    return min(direct, 12 - direct)


def _normalize_climate_windows(station_id: str) -> list[tuple[str, set[int]]]:
    configured = CITY_CLIMATE_WINDOWS.get(str(station_id or "").upper()) or []
    windows: list[tuple[str, set[int]]] = []
    for item in configured:
        label = str(item.get("label") or "").strip()
        months = {int(value) for value in item.get("months", []) if isinstance(value, int)}
        if label and months:
            windows.append((label, months))
    if windows:
        return windows
    return [
        ("冬季(12-2)", {12, 1, 2}),
        ("春季(3-5)", {3, 4, 5}),
        ("夏季(6-8)", {6, 7, 8}),
        ("秋季(9-11)", {9, 10, 11}),
    ]


def _window_label_for_date(station_id: str, target_date: str | None) -> str | None:
    month = _target_month(target_date)
    if month is None:
        return None
    for label, months in _normalize_climate_windows(station_id):
        if month in months:
            return label
    return None


def _window_label_for_row(station_id: str, row: dict[str, Any]) -> str | None:
    month = _safe_int(row.get("month"))
    if month is None:
        return None
    for label, months in _normalize_climate_windows(station_id):
        if month in months:
            return label
    return None


def _wind_family(sector: str | None) -> str | None:
    sector = str(sector or "").upper().strip()
    if sector in {"N", "NE", "NW"}:
        return "N"
    if sector in {"S", "SE", "SW"}:
        return "S"
    if sector == "E":
        return "E"
    if sector == "W":
        return "W"
    return None


def _historical_temp_for_hour(row: dict[str, Any], hour: int | None) -> float | None:
    if hour is None:
        return _safe_float(row.get("tmean_c"))
    if hour < 10:
        return _safe_float(row.get("morning_temp_c")) or _safe_float(row.get("tmean_c"))
    if hour < 13:
        return _safe_float(row.get("late_morning_temp_c")) or _safe_float(row.get("midday_temp_c"))
    if hour < 16:
        return _safe_float(row.get("midday_temp_c")) or _safe_float(row.get("late_day_temp_c"))
    return _safe_float(row.get("late_day_temp_c")) or _safe_float(row.get("midday_temp_c"))


def _historical_dew_for_hour(row: dict[str, Any], hour: int | None) -> float | None:
    if hour is None:
        return _safe_float(row.get("afternoon_dew_c")) or _safe_float(row.get("morning_dew_c"))
    if hour < 12:
        return _safe_float(row.get("morning_dew_c")) or _safe_float(row.get("afternoon_dew_c"))
    return _safe_float(row.get("afternoon_dew_c")) or _safe_float(row.get("morning_dew_c"))


def _historical_cloud_for_hour(row: dict[str, Any], hour: int | None) -> float | None:
    if hour is None:
        return _safe_float(row.get("midday_low_ceiling_share"))
    if hour < 11:
        return _safe_float(row.get("morning_low_ceiling_share"))
    if hour < 15:
        return _safe_float(row.get("midday_low_ceiling_share"))
    return _safe_float(row.get("afternoon_low_ceiling_share"))


def _parse_signed_tenths(raw: Any) -> float | None:
    if not isinstance(raw, str) or not raw:
        return None
    code = raw.split(",")[0].strip()
    if code in {"+9999", "-9999", "9999"}:
        return None
    try:
        return int(code) / 10.0
    except ValueError:
        return None


def _parse_wind_speed(raw: Any) -> float | None:
    if not isinstance(raw, str) or not raw:
        return None
    parts = raw.split(",")
    if len(parts) < 4:
        return None
    speed_code = parts[3].strip()
    if speed_code == "9999":
        return None
    try:
        return int(speed_code) / 10.0
    except ValueError:
        return None


def _parse_wind_direction(raw: Any) -> float | None:
    if not isinstance(raw, str) or not raw:
        return None
    parts = raw.split(",")
    if not parts:
        return None
    direction_code = parts[0].strip()
    if direction_code in {"999", "VRB"}:
        return None
    try:
        value = int(direction_code)
    except ValueError:
        return None
    if value > 360:
        return None
    return float(value)


def _parse_distance_code(raw: Any) -> float | None:
    if not isinstance(raw, str) or not raw:
        return None
    code = raw.split(",")[0].strip()
    if code in {"99999", "999999", "+99999", "22000"}:
        return None
    try:
        return float(code)
    except ValueError:
        return None


def _raw_precip_flag(raw: dict[str, str]) -> bool:
    for key in ("AA1", "AA2", "AA3", "AA4"):
        value = str(raw.get(key) or "").strip()
        if not value:
            continue
        parts = value.split(",")
        if len(parts) < 2:
            continue
        amount_code = parts[1].strip()
        if amount_code in {"9999", "99999", ""}:
            continue
        try:
            if int(amount_code) > 0:
                return True
        except ValueError:
            continue
    return False

def _flag_true(value: Any) -> bool:
    return str(value or "").strip().lower() in {"1", "true", "yes"}


def _weighted_mean(pairs: list[tuple[float, float | None]]) -> float | None:
    valid = [(float(weight), float(value)) for weight, value in pairs if value is not None and weight > 0]
    if not valid:
        return None
    total_weight = sum(weight for weight, _value in valid)
    if total_weight <= 0:
        return None
    return sum(weight * value for weight, value in valid) / total_weight


def _weighted_quantile(pairs: list[tuple[float, float | None]], quantile: float) -> float | None:
    valid = sorted((float(value), float(weight)) for weight, value in pairs if value is not None and weight > 0)
    if not valid:
        return None
    total_weight = sum(weight for _value, weight in valid)
    if total_weight <= 0:
        return None
    target = total_weight * max(0.0, min(1.0, quantile))
    running = 0.0
    for value, weight in valid:
        running += weight
        if running >= target:
            return value
    return valid[-1][0]


def _clamp(value: float, lower: float, upper: float) -> float:
    return max(lower, min(upper, value))


def _safe_float(value: Any) -> float | None:
    if value in (None, "", "n/a"):
        return None
    try:
        return float(value)
    except Exception:
        return None


def _safe_int(value: Any) -> int | None:
    if value in (None, "", "n/a"):
        return None
    try:
        return int(float(value))
    except Exception:
        return None


def _fmt_c(value: float | None) -> str:
    if value is None:
        return "n/a"
    return f"{value:.1f}°C"


def _fmt_pct(value: float | None) -> str:
    if value is None:
        return "n/a"
    return f"{value * 100:.0f}%"


def _fmt_hour(value: float | None) -> str:
    if value is None:
        return "n/a"
    total_minutes = int(round(float(value) * 60.0))
    total_minutes = max(0, min(total_minutes, 23 * 60 + 59))
    hour, minute = divmod(total_minutes, 60)
    return f"{hour:02d}:{minute:02d}"


def _fmt_peak_value(row: dict[str, Any]) -> str:
    peak_text = str(row.get("peak_time_local") or "").strip()
    if peak_text:
        return peak_text
    return _fmt_hour(_safe_float(row.get("peak_hour_local")))
