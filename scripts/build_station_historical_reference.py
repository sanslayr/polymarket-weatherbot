#!/usr/bin/env python3
"""Build repo-local historical reference rows for selected stations from raw ISD."""

from __future__ import annotations

import argparse
import csv
import gzip
import math
import statistics
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

from historical_context_provider import (
    _parse_distance_code,
    _parse_signed_tenths,
    _parse_wind_direction,
    _parse_wind_speed,
    _raw_precip_flag,
    wind_sector_from_diag,
)
from station_catalog import STATION_TZ

ROOT = Path(__file__).resolve().parent.parent
REFERENCE_DIR = ROOT / "data" / "historical_reference"
RAW_DIR = REFERENCE_DIR / "raw_metar_isd"
STATION_CSV = ROOT / "station_links.csv"
PRIOR_CSV = REFERENCE_DIR / "weatherbot_station_priors.csv"
MONTHLY_CSV = REFERENCE_DIR / "weatherbot_monthly_climatology.csv"
DAILY_CSV = REFERENCE_DIR / "weatherbot_daily_local_regimes.csv"
PROFILE_CSV = REFERENCE_DIR / "metar_station_profiles.csv"

DAILY_FIELDS = [
    "station_id",
    "city",
    "timezone",
    "year",
    "month",
    "local_date",
    "obs_count",
    "temp_obs_count",
    "tmax_c",
    "tmin_c",
    "tmean_c",
    "daily_range_c",
    "peak_hour_local",
    "peak_time_local",
    "morning_temp_c",
    "late_morning_temp_c",
    "midday_temp_c",
    "late_day_temp_c",
    "warmup_am_c",
    "warmup_noon_c",
    "late_ramp_c",
    "morning_dew_c",
    "afternoon_dew_c",
    "dewpoint_dep_afternoon_c",
    "morning_low_ceiling_share",
    "midday_low_ceiling_share",
    "afternoon_low_ceiling_share",
    "morning_visibility_restriction_share",
    "reduced_visibility_share",
    "visibility_min_m",
    "precip_obs_share",
    "morning_precip_share",
    "afternoon_precip_share",
    "wind_speed_mean_ms",
    "light_wind_share",
    "strong_wind_share",
    "wind_shift_count",
    "dominant_wind_sector",
    "warm_day_flag",
    "hot_day_flag",
    "late_peak_flag",
    "very_late_peak_flag",
    "morning_low_ceiling_flag",
    "midday_low_ceiling_flag",
    "afternoon_low_ceiling_flag",
    "reduced_visibility_flag",
    "light_wind_flag",
    "precip_day_flag",
    "late_surge_flag",
    "cloud_suppressed_flag",
    "cloud_break_rebound_flag",
    "humid_sticky_flag",
    "dry_mixing_flag",
    "rain_reset_flag",
    "light_wind_stagnation_flag",
    "wind_shift_transition_flag",
    "low_visibility_day_flag",
    "clean_solar_ramp_flag",
    "primary_regime",
    "regime_tags",
    "regime_confidence",
]

MONTHLY_FIELDS = [
    "station_id",
    "city",
    "month",
    "days",
    "tmax_median_c",
    "peak_hour_median",
    "daily_range_median_c",
    "warmup_am_median_c",
    "warmup_noon_median_c",
    "late_ramp_median_c",
    "afternoon_dew_median_c",
    "morning_low_ceiling_share",
    "midday_low_ceiling_share",
    "reduced_visibility_share",
    "precip_day_share",
    "late_surge_share",
    "cloud_break_share",
    "dry_mixing_share",
    "humid_sticky_share",
]

PRIOR_FIELDS = [
    "station_id",
    "city",
    "timezone",
    "years_covered",
    "warm_peak_hour_median",
    "warm_peak_hour_p75",
    "late_peak_share",
    "very_late_peak_share",
    "daily_range_median_c",
    "daily_range_p90_c",
    "morning_warmup_median_c",
    "noon_warmup_median_c",
    "late_ramp_median_c",
    "hot_day_dewpoint_median_c",
    "morning_low_ceiling_share",
    "midday_low_ceiling_share",
    "afternoon_low_ceiling_share",
    "reduced_visibility_share",
    "light_wind_share",
    "precip_day_share",
    "cloud_break_day_share",
    "rain_reset_day_share",
    "humid_sticky_day_share",
    "dry_mixing_day_share",
    "clean_solar_ramp_day_share",
    "wind_shift_day_share",
    "dominant_wind_regimes",
    "special_features",
]

PRIMARY_ORDER = [
    "cloud_break_rebound",
    "cloud_suppressed",
    "rain_reset",
    "late_surge",
    "humid_sticky",
    "dry_mixing",
    "wind_shift_transition",
    "light_wind_stagnation",
    "clean_solar_ramp",
]

PRECIP_CODE_KEYS = ("MW1", "MW2", "MW3", "MW4", "AW1", "AW2", "AW3", "AW4")
PRECIP_METAR_TOKENS = ("RA", "DZ", "SN", "SG", "PL", "GR", "GS", "UP")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Build historical reference rows for selected stations")
    parser.add_argument(
        "--stations",
        nargs="+",
        required=True,
        help="ICAO station IDs, e.g. LLBG RJTT",
    )
    return parser


def main() -> None:
    args = build_parser().parse_args()
    stations = [str(item).upper() for item in args.stations]
    station_meta = _load_station_meta()

    daily_rows = _load_csv(DAILY_CSV)
    monthly_rows = _load_csv(MONTHLY_CSV)
    prior_rows = _load_csv(PRIOR_CSV)
    profile_rows = _load_csv(PROFILE_CSV)

    for station_id in stations:
        if station_id not in station_meta:
            raise ValueError(f"station not found in station_links.csv: {station_id}")
        if station_id not in STATION_TZ:
            raise ValueError(f"timezone not configured in station_catalog.py: {station_id}")

        station_daily_rows, dominant_wind = _build_station_daily_rows(station_id, station_meta[station_id])
        station_monthly_rows = _build_station_monthly_rows(station_daily_rows, station_meta[station_id]["city"], station_id)
        station_prior_row = _build_station_prior_row(
            station_id,
            station_meta[station_id]["city"],
            STATION_TZ[station_id],
            station_daily_rows,
            dominant_wind,
        )

        daily_rows = [row for row in daily_rows if str(row.get("station_id") or "").upper() != station_id]
        monthly_rows = [row for row in monthly_rows if str(row.get("station_id") or "").upper() != station_id]
        prior_rows = [row for row in prior_rows if str(row.get("station_id") or "").upper() != station_id]
        profile_rows = [row for row in profile_rows if str(row.get("station_id") or "").upper() != station_id]

        daily_rows.extend(station_daily_rows)
        monthly_rows.extend(station_monthly_rows)
        prior_rows.append(station_prior_row)
        profile_rows.append(dict(station_prior_row))

        print(
            f"{station_id}: daily_rows={len(station_daily_rows)} monthly_rows={len(station_monthly_rows)} "
            f"dominant={station_prior_row['dominant_wind_regimes']}"
        )

    _write_csv(
        DAILY_CSV,
        DAILY_FIELDS,
        sorted(daily_rows, key=lambda row: (str(row.get("station_id") or ""), str(row.get("local_date") or ""))),
    )
    _write_csv(
        MONTHLY_CSV,
        MONTHLY_FIELDS,
        sorted(
            monthly_rows,
            key=lambda row: (str(row.get("station_id") or ""), _safe_int(row.get("month")) or 0),
        ),
    )
    sorted_priors = sorted(prior_rows, key=lambda row: str(row.get("station_id") or ""))
    _write_csv(PRIOR_CSV, PRIOR_FIELDS, sorted_priors)
    _write_csv(PROFILE_CSV, PRIOR_FIELDS, sorted(profile_rows, key=lambda row: str(row.get("station_id") or "")))


def _load_station_meta() -> dict[str, dict[str, str]]:
    rows = _load_csv(STATION_CSV)
    return {str(row["icao"]).upper(): row for row in rows}


def _load_csv(path: Path) -> list[dict[str, str]]:
    with path.open(newline="", encoding="utf-8") as handle:
        return list(csv.DictReader(handle))


def _write_csv(path: Path, fieldnames: list[str], rows: list[dict[str, Any]]) -> None:
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow({field: row.get(field, "") for field in fieldnames})


def _build_station_daily_rows(
    station_id: str,
    station: dict[str, str],
) -> tuple[list[dict[str, str]], Counter[str]]:
    zone = ZoneInfo(STATION_TZ[station_id])
    grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
    wind_counter: Counter[str] = Counter()
    station_dir = RAW_DIR / station_id
    if not station_dir.exists():
        raise FileNotFoundError(station_dir)

    for path in sorted(station_dir.glob("*.csv.gz")):
        with gzip.open(path, "rt", encoding="utf-8", newline="") as handle:
            reader = csv.DictReader(handle)
            for raw in reader:
                raw_date = str(raw.get("DATE") or "").strip()
                if not raw_date:
                    continue
                try:
                    utc_dt = datetime.fromisoformat(raw_date)
                except ValueError:
                    continue
                if utc_dt.tzinfo is None:
                    utc_dt = utc_dt.replace(tzinfo=timezone.utc)
                else:
                    utc_dt = utc_dt.astimezone(timezone.utc)
                local_dt = utc_dt.astimezone(zone)
                wind_sector = wind_sector_from_diag(_parse_wind_direction(raw.get("WND")))
                obs = {
                    "local_dt": local_dt,
                    "local_hour": local_dt.hour,
                    "temp_c": _parse_signed_tenths(raw.get("TMP")),
                    "dew_c": _parse_signed_tenths(raw.get("DEW")),
                    "wind_dir_deg": _parse_wind_direction(raw.get("WND")),
                    "wind_sector": wind_sector,
                    "wind_speed_ms": _parse_wind_speed(raw.get("WND")),
                    "visibility_m": _parse_distance_code(raw.get("VIS")),
                    "ceiling_ft": _parse_distance_code(raw.get("CIG")),
                    "precip_flag": _raw_precip_present(raw),
                }
                grouped[str(local_dt.date())].append(obs)
                if wind_sector:
                    wind_counter[wind_sector] += 1

    rows: list[dict[str, Any]] = []
    for local_date in sorted(grouped):
        obs = sorted(grouped[local_date], key=lambda item: item["local_dt"])
        metrics = _daily_metrics(local_date, obs)
        metrics.update(
            {
                "station_id": station_id,
                "city": station["city"],
                "timezone": STATION_TZ[station_id],
            }
        )
        rows.append(metrics)

    if not rows:
        raise ValueError(f"no daily rows built for {station_id}")

    warm_threshold = _percentile([_safe_float(row.get("tmax_c")) for row in rows], 0.67)
    hot_threshold = _percentile([_safe_float(row.get("tmax_c")) for row in rows], 0.85)
    out: list[dict[str, str]] = []
    for row in rows:
        tmax = _safe_float(row.get("tmax_c"))
        warm_day = tmax is not None and warm_threshold is not None and tmax >= warm_threshold
        hot_day = tmax is not None and hot_threshold is not None and tmax >= hot_threshold
        flags = _daily_flags(row)
        primary, tags = _classify_regime(flags)
        out.append(
            {
                "station_id": station_id,
                "city": station["city"],
                "timezone": STATION_TZ[station_id],
                "year": str(row["year"]),
                "month": str(row["month"]),
                "local_date": str(row["local_date"]),
                "obs_count": _fmt_num(row.get("obs_count"), precision=0),
                "temp_obs_count": _fmt_num(row.get("temp_obs_count"), precision=0),
                "tmax_c": _fmt_num(row.get("tmax_c")),
                "tmin_c": _fmt_num(row.get("tmin_c")),
                "tmean_c": _fmt_num(row.get("tmean_c"), precision=3),
                "daily_range_c": _fmt_num(row.get("daily_range_c")),
                "peak_hour_local": _fmt_num(row.get("peak_hour_local"), precision=6),
                "peak_time_local": str(row.get("peak_time_local") or ""),
                "morning_temp_c": _fmt_num(row.get("morning_temp_c")),
                "late_morning_temp_c": _fmt_num(row.get("late_morning_temp_c")),
                "midday_temp_c": _fmt_num(row.get("midday_temp_c")),
                "late_day_temp_c": _fmt_num(row.get("late_day_temp_c")),
                "warmup_am_c": _fmt_num(row.get("warmup_am_c")),
                "warmup_noon_c": _fmt_num(row.get("warmup_noon_c")),
                "late_ramp_c": _fmt_num(row.get("late_ramp_c")),
                "morning_dew_c": _fmt_num(row.get("morning_dew_c")),
                "afternoon_dew_c": _fmt_num(row.get("afternoon_dew_c")),
                "dewpoint_dep_afternoon_c": _fmt_num(row.get("dewpoint_dep_afternoon_c")),
                "morning_low_ceiling_share": _fmt_num(row.get("morning_low_ceiling_share"), precision=12),
                "midday_low_ceiling_share": _fmt_num(row.get("midday_low_ceiling_share"), precision=12),
                "afternoon_low_ceiling_share": _fmt_num(row.get("afternoon_low_ceiling_share"), precision=12),
                "morning_visibility_restriction_share": _fmt_num(row.get("morning_visibility_restriction_share"), precision=12),
                "reduced_visibility_share": _fmt_num(row.get("reduced_visibility_share"), precision=12),
                "visibility_min_m": _fmt_num(row.get("visibility_min_m"), precision=0),
                "precip_obs_share": _fmt_num(row.get("precip_obs_share"), precision=12),
                "morning_precip_share": _fmt_num(row.get("morning_precip_share"), precision=12),
                "afternoon_precip_share": _fmt_num(row.get("afternoon_precip_share"), precision=12),
                "wind_speed_mean_ms": _fmt_num(row.get("wind_speed_mean_ms"), precision=12),
                "light_wind_share": _fmt_num(row.get("light_wind_share"), precision=12),
                "strong_wind_share": _fmt_num(row.get("strong_wind_share"), precision=12),
                "wind_shift_count": _fmt_num(row.get("wind_shift_count"), precision=0),
                "dominant_wind_sector": str(row.get("dominant_wind_sector") or ""),
                "warm_day_flag": _fmt_bool(warm_day),
                "hot_day_flag": _fmt_bool(hot_day),
                "late_peak_flag": _fmt_bool(flags["late_peak_flag"]),
                "very_late_peak_flag": _fmt_bool(flags["very_late_peak_flag"]),
                "morning_low_ceiling_flag": _fmt_bool(flags["morning_low_ceiling_flag"]),
                "midday_low_ceiling_flag": _fmt_bool(flags["midday_low_ceiling_flag"]),
                "afternoon_low_ceiling_flag": _fmt_bool(flags["afternoon_low_ceiling_flag"]),
                "reduced_visibility_flag": _fmt_bool(flags["reduced_visibility_flag"]),
                "light_wind_flag": _fmt_bool(flags["light_wind_flag"]),
                "precip_day_flag": _fmt_bool(flags["precip_day_flag"]),
                "late_surge_flag": _fmt_bool(flags["late_surge_flag"]),
                "cloud_suppressed_flag": _fmt_bool(flags["cloud_suppressed_flag"]),
                "cloud_break_rebound_flag": _fmt_bool(flags["cloud_break_rebound_flag"]),
                "humid_sticky_flag": _fmt_bool(flags["humid_sticky_flag"]),
                "dry_mixing_flag": _fmt_bool(flags["dry_mixing_flag"]),
                "rain_reset_flag": _fmt_bool(flags["rain_reset_flag"]),
                "light_wind_stagnation_flag": _fmt_bool(flags["light_wind_stagnation_flag"]),
                "wind_shift_transition_flag": _fmt_bool(flags["wind_shift_transition_flag"]),
                "low_visibility_day_flag": _fmt_bool(flags["low_visibility_day_flag"]),
                "clean_solar_ramp_flag": _fmt_bool(flags["clean_solar_ramp_flag"]),
                "primary_regime": primary,
                "regime_tags": "; ".join(tags).replace("; ", ";"),
                "regime_confidence": "1.0" if primary != "mixed_transitional" else "0.0",
            }
        )
    return out, wind_counter


def _daily_metrics(local_date: str, obs: list[dict[str, Any]]) -> dict[str, Any]:
    date_obj = datetime.strptime(local_date, "%Y-%m-%d")
    temps = [item["temp_c"] for item in obs if item.get("temp_c") is not None]
    dews = [item["dew_c"] for item in obs if item.get("dew_c") is not None]
    tmax = max(temps) if temps else None
    peak_times = []
    if tmax is not None:
        for item in obs:
            temp = item.get("temp_c")
            if temp is not None and abs(float(temp) - float(tmax)) <= 0.05:
                local_dt = item["local_dt"]
                peak_times.append(local_dt.hour + local_dt.minute / 60.0)
    peak_hour = statistics.mean(peak_times) if peak_times else None
    return {
        "year": date_obj.year,
        "month": date_obj.month,
        "local_date": local_date,
        "obs_count": len(obs),
        "temp_obs_count": len(temps),
        "tmax_c": tmax,
        "tmin_c": min(temps) if temps else None,
        "tmean_c": statistics.mean(temps) if temps else None,
        "daily_range_c": (max(temps) - min(temps)) if len(temps) >= 2 else None,
        "peak_hour_local": peak_hour,
        "peak_time_local": _fmt_hour(peak_hour),
        "morning_temp_c": _window_mean(obs, "temp_c", 6, 8),
        "late_morning_temp_c": _window_mean(obs, "temp_c", 9, 11),
        "midday_temp_c": _window_mean(obs, "temp_c", 12, 14),
        "late_day_temp_c": _window_mean(obs, "temp_c", 15, 17),
        "morning_dew_c": _window_mean(obs, "dew_c", 6, 8),
        "afternoon_dew_c": _window_mean(obs, "dew_c", 12, 17),
        "morning_low_ceiling_share": _window_share(obs, _is_low_ceiling, 6, 8),
        "midday_low_ceiling_share": _window_share(obs, _is_low_ceiling, 12, 14),
        "afternoon_low_ceiling_share": _window_share(obs, _is_low_ceiling, 15, 17),
        "morning_visibility_restriction_share": _window_share(obs, _is_reduced_visibility, 6, 11),
        "reduced_visibility_share": _share([_is_reduced_visibility(item) for item in obs]),
        "visibility_min_m": _min_value(item.get("visibility_m") for item in obs),
        "precip_obs_share": _share([bool(item.get("precip_flag")) for item in obs]),
        "morning_precip_share": _window_share(obs, lambda item: bool(item.get("precip_flag")), 6, 11),
        "afternoon_precip_share": _window_share(obs, lambda item: bool(item.get("precip_flag")), 12, 18),
        "wind_speed_mean_ms": _mean_value(item.get("wind_speed_ms") for item in obs),
        "light_wind_share": _share(
            [
                item.get("wind_speed_ms") is not None and float(item["wind_speed_ms"]) <= 2.5
                for item in obs
                if item.get("wind_speed_ms") is not None
            ]
        ),
        "strong_wind_share": _share(
            [
                item.get("wind_speed_ms") is not None and float(item["wind_speed_ms"]) >= 8.0
                for item in obs
                if item.get("wind_speed_ms") is not None
            ]
        ),
        "wind_shift_count": _wind_shift_count(obs),
        "dominant_wind_sector": _dominant_wind_sector(obs),
        "unique_wind_sectors": _unique_wind_sectors(obs),
    } | _derive_daily_deltas(obs, temps, dews)


def _derive_daily_deltas(obs: list[dict[str, Any]], temps: list[float], dews: list[float]) -> dict[str, Any]:
    morning_temp = _window_mean(obs, "temp_c", 6, 8)
    late_morning_temp = _window_mean(obs, "temp_c", 9, 11)
    midday_temp = _window_mean(obs, "temp_c", 12, 14)
    late_day_temp = _window_mean(obs, "temp_c", 15, 17)
    afternoon_dew = _window_mean(obs, "dew_c", 12, 17)
    return {
        "warmup_am_c": _delta(late_morning_temp, morning_temp),
        "warmup_noon_c": _delta(midday_temp, late_morning_temp),
        "late_ramp_c": _delta(late_day_temp, midday_temp),
        "dewpoint_dep_afternoon_c": _delta(max(temps) if temps else None, afternoon_dew),
    }


def _build_station_monthly_rows(
    daily_rows: list[dict[str, str]],
    city: str,
    station_id: str,
) -> list[dict[str, str]]:
    grouped: dict[int, list[dict[str, str]]] = defaultdict(list)
    for row in daily_rows:
        month = _safe_int(row.get("month"))
        if month is not None:
            grouped[month].append(row)

    out: list[dict[str, str]] = []
    for month in sorted(grouped):
        rows = grouped[month]
        out.append(
            {
                "station_id": station_id,
                "city": city,
                "month": str(month),
                "days": str(len(rows)),
                "tmax_median_c": _fmt_num(_median(rows, "tmax_c")),
                "peak_hour_median": _fmt_num(_median(rows, "peak_hour_local")),
                "daily_range_median_c": _fmt_num(_median(rows, "daily_range_c")),
                "warmup_am_median_c": _fmt_num(_median(rows, "warmup_am_c")),
                "warmup_noon_median_c": _fmt_num(_median(rows, "warmup_noon_c")),
                "late_ramp_median_c": _fmt_num(_median(rows, "late_ramp_c")),
                "afternoon_dew_median_c": _fmt_num(_median(rows, "afternoon_dew_c")),
                "morning_low_ceiling_share": _fmt_num(_mean(rows, "morning_low_ceiling_share"), precision=12),
                "midday_low_ceiling_share": _fmt_num(_mean(rows, "midday_low_ceiling_share"), precision=12),
                "reduced_visibility_share": _fmt_num(_share_true(rows, "reduced_visibility_flag"), precision=12),
                "precip_day_share": _fmt_num(_share_true(rows, "precip_day_flag"), precision=12),
                "late_surge_share": _fmt_num(_share_true(rows, "late_surge_flag"), precision=12),
                "cloud_break_share": _fmt_num(_share_true(rows, "cloud_break_rebound_flag"), precision=12),
                "dry_mixing_share": _fmt_num(_share_true(rows, "dry_mixing_flag"), precision=12),
                "humid_sticky_share": _fmt_num(_share_true(rows, "humid_sticky_flag"), precision=12),
            }
        )
    return out


def _build_station_prior_row(
    station_id: str,
    city: str,
    timezone_name: str,
    daily_rows: list[dict[str, str]],
    dominant_wind: Counter[str],
) -> dict[str, str]:
    warm_rows = [row for row in daily_rows if _is_true(row.get("warm_day_flag"))]
    hot_rows = [row for row in daily_rows if _is_true(row.get("hot_day_flag"))]
    warm_base = warm_rows or daily_rows
    hot_base = hot_rows or warm_base
    prior = {
        "station_id": station_id,
        "city": city,
        "timezone": timezone_name,
        "years_covered": "2022-2025",
        "warm_peak_hour_median": _fmt_num(_median(warm_base, "peak_hour_local")),
        "warm_peak_hour_p75": _fmt_num(_percentile([_safe_float(row.get("peak_hour_local")) for row in warm_base], 0.75)),
        "late_peak_share": _fmt_num(_share_true(warm_base, "late_peak_flag"), precision=12),
        "very_late_peak_share": _fmt_num(_share_true(warm_base, "very_late_peak_flag"), precision=12),
        "daily_range_median_c": _fmt_num(_median(daily_rows, "daily_range_c")),
        "daily_range_p90_c": _fmt_num(_percentile([_safe_float(row.get("daily_range_c")) for row in daily_rows], 0.90)),
        "morning_warmup_median_c": _fmt_num(_median(daily_rows, "warmup_am_c")),
        "noon_warmup_median_c": _fmt_num(_median(daily_rows, "warmup_noon_c")),
        "late_ramp_median_c": _fmt_num(_median(daily_rows, "late_ramp_c")),
        "hot_day_dewpoint_median_c": _fmt_num(_median(hot_base, "afternoon_dew_c")),
        "morning_low_ceiling_share": _fmt_num(_mean(daily_rows, "morning_low_ceiling_share"), precision=12),
        "midday_low_ceiling_share": _fmt_num(_mean(daily_rows, "midday_low_ceiling_share"), precision=12),
        "afternoon_low_ceiling_share": _fmt_num(_mean(daily_rows, "afternoon_low_ceiling_share"), precision=12),
        "reduced_visibility_share": _fmt_num(_share_true(daily_rows, "reduced_visibility_flag"), precision=12),
        "light_wind_share": _fmt_num(_mean(daily_rows, "light_wind_share"), precision=12),
        "precip_day_share": _fmt_num(_share_true(daily_rows, "precip_day_flag"), precision=12),
        "cloud_break_day_share": _fmt_num(_share_true(daily_rows, "cloud_break_rebound_flag"), precision=12),
        "rain_reset_day_share": _fmt_num(_share_true(daily_rows, "rain_reset_flag"), precision=12),
        "humid_sticky_day_share": _fmt_num(_share_true(daily_rows, "humid_sticky_flag"), precision=12),
        "dry_mixing_day_share": _fmt_num(_share_true(daily_rows, "dry_mixing_flag"), precision=12),
        "clean_solar_ramp_day_share": _fmt_num(_share_true(daily_rows, "clean_solar_ramp_flag"), precision=12),
        "wind_shift_day_share": _fmt_num(_share_true(daily_rows, "wind_shift_transition_flag"), precision=12),
        "dominant_wind_regimes": _format_dominant_winds(dominant_wind),
    }
    prior["special_features"] = _special_features(prior)
    return prior


def _daily_flags(row: dict[str, Any]) -> dict[str, bool]:
    peak_hour = _safe_float(row.get("peak_hour_local"))
    daily_range = _safe_float(row.get("daily_range_c"))
    warmup_am = _safe_float(row.get("warmup_am_c"))
    warmup_noon = _safe_float(row.get("warmup_noon_c"))
    late_ramp = _safe_float(row.get("late_ramp_c"))
    morning_cloud = _safe_float(row.get("morning_low_ceiling_share")) or 0.0
    midday_cloud = _safe_float(row.get("midday_low_ceiling_share")) or 0.0
    afternoon_cloud = _safe_float(row.get("afternoon_low_ceiling_share")) or 0.0
    reduced_vis = _safe_float(row.get("reduced_visibility_share")) or 0.0
    precip_share = _safe_float(row.get("precip_obs_share")) or 0.0
    morning_precip = _safe_float(row.get("morning_precip_share")) or 0.0
    afternoon_precip = _safe_float(row.get("afternoon_precip_share")) or 0.0
    wind_speed = _safe_float(row.get("wind_speed_mean_ms"))
    light_share = _safe_float(row.get("light_wind_share")) or 0.0
    wind_shift_count = _safe_int(row.get("wind_shift_count")) or 0
    unique_sectors = _safe_int(row.get("unique_wind_sectors")) or 0
    tmax = _safe_float(row.get("tmax_c"))
    afternoon_dew = _safe_float(row.get("afternoon_dew_c"))
    dew_dep = _safe_float(row.get("dewpoint_dep_afternoon_c"))
    visibility_min = _safe_float(row.get("visibility_min_m"))

    midday_flag = midday_cloud >= 0.80
    reduced_vis_flag = reduced_vis >= 0.12 or (visibility_min is not None and visibility_min <= 4000.0)
    precip_flag = precip_share >= 0.08 or morning_precip >= 0.12 or afternoon_precip >= 0.12
    late_peak_flag = peak_hour is not None and peak_hour >= 16.0
    very_late_peak_flag = peak_hour is not None and peak_hour >= 17.0
    late_surge_flag = (
        late_peak_flag
        and late_ramp is not None
        and late_ramp >= 1.0
        and midday_cloud <= 0.35
        and precip_share <= 0.05
    )
    cloud_break_flag = (
        morning_cloud >= 0.50
        and midday_cloud <= 0.50
        and warmup_noon is not None
        and warmup_noon >= 3.5
        and precip_share <= 0.05
    )
    cloud_suppressed_flag = midday_flag and (
        (daily_range is not None and daily_range <= 7.0)
        or (warmup_noon is not None and warmup_noon <= 1.8)
        or reduced_vis >= 0.20
        or light_share >= 0.75
    )
    humid_flag = (
        tmax is not None
        and tmax >= 24.0
        and afternoon_dew is not None
        and afternoon_dew >= 18.0
        and dew_dep is not None
        and dew_dep <= 8.0
    )
    dry_flag = (
        morning_cloud <= 0.15
        and midday_cloud <= 0.15
        and afternoon_cloud <= 0.15
        and precip_share <= 0.02
        and reduced_vis <= 0.05
        and daily_range is not None
        and daily_range >= 12.0
        and dew_dep is not None
        and dew_dep >= 14.0
    )
    rain_reset_flag = (
        (
            precip_share >= 0.08
            or morning_precip >= 0.12
            or afternoon_precip >= 0.12
            or (precip_share >= 0.02 and reduced_vis >= 0.20)
            or (cloud_break_flag and precip_share >= 0.02)
        )
        and (
            not cloud_suppressed_flag
            or precip_share >= 0.08
            or morning_precip >= 0.12
            or afternoon_precip >= 0.12
        )
    )
    stagnation_flag = (
        wind_speed is not None
        and wind_speed <= 2.2
        and light_share >= 0.75
        and (reduced_vis >= 0.12 or morning_cloud >= 0.50)
    )
    wind_shift_flag = wind_shift_count >= 12 and unique_sectors >= 3
    low_visibility_day_flag = reduced_vis >= 0.25 or (visibility_min is not None and visibility_min <= 2500.0)
    clean_solar_flag = (
        morning_cloud <= 0.10
        and midday_cloud <= 0.10
        and afternoon_cloud <= 0.10
        and precip_share <= 0.02
        and reduced_vis <= 0.05
        and warmup_am is not None
        and warmup_am >= 3.5
        and warmup_noon is not None
        and warmup_noon >= 3.0
        and daily_range is not None
        and daily_range >= 10.0
        and dew_dep is not None
        and dew_dep >= 12.0
    )
    return {
        "late_peak_flag": late_peak_flag,
        "very_late_peak_flag": very_late_peak_flag,
        "morning_low_ceiling_flag": morning_cloud >= 0.80,
        "midday_low_ceiling_flag": midday_flag,
        "afternoon_low_ceiling_flag": afternoon_cloud >= 0.80,
        "reduced_visibility_flag": reduced_vis_flag,
        "light_wind_flag": light_share >= 0.65,
        "precip_day_flag": precip_flag,
        "late_surge_flag": late_surge_flag,
        "cloud_suppressed_flag": cloud_suppressed_flag,
        "cloud_break_rebound_flag": cloud_break_flag,
        "humid_sticky_flag": humid_flag,
        "dry_mixing_flag": dry_flag,
        "rain_reset_flag": rain_reset_flag,
        "light_wind_stagnation_flag": stagnation_flag,
        "wind_shift_transition_flag": wind_shift_flag,
        "low_visibility_day_flag": low_visibility_day_flag,
        "clean_solar_ramp_flag": clean_solar_flag,
    }


def _classify_regime(flags: dict[str, bool]) -> tuple[str, list[str]]:
    tag_keys = [
        key
        for key in [
            "cloud_break_rebound_flag",
            "cloud_suppressed_flag",
            "rain_reset_flag",
            "late_surge_flag",
            "humid_sticky_flag",
            "dry_mixing_flag",
            "wind_shift_transition_flag",
            "light_wind_stagnation_flag",
            "low_visibility_day_flag",
            "clean_solar_ramp_flag",
        ]
        if flags.get(key)
    ]
    tags = [key.replace("_flag", "") for key in tag_keys]
    primary = "mixed_transitional"
    for candidate in PRIMARY_ORDER:
        if candidate in tags:
            primary = candidate
            break
    if not tags:
        tags = ["mixed_transitional"]
    return primary, tags


def _special_features(prior: dict[str, str]) -> str:
    features: list[str] = []
    late_peak_share = _safe_float(prior.get("late_peak_share"))
    very_late_peak_share = _safe_float(prior.get("very_late_peak_share"))
    daily_range = _safe_float(prior.get("daily_range_median_c"))
    hot_day_dew = _safe_float(prior.get("hot_day_dewpoint_median_c"))
    midday_cloud = _safe_float(prior.get("midday_low_ceiling_share"))
    visibility = _safe_float(prior.get("reduced_visibility_share"))
    rain_reset = _safe_float(prior.get("rain_reset_day_share"))
    cloud_break = _safe_float(prior.get("cloud_break_day_share"))
    dry_mixing = _safe_float(prior.get("dry_mixing_day_share"))
    humid = _safe_float(prior.get("humid_sticky_day_share"))
    wind_shift = _safe_float(prior.get("wind_shift_day_share"))

    if (late_peak_share or 0.0) >= 0.55 or (very_late_peak_share or 0.0) >= 0.30:
        features.append("late-day surge risk")
    if daily_range is not None and daily_range >= 11.5:
        features.append("large diurnal swing")
    elif daily_range is not None and daily_range <= 6.8:
        features.append("muted diurnal range")
    if ((humid or 0.0) >= 0.12) or (hot_day_dew is not None and hot_day_dew >= 18.0):
        features.append("humid-heat persistence")
    if (midday_cloud or 0.0) >= 0.18:
        features.append("midday cloud suppression risk")
    if (visibility or 0.0) >= 0.15:
        features.append("frequent visibility restrictions")
    if (rain_reset or 0.0) >= 0.12:
        features.append("frequent precip resets")
    if (cloud_break or 0.0) >= 0.10:
        features.append("frequent cloud-break rebounds")
    if (dry_mixing or 0.0) >= 0.18 and (daily_range is None or daily_range < 11.5):
        features.append("dry-mixing upside")
    if (wind_shift or 0.0) >= 0.20:
        features.append("wind-shift sensitive")
    if not features:
        features.append("balanced baseline station")
    return "; ".join(features)


def _format_dominant_winds(counter: Counter[str]) -> str:
    total = sum(counter.values())
    if total <= 0:
        return "n/a"
    items = []
    for sector, count in counter.most_common(3):
        pct = int(round(count / total * 100.0))
        items.append(f"{sector}:{pct}%")
    return ", ".join(items)


def _window_mean(obs: list[dict[str, Any]], key: str, start_hour: int, end_hour: int) -> float | None:
    values = [
        _safe_float(item.get(key))
        for item in obs
        if start_hour <= int(item["local_hour"]) <= end_hour and item.get(key) is not None
    ]
    values = [value for value in values if value is not None]
    if not values:
        return None
    return statistics.mean(values)


def _window_share(
    obs: list[dict[str, Any]],
    predicate,
    start_hour: int,
    end_hour: int,
) -> float | None:
    window = [item for item in obs if start_hour <= int(item["local_hour"]) <= end_hour]
    if not window:
        return None
    return _share([bool(predicate(item)) for item in window])


def _is_low_ceiling(item: dict[str, Any]) -> bool:
    ceiling_ft = _safe_float(item.get("ceiling_ft"))
    return ceiling_ft is not None and ceiling_ft <= 6500.0


def _is_reduced_visibility(item: dict[str, Any]) -> bool:
    visibility_m = _safe_float(item.get("visibility_m"))
    return visibility_m is not None and visibility_m <= 5000.0


def _raw_precip_present(raw: dict[str, str]) -> bool:
    if _raw_precip_flag(raw):
        return True
    for key in PRECIP_CODE_KEYS:
        value = str(raw.get(key) or "").strip()
        if not value:
            continue
        code = value.split(",")[0].strip()
        try:
            if int(code) >= 50:
                return True
        except ValueError:
            continue
    rem = f" {str(raw.get('REM') or '').upper()} "
    if " VCSH " in rem or " VCTS " in rem:
        rem = rem.replace(" VCSH ", " ").replace(" VCTS ", " ")
    return any(f" {token} " in rem or f"-{token}" in rem or f"+{token}" in rem for token in PRECIP_METAR_TOKENS)


def _wind_shift_count(obs: list[dict[str, Any]]) -> int:
    sectors = [str(item.get("wind_sector") or "") for item in obs if item.get("wind_sector")]
    prev = None
    count = 0
    for sector in sectors:
        if prev is not None and sector != prev:
            count += 1
        prev = sector
    return count


def _dominant_wind_sector(obs: list[dict[str, Any]]) -> str:
    counter = Counter(str(item.get("wind_sector") or "") for item in obs if item.get("wind_sector"))
    return counter.most_common(1)[0][0] if counter else ""


def _unique_wind_sectors(obs: list[dict[str, Any]]) -> int:
    return len({str(item.get("wind_sector") or "") for item in obs if item.get("wind_sector")})


def _mean(rows: list[dict[str, str]], key: str) -> float | None:
    values = [_safe_float(row.get(key)) for row in rows]
    clean = [value for value in values if value is not None]
    if not clean:
        return None
    return statistics.mean(clean)


def _median(rows: list[dict[str, str]], key: str) -> float | None:
    values = [_safe_float(row.get(key)) for row in rows]
    clean = [value for value in values if value is not None]
    if not clean:
        return None
    return statistics.median(clean)


def _percentile(values: list[float | None], pct: float) -> float | None:
    clean = sorted(value for value in values if value is not None)
    if not clean:
        return None
    if len(clean) == 1:
        return clean[0]
    index = max(0.0, min(1.0, pct)) * (len(clean) - 1)
    lo = int(math.floor(index))
    hi = int(math.ceil(index))
    if lo == hi:
        return clean[lo]
    weight = index - lo
    return clean[lo] * (1.0 - weight) + clean[hi] * weight


def _share(values: list[bool]) -> float | None:
    if not values:
        return None
    return sum(1 for value in values if value) / len(values)


def _share_true(rows: list[dict[str, str]], key: str) -> float | None:
    if not rows:
        return None
    return sum(1 for row in rows if _is_true(row.get(key))) / len(rows)


def _mean_value(values) -> float | None:
    clean = [_safe_float(value) for value in values]
    clean = [value for value in clean if value is not None]
    if not clean:
        return None
    return statistics.mean(clean)


def _min_value(values) -> float | None:
    clean = [_safe_float(value) for value in values]
    clean = [value for value in clean if value is not None]
    if not clean:
        return None
    return min(clean)


def _delta(v1: float | None, v0: float | None) -> float | None:
    if v1 is None or v0 is None:
        return None
    return v1 - v0


def _fmt_bool(value: bool) -> str:
    return "True" if value else "False"


def _fmt_hour(hour_value: float | None) -> str:
    if hour_value is None:
        return ""
    total_minutes = int(round(float(hour_value) * 60.0))
    total_minutes = max(0, min(total_minutes, 23 * 60 + 59))
    hour, minute = divmod(total_minutes, 60)
    return f"{hour:02d}:{minute:02d}"


def _fmt_num(value: Any, precision: int = 1) -> str:
    numeric = _safe_float(value)
    if numeric is None:
        return ""
    if precision == 0:
        return str(int(round(numeric)))
    return f"{numeric:.{precision}f}".rstrip("0").rstrip(".") if precision <= 6 else f"{numeric:.{precision}f}".rstrip("0").rstrip(".")


def _safe_float(value: Any) -> float | None:
    try:
        if value in (None, "", "n/a"):
            return None
        return float(value)
    except (TypeError, ValueError):
        return None


def _safe_int(value: Any) -> int | None:
    numeric = _safe_float(value)
    if numeric is None:
        return None
    return int(round(numeric))


def _is_true(value: Any) -> bool:
    return str(value or "").strip().lower() in {"1", "true", "yes"}


if __name__ == "__main__":
    main()
