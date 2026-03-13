#!/usr/bin/env python3
"""Build station-level weather links for Tmax analysis.

This script focuses on two deterministic pieces:
1) Polymarket date slug format with year, e.g. february-26-2026.
2) TropicalTidbits sounding URL selection using latest available model runtime
   aligned to the Tmax window time.
"""

from __future__ import annotations

import argparse
import csv
import json
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

MONTH_NAMES = [
    "january",
    "february",
    "march",
    "april",
    "may",
    "june",
    "july",
    "august",
    "september",
    "october",
    "november",
    "december",
]


@dataclass(frozen=True)
class ModelConfig:
    cycle_hours: int
    availability_lag_hours: int
    fh_step_hours: int
    max_fh_hours: int


@dataclass(frozen=True)
class WeatherMapLink:
    label: str
    url: str


MODEL_CONFIGS: dict[str, ModelConfig] = {
    "gfs": ModelConfig(cycle_hours=6, availability_lag_hours=5, fh_step_hours=3, max_fh_hours=384),
    # 实盘口径：ECMWF按6小时周期看可用时次，通常有3-5小时发布延迟。
    "ecmwf": ModelConfig(cycle_hours=6, availability_lag_hours=5, fh_step_hours=3, max_fh_hours=240),
}


def parse_utc(ts: str) -> datetime:
    if ts.endswith("Z"):
        ts = ts[:-1] + "+00:00"
    dt = datetime.fromisoformat(ts)
    if dt.tzinfo is None:
        raise ValueError(f"Timestamp must include timezone: {ts}")
    return dt.astimezone(timezone.utc)


def parse_date(d: str) -> datetime:
    dt = datetime.strptime(d, "%Y-%m-%d")
    return dt.replace(tzinfo=timezone.utc)


def floor_to_cycle(dt_utc: datetime, cycle_hours: int) -> datetime:
    floored_hour = (dt_utc.hour // cycle_hours) * cycle_hours
    return dt_utc.replace(hour=floored_hour, minute=0, second=0, microsecond=0)


def latest_available_runtime(now_utc: datetime, cfg: ModelConfig) -> datetime:
    ready_time = now_utc - timedelta(hours=cfg.availability_lag_hours)
    return floor_to_cycle(ready_time, cfg.cycle_hours)


def normalize_fh(raw_fh_hours: float, cfg: ModelConfig) -> int:
    if raw_fh_hours < 0:
        return 0
    stepped = round(raw_fh_hours / cfg.fh_step_hours) * cfg.fh_step_hours
    return int(max(0, min(stepped, cfg.max_fh_hours)))


def pick_runtime_and_fh(
    model: str,
    now_utc: datetime,
    target_valid_utc: datetime,
) -> tuple[datetime, int]:
    cfg = MODEL_CONFIGS[model]
    latest_runtime = latest_available_runtime(now_utc, cfg)

    # Runtime must be available now and not later than target valid time.
    runtime = min(latest_runtime, floor_to_cycle(target_valid_utc, cfg.cycle_hours))
    while runtime > target_valid_utc:
        runtime -= timedelta(hours=cfg.cycle_hours)

    raw_fh = (target_valid_utc - runtime).total_seconds() / 3600.0
    fh = normalize_fh(raw_fh, cfg)
    return runtime, fh


def format_runtime(runtime_utc: datetime) -> str:
    return runtime_utc.strftime("%Y%m%d%H")


def format_polymarket_date_slug(target_date_utc: datetime) -> str:
    month = MONTH_NAMES[target_date_utc.month - 1]
    return f"{month}-{target_date_utc.day}-{target_date_utc.year}"


def load_station(csv_path: Path, city_or_icao: str) -> dict[str, str]:
    key = city_or_icao.strip().lower()
    with csv_path.open(newline="", encoding="utf-8") as f:
        rows = list(csv.DictReader(f))

    for row in rows:
        if row["icao"].strip().lower() == key or row["city"].strip().lower() == key:
            return row

    raise ValueError(f"Station not found for: {city_or_icao}")


def maybe_nws_link(row: dict[str, str]) -> str | None:
    wfo = row.get("nws_wfo", "").strip().upper()
    template = row.get("nws_discuss_url_format", "").strip()
    if not wfo or not template:
        return None
    return template.format(wfo=wfo)


def weather_map_link(row: dict[str, str]) -> WeatherMapLink:
    icao = row["icao"].strip().upper()

    if icao in {"KMIA", "KATL"}:
        return WeatherMapLink(
            label="WPC East South",
            url="https://www.wpc.ncep.noaa.gov/sfc/namsesfcwbg.gif",
        )
    if icao in {"KLGA", "CYYZ"}:
        return WeatherMapLink(
            label="WPC East North",
            url="https://www.wpc.ncep.noaa.gov/sfc/namnesfcwbg.gif",
        )
    if icao == "KORD":
        return WeatherMapLink(
            label="WPC Central North",
            url="https://www.wpc.ncep.noaa.gov/sfc/namncsfcwbg.gif",
        )
    if icao == "KDAL":
        return WeatherMapLink(
            label="WPC Central South",
            url="https://www.wpc.ncep.noaa.gov/sfc/namscsfcwbg.gif",
        )
    if icao == "KSEA":
        return WeatherMapLink(
            label="WPC West North",
            url="https://www.wpc.ncep.noaa.gov/sfc/namnwsfcwbg.gif",
        )
    if icao.startswith("K"):
        return WeatherMapLink(
            label="WPC North America",
            url="https://www.wpc.ncep.noaa.gov/html/sfctxt.html",
        )
    if icao in {"EGLC", "LFPG", "EDDM"}:
        return WeatherMapLink(
            label="Met Office",
            url="https://weather.metoffice.gov.uk/maps-and-charts/surface-pressure",
        )
    if icao == "LTAC":
        return WeatherMapLink(
            label="MGM",
            url="https://www.mgm.gov.tr/eng/actualmaps.aspx",
        )
    if icao == "LLBG":
        return WeatherMapLink(
            label="IMS",
            url="https://ims.gov.il/en/analyzedSynopticMaps",
        )
    if icao.startswith("Z"):
        return WeatherMapLink(
            label="NMC",
            url="http://nmc.cn/publish/observations/china/dm/weatherchart-h000.htm",
        )
    if icao in {"RJTT", "RKSI", "WSSS"}:
        return WeatherMapLink(
            label="JMA",
            url="https://www.jma.go.jp/bosai/weather_map/",
        )
    if icao == "VHHH":
        return WeatherMapLink(
            label="HKO",
            url="https://www.weather.gov.hk/en/wxinfo/currwx/wxcht.htm",
        )
    if icao == "NZWN":
        return WeatherMapLink(
            label="MetService",
            url="https://www.metservice.com/maps-radar/weather-maps/isobars",
        )
    if icao in {"SAEZ", "SBGR"}:
        return WeatherMapLink(
            label="CHM",
            url="https://www.marinha.mil.br/chm/dados-do-smm-cartas-sinoticas/cartas-sinoticas",
        )
    if icao == "VILK":
        return WeatherMapLink(
            label="IMD",
            url="https://rsmcnewdelhi.imd.gov.in/surface_chart00.php",
        )
    return WeatherMapLink(
        label="NOAA Unified",
        url="https://ocean.weather.gov/unified_analysis.php",
    )


def build_links(
    row: dict[str, str],
    model: str,
    now_utc: datetime,
    target_valid_utc: datetime,
    target_date_utc: datetime,
    sounding_model: str = "ecmwf",
    sounding_target_valid_utc: datetime | None = None,
) -> dict[str, Any]:
    runtime_utc, fh = pick_runtime_and_fh(
        model=model,
        now_utc=now_utc,
        target_valid_utc=target_valid_utc,
    )

    # Sounding link policy: prefer ECMWF links by default.
    snd_model = sounding_model if sounding_model in MODEL_CONFIGS else "ecmwf"
    snd_target_valid_utc = sounding_target_valid_utc or target_valid_utc
    snd_runtime_utc, snd_fh = pick_runtime_and_fh(
        model=snd_model,
        now_utc=now_utc,
        target_valid_utc=snd_target_valid_utc,
    )

    runtime = format_runtime(runtime_utc)
    snd_runtime = format_runtime(snd_runtime_utc)
    station_id = row["icao"].strip().lower()
    date_slug = format_polymarket_date_slug(target_date_utc)

    tropical_url = row["tropicaltidbits_sounding_url_format"].format(
        model=snd_model,
        runtime=snd_runtime,
        fh=snd_fh,
        lat=row["lat"].strip(),
        lon=row["lon"].strip(),
        station_id=station_id,
    )

    polymarket_event_url = row["polymarket_event_url_format"].format(
        city_slug=row["polymarket_city_slug"].strip(),
        date_slug=date_slug,
    )
    weather_map = weather_map_link(row)

    return {
        "city": row["city"].strip(),
        "icao": row["icao"].strip(),
        "model": model,
        "runtime_utc": runtime,
        "fh_hours": fh,
        "sounding_model": snd_model,
        "sounding_runtime_utc": snd_runtime,
        "sounding_fh_hours": snd_fh,
        "target_valid_utc": target_valid_utc.isoformat().replace("+00:00", "Z"),
        "date_slug": date_slug,
        "links": {
            "weather_map": weather_map.url,
            "weather_map_label": weather_map.label,
            "sounding_tropicaltidbits": tropical_url,
            "metar_latest": row["metar_api_latest"].strip(),
            "metar_24h": row["metar_api_24h"].strip(),
            "wunderground": row["wunderground_url"].strip(),
            "nws_discuss": maybe_nws_link(row),
            "polymarket_search": row["polymarket_search_url"].strip(),
            "polymarket_event": polymarket_event_url,
        },
    }


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="Build links for city/airport Tmax report")
    p.add_argument("--csv", default="station_links.csv", help="Path to station csv")
    p.add_argument("--station", required=True, help="City name or ICAO code")
    p.add_argument("--model", default="gfs", choices=sorted(MODEL_CONFIGS.keys()))
    p.add_argument("--sounding-model", default="ecmwf", choices=sorted(MODEL_CONFIGS.keys()))
    p.add_argument(
        "--target-valid-utc",
        required=True,
        help="Tmax window valid time in UTC, ISO8601 (e.g. 2026-02-26T06:00:00Z)",
    )
    p.add_argument(
        "--target-date",
        required=True,
        help="Date used for polymarket slug, format YYYY-MM-DD",
    )
    p.add_argument(
        "--now-utc",
        default=None,
        help="Override now in UTC ISO8601 for deterministic runs",
    )
    return p


def main() -> None:
    args = build_parser().parse_args()
    csv_path = Path(args.csv)
    row = load_station(csv_path, args.station)

    now_utc = parse_utc(args.now_utc) if args.now_utc else datetime.now(timezone.utc)
    target_valid_utc = parse_utc(args.target_valid_utc)
    target_date_utc = parse_date(args.target_date)

    payload = build_links(
        row=row,
        model=args.model,
        now_utc=now_utc,
        target_valid_utc=target_valid_utc,
        target_date_utc=target_date_utc,
        sounding_model=args.sounding_model,
    )
    print(json.dumps(payload, ensure_ascii=True, indent=2))


if __name__ == "__main__":
    main()
