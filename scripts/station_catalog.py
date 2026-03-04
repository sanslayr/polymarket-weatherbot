from __future__ import annotations

import csv
import os
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
DEFAULT_STATION_CSV = ROOT / "station_links.csv"

CITY_ALIASES = {
    "nyc": "new york",
    "newyork": "new york",
    "la": "los angeles",
    "sao": "sao paulo",
    "saopaulo": "sao paulo",
    "buenos": "buenos aires",
    "seoul": "seoul",
    "ank": "ankara",
}

STATION_TZ = {
    "LTAC": "Europe/Istanbul",
    "EGLC": "Europe/London",
    "LFPG": "Europe/Paris",
    "NZWN": "Pacific/Auckland",
    "CYYZ": "America/Toronto",
    "KATL": "America/New_York",
    "KJFK": "America/New_York",
    "KLGA": "America/New_York",
    "KDFW": "America/Chicago",
    "KDAL": "America/Chicago",
    "KORD": "America/Chicago",
    "KSEA": "America/Los_Angeles",
    "KMIA": "America/New_York",
    "SBSP": "America/Sao_Paulo",
    "SBGR": "America/Sao_Paulo",
    "RKSI": "Asia/Seoul",
    "SAEZ": "America/Argentina/Buenos_Aires",
    "VILK": "Asia/Kolkata",
    "EDDM": "Europe/Berlin",
}

_STATION_META_MAP: dict[str, dict[str, dict[str, str]]] = {}


@dataclass
class Station:
    city: str
    icao: str
    lat: float
    lon: float


def norm_text(s: str) -> str:
    return "".join(ch for ch in s.strip().lower() if ch.isalnum())


def resolve_station(station_hint: str, station_csv: Path = DEFAULT_STATION_CSV) -> Station:
    raw = station_hint.strip().lower()
    key = CITY_ALIASES.get(raw, raw)
    key_norm = norm_text(key)
    with station_csv.open(newline="", encoding="utf-8") as f:
        rows = list(csv.DictReader(f))

    def as_station(row: dict[str, str]) -> Station:
        return Station(
            city=row["city"].strip(),
            icao=row["icao"].strip().upper(),
            lat=float(row["lat"]),
            lon=float(row["lon"]),
        )

    exact: list[dict[str, str]] = []
    prefix: list[dict[str, str]] = []
    contains: list[dict[str, str]] = []
    for row in rows:
        icao = row["icao"].strip().lower()
        city = row["city"].strip().lower()
        icao_n = norm_text(icao)
        city_n = norm_text(city)

        if icao == key or city == key or icao_n == key_norm or city_n == key_norm:
            exact.append(row)
            continue
        if icao.startswith(key) or city.startswith(key) or icao_n.startswith(key_norm) or city_n.startswith(key_norm):
            prefix.append(row)
            continue
        if key in city or key_norm in city_n:
            contains.append(row)

    if len(exact) == 1:
        return as_station(exact[0])
    if len(exact) > 1:
        labels = ", ".join(sorted({f"{r['city']}({r['icao']})" for r in exact}))
        raise ValueError(f"Ambiguous station hint '{station_hint}'. Candidates: {labels}")
    if len(prefix) == 1:
        return as_station(prefix[0])
    if len(contains) == 1:
        return as_station(contains[0])

    if prefix or contains:
        cands = prefix if prefix else contains
        labels = ", ".join(sorted({f"{r['city']}({r['icao']})" for r in cands}))
        raise ValueError(f"Ambiguous station hint '{station_hint}'. Candidates: {labels}")
    raise ValueError(f"Unknown station/city: {station_hint}")


def default_model_for_station(st: Station) -> str:
    m = (os.getenv("LOOK_DEFAULT_MODEL", "gfs") or "gfs").strip().lower()
    return m if m in {"gfs", "ecmwf"} else "gfs"


def station_timezone_name(st: Station) -> str:
    return STATION_TZ.get(str(st.icao).upper(), "UTC")


def format_utc_offset(dt: datetime) -> str:
    z = dt.strftime("%z")
    if not z:
        return "UTC+00"
    return f"UTC{z[:3]}"


def station_meta_for(icao: str, station_csv: Path = DEFAULT_STATION_CSV) -> dict[str, str]:
    csv_key = str(station_csv.resolve())
    try:
        mp = _STATION_META_MAP.get(csv_key)
        if mp is None:
            tmp: dict[str, dict[str, str]] = {}
            with station_csv.open(newline="", encoding="utf-8") as f:
                for row in csv.DictReader(f):
                    k = str(row.get("icao") or "").upper().strip()
                    if not k:
                        continue
                    t1 = str(row.get("terrain_tag") or "").strip()
                    t2 = str(row.get("terrain_tag2") or "").strip()
                    topo_tokens = ["低地", "高地", "丘陵", "平原", "山地", "高原", "台地"]
                    redundant = False
                    if t1 and t2:
                        redundant = any((tk in t1 and tk in t2) for tk in topo_tokens)
                    terr = t1 if (t1 and (not t2 or redundant)) else (f"{t1}·{t2}" if (t1 and t2) else (t1 or ""))
                    tmp[k] = {
                        "terrain": terr,
                        "site_tag": str(row.get("site_tag") or "").strip(),
                        "factor_summary": str(row.get("factor_summary") or "").strip(),
                        "terrain_sector": str(row.get("terrain_sector") or "").strip(),
                        "water_factor": str(row.get("water_factor") or "").strip(),
                        "water_sector": str(row.get("water_sector") or "").strip(),
                        "city_sector": str(row.get("city_sector") or "").strip(),
                        "city_distance_km": str(row.get("city_distance_km") or "").strip(),
                        "urban_position": str(row.get("urban_position") or "").strip(),
                    }
            _STATION_META_MAP[csv_key] = tmp
            mp = tmp
        return (mp or {}).get(str(icao).upper(), {})
    except Exception:
        return {}


def terrain_tag_for(icao: str, station_csv: Path = DEFAULT_STATION_CSV) -> str | None:
    t = station_meta_for(icao, station_csv).get("terrain")
    return t if t else None


def site_tag_for(icao: str, station_csv: Path = DEFAULT_STATION_CSV) -> str | None:
    t = station_meta_for(icao, station_csv).get("site_tag")
    return t if t else None


def factor_summary_for(icao: str, station_csv: Path = DEFAULT_STATION_CSV) -> str | None:
    t = station_meta_for(icao, station_csv).get("factor_summary")
    return t if t else None


def direction_factor_for(icao: str, station_csv: Path = DEFAULT_STATION_CSV) -> str | None:
    m = station_meta_for(icao, station_csv)
    water_sec = str(m.get("water_sector") or "").strip()
    urban_pos = str(m.get("urban_position") or "").strip()
    bits = []
    if water_sec and water_sec not in {"内陆主导", "未知"}:
        bits.append(water_sec)
    if urban_pos and urban_pos != "未知":
        bits.append(urban_pos)
    if not bits:
        return None
    return " | ".join(bits)
