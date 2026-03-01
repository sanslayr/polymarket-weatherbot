#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import json
from pathlib import Path
from typing import Any

import requests

ROOT = Path(__file__).resolve().parent.parent
STATION_CSV = ROOT / "station_links.csv"
OUT_JSON = ROOT / "config" / "station_terrain_tags.json"


def classify_terrain(base_m: float, relief_m: float) -> str:
    # Keep tags short (1-2 words) and robust across stations.
    if base_m <= 60:
        if relief_m >= 180:
            return "低地起伏"
        if relief_m >= 80:
            return "低地缓丘"
        return "低地平原"

    if base_m >= 900:
        return "高原起伏" if relief_m >= 180 else "高原台地"
    if base_m >= 600:
        return "高地起伏" if relief_m >= 200 else "高原台地"
    if base_m >= 250:
        return "山地起伏" if relief_m >= 220 else "内陆高地"

    # 60~250m low to mid elevation
    if relief_m >= 220:
        return "丘陵起伏"
    if relief_m >= 100:
        return "平原缓丘"
    return "低地平原"


def fetch_elevation_points(points: list[tuple[float, float]]) -> list[float]:
    url = "https://api.open-meteo.com/v1/elevation"
    lat = ",".join(f"{p[0]:.6f}" for p in points)
    lon = ",".join(f"{p[1]:.6f}" for p in points)
    r = requests.get(url, params={"latitude": lat, "longitude": lon}, timeout=20)
    r.raise_for_status()
    obj = r.json()
    vals = obj.get("elevation")
    if not isinstance(vals, list) or len(vals) != len(points):
        raise RuntimeError("invalid elevation response")
    return [float(x) for x in vals]


def sample_points(lat: float, lon: float, d: float = 0.15) -> list[tuple[float, float]]:
    # center + cross around station (~15km)
    return [
        (lat, lon),
        (lat + d, lon),
        (lat - d, lon),
        (lat, lon + d),
        (lat, lon - d),
    ]


def build_tags() -> dict[str, Any]:
    out: dict[str, Any] = {}
    with STATION_CSV.open("r", encoding="utf-8") as f:
        rd = csv.DictReader(f)
        for row in rd:
            icao = str(row.get("icao") or "").upper().strip()
            if not icao:
                continue
            try:
                lat = float(row.get("lat") or 0.0)
                lon = float(row.get("lon") or 0.0)
                pts = sample_points(lat, lon)
                elev = fetch_elevation_points(pts)
                base = elev[0]
                relief = max(elev) - min(elev)
                tag = classify_terrain(base, relief)
                out[icao] = {
                    "tag": tag,
                    "elevation_m": round(base, 1),
                    "relief_15km_m": round(relief, 1),
                }
            except Exception as exc:
                out[icao] = {
                    "tag": "地形未知",
                    "error": str(exc),
                }
    return out


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--output", default=str(OUT_JSON))
    args = ap.parse_args()

    tags = build_tags()
    out = Path(args.output)
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(tags, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"wrote {out} ({len(tags)} stations)")


if __name__ == "__main__":
    main()
