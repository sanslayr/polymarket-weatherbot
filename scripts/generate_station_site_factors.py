#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import json
import math
from pathlib import Path
from typing import Any

import requests

ROOT = Path(__file__).resolve().parent.parent
STATION_CSV = ROOT / "station_links.csv"

SECTORS = [
    (0, "北"),
    (45, "东北"),
    (90, "东"),
    (135, "东南"),
    (180, "南"),
    (225, "西南"),
    (270, "西"),
    (315, "西北"),
]


def _dest_point(lat: float, lon: float, bearing_deg: float, distance_km: float) -> tuple[float, float]:
    # great-circle destination point
    r = 6371.0
    br = math.radians(bearing_deg)
    p1 = math.radians(lat)
    l1 = math.radians(lon)
    dr = distance_km / r

    p2 = math.asin(math.sin(p1) * math.cos(dr) + math.cos(p1) * math.sin(dr) * math.cos(br))
    l2 = l1 + math.atan2(math.sin(br) * math.sin(dr) * math.cos(p1), math.cos(dr) - math.sin(p1) * math.sin(p2))
    return math.degrees(p2), ((math.degrees(l2) + 540) % 360) - 180


def _fetch_elev(points: list[tuple[float, float]]) -> list[float]:
    url = "https://api.open-meteo.com/v1/elevation"
    r = requests.get(
        url,
        params={
            "latitude": ",".join(f"{a:.6f}" for a, _ in points),
            "longitude": ",".join(f"{b:.6f}" for _, b in points),
        },
        timeout=20,
    )
    r.raise_for_status()
    obj = r.json()
    arr = obj.get("elevation")
    if not isinstance(arr, list) or len(arr) != len(points):
        raise RuntimeError("invalid elevation response")
    return [float(x) for x in arr]


def _city_sector(city: str, st_lat: float, st_lon: float) -> str:
    # open-meteo geocoding, choose nearest candidate to station
    try:
        url = "https://geocoding-api.open-meteo.com/v1/search"
        r = requests.get(url, params={"name": city, "count": 8, "language": "zh", "format": "json"}, timeout=15)
        r.raise_for_status()
        arr = (r.json() or {}).get("results") or []
        if not arr:
            return "未知"

        def dist2(x: dict[str, Any]) -> float:
            la = float(x.get("latitude") or 0.0)
            lo = float(x.get("longitude") or 0.0)
            return (la - st_lat) ** 2 + (lo - st_lon) ** 2

        best = sorted(arr, key=dist2)[0]
        la = float(best.get("latitude") or st_lat)
        lo = float(best.get("longitude") or st_lon)

        # bearing from station to city center
        y = math.sin(math.radians(lo - st_lon)) * math.cos(math.radians(la))
        x = math.cos(math.radians(st_lat)) * math.sin(math.radians(la)) - math.sin(math.radians(st_lat)) * math.cos(math.radians(la)) * math.cos(math.radians(lo - st_lon))
        br = (math.degrees(math.atan2(y, x)) + 360) % 360
        idx = int(((br + 22.5) % 360) // 45)
        return ["北", "东北", "东", "东南", "南", "西南", "西", "西北"][idx]
    except Exception:
        return "未知"


def _water_factor(tag2: str) -> str:
    t = str(tag2 or "")
    if any(k in t for k in ["滨海", "海湾"]):
        return "沿海影响"
    if any(k in t for k in ["河口"]):
        return "河口影响"
    if any(k in t for k in ["湖滨", "湾岸"]):
        return "近水体影响"
    return "内陆主导"


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--radius-km", type=float, default=25.0)
    args = ap.parse_args()

    rows = list(csv.DictReader(STATION_CSV.open("r", encoding="utf-8")))
    fieldnames = list(rows[0].keys()) if rows else []
    for col in ["terrain_sector", "water_factor", "city_sector", "factor_summary"]:
        if col not in fieldnames:
            fieldnames.append(col)

    for r in rows:
        city = str(r.get("city") or "")
        try:
            lat = float(r.get("lat") or 0.0)
            lon = float(r.get("lon") or 0.0)

            pts = [(lat, lon)] + [_dest_point(lat, lon, b, args.radius_km) for b, _ in SECTORS]
            elev = _fetch_elev(pts)
            base = elev[0]
            ring = elev[1:]

            deltas = [ring[i] - base for i in range(len(SECTORS))]
            best_i = max(range(len(SECTORS)), key=lambda i: deltas[i])
            sec = SECTORS[best_i][1]
            terrain_sector = f"{sec}侧高地" if deltas[best_i] >= 80 else "高地方位不显著"

            t2 = str(r.get("terrain_tag2") or "")
            water = _water_factor(t2)
            csec = _city_sector(city, lat, lon)

            r["terrain_sector"] = terrain_sector
            r["water_factor"] = water
            r["city_sector"] = csec
            r["factor_summary"] = f"{terrain_sector}·{water}·主城在{csec}侧"
        except Exception as exc:
            r["terrain_sector"] = "未知"
            r["water_factor"] = "未知"
            r["city_sector"] = "未知"
            r["factor_summary"] = f"未知({exc})"

    with STATION_CSV.open("w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        w.writerows(rows)

    print(f"updated {len(rows)} stations in {STATION_CSV}")


if __name__ == "__main__":
    main()
