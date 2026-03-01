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


def _haversine_km(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    r = 6371.0
    p1 = math.radians(lat1)
    p2 = math.radians(lat2)
    dlat = math.radians(lat2 - lat1)
    dlon = math.radians(lon2 - lon1)
    a = math.sin(dlat / 2.0) ** 2 + math.cos(p1) * math.cos(p2) * math.sin(dlon / 2.0) ** 2
    return 2.0 * r * math.atan2(math.sqrt(a), math.sqrt(1.0 - a))


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


def _city_sector(city: str, st_lat: float, st_lon: float) -> tuple[str, float | None]:
    # open-meteo geocoding, choose nearest candidate to station
    try:
        url = "https://geocoding-api.open-meteo.com/v1/search"
        r = requests.get(url, params={"name": city, "count": 8, "language": "zh", "format": "json"}, timeout=15)
        r.raise_for_status()
        arr = (r.json() or {}).get("results") or []
        if not arr:
            return "未知", None

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
        sec = ["北", "东北", "东", "东南", "南", "西南", "西", "西北"][idx]
        dkm = _haversine_km(st_lat, st_lon, la, lo)
        return sec, round(dkm, 1)
    except Exception:
        return "未知", None


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
    for col in ["terrain_sector", "water_factor", "water_sector", "city_sector", "city_distance_km", "urban_position", "factor_summary", "site_tag"]:
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

            # water-side proxy: lowest surrounding side likely where nearby water/lowland opens.
            low_i = min(range(len(SECTORS)), key=lambda i: deltas[i])
            water_sec = SECTORS[low_i][1]

            t2 = str(r.get("terrain_tag2") or "")
            water = _water_factor(t2)
            csec, cdist = _city_sector(city, lat, lon)

            opp = {
                "北": "南", "南": "北", "东": "西", "西": "东",
                "东北": "西南", "西南": "东北", "东南": "西北", "西北": "东南",
            }
            st_from_city = opp.get(csec, "未知")
            if cdist is not None and cdist <= 8:
                urban_pos = "城中"
            elif cdist is not None and cdist <= 25:
                urban_pos = f"主城{st_from_city}侧(城郊)"
            elif cdist is not None and cdist <= 50:
                urban_pos = f"主城{st_from_city}侧(远郊)"
            else:
                urban_pos = f"主城{st_from_city}侧"

            water_sector_txt = f"{water_sec}侧临水" if water != "内陆主导" else "内陆主导"

            r["terrain_sector"] = terrain_sector
            r["water_factor"] = water
            r["water_sector"] = water_sector_txt
            r["city_sector"] = csec
            r["city_distance_km"] = "" if cdist is None else f"{cdist:.1f}"
            r["urban_position"] = urban_pos
            r["factor_summary"] = f"{terrain_sector}·{water_sector_txt}·{urban_pos}"

            # site_tag is intended as fixed curated station label.
            # only auto-fill when empty.
            cur_site = str(r.get("site_tag") or "").strip()
            if not cur_site:
                water_short = {
                    "沿海影响": "沿海",
                    "河口影响": "河口",
                    "近水体影响": "近水",
                    "内陆主导": "内陆",
                }.get(water, "内陆")
                t1 = str(r.get("terrain_tag") or "").strip()
                if t1 and water_short and (water_short not in t1):
                    r["site_tag"] = f"{t1}·{water_short}"
                else:
                    r["site_tag"] = t1 or water_short
        except Exception as exc:
            r["terrain_sector"] = "未知"
            r["water_factor"] = "未知"
            r["water_sector"] = "未知"
            r["city_sector"] = "未知"
            r["city_distance_km"] = ""
            r["urban_position"] = "未知"
            r["factor_summary"] = f"未知({exc})"

    with STATION_CSV.open("w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        w.writerows(rows)

    print(f"updated {len(rows)} stations in {STATION_CSV}")


if __name__ == "__main__":
    main()
