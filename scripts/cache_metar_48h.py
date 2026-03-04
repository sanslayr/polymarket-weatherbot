#!/usr/bin/env python3
"""Cache recent 48h METAR locally for all stations in station_links.csv."""

from __future__ import annotations

import argparse
import csv
import json
from datetime import datetime, timezone
from pathlib import Path

import requests


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def load_stations(csv_path: Path) -> list[dict[str, str]]:
    with csv_path.open(newline="", encoding="utf-8") as f:
        return list(csv.DictReader(f))


def fetch_metar_48h(icao: str) -> list[dict]:
    url = f"https://aviationweather.gov/api/data/metar?ids={icao}&format=json&hours=48"
    r = requests.get(url, timeout=60)
    r.raise_for_status()
    data = r.json()
    return data if isinstance(data, list) else []


def main() -> None:
    p = argparse.ArgumentParser(description="Cache 48h METAR for stations")
    p.add_argument("--csv", default="station_links.csv")
    p.add_argument("--out-dir", default="cache/metar")
    p.add_argument("--station", default="", help="Optional city or ICAO filter")
    args = p.parse_args()

    csv_path = Path(args.csv)
    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    stations = load_stations(csv_path)
    filt = args.station.strip().lower()
    if filt:
        stations = [
            r
            for r in stations
            if r.get("icao", "").strip().lower() == filt or r.get("city", "").strip().lower() == filt
        ]

    ts = utc_now_iso()
    ok = 0
    fail = 0

    for row in stations:
        icao = row["icao"].strip().upper()
        city = row["city"].strip()
        try:
            metar = fetch_metar_48h(icao)
            payload = {
                "meta": {
                    "icao": icao,
                    "city": city,
                    "hours": 48,
                    "fetched_at_utc": ts,
                    "source": "https://aviationweather.gov/api/data/metar",
                },
                "data": metar,
            }
            (out_dir / f"{icao}_48h.json").write_text(
                json.dumps(payload, ensure_ascii=False, indent=2) + "\n",
                encoding="utf-8",
            )
            ok += 1
        except Exception as exc:
            fail += 1
            err = {
                "meta": {
                    "icao": icao,
                    "city": city,
                    "hours": 48,
                    "fetched_at_utc": ts,
                },
                "error": str(exc),
            }
            (out_dir / f"{icao}_48h.error.json").write_text(
                json.dumps(err, ensure_ascii=False, indent=2) + "\n",
                encoding="utf-8",
            )

    print(json.dumps({"stations": len(stations), "ok": ok, "fail": fail, "out_dir": str(out_dir)}, ensure_ascii=True))


if __name__ == "__main__":
    main()
