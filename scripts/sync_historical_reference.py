#!/usr/bin/env python3
"""Copy archive-derived METAR reference files into weatherbot data."""

from __future__ import annotations

import argparse
import shutil
from pathlib import Path

REPORT_FILES = (
    "weatherbot_station_priors.csv",
    "weatherbot_daily_local_regimes.csv",
    "weatherbot_monthly_climatology.csv",
    "weatherbot_metar_reference.md",
    "metar_station_profiles.md",
    "metar_station_profiles.csv",
)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Sync archive-derived METAR reference files into weatherbot data")
    parser.add_argument(
        "--reports-source-dir",
        default="/Users/ham/polymarket-weather-archive/reports",
        help="Directory containing archive report outputs",
    )
    parser.add_argument(
        "--raw-source-dir",
        default="/Users/ham/polymarket-weather-archive/data/raw/metar_isd",
        help="Directory containing raw METAR/ISD yearly csv.gz files",
    )
    parser.add_argument(
        "--dest-dir",
        default="data/historical_reference",
        help="Destination directory inside weatherbot repo",
    )
    return parser


def main() -> None:
    args = build_parser().parse_args()
    reports_source_dir = Path(args.reports_source_dir).resolve()
    raw_source_dir = Path(args.raw_source_dir).resolve()
    dest_dir = Path(args.dest_dir).resolve()
    dest_dir.mkdir(parents=True, exist_ok=True)
    raw_dest_dir = dest_dir / "raw_metar_isd"
    raw_dest_dir.mkdir(parents=True, exist_ok=True)

    copied = 0
    missing: list[str] = []
    for filename in REPORT_FILES:
        source = reports_source_dir / filename
        if not source.exists():
            missing.append(filename)
            continue
        shutil.copy2(source, dest_dir / filename)
        copied += 1

    raw_files = 0
    if raw_source_dir.exists():
        for station_dir in sorted(raw_source_dir.iterdir()):
            if not station_dir.is_dir():
                continue
            target_station_dir = raw_dest_dir / station_dir.name
            target_station_dir.mkdir(parents=True, exist_ok=True)
            for source in sorted(station_dir.glob("*.csv.gz")):
                shutil.copy2(source, target_station_dir / source.name)
                raw_files += 1

    print(f"copied={copied}")
    print(f"dest_dir={dest_dir}")
    print(f"raw_files={raw_files}")
    if missing:
        print("missing=" + ",".join(missing))


if __name__ == "__main__":
    main()
