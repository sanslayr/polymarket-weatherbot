#!/usr/bin/env python3
"""Copy lightweight archive outputs into weatherbot cache for online use."""

from __future__ import annotations

import argparse
import shutil
from pathlib import Path

FILES = (
    "weatherbot_station_priors.csv",
    "weatherbot_daily_local_regimes.csv",
    "weatherbot_monthly_climatology.csv",
    "weatherbot_metar_reference.md",
    "metar_station_profiles.md",
    "metar_station_profiles.csv",
)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Sync archive-derived METAR reference files into weatherbot cache")
    parser.add_argument(
        "--source-dir",
        default="/Users/ham/polymarket-weather-archive/reports",
        help="Directory containing archive report outputs",
    )
    parser.add_argument(
        "--dest-dir",
        default="cache/historical_reference",
        help="Destination directory inside weatherbot repo",
    )
    return parser


def main() -> None:
    args = build_parser().parse_args()
    source_dir = Path(args.source_dir).resolve()
    dest_dir = Path(args.dest_dir).resolve()
    dest_dir.mkdir(parents=True, exist_ok=True)

    copied = 0
    missing: list[str] = []
    for filename in FILES:
        source = source_dir / filename
        if not source.exists():
            missing.append(filename)
            continue
        shutil.copy2(source, dest_dir / filename)
        copied += 1

    print(f"copied={copied}")
    print(f"dest_dir={dest_dir}")
    if missing:
        print("missing=" + ",".join(missing))


if __name__ == "__main__":
    main()
