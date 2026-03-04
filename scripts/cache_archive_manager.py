#!/usr/bin/env python3
"""Cache + archive manager for slow-updating analysis modules.

Use for synoptic/sounding style outputs that update every 3-6 hours.
"""

from __future__ import annotations

import argparse
import hashlib
import json
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

DEFAULT_TTL_HOURS = 6
MIN_TTL_HOURS = 3
MAX_TTL_HOURS = 6


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


def parse_iso(ts: str) -> datetime:
    if ts.endswith("Z"):
        ts = ts[:-1] + "+00:00"
    return datetime.fromisoformat(ts).astimezone(timezone.utc)


def iso(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def build_key(station: str, target_date: str, model: str, module: str) -> str:
    raw = f"{station.lower()}|{target_date}|{model.lower()}|{module.lower()}"
    digest = hashlib.sha1(raw.encode("utf-8")).hexdigest()[:12]
    return f"{station.lower()}_{target_date}_{model.lower()}_{module.lower()}_{digest}"


def cache_path(cache_dir: Path, key: str) -> Path:
    return cache_dir / f"{key}.json"


def archive_path(archive_dir: Path, key: str, ts: datetime) -> Path:
    day = ts.strftime("%Y-%m-%d")
    t = ts.strftime("%Y%m%dT%H%M%SZ")
    return archive_dir / day / f"{key}_{t}.json"


def load_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def save_json(path: Path, data: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data, ensure_ascii=True, indent=2) + "\n", encoding="utf-8")


def is_cache_fresh(cache_doc: dict[str, Any], ttl_hours: int, now: datetime) -> bool:
    updated = parse_iso(cache_doc["meta"]["updated_at_utc"])
    return now - updated <= timedelta(hours=ttl_hours)


def clamp_ttl(ttl: int) -> int:
    if ttl < MIN_TTL_HOURS:
        return MIN_TTL_HOURS
    if ttl > MAX_TTL_HOURS:
        return MAX_TTL_HOURS
    return ttl


def cmd_get(args: argparse.Namespace) -> int:
    now = utcnow()
    ttl = clamp_ttl(args.ttl_hours)

    key = build_key(args.station, args.target_date, args.model, args.module)
    path = cache_path(Path(args.cache_dir), key)

    if not path.exists():
        print(json.dumps({
            "status": "miss",
            "reason": "cache_not_found",
            "key": key,
            "cache_path": str(path),
            "ttl_hours": ttl,
        }, ensure_ascii=True, indent=2))
        return 0

    doc = load_json(path)
    fresh = is_cache_fresh(doc, ttl, now)
    if args.force_refresh:
        fresh = False

    print(json.dumps({
        "status": "hit" if fresh else "stale",
        "key": key,
        "cache_path": str(path),
        "ttl_hours": ttl,
        "meta": doc.get("meta", {}),
        "payload": doc.get("payload") if fresh else None,
    }, ensure_ascii=True, indent=2))
    return 0


def cmd_put(args: argparse.Namespace) -> int:
    now = utcnow()
    ttl = clamp_ttl(args.ttl_hours)

    key = build_key(args.station, args.target_date, args.model, args.module)
    cpath = cache_path(Path(args.cache_dir), key)
    payload = load_json(Path(args.payload_file))

    doc = {
        "meta": {
            "key": key,
            "station": args.station,
            "target_date": args.target_date,
            "model": args.model,
            "module": args.module,
            "ttl_hours": ttl,
            "updated_at_utc": iso(now),
            "expires_at_utc": iso(now + timedelta(hours=ttl)),
            "source": args.source,
        },
        "payload": payload,
    }

    save_json(cpath, doc)

    apath = archive_path(Path(args.archive_dir), key, now)
    save_json(apath, doc)

    print(json.dumps({
        "status": "stored",
        "key": key,
        "cache_path": str(cpath),
        "archive_path": str(apath),
        "expires_at_utc": doc["meta"]["expires_at_utc"],
    }, ensure_ascii=True, indent=2))
    return 0


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="Cache + archive manager for analysis modules")
    sub = p.add_subparsers(dest="cmd", required=True)

    def add_common(sp: argparse.ArgumentParser) -> None:
        sp.add_argument("--station", required=True)
        sp.add_argument("--target-date", required=True, help="YYYY-MM-DD")
        sp.add_argument("--model", required=True)
        sp.add_argument("--module", required=True, choices=["synoptic", "sounding", "combined"])
        sp.add_argument("--ttl-hours", type=int, default=DEFAULT_TTL_HOURS)
        sp.add_argument("--cache-dir", default="cache")
        sp.add_argument("--archive-dir", default="archive")

    g = sub.add_parser("get", help="Read cache status/payload")
    add_common(g)
    g.add_argument("--force-refresh", action="store_true")

    put = sub.add_parser("put", help="Write cache and archive snapshot")
    add_common(put)
    put.add_argument("--payload-file", required=True)
    put.add_argument("--source", default="manual")

    return p


def main() -> None:
    args = build_parser().parse_args()
    if args.cmd == "get":
        raise SystemExit(cmd_get(args))
    if args.cmd == "put":
        raise SystemExit(cmd_put(args))
    raise SystemExit(2)


if __name__ == "__main__":
    main()
