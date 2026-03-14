from __future__ import annotations

import json
import sys
from datetime import datetime
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parent.parent
SCRIPTS = ROOT / "scripts"
if str(SCRIPTS) not in sys.path:
    sys.path.insert(0, str(SCRIPTS))

from ecmwf_ensemble_factor_service import build_ecmwf_ensemble_factor_batch, ensemble_factor_detail_level
from forecast_cache_worker import (
    _build_analysis_window,
    _ecmwf_cycle_runtime_tag,
    _load_station_rows,
    _station_from_row,
    _target_dates_for_station,
    _utc_now,
)
from station_catalog import station_timezone_name


def _build_requests(days_ahead: int) -> list[dict[str, Any]]:
    now_utc = _utc_now()
    runtime_tag = _ecmwf_cycle_runtime_tag(now_utc)
    requests: list[dict[str, Any]] = []
    for row in _load_station_rows():
        station = _station_from_row(row)
        tz_name = station_timezone_name(station)
        for target_date in _target_dates_for_station(station, now_utc=now_utc, days_ahead=days_ahead):
            try:
                (
                    _hourly_payload,
                    _hourly_day,
                    metar24,
                    metar_diag,
                    _primary_window,
                    analysis_window,
                    _temp_shape_analysis,
                    _unit_pref,
                    _model,
                ) = _build_analysis_window(
                    station=station,
                    target_date=target_date,
                    tz_name=tz_name,
                )
            except Exception as exc:
                print(
                    f"SKIP {station.icao} {target_date} analysis_window_error={exc}",
                    flush=True,
                )
                continue
            peak_local = str(analysis_window.get("peak_local") or "").strip()
            if not peak_local:
                print(f"SKIP {station.icao} {target_date} missing_peak_local", flush=True)
                continue
            requests.append(
                {
                    "request_id": f"{station.icao}:{target_date}",
                    "station_icao": station.icao,
                    "station_lat": float(station.lat),
                    "station_lon": float(station.lon),
                    "peak_local": peak_local,
                    "analysis_local": str(metar_diag.get("latest_report_local") or ""),
                    "tz_name": tz_name,
                    "preferred_runtime_tag": runtime_tag,
                    "metar24": list(metar24 or []),
                }
            )
    return requests


def main() -> int:
    days_ahead = 1
    started_at = datetime.utcnow().isoformat() + "Z"
    requests = _build_requests(days_ahead=days_ahead)
    print(f"START total={len(requests)} at={started_at}", flush=True)
    if not requests:
        print("SUMMARY total=0 surface_ready=0", flush=True)
        return 0

    try:
        results = build_ecmwf_ensemble_factor_batch(requests=requests, detail_stage="auto", root=ROOT)
    except Exception as exc:
        print(f"FATAL batch_surface_ens_failed={exc}", flush=True)
        return 1

    surface_ready = 0
    for req in requests:
        request_id = str(req["request_id"])
        payload = dict(results.get(request_id) or {})
        detail_level = ensemble_factor_detail_level(payload)
        if detail_level in {"surface_anchor", "surface_trajectory"}:
            surface_ready += 1
        print(
            "OK "
            + json.dumps(
                {
                    "request_id": request_id,
                    "station": req["station_icao"],
                    "detail_level": detail_level,
                    "member_count": payload.get("member_count"),
                },
                ensure_ascii=False,
            ),
            flush=True,
        )
    print(f"SUMMARY total={len(requests)} surface_ready={surface_ready}", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
