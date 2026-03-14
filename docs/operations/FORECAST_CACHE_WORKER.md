# Forecast Cache Worker

Last updated: 2026-03-10

## Purpose

Prewarm `/look` forecast caches in the background so user-facing requests spend less time waiting on:

- `open-meteo` hourly fetch
- ECMWF synoptic / forecast cache build

This worker does not render `/look` output.
It only builds the upstream cache layers used by `/look`.

## Entry Point

```bash
python3 scripts/forecast_cache_worker.py
```

## What It Warms

- hourly cache via `fetch_hourly_openmeteo`
- forecast decision cache via `load_or_build_forecast_decision`
- synoptic / 3D cache transitively used by the forecast layer

## Defaults

- `WEATHERBOT_OPENMETEO_TIMEOUT_SECONDS=3`
- `FORECAST_CACHE_PREWARM_POLL_SECONDS=60`
- `FORECAST_CACHE_PREWARM_DAYS_AHEAD=1`
- `FORECAST_CACHE_PREWARM_MAX_WORKERS=2`
- `FORECAST_CACHE_PREWARM_ALWAYS_ON=1`
- `FORECAST_CACHE_CONTINUOUS_INTERVAL_SECONDS=900`
- `FORECAST_CACHE_PREWARM_CYCLE_START_HOURS=3`
- `FORECAST_CACHE_PREWARM_CYCLE_POLL_MINUTES=30`
- `FORECAST_CACHE_PREWARM_CYCLE_STOP_HOURS=6`
- `FORECAST_CACHE_MAX_AGE_HOURS=24`

## Scheduling

The worker now defaults to continuous mode.

- In continuous mode, it keeps refreshing station/date caches on an interval and also reacts to METAR updates.
- The default interval is `900` seconds.
- Set `FORECAST_CACHE_PREWARM_ALWAYS_ON=0` if you explicitly want the older cycle-driven behavior.

When continuous mode is disabled, the worker is cycle-driven, not interval-driven.

- It watches the ECMWF 6-hour runtime buckets: `00Z`, `06Z`, `12Z`, `18Z`
- It starts probing a new cycle only after the configured post-cycle delay
- Default probe start is `3` hours after cycle time
- If the new runtime is not ready yet, it probes again every `30` minutes
- Once the cycle reaches `+6` hours from its base time, probing stops for that cycle
- A cycle is considered complete only when the returned `runtime_tag` matches the target ECMWF cycle

Example:

- `06Z` cycle starts probing at `09:00 UTC`
- if `09:00` still returns an older runtime, the worker retries at `09:30`, `10:00`, and so on
- after `12:00 UTC`, the worker stops trying `06Z` and waits for the `12Z` probe window
- once a station/date finishes successfully for that cycle, older forecast caches for the same station/date are purged
- even if a new cycle is not available yet, forecast cache files older than 24 hours are purged

## Runtime Files

- state: `cache/runtime/forecast_cache_worker/state.json`
- pid: `cache/runtime/forecast_cache_worker/worker.pid`
- log: `cache/runtime/forecast_cache_worker/worker.log`

## Operational Notes

- The worker uses station-local dates.
- It is intended to run continuously in the background.
- Increase `FORECAST_CACHE_PREWARM_DAYS_AHEAD` if you want tomorrow's local-date cache warmed too.
- Old `forecast_decision` and `forecast_3d_bundle` caches are removed after a newer runtime for the same station/date is written.
- Stale `forecast_decision` and `forecast_3d_bundle` files older than `FORECAST_CACHE_MAX_AGE_HOURS` are removed even when the latest cycle has not been fetched yet.
