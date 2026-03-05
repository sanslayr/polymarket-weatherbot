# polymarket-weatherbot

Polymarket weather forecasting skill and runtime workspace backup for station-based Tmax analysis, report generation, and operational cache/state continuity.

## What This Repository Contains

- Weatherbot forecasting and report generation scripts for station/city daily max temperature workflows
- Station catalog and metadata (`station_links.csv`) with per-station operational links
- Forecast/synoptic analysis modules used by `/look` command flow
- Docs covering architecture, operation guardrails, review records, and research handoff notes
- Runtime/cache snapshots and local workspace state for backup continuity

## Key Entry Points

- Main CLI orchestration: `scripts/telegram_report_cli.py`
- Command parsing and station routing: `scripts/look_command.py`, `scripts/station_catalog.py`
- Forecast pipeline: `scripts/forecast_pipeline.py`, `scripts/synoptic_runner.py`
- Report rendering: `scripts/report_render_service.py`, `scripts/report_peak_module.py`
- Station data source: `station_links.csv`

## Notes

- This repository intentionally includes runtime/cache data for backup purposes.
- If you want a code-only branch later, create a new branch and add a strict `.gitignore` policy.
