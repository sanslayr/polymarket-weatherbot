# polymarket-weatherbot

Polymarket weather forecasting skill for station-based Tmax analysis, report generation, and operational decision support.

## What This Repository Contains

- Weatherbot forecasting and report generation scripts for station/city daily max temperature workflows
- Station catalog and metadata (`station_links.csv`) with per-station operational links
- Forecast/synoptic analysis modules used by `/look` command flow
- Docs covering architecture, operation guardrails, review records, and research handoff notes
- Runtime logic and lightweight local state handling used by the forecasting workflow

## Key Entry Points

- Main CLI orchestration: `scripts/telegram_report_cli.py`
- Command parsing and station routing: `scripts/look_command.py`, `scripts/station_catalog.py`
- Forecast pipeline: `scripts/forecast_pipeline.py`, `scripts/synoptic_runner.py`
- Report rendering: `scripts/report_render_service.py`, `scripts/report_peak_module.py`
- Station data source: `station_links.csv`

## Notes

- Runtime/cache artifacts are not part of the source tree and should stay ignored.
- Large binary forecast caches such as `grib2` files should be treated as ephemeral runtime data only.
