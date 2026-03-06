# Historical Archive Integration

## Goal

Feed archive-derived METAR reference outputs into `weatherbot` in a self-contained way, including raw station observations needed for intraday analog matching.

## Current Online Inputs

`scripts/historical_context_provider.py` reads these files:

- `weatherbot_station_priors.csv`
- `weatherbot_daily_local_regimes.csv`
- `weatherbot_monthly_climatology.csv`
- `weatherbot_metar_reference.md` (optional human reference)

Default lookup order (unless `WEATHERBOT_HISTORICAL_DIR` is explicitly set, which overrides all defaults):

1. `data/historical_reference/`
2. `cache/historical_reference/` (legacy fallback)
3. sibling archive repo: `../polymarket-weather-archive/reports`

Repo-local raw METAR/ISD for intraday analog matching:

- `data/historical_reference/raw_metar_isd/`

Raw lookup order (unless `WEATHERBOT_HISTORICAL_RAW_DIR` is explicitly set, which overrides all defaults):

1. `data/historical_reference/raw_metar_isd/`
2. sibling archive repo raw path

## Sync Workflow

Copy fresh archive outputs into weatherbot data:

```bash
python3 scripts/sync_historical_reference.py
```

Custom source:

```bash
python3 scripts/sync_historical_reference.py \
  --reports-source-dir /path/to/polymarket-weather-archive/reports \
  --raw-source-dir /path/to/polymarket-weather-archive/data/raw/metar_isd
```

## Runtime Behavior

`telegram_report_cli.py` now appends a historical block to `/look` output:

- station background and site factors
- station-level long-run METAR priors
- same-month climatology baseline
- live local-regime inference from current METAR diagnostics
- top similar historical days
- lightweight adjustment hint

The provider also stores this payload on `metar_diag["historical_context"]` and
`metar_diag["historical_adjustment_hint"]` so later forecast logic can consume it
without changing the fetch/render call chain again.

It now also stores:

- `metar_diag["historical_weighted_reference"]`
- `metar_diag["historical_recommended_tmax_c"]`
- `metar_diag["historical_synoptic_context"]`

This means historical analog guidance can already participate in the online Tmax
range calculation, not only as a report appendix.

Feature flag:

- default enabled
- disable with `LOOK_ENABLE_HISTORICAL_CONTEXT=0`

## Scope Boundary

This is still `METAR local regime` guidance, not full large-scale circulation typing.

## Future ERA5 Hook

Historical fusion now uses a normalized synoptic interface, implemented in:

- `scripts/synoptic_adjustment_context.py`

Current source:

- `forecast_decision -> synoptic_adjustment_context`

Reserved future source:

- `ERA5-derived synoptic context -> synoptic_adjustment_context`

Expected normalized fields:

- `source`
- `line_500`
- `line_850`
- `extra`
- `synoptic_text`
- `warming_support_score`
- `cooling_support_score`
- `warm_tokens`
- `cool_tokens`

`build_historical_context(...)` already accepts an optional external
`synoptic_context`. Once ERA5 daily/background features are available, feed them
into that parameter using the same normalized contract instead of changing the
historical matching or peak-range modules again.
