# PROJECT OVERVIEW

Last updated: 2026-03-09

## Goal

`polymarket-weatherbot` provides a station-centric Tmax analysis runtime.

Current objectives:

- produce a clear human-readable Tmax report
- support future headless analysis, scheduled scanning, and opportunity triggers without depending on `/look`
- keep weather inference independent from market labeling
- preserve a structured analysis layer that can later feed posterior/probability and execution modules

## Current System Shape

The current user-facing runtime chain is:

1. parse `/look` command and resolve station/date
2. fetch hourly forecast, METAR, and 3D synoptic context
3. build forecast decision artifacts and realtime observation diagnostics
4. assemble a structured `analysis_snapshot`
5. build report-support focus bundle
6. render human report output
7. optionally render Polymarket market section from weather-side outputs

This means human report output is now a consumer of the analysis layer, not the place where core Tmax logic should live. `/look` is the current public entrypoint, not the intended long-term boundary of the system.

## Main Layers

### 1) Ingress / Routing

- `scripts/telegram_report_cli.py`
- `scripts/look_command.py`
- `scripts/station_catalog.py`

### 2) Data Providers

- `scripts/hourly_data_service.py`
  - lightweight hourly forecast and Tmax window detection
  - `open-meteo` primary
- `scripts/synoptic_provider_router.py`
  - 3D provider routing
  - `ecmwf-open-data` primary, `gfs-grib2` fallback
- `scripts/ecmwf_open_data_provider.py`
- `scripts/gfs_grib_provider.py`
- `scripts/metar_utils.py`
- `scripts/sounding_obs_service.py`

### 3) Decision / Analysis

- `scripts/synoptic_runner.py`
- `scripts/forecast_pipeline.py`
- `scripts/vertical_3d.py`
- `scripts/advection_review.py`
- `scripts/temperature_shape_analysis.py`
- `scripts/temperature_window_resolver.py`
- `scripts/temperature_phase_decision.py`
- `scripts/boundary_layer_regime.py`
- `scripts/diagnostics_sounding.py`
- `scripts/synoptic_summary_service.py`
- `scripts/peak_range_service.py`
- `scripts/peak_range_signal_service.py`
- `scripts/peak_range_history_service.py`
- `scripts/peak_range_render_service.py`

### 4) Snapshot / Rendering

- `scripts/analysis_snapshot_service.py`
- `scripts/report_focus_service.py`
- `scripts/report_render_service.py`
- `scripts/metar_analysis_service.py`
- `scripts/polymarket_render_service.py`
- `scripts/market_label_policy.py`

## Current Maintenance Review

The architecture is materially cleaner than the pre-snapshot stage, but three issues still matter:

1. `analysis_snapshot` 现在已显式带 `canonical_raw_state`、`posterior_feature_vector`、`quality_snapshot` 和 `weather_posterior`，天气主链 contract 已立起来，但字段覆盖仍需继续扩展。
2. `report_render_service.py` 已退出主关注项推理，当前主路径只消费 snapshot 与 `report_focus_service.py` 的结果。
3. `peak_range_service.py` has become the new hotspot; render/history/signal helpers have already been split out, but the core range computation remains large. Long-term it should continue splitting into:
   - posterior/range computation
   - historical/calibration helpers

## Recommended Next-Step Direction

To support future probability estimation and automated execution, the repo should evolve toward the target design documented in [TARGET_ARCHITECTURE.md](/home/ubuntu/.openclaw/workspace/skills/polymarket-weatherbot/docs/core/TARGET_ARCHITECTURE.md).

The current codebase already has partial `physical_feature_layer` behavior via `analysis_snapshot_service.py`, but it still mixes some presentation-oriented fields with quantitative diagnostics.
