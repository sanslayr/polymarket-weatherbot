# PROJECT OVERVIEW

## Goal

`polymarket-weatherbot` provides a station-centric weather analysis pipeline for temperature-market workflows (primarily daily Tmax), integrating forecast fields, METAR observations, synoptic diagnostics, and market range interpretation into a single operational report output.

## System Shape

The project follows a modular pipeline:

1. Parse user command and resolve station/city
2. Fetch/assemble forecast and synoptic context
3. Ingest/diagnose METAR and short-term station behavior
4. Apply scoring/label logic against market ladders
5. Render final report output and archive runtime artifacts

## Core Modules

- `scripts/look_command.py`: command interpretation and high-level flow input shaping
- `scripts/station_catalog.py`: station resolution, timezone mapping, and station metadata helpers
- `scripts/hourly_data_service.py`: hourly forecast retrieval and normalization
- `scripts/metar_analysis_service.py`: METAR feature extraction and observational diagnostics
- `scripts/polymarket_client.py`: Polymarket event fetching with runtime cache
- `scripts/polymarket_render_service.py`: market range parsing and output-ready market sections
- `scripts/report_render_service.py`: report assembly and section rendering
- `scripts/report_peak_module.py`: Tmax-focused peak window logic
- `scripts/forecast_pipeline.py`, `scripts/synoptic_runner.py`, `scripts/synoptic_2d_detector.py`: forecast-synoptic processing and decision artifacts

## Data and Config

- `station_links.csv`: station/city master table and external links
- `config/tmax_learning_params.json`: learning/scoring parameters for report logic
- `config/station_terrain_tags.json`: station terrain metadata
- `cache/runtime/`: runtime caches and model artifacts
- `runtime/`: runtime logs/outputs for operational continuity

## Documentation Map

- Architecture and contracts: `docs/core/ARCHITECTURE.md`, `docs/core/LOOK_OUTPUT_CONTRACT.md`, `docs/core/DECISION_SCHEMA.md`
- Guardrails and implementation notes: `docs/core/AGENT_UPDATE_GUARDRAILS.md`, `docs/core/TECHNICAL_IMPLEMENTATION_NOTES.md`
- Operations and reviews: `docs/operations/*`, `docs/reviews/*`

## Backup Positioning

This repository is currently maintained as both:

- source code repository, and
- full operational backup snapshot (including cache/runtime state)

That design choice optimizes rapid server migration and disaster recovery at the cost of larger repository size.
