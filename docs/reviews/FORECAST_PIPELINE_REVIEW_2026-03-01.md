# FORECAST PIPELINE REVIEW — 2026-03-01

## Scope
This update aligned data acquisition, forecast architecture, and report timing with the current 3D-system upgrade direction.

## What changed

### 1) Data acquisition resilience / flow control
- `FORECAST_ANCHOR_LIMIT` default changed to `0` (full-day anchors by default).
- Added `FORECAST_MAX_WORKERS` (default `1`) to control concurrent anchor pulls.
- Added optional jitter `FORECAST_ANCHOR_JITTER_MS` to smooth burst requests.
- Added cross-runtime fallback reader for synoptic bundles:
  - `_read_recent_synoptic_bundle(...)`
  - Used when current runtime anchor pulls fail.
- Added fallback quality state: `source_state = "fallback-cache"`.

### 2) Detection library expansion (synoptic/object_3d)
- Added new synoptic detectors:
  - `frontogenesis_zone`
  - `llj_shear_zone`
  - `dry_intrusion_700`
  - `baroclinic_coupling`
- `vertical_3d` type grouping expanded for `baroclinic` and `shear`.
- `object_3d` scoring now ingests 700/925 diagnostic constraints and outputs:
  - `confidence` (high/medium/low)
  - `evidence.support`
  - `evidence.conflict`

### 3) Sounding integration track
- Added `thermo` structured hook in sounding diagnostics and forecast features:
  - CAPE/CIN/LCL/LFC/EL placeholders are now first-class fields.
- Report can consume thermo signals when upstream profile-derived values are present.

### 4) Report timing transparency
- `/look` header now prints stage timing summary:
  - fetch / metar / forecast(total)
  - forecast split: cache read / synoptic build / decision / cache write
- Added user-facing 429 degradation notice in plain language.

## Current architecture status

### Good
- Runtime now supports full-day 3D coverage by default.
- Forecast module can degrade more gracefully via fallback synoptic cache.
- Report stays concise while backend grows in dimensionality.

### Still pending
- Full local CAPE/CIN computation from raw sounding profile (not only pass-through fields).
- P1 real-time trigger pack (cool-pool/ceiling-lock/peak-lock detector) in METAR decision chain.
- Strict summary-layer selection (2-4 highest impact items) from expanded detector outputs.

## Recommended next steps
1. Implement local sounding profile parser + thermo solver (full/lite/skip modes).
2. Add P1 trigger engine to real-time section, ranked by impact × urgency.
3. Add detector output ranker so default report remains concise even as backend expands.
4. Keep `FORECAST_MAX_WORKERS=1` for stability; raise only during critical windows.
