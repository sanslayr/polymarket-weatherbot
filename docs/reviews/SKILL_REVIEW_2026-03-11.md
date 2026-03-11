# Skill Review 2026-03-11

## Scope

Review recent `/look` / market alert / runtime-boundary updates for patch accumulation, mixed old-new coupling, and doc drift.

## Findings

### 1. Station-specific external source logic was embedded in `/look` orchestrator

- Problem:
  - `look_report_service.py` contained LTAC-only `MGM` cache / fetch / render code.
  - This mixed station-specific external-source behavior into the main `/look` orchestration path.
- Risk:
  - Every future station exception would have been tempted into the same file.
  - `/look` service complexity would keep growing for non-core reasons.
- Fix:
  - Extracted to `scripts/station_external_reference_service.py`
  - `look_report_service.py` now only consumes the adapter

### 2. Market alert push logic lacked a single-source contract doc

- Problem:
  - Trigger design lived in `MARKET_IMPLIED_REPORT_SIGNAL_PLAN.md`
  - Worker runtime behavior lived in `MARKET_ALERT_WORKER.md`
  - Latest message-format changes mostly lived in code / tests
- Risk:
  - Future edits could easily update one layer and miss the others
- Fix:
  - Added `docs/core/MARKET_ALERT_NOTIFICATION_FLOW.md`

### 3. Architecture/docs index lagged behind current runtime boundaries

- Problem:
  - Core docs did not clearly list the new station-external-reference adapter
  - Docs index did not expose market alert notification flow as a current truth source
- Fix:
  - Updated `ARCHITECTURE.md`
  - Updated `DOCS_INDEX.md`
  - Updated `MARKET_ARCHITECTURE.md`

## Remaining Risks

- `report_render_service.py` is still a major complexity hotspot even after recent render-only cleanup.
- `market_alert_worker.py` still combines scheduling, state persistence, and delivery orchestration in one file; long-term it should split into scheduler/state/delivery modules.
- `/look` and proactive alert still share some render-era assumptions around market labels and report phrasing, although the runtime paths are now separated.

## Recommendation

Next structural pass should target:

1. split `market_alert_worker.py` into scheduler/state/delivery pieces
2. continue shrinking `report_render_service.py` into pure presentation helpers
3. keep station-specific external data sources behind adapter modules only
