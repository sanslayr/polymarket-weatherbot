# Skill Review 2026-03-11

## Scope

Review recent `/look` / market alert / runtime-boundary updates for patch accumulation, mixed old-new coupling, and doc drift, then complete the next structural cleanup pass.

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

### 4. Proactive alert worker mixed scheduler/state/delivery in one loop

- Problem:
  - `market_alert_worker.py` handled cadence inference, state persistence, dedupe, Telegram delivery and task orchestration in one file.
- Risk:
  - Every alert-policy change would keep adding special cases to the same loop.
  - Runtime state and delivery changes would remain tightly coupled to scheduler edits.
- Fix:
  - Split scheduler logic to `scripts/market_alert_scheduler.py`
  - Split runtime state and singleton lock to `scripts/market_alert_runtime_state.py`
  - Split cooldown / dedupe / Telegram send path to `scripts/market_alert_delivery_service.py`
  - Keep `market_alert_worker.py` as a thin orchestrator

### 5. `/look` render file still mixed synoptic phrasing helpers with final assembly

- Problem:
  - `report_render_service.py` still carried background/far-window mechanism sentence selection together with final section assembly.
- Risk:
  - Every phrasing tweak would keep re-expanding the main render file and blur the presentation boundary again.
- Fix:
  - Extracted synoptic/background render helpers to `scripts/report_synoptic_service.py`
  - Kept `report_render_service.py` focused on final report composition

### 6. Market monitor cycle/event-window paths duplicated the same setup pipeline

- Problem:
  - `run_market_monitor_cycle()` and `run_market_monitor_event_window()` duplicated catalog loading, subscription planning and signal inference setup.
- Risk:
  - Future edits to market watch scope or signal inputs could diverge across the two paths.
- Fix:
  - Centralized shared setup in `market_monitor_service.py`
  - `cycle` and `event_window` now share the same subscription/signal helper path

## Remaining Risks

- `peak_range_service.py` is now the clearest remaining complexity hotspot.
- `/look` and proactive alert still share some wording-era assumptions around market labels and report phrasing, although the runtime paths are now separated.
- If per-station alert policy grows beyond cadence/window overrides, it should move into an explicit alert-policy contract instead of more local config sprawl.

## Recommendation

This structural pass completed the previous top three targets:

1. split `market_alert_worker.py` into scheduler/state/delivery pieces
2. continue shrinking `report_render_service.py` into pure presentation helpers
3. keep station-specific external data sources behind adapter modules only

Next pass should focus on:

1. splitting `peak_range_service.py`
2. keeping new alert/report helpers from re-coupling into the orchestrators
3. moving any future per-station alert policy growth behind explicit contracts
