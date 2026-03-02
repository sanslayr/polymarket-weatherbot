# Forecast Pipeline Static Review (2026-02-28)

## 1) Module Dependency Graph (current)

- `telegram_report_cli.py`
  - orchestration + command parsing + rendering
  - imports: `forecast_pipeline.load_or_build_forecast_decision`
  - no direct diagnostics imports
- `forecast_pipeline.py`
  - decision build + cache + anchor scheduling
  - imports diagnostics + `vertical_3d` + `synoptic_regime`
  - calls `run_synoptic_fn` callback (provided by CLI wrapper -> synoptic runner)
- `synoptic_runner.py`
  - synoptic pull/detect/cache merge
  - runs `build_2d_grid_payload.py` + `synoptic_2d_detector.py`
- `build_2d_grid_payload.py`
  - Open-Meteo grid fetch + row cache + 429 breaker hard-stop
- `vertical_3d.py`
  - 3D object assembly from synoptic systems

## 2) Closure Status

### Completed
- choose_section_text -> render-only path.
- Synoptic heavy logic extracted from CLI into `synoptic_runner.py`.
- 429 hard short-circuit in both hourly and grid paths.
- Retry-After respected for breaker duration.

### Remaining technical debt
- `run_synoptic_fn` callback wiring still links CLI->pipeline (acceptable but not fully inverted).
- `synoptic_runner` still subprocess-based; in-process call path not yet done.

## 3) Static audit checks (no network)

- `telegram_report_cli.py`: diagnostics imports removed.
- py_compile passes for:
  - `telegram_report_cli.py`
  - `forecast_pipeline.py`
  - `synoptic_runner.py`
  - `build_2d_grid_payload.py`

## 4) Runtime risk notes

- Even with anchor limiting and two-pass domains, sync cold-runs can still hit SIGTERM under heavy 429/timeout periods.
- Primary bottleneck remains synoptic subprocess chain (build+detect+history), not render path.

## 5) Recommended final cleanup sequence

1. Move `run_synoptic_section` wrapper call site into a dedicated ingest module (reduce CLI role to pure shell).
2. Add forecast hard budget cutoff (e.g., 30s global).
3. Shift synoptic detector path from subprocess JSON IO to in-process function calls.
