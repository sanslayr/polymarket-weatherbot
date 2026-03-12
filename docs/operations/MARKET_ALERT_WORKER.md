# Market Alert Worker

Last updated: 2026-03-12

## Purpose

Run the proactive market-move notification path inside the `polymarket-weatherbot` workspace.

This worker is separate from `/look`:

- `/look` stays request/response
- `market_alert_worker.py` handles continuous monitoring around routine METAR windows
- Telegram delivery goes through the `weatherbot` account by default

Detailed push contract now lives in:

- [MARKET_ALERT_NOTIFICATION_FLOW.md](/home/ubuntu/.openclaw/workspace/skills/polymarket-weatherbot/docs/core/MARKET_ALERT_NOTIFICATION_FLOW.md)

## Module Boundary

The alert stack currently lives in:

- `scripts/market_metadata_service.py`
- `scripts/market_stream_service.py`
- `scripts/market_monitor_service.py`
- `scripts/market_implied_weather_signal.py`
- `scripts/market_signal_alert_service.py`
- `scripts/alert_delivery_policy.py`
- `scripts/telegram_notifier.py`
- `scripts/market_alert_scheduler.py`
- `scripts/market_alert_runtime_state.py`
- `scripts/market_alert_delivery_service.py`
- `scripts/market_alert_worker.py`

This is the correct place for the abnormal-move notification module when using the weatherbot workspace.

## Runtime Behavior

`market_alert_worker.py` is now a thin orchestrator. The runtime path is:

1. `market_alert_runtime_state.py`
   - acquires singleton lock
   - owns `state.json` / `worker.pid` / `worker.log`
2. `market_alert_scheduler.py`
   - loads `station_links.csv`
   - reads recent METAR observations from the shared `metar24` runtime cache
   - estimates routine reporting cadence
   - identifies the current routine report window, next routine window, or resident monitoring block
3. `market_monitor_service.py`
   - subscribes to the relevant Polymarket buckets during that window
   - keeps the pre-report baseline for routine windows
   - resident mode starts without inherited baseline
   - when the station returns to routine mode, routine pre-report snapshots build a fresh baseline again
   - evaluates both report-window repricing and resident-mode repricing
4. `market_signal_alert_service.py`
   - formats the Telegram text with the `盘口归零异动` title
5. `market_alert_delivery_service.py`
   - applies cooldown / duplicate suppression
   - sends Telegram notifications and records delivery summary

State files:

- runtime state: `cache/runtime/market_alert_worker/state.json`
- worker pid: `cache/runtime/market_alert_worker/worker.pid`
- worker log: `cache/runtime/market_alert_worker/worker.log`

## Required Environment

At least one Telegram target must be configured:

- `TELEGRAM_DIRECT_CHAT_ID`
- `TELEGRAM_ALERT_CHAT_ID`
- `TELEGRAM_CHAT_ID`
- or `TELEGRAM_ALERT_TARGETS` for multiple targets

Token resolution order:

1. `TELEGRAM_BOT_TOKEN`
2. `weatherbot` token in `~/.openclaw/openclaw.json`
3. fallback default token in `~/.openclaw/openclaw.json`

## Optional Environment

- `MARKET_ALERT_MAX_WORKERS`
  - default `12`
- `MARKET_ALERT_COOLDOWN_SECONDS`
  - default `900`
- `MARKET_EVENT_WINDOW_STREAM_SECONDS`
  - default `245`
- `MARKET_EVENT_WINDOW_BASELINE_SECONDS`
  - default `2`
- `MARKET_ALERT_RESIDENT_BLOCK_SECONDS`
  - default `240`
- `MARKET_ALERT_RESIDENT_SPECI_HOURS`
  - default `2`
- `MARKET_ALERT_RESIDENT_ACTIVE_MINUTES`
  - default `90`
- `MARKET_ALERT_RESIDENT_LIKELY_MINUTES`
  - default `45`
- `MARKET_SIGNAL_PRICE_FLOOR`
  - default `0.02`
  - minimum `best_bid` required for a bucket to count as an actionable live level

## Start Command

From the skill root:

```bash
python3 scripts/market_alert_worker.py
```

If you want the weatherbot virtualenv explicitly:

```bash
./.venv_gfs/bin/python scripts/market_alert_worker.py
```

## Verification

Recommended local verification before long-running deployment:

```bash
python3 -m unittest \
  tests/test_alert_delivery_policy.py \
  tests/test_market_signal_alert_service.py \
  tests/test_market_alert_worker_schedule.py \
  tests/test_market_monitor_service.py
```

## Operational Notes

- The worker is designed for proactive alerts, not for rendering `/look` output.
- Routine monitoring starts at the routine report timestamp and ends 4 minutes later.
- Stations with `recent_speci_2h` or elevated `speci_active/speci_likely` state enter resident monitoring blocks even outside the routine window.
- Formal routine-window signal evaluation begins after the post-report `+30s` gate; resident blocks evaluate continuously.
- Resident blocks do not inherit a cross-block baseline; entering resident mode discards any prior baseline state.
- If a resident block would overlap the next routine window, the resident block is clipped so routine monitoring can take over on time.
- If a station resolves to `no_subscriptions` because the current event day has no tradable active market, the worker suppresses that station for the rest of that event day.
- Duplicate suppression is keyed by station + signal type + event URL + bucket identity, not by `scheduled_report_utc` alone.
- By default, notifications prefer direct chat targets before group targets.
- Alert wording must remain probabilistic. Do not phrase alerts as confirmed official observations.
- Telegram market links are sent with preview enabled.
