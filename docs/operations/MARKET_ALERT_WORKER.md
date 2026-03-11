# Market Alert Worker

Last updated: 2026-03-10

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
- `scripts/market_alert_worker.py`

This is the correct place for the abnormal-move notification module when using the weatherbot workspace.

## Runtime Behavior

`market_alert_worker.py` loops over stations from `station_links.csv` and:

1. fetches recent METAR observations
2. estimates each station's routine reporting cadence
3. identifies the current or next report-time monitoring window
4. subscribes to the relevant Polymarket buckets during that window
5. detects market-implied report signals
6. formats a short alert message
7. sends Telegram notifications with duplicate suppression

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
- The monitoring window starts at the routine report timestamp and ends 4 minutes later.
- Duplicate suppression is keyed by station + signal type + scheduled report timestamp + bucket.
- By default, notifications prefer direct chat targets before group targets.
- Alert wording must remain probabilistic. Do not phrase alerts as confirmed official observations.
