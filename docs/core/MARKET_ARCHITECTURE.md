# Market Architecture

Last updated: 2026-03-09

## Goal

Build a market branch that can:

- support current report-time market display
- support future continuous price monitoring
- support multiple trading strategies
- remain decoupled from weather inference

Detailed rollout and contract planning lives in [MARKET_TRADING_PLAN.md](/home/ubuntu/.openclaw/workspace/skills/polymarket-weatherbot/docs/core/MARKET_TRADING_PLAN.md).

## Current State

The runtime market path is still display-oriented:

- `scripts/polymarket_client.py`
  - fetches Gamma event/market metadata with short TTL cache
- `scripts/polymarket_render_service.py`
  - parses temperature buckets and renders report-time market section
- `scripts/market_label_policy.py`
  - gates display labels such as best/alpha-style markers

This is enough for `/look`, but not enough for:

- live order book monitoring
- continuous edge detection
- multi-strategy execution
- portfolio/risk management

At the same time, a first proactive alert branch now exists inside the weatherbot workspace:

- `scripts/market_metadata_service.py`
- `scripts/market_stream_service.py`
- `scripts/market_monitor_service.py`
- `scripts/market_implied_weather_signal.py`
- `scripts/market_signal_alert_service.py`
- `scripts/telegram_notifier.py`
- `scripts/market_alert_worker.py`

That branch is intentionally alert-only for now:

- monitor report-window repricing
- infer market-implied observation hints
- push short Telegram notifications

It should remain separate from `/look` rendering and from any future execution layer.

## Target Market Mainline

The market branch should evolve into:

1. `market_metadata_service`
   - resolve `event_slug -> markets -> condition_id -> clob token ids`
   - source of truth for bucket/token mapping

2. `market_stream_service`
   - subscribe to Polymarket CLOB market websocket
   - maintain reconnect, heartbeat, subscription and staleness handling

3. `market_state_store`
   - keep current best bid/ask, last trade, top-of-book depth, resolution state
   - expose a read-optimized snapshot for reports, alerts and strategies

4. `market_feature_service`
   - derive structured market features:
     - `mid`
     - `spread`
     - `depth_top1`
     - `depth_top3`
     - `imbalance`
     - `microprice`
     - `short_horizon_momentum`
     - `staleness_ms`

5. `opportunity_service`
   - compare `weather_posterior` vs market-implied pricing
   - output:
     - `fair_value`
     - `edge`
     - `mispricing_score`
     - `tradability_score`

6. `strategy_layer`
   - map opportunities to standardized strategy intents
   - multiple strategies should plug in here without changing execution code

7. `execution_adapter`
   - translate strategy intents into concrete market actions
   - place/cancel/replace/status reconciliation

8. `portfolio_risk_service`
   - position caps
   - market caps
   - correlated exposure control
   - circuit breakers / cool-down

## How It Connects To Weather

Weather and market should remain separate until the opportunity layer:

- weather branch:
  - `canonical_raw_state`
  - `posterior_feature_vector`
  - `quality_snapshot`
  - `weather_posterior`

- market branch:
  - `market_metadata_service`
  - `market_stream_service`
  - `market_state_store`
  - `market_feature_service`

- fusion:
  - `opportunity_service`
  - `strategy_layer`
  - `execution_adapter`
  - `portfolio_risk_service`

This preserves a clean rule:

- weather inference does not depend on market state
- strategies consume weather posterior, not report text

## CLI vs Websocket

For this project, Polymarket CLI is useful, but it should not become the core live-state layer.

Recommended positioning:

- websocket: primary path for live market monitoring
  - lowest-latency source for books, best bid/ask and recent market changes
  - should feed `market_state_store`

- CLI: operator/agent execution and diagnostics adapter
  - better suited for:
    - manual or agent-driven order placement
    - cancel/replace flows
    - open-order inspection
    - account/order diagnostics
  - fits OpenClaw/agent workflows well because command I/O is explicit and auditable

In short:

- use websocket for continuous market state
- use CLI as a controlled execution surface or operational tool

## Suggested Incremental Implementation

1. keep current report-time Gamma fetch path
2. add `market_metadata_service`
3. add websocket-backed `market_stream_service`
4. add `market_state_store`
5. add `market_feature_service`
6. connect `weather_posterior` to a new `opportunity_service`
7. add `strategy_layer`
8. add CLI-backed `execution_adapter`
9. add portfolio/risk service

## External References

- Polymarket CLOB websocket overview:
  - https://docs.polymarket.com/market-data/websocket/overview
- Polymarket market channel:
  - https://docs.polymarket.com/market-data/websocket/market-channel
- Polymarket clients / SDKs:
  - https://docs.polymarket.com/developers/CLOB/clients
- Polymarket agents repository:
  - https://github.com/Polymarket/agents
