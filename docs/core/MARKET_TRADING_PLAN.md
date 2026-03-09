# Polymarket Trading Plan

Last updated: 2026-03-09

## Goal

Build a market/trading branch that can evolve in stages:

1. report-time market display
2. continuous price monitoring
3. opportunity detection
4. multi-strategy decisioning
5. controlled execution
6. portfolio/risk control

This branch should stay cleanly separated from weather inference. Weather produces `weather_posterior`; market/trading consumes it.

## Design Principles

- Weather and market remain independent until `opportunity_service`.
- Live market state should come from websocket, not repeated report-time polling.
- CLI/agent tooling is useful for explicit operator actions, but should not replace the streaming market-state path.
- Strategy logic, execution logic, and portfolio/risk logic must remain separate.
- Every cross-layer handoff should be structural and machine-friendly, not report-text-driven.

## Current Baseline

The repo currently has only a display-oriented market path:

- `scripts/polymarket_client.py`
  - fetches Gamma event metadata for `/look`
- `scripts/polymarket_render_service.py`
  - renders report-time bucket display
- `scripts/market_label_policy.py`
  - gates report labels

This is enough for report display, but not enough for:

- continuous price monitoring
- market microstructure features
- mispricing detection
- multi-strategy operation
- account/order reconciliation

## External Interface Assumptions

Planning is based on the current Polymarket public documentation:

- Gamma metadata remains the source for `event -> markets -> token ids`
- CLOB market websocket remains the source for live market/book events
- client/agent tooling can be used for explicit order actions and diagnostics

Official references:

- Gamma market structure:
  - https://docs.polymarket.com/developers/gamma-markets-api/gamma-structure
- CLOB websocket overview:
  - https://docs.polymarket.com/market-data/websocket/overview
- Market websocket channel:
  - https://docs.polymarket.com/market-data/websocket/market-channel
- Client libraries / SDKs:
  - https://docs.polymarket.com/developers/CLOB/clients
- Polymarket agents repository:
  - https://github.com/Polymarket/agents

## Target Mainline

```mermaid
flowchart LR
  A[Gamma Metadata] --> B[market_metadata_service]
  C[CLOB WebSocket] --> D[market_stream_service]
  B --> E[market_state_store]
  D --> E
  E --> F[market_feature_service]
  G[weather_posterior] --> H[opportunity_service]
  F --> H
  H --> I[strategy_layer]
  I --> J[execution_adapter]
  J --> K[portfolio_risk_service]
  K --> J
  E --> L[/look market render]
```

## Proposed Layers

### 1) `market_metadata_service`

Responsibility:

- resolve `event_slug -> event -> markets -> condition_id -> token ids`
- normalize bucket metadata
- keep report-time bucket display and execution-layer market mapping consistent

Key output:

- `market_catalog_snapshot`

Suggested fields:

- `event_slug`
- `event_id`
- `condition_id`
- `market_slug`
- `bucket_label`
- `temperature_bucket`
- `yes_token_id`
- `no_token_id`
- `resolved`
- `close_time`

### 2) `market_stream_service`

Responsibility:

- hold websocket connection
- subscribe/unsubscribe token sets
- normalize incoming market events
- handle reconnect, heartbeat, staleness

Key output:

- append-only normalized market event stream

Important rule:

- this service should not compute strategy decisions
- it only maintains clean transport and normalization

### 3) `market_state_store`

Responsibility:

- maintain latest per-token market state
- expose read-optimized snapshots

Suggested fields:

- `best_bid`
- `best_ask`
- `mid`
- `last_trade_price`
- `top_depth_bid`
- `top_depth_ask`
- `book_timestamp`
- `trade_timestamp`
- `staleness_ms`
- `resolved`

### 4) `market_feature_service`

Responsibility:

- derive structured market features from market state

Suggested fields:

- `mid`
- `spread`
- `spread_bps`
- `depth_top1`
- `depth_top3`
- `imbalance`
- `microprice`
- `trade_momentum_1m`
- `trade_momentum_5m`
- `realized_vol_5m`
- `staleness_ms`
- `tradability_score`

This layer should be reusable by:

- `/look` market section
- monitor/alerting
- strategy backtests
- live execution

### 5) `opportunity_service`

Responsibility:

- combine `weather_posterior` and `market_feature_vector`
- produce machine-readable edge assessment

Suggested fields:

- `fair_value_yes`
- `fair_value_no`
- `market_mid_yes`
- `edge_yes`
- `edge_no`
- `mispricing_score`
- `tradability_score`
- `confidence_score`
- `opportunity_state`

Important rule:

- weather posterior remains the weather source of truth
- market data only affects opportunity ranking and execution, not weather inference

### 6) `strategy_layer`

Responsibility:

- host multiple strategies without changing the execution code path

Suggested plugin model:

- `value_reversion_strategy`
- `window_breakout_strategy`
- `late_lock_strategy`
- `event_transition_strategy`
- `maker_liquidity_strategy`

Standardized strategy output:

- `strategy_id`
- `market_slug`
- `bucket_id`
- `side`
- `thesis`
- `fair_value`
- `target_entry`
- `max_price`
- `min_edge`
- `size`
- `urgency`
- `ttl_seconds`
- `cancel_if`
- `risk_tags`

### 7) `execution_adapter`

Responsibility:

- convert strategy intent into explicit market actions
- reconcile order lifecycle

Recommended positioning:

- websocket remains the live-state path
- CLI/client tooling is used here for explicit actions and diagnostics

Execution modes:

- `observe_only`
- `alert_only`
- `manual_assist`
- `semi_auto`
- `full_auto`

### 8) `portfolio_risk_service`

Responsibility:

- enforce cross-market and cross-strategy risk controls

Suggested controls:

- max gross exposure
- max net exposure by event
- per-station limit
- correlated weather basket limit
- stale-market block
- low-confidence-weather block
- drawdown/cooldown circuit breaker

## Core Contracts To Add

### A. `market-raw-state.v1`

This should be the market-side equivalent of `canonical_raw_state`.

Suggested structure:

```json
{
  "schema_version": "market-raw-state.v1",
  "meta": {},
  "catalog": {},
  "books": {},
  "trades": {},
  "resolution": {},
  "source": {}
}
```

### B. `market-feature-vector.v1`

Machine-friendly per-market features only.

Suggested structure:

```json
{
  "schema_version": "market-feature-vector.v1",
  "meta": {},
  "microstructure": {},
  "liquidity": {},
  "momentum": {},
  "quality": {}
}
```

### C. `opportunity-snapshot.v1`

Fusion output from weather + market.

Suggested structure:

```json
{
  "schema_version": "opportunity-snapshot.v1",
  "meta": {},
  "weather": {},
  "market": {},
  "edge": {},
  "quality": {}
}
```

### D. `strategy-intent.v1`

Unified handoff from strategy to execution.

Suggested structure:

```json
{
  "schema_version": "strategy-intent.v1",
  "strategy_id": "",
  "market_slug": "",
  "bucket_id": "",
  "side": "",
  "target_entry": null,
  "max_price": null,
  "size": null,
  "ttl_seconds": null,
  "cancel_if": [],
  "risk_tags": []
}
```

## CLI vs Websocket Positioning

For this project:

- websocket should own continuous market monitoring
- CLI/client tooling should own explicit order and account operations

This split works well for agent environments:

- websocket keeps the system stateful and reactive
- CLI/client calls keep execution explicit, auditable and easier to gate

The important rule is:

- do not make `/look` responsible for long-running websocket state
- market monitoring should run as its own service or runtime worker

## Suggested Incremental Rollout

### Phase 1: Observe

- add `market_metadata_service`
- add `market_stream_service`
- add `market_state_store`
- build a local monitor that watches selected temperature markets

Deliverable:

- real-time price monitoring without any trading

### Phase 2: Quantify

- add `market_feature_service`
- add `opportunity_service`
- compare `weather_posterior` with market mid pricing

Deliverable:

- machine-readable mispricing and alerting

### Phase 3: Strategize

- add `strategy_layer`
- start with one or two constrained strategies
- evaluate by paper trading first

Deliverable:

- standardized strategy intents

### Phase 4: Execute

- add `execution_adapter`
- add order lifecycle tracking
- support operator-confirmed actions first

Deliverable:

- manual-assist or semi-auto execution

### Phase 5: Control

- add `portfolio_risk_service`
- add global exposure limits and automated guards
- add post-trade review hooks

Deliverable:

- live-trading-ready control surface

## What To Avoid

- do not let report text drive trading logic
- do not mix strategy logic into websocket transport
- do not couple weather inference to market state
- do not put all strategies directly in the execution adapter
- do not make runtime depend on historical training code or notebooks

## Recommended Next Step

Implement in this order:

1. `market_metadata_service`
2. `market_raw_state.v1`
3. `market_stream_service`
4. `market_state_store`
5. `market_feature_vector.v1`
6. `opportunity_service`

This creates a complete observe-and-rank loop before execution is added.
