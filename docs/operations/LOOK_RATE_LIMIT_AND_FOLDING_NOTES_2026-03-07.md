# /look Group Rate Limit And Folding Notes

Date: 2026-03-07

## Scope

This note records the current `/look` runtime-control design for Telegram group chats, and the decision to defer any message folding behavior.

## Active Runtime Policy

Current config file:

- `config/look_group_policy.json`

Current implementation files:

- `scripts/look_group_policy.py`
- `scripts/look_runtime_control.py`
- `scripts/telegram_report_cli.py`

Default active policy:

- group chats only
- direct messages are not rate-limited
- adaptive per-user cooldown, scoped per group
  - base cooldown: 15 seconds
  - burst step: +15 seconds per extra recent request
  - cooldown cap: 90 seconds
  - look-back window: 180 seconds
- same-query completed-result reuse is disabled while `/look` forces live METAR/Polymarket refresh
- same-query runtime scope: `group-only`
- in-flight wait window: 3 seconds
- stale in-flight cleanup: 120 seconds

## Current Behavior

For Telegram group chats:

1. A user who repeats `/look` too quickly is blocked by an adaptive cooldown, not a fixed 60-second wall.
2. The cooldown is evaluated per sender per group, so one noisy group does not impose a global sender lock across all groups.
3. The adaptive cooldown starts from 15 seconds and steps upward by burst level within a 180-second window, capped at 90 seconds.
4. If the same query is still running, later requests wait briefly and then reuse the completed in-flight result when available.
5. Because `/look` now forces live METAR and live Polymarket refresh, the completed-result reuse window is intentionally disabled for normal follow-up requests.

For direct messages:

1. The runtime-control layer does not apply the above cooldown or shared-result rules.
2. Private `/look` requests continue to run normally.

## Why The Policy Was Refactored

The original single-policy implementation was enough for one group, but not for multi-group operations.

The refactor introduces:

- a default policy document with per-group overrides
- explicit runtime context resolution for channel, peer kind, peer id, and sender id
- configurable shared-result scope

This keeps the current behavior simple while allowing later per-group tuning without rewriting the core controller.

## Group Customization Model

Per-group overrides live under:

- `groups.<telegram_chat_id>`

Supported rate-limit fields:

- `enabled`
- `apply_in_direct`
- `user_cooldown.mode`
- `user_cooldown.scope`
- `user_cooldown.fixed_sec`
- `user_cooldown.base_sec`
- `user_cooldown.step_sec`
- `user_cooldown.max_sec`
- `user_cooldown.window_sec`
- `user_cooldown.burst_soft_limit`
- `result_scope`
- `inflight_wait_sec`
- `inflight_stale_sec`

Supported shared-result scopes:

- `telegram-groups-shared`: allow reuse across Telegram groups
- `group-only`: only reuse inside the same group

## Runtime Context Requirement

Group-aware policy depends on runtime context being available:

- `peer_id`
- `sender_id`
- `session_key`

The controller can infer context from explicit CLI arguments, environment variables, or a compatible `session_key`, but group-specific behavior is only as good as the runtime metadata passed into `telegram_report_cli.py`.

If context is missing, the command still runs, but group-aware controls may degrade to generic/default behavior.

## Folding Discussion

Message folding was discussed and intentionally deferred.

Current decision:

- do not implement Telegram message folding now
- do not add summary artifacts to the runtime-control cache
- do not mix delivery shaping with rate limiting

Reasoning:

1. Telegram does not provide a clean generic "fold/unfold arbitrary bot report" primitive that fits this workflow well.
2. A reliable folding implementation would need a separate delivery layer, usually based on delayed message edit or delete behavior.
3. That design would require storing full-text and summary-text artifacts separately, which is not needed for the current rate-limit goal.

As of this note, the runtime-control layer stores and reuses only the completed full result text.

## Future Revisit Conditions

If folding is revisited later, treat it as a delivery-policy problem, not a rate-limit problem.

Recommended conditions before revisiting:

1. confirm runtime context is consistently available in production
2. separate canonical report output from delivery-specific artifacts
3. define per-group delivery policy independently from rate-limit policy

## Operational Summary

Current production direction is:

- keep `/look` private chats unrestricted
- limit repeated group-chat calls with adaptive per-group sender cooldown
- keep same-query in-flight dedupe inside the same group
- defer message folding until there is a concrete delivery-layer design
