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
- per-user cooldown: 60 seconds
- same-query shared result reuse: 120 seconds
- same-query shared result scope: `telegram-groups-shared`
- in-flight wait window: 20 seconds
- stale in-flight cleanup: 120 seconds

## Current Behavior

For Telegram group chats:

1. A user who repeats `/look` too quickly is blocked by a 60-second user cooldown.
2. If the same `/look` query was already completed within 120 seconds, the previous result is reused instead of recomputed.
3. By default, completed results can be reused across Telegram groups for the same station/date query during the 120-second reuse window.
4. If the same query is still running, later requests wait briefly and then reuse the completed result when available.

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
- `user_cooldown_sec`
- `user_cooldown_scope`
- `shared_result_ttl_sec`
- `shared_result_scope`
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
- limit repeated group-chat calls
- reuse the same recent result instead of recomputing
- defer message folding until there is a concrete delivery-layer design
