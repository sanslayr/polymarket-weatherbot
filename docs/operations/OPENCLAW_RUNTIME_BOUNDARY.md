# OpenClaw Runtime Boundary

Last updated: 2026-03-11

## Goal

Keep the Telegram gateway runtime on this host single-sourced and operationally clear.

## Canonical Runtime

- Package root: `/home/ubuntu/.npm-global/lib/node_modules/openclaw`
- User-facing CLI entry: `/home/ubuntu/.local/bin/openclaw`
- Backing executable: `/home/ubuntu/.npm-global/bin/openclaw`
- Gateway unit: `/home/ubuntu/.config/systemd/user/openclaw-gateway.service`
- Bound ports: `127.0.0.1:18789` and `127.0.0.1:18791`

This is the only supported OpenClaw runtime on the machine.

## Required Boundary

- Telegram ingress and tool dispatch belong to OpenClaw gateway.
- `/look` weather analysis, METAR refresh, and Polymarket refresh belong to the weatherbot runtime in this repository.
- Weatherbot may evolve its own scripts and contracts, but it should not depend on a second OpenClaw install tree.

## Disallowed States

- A second `openclaw-gateway.service` under `/etc/systemd/system`
- A second active OpenClaw install under `/usr/lib/node_modules/openclaw`
- Running system-level and user-level gateway services at the same time
- One-off hot patches applied to an inactive install tree while the live service uses another tree

## Upgrade Workflow

1. Run `npm install -g openclaw@<version>` as user `ubuntu`.
2. Confirm `npm config get prefix` returns `/home/ubuntu/.npm-global`.
3. Reinstall the user service with `openclaw gateway install --force`.
4. Restart with `systemctl --user restart openclaw-gateway.service`.
5. Verify:
   - `command -v openclaw`
   - `openclaw --version`
   - `systemctl --user status openclaw-gateway.service --no-pager`
   - `ss -ltnp | rg '18789|18791'`

## Operational Guardrail

If `/look` latency or behavior regresses, debug the active user service first. Do not patch `/usr/lib/node_modules/openclaw` or revive a system-level gateway as a shortcut.
