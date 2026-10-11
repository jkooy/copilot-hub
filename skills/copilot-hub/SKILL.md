---
name: copilot-hub
description: Install, configure, operate, diagnose, and restart the local personal Copilot Hub.
---

# Copilot Hub

Use this skill for the local personal Copilot Hub.

## Project and state

- Project: resolve the directory containing `pyproject.toml` with project name
  `copilot-hub`.
- State: `${COPILOT_HUB_STATE_DIR:-$HOME/.copilot-hub}`
- Default URL: `http://127.0.0.1:8767/`
- Default managed workspace: `~/copilot_hub_workspace`

## Commands

```bash
./scripts/setup.sh
./scripts/start.sh
./scripts/stop.sh
./scripts/restart.sh
./scripts/doctor.sh
```

Use `restart.sh` from a Manager or Worker terminal. Never synchronously stop the
server that owns the current PTY.

Managed sessions are Claude Code processes. By default they use
`--dangerously-skip-permissions` (`COPILOT_HUB_PERMISSION_MODE=bypassPermissions`);
explain that they can run tools without interactive confirmation before
enabling the Hub for a new user. Session event logs written by the Hub's Claude
Code hooks live under `${COPILOT_HUB_STATE_DIR:-$HOME/.copilot-hub}/sessions`, and
hook failures are logged to `logs/claude-hooks.log` in the same state directory.

## Verification

```bash
uv run ruff check .
uv run pytest -q
curl --fail --silent http://127.0.0.1:8767/api/health
```
