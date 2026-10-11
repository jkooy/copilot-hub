# Set up Copilot Hub with Claude Code

Make sure `claude` starts and is signed in from an ordinary terminal first.

Run these commands from the standalone project directory:

```bash
./scripts/setup.sh
./scripts/doctor.sh
./scripts/start.sh
```

Then open `http://127.0.0.1:8767/`.

The setup script:

1. Verifies `uv` and `claude`.
2. Installs Python and development dependencies.
3. Initializes `~/.copilot-hub/copilot_hub.db`.
4. Marks only the project directory and the configured managed working
   directory as trusted in `~/.claude.json`. The home directory is not trusted
   as a whole.
5. Links the generic `copilot-hub` skill into `~/.claude/skills`.

## Important trust warning

By default, Manager and Worker sessions run with
`--dangerously-skip-permissions`, so tool confirmations are disabled. They can
run commands and modify files available to your account. Use a trusted working
directory, keep the server bound to loopback, review delegated scope, and do not
run the Hub on a shared or untrusted machine.

To keep Claude Code's permission prompts instead:

```bash
export COPILOT_HUB_PERMISSION_MODE=acceptEdits   # or auto, default, plan
./scripts/start.sh
```

To override the isolated default port:

```bash
export COPILOT_HUB_PORT=8877
./scripts/start.sh
```

To use a different managed directory:

```bash
export COPILOT_HUB_CWD="$HOME/my-claude-work"
./scripts/setup.sh
./scripts/start.sh
```

From a managed terminal, restart with:

```bash
./scripts/restart.sh
```

Do not call `stop.sh` from a managed terminal because the server owns that
terminal's PTY.
