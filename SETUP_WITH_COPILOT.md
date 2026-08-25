# Set up Copilot Hub with Copilot CLI

Run these commands from the standalone project directory:

```bash
./scripts/setup.sh
./scripts/doctor.sh
./scripts/start.sh
```

Then open `http://127.0.0.1:8767/`.

The setup script:

1. Verifies `uv` and `copilot`.
2. Installs Python and development dependencies.
3. Initializes `~/.copilot-hub/copilot_hub.db`.
4. Adds only the project directory and configured managed working directory to
   Copilot's trusted folders. The home directory is not trusted as a whole.
5. Links the generic `copilot-hub` skill into `~/.copilot/skills`.

## Important trust warning

Manager and Worker sessions use `--yolo --no-ask-user`, with tool confirmations
disabled. They can run commands and modify files available to your account.
Use a trusted working directory, keep the server bound to loopback, review
delegated scope, and do not run the Hub on a shared or untrusted machine.

To override the isolated default port:

```bash
export COPILOT_HUB_PORT=8877
./scripts/start.sh
```

To use a different managed directory:

```bash
export COPILOT_HUB_CWD="$HOME/my-copilot-work"
./scripts/setup.sh
./scripts/start.sh
```

From a managed terminal, restart with:

```bash
./scripts/restart.sh
```

Do not call `stop.sh` from a managed terminal because the server owns that
terminal's PTY.
