# Copilot Hub

Copilot Hub is a local personal workspace for persistent GitHub Copilot CLI
sessions. It keeps one Manager and a configurable pool of Workers alive behind
a browser terminal UI, stores task and result state in SQLite, and recovers
session ownership after server restarts.

The Manager handles ordinary conversation and read-only questions about the
local Hub. It delegates implementation, commands, tool use, and long-running
tasks to Workers. Worker results are durably queued back into the Manager and
also remain available in the Results panel.

## Trust model

Managed Copilot sessions run with `--yolo` and `--no-ask-user`. This gives each
session permission to use tools without interactive confirmation. Only run the
Hub on a trusted machine, use a trusted working directory, review the Manager's
delegation, and do not expose the server outside loopback unless you understand
the risk. The browser mutation APIs enforce same-origin requests, but the Hub
does not provide multi-user authentication.

## Requirements

- GitHub Copilot CLI (`copilot`)
- `uv`
- `curl` for scripts and health checks
- A browser

Python dependencies are installed into the project environment by `uv`.

The project is standalone, but not an offline bundle. It still requires an
installed and authenticated Copilot CLI plus the public Python dependencies
resolved by `uv`. Runtime state is created outside the source folder under
`~/.copilot-hub`, `~/.copilot`, and the configured managed workspace.

The Hub is not a sandbox. Managed sessions run with the current user's operating
system permissions and can access files that account can access. Setup adds only
the Hub project and managed workspace to Copilot's trusted folders; it does not
trust the whole home directory.

## Install and run

```bash
./scripts/setup.sh
./scripts/doctor.sh
./scripts/start.sh
```

Open `http://127.0.0.1:8767/`, or run:

```bash
uv run copilot-hub open
```

Stop from an ordinary shell:

```bash
./scripts/stop.sh
```

From a Hub-owned terminal, use `./scripts/restart.sh`. The detached restart
controller waits for the current interaction to reach a terminal state, stops
only the generation-fenced Hub process, starts one replacement, and verifies
its health.

## CLI

```text
copilot-hub init
copilot-hub serve [--host HOST] [--port PORT]
copilot-hub dispatch --title TITLE --prompt PROMPT
                     [--task-type TYPE] [--requires-approval]
copilot-hub status
copilot-hub restart
copilot-hub open
```

## Configuration

| Variable | Default | Purpose |
| --- | --- | --- |
| `COPILOT_HUB_STATE_DIR` | `~/.copilot-hub` | Persistent local state |
| `COPILOT_HUB_DB` | `$STATE_DIR/copilot_hub.db` | SQLite database |
| `COPILOT_HUB_LOG_DIR` | `$STATE_DIR/logs` | Task logs |
| `COPILOT_HUB_HOST` | `127.0.0.1` | Bind host |
| `COPILOT_HUB_PORT` | `8767` | Bind port |
| `COPILOT_HUB_ALLOW_REMOTE` | `0` | Explicitly allow non-loopback binding |
| `COPILOT_HUB_COPILOT` | `copilot` | Copilot CLI executable |
| `COPILOT_HUB_CWD` | `~/copilot_hub_workspace` | Managed session directory |
| `COPILOT_HUB_MODEL` | `gpt-5.6-sol` | Session model |
| `COPILOT_HUB_EFFORT` | `xhigh` | Reasoning effort |
| `COPILOT_HUB_CONTEXT` | `long_context` | Context tier |
| `COPILOT_HUB_MAX_WORKERS` | `7` | Worker cap, excluding Manager |
| `COPILOT_HUB_MANAGER_SESSION_ID` | unset | Resume a chosen Manager session |
| `COPILOT_HUB_TERMINALS` | `1` | Enable managed browser terminals |

The saved worker limit in the UI takes precedence over the environment default.
Workers are created lazily and may also be created or retired from the Sessions
panel. Setup adds only the project root and the configured managed session
directory to Copilot's trusted folders; it does not trust the entire home
directory.

## Durable behavior

- Each Worker and the Manager retain a stable Copilot session ID.
- PTY output has a stream ID and byte offset so reconnects resume without
  duplicating output.
- Browser drafts and unacknowledged terminal input remain associated with the
  selected session and are resent idempotently after reconnect. Accepted input
  sequences are committed to SQLite before prompt or direct-activity side
  effects and before the PTY write, providing at-most-once acceptance across a
  server restart. A crash in the commit-to-write gap can drop input; perfect
  exactly-once delivery is impossible across the SQLite and PTY boundary.
- Task dispatch, terminal replacement, direct activity, recovery, and input
  ownership use persisted generation fences.
- Worker result content is quoted as untrusted data during automatic Manager
  delivery. That delivery cannot use tools or delegate more work.
- Result delivery retries reuse the same durable Manager task.
- Restart handoffs record process identity, server generation, initiators, and
  terminal completion before replacing the server.
- The Results, Handoffs, Memory, and Usage views are derived from local SQLite
  and Copilot session telemetry.

## HTTP surface

- `GET /` — personal workspace
- `GET /api/health` — server generation and process health
- `GET /api/agents/state` — workers, tasks, active work, reporting
- `POST /api/agents/chat` — Manager conversation
- `POST /api/agents/tasks` — generic task dispatch
- `GET /api/agents/tasks/{id}` — task and event details
- `GET /api/agents/results` — durable result inbox
- `POST /api/control/restart` — generation-fenced restart handoff
- `WS /ws/agents/terminal/{worker_id}` — terminal replay and input

## Development

```bash
uv run ruff check .
uv run pytest -q
```

The project is standalone and does not require a Git checkout.
