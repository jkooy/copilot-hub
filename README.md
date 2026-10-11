# Copilot Hub

Copilot Hub is a local personal workspace for persistent Claude Code sessions.
It keeps one Manager and a configurable pool of Workers alive behind
a browser terminal UI, stores task and result state in SQLite, and recovers
session ownership after server restarts.

The Manager handles ordinary conversation and read-only questions about the
local Hub. It delegates implementation, commands, tool use, and long-running
tasks to Workers. Worker results are durably queued back into the Manager and
also remain available in the Results panel.

## Worker types

The Manager routes each task to a worker type with `task_type`:

| Type | Worker | Used for |
| --- | --- | --- |
| `code` | coder | implementing features and fixes, writing and running tests |
| `research` | researcher | investigating code, docs, logs, and data without changing them |
| `review` | reviewer | reviewing changes and designs for bugs, security issues, test gaps |
| `operate` | operator | running commands, builds, deployments, environment setup |

A task goes to an idle worker of its type, otherwise a new worker of that type
is started (named `coder-1`, `reviewer-2`, ...), up to the worker limit. At the
limit, an idle worker of another type takes the task; the worker prompt always
carries the role for the task. Independent tasks run in parallel. Workers can
also be added by type from the Sessions panel.

The Manager session's system prompt tells it to delegate hands-on work and how:

```bash
copilot-hub dispatch --task-type research --title "Survey tests" --prompt - <<'TASK'
Self-contained instructions for the worker.
TASK
```

Some Claude Code distributions run shell commands in a sandbox that blocks
loopback connections to the Hub server. Inside a managed session,
`dispatch` therefore queues the request as a file in
`$COPILOT_HUB_STATE_DIR/inbox/`, which the server polls; `status` reads the
database directly. Worker results are delivered back into the Manager terminal.

## Daily budget

The left rail shows today's spend against `COPILOT_HUB_DAILY_BUDGET_USD`
(default `$300`), what remains, and how much of it the Hub's own sessions used;
each session shows its spend and tokens for today.

By default spend covers the Hub's own sessions, priced from their transcripts'
token counts at list prices (Opus 5.5 $4/$20 per MTok input/output, Sonnet 5.5
$2/$10, Haiku 4.5 $1/$5; cache reads and 1.25x cache writes included). To count
every Claude Code request on the machine, or to use billed amounts, point
`COPILOT_HUB_SPEND_LEDGER` at a ledger plugin, `path/to/file.py:ClassName` or
`package.module:ClassName`. A ledger has a `label` shown in the UI,
`available()`, and `requests_between(start, end)` returning
`copilot_hub.costs.Request` records with each request's Claude session ID, so
spend is attributed to Hub sessions. If the plugin fails to load, the Hub falls
back to transcripts. The day starts at midnight in
`COPILOT_HUB_BUDGET_TIMEZONE` (default: this machine's time zone). The budget is
shown, not enforced.

## Surviving Claude Code updates

The Hub types into Claude Code's terminal UI, so an update that adds a startup
dialog can swallow dispatched prompts. Two safeguards:

- Startup dialogs the Hub knows (folder trust, bypass permissions, the "make
  auto mode your default" prompt) are answered without changing user defaults.
  An unrecognized selection dialog drawn over a live prompt box is dismissed
  with Esc and logged to `server.log`. Dialogs before the prompt box exists are
  left alone, since Esc there can quit Claude; readiness errors include the end
  of the terminal screen.
- When `claude --version` differs from the last version that passed, the Hub
  pastes a marker into an idle worker's prompt box at startup, confirms Claude
  draws it, and clears it. Nothing is submitted to the model. A failure shows a
  banner above the terminal with the screen contents and a re-run button
  (`POST /api/control/self-check`).

## How Claude Code is driven

Each Manager and Worker is an interactive `claude` process in a PTY, launched
with a fixed `--session-id` (or `--resume` once a transcript exists), the Hub
instructions via `--append-system-prompt`, and a `--settings` document that
registers Claude Code hooks (`SessionStart`, `UserPromptSubmit`, `PreToolUse`,
`PostToolUse`, `Stop`, `SessionEnd`, ...). The hooks run
`copilot_hub/claude_events.py`, which appends an event log per session under
`$COPILOT_HUB_STATE_DIR/sessions/<session-id>/events.jsonl`. The Hub uses that
log, not screen scraping, to tell when a prompt was accepted, when a turn
finished, what the final answer was, and whether the session is waiting for
input. Turns interrupted with Esc are detected from the Claude transcript.

Hooks must be allowed. If an administrator policy sets `disableAllHooks` or
allows managed hooks only, sessions never become ready and the Hub reports
that `SessionStart` was never received.

When the Hub is started from inside a Claude Code session, the parent session's
`CLAUDECODE`, `*CLAUDE_CODE_*`, and proxy variables are
removed from managed sessions so each one starts its own Claude Code runtime.

## Trust model

By default managed sessions run with `--dangerously-skip-permissions`
(`COPILOT_HUB_PERMISSION_MODE=bypassPermissions`). This gives each session
permission to use tools without interactive confirmation. Set
`COPILOT_HUB_PERMISSION_MODE` to `acceptEdits`, `auto`, `default`, or `plan` to
keep Claude Code's permission prompts; a pending prompt marks the session as
needing input in the UI, and you answer it in the browser terminal. Only run the
Hub on a trusted machine, use a trusted working directory, review the Manager's
delegation, and do not expose the server outside loopback unless you understand
the risk. The browser mutation APIs enforce same-origin requests, but the Hub
does not provide multi-user authentication.

## Requirements

- Claude Code (`claude`), already set up and signed in
- `uv`
- `curl` for scripts and health checks
- A browser

Python dependencies are installed into the project environment by `uv`.

The project is standalone, but not an offline bundle. It still requires an
installed and authenticated Claude Code plus the Python dependencies resolved
by `uv`. Runtime state is created outside the source folder under
`~/.copilot-hub`, `~/.claude`, and the configured managed workspace. Linux and
macOS are supported.

The Hub is not a sandbox. Managed sessions run with the current user's operating
system permissions and can access files that account can access. Setup marks
only the Hub project and managed workspace as trusted in `~/.claude.json`; it
does not trust the whole home directory.

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
| `COPILOT_HUB_CLAUDE` | `claude` | Claude Code executable |
| `COPILOT_HUB_PERMISSION_MODE` | `bypassPermissions` | Claude Code permission mode for managed sessions |
| `COPILOT_HUB_CWD` | `~/copilot_hub_workspace` | Managed session directory |
| `COPILOT_HUB_MODEL` | `opus` | Session model (`--model`); the `opus` alias follows the newest Opus |
| `COPILOT_HUB_EFFORT` | `xhigh` | Effort (`--effort`: low, medium, high, xhigh, max) |
| `COPILOT_HUB_CONTEXT` | `standard` | `long_context` appends `[1m]` to the model |
| `COPILOT_HUB_MAX_WORKERS` | `7` | Worker cap, excluding Manager |
| `COPILOT_HUB_DAILY_BUDGET_USD` | `300` | Daily spend budget shown in the UI |
| `COPILOT_HUB_BUDGET_TIMEZONE` | local | IANA time zone where the budget day starts, e.g. `America/Los_Angeles` |
| `COPILOT_HUB_SPEND_LEDGER` | unset | Spend ledger plugin, `path/to/file.py:ClassName` |
| `COPILOT_HUB_MANAGER_SESSION_ID` | unset | Resume a chosen Manager session |
| `COPILOT_HUB_TERMINALS` | `1` | Enable managed browser terminals |

Settings can also live in `$COPILOT_HUB_STATE_DIR/hub.env` as `KEY=VALUE`
lines, which keeps machine-specific values out of the repository; variables set
in the environment take precedence.

The saved worker limit in the UI takes precedence over the environment default.
Workers are created lazily and may also be created or retired from the Sessions
panel. The model, effort, and context tier are stored per session when it is
created, so changing these variables affects new sessions only.

## Durable behavior

- Each Worker and the Manager retain a stable Claude Code session ID. Running
  `/clear` in a managed terminal starts a new Claude conversation; the Hub
  follows it and resumes that conversation after a restart.
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
  and token usage recorded from Claude Code transcripts.

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

To check the integration against the real `claude` CLI (one short Haiku turn,
throwaway state and workspace, default permission mode):

```bash
uv run python scripts/smoke_test_claude.py
```

The project is standalone and does not require a Git checkout.
