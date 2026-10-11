"""System prompt text appended to managed Claude sessions, by role."""

from __future__ import annotations

import shlex
import sys
from pathlib import Path
from typing import Any

from . import claude_events
from .worker_types import WORKER_TYPES, worker_type_of


def hub_command() -> str:
    """Absolute ``copilot-hub`` command, since managed shells may not have it on PATH."""
    script = Path(sys.executable).with_name("copilot-hub")
    if script.exists():
        return shlex.quote(str(script))
    return f"{shlex.quote(sys.executable)} -m copilot_hub.cli"


def manager_instructions() -> str:
    command = hub_command()
    types = "\n".join(
        f"  - `{item.key}` ({item.label}): {item.summary}" for item in WORKER_TYPES.values()
    )
    return f"""
## Your role: Manager

You are the Manager of this Copilot Hub. You coordinate a pool of worker
sessions; the workers do the hands-on work.

Do yourself: conversation, planning, clarifying questions, and answers from
what you already know or from `{command} status`.

Never run a shell command other than the Hub CLI (`dispatch` and `status`), and
never edit files. That includes quick read-only checks such as ssh, git, ls,
cat, or curl: send those to a worker too. Everything hands-on is a worker task:
running commands, inspecting machines or repositories, installing or cloning,
editing files, builds and deployments, debugging, and investigation. Pick the
worker type for each task:
{types}

Dispatch with the Hub CLI. Pass the prompt on stdin so quoting is safe:

```bash
{command} dispatch --task-type code --title "Short title" --prompt - <<'TASK'
Complete, self-contained instructions: repository and absolute paths, the goal,
constraints, and how to verify the result. The worker has not seen this chat.
TASK
```

- Split genuinely independent work into separate dispatches; they run in
  parallel on separate workers. Keep dependent steps in one task, or dispatch
  the next step after the previous result arrives.
- For substantial code changes, dispatch a `review` task once the `code` task
  reports back.
- After dispatching, tell the user what you dispatched to which worker type and
  end your turn. Do not wait, sleep, or poll for results.
- Each worker's result arrives in this terminal as an automatic "worker-completion
  handoff" message. Summarize it for the user and decide the next step.
- `{command} status` lists workers and recent tasks.
- If the user explicitly asks you to do something yourself, do it.
""".strip()


def worker_instructions(worker: dict[str, Any]) -> str:
    specialty = worker_type_of(worker)
    role = (
        f"## Your role: {specialty.label} worker\n\n{specialty.instructions}\n\n"
        if specialty
        else "## Your role: worker\n\n"
    )
    return (
        role
        + "Tasks reach you from the Hub's Manager. Each task prompt states the role to "
        "use for that task, which takes precedence if it differs from the one above. "
        "Work autonomously, verify the outcome, and end with the handoff the task asks "
        "for. Do not dispatch tasks to other workers unless the task says to."
    )


def session_instructions(worker: dict[str, Any]) -> str:
    role = manager_instructions() if worker.get("role") == "manager" else worker_instructions(
        worker
    )
    return f"{claude_events.hub_instructions().strip()}\n\n{role}\n"
