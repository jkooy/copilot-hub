"""Input self-check that runs when the installed Claude Code version changes.

Claude Code updates can add startup dialogs or change the prompt box, which
breaks dispatch because the Hub types into the terminal. When the version
differs from the last one that passed, the Hub pastes a marker into an idle
worker's prompt box, confirms Claude drew it, and clears it again. Nothing is
sent to the model.
"""

from __future__ import annotations

import json
import logging
import os
import re
import subprocess
import threading
from typing import TYPE_CHECKING, Any

from . import claude_events
from .db import Repository, utc_now

if TYPE_CHECKING:
    from .config import Settings
    from .terminal_runtime import TerminalRuntime

logger = logging.getLogger(__name__)

STATE_KEY = "claude_input_self_check"
_VERSION_LINE = re.compile(r"(\d+\.\d+\.\d+)\S*\s*(?:\([^)]*\)\s*)?-\s*(@[\w./-]+)")


def claude_version(settings: Settings) -> str:
    """Package versions reported by ``claude --version``, including any wrapper's."""
    try:
        result = subprocess.run(
            [settings.claude_executable, "--version"],
            capture_output=True,
            text=True,
            timeout=30,
            env=claude_events.child_environment(os.environ),
            check=False,
        )
    except (OSError, subprocess.TimeoutExpired) as exc:
        return f"unknown ({type(exc).__name__})"
    packages = [
        f"{package} {version}" for version, package in _VERSION_LINE.findall(result.stdout)
    ]
    return ", ".join(sorted(packages)) or result.stdout.strip()[:200] or "unknown"


def load(repository: Repository) -> dict[str, Any]:
    try:
        value = json.loads(repository.get_state(STATE_KEY) or "{}")
    except json.JSONDecodeError:
        return {}
    return value if isinstance(value, dict) else {}


def _save(repository: Repository, value: dict[str, Any]) -> dict[str, Any]:
    repository.set_state(STATE_KEY, json.dumps(value, ensure_ascii=False))
    return value


def _probe_candidate(repository: Repository) -> dict[str, Any] | None:
    """An idle worker to probe; the Manager is skipped because the user types there."""
    idle = [
        worker
        for worker in repository.list_agent_workers(role="worker")
        if worker["state"] == "idle"
        and not worker.get("current_task_id")
        and not worker.get("direct_submitted_at")
    ]
    return idle[0] if idle else None


def run(
    repository: Repository,
    terminal_runtime: TerminalRuntime,
    settings: Settings,
    *,
    force: bool = False,
) -> dict[str, Any]:
    version = claude_version(settings)
    previous = load(repository)
    if (
        not force
        and previous.get("status") == "passed"
        and previous.get("version") == version
    ):
        return previous
    _save(
        repository,
        {**previous, "status": "running", "version": version, "started_at": utc_now()},
    )
    worker = _probe_candidate(repository)
    if worker is None:
        result: dict[str, Any] = {
            "status": "skipped",
            "detail": "No idle worker to probe; the check runs again at the next start",
        }
    else:
        result = terminal_runtime.probe_input(worker)
        result["worker"] = worker["name"]
    if result["status"] == "failed":
        logger.warning("Claude input self-check failed for %s: %s", version, result["detail"])
    return _save(
        repository,
        {
            **result,
            "version": version,
            "previous_version": previous.get("version"),
            "checked_at": utc_now(),
        },
    )


def start(
    repository: Repository,
    terminal_runtime: TerminalRuntime,
    settings: Settings,
    *,
    force: bool = False,
) -> threading.Thread:
    def target() -> None:
        try:
            run(repository, terminal_runtime, settings, force=force)
        except Exception:
            logger.exception("Claude input self-check crashed")
            _save(
                repository,
                {**load(repository), "status": "failed", "detail": "The self-check crashed"},
            )

    thread = threading.Thread(target=target, name="claude-input-self-check", daemon=True)
    thread.start()
    return thread
