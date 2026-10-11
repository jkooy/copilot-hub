"""File-based task dispatch for commands run inside managed sessions.

Shell commands that managed Claude sessions run may execute inside a sandbox,
as some Claude Code distributions do, that blocks loopback connections to the
Hub server but allows file access. ``copilot-hub dispatch`` therefore drops a request file into the
state directory; the server polls the inbox, submits the task, and writes a
result file that the command waits for.
"""

from __future__ import annotations

import json
import logging
import os
import threading
import time
import uuid
from collections.abc import Callable
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)

REQUEST_SUFFIX = ".request.json"
RESULT_SUFFIX = ".result.json"
RESULT_RETENTION_SECONDS = 24 * 60 * 60


def inbox_dir(state_dir: Path) -> Path:
    return state_dir / "inbox"


def _write_atomic(path: Path, payload: dict[str, Any]) -> None:
    temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    temporary.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")
    os.replace(temporary, path)


def submit_request(
    state_dir: Path,
    payload: dict[str, Any],
    *,
    timeout_seconds: float = 20,
) -> dict[str, Any]:
    """Queue a dispatch request and wait for the server's result."""
    directory = inbox_dir(state_dir)
    directory.mkdir(parents=True, exist_ok=True)
    request_id = uuid.uuid4().hex
    _write_atomic(
        directory / f"{request_id}{REQUEST_SUFFIX}",
        {**payload, "request_id": request_id, "submitted_at": time.time()},
    )
    result_path = directory / f"{request_id}{RESULT_SUFFIX}"
    deadline = time.monotonic() + timeout_seconds
    while time.monotonic() < deadline:
        if result_path.exists():
            result = json.loads(result_path.read_text(encoding="utf-8"))
            result_path.unlink(missing_ok=True)
            return result
        time.sleep(0.2)
    return {
        "status": "pending",
        "request_id": request_id,
        "message": (
            "The request is queued in the Hub inbox but the server has not picked it up "
            "yet. It will be dispatched as soon as the Hub server is running."
        ),
    }


class InboxPoller:
    """Server-side loop that turns inbox request files into Hub tasks."""

    def __init__(
        self,
        state_dir: Path,
        handler: Callable[[dict[str, Any]], dict[str, Any]],
        *,
        interval_seconds: float = 0.5,
    ):
        self.directory = inbox_dir(state_dir)
        self.handler = handler
        self.interval_seconds = interval_seconds
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None

    def process_once(self) -> int:
        try:
            requests = sorted(
                self.directory.glob(f"*{REQUEST_SUFFIX}"),
                key=lambda path: path.stat().st_mtime,
            )
        except OSError:
            return 0
        for path in requests:
            request_id = path.name.removesuffix(REQUEST_SUFFIX)
            try:
                payload = json.loads(path.read_text(encoding="utf-8"))
                result = self.handler(payload)
            except Exception as exc:  # Report any failure back to the caller.
                logger.exception("Inbox dispatch request %s failed", request_id)
                result = {"status": "error", "error": str(exc)}
            _write_atomic(self.directory / f"{request_id}{RESULT_SUFFIX}", result)
            path.unlink(missing_ok=True)
        self._prune_results()
        return len(requests)

    def _prune_results(self) -> None:
        cutoff = time.time() - RESULT_RETENTION_SECONDS
        for path in self.directory.glob(f"*{RESULT_SUFFIX}"):
            try:
                if path.stat().st_mtime < cutoff:
                    path.unlink()
            except OSError:
                continue

    def _run(self) -> None:
        while not self._stop.wait(self.interval_seconds):
            try:
                self.process_once()
            except Exception:
                logger.exception("Inbox poller iteration failed")

    def start(self) -> None:
        self.directory.mkdir(parents=True, exist_ok=True)
        self._stop.clear()
        self._thread = threading.Thread(target=self._run, name="hub-inbox", daemon=True)
        self._thread.start()

    def stop(self) -> None:
        self._stop.set()
        if self._thread is not None:
            self._thread.join(timeout=2)
            self._thread = None
