from __future__ import annotations

import json
import os
import subprocess
import sys
import threading
import time

from copilot_hub import dispatch_inbox, session_prompts
from copilot_hub.db import Repository


def serve_inbox(state_dir, handler):
    poller = dispatch_inbox.InboxPoller(state_dir, handler, interval_seconds=0.05)
    poller.start()
    return poller


def test_request_round_trips_through_the_inbox(tmp_path):
    seen = []

    def handler(payload):
        seen.append(payload)
        return {"status": "accepted", "task_id": "t1"}

    poller = serve_inbox(tmp_path, handler)
    try:
        result = dispatch_inbox.submit_request(tmp_path, {"title": "x", "prompt": "y"})
    finally:
        poller.stop()

    assert result == {"status": "accepted", "task_id": "t1"}
    assert seen[0]["title"] == "x"
    assert list(dispatch_inbox.inbox_dir(tmp_path).iterdir()) == []


def test_handler_errors_are_reported_to_the_caller(tmp_path):
    def handler(payload):
        raise ValueError("no workers")

    poller = serve_inbox(tmp_path, handler)
    try:
        result = dispatch_inbox.submit_request(tmp_path, {"title": "x", "prompt": "y"})
    finally:
        poller.stop()
    assert result == {"status": "error", "error": "no workers"}


def test_request_stays_queued_when_the_server_is_down(tmp_path):
    result = dispatch_inbox.submit_request(tmp_path, {"title": "x"}, timeout_seconds=0.3)
    assert result["status"] == "pending"
    queued = list(dispatch_inbox.inbox_dir(tmp_path).glob("*.request.json"))
    assert len(queued) == 1

    handled = []
    poller = dispatch_inbox.InboxPoller(tmp_path, lambda payload: handled.append(payload) or {})
    assert poller.process_once() == 1
    assert handled[0]["title"] == "x"


def cli_env(tmp_path, **extra):
    return {
        **os.environ,
        "HOME": str(tmp_path),
        "COPILOT_HUB_STATE_DIR": str(tmp_path / "state"),
        "COPILOT_HUB_CWD": str(tmp_path / "work"),
        "COPILOT_HUB_PORT": "9",
        **extra,
    }


def test_managed_session_dispatch_uses_the_inbox_and_reads_prompt_from_stdin(tmp_path):
    received = []

    def handler(payload):
        received.append(payload)
        return {"status": "accepted", "task_id": "t9", "worker_type": "researcher"}

    poller = serve_inbox(tmp_path / "state", handler)
    try:
        result = subprocess.run(
            [sys.executable, "-m", "copilot_hub.cli", "dispatch", "--task-type", "research",
             "--title", "Survey tests", "--prompt", "-"],
            input="Look at 'tests/' and \"report\"\n",
            capture_output=True,
            text=True,
            env=cli_env(tmp_path, COPILOT_HUB_WORKER_ID="manager-id"),
            timeout=30,
            check=False,
        )
    finally:
        poller.stop()

    assert result.returncode == 0, result.stderr
    assert json.loads(result.stdout)["task_id"] == "t9"
    assert received[0]["prompt"] == "Look at 'tests/' and \"report\""
    assert received[0]["task_type"] == "research"


def test_dispatch_without_a_reachable_server_falls_back_to_the_inbox(tmp_path):
    poller = serve_inbox(tmp_path / "state", lambda payload: {"status": "accepted"})
    try:
        result = subprocess.run(
            [sys.executable, "-m", "copilot_hub.cli", "dispatch", "--title", "t",
             "--prompt", "p"],
            capture_output=True,
            text=True,
            env=cli_env(tmp_path),
            timeout=30,
            check=False,
        )
    finally:
        poller.stop()
    assert result.returncode == 0, result.stderr
    assert json.loads(result.stdout)["status"] == "accepted"


def test_managed_session_status_reads_the_database(tmp_path):
    repository = Repository(tmp_path / "state" / "copilot_hub.db")
    repository.initialize()
    repository.create_agent_worker(
        copilot_session_id="s1",
        name="reviewer-1",
        role="worker",
        model="opus",
        reasoning_effort="high",
        context_tier="standard",
        cwd=str(tmp_path),
        capabilities=["review"],
    )

    result = subprocess.run(
        [sys.executable, "-m", "copilot_hub.cli", "status"],
        capture_output=True,
        text=True,
        env=cli_env(tmp_path, COPILOT_HUB_WORKER_ID="manager-id"),
        timeout=30,
        check=False,
    )

    assert result.returncode == 0, result.stderr
    status = json.loads(result.stdout)
    assert status["workers"][0]["name"] == "reviewer-1"
    assert status["workers"][0]["worker_type"] == "review"


def test_manager_prompt_explains_dispatch_and_workers_know_their_role():
    manager = session_prompts.session_instructions({"role": "manager"})
    assert "Your role: Manager" in manager
    assert "dispatch --task-type code" in manager
    assert "--prompt - <<'TASK'" in manager
    assert "Never run a shell command other than the Hub CLI" in manager
    for label in ("coder", "researcher", "reviewer", "operator"):
        assert label in manager
    assert session_prompts.hub_command().split()[0].strip("'").endswith(
        ("copilot-hub", "python", "python3")
    ) or "copilot_hub.cli" in session_prompts.hub_command()

    worker = session_prompts.session_instructions({"role": "worker", "capabilities": ["review"]})
    assert "Your role: reviewer worker" in worker
    assert "You are the reviewer." in worker
    assert "Your role: Manager" not in worker


def test_inbox_poller_thread_stops_cleanly(tmp_path):
    poller = serve_inbox(tmp_path, lambda payload: {})
    started = time.monotonic()
    poller.stop()
    assert time.monotonic() - started < 2
    assert not any(thread.name == "hub-inbox" for thread in threading.enumerate())
