from __future__ import annotations

import json
import os
from dataclasses import replace
from pathlib import Path

from copilot_hub.config import Settings
from copilot_hub.db import Repository
from copilot_hub.terminal_runtime import (
    PtySession,
    TerminalPromptInput,
    TerminalRuntime,
)


class Process:
    pid = 100

    @staticmethod
    def poll():
        return None


class RecordingSession:
    def __init__(self):
        self.writes = []

    def write(self, data):
        self.writes.append(data)
        return len(data)


def settings(tmp_path):
    return Settings(
        state_dir=tmp_path / "state",
        db_path=tmp_path / "state" / "hub.db",
        log_dir=tmp_path / "state" / "logs",
        host="127.0.0.1",
        port=9988,
        claude_executable="claude",
        permission_mode="bypassPermissions",
        agent_default_cwd=str(tmp_path),
        agent_model="model",
        agent_reasoning_effort="high",
        agent_context_tier="long",
        agent_max_workers=2,
        manager_session_id=None,
        terminals_enabled=True,
        allow_remote=False,
    )


def test_prompt_tracker_preserves_multiline_bracketed_paste():
    prompt = TerminalPromptInput()
    text = "first line\n" + ("visible token " * 2000) + "\nlast line"

    submitted, cancelled = prompt.consume(
        "\x1b[200~" + text + "\x1b[201~\r"
    )

    assert not cancelled
    assert submitted == text
    assert not prompt.dirty


def test_prompt_tracker_fences_unknown_edits_and_cancellation():
    prompt = TerminalPromptInput()
    prompt.consume("draft")
    assert prompt.dirty
    prompt.consume("\x1b[D")
    submitted, _ = prompt.consume("\r")
    assert submitted is None
    prompt.consume("another")
    submitted, cancelled = prompt.consume("\x1b")
    assert submitted is None
    assert cancelled
    assert not prompt.dirty


def test_pty_replay_resumes_by_stream_and_offset():
    read_fd, write_fd = os.pipe()
    try:
        session = PtySession(
            worker_id="worker",
            master_fd=write_fd,
            process=Process(),
        )
        session.append(b"hello")
        first, replay = session.replay_snapshot(None, None)
        assert first["reset"]
        assert replay == b"hello"
        resumed, replay = session.replay_snapshot(session.stream_id, 3)
        assert not resumed["reset"]
        assert resumed["offset"] == 3
        assert replay == b"lo"
        session.append(b" world")
        data, offset = session.read_from(5)
        assert data == b" world"
        assert offset == 11
    finally:
        os.close(read_fd)
        try:
            os.close(write_fd)
        except OSError:
            pass


def test_terminal_launch_args_are_persistent_and_explicitly_trusted(tmp_path, monkeypatch):
    monkeypatch.setenv("CLAUDE_CONFIG_DIR", str(tmp_path / "claude"))
    configured = settings(tmp_path)
    repository = Repository(configured.db_path)
    repository.initialize()
    runtime = TerminalRuntime(repository, configured)
    worker = repository.create_agent_worker(
        copilot_session_id="session-id",
        name="worker-1",
        role="worker",
        model="opus",
        reasoning_effort="high",
        context_tier="long_context",
        cwd=str(tmp_path),
    )

    arguments = runtime.claude_args(worker)

    assert arguments[0] == "claude"
    assert arguments[arguments.index("--model") + 1] == "opus[1m]"
    assert arguments[arguments.index("--effort") + 1] == "high"
    assert "--dangerously-skip-permissions" in arguments
    assert arguments[arguments.index("--system-prompt-snapshot") + 1] == "off"
    role_prompt = arguments[arguments.index("--append-system-prompt") + 1]
    assert "Your role: coder worker" not in role_prompt
    assert "Your role: worker" in role_prompt
    hub_settings = json.loads(arguments[arguments.index("--settings") + 1])
    assert hub_settings["skipDangerousModePermissionPrompt"] is True
    hook = hub_settings["hooks"]["Stop"][0]["hooks"][0]["command"]
    assert "claude_events.py" in hook
    assert str(configured.state_dir) in hook
    assert runtime._is_managed_claude_command(arguments)
    assert arguments[-4:] == [
        "--session-id",
        "session-id",
        "--name",
        "worker-1",
    ]

    transcript = tmp_path / "claude" / "projects" / "-tmp-work" / "session-id.jsonl"
    transcript.parent.mkdir(parents=True)
    transcript.write_text("{}\n", encoding="utf-8")
    assert runtime.claude_args(worker)[-1] == "--resume=session-id"


def test_non_bypass_permission_mode_uses_permission_flag(tmp_path):
    configured = replace(settings(tmp_path), permission_mode="acceptEdits")
    repository = Repository(configured.db_path)
    repository.initialize()
    runtime = TerminalRuntime(repository, configured)

    arguments, extra = runtime.claude_permission_args()

    assert arguments == ["--permission-mode", "acceptEdits"]
    assert extra == {}


def test_browser_input_is_not_reapplied_after_runtime_restart(tmp_path):
    configured = settings(tmp_path)
    first_repository = Repository(configured.db_path)
    first_repository.initialize()
    worker = first_repository.create_agent_worker(
        copilot_session_id="session-id",
        name="worker-1",
        role="worker",
        model="model",
        reasoning_effort="high",
        context_tier="long",
        cwd=str(tmp_path),
    )
    first_runtime = TerminalRuntime(first_repository, configured)
    session = RecordingSession()
    side_effects = []

    for sequence, data, kind in (
        (1, "\x1b[<0;4;2M", "transport"),
        (2, "prompt\r", "prompt"),
    ):
        assert first_runtime._apply_browser_input_once(
            session,
            worker_id=worker["id"],
            client_id="browser",
            sequence=sequence,
            data=data,
            before_write=lambda kind=kind: side_effects.append(kind),
        )

    restarted_repository = Repository(configured.db_path)
    restarted_repository.initialize()
    restarted_runtime = TerminalRuntime(restarted_repository, configured)
    for sequence, data, kind in (
        (1, "\x1b[<0;4;2M", "transport"),
        (2, "prompt\r", "prompt"),
    ):
        assert not restarted_runtime._apply_browser_input_once(
            session,
            worker_id=worker["id"],
            client_id="browser",
            sequence=sequence,
            data=data,
            before_write=lambda kind=kind: side_effects.append(f"duplicate-{kind}"),
        )

    assert session.writes == [b"\x1b[<0;4;2M", b"prompt\r"]
    assert side_effects == ["transport", "prompt"]


def test_terminal_source_uses_only_generic_ownership_environment():
    source = Path("copilot_hub/terminal_runtime.py").read_text(encoding="utf-8")
    for name in (
        "COPILOT_HUB_WORKER_ID",
        "COPILOT_HUB_SESSION_ID",
        "COPILOT_HUB_TERMINAL_GENERATION",
        "COPILOT_HUB_STATE_DIR",
    ):
        assert name in source


def test_direct_prompt_tracking_starts_monitor_and_tolerates_busy_worker(tmp_path, monkeypatch):
    configured = settings(tmp_path)
    repository = Repository(configured.db_path)
    repository.initialize()
    runtime = TerminalRuntime(repository, configured)
    monitors = []
    monkeypatch.setattr(runtime, "_start_direct_monitor", monitors.append)
    worker = repository.create_agent_worker(
        copilot_session_id="session-id",
        name="copilot-hub-manager",
        role="manager",
        model="opus",
        reasoning_effort="high",
        context_tier="standard",
        cwd=str(tmp_path),
    )

    first = runtime.track_direct_prompt_submission(worker, "first prompt")
    follow_up = runtime.track_direct_prompt_submission(worker, "typed while busy")

    assert first == {"status": "active", "worker_id": worker["id"]}
    assert follow_up == {"status": "ignored"}
    assert monitors == [worker["id"]]


def test_manager_launch_disallows_editing_tools(tmp_path, monkeypatch):
    monkeypatch.setenv("CLAUDE_CONFIG_DIR", str(tmp_path / "claude"))
    configured = settings(tmp_path)
    repository = Repository(configured.db_path)
    repository.initialize()
    runtime = TerminalRuntime(repository, configured)
    manager = repository.create_agent_worker(
        copilot_session_id="manager-session",
        name="copilot-hub-manager",
        role="manager",
        model="opus",
        reasoning_effort="high",
        context_tier="standard",
        cwd=str(tmp_path),
    )
    worker = repository.create_agent_worker(
        copilot_session_id="worker-session",
        name="coder-1",
        role="worker",
        model="opus",
        reasoning_effort="high",
        context_tier="standard",
        cwd=str(tmp_path),
        capabilities=["code"],
    )

    manager_args = runtime.claude_args(manager)
    start = manager_args.index("--disallowedTools") + 1
    assert manager_args[start : start + 3] == ["Edit", "Write", "NotebookEdit"]
    assert "Your role: Manager" in manager_args[manager_args.index("--append-system-prompt") + 1]
    assert "--disallowedTools" not in runtime.claude_args(worker)
