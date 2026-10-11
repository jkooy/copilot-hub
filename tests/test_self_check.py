from __future__ import annotations

import pytest
from test_terminal_runtime import settings

from copilot_hub import self_check
from copilot_hub.db import Repository
from copilot_hub.terminal_runtime import PtySession, TerminalRuntime


class RunningProcess:
    pid = 4242

    @staticmethod
    def poll():
        return None


class EchoingSession(PtySession):
    """A PTY whose prompt box echoes pasted text, like a healthy Claude TUI."""

    echo = True

    def write(self, data):
        self.writes.append(data)
        if self.echo and data.startswith(b"\x1b[200~"):
            self.append(b"\x1b[2m> \x1b[0m" + data[6:-6])
        return len(data)


@pytest.fixture
def runtime(tmp_path, monkeypatch):
    monkeypatch.setenv("CLAUDE_CONFIG_DIR", str(tmp_path / "claude"))
    configured = settings(tmp_path)
    repository = Repository(configured.db_path)
    repository.initialize()
    value = TerminalRuntime(repository, configured)
    monkeypatch.setattr(value, "_wait_until_ready", lambda session, **kwargs: None)
    return value


def add_worker(runtime, name="reviewer-1"):
    return runtime.repository.create_agent_worker(
        copilot_session_id=f"session-{name}",
        name=name,
        role="worker",
        model="opus",
        reasoning_effort="high",
        context_tier="standard",
        cwd="/tmp",
        capabilities=["review"],
    )


def attach(runtime, monkeypatch, worker, *, echo=True):
    session = EchoingSession(
        worker_id=worker["id"],
        master_fd=-1,
        process=RunningProcess(),
        copilot_session_id=worker["copilot_session_id"],
    )
    session.writes = []
    session.echo = echo
    monkeypatch.setattr(runtime, "ensure_terminal", lambda worker, **kwargs: session)
    return session


def test_probe_passes_when_paste_is_drawn_and_clears_the_prompt_box(runtime, monkeypatch):
    worker = add_worker(runtime)
    session = attach(runtime, monkeypatch, worker)

    result = runtime.probe_input(worker)

    assert result["status"] == "passed"
    assert session.writes[0] == b"\x15"
    assert session.writes[1].startswith(b"\x1b[200~hub input probe ")
    assert session.writes[-1] == b"\x15"
    assert b"\r" not in b"".join(session.writes)
    assert not runtime.is_probing(worker["id"])


def test_probe_reports_the_screen_when_paste_is_swallowed(runtime, monkeypatch):
    worker = add_worker(runtime)
    session = attach(runtime, monkeypatch, worker, echo=False)
    session.append(b"Make auto mode your default permission mode? \xe2\x9d\xaf Yes")

    result = runtime.probe_input(worker, render_timeout_seconds=0.3)

    assert result["status"] == "failed"
    assert "Make auto mode your default permission mode?" in result["detail"]
    assert session.writes[-1] == b"\x15"


def test_probe_skips_a_worker_that_became_busy(runtime, monkeypatch):
    worker = add_worker(runtime)
    session = attach(runtime, monkeypatch, worker)
    runtime.repository.update_agent_worker(worker["id"], state="busy")

    assert runtime.probe_input(worker)["status"] == "skipped"
    assert session.writes == []


def test_workers_being_probed_are_not_leased_for_tasks(tmp_path):
    from test_agent_runtime import repository_and_runtime

    _, agent_runtime = repository_and_runtime(tmp_path)
    agent_runtime.ensure_manager()
    worker = agent_runtime.add_worker("review")

    class Probing:
        @staticmethod
        def is_probing(worker_id):
            return worker_id == worker["id"]

    agent_runtime.terminal_runtime = Probing()
    leased = agent_runtime._acquire_worker({"id": "t1", "task_type": "review"})
    assert leased is not None and leased["id"] != worker["id"]


def test_self_check_runs_only_when_the_version_changes(runtime, monkeypatch):
    worker = add_worker(runtime)
    attach(runtime, monkeypatch, worker)
    version = {"value": "@anthropic-ai/claude-code 2.1.293"}
    monkeypatch.setattr(self_check, "claude_version", lambda settings: version["value"])
    probes = []
    real_probe = runtime.probe_input
    monkeypatch.setattr(
        runtime, "probe_input", lambda worker: probes.append(worker["name"]) or real_probe(worker)
    )

    first = self_check.run(runtime.repository, runtime, runtime.settings)
    again = self_check.run(runtime.repository, runtime, runtime.settings)
    version["value"] = "@anthropic-ai/claude-code 2.1.300"
    upgraded = self_check.run(runtime.repository, runtime, runtime.settings)

    assert first["status"] == "passed" and first["worker"] == "reviewer-1"
    assert again == first
    assert upgraded["status"] == "passed"
    assert upgraded["previous_version"] == "@anthropic-ai/claude-code 2.1.293"
    assert probes == ["reviewer-1", "reviewer-1"]
    assert self_check.load(runtime.repository)["version"].endswith("2.1.300")


def test_self_check_skips_without_an_idle_worker(runtime, monkeypatch):
    monkeypatch.setattr(self_check, "claude_version", lambda settings: "v1")
    result = self_check.run(runtime.repository, runtime, runtime.settings)
    assert result["status"] == "skipped"


def test_version_parsing_ignores_update_notices(monkeypatch, runtime):
    class Result:
        stdout = (
            "2.1.293 - @anthropic-ai/claude-code\n"
            "1.4.0 (@latest) - @example/claude-wrapper - ✅ latest version\n"
        )

    monkeypatch.setattr(self_check.subprocess, "run", lambda *args, **kwargs: Result())
    assert self_check.claude_version(runtime.settings) == (
        "@anthropic-ai/claude-code 2.1.293, @example/claude-wrapper 1.4.0"
    )


def test_unrecognized_dialog_over_prompt_box_is_dismissed_with_escape(runtime, monkeypatch):
    monkeypatch.setattr(TerminalRuntime, "_guarded_sleep", classmethod(lambda cls, s, g: None))
    worker = add_worker(runtime)
    session = attach(runtime, monkeypatch, worker, echo=False)
    session.append(
        "\xe2\x9d\xaf  Try something\r\n \xe2\x8f\xb5\xe2\x8f\xb5 bypass permissions on "
        "(shift+tab to cycle)\r\n Enable the new turbo mode?\r\n "
        "\x1b[36m\xe2\x9d\xaf Turn it on\x1b[0m\r\n   Not now".encode("latin-1")
    )

    assert runtime._handle_startup_dialogs(session, None)
    assert session.writes == [b"\x1b"]


def test_dialog_before_the_prompt_box_is_left_alone(runtime, monkeypatch):
    worker = add_worker(runtime)
    session = attach(runtime, monkeypatch, worker, echo=False)
    session.append("Accept the new terms?\r\n\xe2\x9d\xaf Accept\r\n  Exit".encode("latin-1"))

    assert not runtime._handle_startup_dialogs(session, None)
    assert session.writes == []


def test_prompt_box_without_dialog_is_not_dismissed():
    visible = TerminalRuntime._visible_compact_text(
        "❯  Try \"fix lint errors\"\r\n ⏵⏵ bypass permissions on "
        "(shift+tab to cycle) · ← for agents".encode()
    )
    assert not TerminalRuntime._dialog_over_prompt_box(visible)
