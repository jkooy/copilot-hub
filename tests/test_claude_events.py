from __future__ import annotations

import json
import subprocess
import sys
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from copilot_hub import claude_events
from copilot_hub.agent_runtime import CopilotAgentRuntime
from copilot_hub.terminal_runtime import PtySession, TerminalRuntime
from copilot_hub.usage import SessionUsageMonitor

SESSION = "hub-session"


@pytest.fixture
def state_dir(tmp_path, monkeypatch):
    state = tmp_path / "state"
    monkeypatch.setattr(TerminalRuntime, "_sessions_root", claude_events.sessions_root(state))
    monkeypatch.setattr(TerminalRuntime, "_interrupt_checked_at", {})
    monkeypatch.setenv("CLAUDE_CONFIG_DIR", str(tmp_path / "claude"))
    return state


def hook(state_dir, event, *, launch_id="launch-1", **payload):
    claude_events.handle_hook(
        {
            "hook_event_name": event,
            "session_id": "claude-session",
            **payload,
        },
        state_dir=state_dir,
        session_id=SESSION,
        launch_id=launch_id,
    )


def events(state_dir):
    path = claude_events.events_path(SESSION, claude_events.sessions_root(state_dir))
    return [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines()]


def write_transcript(path: Path, entries: list[dict]) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("".join(json.dumps(entry) + "\n" for entry in entries), encoding="utf-8")
    return path


def test_completed_turn_is_settled_with_final_answer(state_dir):
    hook(state_dir, "SessionStart", source="startup")
    cursor = TerminalRuntime.event_cursor(SESSION)
    hook(state_dir, "UserPromptSubmit", prompt="Run the tests\r\nplease")
    hook(
        state_dir,
        "PreToolUse",
        tool_name="Bash",
        tool_use_id="toolu_1",
        tool_input={"command": "pytest"},
    )
    interaction_id = TerminalRuntime.find_prompt_after(SESSION, cursor, "Run the tests\nplease")
    assert interaction_id

    running = TerminalRuntime._interaction_lifecycle(
        TerminalRuntime.events_after(SESSION, cursor),
        interaction_id,
    )
    assert not running.settled
    assert running.active_tool_call_ids == ("toolu_1",)
    assert claude_events.turn_open(SESSION, claude_events.sessions_root(state_dir))

    hook(state_dir, "PostToolUse", tool_name="Bash", tool_use_id="toolu_1")
    hook(state_dir, "Stop", last_assistant_message="All 12 tests passed.")

    current = TerminalRuntime.events_after(SESSION, cursor)
    lifecycle = TerminalRuntime._interaction_lifecycle(current, interaction_id)
    assert lifecycle.settled
    assert lifecycle.final_content == "All 12 tests passed."
    assert TerminalRuntime._interaction_has_clean_checkpoint(current, interaction_id)
    assert (
        TerminalRuntime.response_after_cursor(SESSION, cursor, interaction_id)
        == "All 12 tests passed."
    )
    assert not claude_events.turn_open(SESSION, claude_events.sessions_root(state_dir))
    assert [entry["role"] for entry in TerminalRuntime.history(SESSION)] == [
        "user",
        "tool",
        "tool",
        "assistant",
    ]


def test_stop_without_message_reads_final_step_from_transcript(state_dir, tmp_path):
    transcript = write_transcript(
        tmp_path / "transcript.jsonl",
        [
            {"type": "user", "message": {"role": "user", "content": "hi"}},
            {
                "type": "assistant",
                "message": {"content": [{"type": "text", "text": "Earlier step"}]},
            },
            {
                "type": "user",
                "message": {"content": [{"type": "tool_result", "tool_use_id": "t"}]},
            },
            {"type": "assistant", "message": {"content": [{"type": "thinking"}]}},
            {"type": "assistant", "message": {"content": [{"type": "text", "text": "Final"}]}},
            {
                "type": "assistant",
                "isSidechain": True,
                "message": {"content": [{"type": "text", "text": "subagent"}]},
            },
            {"type": "assistant", "message": {"content": [{"type": "text", "text": "answer"}]}},
        ],
    )
    hook(state_dir, "SessionStart", source="startup", transcript_path=str(transcript))
    hook(state_dir, "UserPromptSubmit", prompt="hi")
    hook(state_dir, "Stop")

    final = [event for event in events(state_dir) if event["type"] == "assistant.message"]
    assert final[-1]["data"]["content"] == "Final\n\nanswer"
    assert final[-1]["data"]["phase"] == "final_answer"


def test_escape_interrupt_is_reconciled_from_transcript(state_dir, tmp_path):
    transcript = tmp_path / "claude" / "projects" / "-work" / "claude-session.jsonl"
    hook(state_dir, "SessionStart", source="startup", transcript_path=str(transcript))
    hook(state_dir, "UserPromptSubmit", prompt="long task")
    hook(state_dir, "PreToolUse", tool_name="Bash", tool_use_id="toolu_9", tool_input={})
    later = (datetime.now(UTC) + timedelta(seconds=1)).isoformat()
    write_transcript(
        transcript,
        [
            {
                "type": "user",
                "timestamp": later,
                "message": {"content": [{"type": "text", "text": "[Request interrupted by user]"}]},
            }
        ],
    )
    root = claude_events.sessions_root(state_dir)

    assert claude_events.reconcile_interrupted_turn(SESSION, root)
    assert not claude_events.reconcile_interrupted_turn(SESSION, root)

    current = events(state_dir)
    interaction_id = next(
        event["data"]["interactionId"] for event in current if event["type"] == "user.message"
    )
    lifecycle = TerminalRuntime._interaction_lifecycle(current, interaction_id)
    assert lifecycle.settled
    assert lifecycle.final_content is None
    assert current[-2]["data"]["reason"] == "interrupted"
    assert not claude_events.turn_open(SESSION, root)


def test_stale_interrupt_marker_does_not_close_new_turn(state_dir, tmp_path):
    transcript = write_transcript(
        tmp_path / "transcript.jsonl",
        [
            {
                "type": "user",
                "timestamp": "2020-01-01T00:00:00.000Z",
                "message": {"content": "[Request interrupted by user]"},
            }
        ],
    )
    hook(state_dir, "SessionStart", source="startup", transcript_path=str(transcript))
    hook(state_dir, "UserPromptSubmit", prompt="next")

    assert not claude_events.reconcile_interrupted_turn(
        SESSION,
        claude_events.sessions_root(state_dir),
    )


def test_ask_user_question_marks_session_as_needing_input(state_dir):
    hook(state_dir, "SessionStart", source="startup")
    hook(state_dir, "UserPromptSubmit", prompt="deploy")
    hook(
        state_dir,
        "PreToolUse",
        tool_name="AskUserQuestion",
        tool_use_id="toolu_ask",
        tool_input={
            "questions": [
                {
                    "question": "Which environment?",
                    "options": [{"label": "staging"}, {"label": "production"}],
                }
            ]
        },
    )

    request = TerminalRuntime.user_input_request_after_cursor(SESSION, 0)
    assert request is not None
    assert request.source == "ask_user"
    assert request.tool_call_id == "toolu_ask"
    assert "Which environment?" in request.reason
    assert "staging" in request.reason

    hook(state_dir, "PostToolUse", tool_name="AskUserQuestion", tool_use_id="toolu_ask")
    assert TerminalRuntime.user_input_request_after_cursor(SESSION, 0) is None


def test_permission_notification_is_pending_input_until_work_resumes(state_dir):
    hook(state_dir, "SessionStart", source="startup")
    hook(state_dir, "UserPromptSubmit", prompt="edit")
    hook(
        state_dir,
        "Notification",
        notification_type="permission_prompt",
        message="Claude needs your permission to use Bash",
    )
    request = TerminalRuntime.user_input_request_after_cursor(SESSION, 0)
    assert request is not None
    assert "permission" in request.reason

    hook(state_dir, "PreToolUse", tool_name="Bash", tool_use_id="toolu_2", tool_input={})
    assert TerminalRuntime.user_input_request_after_cursor(SESSION, 0) is None


def test_subagent_internal_tools_do_not_affect_top_level_lifecycle(state_dir):
    hook(state_dir, "SessionStart", source="startup")
    hook(state_dir, "UserPromptSubmit", prompt="research")
    hook(
        state_dir,
        "PreToolUse",
        tool_name="Agent",
        tool_use_id="toolu_agent",
        tool_input={"subagent_type": "Explore"},
    )
    hook(
        state_dir,
        "PreToolUse",
        tool_name="Grep",
        tool_use_id="toolu_inner",
        tool_input={},
        agent_id="agent-1",
    )
    current = events(state_dir)
    interaction_id = current[1]["data"]["interactionId"]
    lifecycle = TerminalRuntime._interaction_lifecycle(current, interaction_id)
    assert lifecycle.active_tool_call_ids == ("toolu_agent",)
    assert lifecycle.active_subagent_call_ids == ("toolu_agent",)
    assert any(event.get("agentId") == "agent-1" for event in current)

    hook(state_dir, "PostToolUse", tool_name="Agent", tool_use_id="toolu_agent")
    hook(state_dir, "Stop", last_assistant_message="Found it.")
    lifecycle = TerminalRuntime._interaction_lifecycle(events(state_dir), interaction_id)
    assert lifecycle.settled


def test_relaunch_after_crash_records_shutdown_before_resume(state_dir):
    hook(state_dir, "SessionStart", source="startup")
    hook(state_dir, "UserPromptSubmit", prompt="work")
    hook(state_dir, "PreToolUse", tool_name="Bash", tool_use_id="toolu_3", tool_input={})
    hook(state_dir, "SessionStart", source="resume", launch_id="launch-2")

    types = [event["type"] for event in events(state_dir)]
    assert types[-2:] == ["session.shutdown", "session.resume"]
    root = claude_events.sessions_root(state_dir)
    assert claude_events.launch_started(SESSION, "launch-2", root)
    assert not claude_events.launch_started(SESSION, "launch-1", root)
    assert claude_events.resume_session_id(SESSION, root) == "claude-session"


def test_clean_session_end_is_not_reported_as_abnormal(state_dir):
    hook(state_dir, "SessionStart", source="startup")
    hook(state_dir, "SessionEnd", reason="prompt_input_exit")
    hook(state_dir, "SessionStart", source="resume", launch_id="launch-2")

    shutdowns = [event for event in events(state_dir) if event["type"] == "session.shutdown"]
    assert [event["data"]["shutdownType"] for event in shutdowns] == ["prompt_input_exit"]


def test_usage_summary_counts_each_message_once_and_feeds_monitor(state_dir, tmp_path):
    usage = {
        "input_tokens": 10,
        "output_tokens": 5,
        "cache_read_input_tokens": 100,
        "cache_creation_input_tokens": 7,
    }
    transcript = write_transcript(
        tmp_path / "transcript.jsonl",
        [
            {"type": "assistant", "message": {"id": "m1", "model": "opus", "usage": usage}},
            {"type": "assistant", "message": {"id": "m1", "model": "opus", "usage": usage}},
            {"type": "assistant", "message": {"id": "m2", "model": "opus", "usage": usage}},
        ],
    )
    hook(state_dir, "SessionStart", source="startup", transcript_path=str(transcript))
    hook(state_dir, "UserPromptSubmit", prompt="hi")
    hook(state_dir, "Stop", last_assistant_message="done")

    recorded = SessionUsageMonitor(claude_events.sessions_root(state_dir)).session_usage(SESSION)
    assert recorded["input_tokens"] == 20
    assert recorded["output_tokens"] == 10
    assert recorded["cache_read_tokens"] == 200
    assert recorded["cache_write_tokens"] == 14
    assert recorded["recorded_tokens_available"] is True
    assert recorded["models"] == ["opus"]


def test_hook_script_runs_standalone_and_stays_silent(tmp_path):
    state = tmp_path / "state"
    payload = {"hook_event_name": "UserPromptSubmit", "session_id": "s1", "prompt": "hello"}
    command = claude_events.hook_command(state)

    result = subprocess.run(
        command,
        shell=True,
        input=json.dumps(payload),
        capture_output=True,
        text=True,
        env={"PATH": "/usr/bin:/bin", "COPILOT_HUB_SESSION_ID": "hub-1"},
        cwd=tmp_path,
        check=False,
    )

    assert result.returncode == 0
    assert result.stdout == ""
    log = claude_events.events_path("hub-1", claude_events.sessions_root(state))
    assert json.loads(log.read_text().splitlines()[0])["data"]["content"] == "hello"


def test_hook_script_swallows_bad_input(tmp_path):
    result = subprocess.run(
        [sys.executable, claude_events.__file__, "--state-dir", str(tmp_path)],
        input="not json",
        capture_output=True,
        text=True,
        check=False,
    )
    assert result.returncode == 0
    assert result.stdout == ""
    assert "JSONDecodeError" in (tmp_path / "logs" / "claude-hooks.log").read_text()


def test_child_environment_strips_parent_claude_session_only_when_nested():
    plain = {"PATH": "/bin", "CLAUDE_CODE_USE_BEDROCK": "1"}
    assert claude_events.child_environment(plain) == plain

    nested = claude_events.child_environment(
        {
            "PATH": "/bin",
            "HOME": "/Users/me",
            "CLAUDECODE": "1",
            "CLAUDE_CODE_SESSION_ID": "parent",
            "WRAPPER_CLAUDE_CODE_PORT": "1234",
            "ANTHROPIC_BASE_URL": "http://localhost:1234",
            "ANTHROPIC_DEFAULT_OPUS_MODEL": "x",
            "CLAUDE_PID": "1",
        }
    )
    assert nested == {"PATH": "/bin", "HOME": "/Users/me"}


def test_stream_json_messages_map_to_task_events():
    assistant = {
        "type": "assistant",
        "message": {
            "content": [
                {"type": "text", "text": "Looking"},
                {"type": "tool_use", "id": "toolu_1", "name": "Read"},
            ]
        },
    }
    tool_result = {
        "type": "user",
        "message": {
            "content": [{"type": "tool_result", "tool_use_id": "toolu_1", "is_error": True}]
        },
    }
    nested = {**assistant, "parent_tool_use_id": "toolu_agent"}

    assert CopilotAgentRuntime._claude_stream_events(assistant) == [
        ("assistant.message", {"content": "Looking"}),
        ("tool.execution_start", {"toolCallId": "toolu_1", "toolName": "Read"}),
    ]
    assert CopilotAgentRuntime._claude_stream_events(tool_result) == [
        ("tool.execution_complete", {"toolCallId": "toolu_1", "success": False}),
    ]
    assert CopilotAgentRuntime._claude_stream_events(nested) == []
    assert CopilotAgentRuntime._claude_stream_events({"type": "result", "result": "ok"})[0][0] == (
        "result"
    )


def test_prompt_render_detection_tolerates_wrapping_and_collapsed_pastes():
    prompt = "Investigate the failing deployment pipeline for service alpha\nthen report back"
    wrapped = (
        b"\x1b[2K> Investigate the failing deploy\x1b[1B\x1b[2Kment pipeline for service al"
    )
    assert TerminalRuntime._prompt_rendered(wrapped, prompt)
    assert TerminalRuntime._prompt_rendered(b"> \x1b[2m[Pasted text #1 +40 lines]\x1b[0m", prompt)
    assert not TerminalRuntime._prompt_rendered(b"> something else", prompt)


class RunningProcess:
    pid = 4242

    @staticmethod
    def poll():
        return None


class StubRepository:
    def get_agent_worker(self, worker_id):
        return None


def runtime_for(state_dir):
    runtime = TerminalRuntime.__new__(TerminalRuntime)
    runtime.settings = type("S", (), {"sessions_dir": claude_events.sessions_root(state_dir)})()
    runtime.repository = StubRepository()
    runtime._lock = __import__("threading").RLock()
    runtime._folder_trust_confirmations = set()
    runtime._bypass_confirmations = set()
    runtime._auto_mode_prompt_answers = set()
    runtime._dismissed_dialogs = set()
    runtime._probing = set()
    runtime._worker_idle_callback = None
    return runtime


def test_readiness_follows_hook_state(state_dir, monkeypatch):
    monkeypatch.setattr(TerminalRuntime, "SETTLE_SECONDS", 0)
    monkeypatch.setattr(TerminalRuntime, "STARTUP_DIALOG_GRACE_SECONDS", 0)
    runtime = runtime_for(state_dir)
    session = PtySession(
        worker_id="w",
        master_fd=-1,
        process=RunningProcess(),
        copilot_session_id=SESSION,
        launch_id="launch-1",
    )

    with pytest.raises(RuntimeError, match="never reported SessionStart"):
        runtime._wait_until_ready(session, timeout_seconds=0.2)

    hook(state_dir, "SessionStart", source="startup")
    # SessionStart fires before the prompt box accepts input.
    with pytest.raises(RuntimeError, match="Timed out waiting for Claude terminal readiness"):
        runtime._wait_until_ready(session, timeout_seconds=0.3)

    session.append(
        "\x1b[2m❯ \x1b[0m\r\n  \x1b[35m⏵⏵ bypass permissions on\x1b[0m (shift+tab to cy".encode()
    )
    session.append(b"cle)")
    runtime._wait_until_ready(session, timeout_seconds=1)

    hook(state_dir, "UserPromptSubmit", prompt="busy")
    with pytest.raises(RuntimeError, match="Timed out waiting for Claude terminal readiness"):
        runtime._wait_until_ready(session, timeout_seconds=0.3)

    hook(state_dir, "Stop", last_assistant_message="done")
    runtime._wait_until_ready(session, timeout_seconds=1)


class WritableSession(PtySession):
    def write(self, data):
        self.writes.append(data)
        return len(data)


def test_startup_folder_trust_dialog_is_confirmed_once(state_dir, monkeypatch):
    monkeypatch.setattr(TerminalRuntime, "_guarded_sleep", classmethod(lambda cls, s, g: None))
    runtime = runtime_for(state_dir)
    runtime.repository = type(
        "Repo",
        (),
        {
            "get_agent_worker": staticmethod(
                lambda worker_id: {
                    "copilot_session_id": SESSION,
                    "terminal_generation": 0,
                    "current_task_id": None,
                    "activity_kind": None,
                    "direct_generation": 0,
                    "direct_submitted_at": None,
                }
            )
        },
    )()
    session = WritableSession(
        worker_id="w",
        master_fd=-1,
        process=RunningProcess(),
        copilot_session_id=SESSION,
    )
    session.writes = []
    session.append(
        "\x1b[1mDo you trust the files in this folder?\x1b[0m\r\n"
        "\x1b[36m❯ 1. Yes, proceed\x1b[0m\r\n  2. No, exit".encode()
    )

    assert runtime._handle_startup_dialogs(session, None)
    assert session.writes == [b"\r"]
    assert not runtime._handle_startup_dialogs(session, None)

    session.append(
        "WARNING: Claude Code running in Bypass Permissions mode\r\n"
        "\x1b[36m❯ 1. No, exit\x1b[0m\r\n  2. Yes, I accept".encode()
    )
    assert runtime._handle_startup_dialogs(session, None)
    assert session.writes == [b"\r", b"\x1b[B", b"\r"]


def test_dialog_keys_follow_the_selected_option():
    def keys(screen):
        return TerminalRuntime._affirmative_dialog_keys(
            TerminalRuntime._visible_compact_text(screen.encode())
        )

    # Layout reported by Claude Code 2.1.284.
    quick_safety_check = (
        " Quick safety check: Is this a project you created or one you trust?\r\n"
        "  Security guide \r\n\r\n"
        "  \x1b[36m❯ No, exit \x1b[0m\r\n"
        " Yes, I trust this folder\r\n\r\n"
        "  Enter to confirm · Esc to cancel"
    )
    assert keys(quick_safety_check) == (b"\x1b[B", b"\r")
    assert keys("❯ 1. Yes, proceed\r\n  2. No, exit") == (b"\r",)
    assert keys("1. No, exit\r\n2. Yes, I accept") == (b"\x1b[B", b"\r")
    assert keys("1. Yes, proceed\r\n2. No, exit") == (b"\r",)


def test_zombie_only_process_group_on_macos_is_not_running(monkeypatch):
    def eperm(process_group, sig):
        raise PermissionError(1, "Operation not permitted")

    monkeypatch.setattr("os.killpg", eperm)
    monkeypatch.setattr(Path, "iterdir", lambda self: (_ for _ in ()).throw(OSError()))
    monkeypatch.setattr(
        TerminalRuntime,
        "_ps_processes",
        staticmethod(lambda: [(10, 10, 501, "Z", "(claude)")]),
    )
    assert not TerminalRuntime._process_group_exists(10)

    monkeypatch.setattr(TerminalRuntime, "_ps_processes", staticmethod(list))
    assert not TerminalRuntime._process_group_exists(10)


def test_follow_up_prompts_let_a_direct_activity_finish(state_dir, tmp_path):
    from copilot_hub.db import Repository

    repository = Repository(tmp_path / "hub.db")
    repository.initialize()
    worker = repository.create_agent_worker(
        copilot_session_id=SESSION,
        name="copilot-hub-manager",
        role="manager",
        model="opus",
        reasoning_effort="high",
        context_tier="standard",
        cwd=str(tmp_path),
    )
    hook(state_dir, "SessionStart", source="startup")
    cursor = TerminalRuntime.event_cursor(SESSION)
    repository.register_direct_activity(worker["id"], event_cursor=cursor, prompt_hash="x")
    generation = int(repository.get_agent_worker(worker["id"])["direct_generation"])
    hook(state_dir, "UserPromptSubmit", prompt="first")
    first = TerminalRuntime.latest_user_interaction_after_cursor(SESSION, cursor)
    assert repository.accept_direct_activity(
        worker["id"], generation=generation, interaction_id=first, context="first"
    )
    # A follow-up typed while Claude works joins the same direct activity.
    hook(state_dir, "UserPromptSubmit", prompt="second")
    hook(state_dir, "Stop", last_assistant_message="done")
    second = TerminalRuntime.latest_user_interaction_after_cursor(SESSION, cursor)

    events = TerminalRuntime.events_after(SESSION, cursor)
    active, _, _ = TerminalRuntime._current_direct_interaction(events, first)
    assert active == second
    assert not repository.finish_direct_activity(
        worker["id"], generation=generation, interaction_id=second
    )

    assert repository.advance_direct_interaction(
        worker["id"], generation=generation, from_interaction_id=first,
        to_interaction_id=second,
    )
    assert not repository.advance_direct_interaction(
        worker["id"], generation=generation, from_interaction_id=first,
        to_interaction_id=second,
    )
    assert TerminalRuntime._interaction_lifecycle(events, second).settled
    assert repository.finish_direct_activity(
        worker["id"], generation=generation, interaction_id=second
    )
    assert repository.get_agent_worker(worker["id"])["state"] == "idle"


def owning_repository():
    return type(
        "Repo",
        (),
        {
            "get_agent_worker": staticmethod(
                lambda worker_id: {
                    "copilot_session_id": SESSION,
                    "terminal_generation": 0,
                    "current_task_id": None,
                    "activity_kind": None,
                    "direct_generation": 0,
                    "direct_submitted_at": None,
                }
            )
        },
    )()


def test_auto_mode_prompt_after_launch_is_declined_before_ready(state_dir, monkeypatch):
    monkeypatch.setattr(TerminalRuntime, "STARTUP_DIALOG_GRACE_SECONDS", 0.2)
    monkeypatch.setattr(TerminalRuntime, "SETTLE_SECONDS", 0)
    runtime = runtime_for(state_dir)
    runtime.repository = owning_repository()
    session = WritableSession(
        worker_id="w",
        master_fd=-1,
        process=RunningProcess(),
        copilot_session_id=SESSION,
        launch_id="launch-1",
    )
    session.writes = []
    hook(state_dir, "SessionStart", source="startup")
    # Screen recorded from Claude Code 2.1.293: prompt box first, then the dialog.
    session.append(
        "❯  Try \"fix lint errors\"\r\n ⏵⏵ bypass permissions on  (shift+tab to cycle)"
        "\r\n Make auto mode your default permission mode?\r\n"
        "  \x1b[36m❯ Yes, set auto mode as my default permission mode\x1b[0m\r\n"
        "    No, keep manual mode".encode()
    )

    runtime._wait_until_ready(session, timeout_seconds=5)

    assert session.writes == [b"\x1b[B", b"\r"]
    assert session.startup_complete
    # Conversation text that mentions the dialog later must not be answered.
    session.append(b"Make auto mode your default permission mode? No, keep manual mode")
    runtime._wait_until_ready(session, timeout_seconds=1)
    assert session.writes == [b"\x1b[B", b"\r"]


def test_negative_dialog_keys_follow_the_cursor():
    def keys(screen):
        return TerminalRuntime._negative_dialog_keys(
            TerminalRuntime._visible_compact_text(screen.encode())
        )

    assert keys("❯ Yes, set auto mode\r\n  No, keep manual mode") == (b"\x1b[B", b"\r")
    assert keys("  Yes, set auto mode\r\n❯ No, keep manual mode") == (b"\r",)
