"""Bridge Claude Code hooks into the Hub's per-session event log.

The Hub's lifecycle logic consumes an append-only ``events.jsonl`` per managed
session (``user.message``, ``assistant.turn_start``, ``tool.execution_start``,
``assistant.turn_end``, ...) and uses byte offsets into it as durable cursors.
Claude Code does not write such a log, so every managed session is launched
with hooks that run this file and append the equivalent events.

This module is executed directly by path from Claude Code hooks, so it must
only depend on the standard library.
"""

from __future__ import annotations

import argparse
import fcntl
import json
import os
import shlex
import sys
import uuid
from collections.abc import Iterator, Mapping
from contextlib import contextmanager
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

HOOK_EVENTS = (
    "SessionStart",
    "UserPromptSubmit",
    "PreToolUse",
    "PostToolUse",
    "PostToolUseFailure",
    "Notification",
    "Stop",
    "StopFailure",
    "SessionEnd",
)
AGENT_TOOLS = {"Task", "Agent"}
TOOL_NAMES = {"AskUserQuestion": "ask_user", "Bash": "bash"}
INPUT_NOTIFICATIONS = {"permission_prompt", "elicitation_dialog"}
INTERRUPT_MARKER = "[Request interrupted by user"
MAX_ARGUMENT_CHARS = 2000
TRANSCRIPT_TAIL_BYTES = 2 * 1024 * 1024
MAX_USAGE_TRANSCRIPT_BYTES = 64 * 1024 * 1024


def utc_now() -> str:
    return datetime.now(UTC).isoformat()


def default_state_dir() -> Path:
    return Path(os.environ.get("COPILOT_HUB_STATE_DIR", "~/.copilot-hub")).expanduser()


def sessions_root(state_dir: Path | None = None) -> Path:
    return (state_dir or default_state_dir()) / "sessions"


def events_path(session_id: str, root: Path | None = None) -> Path:
    return (root or sessions_root()) / session_id / "events.jsonl"


def claude_config_dir() -> Path:
    return Path(os.environ.get("CLAUDE_CONFIG_DIR", "~/.claude")).expanduser()


def hub_instructions() -> str:
    return (Path(__file__).parent / "agent_instructions" / "copilot-hub.instructions.md").read_text(
        encoding="utf-8"
    )


def model_argument(model: str, context_tier: str | None) -> str:
    if context_tier == "long_context" and not model.endswith("[1m]"):
        return f"{model}[1m]"
    return model


def permission_arguments(permission_mode: str) -> tuple[list[str], dict[str, Any]]:
    """CLI flags plus extra ``--settings`` entries for a permission mode."""
    if permission_mode == "bypassPermissions":
        return ["--dangerously-skip-permissions"], {"skipDangerousModePermissionPrompt": True}
    return ["--permission-mode", permission_mode], {}


def child_environment(base: Mapping[str, str]) -> dict[str, str]:
    """Environment for a managed Claude process.

    When the Hub itself runs inside a Claude Code session, that session's
    proxy URL, sockets and process IDs must not leak into managed sessions.
    """
    environment = dict(base)
    if "CLAUDECODE" not in environment:
        return environment
    return {
        key: value
        for key, value in environment.items()
        if key not in {"CLAUDECODE", "CLAUDE_PID", "CLAUDE_EFFORT", "ANTHROPIC_BASE_URL"}
        # Also covers wrapper-prefixed variables such as ``<VENDOR>_CLAUDE_CODE_PORT``.
        and "CLAUDE_CODE_" not in key
        and not key.startswith("ANTHROPIC_DEFAULT_")
    }


def hook_command(state_dir: Path, python: str | None = None) -> str:
    return " ".join(
        [
            shlex.quote(python or sys.executable),
            shlex.quote(str(Path(__file__).resolve())),
            "--state-dir",
            shlex.quote(str(state_dir)),
        ]
    )


def hook_settings(state_dir: Path, python: str | None = None) -> dict[str, Any]:
    command = hook_command(state_dir, python)
    return {
        "hooks": {
            event: [{"hooks": [{"type": "command", "command": command, "timeout": 15}]}]
            for event in HOOK_EVENTS
        }
    }


def load_state(session_id: str, root: Path | None = None) -> dict[str, Any]:
    path = (root or sessions_root()) / session_id / "state.json"
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    return value if isinstance(value, dict) else {}


def find_transcript(session_id: str, root: Path | None = None) -> Path | None:
    """Return the Claude transcript backing a Hub session, if one exists."""
    state = load_state(session_id, root)
    recorded = state.get("transcript_path")
    if isinstance(recorded, str) and recorded and Path(recorded).exists():
        return Path(recorded)
    claude_session_id = state.get("claude_session_id") or session_id
    projects = claude_config_dir() / "projects"
    try:
        return next(projects.glob(f"*/{claude_session_id}.jsonl"), None)
    except OSError:
        return None


def resume_session_id(session_id: str, root: Path | None = None) -> str:
    """Claude session to resume; differs from the Hub ID after ``/clear``."""
    value = load_state(session_id, root).get("claude_session_id")
    return value if isinstance(value, str) and value else session_id


@contextmanager
def _locked(session_dir: Path) -> Iterator[None]:
    session_dir.mkdir(parents=True, exist_ok=True)
    with (session_dir / ".lock").open("a") as handle:
        fcntl.flock(handle, fcntl.LOCK_EX)
        try:
            yield
        finally:
            fcntl.flock(handle, fcntl.LOCK_UN)


def _save_state(session_dir: Path, state: dict[str, Any]) -> None:
    path = session_dir / "state.json"
    temporary = path.with_name(f".state.{os.getpid()}.json")
    temporary.write_text(json.dumps(state), encoding="utf-8")
    os.replace(temporary, path)


def _append(session_dir: Path, events: list[dict[str, Any]]) -> None:
    if not events:
        return
    rendered = "".join(json.dumps(event, ensure_ascii=False) + "\n" for event in events)
    with (session_dir / "events.jsonl").open("a", encoding="utf-8") as stream:
        stream.write(rendered)
        stream.flush()
        os.fsync(stream.fileno())


def _event(event_type: str, data: dict[str, Any], agent_id: str | None = None) -> dict[str, Any]:
    event: dict[str, Any] = {
        "id": str(uuid.uuid4()),
        "type": event_type,
        "timestamp": utc_now(),
        "data": data,
    }
    if agent_id:
        event["agentId"] = agent_id
    return event


def _truncate(value: Any) -> Any:
    if isinstance(value, str):
        return value if len(value) <= MAX_ARGUMENT_CHARS else value[:MAX_ARGUMENT_CHARS] + "…"
    if isinstance(value, dict):
        return {key: _truncate(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_truncate(item) for item in value[:50]]
    return value


def _tool_arguments(tool_name: str, tool_input: Any) -> Any:
    if tool_name != "AskUserQuestion" or not isinstance(tool_input, dict):
        return _truncate(tool_input)
    questions = tool_input.get("questions")
    first = questions[0] if isinstance(questions, list) and questions else {}
    if not isinstance(first, dict):
        first = {}
    options = first.get("options") if isinstance(first.get("options"), list) else []
    return {
        "question": first.get("question") or tool_input.get("question"),
        "choices": [
            option.get("label") if isinstance(option, dict) else option for option in options
        ],
    }


def _turn_data(state: dict[str, Any], **extra: Any) -> dict[str, Any]:
    return {
        "interactionId": state.get("interaction_id"),
        "turnId": int(state.get("turn_id") or 0),
        **extra,
    }


def _close_pending_asks(state: dict[str, Any]) -> list[dict[str, Any]]:
    events = [
        _event(
            "tool.execution_complete",
            _turn_data(state, toolCallId=call_id, toolName="ask_user", success=True),
        )
        for call_id in state.get("pending_asks") or []
    ]
    state["pending_asks"] = []
    return events


def _close_open_tools(state: dict[str, Any], reason: str) -> list[dict[str, Any]]:
    events: list[dict[str, Any]] = []
    for call_id, tool_name in (state.get("open_tools") or {}).items():
        events.append(
            _event(
                "tool.execution_complete",
                _turn_data(
                    state,
                    toolCallId=call_id,
                    toolName=tool_name,
                    success=False,
                    reason=reason,
                ),
            )
        )
        if tool_name in AGENT_TOOLS:
            events.append(_event("subagent.failed", _turn_data(state, toolCallId=call_id)))
    state["open_tools"] = {}
    return events


def _finish_turn(
    state: dict[str, Any],
    *,
    content: str | None,
    reason: str,
    transcript: Path | None,
) -> list[dict[str, Any]]:
    events = _close_pending_asks(state) + _close_open_tools(state, reason)
    if content:
        events.append(
            _event("assistant.message", _turn_data(state, content=content, phase="final_answer"))
        )
    events.append(_event("assistant.turn_end", _turn_data(state, reason=reason)))
    events.append(_event("session.usage_checkpoint", usage_summary(transcript)))
    state["turn_open"] = False
    return events


def _read_tail(path: Path, limit: int) -> list[str]:
    try:
        with path.open("rb") as stream:
            size = stream.seek(0, os.SEEK_END)
            stream.seek(max(0, size - limit))
            data = stream.read().decode(errors="ignore")
    except OSError:
        return []
    lines = data.splitlines()
    return lines[1:] if size > limit else lines


def _message_text(entry: dict[str, Any]) -> list[str]:
    content = (entry.get("message") or {}).get("content")
    if isinstance(content, str):
        return [content]
    if not isinstance(content, list):
        return []
    return [
        str(block.get("text") or "")
        for block in content
        if isinstance(block, dict) and block.get("type") == "text" and block.get("text")
    ]


def final_assistant_text(transcript: Path | None) -> str | None:
    """Text of the last assistant step, read from the end of the transcript."""
    if transcript is None:
        return None
    parts: list[str] = []
    for line in reversed(_read_tail(transcript, TRANSCRIPT_TAIL_BYTES)):
        try:
            entry = json.loads(line)
        except json.JSONDecodeError:
            continue
        if not isinstance(entry, dict) or entry.get("isSidechain"):
            continue
        if entry.get("type") == "user":
            break
        if entry.get("type") == "assistant":
            parts[:0] = _message_text(entry)
    text = "\n\n".join(part.strip() for part in parts if part.strip())
    return text or None


def usage_summary(transcript: Path | None) -> dict[str, Any]:
    """Cumulative token usage per model, in the Copilot ``modelMetrics`` shape."""
    by_message: dict[str, tuple[str, dict[str, Any]]] = {}
    try:
        if transcript is None or transcript.stat().st_size > MAX_USAGE_TRANSCRIPT_BYTES:
            return {}
        lines = transcript.read_text(encoding="utf-8", errors="ignore").splitlines()
    except OSError:
        return {}
    for line in lines:
        if '"usage"' not in line:
            continue
        try:
            entry = json.loads(line)
        except json.JSONDecodeError:
            continue
        message = entry.get("message") if isinstance(entry, dict) else None
        if entry.get("type") != "assistant" or not isinstance(message, dict):
            continue
        usage = message.get("usage")
        if not isinstance(usage, dict):
            continue
        key = str(message.get("id") or entry.get("uuid") or len(by_message))
        by_message[key] = (str(message.get("model") or "unknown"), usage)
    metrics: dict[str, dict[str, Any]] = {}
    for model, usage in by_message.values():
        totals = metrics.setdefault(
            model,
            {
                "usage": {
                    "inputTokens": 0,
                    "outputTokens": 0,
                    "cacheReadTokens": 0,
                    "cacheWriteTokens": 0,
                }
            },
        )["usage"]
        totals["inputTokens"] += int(usage.get("input_tokens") or 0)
        totals["outputTokens"] += int(usage.get("output_tokens") or 0)
        totals["cacheReadTokens"] += int(usage.get("cache_read_input_tokens") or 0)
        totals["cacheWriteTokens"] += int(usage.get("cache_creation_input_tokens") or 0)
    if not metrics:
        return {}
    return {
        "modelMetrics": metrics,
        "modelCacheState": [{"modelId": model} for model in metrics],
        "currentModel": next(reversed(metrics)),
    }


def handle_hook(
    payload: dict[str, Any],
    *,
    state_dir: Path,
    session_id: str | None = None,
    launch_id: str | None = None,
) -> None:
    """Translate one Claude Code hook payload into Hub session events."""
    hook = str(payload.get("hook_event_name") or "")
    claude_session_id = str(payload.get("session_id") or "")
    # The Hub's session ID stays stable even if the user runs /clear in the terminal.
    session_id = session_id or claude_session_id
    if not session_id:
        return
    session_dir = sessions_root(state_dir) / session_id
    agent_id = payload.get("agent_id") if isinstance(payload.get("agent_id"), str) else None
    with _locked(session_dir):
        state = load_state(session_id, sessions_root(state_dir))
        if claude_session_id:
            state["claude_session_id"] = claude_session_id
        if isinstance(payload.get("transcript_path"), str) and payload["transcript_path"]:
            state["transcript_path"] = payload["transcript_path"]
        transcript = Path(state["transcript_path"]) if state.get("transcript_path") else None
        events: list[dict[str, Any]] = []

        if hook == "SessionStart":
            source = str(payload.get("source") or "startup")
            previous_launch = state.get("launch_id")
            if (
                previous_launch
                and previous_launch != launch_id
                and not state.get("shutdown_recorded")
            ):
                events.append(_event("session.shutdown", {"shutdownType": "abnormal"}))
            state["launch_id"] = launch_id
            state["shutdown_recorded"] = False
            data = {
                "source": source,
                "launchId": launch_id,
                "model": payload.get("model"),
                "transcriptPath": state.get("transcript_path"),
            }
            if source == "resume":
                events.append(_event("session.resume", {**data, "resumeTime": utc_now()}))
            else:
                events.append(_event("session.start", data))

        elif hook == "UserPromptSubmit":
            if state.get("turn_open"):
                events += _finish_turn(
                    state, content=None, reason="superseded", transcript=transcript
                )
            events += _close_pending_asks(state)
            state["interaction_id"] = str(uuid.uuid4())
            state["turn_id"] = int(state.get("turn_id") or 0) + 1
            state["turn_open"] = True
            state["turn_started_at"] = utc_now()
            prompt = payload.get("prompt")
            events.append(
                _event(
                    "user.message",
                    _turn_data(state, content=prompt if isinstance(prompt, str) else ""),
                )
            )
            events.append(_event("assistant.turn_start", _turn_data(state)))

        elif hook == "PreToolUse":
            tool_name = str(payload.get("tool_name") or "tool")
            call_id = str(payload.get("tool_use_id") or uuid.uuid4())
            if not agent_id:
                events += _close_pending_asks(state)
                state.setdefault("open_tools", {})[call_id] = tool_name
            events.append(
                _event(
                    "tool.execution_start",
                    _turn_data(
                        state,
                        toolCallId=call_id,
                        toolName=TOOL_NAMES.get(tool_name, tool_name),
                        arguments=_tool_arguments(tool_name, payload.get("tool_input")),
                    ),
                    agent_id,
                )
            )
            if tool_name in AGENT_TOOLS:
                tool_input = payload.get("tool_input") or {}
                events.append(
                    _event(
                        "subagent.started",
                        _turn_data(
                            state,
                            toolCallId=call_id,
                            agentName=tool_input.get("subagent_type")
                            if isinstance(tool_input, dict)
                            else None,
                        ),
                        agent_id,
                    )
                )

        elif hook in {"PostToolUse", "PostToolUseFailure"}:
            tool_name = str(payload.get("tool_name") or "tool")
            call_id = str(payload.get("tool_use_id") or "")
            success = hook == "PostToolUse"
            if not agent_id:
                events += _close_pending_asks(state)
                (state.get("open_tools") or {}).pop(call_id, None)
            data = _turn_data(
                state,
                toolCallId=call_id,
                toolName=TOOL_NAMES.get(tool_name, tool_name),
                success=success,
            )
            if not success and payload.get("error"):
                data["error"] = _truncate(str(payload["error"]))
            events.append(_event("tool.execution_complete", data, agent_id))
            if tool_name in AGENT_TOOLS:
                events.append(
                    _event(
                        "subagent.completed" if success else "subagent.failed",
                        _turn_data(state, toolCallId=call_id),
                        agent_id,
                    )
                )

        elif hook == "Notification":
            if payload.get("notification_type") in INPUT_NOTIFICATIONS and state.get("turn_open"):
                call_id = f"notification-{uuid.uuid4()}"
                state.setdefault("pending_asks", []).append(call_id)
                events.append(
                    _event(
                        "tool.execution_start",
                        _turn_data(
                            state,
                            toolCallId=call_id,
                            toolName="ask_user",
                            arguments={"question": payload.get("message")},
                        ),
                    )
                )

        elif hook in {"Stop", "StopFailure"}:
            if agent_id or state.get("interaction_id") is None:
                return
            content = payload.get("last_assistant_message")
            if not isinstance(content, str) or not content.strip():
                content = final_assistant_text(transcript)
            reason = "completed"
            if hook == "StopFailure":
                reason = "error"
                error = payload.get("error") or payload.get("error_details") or "unknown error"
                content = f"Claude Code stopped with an error: {error}" + (
                    f"\n\n{content}" if content else ""
                )
            events += _finish_turn(state, content=content, reason=reason, transcript=transcript)

        elif hook == "SessionEnd":
            events.append(
                _event(
                    "session.shutdown",
                    {"shutdownType": payload.get("reason"), **usage_summary(transcript)},
                )
            )
            state["shutdown_recorded"] = True

        _append(session_dir, events)
        _save_state(session_dir, state)


def _parse_timestamp(value: Any) -> datetime | None:
    if not isinstance(value, str) or not value:
        return None
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError:
        return None
    return parsed if parsed.tzinfo else parsed.replace(tzinfo=UTC)


def reconcile_interrupted_turn(session_id: str, root: Path | None = None) -> bool:
    """Close a turn the user interrupted with Esc.

    Claude Code does not run the Stop hook for an interrupted turn; it only
    records an interruption marker in the transcript.
    """
    root = root or sessions_root()
    state = load_state(session_id, root)
    if not state.get("turn_open"):
        return False
    started_at = _parse_timestamp(state.get("turn_started_at"))
    transcript = find_transcript(session_id, root)
    if started_at is None or transcript is None:
        return False
    interrupted = False
    for line in reversed(_read_tail(transcript, TRANSCRIPT_TAIL_BYTES)):
        if INTERRUPT_MARKER not in line:
            continue
        try:
            entry = json.loads(line)
        except json.JSONDecodeError:
            continue
        timestamp = _parse_timestamp(entry.get("timestamp"))
        if entry.get("type") == "user" and timestamp is not None and timestamp >= started_at:
            interrupted = True
        break
    if not interrupted:
        return False
    session_dir = root / session_id
    with _locked(session_dir):
        current = load_state(session_id, root)
        if not current.get("turn_open") or current.get("turn_id") != state.get("turn_id"):
            return False
        events = _finish_turn(current, content=None, reason="interrupted", transcript=transcript)
        _append(session_dir, events)
        _save_state(session_dir, current)
    return True


def launch_started(session_id: str, launch_id: str, root: Path | None = None) -> bool:
    return load_state(session_id, root).get("launch_id") == launch_id


def turn_open(session_id: str, root: Path | None = None) -> bool:
    return bool(load_state(session_id, root).get("turn_open"))


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--state-dir", type=Path, default=None)
    args = parser.parse_args(argv)
    state_dir = (args.state_dir or default_state_dir()).expanduser()
    try:
        payload = json.loads(sys.stdin.read() or "{}")
        if isinstance(payload, dict):
            handle_hook(
                payload,
                state_dir=state_dir,
                session_id=os.environ.get("COPILOT_HUB_SESSION_ID") or None,
                launch_id=os.environ.get("COPILOT_HUB_LAUNCH_ID") or None,
            )
    except Exception as exc:  # noqa: BLE001 - a failing hook must never disturb Claude.
        try:
            log_dir = state_dir / "logs"
            log_dir.mkdir(parents=True, exist_ok=True)
            with (log_dir / "claude-hooks.log").open("a", encoding="utf-8") as log:
                log.write(f"{utc_now()} {type(exc).__name__}: {exc}\n")
        except OSError:
            pass
    # Hook stdout is shown to Claude for some events, so stay silent.
    sys.exit(0)


if __name__ == "__main__":
    main()
