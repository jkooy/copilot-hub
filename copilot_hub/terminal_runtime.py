from __future__ import annotations

import asyncio
import errno
import fcntl
import hashlib
import inspect
import json
import logging
import os
import pty
import re
import select
import shutil
import signal
import sqlite3
import struct
import subprocess
import termios
import threading
import time
import uuid
from collections.abc import Callable
from contextlib import nullcontext
from dataclasses import dataclass, field
from datetime import UTC, datetime
from functools import partial
from pathlib import Path
from typing import Any

from fastapi import WebSocket, WebSocketDisconnect

from .config import Settings
from .db import Repository, utc_now
from .presentation import sanitize_context_text

logger = logging.getLogger(__name__)


@dataclass
class PtySession:
    worker_id: str
    master_fd: int
    process: subprocess.Popen[bytes]
    process_group: int | None = None
    copilot_session_id: str = ""
    terminal_generation: int = 0
    resume_session_id: str | None = None
    launch_task_id: str | None = None
    launch_activity_kind: str | None = None
    launch_direct_generation: int = 0
    launch_direct_submitted_at: str | None = None
    resume_confirmation_sent: bool = False
    resume_confirmation_sent_at: float | None = None
    stream_id: str = field(default_factory=lambda: uuid.uuid4().hex)
    buffer: bytearray = field(default_factory=bytearray)
    base_offset: int = 0
    lock: threading.Lock = field(default_factory=threading.Lock)
    fd_lock: threading.Lock = field(default_factory=threading.Lock)
    prompt_dispatch_lock: threading.RLock = field(default_factory=threading.RLock)
    terminated: threading.Event = field(default_factory=threading.Event)
    prompt_input: TerminalPromptInput = field(
        default_factory=lambda: TerminalPromptInput()
    )

    MAX_BUFFER_BYTES = 20 * 1024 * 1024

    @property
    def prompt_input_dirty(self) -> bool:
        return self.prompt_input.dirty

    @prompt_input_dirty.setter
    def prompt_input_dirty(self, value: bool) -> None:
        if value:
            self.prompt_input.known = False
        else:
            self.prompt_input.reset()

    def append(self, data: bytes) -> None:
        with self.lock:
            self.buffer.extend(data)
            excess = len(self.buffer) - self.MAX_BUFFER_BYTES
            if excess > 0:
                del self.buffer[:excess]
                self.base_offset += excess

    def read_from(self, offset: int) -> tuple[bytes, int]:
        with self.lock:
            offset = max(offset, self.base_offset)
            index = offset - self.base_offset
            data = bytes(self.buffer[index:])
            return data, self.base_offset + len(self.buffer)

    def end_offset(self) -> int:
        with self.lock:
            return self.base_offset + len(self.buffer)

    def write(self, data: bytes) -> int:
        with self.fd_lock:
            if self.master_fd < 0:
                raise RuntimeError("Copilot terminal is closed")
            try:
                return os.write(self.master_fd, data)
            except OSError as exc:
                if exc.errno == errno.EBADF:
                    raise RuntimeError("Copilot terminal is closed") from exc
                raise

    def close_master_fd(self) -> None:
        with self.fd_lock:
            if self.master_fd < 0:
                return
            master_fd = self.master_fd
            self.master_fd = -1
            try:
                os.close(master_fd)
            except OSError:
                pass

    def replay_snapshot(
        self,
        requested_stream_id: str | None,
        requested_offset: int | None,
    ) -> tuple[dict[str, Any], bytes]:
        with self.lock:
            end_offset = self.base_offset + len(self.buffer)
            can_resume = (
                requested_stream_id == self.stream_id
                and requested_offset is not None
                and self.base_offset <= requested_offset <= end_offset
            )
            replay_offset = requested_offset if can_resume else self.base_offset
            index = replay_offset - self.base_offset
            return (
                {
                    "type": "terminal_stream",
                    "stream_id": self.stream_id,
                    "reset": not can_resume,
                    "base_offset": self.base_offset,
                    "offset": replay_offset,
                    "replay_end": end_offset,
                },
                bytes(self.buffer[index:]),
            )


@dataclass
class DirectTurnState:
    accepted_interactions: list[str] = field(default_factory=list)
    completed: bool = False
    event_count: int = 0
    latest_user_message: str | None = None


@dataclass
class AssistantTurnLifecycle:
    interaction_id: str | None
    turn_id: int
    start_index: int
    end_index: int | None = None
    messages: list[tuple[int, str, str | None]] = field(default_factory=list)


@dataclass(frozen=True)
class InteractionLifecycleState:
    settled: bool
    final_content: str | None
    final_turn_id: int | None
    final_turn_end_index: int | None
    active_tool_call_ids: tuple[str, ...] = ()
    active_subagent_call_ids: tuple[str, ...] = ()
    active_shell_ids: tuple[str, ...] = ()
    unfinished_turn_ids: tuple[int, ...] = ()


@dataclass(frozen=True)
class UserInputRequest:
    source: str
    reason: str
    requested_at: str | None
    interaction_id: str | None
    turn_id: int | None
    tool_call_id: str | None


@dataclass
class TerminalPromptInput:
    text: str = ""
    known: bool = True
    bracketed_paste: bool = False

    @property
    def dirty(self) -> bool:
        return not self.known or bool(self.text)

    def reset(self) -> None:
        self.text = ""
        self.known = True
        self.bracketed_paste = False

    def consume(self, data: str) -> tuple[str | None, bool]:
        if data == "\x1b":
            self.reset()
            return None, True
        submitted: str | None = None
        cancelled = False
        index = 0
        while index < len(data):
            if data.startswith("\x1b[200~", index):
                self.bracketed_paste = True
                index += 6
                continue
            if data.startswith("\x1b[201~", index):
                self.bracketed_paste = False
                index += 6
                continue
            character = data[index]
            if character in {"\x03", "\x15"}:
                self.reset()
                cancelled = True
            elif character == "\r" and not self.bracketed_paste:
                submitted = self.text if self.known and self.text.strip() else None
                self.reset()
            elif character in {"\r", "\n"} and self.bracketed_paste:
                if self.known:
                    self.text += "\n"
            elif character in {"\x7f", "\b"}:
                if self.known:
                    self.text = self.text[:-1]
            elif character == "\x1b":
                self.known = False
                if index + 1 < len(data) and data[index + 1] == "[":
                    index += 2
                    while index < len(data) and not "@" <= data[index] <= "~":
                        index += 1
                    if index < len(data):
                        index += 1
                    continue
            elif character == "\t":
                self.known = False
            elif character >= " ":
                if self.known:
                    self.text += character
            else:
                self.known = False
            index += 1
        return submitted, cancelled


class TerminalRuntime:
    DIRECT_PROMPT_ACCEPTANCE_GRACE_SECONDS = 5
    DIRECT_ACTIVITY_STALL_SECONDS = 10 * 60
    DIRECT_WATCHDOG_INTERVAL_SECONDS = 2
    RESUME_CONFIRMATION_PERSIST_SECONDS = 2
    PTY_READ_POLL_SECONDS = 0.05
    PTY_EXIT_IDLE_GRACE_SECONDS = 0.25
    PTY_EXIT_DRAIN_MAX_SECONDS = 2
    ACCEPTED_RECOVERY_STARTUP_SECONDS = 5
    UNKNOWN_DIRECT_PROMPT_HASH = "generation-only"
    _ANSI_OSC = re.compile(r"\x1b\].*?(?:\x07|\x1b\\)", re.DOTALL)
    _ANSI_CSI = re.compile(r"\x1b\[[0-?]*[ -/]*[@-~]")
    _SGR_MOUSE_INPUT = re.compile(r"(?:\x1b\[<\d+;\d+;\d+[Mm])+\Z")
    _TERMINAL_REPORT_INPUT = re.compile(
        r"(?:"
        r"\x1b\[[?>]?[0-9;:]*\$?[cnyR]"
        r"|\x1b\[[IO]"
        r"|\x1b\][^\x07]*(?:\x07|\x1b\\)"
        r"|\x1bP.*?\x1b\\"
        r")+\Z",
        re.DOTALL,
    )
    _RESUME_CONFLICT_PROMPT = re.compile(
        r"""
        Session\s+in\s+use
        .*?This\s+session\s+was\s+last\s+active\b
        .*?appears\s+to\s+be\s+in\s+use\s+by\s+another\s+CLI\s+or\s+application\.
        .*?Resuming\s+it\s+here\s+may\s+cause\s+conflicts\.
        .*?\u276f\s*1\.\s*Resume\s+anyway
        .*?2\.\s*Go\s+back\s*\(Esc\)
        .*?\u2191/\u2193\s+to\s+navigate
        \s*\u00b7\s*enter\s+to\s+select
        \s*\u00b7\s*esc\s+to\s+cancel
        """,
        re.IGNORECASE | re.DOTALL | re.VERBOSE,
    )
    _INPUT_REQUIRED = re.compile(
        r"""(?ix)
        \b(?:
          need(?:ed)?\s+(?:your|a)\s+(?:input|decision|choice|confirmation)|
          waiting\s+for\s+(?:your|a)\s+(?:input|decision|choice|confirmation)|
          before\s+(?:i|we)\s+can\s+(?:continue|proceed|finish)|
          cannot\s+(?:continue|proceed|finish)\s+(?:until|without)|
          please\s+(?:choose|select|confirm|provide|decide|tell\s+me)|
          choose\s+(?:one|between)|
          should\s+i\s+(?:continue|proceed|use|select|choose)|
          (?:can|could)\s+you\s+(?:confirm|provide|choose|select|decide)
        )\b
        """
    )
    _QUESTION_START = re.compile(
        r"(?i)^(?:which|what|where|when|who|how|should|can|could|do|does|is|are|will|would)\b"
    )
    _COMPLETION_LEAD = re.compile(
        r"(?i)^\s*(?:\*\*)?(?:outcome|completed|done|implemented|fixed|finished|succeeded|verified|result)\b"
    )
    _OPTIONAL_FOLLOW_UP = re.compile(r"(?i)\bwould\s+you\s+like\s+me\s+to\b")

    def __init__(self, repository: Repository, settings: Settings):
        self.repository = repository
        self.settings = settings
        self.instructions_dir = str(Path(__file__).parent / "agent_instructions")
        self._sessions: dict[str, PtySession] = {}
        self._clients: dict[str, tuple[str, WebSocket]] = {}
        self._lock = threading.RLock()
        self._last_activity_updates: dict[str, float] = {}
        self._direct_monitors: dict[str, threading.Thread] = {}
        self._direct_interrupts: dict[str, int] = {}
        self._direct_watchdog: threading.Thread | None = None
        self._direct_watchdog_stop = threading.Event()
        self._resume_confirmations: set[tuple[str, str, int]] = set()
        self._folder_trust_confirmations: set[tuple[str, str, int]] = set()
        self._last_process_exits: dict[str, dict[str, Any]] = {}
        self._worker_idle_callback: Callable[[], None] | None = None
        self._worker_recovery_callback: Callable[[str], None] | None = None
        self._shutting_down = False

    @staticmethod
    def _check_guard(guard: Callable[[], None] | None) -> None:
        if guard:
            guard()

    @classmethod
    def _guarded_sleep(
        cls,
        seconds: float,
        guard: Callable[[], None] | None,
    ) -> None:
        cls._check_guard(guard)
        deadline = time.monotonic() + seconds
        while time.monotonic() < deadline:
            cls._check_guard(guard)
            time.sleep(min(0.05, max(0.0, deadline - time.monotonic())))
        cls._check_guard(guard)

    @staticmethod
    def _call_with_supported_kwargs(
        method: Callable[..., Any],
        *args: Any,
        **kwargs: Any,
    ) -> Any:
        parameters = inspect.signature(method).parameters.values()
        if any(parameter.kind == inspect.Parameter.VAR_KEYWORD for parameter in parameters):
            supported = kwargs
        else:
            names = {parameter.name for parameter in parameters}
            supported = {key: value for key, value in kwargs.items() if key in names}
        return method(*args, **supported)

    def _apply_browser_input_once(
        self,
        session: PtySession,
        *,
        worker_id: str,
        client_id: str,
        sequence: int,
        data: str,
        before_write: Callable[[], None] | None = None,
    ) -> bool:
        if sequence and not self.repository.accept_terminal_input_sequence(
            worker_id,
            client_id,
            sequence,
        ):
            return False
        # Persist acceptance before side effects and the PTY write. A crash in the
        # remaining gap can drop input, but a retry cannot apply it twice.
        if before_write is not None:
            before_write()
        session.write(data.encode())
        return True

    @staticmethod
    def _mark_prompt_input_unknown(session: PtySession) -> None:
        if session.prompt_input_dirty:
            session.prompt_input.known = False

    def _apply_prompt_input_side_effects(
        self,
        *,
        session: PtySession,
        worker: dict[str, Any],
        data: str,
        message: dict[str, Any],
        attached_session_id: str,
        attached_terminal_generation: int,
        outcome: dict[str, Any],
    ) -> None:
        draft_was_dirty = session.prompt_input_dirty
        tracked_prompt, cancelled = session.prompt_input.consume(data)
        outcome["released_draft"] = (
            draft_was_dirty and not session.prompt_input_dirty
        )
        if cancelled or message.get("prompt_cancel") is True:
            outcome["released_pending"] = (
                self.repository.cancel_pending_direct_activity(worker["id"])
            )
        if data == "\x1b" and worker.get("direct_submitted_at"):
            outcome["direct_interrupt_generation"] = self.request_direct_interrupt(
                worker,
                write_escape=False,
            )
        if (
            message.get("prompt_submit") is True
            and (draft_was_dirty or tracked_prompt is not None)
        ):
            submitted_prompt = message.get("prompt")
            if not isinstance(submitted_prompt, str):
                submitted_prompt = tracked_prompt
            outcome["direct_activity"] = self.track_direct_prompt_submission(
                worker,
                submitted_prompt if isinstance(submitted_prompt, str) else None,
                copilot_session_id=attached_session_id,
                terminal_generation=attached_terminal_generation,
            )
        self.touch_worker(worker["id"])

    def set_worker_idle_callback(self, callback: Callable[[], None]) -> None:
        self._worker_idle_callback = callback

    def set_worker_recovery_callback(
        self,
        callback: Callable[[str], None],
    ) -> None:
        self._worker_recovery_callback = callback

    @staticmethod
    def _proc_process_group(proc_dir: Path) -> int | None:
        try:
            fields = (proc_dir / "stat").read_text().rsplit(") ", 1)[1].split()
            return int(fields[2])
        except (IndexError, OSError, ValueError):
            return None

    def _managed_terminal_process_groups(
        self,
        proc_root: Path = Path("/proc"),
    ) -> set[int]:
        try:
            expected_state_dir = self.settings.state_dir.expanduser().resolve()
            configured_executable = self.settings.copilot_executable
            resolved_executable = shutil.which(configured_executable) or configured_executable
            executable_paths = {
                str(Path(configured_executable).expanduser()),
                str(Path(resolved_executable).expanduser()),
                str(Path(resolved_executable).expanduser().resolve()),
            }
            entries = list(proc_root.iterdir())
        except OSError:
            return set()

        process_groups: set[int] = set()
        for proc_dir in entries:
            if not proc_dir.name.isdigit():
                continue
            try:
                if proc_dir.stat().st_uid != os.getuid():
                    continue
                environment = {
                    key.decode(errors="ignore"): value.decode(errors="ignore")
                    for item in (proc_dir / "environ").read_bytes().split(b"\0")
                    if item and b"=" in item
                    for key, value in [item.split(b"=", 1)]
                }
                state_dir = environment.get("COPILOT_HUB_STATE_DIR")
                if (
                    not state_dir
                    or Path(state_dir).expanduser().resolve() != expected_state_dir
                    or not environment.get("COPILOT_HUB_WORKER_ID")
                    or not environment.get("COPILOT_HUB_SESSION_ID")
                ):
                    continue
                arguments = [
                    item.decode(errors="ignore")
                    for item in (proc_dir / "cmdline").read_bytes().split(b"\0")
                    if item
                ]
            except (OSError, ValueError):
                continue
            if not executable_paths.intersection(arguments):
                continue
            if not {
                "--yolo",
                "--no-ask-user",
                "--mouse=on",
                "--no-remote-export",
            }.issubset(arguments):
                continue
            pid = int(proc_dir.name)
            process_group = self._proc_process_group(proc_dir)
            if process_group == pid and process_group != os.getpgrp():
                process_groups.add(process_group)
        return process_groups

    @staticmethod
    def _process_group_exists(process_group: int) -> bool:
        try:
            os.killpg(process_group, 0)
        except ProcessLookupError:
            return False
        try:
            members = []
            for proc_dir in Path("/proc").iterdir():
                if not proc_dir.name.isdigit():
                    continue
                try:
                    fields = (proc_dir / "stat").read_text().rsplit(") ", 1)[1].split()
                    if int(fields[2]) == process_group:
                        members.append(fields[0])
                except (IndexError, OSError, ValueError):
                    continue
        except OSError:
            return True
        if members:
            return any(state != "Z" for state in members)
        try:
            os.killpg(process_group, 0)
        except ProcessLookupError:
            return False
        return True

    def cleanup_orphaned_terminals(
        self,
        *,
        proc_root: Path = Path("/proc"),
        timeout_seconds: float = 5,
    ) -> list[int]:
        with self._lock:
            if self._sessions:
                raise RuntimeError("Cannot clean orphaned terminals after recovery starts")
        process_groups = sorted(self._managed_terminal_process_groups(proc_root))
        if process_groups:
            logger.warning(
                "Cleaning orphaned Hub terminal process groups",
                extra={
                    "event_type": "terminal.orphans_cleanup_started",
                    "process_groups": process_groups,
                },
            )
        for process_group in process_groups:
            try:
                os.killpg(process_group, signal.SIGTERM)
            except ProcessLookupError:
                pass
        deadline = time.monotonic() + timeout_seconds
        while (
            any(self._process_group_exists(group) for group in process_groups)
            and time.monotonic() < deadline
        ):
            time.sleep(0.05)
        for process_group in process_groups:
            if not self._process_group_exists(process_group):
                continue
            try:
                os.killpg(process_group, signal.SIGKILL)
            except ProcessLookupError:
                pass
        kill_deadline = time.monotonic() + max(
            0.25,
            min(timeout_seconds, 1.0),
        )
        while (
            any(self._process_group_exists(group) for group in process_groups)
            and time.monotonic() < kill_deadline
        ):
            time.sleep(0.05)
        survivors = [
            group for group in process_groups if self._process_group_exists(group)
        ]
        if survivors:
            raise RuntimeError(
                "Could not stop orphaned Hub terminal process groups: "
                + ", ".join(str(group) for group in survivors)
            )
        for worker in self.repository.list_agent_workers(include_retired=True):
            if (
                worker.get("pid") in process_groups
                or worker.get("pgid") in process_groups
            ):
                self.repository.update_agent_worker(
                    worker["id"],
                    pid=None,
                    pgid=None,
                    heartbeat_at=utc_now(),
                )
        if process_groups:
            logger.info(
                "Cleaned orphaned Hub terminal process groups",
                extra={
                    "event_type": "terminal.orphans_cleanup_completed",
                    "process_groups": process_groups,
                },
            )
        return process_groups

    def recover(self) -> None:
        if not self.settings.terminals_enabled:
            return
        self._shutting_down = False
        try:
            self._start_direct_watchdog()
            for persisted in self.repository.list_agent_workers():
                worker = self.reconcile_pending_direct_activity(persisted)
                self.ensure_terminal(worker, wait_for_ready=False)
                if worker.get("direct_submitted_at"):
                    self._start_direct_monitor(worker["id"])
        except Exception:
            self.shutdown()
            raise

    def reconcile_pending_direct_activity(
        self,
        worker: dict[str, Any],
    ) -> dict[str, Any]:
        if not worker.get("direct_submitted_at") or worker.get("direct_interaction_id"):
            return worker
        events = self.events_after(
            worker["copilot_session_id"],
            int(worker.get("direct_event_cursor") or 0),
        )
        prompt_hash = worker.get("direct_prompt_hash")
        matched = self.matching_direct_prompt(events, str(prompt_hash)) if prompt_hash else None
        if (
            matched is None
            and worker.get("current_task_id") is None
            and worker.get("activity_kind") == "direct"
        ):
            interaction_id = self.direct_lifecycle_interaction(events)
            if interaction_id:
                matched = (interaction_id, None)
        if matched:
            interaction_id, content = matched
            context = sanitize_context_text(content) or None if content is not None else None
            self.repository.accept_direct_activity(
                worker["id"],
                generation=int(worker.get("direct_generation") or 0),
                interaction_id=interaction_id,
                context=context,
            )
        return self.repository.get_agent_worker(worker["id"]) or worker

    def _manager(self) -> dict[str, Any]:
        managers = self.repository.list_agent_workers(role="manager")
        if not managers:
            raise RuntimeError("Manager worker is not initialized")
        return managers[0]

    def copilot_args(self, worker: dict[str, Any]) -> list[str]:
        args = [
            self.settings.copilot_executable,
            "--model",
            worker["model"],
            "--effort",
            worker["reasoning_effort"],
            "--context",
            worker["context_tier"],
            "--yolo",
            "--no-ask-user",
            "--mouse=on",
            "--no-remote-export",
            "--add-dir",
            self.settings.agent_default_cwd,
        ]
        session_path = Path("~/.copilot/session-state").expanduser() / worker["copilot_session_id"]
        if int(worker.get("turn_count", 0)) > 0 or session_path.exists():
            args.append(f"--resume={worker['copilot_session_id']}")
        else:
            args.extend(
                [
                    "--session-id",
                    worker["copilot_session_id"],
                    "--name",
                    worker["name"],
                ]
            )
        return args

    @staticmethod
    def _set_size(fd: int, rows: int, cols: int) -> None:
        fcntl.ioctl(
            fd,
            termios.TIOCSWINSZ,
            struct.pack("HHHH", rows, cols, 0, 0),
        )

    def resize_terminal(self, session: PtySession, rows: int, cols: int) -> None:
        with session.fd_lock:
            self._set_size(session.master_fd, rows, cols)
        self.request_redraw(session)

    @staticmethod
    def request_redraw(session: PtySession) -> None:
        try:
            os.killpg(os.getpgid(session.process.pid), signal.SIGWINCH)
        except ProcessLookupError:
            pass

    def ensure_terminal(
        self,
        worker: dict[str, Any],
        *,
        wait_for_ready: bool = True,
        guard: Callable[[], None] | None = None,
        expected_task_id: str | None = None,
        require_task_owner: bool = False,
    ) -> PtySession:
        if worker.get("retired_at"):
            raise RuntimeError("Retired workers cannot start terminal sessions")
        self._check_guard(guard)
        created = False
        with self._lock:
            self._check_guard(guard)
            existing = self._sessions.get(worker["id"])
            if (
                existing
                and existing.process.poll() is None
                and existing.copilot_session_id == worker["copilot_session_id"]
                and existing.terminal_generation == int(worker.get("terminal_generation") or 0)
            ):
                session = existing
            else:
                if existing:
                    self._sessions.pop(worker["id"], None)
                    self._terminate_session(existing)
                    existing.close_master_fd()

                self._check_guard(guard)
                master_fd, slave_fd = pty.openpty()
                self._set_size(slave_fd, 36, 120)
                copilot_args = self.copilot_args(worker)
                resume_session_id = next(
                    (
                        arg.removeprefix("--resume=")
                        for arg in copilot_args
                        if arg.startswith("--resume=")
                    ),
                    None,
                )
                env = {
                    **os.environ,
                    "COPILOT_CUSTOM_INSTRUCTIONS_DIRS": self.instructions_dir,
                    "COPILOT_HUB_ROLE": worker["role"],
                    "COPILOT_HUB_WORKER_ID": worker["id"],
                    "COPILOT_HUB_SESSION_ID": worker["copilot_session_id"],
                    "COPILOT_HUB_TERMINAL_GENERATION": str(
                        int(worker.get("terminal_generation") or 0)
                    ),
                    "COPILOT_HUB_STATE_DIR": str(self.settings.state_dir),
                    "COPILOT_HUB_PORT": str(self.settings.port),
                    "COPILOT_HUB_PROJECT_ROOT": str(Path(__file__).parent.parent),
                    "TERM": "xterm-256color",
                }
                self._check_guard(guard)
                try:
                    process = subprocess.Popen(
                        copilot_args,
                        cwd=worker.get("cwd") or self.settings.agent_default_cwd,
                        stdin=slave_fd,
                        stdout=slave_fd,
                        stderr=slave_fd,
                        env=env,
                        close_fds=True,
                        start_new_session=True,
                    )
                except Exception:
                    os.close(master_fd)
                    os.close(slave_fd)
                    raise
                os.close(slave_fd)
                session = PtySession(
                    worker_id=worker["id"],
                    master_fd=master_fd,
                    process=process,
                    process_group=os.getpgid(process.pid),
                    copilot_session_id=worker["copilot_session_id"],
                    terminal_generation=int(worker.get("terminal_generation") or 0),
                    resume_session_id=resume_session_id,
                    launch_task_id=worker.get("current_task_id"),
                    launch_activity_kind=worker.get("activity_kind"),
                    launch_direct_generation=int(worker.get("direct_generation") or 0),
                    launch_direct_submitted_at=worker.get("direct_submitted_at"),
                )
                try:
                    self._check_guard(guard)
                except Exception:
                    self._terminate_session(session)
                    raise
                self._sessions[worker["id"]] = session
                recorded = self.repository.set_agent_worker_process(
                    worker["id"],
                    copilot_session_id=worker["copilot_session_id"],
                    terminal_generation=int(worker.get("terminal_generation") or 0),
                    pid=process.pid,
                    pgid=os.getpgid(process.pid),
                    expected_task_id=expected_task_id,
                    require_task_owner=require_task_owner,
                )
                if not recorded:
                    self._sessions.pop(worker["id"], None)
                    self._terminate_session(session)
                    raise RuntimeError("Worker changed while the Copilot terminal was starting")
                reader = threading.Thread(
                    target=self._read_loop,
                    args=(session,),
                    name=f"agent-pty-{worker['id'][:8]}",
                    daemon=True,
                )
                try:
                    reader.start()
                except Exception:
                    if self._sessions.get(worker["id"]) is session:
                        self._sessions.pop(worker["id"], None)
                    self.repository.clear_agent_worker_process(
                        worker["id"],
                        pid=process.pid,
                        copilot_session_id=worker["copilot_session_id"],
                        terminal_generation=int(worker.get("terminal_generation") or 0),
                    )
                    self._terminate_session(session)
                    raise
                created = True
        if wait_for_ready:
            self._wait_until_ready(session, fresh=not created, guard=guard)
        self._check_guard(guard)
        return session

    def touch_worker(self, worker_id: str, *, force: bool = False) -> None:
        now = time.monotonic()
        with self._lock:
            last_update = self._last_activity_updates.get(worker_id, 0.0)
            if not force and now - last_update < 5:
                return
            self._last_activity_updates[worker_id] = now
        self.repository.update_agent_worker(worker_id, heartbeat_at=utc_now())

    @classmethod
    def _is_terminal_transport_input(cls, data: str) -> bool:
        return bool(
            cls._SGR_MOUSE_INPUT.fullmatch(data)
            or cls._TERMINAL_REPORT_INPUT.fullmatch(data)
        )

    def shutdown(self) -> None:
        self._shutting_down = True
        self._direct_watchdog_stop.set()
        watchdog = self._direct_watchdog
        if watchdog and watchdog is not threading.current_thread():
            watchdog.join(timeout=2)
        with self._lock:
            sessions = list(self._sessions.values())
            self._sessions.clear()
        for session in sessions:
            session.terminated.set()
            self._signal_session_process_group(session, signal.SIGTERM)
        deadline = time.monotonic() + 5
        while time.monotonic() < deadline:
            if all(
                session.process.poll() is not None
                and not self._session_process_group_exists(session)
                for session in sessions
            ):
                break
            time.sleep(0.05)
        for session in sessions:
            if (
                session.process.poll() is None
                or self._session_process_group_exists(session)
            ):
                self._signal_session_process_group(session, signal.SIGKILL)
            session.close_master_fd()
            self.repository.update_agent_worker(
                session.worker_id,
                pid=None,
                pgid=None,
                heartbeat_at=utc_now(),
            )

    def terminate_worker_terminal(
        self,
        worker_id: str,
        *,
        expected_pid: int | None = None,
        expected_pgid: int | None = None,
    ) -> None:
        with self._lock:
            candidate = self._sessions.get(worker_id)
            if (
                candidate is not None
                and expected_pid is not None
                and candidate.process.pid != expected_pid
            ):
                session = None
            else:
                session = self._sessions.pop(worker_id, None)
        pid = expected_pid
        if session is not None:
            pid = session.process.pid
            self._terminate_session(session)
        elif expected_pgid:
            try:
                os.killpg(expected_pgid, signal.SIGTERM)
            except ProcessLookupError:
                pass
        if pid is not None:
            self.repository.clear_agent_worker_process(worker_id, pid=pid)

    def _read_loop(self, session: PtySession) -> None:
        process_exit_at: float | None = None
        last_output_at: float | None = None
        try:
            while True:
                now = time.monotonic()
                if session.process.poll() is not None and process_exit_at is None:
                    process_exit_at = now
                    last_output_at = now
                if (
                    process_exit_at is not None
                    and now - process_exit_at >= self.PTY_EXIT_DRAIN_MAX_SECONDS
                ):
                    break
                data: bytes | None = None
                with session.fd_lock:
                    master_fd = session.master_fd
                if master_fd < 0:
                    break
                try:
                    readable, _, _ = select.select(
                        [master_fd],
                        [],
                        [],
                        self.PTY_READ_POLL_SECONDS,
                    )
                except (OSError, ValueError):
                    break
                if readable:
                    with session.fd_lock:
                        if session.master_fd != master_fd:
                            break
                        try:
                            data = os.read(master_fd, 65536)
                        except OSError:
                            break
                if data is None:
                    if (
                        process_exit_at is not None
                        and last_output_at is not None
                        and time.monotonic() - last_output_at
                        >= self.PTY_EXIT_IDLE_GRACE_SECONDS
                    ):
                        break
                    continue
                if not data:
                    break
                session.append(data)
                if process_exit_at is not None:
                    last_output_at = time.monotonic()
        finally:
            session.close_master_fd()
            if not self._shutting_down:
                self._reap_session_process_group(session)
            return_code = session.process.poll()
            process_group_reaped = not self._session_process_group_exists(session)
            with self._lock:
                self._last_process_exits[session.worker_id] = {
                    "session_id": session.copilot_session_id,
                    "terminal_generation": session.terminal_generation,
                    "pid": getattr(session.process, "pid", None),
                    "process_group": self._session_process_group(session),
                    "wrapper_return_code": return_code,
                    "process_group_reaped": process_group_reaped,
                }
            session.append(f"\r\n[Copilot process exited: {return_code}]\r\n".encode())
            self._handle_process_exit(session)

    @staticmethod
    def _session_process_group(session: PtySession) -> int | None:
        if session.process_group is not None:
            return session.process_group
        pid = getattr(session.process, "pid", None)
        if not isinstance(pid, int):
            return None
        try:
            return os.getpgid(pid)
        except ProcessLookupError:
            return None

    @classmethod
    def _session_process_group_exists(cls, session: PtySession) -> bool:
        process_group = cls._session_process_group(session)
        return bool(process_group and cls._process_group_exists(process_group))

    @classmethod
    def _signal_session_process_group(
        cls,
        session: PtySession,
        sig: signal.Signals,
    ) -> None:
        process_group = cls._session_process_group(session)
        if process_group is None:
            return
        try:
            os.killpg(process_group, sig)
        except ProcessLookupError:
            pass

    @classmethod
    def _reap_session_process_group(
        cls,
        session: PtySession,
        timeout_seconds: float = 3,
    ) -> None:
        if cls._session_process_group(session) is None:
            return
        cls._signal_session_process_group(session, signal.SIGTERM)
        wait = getattr(session.process, "wait", None)
        deadline = time.monotonic() + timeout_seconds
        while time.monotonic() < deadline:
            return_code = session.process.poll()
            if return_code is not None and wait is None:
                return
            if return_code is not None and not cls._session_process_group_exists(session):
                return
            time.sleep(0.05)
        cls._signal_session_process_group(session, signal.SIGKILL)
        if wait is not None:
            try:
                wait(timeout=timeout_seconds)
            except subprocess.TimeoutExpired:
                pass

    @classmethod
    def _terminate_session(cls, session: PtySession, timeout_seconds: float = 3) -> None:
        session.terminated.set()
        cls._reap_session_process_group(session, timeout_seconds)
        session.close_master_fd()

    def _handle_process_exit(self, session: PtySession) -> None:
        session.terminated.set()
        if self._shutting_down:
            return
        with self._lock:
            if self._sessions.get(session.worker_id) is not session:
                return
            self._sessions.pop(session.worker_id, None)
        pid = getattr(session.process, "pid", None)
        if pid is not None:
            self.repository.clear_agent_worker_process(
                session.worker_id,
                pid=pid,
                copilot_session_id=session.copilot_session_id,
                terminal_generation=session.terminal_generation,
            )
        worker = self.repository.get_agent_worker(session.worker_id)
        if not worker or worker.get("activity_kind") != "direct":
            return
        generation = self.repository.mark_direct_activity_recovering(
            session.worker_id,
            generation=int(worker.get("direct_generation") or 0),
            error=f"Copilot terminal exited with status {session.process.poll()}",
        )
        if generation is not None:
            self._notify_worker_recovery(session.worker_id)

    @classmethod
    def _is_resume_conflict_prompt(cls, text: str) -> bool:
        visible = cls._ANSI_CSI.sub("", cls._ANSI_OSC.sub("", text))
        visible = re.sub(r"[\u2500-\u257f]", " ", visible)
        return bool(cls._RESUME_CONFLICT_PROMPT.search(visible))

    @staticmethod
    def _resume_confirmation_key(session: PtySession) -> tuple[str, str, int]:
        return (
            session.worker_id,
            session.copilot_session_id,
            session.terminal_generation,
        )

    def _resume_confirmation_was_sent(self, session: PtySession) -> bool:
        with self._lock:
            return self._resume_confirmation_key(session) in self._resume_confirmations

    def _assert_resume_confirmation_ownership(self, session: PtySession) -> dict[str, Any]:
        if session.resume_session_id != session.copilot_session_id:
            raise RuntimeError(
                "Copilot resume conflict prompt targeted a different session"
            )
        return self._assert_terminal_prompt_ownership(
            session,
            prompt_name="resume confirmation",
        )

    def _assert_terminal_prompt_ownership(
        self,
        session: PtySession,
        *,
        prompt_name: str,
    ) -> dict[str, Any]:
        current = self.repository.get_agent_worker(session.worker_id)
        launch_ownership_matches = bool(
            current
            and current.get("current_task_id") == session.launch_task_id
            and current.get("activity_kind") == session.launch_activity_kind
            and int(current.get("direct_generation") or 0)
            == session.launch_direct_generation
            and current.get("direct_submitted_at")
            == session.launch_direct_submitted_at
        )
        launched_idle = (
            session.launch_task_id is None
            and session.launch_activity_kind is None
            and session.launch_direct_submitted_at is None
        )
        current_ownership_is_consistent = bool(
            current
            and (
                (
                    current.get("activity_kind") is None
                    and current.get("current_task_id") is None
                    and current.get("direct_submitted_at") is None
                )
                or (
                    current.get("activity_kind") == "task"
                    and current.get("current_task_id") is not None
                    and current.get("direct_submitted_at") is None
                )
                or (
                    current.get("activity_kind") == "direct"
                    and current.get("current_task_id") is None
                    and current.get("direct_submitted_at") is not None
                )
            )
        )
        if (
            not current
            or current["copilot_session_id"] != session.copilot_session_id
            or int(current.get("terminal_generation") or 0)
            != session.terminal_generation
            or not (
                launch_ownership_matches
                or (launched_idle and current_ownership_is_consistent)
            )
        ):
            raise RuntimeError(
                f"Worker session ownership changed before Copilot {prompt_name}"
            )
        return current

    def _confirm_resume_conflict(
        self,
        session: PtySession,
        guard: Callable[[], None] | None,
    ) -> bool:
        if session.resume_session_id is None:
            return False
        key = self._resume_confirmation_key(session)
        with self._lock:
            if key in self._resume_confirmations:
                session.resume_confirmation_sent = True
                return False
            self._check_guard(guard)
            current = self._assert_resume_confirmation_ownership(session)
            self._check_guard(guard)
            written = session.write(b"\r")
            if written != 1:
                raise RuntimeError(
                    "Failed to submit Copilot resume conflict confirmation"
                )
            sent_at = time.monotonic()
            self._resume_confirmations.add(key)
            session.resume_confirmation_sent = True
            session.resume_confirmation_sent_at = sent_at
        logger.info(
            "Handled Copilot resume conflict confirmation",
            extra={
                "event_type": "terminal.resume_conflict_confirmed",
                "worker_id": session.worker_id,
                "copilot_session_id": session.copilot_session_id,
                "terminal_generation": session.terminal_generation,
                "activity_kind": current.get("activity_kind"),
                "task_id": current.get("current_task_id"),
            },
        )
        return True

    def _folder_trust_confirmation_was_sent(self, session: PtySession) -> bool:
        with self._lock:
            return self._resume_confirmation_key(session) in self._folder_trust_confirmations

    def _confirm_folder_trust(
        self,
        session: PtySession,
        guard: Callable[[], None] | None,
    ) -> bool:
        key = self._resume_confirmation_key(session)
        with self._lock:
            if key in self._folder_trust_confirmations:
                return False
            self._check_guard(guard)
            current = self._assert_terminal_prompt_ownership(
                session,
                prompt_name="folder trust confirmation",
            )
            self._check_guard(guard)
            written = session.write(b"2\r")
            if written != 2:
                raise RuntimeError("Failed to submit Copilot folder trust confirmation")
            self._folder_trust_confirmations.add(key)
        logger.info(
            "Handled Copilot folder trust confirmation",
            extra={
                "event_type": "terminal.folder_trust_confirmed",
                "worker_id": session.worker_id,
                "copilot_session_id": session.copilot_session_id,
                "terminal_generation": session.terminal_generation,
                "activity_kind": current.get("activity_kind"),
                "task_id": current.get("current_task_id"),
            },
        )
        return True

    def _wait_until_ready(
        self,
        session: PtySession,
        timeout_seconds: float = 180,
        *,
        fresh: bool = False,
        guard: Callable[[], None] | None = None,
    ) -> None:
        self._check_guard(guard)
        offset = session.end_offset() if fresh else 0
        screen = b""
        if fresh:
            self.request_redraw(session)
        deadline = time.monotonic() + timeout_seconds
        resume_prompt_reappeared_at: float | None = None
        folder_trust_prompt_reappeared_at: float | None = None
        while time.monotonic() < deadline:
            self._check_guard(guard)
            if session.process.poll() is not None:
                raise RuntimeError(
                    f"Copilot terminal exited before becoming ready: {session.process.poll()}"
                )
            data, offset = session.read_from(offset)
            if data:
                screen = (screen + data)[-100000:]
                clear_at = screen.rfind(b"\x1b[H\x1b[2J")
                if clear_at >= 0:
                    screen = screen[clear_at:]
            text = screen.decode(errors="ignore")
            normalized_text = text.lower()
            ready_at = normalized_text.rfind("open sidebar")
            resume_prompt_visible = (
                self._is_resume_conflict_prompt(text)
                and normalized_text.rfind("session in use") > ready_at
            )
            confirmation_sent = self._resume_confirmation_was_sent(session)
            if confirmation_sent:
                self._check_guard(guard)
                self._assert_resume_confirmation_ownership(session)
            if resume_prompt_visible and session.resume_session_id is not None:
                if not confirmation_sent:
                    self._confirm_resume_conflict(session, guard)
                    screen = b""
                    resume_prompt_reappeared_at = None
                    continue
                if resume_prompt_reappeared_at is None:
                    resume_prompt_reappeared_at = time.monotonic()
                elif (
                    time.monotonic() - resume_prompt_reappeared_at
                    >= self.RESUME_CONFIRMATION_PERSIST_SECONDS
                ):
                    raise RuntimeError(
                        "Copilot resume conflict confirmation persisted after it was handled"
                    )
            else:
                resume_prompt_reappeared_at = None
            folder_trust_prompt_visible = (
                max(
                    normalized_text.rfind("confirm folder trust"),
                    normalized_text.rfind("do you trust the files"),
                )
                > ready_at
            )
            if folder_trust_prompt_visible:
                if not self._folder_trust_confirmation_was_sent(session):
                    self._confirm_folder_trust(session, guard)
                    screen = b""
                    folder_trust_prompt_reappeared_at = None
                    continue
                if folder_trust_prompt_reappeared_at is None:
                    folder_trust_prompt_reappeared_at = time.monotonic()
                elif (
                    time.monotonic() - folder_trust_prompt_reappeared_at
                    >= self.RESUME_CONFIRMATION_PERSIST_SECONDS
                ):
                    raise RuntimeError(
                        "Copilot folder trust confirmation persisted after it was handled"
                    )
                self._guarded_sleep(0.1, guard)
                continue
            folder_trust_prompt_reappeared_at = None
            paste_at = normalized_text.rfind("[paste #")
            working_at = normalized_text.rfind("working esc interrupt")
            pending_paste = paste_at >= 0 and not (
                working_at > paste_at and ready_at > working_at
            )
            blocking_at = max(
                working_at,
                normalized_text.rfind("resuming session"),
                normalized_text.rfind("session in use"),
            )
            if ready_at >= 0 and ready_at > blocking_at and not pending_paste:
                self._check_guard(guard)
                return
            self._guarded_sleep(0.1, guard)
        self._check_guard(guard)
        if self._resume_confirmation_was_sent(session):
            raise RuntimeError(
                "Timed out waiting for Copilot terminal readiness after handling "
                "the resume conflict confirmation"
            )
        if self._folder_trust_confirmation_was_sent(session):
            raise RuntimeError(
                "Timed out waiting for Copilot terminal readiness after handling "
                "the folder trust confirmation"
            )
        raise RuntimeError("Timed out waiting for Copilot terminal readiness")

    def render_prompt(
        self,
        worker: dict[str, Any],
        prompt: str,
        *,
        timeout_seconds: float = 5,
        wait_for_ready: bool = True,
        expected_session: PtySession | None = None,
        guard: Callable[[], None] | None = None,
        expected_task_id: str | None = None,
        require_task_owner: bool = False,
    ) -> None:
        self._check_guard(guard)
        if expected_session is None:
            session = self._call_with_supported_kwargs(
                self.ensure_terminal,
                worker,
                wait_for_ready=wait_for_ready,
                guard=guard,
                expected_task_id=expected_task_id,
                require_task_owner=require_task_owner,
            )
        else:
            with self._lock:
                if self._sessions.get(worker["id"]) is not expected_session:
                    raise RuntimeError("Manager terminal changed before prompt rendering")
            session = expected_session
        self.touch_worker(worker["id"])
        session.write(b"\x15")
        self._guarded_sleep(0.1, guard)
        offset = session.end_offset()
        session.write(
            b"\x1b[200~" + prompt.encode() + b"\x1b[201~",
        )
        deadline = time.monotonic() + timeout_seconds
        visible_lines = [
            line.encode()
            for line in prompt.splitlines()
            if line.strip()
        ]
        render_markers = {
            visible_lines[0],
            visible_lines[-1],
        }
        rendered = b""
        while time.monotonic() < deadline:
            self._check_guard(guard)
            data, offset = session.read_from(offset)
            if data:
                rendered = (rendered + data)[-50000:]
            if b"[Paste #" in rendered or any(
                marker in rendered for marker in render_markers
            ):
                break
            self._guarded_sleep(0.05, guard)
        else:
            raise RuntimeError("Copilot did not render the dispatched prompt")
        self._check_guard(guard)

    def submit_rendered_prompt(
        self,
        worker: dict[str, Any],
        *,
        wait_for_ready: bool = False,
        expected_session: PtySession | None = None,
        guard: Callable[[], None] | None = None,
        expected_task_id: str | None = None,
        require_task_owner: bool = False,
    ) -> None:
        self._check_guard(guard)
        if expected_session is None:
            session = self._call_with_supported_kwargs(
                self.ensure_terminal,
                worker,
                wait_for_ready=wait_for_ready,
                guard=guard,
                expected_task_id=expected_task_id,
                require_task_owner=require_task_owner,
            )
        else:
            with self._lock:
                if self._sessions.get(worker["id"]) is not expected_session:
                    raise RuntimeError("Manager terminal changed before prompt submission")
            session = expected_session
        self._guarded_sleep(0.05, guard)
        session.write(b"\r")

    def send_prompt(
        self,
        worker: dict[str, Any],
        prompt: str,
        *,
        guard: Callable[[], None] | None = None,
        expected_task_id: str | None = None,
        require_task_owner: bool = False,
    ) -> None:
        self.render_prompt(
            worker,
            prompt,
            guard=guard,
            expected_task_id=expected_task_id,
            require_task_owner=require_task_owner,
        )
        self.submit_rendered_prompt(
            worker,
            wait_for_ready=False,
            guard=guard,
            expected_task_id=expected_task_id,
            require_task_owner=require_task_owner,
        )

    def send_interruption_recovery_prompt(
        self,
        worker: dict[str, Any],
        prompt: str,
        *,
        event_cursor: int,
        guard: Callable[[], None] | None = None,
        before_submit: Callable[[], bool] | None = None,
        ready_timeout_seconds: float = 12,
        render_timeout_seconds: float = 5,
        expected_task_id: str,
    ) -> bool:
        session = self._call_with_supported_kwargs(
            self.ensure_terminal,
            worker,
            wait_for_ready=False,
            guard=guard,
            expected_task_id=expected_task_id,
            require_task_owner=True,
        )
        self._wait_until_ready(
            session,
            timeout_seconds=ready_timeout_seconds,
            fresh=False,
            guard=guard,
        )
        with session.prompt_dispatch_lock:
            with self._lock:
                if self._sessions.get(worker["id"]) is not session:
                    raise RuntimeError(
                        "Copilot terminal changed before interruption recovery"
                    )
                if session.prompt_input_dirty:
                    return False
            if self.has_top_level_user_message_after(
                session.copilot_session_id,
                event_cursor,
            ):
                return False
            prompt_may_be_visible = True
            try:
                self.render_prompt(
                    worker,
                    prompt,
                    timeout_seconds=render_timeout_seconds,
                    wait_for_ready=False,
                    expected_session=session,
                    guard=guard,
                    expected_task_id=expected_task_id,
                    require_task_owner=True,
                )
                if before_submit is not None and not before_submit():
                    return False
                self.submit_rendered_prompt(
                    worker,
                    wait_for_ready=False,
                    expected_session=session,
                    guard=guard,
                    expected_task_id=expected_task_id,
                    require_task_owner=True,
                )
                prompt_may_be_visible = False
            finally:
                if prompt_may_be_visible:
                    try:
                        session.write(b"\x15")
                    except (OSError, RuntimeError):
                        logger.warning(
                            "Could not clear an unsubmitted interruption recovery prompt",
                            extra={"worker_id": worker["id"]},
                        )
                    session.prompt_input.reset()
        return True

    def send_prompt_if_input_clear(
        self,
        worker: dict[str, Any],
        prompt: str,
        *,
        guard: Callable[[], None] | None = None,
        before_submit: Callable[[], bool] | None = None,
        ready_timeout_seconds: float = 12,
        render_timeout_seconds: float = 1.5,
        expected_task_id: str | None = None,
        require_task_owner: bool = False,
    ) -> bool:
        session = self._call_with_supported_kwargs(
            self.ensure_terminal,
            worker,
            wait_for_ready=False,
            guard=guard,
            expected_task_id=expected_task_id,
            require_task_owner=require_task_owner,
        )
        activity_cursor = self.event_cursor(session.copilot_session_id)
        self._wait_until_ready(
            session,
            timeout_seconds=ready_timeout_seconds,
            fresh=False,
            guard=guard,
        )
        with session.prompt_dispatch_lock:
            with self._lock:
                if self._sessions.get(worker["id"]) is not session:
                    raise RuntimeError("Manager terminal changed before result delivery")
                if session.prompt_input_dirty:
                    return False
            if self.has_top_level_user_message_after(
                session.copilot_session_id,
                activity_cursor,
            ) or self.session_has_unsettled_interaction(
                session.copilot_session_id,
            ):
                return False
            if before_submit is not None and not before_submit():
                return False
            prompt_may_be_visible = True
            try:
                self.render_prompt(
                    worker,
                    prompt,
                    timeout_seconds=render_timeout_seconds,
                    wait_for_ready=False,
                    expected_session=session,
                    guard=guard,
                    expected_task_id=expected_task_id,
                    require_task_owner=require_task_owner,
                )
                self.submit_rendered_prompt(
                    worker,
                    wait_for_ready=False,
                    expected_session=session,
                    guard=guard,
                    expected_task_id=expected_task_id,
                    require_task_owner=require_task_owner,
                )
                prompt_may_be_visible = False
            finally:
                if prompt_may_be_visible:
                    try:
                        session.write(b"\x15")
                    except (OSError, RuntimeError):
                        logger.warning(
                            "Could not clear an unsubmitted automatic prompt",
                            extra={"worker_id": worker["id"]},
                        )
                    session.prompt_input.reset()
        return True

    def clear_current_prompt(self, worker: dict[str, Any]) -> None:
        session = self.ensure_terminal(worker, wait_for_ready=False)
        session.write(b"\x15")

    def wait_until_ready(
        self,
        worker: dict[str, Any],
        *,
        timeout_seconds: float = 180,
        guard: Callable[[], None] | None = None,
        expected_task_id: str | None = None,
        require_task_owner: bool = False,
    ) -> None:
        self._check_guard(guard)
        session = self.ensure_terminal(
            worker,
            wait_for_ready=False,
            guard=guard,
            expected_task_id=expected_task_id,
            require_task_owner=require_task_owner,
        )
        self._wait_until_ready(
            session,
            timeout_seconds=timeout_seconds,
            fresh=True,
            guard=guard,
        )

    def recover_prompt_submission(
        self,
        worker: dict[str, Any],
        *,
        session_id: str,
        cursor: int,
        prompt: str,
        timeout_seconds: float,
        guard: Callable[[], None] | None = None,
        expected_task_id: str | None = None,
    ) -> str | None:
        self._check_guard(guard)
        interaction_id = self.find_prompt_after(session_id, cursor, prompt)
        if interaction_id:
            return interaction_id
        current = self.repository.get_agent_worker(worker["id"])
        if (
            not current
            or current["copilot_session_id"] != session_id
            or int(current.get("terminal_generation") or 0)
            != int(worker.get("terminal_generation") or 0)
        ):
            raise RuntimeError("Worker terminal generation changed during prompt recovery")
        self.wait_until_ready(
            current,
            timeout_seconds=timeout_seconds,
            guard=guard,
            expected_task_id=expected_task_id,
            require_task_owner=expected_task_id is not None,
        )
        self._check_guard(guard)
        interaction_id = self.find_prompt_after(session_id, cursor, prompt)
        if interaction_id:
            return interaction_id
        self.clear_current_prompt(current)
        return None

    def recycle_terminal(
        self,
        worker: dict[str, Any],
        *,
        expected_terminal_generation: int,
        expected_task_id: str | None,
        recovery_attempt: int,
        recovery_error: str,
        timeout_seconds: float,
        expected_task_generation: int | None = None,
        expected_recovery_generation: int | None = None,
        guard: Callable[[], None] | None = None,
    ) -> dict[str, Any]:
        self._check_guard(guard)
        with self._lock:
            self._check_guard(guard)
            rebound = self.repository.rebind_agent_worker_session(
                worker["id"],
                expected_terminal_generation=expected_terminal_generation,
                expected_task_id=expected_task_id,
                expected_task_generation=expected_task_generation,
                expected_recovery_generation=expected_recovery_generation,
                copilot_session_id=str(uuid.uuid4()),
                recovery_attempt=recovery_attempt,
                recovery_error=recovery_error,
            )
            if rebound is None:
                raise RuntimeError("Worker changed while terminal recovery was in progress")
            previous = self._sessions.pop(worker["id"], None)
        if previous:
            self._terminate_session(previous)
        self._check_guard(guard)
        session = self._call_with_supported_kwargs(
            self.ensure_terminal,
            rebound,
            wait_for_ready=False,
            guard=guard,
            expected_task_id=expected_task_id,
            require_task_owner=expected_task_id is not None,
        )
        self._wait_until_ready(
            session,
            timeout_seconds=timeout_seconds,
            fresh=False,
            guard=guard,
        )
        self._check_guard(guard)
        return self.repository.get_agent_worker(worker["id"]) or rebound

    def recover_accepted_task_terminal(
        self,
        worker: dict[str, Any],
        *,
        task_id: str,
        dispatch_generation: int,
        expected_session_id: str,
        expected_terminal_generation: int,
        expected_recovery_generation: int,
        recovery_attempt: int,
        recovery_error: str,
        guard: Callable[[], None] | None = None,
    ) -> dict[str, Any]:
        self._check_guard(guard)
        with self._lock:
            self._check_guard(guard)
            rebound = self.repository.begin_agent_task_terminal_recovery(
                worker["id"],
                task_id,
                dispatch_generation=dispatch_generation,
                expected_session_id=expected_session_id,
                expected_terminal_generation=expected_terminal_generation,
                expected_recovery_generation=expected_recovery_generation,
                recovery_attempt=recovery_attempt,
                recovery_error=recovery_error,
            )
            if rebound is None:
                raise RuntimeError(
                    "Accepted task ownership changed during terminal recovery"
                )
            previous = self._sessions.pop(worker["id"], None)
        if previous is not None:
            self._terminate_session(previous)
            if self._session_process_group_exists(previous):
                with self._lock:
                    self._sessions.setdefault(worker["id"], previous)
                raise RuntimeError(
                    "Prior Copilot process group survived terminal recovery cleanup"
                )
        self._check_guard(guard)
        session: PtySession | None = None
        try:
            session = self._call_with_supported_kwargs(
                self.ensure_terminal,
                rebound,
                wait_for_ready=False,
                guard=guard,
                expected_task_id=task_id,
                require_task_owner=True,
            )
            try:
                self._wait_until_ready(
                    session,
                    timeout_seconds=self.ACCEPTED_RECOVERY_STARTUP_SECONDS,
                    fresh=False,
                    guard=guard,
                )
            except RuntimeError as exc:
                if session.process.poll() is not None or not str(exc).startswith(
                    "Timed out waiting for Copilot terminal readiness"
                ):
                    raise
        except RuntimeError:
            with self._lock:
                current = self._sessions.get(worker["id"])
                if session is not None and current is session:
                    self._sessions.pop(worker["id"], None)
            if session is not None and current is session:
                self._terminate_session(session)
                self.repository.clear_agent_worker_process(
                    worker["id"],
                    pid=session.process.pid,
                    copilot_session_id=rebound["copilot_session_id"],
                    terminal_generation=int(rebound["terminal_generation"]),
                )
            raise
        self._check_guard(guard)
        return self.repository.get_agent_worker(worker["id"]) or rebound

    def terminal_exit_status(self, worker: dict[str, Any]) -> dict[str, Any] | None:
        with self._lock:
            value = self._last_process_exits.get(worker["id"])
            if (
                value is None
                or value["session_id"] != worker["copilot_session_id"]
                or int(value["terminal_generation"])
                != int(worker.get("terminal_generation") or 0)
            ):
                return None
            return dict(value)

    @staticmethod
    def event_cursor(session_id: str) -> int:
        path = Path("~/.copilot/session-state").expanduser() / session_id / "events.jsonl"
        return path.stat().st_size if path.exists() else 0

    @staticmethod
    def events_after(session_id: str, cursor: int) -> list[dict[str, Any]]:
        path = Path("~/.copilot/session-state").expanduser() / session_id / "events.jsonl"
        if not path.exists():
            return []
        with path.open("rb") as stream:
            stream.seek(cursor)
            data = stream.read().decode(errors="ignore")
        events = []
        for line in data.splitlines():
            try:
                events.append(json.loads(line))
            except json.JSONDecodeError:
                continue
        return events

    @classmethod
    def has_top_level_user_message_after(
        cls,
        session_id: str,
        cursor: int,
    ) -> bool:
        return any(
            event.get("agentId") is None
            and event.get("type") == "user.message"
            for event in cls.events_after(session_id, cursor)
        )

    @classmethod
    def session_has_unsettled_interaction(cls, session_id: str) -> bool:
        events = cls.events_after(session_id, 0)
        interaction_id = next(
            (
                cls._interaction_id(event.get("data") or {})
                for event in reversed(events)
                if event.get("agentId") is None
                and event.get("type") == "user.message"
            ),
            None,
        )
        if interaction_id is None:
            return False
        return not cls._interaction_lifecycle(events, interaction_id).settled

    @staticmethod
    def direct_prompt_hash(prompt: str) -> str:
        normalized = prompt.replace("\r\n", "\n").replace("\r", "\n").strip()
        return hashlib.sha256(normalized.encode()).hexdigest()

    @classmethod
    def matching_direct_prompt(
        cls,
        events: list[dict[str, Any]],
        prompt_hash: str,
    ) -> tuple[str, str] | None:
        for event in events:
            if event.get("agentId") is not None or event.get("type") != "user.message":
                continue
            data = event.get("data") or {}
            interaction_id = data.get("interactionId")
            content = data.get("content")
            if (
                isinstance(interaction_id, str)
                and interaction_id.strip()
                and isinstance(content, str)
                and (
                    prompt_hash == cls.UNKNOWN_DIRECT_PROMPT_HASH
                    or cls.direct_prompt_hash(content) == prompt_hash
                )
            ):
                return interaction_id, content
        return None

    @staticmethod
    def direct_lifecycle_interaction(
        events: list[dict[str, Any]],
    ) -> str | None:
        for event in events:
            if event.get("agentId") is not None or event.get("type") not in {
                "assistant.turn_start",
                "assistant.message",
                "tool.execution_start",
            }:
                continue
            interaction_id = (event.get("data") or {}).get("interactionId")
            if isinstance(interaction_id, str) and interaction_id.strip():
                return interaction_id
        return None

    @classmethod
    def find_prompt_after(
        cls,
        session_id: str,
        cursor: int,
        prompt: str,
    ) -> str | None:
        for event in cls.events_after(session_id, cursor):
            if event.get("agentId") is not None or event.get("type") != "user.message":
                continue
            data = event.get("data") or {}
            content = data.get("content")
            interaction_id = data.get("interactionId")
            if (
                isinstance(content, str)
                and content.strip() == prompt.strip()
                and isinstance(interaction_id, str)
            ):
                return interaction_id
        return None

    @classmethod
    def wait_for_prompt_after(
        cls,
        session_id: str,
        cursor: int,
        prompt: str,
        timeout_seconds: float = 30,
        guard: Callable[[], None] | None = None,
    ) -> str:
        cls._check_guard(guard)
        deadline = time.monotonic() + timeout_seconds
        while time.monotonic() < deadline:
            cls._check_guard(guard)
            interaction_id = cls.find_prompt_after(session_id, cursor, prompt)
            if interaction_id:
                cls._check_guard(guard)
                return interaction_id
            cls._guarded_sleep(0.1, guard)
        cls._check_guard(guard)
        raise RuntimeError("Copilot did not accept the dispatched prompt")

    @staticmethod
    def _interaction_id(data: dict[str, Any]) -> str | None:
        value = data.get("interactionId")
        return value if isinstance(value, str) and value else None

    @staticmethod
    def _background_shell_id(data: dict[str, Any]) -> str | None:
        telemetry = data.get("toolTelemetry")
        properties = telemetry.get("properties") if isinstance(telemetry, dict) else None
        execution_mode = (
            str(properties.get("executionMode") or "") if isinstance(properties, dict) else ""
        )
        detached = (
            str(properties.get("detached") or "").lower() == "true"
            if isinstance(properties, dict)
            else False
        )
        if execution_mode != "async" and not detached:
            return None
        result = data.get("result")
        fragments: list[str] = []
        if isinstance(result, str):
            fragments.append(result)
        elif isinstance(result, dict):
            fragments.extend(value for value in result.values() if isinstance(value, str))
        match = re.search(
            r"\bshellId\s*:\s*([A-Za-z0-9_.-]+)",
            "\n".join(fragments),
        )
        return match.group(1) if match else None

    @classmethod
    def _interaction_lifecycle(
        cls,
        events: list[dict[str, Any]],
        interaction_id: str,
    ) -> InteractionLifecycleState:
        turns: list[AssistantTurnLifecycle] = []
        turns_by_id: dict[int, list[AssistantTurnLifecycle]] = {}
        target_user_index: int | None = None
        tool_starts: dict[str, tuple[int, str | None, str | None]] = {}
        tool_completes: dict[str, tuple[int, dict[str, Any]]] = {}
        subagent_starts: dict[str, tuple[int, str | None]] = {}
        subagent_finishes: set[str] = set()
        completed_shells: set[str] = set()

        def latest_turn(
            turn_id: int,
            *,
            owner: str | None = None,
            open_only: bool = False,
        ) -> AssistantTurnLifecycle | None:
            for turn in reversed(turns_by_id.get(turn_id, [])):
                if open_only and turn.end_index is not None:
                    continue
                if owner is not None and turn.interaction_id not in {None, owner}:
                    continue
                return turn
            return None

        def ensure_turn(
            turn_id: int,
            owner: str | None,
            index: int,
        ) -> AssistantTurnLifecycle:
            turn = latest_turn(turn_id, owner=owner, open_only=True)
            if turn is None:
                turn = AssistantTurnLifecycle(
                    interaction_id=owner,
                    turn_id=turn_id,
                    start_index=index,
                )
                turns.append(turn)
                turns_by_id.setdefault(turn_id, []).append(turn)
            elif turn.interaction_id is None and owner is not None:
                turn.interaction_id = owner
            return turn

        for index, event in enumerate(events):
            if event.get("agentId") is not None:
                continue
            event_type = event.get("type")
            data = event.get("data") or {}
            if not isinstance(data, dict):
                continue
            owner = cls._interaction_id(data)
            turn_id = cls._event_turn_index(data.get("turnId"))

            if event_type == "user.message":
                if owner == interaction_id and target_user_index is None:
                    target_user_index = index
                continue

            if event_type == "assistant.turn_start" and turn_id is not None:
                ensure_turn(turn_id, owner, index)
                continue

            if event_type == "assistant.message" and turn_id is not None:
                turn = ensure_turn(turn_id, owner, index)
                content = data.get("content")
                if isinstance(content, str) and content:
                    phase = data.get("phase") or data.get("messageType")
                    turn.messages.append(
                        (
                            index,
                            content,
                            phase if isinstance(phase, str) and phase else None,
                        )
                    )
                continue

            if event_type == "assistant.turn_end" and turn_id is not None:
                turn = latest_turn(turn_id, open_only=True)
                if turn is not None:
                    turn.end_index = index
                continue

            if event_type == "tool.execution_start":
                tool_call_id = data.get("toolCallId")
                if not tool_call_id:
                    continue
                if owner is None and turn_id is not None:
                    turn = latest_turn(turn_id)
                    owner = turn.interaction_id if turn else None
                tool_starts[str(tool_call_id)] = (
                    index,
                    owner,
                    str(data.get("toolName") or "") or None,
                )
                continue

            if event_type == "tool.execution_complete":
                tool_call_id = data.get("toolCallId")
                if tool_call_id:
                    tool_completes[str(tool_call_id)] = (index, data)
                continue

            if event_type == "subagent.started":
                tool_call_id = data.get("toolCallId")
                if not tool_call_id:
                    continue
                key = str(tool_call_id)
                start = tool_starts.get(key)
                subagent_starts[key] = (
                    index,
                    start[1] if start else owner,
                )
                continue

            if event_type in {"subagent.completed", "subagent.failed"}:
                tool_call_id = data.get("toolCallId")
                if tool_call_id:
                    subagent_finishes.add(str(tool_call_id))
                continue

            if event_type == "system.notification":
                kind = data.get("kind")
                if not isinstance(kind, dict):
                    continue
                if kind.get("type") not in {
                    "shell_completed",
                    "shell_detached_completed",
                }:
                    continue
                shell_id = kind.get("shellId")
                if shell_id is not None:
                    completed_shells.add(str(shell_id))

        target_turns = [turn for turn in turns if turn.interaction_id == interaction_id]
        anchor_index = target_user_index
        if anchor_index is None and target_turns:
            anchor_index = min(turn.start_index for turn in target_turns)

        def relevant(index: int, owner: str | None) -> bool:
            if owner is not None:
                return owner == interaction_id
            return anchor_index is not None and index >= anchor_index

        active_tools = sorted(
            tool_call_id
            for tool_call_id, (index, owner, _) in tool_starts.items()
            if relevant(index, owner) and tool_call_id not in tool_completes
        )
        active_subagents = sorted(
            tool_call_id
            for tool_call_id, (index, owner) in subagent_starts.items()
            if relevant(index, owner) and tool_call_id not in subagent_finishes
        )
        active_shells: list[str] = []
        for tool_call_id, (index, owner, tool_name) in tool_starts.items():
            if tool_name != "bash" or not relevant(index, owner):
                continue
            completion = tool_completes.get(tool_call_id)
            if completion is None:
                continue
            shell_id = cls._background_shell_id(completion[1])
            if shell_id and shell_id not in completed_shells:
                active_shells.append(shell_id)

        unfinished_turns = sorted({turn.turn_id for turn in target_turns if turn.end_index is None})
        final_turns = [
            turn for turn in target_turns if turn.end_index is not None and turn.messages
        ]
        authoritative_turns = [
            turn
            for turn in final_turns
            if any(message[2] == "final_answer" for message in turn.messages)
        ]
        final_turn = None
        final_message: tuple[int, str, str | None] | None = None
        if authoritative_turns:
            final_turn = min(
                authoritative_turns,
                key=lambda turn: (
                    turn.end_index if turn.end_index is not None else -1,
                    turn.start_index,
                ),
            )
            final_message = next(
                message for message in reversed(final_turn.messages) if message[2] == "final_answer"
            )
        elif final_turns:
            final_turn = max(
                final_turns,
                key=lambda turn: (
                    turn.end_index if turn.end_index is not None else -1,
                    turn.messages[-1][0],
                ),
            )
            final_message = final_turn.messages[-1]
        settled = bool(target_turns) and not (
            active_tools or active_subagents or active_shells or unfinished_turns
        )
        return InteractionLifecycleState(
            settled=settled,
            final_content=(final_message[1] if final_message else None),
            final_turn_id=(final_turn.turn_id if final_turn else None),
            final_turn_end_index=(final_turn.end_index if final_turn else None),
            active_tool_call_ids=tuple(active_tools),
            active_subagent_call_ids=tuple(active_subagents),
            active_shell_ids=tuple(sorted(active_shells)),
            unfinished_turn_ids=tuple(unfinished_turns),
        )

    @classmethod
    def response_after_cursor(
        cls,
        session_id: str,
        cursor: int,
        interaction_id: str,
        quiet_seconds: float = 1,
    ) -> str | None:
        events = cls.events_after(session_id, cursor)
        _ = quiet_seconds
        state = cls._interaction_lifecycle(events, interaction_id)
        return state.final_content if state.settled else None

    @classmethod
    def restart_interrupted_tool_state(
        cls,
        session_id: str,
        cursor: int,
        interaction_id: str,
        *,
        resume_grace_seconds: float = 2,
    ) -> dict[str, Any] | None:
        events = cls.events_after(session_id, cursor)
        lifecycle = cls._interaction_lifecycle(events, interaction_id)
        active_ids = set(lifecycle.active_tool_call_ids)
        if not active_ids:
            return None
        tool_starts: dict[str, tuple[int, str]] = {}
        for index, event in enumerate(events):
            if event.get("agentId") is not None or event.get("type") != "tool.execution_start":
                continue
            data = event.get("data") or {}
            tool_call_id = str(data.get("toolCallId") or "")
            if tool_call_id in active_ids:
                tool_starts[tool_call_id] = (
                    index,
                    str(data.get("toolName") or "tool"),
                )
        if not tool_starts:
            return None
        shutdowns = [
            (index, event)
            for index, event in enumerate(events)
            if event.get("agentId") is None and event.get("type") == "session.shutdown"
        ]
        for shutdown_index, shutdown in reversed(shutdowns):
            interrupted = [
                (tool_call_id, name)
                for tool_call_id, (start_index, name) in tool_starts.items()
                if start_index < shutdown_index
            ]
            if not interrupted:
                continue
            resumes = [
                event
                for index, event in enumerate(events)
                if index > shutdown_index
                and event.get("agentId") is None
                and event.get("type") == "session.resume"
            ]
            if not resumes:
                continue
            resumed = resumes[-1]
            resumed_at = resumed.get("timestamp") or (resumed.get("data") or {}).get(
                "resumeTime"
            )
            if cls._seconds_since(resumed_at) < resume_grace_seconds:
                return None
            shutdown_at = shutdown.get("timestamp")
            return {
                "active_tool_calls": [
                    {"id": tool_call_id, "name": name}
                    for tool_call_id, name in sorted(interrupted)
                ],
                "active_subagent_call_ids": list(lifecycle.active_subagent_call_ids),
                "active_shell_ids": list(lifecycle.active_shell_ids),
                "unfinished_turn_ids": list(lifecycle.unfinished_turn_ids),
                "shutdown_at": shutdown_at,
                "resumed_at": resumed_at,
                "shutdown_type": (shutdown.get("data") or {}).get("shutdownType"),
            }
        return None

    def interrupt(self, worker: dict[str, Any]) -> None:
        session = self.ensure_terminal(worker, wait_for_ready=False)
        session.write(b"\x1b")

    def request_direct_interrupt(
        self,
        worker: dict[str, Any],
        *,
        write_escape: bool = True,
    ) -> int | None:
        current = self.repository.get_agent_worker(worker["id"])
        if not current or not current.get("direct_submitted_at"):
            return None
        generation = int(current.get("direct_generation") or 0)
        with self._lock:
            self._direct_interrupts[current["id"]] = generation
        if write_escape:
            session = self.ensure_terminal(current, wait_for_ready=False)
            session.write(b"\x1b")
        self._start_direct_monitor(current["id"])
        return generation

    def terminal_is_running(self, worker: dict[str, Any]) -> bool:
        with self._lock:
            session = self._sessions.get(worker["id"])
        return bool(
            session
            and session.process.poll() is None
            and session.copilot_session_id == worker["copilot_session_id"]
            and session.terminal_generation == int(worker.get("terminal_generation") or 0)
        )

    def has_pending_prompt_input(self, worker: dict[str, Any]) -> bool:
        with self._lock:
            session = self._sessions.get(worker["id"])
            return bool(
                session
                and session.copilot_session_id == worker["copilot_session_id"]
                and session.terminal_generation
                == int(worker.get("terminal_generation") or 0)
                and session.prompt_input_dirty
            )

    def track_direct_prompt_submission(
        self,
        worker: dict[str, Any],
        prompt: str | None,
        *,
        copilot_session_id: str | None = None,
        terminal_generation: int | None = None,
    ) -> dict[str, Any]:
        if prompt is not None and not prompt.strip():
            return {"status": "ignored"}
        current = self.repository.get_agent_worker(worker["id"])
        if not current:
            return {"status": "ignored"}
        if copilot_session_id is not None and current["copilot_session_id"] != copilot_session_id:
            return {"status": "ignored"}
        if (
            terminal_generation is not None
            and int(current.get("terminal_generation") or 0) != terminal_generation
        ):
            return {"status": "ignored"}
        cursor = self.event_cursor(current["copilot_session_id"])
        activity = self.repository.register_direct_activity(
            current["id"],
            event_cursor=cursor,
            prompt_hash=(
                self.direct_prompt_hash(prompt)
                if prompt is not None
                else self.UNKNOWN_DIRECT_PROMPT_HASH
            ),
        )
        if activity["status"] in {"active", "pending"}:
            self._start_direct_monitor(current["id"])
        return activity

    def _start_direct_watchdog(self) -> None:
        with self._lock:
            existing = self._direct_watchdog
            if existing and existing.is_alive():
                return
            self._direct_watchdog_stop.clear()
            thread = threading.Thread(
                target=self._watch_direct_monitors,
                name="agent-direct-watchdog",
                daemon=True,
            )
            self._direct_watchdog = thread
        try:
            thread.start()
        except Exception:
            with self._lock:
                if self._direct_watchdog is thread:
                    self._direct_watchdog = None
            raise

    def _watch_direct_monitors(self) -> None:
        while not self._direct_watchdog_stop.wait(
            self.DIRECT_WATCHDOG_INTERVAL_SECONDS
        ):
            try:
                self._reconcile_direct_monitors_once()
            except Exception:
                logger.exception("Direct activity watchdog reconciliation failed")

    def _reconcile_direct_monitors_once(self) -> None:
        for worker in self.repository.list_agent_workers():
            if worker.get("direct_submitted_at"):
                self._start_direct_monitor(worker["id"])

    @classmethod
    def _current_direct_interaction(
        cls,
        events: list[dict[str, Any]],
        initial_interaction_id: str | None,
    ) -> tuple[str | None, str | None, DirectTurnState]:
        state = cls._direct_turn_state(
            events,
            initial_interaction_id=initial_interaction_id,
        )
        interaction_id = (
            state.accepted_interactions[-1]
            if state.accepted_interactions
            else initial_interaction_id
        )
        return interaction_id, state.latest_user_message, state

    @classmethod
    def _interaction_anchor_index(
        cls,
        events: list[dict[str, Any]],
        interaction_id: str,
    ) -> int | None:
        anchor = None
        for index, event in enumerate(events):
            if event.get("agentId") is not None:
                continue
            data = event.get("data") or {}
            if not isinstance(data, dict):
                continue
            if (
                event.get("type") == "user.message"
                and cls._interaction_id(data) == interaction_id
            ) or (
                anchor is None
                and cls._interaction_id(data) == interaction_id
            ):
                anchor = index
        return anchor

    @classmethod
    def _interaction_has_clean_checkpoint(
        cls,
        events: list[dict[str, Any]],
        interaction_id: str,
    ) -> bool:
        anchor = cls._interaction_anchor_index(events, interaction_id)
        if anchor is None:
            return False
        work_types = {
            "user.message",
            "assistant.turn_start",
            "assistant.message",
            "assistant.turn_end",
            "tool.execution_start",
            "tool.execution_complete",
            "subagent.started",
            "subagent.completed",
            "subagent.failed",
        }
        last_work_index = max(
            (
                index
                for index, event in enumerate(events)
                if index >= anchor
                and event.get("agentId") is None
                and event.get("type") in work_types
            ),
            default=-1,
        )
        return any(
            index > last_work_index
            and event.get("agentId") is None
            and event.get("type") == "session.usage_checkpoint"
            for index, event in enumerate(events)
        )

    def direct_activity_diagnostics(
        self,
        worker: dict[str, Any],
    ) -> dict[str, Any] | None:
        if not worker.get("direct_submitted_at"):
            return None
        cursor = int(worker.get("direct_event_cursor") or 0)
        events = self.events_after(worker["copilot_session_id"], cursor)
        interaction_id, _, _ = self._current_direct_interaction(
            events,
            (
                str(worker["direct_interaction_id"])
                if worker.get("direct_interaction_id")
                else None
            ),
        )
        lifecycle = (
            self._interaction_lifecycle(events, interaction_id)
            if interaction_id
            else InteractionLifecycleState(
                settled=False,
                final_content=None,
                final_turn_id=None,
                final_turn_end_index=None,
            )
        )
        anchor = (
            self._interaction_anchor_index(events, interaction_id)
            if interaction_id
            else None
        )
        relevant_events = [
            event
            for index, event in enumerate(events)
            if (anchor is None or index >= anchor)
            and event.get("agentId") is None
            and event.get("type")
            in {
                "user.message",
                "assistant.turn_start",
                "assistant.message",
                "assistant.turn_end",
                "tool.execution_start",
                "tool.execution_complete",
                "subagent.started",
                "subagent.completed",
                "subagent.failed",
                "system.notification",
            }
        ]
        last_event_at = next(
            (
                str(event["timestamp"])
                for event in reversed(relevant_events)
                if event.get("timestamp")
            ),
            worker.get("activity_updated_at") or worker.get("direct_submitted_at"),
        )
        last_event_age_seconds = int(self._seconds_since(last_event_at))
        active_tool_calls = []
        active_tool_ids = set(lifecycle.active_tool_call_ids)
        for event in relevant_events:
            if event.get("type") != "tool.execution_start":
                continue
            data = event.get("data") or {}
            tool_call_id = str(data.get("toolCallId") or "")
            if tool_call_id not in active_tool_ids:
                continue
            active_tool_calls.append(
                {
                    "id": tool_call_id,
                    "name": str(data.get("toolName") or "tool"),
                }
            )
        active_state = bool(
            lifecycle.active_tool_call_ids
            or lifecycle.active_subagent_call_ids
            or lifecycle.active_shell_ids
            or lifecycle.unfinished_turn_ids
        )
        needs_input = bool(worker.get("needs_user_input"))
        stalled = (
            not needs_input
            and active_state
            and last_event_age_seconds >= self.DIRECT_ACTIVITY_STALL_SECONDS
        )
        if active_tool_calls:
            tool_state = ", ".join(
                f"{call['name']} ({call['id']})" for call in active_tool_calls
            )
        elif lifecycle.active_subagent_call_ids:
            tool_state = "subagent " + ", ".join(lifecycle.active_subagent_call_ids)
        elif lifecycle.active_shell_ids:
            tool_state = "detached shell " + ", ".join(lifecycle.active_shell_ids)
        elif lifecycle.unfinished_turn_ids:
            tool_state = "assistant turn " + ", ".join(
                str(turn_id) for turn_id in lifecycle.unfinished_turn_ids
            )
        else:
            tool_state = None
        stall_detail = None
        if stalled:
            minutes = max(1, last_event_age_seconds // 60)
            stall_detail = (
                f"No direct events for {minutes}m"
                + (f"; active {tool_state}" if tool_state else "")
            )
        return {
            "status": (
                "needs_user_input"
                if needs_input
                else "stalled"
                if stalled
                else "settled_waiting_ready"
                if lifecycle.settled
                else "working"
            ),
            "interaction_id": interaction_id,
            "settled": lifecycle.settled,
            "stalled": stalled,
            "last_event_at": last_event_at,
            "last_event_age_seconds": last_event_age_seconds,
            "active_tool_calls": active_tool_calls,
            "active_subagent_call_ids": list(lifecycle.active_subagent_call_ids),
            "active_shell_ids": list(lifecycle.active_shell_ids),
            "unfinished_turn_ids": list(lifecycle.unfinished_turn_ids),
            "tool_state": tool_state,
            "stall_detail": stall_detail,
        }

    def _reconcile_completed_task_followup(
        self,
        worker: dict[str, Any],
        *,
        direct_lifecycle: InteractionLifecycleState,
    ) -> dict[str, Any]:
        task_id = worker.get("current_task_id")
        if not task_id or not direct_lifecycle.settled:
            return worker
        task = self.repository.get_agent_task(str(task_id))
        if task is None:
            return worker
        terminal_statuses = {"succeeded", "failed", "cancelled", "interrupted"}
        if task.get("status") not in terminal_statuses:
            session_id = task.get("dispatch_session_id")
            cursor = task.get("dispatch_event_cursor")
            interaction_id = task.get("dispatch_interaction_id")
            if (
                not isinstance(session_id, str)
                or not isinstance(cursor, int)
                or not isinstance(interaction_id, str)
            ):
                return worker
            response = self.response_after_cursor(
                session_id,
                cursor,
                interaction_id,
            )
            if response is None:
                return worker
            completed = self.repository.finish_agent_task_assignment(
                str(task_id),
                worker["id"],
                generation=int(task.get("dispatch_generation") or 0),
                status="succeeded",
                result={
                    "response": response,
                    "copilot": {
                        "type": "interactive_terminal",
                        "sessionId": session_id,
                        "interactionId": interaction_id,
                    },
                },
            )
            if completed:
                self.repository.add_agent_message(
                    role="worker",
                    content=response,
                    task_id=str(task_id),
                )
            task = self.repository.get_agent_task(str(task_id)) or task
        if task.get("status") in terminal_statuses:
            self.repository.release_agent_worker_task(worker["id"], str(task_id))
        return self.repository.get_agent_worker(worker["id"]) or worker

    def _finish_settled_direct_activity(
        self,
        worker_id: str,
        *,
        generation: int,
        interaction_id: str,
    ) -> bool:
        current = self.repository.get_agent_worker(worker_id)
        if (
            not current
            or int(current.get("direct_generation") or 0) != generation
            or current.get("current_task_id")
            or current.get("activity_kind") != "direct"
            or current.get("input_state") == "needs_user_input"
        ):
            return False
        cursor = int(current.get("direct_event_cursor") or 0)
        events = self.events_after(current["copilot_session_id"], cursor)
        active_interaction_id, context, _ = self._current_direct_interaction(
            events,
            (
                str(current["direct_interaction_id"])
                if current.get("direct_interaction_id")
                else None
            ),
        )
        if active_interaction_id != interaction_id:
            if active_interaction_id:
                self.repository.touch_direct_activity(
                    worker_id,
                    generation=generation,
                    interaction_id=active_interaction_id,
                    context=sanitize_context_text(context) or None,
                )
            return False
        lifecycle = self._interaction_lifecycle(events, interaction_id)
        if not lifecycle.settled:
            return False
        if self.user_input_request_after_cursor(
            current["copilot_session_id"],
            cursor,
            interaction_id=interaction_id,
        ):
            return False
        return self.repository.finish_direct_activity(
            worker_id,
            generation=generation,
            interaction_id=interaction_id,
        )

    def _start_direct_monitor(self, worker_id: str) -> None:
        with self._lock:
            existing = self._direct_monitors.get(worker_id)
            if existing and existing.is_alive():
                return
            thread = threading.Thread(
                target=self._monitor_direct_activity,
                args=(worker_id,),
                name=f"agent-direct-{worker_id[:8]}",
                daemon=True,
            )
            self._direct_monitors[worker_id] = thread
        try:
            thread.start()
        except Exception:
            with self._lock:
                if self._direct_monitors.get(worker_id) is thread:
                    self._direct_monitors.pop(worker_id, None)
            raise

    def _monitor_direct_activity(self, worker_id: str) -> None:
        last_event_count = -1
        try:
            while True:
                worker = self.repository.get_agent_worker(worker_id)
                if not worker or not worker.get("direct_submitted_at"):
                    return
                generation = int(worker.get("direct_generation") or 0)
                cursor = int(worker.get("direct_event_cursor") or 0)
                events = self.events_after(worker["copilot_session_id"], cursor)
                if not worker.get("direct_interaction_id"):
                    prompt_hash = worker.get("direct_prompt_hash")
                    matched = (
                        self.matching_direct_prompt(events, str(prompt_hash))
                        if prompt_hash
                        else None
                    )
                    if (
                        matched is None
                        and worker.get("current_task_id") is None
                        and worker.get("activity_kind") == "direct"
                    ):
                        interaction_id = self.direct_lifecycle_interaction(events)
                        if interaction_id:
                            matched = (interaction_id, None)
                    if matched:
                        interaction_id, content = matched
                        context = (
                            sanitize_context_text(content) or None if content is not None else None
                        )
                        if self.repository.accept_direct_activity(
                            worker_id,
                            generation=generation,
                            interaction_id=interaction_id,
                            context=context,
                        ):
                            continue
                    if (
                        self._seconds_since(worker.get("direct_submitted_at"))
                        >= self.DIRECT_PROMPT_ACCEPTANCE_GRACE_SECONDS
                    ):
                        standalone = (
                            worker.get("current_task_id") is None
                            and worker.get("activity_kind") == "direct"
                            and worker.get("state") in {"busy", "needs_user_input"}
                        )
                        if worker.get("input_state") == "needs_user_input":
                            time.sleep(0.2)
                            continue
                        if standalone:
                            session = self.ensure_terminal(
                                worker,
                                wait_for_ready=False,
                            )
                            if session.process.poll() is None and not self._ready_after_direct_turn(
                                session
                            ):
                                time.sleep(0.2)
                                continue
                        if self.repository.cancel_pending_direct_activity(
                            worker_id,
                            generation=generation,
                        ):
                            current = self.repository.get_agent_worker(worker_id)
                            if current and current["state"] == "recovering":
                                self._notify_worker_recovery(worker_id)
                            elif current and current["state"] == "idle":
                                self._notify_worker_idle()
                        return
                if worker["state"] == "recovering" and not worker.get("current_task_id"):
                    session = self.ensure_terminal(worker, wait_for_ready=False)
                    if self._ready_after_direct_turn(session):
                        result = self.repository.finish_agent_worker_recovery(
                            worker_id,
                            generation=int(worker["recovery_generation"]),
                        )
                        if result in {"direct", "pending"}:
                            continue
                        if result:
                            self._notify_worker_idle()
                            return
                    time.sleep(0.2)
                    continue
                if not worker.get("direct_interaction_id"):
                    session = self.ensure_terminal(worker, wait_for_ready=False)
                    if session.process.poll() is not None:
                        if self.repository.cancel_pending_direct_activity(
                            worker_id,
                            generation=generation,
                        ):
                            self._notify_worker_idle()
                        return
                    time.sleep(0.2)
                    continue
                if (
                    worker["state"] not in {"busy", "needs_user_input"}
                    and not worker.get("current_task_id")
                ):
                    return
                session = self.ensure_terminal(worker, wait_for_ready=False)
                if session.process.poll() is not None:
                    self._handle_process_exit(session)
                    return
                active_interaction_id, latest_user_message, turn_state = (
                    self._current_direct_interaction(
                        events,
                        str(worker["direct_interaction_id"]),
                    )
                )
                if not active_interaction_id:
                    time.sleep(0.2)
                    continue
                lifecycle = self._interaction_lifecycle(
                    events,
                    active_interaction_id,
                )
                input_request = self.user_input_request_after_cursor(
                    worker["copilot_session_id"],
                    cursor,
                    interaction_id=active_interaction_id,
                )
                if turn_state.event_count != last_event_count:
                    self.repository.touch_direct_activity(
                        worker_id,
                        generation=generation,
                        interaction_id=active_interaction_id,
                        context=sanitize_context_text(latest_user_message) or None,
                    )
                    last_event_count = turn_state.event_count
                with self._lock:
                    interrupted_generation = self._direct_interrupts.get(worker_id)
                    if (
                        interrupted_generation is not None
                        and interrupted_generation != generation
                    ):
                        self._direct_interrupts.pop(worker_id, None)
                        interrupted_generation = None
                if interrupted_generation == generation:
                    ready = self._interaction_has_clean_checkpoint(
                        events,
                        active_interaction_id,
                    ) or self._ready_after_direct_turn(session, timeout_seconds=5)
                    if ready and self.repository.cancel_direct_activity(
                        worker_id,
                        generation=generation,
                        interaction_id=active_interaction_id,
                    ):
                        with self._lock:
                            self._direct_interrupts.pop(worker_id, None)
                        current = self.repository.get_agent_worker(worker_id)
                        if current and current["state"] == "idle":
                            self._notify_worker_idle()
                        return
                if worker.get("current_task_id"):
                    worker = self._reconcile_completed_task_followup(
                        worker,
                        direct_lifecycle=lifecycle,
                    )
                    if not worker.get("current_task_id"):
                        if (
                            worker.get("activity_kind") == "direct"
                            or worker.get("direct_submitted_at")
                        ):
                            continue
                        self._notify_worker_idle()
                        return
                    time.sleep(0.2)
                    continue
                if worker.get("activity_kind") == "recovery":
                    time.sleep(0.2)
                    continue
                if worker.get("activity_kind") != "direct":
                    return
                if input_request:
                    self.repository.set_direct_input_request(
                        worker_id,
                        generation=generation,
                        reason=input_request.reason,
                        source=input_request.source,
                        requested_at=input_request.requested_at,
                        event_cursor=cursor,
                        interaction_id=input_request.interaction_id,
                        tool_call_id=input_request.tool_call_id,
                    )
                elif worker.get("input_state") == "needs_user_input":
                    self.repository.clear_direct_input_request(
                        worker_id,
                        generation=generation,
                    )
                if (
                    lifecycle.settled
                    and input_request is None
                    and (
                        self._interaction_has_clean_checkpoint(
                            events,
                            active_interaction_id,
                        )
                        or self._ready_after_direct_turn(session)
                    )
                    and self._finish_settled_direct_activity(
                        worker_id,
                        generation=generation,
                        interaction_id=active_interaction_id,
                    )
                ):
                    self._notify_worker_idle()
                    return
                time.sleep(0.2)
        except Exception:
            logger.exception("Direct activity monitor failed for worker %s", worker_id)
        finally:
            with self._lock:
                current = self._direct_monitors.get(worker_id)
                if current is threading.current_thread():
                    self._direct_monitors.pop(worker_id, None)

    def _ready_after_direct_turn(
        self,
        session: PtySession,
        *,
        timeout_seconds: float = 3,
    ) -> bool:
        try:
            self._wait_until_ready(
                session,
                timeout_seconds=timeout_seconds,
                fresh=True,
            )
            return True
        except RuntimeError:
            return False

    def _notify_worker_idle(self) -> None:
        callback = self._worker_idle_callback
        if callback:
            callback()

    def _notify_worker_recovery(self, worker_id: str) -> None:
        callback = self._worker_recovery_callback
        if callback:
            callback(worker_id)

    @staticmethod
    def _seconds_since(value: Any) -> float:
        if not value:
            return 0
        try:
            parsed = datetime.fromisoformat(str(value))
        except ValueError:
            return 0
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=UTC)
        return max(0, (datetime.now(UTC) - parsed.astimezone(UTC)).total_seconds())

    @classmethod
    def _direct_turn_state(
        cls,
        events: list[dict[str, Any]],
        *,
        initial_interaction_id: str | None = None,
    ) -> DirectTurnState:
        accepted = [initial_interaction_id] if initial_interaction_id else []
        started = {initial_interaction_id: set()} if initial_interaction_id else {}
        ended = {initial_interaction_id: set()} if initial_interaction_id else {}
        turn_owner: dict[int, str] = {}
        latest_user_message: str | None = None
        tracking = initial_interaction_id is None
        relevant_event_count = 0

        for event in events:
            if event.get("agentId") is not None:
                continue
            event_type = event.get("type")
            data = event.get("data") or {}
            if event_type == "user.message":
                interaction_id = data.get("interactionId")
                if not isinstance(interaction_id, str) or not interaction_id.strip():
                    continue
                if not tracking:
                    if interaction_id != initial_interaction_id:
                        continue
                    tracking = True
                key = interaction_id
                relevant_event_count += 1
                if key not in accepted:
                    accepted.append(key)
                    started[key] = set()
                    ended[key] = set()
                turn_id = cls._event_turn_index(data.get("turnId"))
                if turn_id is not None:
                    turn_owner[turn_id] = key
                content = data.get("content")
                if isinstance(content, str) and content.strip():
                    latest_user_message = content
                continue
            if not tracking:
                event_interaction_id = data.get("interactionId")
                if (
                    not isinstance(event_interaction_id, str)
                    or event_interaction_id != initial_interaction_id
                ):
                    continue
                tracking = True
            if event_type in {"assistant.turn_start", "assistant.message"}:
                turn_id = cls._event_turn_index(data.get("turnId"))
                if turn_id is None:
                    continue
                interaction_id = data.get("interactionId")
                owner = (
                    interaction_id
                    if isinstance(interaction_id, str) and interaction_id in started
                    else turn_owner.get(turn_id)
                )
                if owner is None and accepted:
                    incomplete = [
                        key
                        for key in accepted
                        if not started[key] or not started[key].issubset(ended[key])
                    ]
                    if len(incomplete) == 1:
                        owner = incomplete[0]
                if owner is not None:
                    relevant_event_count += 1
                    turn_owner[turn_id] = owner
                    started[owner].add(turn_id)
                continue
            if event_type == "assistant.turn_end":
                turn_id = cls._event_turn_index(data.get("turnId"))
                if turn_id is None:
                    continue
                owner = turn_owner.get(turn_id)
                if owner is not None:
                    relevant_event_count += 1
                    ended[owner].add(turn_id)

        completed = bool(accepted) and all(
            cls._interaction_lifecycle(events, key).settled for key in accepted
        )
        return DirectTurnState(
            accepted_interactions=accepted,
            completed=completed,
            event_count=relevant_event_count,
            latest_user_message=latest_user_message,
        )

    @classmethod
    def _assistant_input_reason(cls, content: object) -> str | None:
        text = sanitize_context_text(content, limit=500)
        if not text:
            return None
        paragraphs = [paragraph.strip() for paragraph in text.split("\n\n") if paragraph.strip()]
        final_paragraph = paragraphs[-1] if paragraphs else text
        required = cls._INPUT_REQUIRED.search(text)
        if required:
            return final_paragraph
        if not final_paragraph.endswith("?"):
            return None
        question_match = re.search(
            r"(?s)(?:^|[.!]\s+)([^.!?]*\?)\s*$",
            final_paragraph,
        )
        if not question_match:
            return None
        normalized_question = re.sub(
            r"^[#>*\-\d.\s]+",
            "",
            question_match.group(1),
        )
        if not cls._QUESTION_START.match(normalized_question):
            return None
        if cls._COMPLETION_LEAD.search(text) and cls._OPTIONAL_FOLLOW_UP.search(final_paragraph):
            return None
        return final_paragraph

    @staticmethod
    def _ask_user_reason(arguments: object) -> str:
        if not isinstance(arguments, dict):
            return "Copilot is waiting for your response."
        question = sanitize_context_text(arguments.get("question"), limit=400)
        choices = arguments.get("choices")
        labels: list[str] = []
        if isinstance(choices, list):
            for choice in choices[:4]:
                if isinstance(choice, dict):
                    label = next(
                        (
                            choice.get(key)
                            for key in ("label", "name", "value", "description")
                            if choice.get(key)
                        ),
                        None,
                    )
                else:
                    label = choice
                sanitized = sanitize_context_text(label, limit=80)
                if sanitized:
                    labels.append(sanitized)
        if question and labels:
            return sanitize_context_text(
                f"{question} Options: {', '.join(labels)}",
                limit=500,
            )
        return question or "Copilot is waiting for your response."

    @classmethod
    def latest_user_interaction_after_cursor(
        cls,
        session_id: str,
        cursor: int,
    ) -> str | None:
        latest = None
        for event in cls.events_after(session_id, cursor):
            if event.get("agentId") is not None or event.get("type") != "user.message":
                continue
            interaction_id = (event.get("data") or {}).get("interactionId")
            if isinstance(interaction_id, str) and interaction_id:
                latest = interaction_id
        return latest

    @classmethod
    def recovery_interaction_state(
        cls,
        session_id: str,
        cursor: int,
        interaction_id: str,
        *,
        recovery_event_cursor: int | None = None,
        recovery_started_at: str | None = None,
    ) -> tuple[str, str] | None:
        events = cls.events_after(session_id, cursor)
        latest_interaction_id = interaction_id
        for event in events:
            if event.get("agentId") is not None or event.get("type") != "user.message":
                continue
            candidate = (event.get("data") or {}).get("interactionId")
            if isinstance(candidate, str) and candidate:
                latest_interaction_id = candidate

        if recovery_event_cursor is not None:
            recovery_events = cls.events_after(session_id, recovery_event_cursor)
        else:
            recovery_events = []
            if recovery_started_at:
                try:
                    recovery_started = datetime.fromisoformat(recovery_started_at)
                except ValueError:
                    recovery_started = None
                if recovery_started is not None:
                    if recovery_started.tzinfo is None:
                        recovery_started = recovery_started.replace(tzinfo=UTC)
                    for event in events:
                        timestamp = event.get("timestamp")
                        if not isinstance(timestamp, str):
                            continue
                        try:
                            event_time = datetime.fromisoformat(timestamp)
                        except ValueError:
                            continue
                        if event_time.tzinfo is None:
                            event_time = event_time.replace(tzinfo=UTC)
                        if event_time >= recovery_started:
                            recovery_events.append(event)

        work_types = {
            "user.message",
            "assistant.turn_start",
            "assistant.message",
            "assistant.turn_end",
            "tool.execution_start",
            "tool.execution_complete",
            "subagent.started",
            "subagent.completed",
            "subagent.failed",
        }
        progressed = any(
            event.get("agentId") is None and event.get("type") in work_types
            for event in recovery_events
        )
        if not progressed:
            return None

        state = cls._interaction_lifecycle(events, latest_interaction_id)
        if not state.settled or state.final_content is None or state.final_turn_end_index is None:
            return latest_interaction_id, "processing"

        target_user_index = -1
        for index, event in enumerate(events):
            if event.get("agentId") is not None or event.get("type") != "user.message":
                continue
            data = event.get("data") or {}
            if data.get("interactionId") == latest_interaction_id:
                target_user_index = index

        last_work_index = max(
            (
                index
                for index, event in enumerate(events)
                if index >= target_user_index
                and event.get("agentId") is None
                and event.get("type") in work_types
            ),
            default=-1,
        )
        clean_ready = any(
            index > last_work_index
            and event.get("agentId") is None
            and event.get("type") == "session.usage_checkpoint"
            for index, event in enumerate(events)
        )
        return latest_interaction_id, ("ready" if clean_ready else "processing")

    @classmethod
    def user_input_request_after_cursor(
        cls,
        session_id: str,
        cursor: int,
        interaction_id: str | None = None,
    ) -> UserInputRequest | None:
        return cls._user_input_request(
            cls.events_after(session_id, cursor),
            interaction_id=interaction_id,
        )

    @classmethod
    def _user_input_request(
        cls,
        events: list[dict[str, Any]],
        *,
        interaction_id: str | None = None,
    ) -> UserInputRequest | None:
        active_interaction = interaction_id
        turn_owner: dict[int, str] = {}
        if active_interaction is None:
            for event in events:
                if event.get("agentId") is not None or event.get("type") != "user.message":
                    continue
                candidate = (event.get("data") or {}).get("interactionId")
                if isinstance(candidate, str) and candidate:
                    active_interaction = candidate
        for event in events:
            if event.get("agentId") is not None:
                continue
            data = event.get("data") or {}
            turn_id = cls._event_turn_index(data.get("turnId"))
            candidate = data.get("interactionId")
            if turn_id is not None and isinstance(candidate, str) and candidate:
                turn_owner[turn_id] = candidate

        completed_calls = {
            str((event.get("data") or {}).get("toolCallId"))
            for event in events
            if event.get("agentId") is None
            and event.get("type") == "tool.execution_complete"
            and (event.get("data") or {}).get("toolCallId")
        }
        pending_asks: list[tuple[int, dict[str, Any], str | None]] = []
        for index, event in enumerate(events):
            if event.get("agentId") is not None or event.get("type") != "tool.execution_start":
                continue
            data = event.get("data") or {}
            if data.get("toolName") != "ask_user":
                continue
            tool_call_id = data.get("toolCallId")
            if tool_call_id and str(tool_call_id) in completed_calls:
                continue
            turn_id = cls._event_turn_index(data.get("turnId"))
            owner = data.get("interactionId")
            if not isinstance(owner, str) or not owner:
                owner = turn_owner.get(turn_id) if turn_id is not None else None
            if active_interaction and owner and owner != active_interaction:
                continue
            pending_asks.append(
                (index, event, owner if isinstance(owner, str) else active_interaction)
            )
        if pending_asks:
            _, event, owner = pending_asks[-1]
            data = event.get("data") or {}
            return UserInputRequest(
                source="ask_user",
                reason=cls._ask_user_reason(data.get("arguments")),
                requested_at=str(event.get("timestamp") or "") or None,
                interaction_id=owner,
                turn_id=cls._event_turn_index(data.get("turnId")),
                tool_call_id=(str(data.get("toolCallId")) if data.get("toolCallId") else None),
            )

        latest_message: tuple[int, dict[str, Any], int, str] | None = None
        for index, event in enumerate(events):
            if event.get("agentId") is not None or event.get("type") != "assistant.message":
                continue
            data = event.get("data") or {}
            content = data.get("content")
            turn_id = cls._event_turn_index(data.get("turnId"))
            owner = data.get("interactionId")
            if not isinstance(owner, str) or not owner:
                owner = turn_owner.get(turn_id) if turn_id is not None else None
            if (
                turn_id is None
                or not isinstance(content, str)
                or not content
                or (active_interaction and owner != active_interaction)
            ):
                continue
            latest_message = (index, event, turn_id, content)
        if latest_message is None:
            return None
        message_index, event, message_turn, content = latest_message
        turn_end_index = next(
            (
                index
                for index, candidate in enumerate(
                    events[message_index + 1 :],
                    message_index + 1,
                )
                if candidate.get("agentId") is None
                and candidate.get("type") == "assistant.turn_end"
                and cls._event_turn_index((candidate.get("data") or {}).get("turnId"))
                == message_turn
            ),
            None,
        )
        if turn_end_index is None:
            return None
        if any(
            candidate.get("agentId") is None
            and candidate.get("type") == "assistant.turn_start"
            and (
                not active_interaction
                or (candidate.get("data") or {}).get("interactionId") == active_interaction
            )
            for candidate in events[turn_end_index + 1 :]
        ):
            return None
        reason = cls._assistant_input_reason(content)
        if not reason:
            return None
        data = event.get("data") or {}
        owner = data.get("interactionId")
        return UserInputRequest(
            source="assistant_question",
            reason=reason,
            requested_at=str(event.get("timestamp") or "") or None,
            interaction_id=(owner if isinstance(owner, str) and owner else active_interaction),
            turn_id=message_turn,
            tool_call_id=None,
        )

    async def attach(
        self,
        websocket: WebSocket,
        worker: dict[str, Any],
        client_id: str,
        requested_stream_id: str | None = None,
        requested_offset: int | None = None,
    ) -> None:
        await websocket.accept()
        self.touch_worker(worker["id"], force=True)
        session = self.ensure_terminal(worker, wait_for_ready=False)
        attached_session_id = session.copilot_session_id or str(worker["copilot_session_id"])
        attached_terminal_generation = (
            int(session.terminal_generation)
            if session.copilot_session_id
            else int(worker.get("terminal_generation") or 0)
        )
        stream_message, replay_data = session.replay_snapshot(
            requested_stream_id,
            requested_offset,
        )
        offset = int(stream_message["replay_end"])
        send_lock = asyncio.Lock()

        async def output_loop() -> None:
            nonlocal offset
            while True:
                with self._lock:
                    current_session = self._sessions.get(worker["id"])
                if current_session is not None and current_session is not session:
                    return
                terminated = session.terminated.is_set()
                data, offset = session.read_from(offset)
                if data:
                    async with send_lock:
                        await websocket.send_bytes(data)
                if terminated:
                    return
                await asyncio.sleep(0.03)

        async def input_loop() -> None:
            while True:
                message = await websocket.receive_json()
                if message.get("type") == "input":
                    try:
                        sequence = int(message.get("sequence") or 0)
                    except (TypeError, ValueError):
                        await websocket.close(code=4400)
                        return
                    if sequence <= 0:
                        await websocket.close(code=4400)
                        return
                    data = str(message.get("data") or "")
                    transport_input = (
                        self._is_terminal_transport_input(data)
                        and message.get("prompt_submit") is not True
                        and message.get("prompt_cancel") is not True
                    )
                    released_pending = False
                    released_draft = False
                    direct_activity = None
                    direct_interrupt_generation = None
                    message_worker_id = message.get("worker_id")
                    if message_worker_id is not None and str(message_worker_id) != worker["id"]:
                        await websocket.close(code=4400)
                        return
                    # xterm emits mouse and terminal-report bursts that have no prompt semantics.
                    if transport_input:
                        mouse_input = self._SGR_MOUSE_INPUT.fullmatch(data)
                        input_lock = (
                            session.prompt_dispatch_lock
                            if mouse_input
                            else nullcontext()
                        )
                        with input_lock, self._lock:
                            current_session = self._sessions.get(worker["id"])
                            current_client = self._clients.get(worker["id"])
                            if (
                                current_session is not session
                                or current_client is None
                                or current_client[0] != client_id
                                or current_client[1] is not websocket
                            ):
                                return

                            self._apply_browser_input_once(
                                session,
                                worker_id=worker["id"],
                                client_id=client_id,
                                sequence=sequence,
                                data=data,
                                before_write=(
                                    partial(self._mark_prompt_input_unknown, session)
                                    if mouse_input
                                    else None
                                ),
                            )
                        if sequence:
                            async with send_lock:
                                await websocket.send_json(
                                    {
                                        "type": "input_ack",
                                        "worker_id": worker["id"],
                                        "sequence": sequence,
                                    }
                                )
                        continue
                    with session.prompt_dispatch_lock, self._lock:
                            current_session = self._sessions.get(worker["id"])
                            current_client = self._clients.get(worker["id"])
                            current_worker = self.repository.get_agent_worker(worker["id"])
                            if current_session is not None and current_session is not session:
                                return
                            if (
                                current_client is None
                                or current_client[0] != client_id
                                or current_client[1] is not websocket
                                or current_worker is None
                                or current_worker["copilot_session_id"] != attached_session_id
                                or int(current_worker.get("terminal_generation") or 0)
                                != attached_terminal_generation
                            ):
                                return

                            input_outcome: dict[str, Any] = {}
                            self._apply_browser_input_once(
                                session,
                                worker_id=worker["id"],
                                client_id=client_id,
                                sequence=sequence,
                                data=data,
                                before_write=partial(
                                    self._apply_prompt_input_side_effects,
                                    session=session,
                                    worker=current_worker,
                                    data=data,
                                    message=message,
                                    attached_session_id=attached_session_id,
                                    attached_terminal_generation=(
                                        attached_terminal_generation
                                    ),
                                    outcome=input_outcome,
                                ),
                            )
                            released_pending = bool(
                                input_outcome.get("released_pending")
                            )
                            released_draft = bool(
                                input_outcome.get("released_draft")
                            )
                            direct_activity = input_outcome.get("direct_activity")
                            direct_interrupt_generation = input_outcome.get(
                                "direct_interrupt_generation"
                            )
                    if released_pending:
                        current = self.repository.get_agent_worker(worker["id"])
                        if current and current["state"] == "recovering":
                            self._notify_worker_recovery(worker["id"])
                        elif current and current["state"] == "idle":
                            self._notify_worker_idle()
                    elif released_draft:
                        current = self.repository.get_agent_worker(worker["id"])
                        if current and current["state"] == "idle":
                            self._notify_worker_idle()
                    if sequence:
                        acknowledgement = {
                            "type": "input_ack",
                            "worker_id": worker["id"],
                            "sequence": sequence,
                        }
                        if direct_activity:
                            acknowledgement["direct_activity"] = direct_activity
                        if released_pending:
                            acknowledgement["direct_cancelled"] = True
                        if direct_interrupt_generation is not None:
                            acknowledgement["direct_interrupt_generation"] = (
                                direct_interrupt_generation
                            )
                        async with send_lock:
                            await websocket.send_json(acknowledgement)
                elif message.get("type") == "resize":
                    self.resize_terminal(
                        session,
                        int(message.get("rows") or 36),
                        int(message.get("cols") or 120),
                    )

        output_task: asyncio.Task[None] | None = None
        input_task: asyncio.Task[None] | None = None
        with self._lock:
            previous = self._clients.get(worker["id"])
            self._clients[worker["id"]] = (client_id, websocket)
        try:
            if previous and previous[1] is not websocket:
                try:
                    await previous[1].close(code=4409)
                except (RuntimeError, WebSocketDisconnect):
                    pass
            async with send_lock:
                await websocket.send_json(stream_message)
                if replay_data:
                    await websocket.send_bytes(replay_data)
                await websocket.send_json(
                    {
                        "type": "terminal_replay_end",
                        "stream_id": session.stream_id,
                        "offset": offset,
                    }
                )
            output_task = asyncio.create_task(output_loop())
            input_task = asyncio.create_task(input_loop())
            done, pending = await asyncio.wait(
                {output_task, input_task},
                return_when=asyncio.FIRST_COMPLETED,
            )
            for task in pending:
                task.cancel()
            for task in done:
                await task
        except (RuntimeError, WebSocketDisconnect):
            pass
        finally:
            if output_task is not None:
                output_task.cancel()
            if input_task is not None:
                input_task.cancel()
            with self._lock:
                current = self._clients.get(worker["id"])
                if current and current[1] is websocket:
                    self._clients.pop(worker["id"], None)

    @staticmethod
    def _event_turn_index(value: Any) -> int | None:
        if isinstance(value, int):
            return value
        if isinstance(value, str) and value.isdigit():
            return int(value)
        return None

    @staticmethod
    def latest_turn_index(session_id: str) -> int:
        events_path = Path("~/.copilot/session-state").expanduser() / session_id / "events.jsonl"
        latest = -1
        if events_path.exists():
            for line in events_path.read_text(errors="ignore").splitlines():
                try:
                    event = json.loads(line)
                except json.JSONDecodeError:
                    continue
                if event.get("type") not in {"user.message", "assistant.message"}:
                    continue
                turn_id = TerminalRuntime._event_turn_index((event.get("data") or {}).get("turnId"))
                if turn_id is not None:
                    latest = max(latest, turn_id)
            if latest >= 0:
                return latest
        path = Path("~/.copilot/session-store.db").expanduser()
        if not path.exists():
            return -1
        connection = sqlite3.connect(f"file:{path}?mode=ro", uri=True)
        try:
            row = connection.execute(
                "SELECT MAX(turn_index) FROM turns WHERE session_id=?",
                (session_id,),
            ).fetchone()
            return int(row[0]) if row and row[0] is not None else -1
        finally:
            connection.close()

    @staticmethod
    def response_after(session_id: str, turn_index: int) -> str | None:
        events_path = Path("~/.copilot/session-state").expanduser() / session_id / "events.jsonl"
        if events_path.exists():
            for line in events_path.read_text(errors="ignore").splitlines():
                try:
                    event = json.loads(line)
                except json.JSONDecodeError:
                    continue
                if event.get("type") != "assistant.message":
                    continue
                data = event.get("data") or {}
                event_turn = TerminalRuntime._event_turn_index(data.get("turnId"))
                content = data.get("content")
                if (
                    event_turn is not None
                    and event_turn > turn_index
                    and isinstance(content, str)
                    and content
                ):
                    return content
        path = Path("~/.copilot/session-store.db").expanduser()
        if not path.exists():
            return None
        connection = sqlite3.connect(f"file:{path}?mode=ro", uri=True)
        try:
            row = connection.execute(
                """
                SELECT assistant_response
                FROM turns
                WHERE session_id=? AND turn_index>?
                  AND assistant_response IS NOT NULL
                  AND length(assistant_response)>0
                ORDER BY turn_index
                LIMIT 1
                """,
                (session_id, turn_index),
            ).fetchone()
            return str(row[0]) if row else None
        finally:
            connection.close()

    @staticmethod
    def history(session_id: str) -> list[dict[str, str]]:
        events_path = Path("~/.copilot/session-state").expanduser() / session_id / "events.jsonl"
        if not events_path.exists():
            return []
        entries: list[dict[str, str]] = []
        for line in events_path.read_text(errors="ignore").splitlines():
            try:
                event = json.loads(line)
            except json.JSONDecodeError:
                continue
            event_type = event.get("type")
            data = event.get("data") or {}
            timestamp = str(event.get("timestamp") or "")
            if event_type == "user.message":
                content = data.get("content")
                if isinstance(content, str) and content:
                    entries.append({"role": "user", "content": content, "timestamp": timestamp})
            elif event_type == "assistant.message":
                content = data.get("content")
                if isinstance(content, str) and content:
                    entries.append(
                        {
                            "role": "assistant",
                            "content": content,
                            "timestamp": timestamp,
                        }
                    )
            elif event_type == "tool.execution_start":
                tool_name = data.get("toolName")
                if tool_name:
                    entries.append(
                        {
                            "role": "tool",
                            "content": f"Started {tool_name}",
                            "timestamp": timestamp,
                        }
                    )
            elif event_type == "tool.execution_complete":
                tool_name = data.get("toolName") or "tool"
                success = data.get("success")
                suffix = "completed" if success is not False else "failed"
                entries.append(
                    {
                        "role": "tool",
                        "content": f"{tool_name} {suffix}",
                        "timestamp": timestamp,
                    }
                )
        return entries
