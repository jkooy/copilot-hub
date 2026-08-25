from __future__ import annotations

import json
import threading
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any


@dataclass
class SessionUsage:
    inode: int | None = None
    cursor: int = 0
    input_tokens: int = 0
    output_tokens: int = 0
    reasoning_tokens: int = 0
    cache_read_tokens: int = 0
    cache_write_tokens: int = 0
    completed_nano_aiu: int = 0
    completed_premium_requests: float = 0
    current_nano_aiu: int = 0
    current_premium_requests: float = 0
    models: set[str] = field(default_factory=set)
    recorded_tokens_available: bool = False
    skipping_oversized_line: bool = False
    backfill_cursor: int = 0

    def as_dict(self) -> dict[str, Any]:
        return {
            "input_tokens": self.input_tokens,
            "output_tokens": self.output_tokens,
            "reasoning_tokens": self.reasoning_tokens,
            "cache_read_tokens": self.cache_read_tokens,
            "cache_write_tokens": self.cache_write_tokens,
            "recorded_tokens": self.input_tokens + self.output_tokens,
            "recorded_tokens_available": self.recorded_tokens_available,
            "nano_aiu": max(self.completed_nano_aiu, self.current_nano_aiu),
            "premium_requests": max(
                self.completed_premium_requests,
                self.current_premium_requests,
            ),
            "models": sorted(self.models),
        }


class CopilotUsageMonitor:
    BOOTSTRAP_BYTES = 16 * 1024 * 1024
    MAX_EVENT_BYTES = 1024 * 1024
    INCREMENTAL_BYTES = 8 * 1024 * 1024

    def __init__(self, sessions_root: Path | None = None):
        self.sessions_root = sessions_root or Path("~/.copilot/session-state").expanduser()
        self._lock = threading.Lock()
        self._sessions: dict[str, SessionUsage] = {}

    @staticmethod
    def _model_usage(data: dict[str, Any]) -> dict[str, int]:
        totals = {
            "input_tokens": 0,
            "output_tokens": 0,
            "reasoning_tokens": 0,
            "cache_read_tokens": 0,
            "cache_write_tokens": 0,
        }
        for metrics in (data.get("modelMetrics") or {}).values():
            usage = metrics.get("usage") or {}
            for key in totals:
                source = {
                    "input_tokens": "inputTokens",
                    "output_tokens": "outputTokens",
                    "reasoning_tokens": "reasoningTokens",
                    "cache_read_tokens": "cacheReadTokens",
                    "cache_write_tokens": "cacheWriteTokens",
                }[key]
                totals[key] += int(usage.get(source) or 0)
        return totals

    def _apply_event(self, usage: SessionUsage, event: dict[str, Any]) -> None:
        data = event.get("data") or {}
        event_type = event.get("type")
        if event_type == "session.usage_checkpoint":
            usage.current_nano_aiu = max(
                usage.current_nano_aiu,
                int(data.get("totalNanoAiu") or 0),
            )
            usage.current_premium_requests = max(
                usage.current_premium_requests,
                float(data.get("totalPremiumRequests") or 0),
            )
            for model in data.get("modelCacheState") or []:
                if model.get("modelId"):
                    usage.models.add(str(model["modelId"]))
        elif event_type == "session.shutdown":
            for key, value in self._model_usage(data).items():
                setattr(usage, key, max(getattr(usage, key), value))
            usage.completed_nano_aiu = max(
                usage.completed_nano_aiu,
                int(data.get("totalNanoAiu") or 0),
            )
            usage.completed_premium_requests = max(
                usage.completed_premium_requests,
                float(data.get("totalPremiumRequests") or 0),
            )
            if data.get("currentModel"):
                usage.models.add(str(data["currentModel"]))
            usage.recorded_tokens_available = True

    def _apply_event_lines(self, usage: SessionUsage, data: bytes) -> None:
        for line in data.splitlines():
            if b"session.shutdown" not in line and b"session.usage_checkpoint" not in line:
                continue
            try:
                event = json.loads(line)
            except (json.JSONDecodeError, UnicodeDecodeError):
                continue
            if isinstance(event, dict):
                self._apply_event(usage, event)

    def _bootstrap(self, path: Path, usage: SessionUsage, size: int) -> int:
        start = max(0, size - self.BOOTSTRAP_BYTES)
        with path.open("rb") as stream:
            stream.seek(start)
            raw_data = stream.read(self.BOOTSTRAP_BYTES)
        complete_size = (
            len(raw_data)
            if raw_data.endswith(b"\n")
            else raw_data.rfind(b"\n") + 1
        )
        data = raw_data[:complete_size]
        backfill_cursor = start
        if start and (newline := data.find(b"\n")) >= 0:
            first_complete_line = newline + 1
            data = data[first_complete_line:]
            backfill_cursor = start + first_complete_line
        elif start:
            data = b""
        self._apply_event_lines(usage, data)
        usage.cursor = start + complete_size
        usage.backfill_cursor = backfill_cursor
        return len(raw_data)

    def _process_backfill(
        self,
        path: Path,
        usage: SessionUsage,
        max_bytes: int,
    ) -> int:
        boundary = usage.backfill_cursor
        start = max(0, boundary - max_bytes)
        with path.open("rb") as stream:
            stream.seek(start)
            data = stream.read(boundary - start)
        next_boundary = 0
        if start:
            newline = data.find(b"\n")
            if newline >= 0 and newline + 1 < len(data):
                next_boundary = start + newline + 1
                data = data[newline + 1 :]
            else:
                next_boundary = start
                data = b""
        self._apply_event_lines(usage, data)
        usage.backfill_cursor = next_boundary
        return len(data) if start == 0 else boundary - start

    def _process_incremental(
        self,
        path: Path,
        usage: SessionUsage,
        size: int,
        max_bytes: int,
    ) -> int:
        consumed = 0
        with path.open("rb") as stream:
            stream.seek(usage.cursor)
            while stream.tell() < size and consumed < max_bytes:
                read_limit = min(self.MAX_EVENT_BYTES + 1, max_bytes - consumed)
                if usage.skipping_oversized_line:
                    chunk = stream.readline(read_limit)
                    consumed += len(chunk)
                    usage.cursor = stream.tell()
                    if chunk.endswith(b"\n"):
                        usage.skipping_oversized_line = False
                    continue
                line_start = stream.tell()
                line = stream.readline(read_limit)
                consumed += len(line)
                if not line.endswith(b"\n"):
                    if len(line) <= self.MAX_EVENT_BYTES:
                        break
                    usage.cursor = stream.tell()
                    usage.skipping_oversized_line = True
                    continue
                usage.cursor = stream.tell()
                if b"session.shutdown" not in line and b"session.usage_checkpoint" not in line:
                    continue
                try:
                    event = json.loads(line)
                except (json.JSONDecodeError, UnicodeDecodeError):
                    usage.cursor = max(usage.cursor, line_start + len(line))
                    continue
                self._apply_event(usage, event)
        return consumed

    def session_usage_with_budget(
        self,
        session_id: str,
        byte_budget: int,
    ) -> tuple[dict[str, Any], int]:
        path = self.sessions_root / session_id / "events.jsonl"
        with self._lock:
            usage = self._sessions.setdefault(session_id, SessionUsage())
            try:
                stat = path.stat()
            except FileNotFoundError:
                return usage.as_dict(), 0
            if usage.inode != stat.st_ino or stat.st_size < usage.cursor:
                fresh_usage = SessionUsage(inode=stat.st_ino)
                required_bytes = min(stat.st_size, self.BOOTSTRAP_BYTES)
                if byte_budget < required_bytes:
                    return fresh_usage.as_dict(), 0
                usage = fresh_usage
                self._sessions[session_id] = usage
                consumed = self._bootstrap(path, usage, stat.st_size)
                return usage.as_dict(), consumed
            remaining = min(self.INCREMENTAL_BYTES, max(0, byte_budget))
            consumed = 0
            if (
                stat.st_size > usage.cursor
                and remaining >= self.MAX_EVENT_BYTES + 1
            ):
                incremental = self._process_incremental(
                    path,
                    usage,
                    stat.st_size,
                    remaining,
                )
                consumed += incremental
                remaining -= incremental
            if usage.backfill_cursor > 0 and remaining >= self.MAX_EVENT_BYTES + 1:
                consumed += self._process_backfill(
                    path,
                    usage,
                    min(remaining, usage.backfill_cursor),
                )
            return usage.as_dict(), consumed

    def session_usage(self, session_id: str) -> dict[str, Any]:
        usage, _ = self.session_usage_with_budget(
            session_id,
            max(self.BOOTSTRAP_BYTES, self.INCREMENTAL_BYTES),
        )
        return usage
