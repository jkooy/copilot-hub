"""Daily Claude spend against a budget, overall and per Hub session.

Spend comes from a *ledger*. The default sees only the Hub's own sessions and
prices their transcripts' token counts at list prices. A machine-local ledger
plugin can report billed costs for every Claude Code request on the machine,
for example from a gateway's request logs; set ``COPILOT_HUB_SPEND_LEDGER`` to
``path/to/file.py:ClassName`` or ``package.module:ClassName``. A ledger has a
``label``, ``available() -> bool``, and ``requests_between(start, end)``
returning :class:`Request` records.
"""

from __future__ import annotations

import importlib
import importlib.util
import json
import logging
import sys
import threading
import time
from collections.abc import Iterable
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any, Protocol
from zoneinfo import ZoneInfo

logger = logging.getLogger(__name__)

# USD per million tokens: input, output, cache read, cache write (5-minute TTL).
PRICING_PER_MTOK: dict[str, tuple[float, float, float, float]] = {
    "opus-5-5": (4.00, 20.00, 0.20, 5.00),
    "sonnet-5-5": (2.00, 10.00, 0.20, 2.50),
    "haiku-4-5": (1.00, 5.00, 0.10, 1.25),
}
# Unknown models are priced as the most expensive known model so the budget errs high.
FALLBACK_PRICING = PRICING_PER_MTOK["opus-5-5"]


def pricing_for(model: str | None) -> tuple[float, float, float, float]:
    value = str(model or "").lower()
    for family, prices in PRICING_PER_MTOK.items():
        if family in value:
            return prices
    for family, prices in (("opus", PRICING_PER_MTOK["opus-5-5"]),
                           ("sonnet", PRICING_PER_MTOK["sonnet-5-5"]),
                           ("haiku", PRICING_PER_MTOK["haiku-4-5"])):
        if family in value:
            return prices
    return FALLBACK_PRICING


def estimate_cost(
    model: str | None,
    *,
    input_tokens: int,
    output_tokens: int,
    cache_read_tokens: int,
    cache_write_tokens: int,
) -> float:
    input_price, output_price, read_price, write_price = pricing_for(model)
    return (
        input_tokens * input_price
        + output_tokens * output_price
        + cache_read_tokens * read_price
        + cache_write_tokens * write_price
    ) / 1_000_000


@dataclass(frozen=True)
class Request:
    key: str
    timestamp: datetime
    session_id: str | None
    model: str
    input_tokens: int
    output_tokens: int
    cache_read_tokens: int
    cache_write_tokens: int
    cost_usd: float
    billed: bool

    @property
    def tokens(self) -> int:
        return (
            self.input_tokens
            + self.output_tokens
            + self.cache_read_tokens
            + self.cache_write_tokens
        )


@dataclass
class SpendTotals:
    cost_usd: float = 0.0
    tokens: int = 0
    input_tokens: int = 0
    output_tokens: int = 0
    cache_read_tokens: int = 0
    cache_write_tokens: int = 0
    requests: int = 0
    estimated_requests: int = 0

    def add(self, request: Request) -> None:
        self.cost_usd += request.cost_usd
        self.tokens += request.tokens
        self.input_tokens += request.input_tokens
        self.output_tokens += request.output_tokens
        self.cache_read_tokens += request.cache_read_tokens
        self.cache_write_tokens += request.cache_write_tokens
        self.requests += 1
        if not request.billed:
            self.estimated_requests += 1

    def as_dict(self) -> dict[str, Any]:
        return {
            "cost_usd": round(self.cost_usd, 4),
            "tokens": self.tokens,
            "input_tokens": self.input_tokens,
            "output_tokens": self.output_tokens,
            "cache_read_tokens": self.cache_read_tokens,
            "cache_write_tokens": self.cache_write_tokens,
            "requests": self.requests,
            "estimated_requests": self.estimated_requests,
        }


def parse_timestamp(value: Any) -> datetime | None:
    try:
        parsed = datetime.fromisoformat(str(value))
    except ValueError:
        return None
    return parsed if parsed.tzinfo else parsed.replace(tzinfo=UTC)


class Ledger(Protocol):
    label: str

    def available(self) -> bool: ...

    def requests_between(self, start: datetime, end: datetime) -> list[Request]: ...


class NoLedger:
    """No machine-wide ledger; spend is estimated from the Hub's own transcripts."""

    label = ""

    def available(self) -> bool:
        return False

    def requests_between(self, start: datetime, end: datetime) -> list[Request]:
        return []


def load_ledger(spec: str | None) -> Ledger:
    """Instantiate ``path/to/file.py:ClassName`` or ``package.module:ClassName``."""
    if not spec:
        return NoLedger()
    target, _, name = spec.rpartition(":")
    try:
        if target.endswith(".py"):
            path = Path(target).expanduser()
            module_name = f"copilot_hub_ledger_{path.stem}"
            module_spec = importlib.util.spec_from_file_location(module_name, path)
            if module_spec is None or module_spec.loader is None:
                raise ImportError(f"cannot load {path}")
            module = importlib.util.module_from_spec(module_spec)
            # Register first: dataclasses and pickling resolve classes through sys.modules.
            sys.modules[module_name] = module
            module_spec.loader.exec_module(module)
        else:
            module = importlib.import_module(target)
        return getattr(module, name)()
    except Exception:
        logger.exception("Could not load spend ledger %s; using Hub transcripts", spec)
        return NoLedger()


def transcript_requests(path: Path) -> list[Request]:
    """Model requests recorded in one Claude transcript, priced from token counts."""
    by_message: dict[str, Request] = {}
    try:
        lines = path.read_text(encoding="utf-8", errors="ignore").splitlines()
    except OSError:
        return []
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
        timestamp = parse_timestamp(entry.get("timestamp"))
        if not isinstance(usage, dict) or timestamp is None:
            continue
        model = str(message.get("model") or "")
        counts = {
            "input_tokens": int(usage.get("input_tokens") or 0),
            "output_tokens": int(usage.get("output_tokens") or 0),
            "cache_read_tokens": int(usage.get("cache_read_input_tokens") or 0),
            "cache_write_tokens": int(usage.get("cache_creation_input_tokens") or 0),
        }
        key = str(message.get("id") or entry.get("uuid") or len(by_message))
        by_message[key] = Request(
            key=key,
            timestamp=timestamp,
            session_id=str(entry.get("sessionId") or "") or None,
            model=model,
            cost_usd=estimate_cost(model, **counts),
            billed=False,
            **counts,
        )
    return list(by_message.values())


class SpendMonitor:
    """Today's spend against the daily budget, overall and per Hub session."""

    def __init__(
        self,
        *,
        daily_budget_usd: float,
        timezone: str | None = None,
        ledger: Ledger | None = None,
        ttl_seconds: float = 5,
    ):
        self.daily_budget_usd = daily_budget_usd
        self.timezone = ZoneInfo(timezone) if timezone else None
        self.ledger = ledger or NoLedger()
        self.ttl_seconds = ttl_seconds
        self._lock = threading.Lock()
        self._cached_at = 0.0
        self._cache_key: Any = None
        self._snapshot: dict[str, Any] | None = None

    def day_window(self, now: datetime | None = None) -> tuple[datetime, datetime]:
        local_now = (now or datetime.now(UTC)).astimezone(self.timezone)
        start = local_now.replace(hour=0, minute=0, second=0, microsecond=0)
        return start.astimezone(UTC), (start + timedelta(days=1)).astimezone(UTC)

    def snapshot(
        self,
        sessions: dict[str, Iterable[str]],
        transcripts: dict[str, Iterable[Path]] | None = None,
        *,
        now: datetime | None = None,
    ) -> dict[str, Any]:
        """Spend for today.

        ``sessions`` maps a Hub worker ID to its Claude session IDs;
        ``transcripts`` maps a worker ID to transcript files, used when no
        machine-wide ledger is available.
        """
        sessions = {worker: set(ids) for worker, ids in sessions.items()}
        cache_key = tuple(sorted((worker, tuple(sorted(ids))) for worker, ids in sessions.items()))
        with self._lock:
            if (
                self._snapshot is not None
                and self._cache_key == cache_key
                and now is None
                and time.monotonic() - self._cached_at < self.ttl_seconds
            ):
                return self._snapshot
            snapshot = self._compute(sessions, transcripts or {}, now)
            if now is None:
                self._snapshot = snapshot
                self._cache_key = cache_key
                self._cached_at = time.monotonic()
            return snapshot

    def _compute(
        self,
        sessions: dict[str, set[str]],
        transcripts: dict[str, Iterable[Path]],
        now: datetime | None,
    ) -> dict[str, Any]:
        start, end = self.day_window(now)
        worker_by_session = {
            session_id: worker for worker, ids in sessions.items() for session_id in ids
        }
        overall = SpendTotals()
        hub = SpendTotals()
        per_worker = {worker: SpendTotals() for worker in sessions}
        source = "ledger"
        source_label = self.ledger.label
        if self.ledger.available():
            requests = self.ledger.requests_between(start, end)
            for request in requests:
                overall.add(request)
                worker = worker_by_session.get(request.session_id or "")
                if worker is not None:
                    hub.add(request)
                    per_worker[worker].add(request)
        else:
            # Without a machine-wide ledger only the Hub's own sessions are visible.
            source = "transcripts"
            source_label = "Estimated from Hub transcripts"
            for worker, paths in transcripts.items():
                for path in paths:
                    for request in transcript_requests(path):
                        if start <= request.timestamp < end:
                            overall.add(request)
                            hub.add(request)
                            per_worker.setdefault(worker, SpendTotals()).add(request)
        remaining = self.daily_budget_usd - overall.cost_usd
        return {
            "source": source,
            "source_label": source_label,
            "day_start": start.isoformat(),
            "day_end": end.isoformat(),
            "budget_usd": self.daily_budget_usd,
            "spent_usd": round(overall.cost_usd, 2),
            "remaining_usd": round(remaining, 2),
            "used_percent": (
                round(100 * overall.cost_usd / self.daily_budget_usd, 1)
                if self.daily_budget_usd
                else None
            ),
            "status": (
                "exhausted"
                if remaining <= 0
                else "warning"
                if remaining <= 0.2 * self.daily_budget_usd
                else "normal"
            ),
            "total": overall.as_dict(),
            "hub": hub.as_dict(),
            "workers": {worker: totals.as_dict() for worker, totals in per_worker.items()},
        }
