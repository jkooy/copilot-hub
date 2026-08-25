from __future__ import annotations

import threading
import time
from collections import defaultdict, deque
from pathlib import Path
from typing import Any

GIB = 1024**3


def _read_kib_values(path: Path) -> dict[str, int]:
    values: dict[str, int] = {}
    for line in path.read_text(encoding="utf-8").splitlines():
        key, separator, raw_value = line.partition(":")
        if not separator:
            continue
        fields = raw_value.strip().split()
        if not fields:
            continue
        multiplier = 1024 if len(fields) > 1 and fields[1].lower() == "kb" else 1
        try:
            values[key] = int(fields[0]) * multiplier
        except ValueError:
            continue
    return values


def _memory_pressure(
    *,
    total_bytes: int,
    available_bytes: int,
    swap_total_bytes: int,
    swap_free_bytes: int,
) -> str:
    total_capacity = total_bytes + swap_total_bytes
    headroom = available_bytes + swap_free_bytes
    headroom_percent = 100 * headroom / total_capacity if total_capacity else 0
    ram_available_percent = 100 * available_bytes / total_bytes if total_bytes else 0
    if (
        available_bytes <= 2 * GIB
        or ram_available_percent <= 5
        or headroom <= 4 * GIB
        or headroom_percent <= 8
    ):
        return "critical"
    if (
        available_bytes <= 8 * GIB
        or ram_available_percent <= 15
        or headroom <= 10 * GIB
        or headroom_percent <= 15
    ):
        return "warning"
    return "normal"


def _process_memory(proc_root: Path) -> tuple[dict[int, int], dict[int, list[int]]]:
    rss_by_pid: dict[int, int] = {}
    children: dict[int, list[int]] = defaultdict(list)
    for entry in proc_root.iterdir():
        if not entry.name.isdigit():
            continue
        try:
            values = _read_kib_values(entry / "status")
        except (FileNotFoundError, PermissionError, ProcessLookupError):
            continue
        pid = int(entry.name)
        parent_pid = values.get("PPid", 0)
        rss_by_pid[pid] = values.get("VmRSS", 0)
        children[parent_pid].append(pid)
    return rss_by_pid, children


def _descendants(root_pid: int, children: dict[int, list[int]]) -> set[int]:
    found: set[int] = set()
    queue = deque([root_pid])
    while queue:
        pid = queue.popleft()
        if pid in found:
            continue
        found.add(pid)
        queue.extend(children.get(pid, ()))
    return found


def collect_memory_snapshot(
    workers: list[dict[str, Any]],
    *,
    proc_root: Path = Path("/proc"),
) -> dict[str, Any]:
    memory = _read_kib_values(proc_root / "meminfo")
    total_bytes = memory["MemTotal"]
    available_bytes = memory["MemAvailable"]
    swap_total_bytes = memory.get("SwapTotal", 0)
    swap_free_bytes = memory.get("SwapFree", 0)
    rss_by_pid, children = _process_memory(proc_root)

    worker_rss: dict[str, int | None] = {}
    role_pids: dict[str, set[int]] = defaultdict(set)
    hub_pids: set[int] = set()
    for worker in workers:
        pid = worker.get("pid")
        if not isinstance(pid, int) or pid not in rss_by_pid:
            worker_rss[worker["id"]] = None
            continue
        process_ids = _descendants(pid, children)
        worker_rss[worker["id"]] = sum(rss_by_pid.get(item, 0) for item in process_ids)
        role_pids[str(worker["role"])].update(process_ids)
        hub_pids.update(process_ids)

    used_bytes = max(0, total_bytes - available_bytes)
    swap_used_bytes = max(0, swap_total_bytes - swap_free_bytes)
    return {
        "total_bytes": total_bytes,
        "used_bytes": used_bytes,
        "available_bytes": available_bytes,
        "used_percent": round(100 * used_bytes / total_bytes, 1) if total_bytes else 0,
        "swap_total_bytes": swap_total_bytes,
        "swap_used_bytes": swap_used_bytes,
        "swap_free_bytes": swap_free_bytes,
        "pressure": _memory_pressure(
            total_bytes=total_bytes,
            available_bytes=available_bytes,
            swap_total_bytes=swap_total_bytes,
            swap_free_bytes=swap_free_bytes,
        ),
        "hub_rss_bytes": sum(rss_by_pid.get(pid, 0) for pid in hub_pids),
        "role_rss_bytes": {
            role: sum(rss_by_pid.get(pid, 0) for pid in process_ids)
            for role, process_ids in role_pids.items()
        },
        "worker_rss_bytes": worker_rss,
    }


class MemoryMonitor:
    def __init__(
        self,
        *,
        proc_root: Path = Path("/proc"),
        ttl_seconds: float = 2,
    ):
        self.proc_root = proc_root
        self.ttl_seconds = ttl_seconds
        self._lock = threading.Lock()
        self._sampled_at = 0.0
        self._worker_key: tuple[tuple[str, str, int | None], ...] = ()
        self._snapshot: dict[str, Any] | None = None

    def snapshot(self, workers: list[dict[str, Any]]) -> dict[str, Any]:
        worker_key = tuple(
            sorted(
                (
                    str(worker["id"]),
                    str(worker["role"]),
                    worker.get("pid") if isinstance(worker.get("pid"), int) else None,
                )
                for worker in workers
            )
        )
        with self._lock:
            now = time.monotonic()
            if (
                self._snapshot is not None
                and self._worker_key == worker_key
                and now - self._sampled_at < self.ttl_seconds
            ):
                return self._snapshot
            snapshot = collect_memory_snapshot(workers, proc_root=self.proc_root)
            self._snapshot = snapshot
            self._worker_key = worker_key
            self._sampled_at = now
            return snapshot
