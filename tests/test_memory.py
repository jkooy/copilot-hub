from __future__ import annotations

from copilot_hub.memory import MemoryMonitor, collect_memory_snapshot


def write_status(root, pid, *, parent, rss_kib):
    directory = root / str(pid)
    directory.mkdir()
    (directory / "status").write_text(
        f"Name:\tproc\nPid:\t{pid}\nPPid:\t{parent}\nVmRSS:\t{rss_kib} kB\n",
        encoding="utf-8",
    )


def test_memory_snapshot_counts_session_process_trees(tmp_path):
    (tmp_path / "meminfo").write_text(
        "MemTotal:       16777216 kB\n"
        "MemAvailable:   10485760 kB\n"
        "SwapTotal:       4194304 kB\n"
        "SwapFree:        3145728 kB",
        encoding="utf-8",
    )
    write_status(tmp_path, 100, parent=1, rss_kib=1000)
    write_status(tmp_path, 101, parent=100, rss_kib=500)
    write_status(tmp_path, 200, parent=1, rss_kib=700)

    snapshot = collect_memory_snapshot(
        [
            {"id": "manager", "role": "manager", "pid": 100},
            {"id": "worker", "role": "worker", "pid": 200},
        ],
        proc_root=tmp_path,
    )

    assert snapshot["worker_rss_bytes"]["manager"] == 1500 * 1024
    assert snapshot["worker_rss_bytes"]["worker"] == 700 * 1024
    assert snapshot["hub_rss_bytes"] == 2200 * 1024
    assert snapshot["pressure"] == "normal"


def test_memory_monitor_caches_until_worker_identity_changes(tmp_path, monkeypatch):
    calls = []

    def sample(workers, *, proc_root):
        calls.append((workers, proc_root))
        return {"sample": len(calls)}

    monkeypatch.setattr("copilot_hub.memory.collect_memory_snapshot", sample)
    monitor = MemoryMonitor(proc_root=tmp_path, ttl_seconds=60)
    workers = [{"id": "w", "role": "worker", "pid": 10}]

    assert monitor.snapshot(workers) == {"sample": 1}
    assert monitor.snapshot(workers) == {"sample": 1}
    assert monitor.snapshot([{"id": "w", "role": "worker", "pid": 11}]) == {
        "sample": 2
    }
