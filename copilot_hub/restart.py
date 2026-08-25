from __future__ import annotations

import fcntl
import hashlib
import json
import os
import signal
import sqlite3
import subprocess
import sys
import time
import urllib.error
import urllib.request
from pathlib import Path
from typing import Any

from .config import Settings
from .db import Repository

RESTART_HANDOFF_TIMEOUT_SECONDS = 300
SERVER_STOP_TIMEOUT_SECONDS = 30
SERVER_HEALTH_TIMEOUT_SECONDS = 45


def normalized_tool_path() -> str:
    root = Path(__file__).parent.parent
    candidates = [
        root / ".venv" / "bin",
        Path.home() / ".local" / "bin",
        Path("/opt/homebrew/bin"),
        Path("/opt/homebrew/sbin"),
        Path.home() / ".pyenv" / "shims",
        Path.home() / ".pyenv" / "bin",
        Path.home() / ".cargo" / "bin",
    ]
    entries = [str(path) for path in candidates if path.is_dir()]
    entries.extend(
        str(Path(value).expanduser())
        for value in os.environ.get("PATH", "").split(os.pathsep)
        if value
    )
    return os.pathsep.join(dict.fromkeys(entries))


def server_pid_path(settings: Settings) -> Path:
    return settings.state_dir / "server.pid"


def server_lock_path(settings: Settings) -> Path:
    return settings.state_dir / "server.lock"


def server_log_path(settings: Settings) -> Path:
    return settings.state_dir / "server.log"


def restart_log_path(settings: Settings, restart_id: str) -> Path:
    return settings.state_dir / f"restart-{restart_id}.log"


def _pid_is_alive(pid: int) -> bool:
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    return True


def _process_identity(pid: int) -> str | None:
    try:
        result = subprocess.run(
            [
                "ps",
                "-p",
                str(pid),
                "-o",
                "lstart=",
                "-o",
                "command=",
            ],
            capture_output=True,
            text=True,
            timeout=2,
            check=False,
        )
    except (OSError, subprocess.TimeoutExpired):
        return None
    value = " ".join(result.stdout.split())
    if result.returncode != 0 or not value:
        return None
    return hashlib.sha256(value.encode()).hexdigest()


def _pid_matches_identity(pid: int, expected_identity: str) -> bool:
    return _process_identity(pid) == expected_identity


def _write_pid_file(path: Path, pid: int) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(".tmp")
    temporary.write_text(f"{pid}\n", encoding="utf-8")
    temporary.replace(path)


def activate_server_runtime(
    repository: Repository,
    settings: Settings,
) -> dict[str, Any]:
    lock_path = server_lock_path(settings)
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    lock_handle = lock_path.open("a+")
    try:
        fcntl.flock(lock_handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
    except BlockingIOError as exc:
        lock_handle.close()
        raise RuntimeError("Another Copilot Hub server owns the runtime lock") from exc
    restart_token = os.environ.get("COPILOT_HUB_RESTART_TOKEN") or None
    try:
        runtime = repository.register_hub_server(
            pid=os.getpid(),
            restart_token=restart_token,
            process_identity=_process_identity(os.getpid()),
        )
    except Exception:
        fcntl.flock(lock_handle.fileno(), fcntl.LOCK_UN)
        lock_handle.close()
        raise
    os.environ.pop("COPILOT_HUB_RESTART_TOKEN", None)
    _write_pid_file(server_pid_path(settings), os.getpid())
    runtime["_lock_handle"] = lock_handle
    return runtime


def deactivate_server_runtime(
    repository: Repository,
    settings: Settings,
    runtime: dict[str, Any],
) -> None:
    try:
        if repository.unregister_hub_server(
            generation=str(runtime["generation"]),
            pid=int(runtime["pid"]),
        ):
            path = server_pid_path(settings)
            try:
                recorded_pid = int(path.read_text(encoding="utf-8").strip())
            except (FileNotFoundError, ValueError):
                recorded_pid = None
            if recorded_pid == int(runtime["pid"]):
                path.unlink(missing_ok=True)
    finally:
        lock_handle = runtime.get("_lock_handle")
        if lock_handle is not None:
            fcntl.flock(lock_handle.fileno(), fcntl.LOCK_UN)
            lock_handle.close()


def spawn_restart_helper(settings: Settings, restart_id: str) -> int:
    log_path = restart_log_path(settings, restart_id)
    log_path.parent.mkdir(parents=True, exist_ok=True)
    with log_path.open("ab") as log:
        process = subprocess.Popen(
            [
                sys.executable,
                "-m",
                "copilot_hub.restart",
                restart_id,
            ],
            cwd=Path(__file__).parent.parent,
            stdin=subprocess.DEVNULL,
            stdout=log,
            stderr=subprocess.STDOUT,
            close_fds=True,
            start_new_session=True,
        )
    return process.pid


def _health(settings: Settings) -> dict[str, Any] | None:
    try:
        with urllib.request.urlopen(
            f"http://{settings.host}:{settings.port}/api/health",
            timeout=1,
        ) as response:
            return json.load(response)
    except (OSError, urllib.error.URLError, json.JSONDecodeError):
        return None


def _source_process_identity(
    settings: Settings,
    restart: dict[str, Any],
) -> str:
    identity = str(restart.get("source_process_identity") or "")
    if identity:
        return identity
    health = _health(settings)
    source_generation = str(restart["source_server_generation"])
    source_pid = int(restart["source_server_pid"])
    if (
        not health
        or health.get("generation") != source_generation
        or int(health.get("pid") or 0) != source_pid
    ):
        raise RuntimeError(
            "Restart source could not be verified through the live health endpoint"
        )
    identity = _process_identity(source_pid) or ""
    if not identity:
        raise RuntimeError("Restart source process identity is unavailable")
    return identity


def _claim_helper(
    repository: Repository,
    restart_id: str,
    helper_pid: int,
) -> bool:
    restart = repository.get_hub_restart(restart_id)
    if restart is None:
        return False
    existing = restart.get("helper_pid")
    replace_pid = None
    if isinstance(existing, int) and existing != helper_pid:
        if _pid_is_alive(existing):
            return False
        replace_pid = existing
    return repository.claim_hub_restart_helper(
        restart_id,
        helper_pid=helper_pid,
        replace_helper_pid=replace_pid,
    )


def _wait_for_handoff(
    repository: Repository,
    restart_id: str,
    timeout_seconds: float,
) -> dict[str, Any]:
    deadline = time.monotonic() + timeout_seconds
    while time.monotonic() < deadline:
        restart = repository.reconcile_hub_restart_handoff(restart_id)
        if restart is None:
            raise RuntimeError("Restart handoff disappeared")
        if restart["status"] == "handed_off":
            return restart
        if restart["status"] in {"failed", "healthy", "superseded"}:
            raise RuntimeError(f"Restart handoff ended as {restart['status']}")
        time.sleep(0.1)
    raise RuntimeError("Timed out waiting for restart interaction handoff")


def _wait_for_process_exit(
    pid: int,
    timeout_seconds: float,
    *,
    expected_identity: str | None = None,
) -> bool:
    deadline = time.monotonic() + timeout_seconds
    while time.monotonic() < deadline:
        if not _pid_is_alive(pid):
            return True
        if expected_identity and not _pid_matches_identity(pid, expected_identity):
            return True
        time.sleep(0.1)
    return not _pid_is_alive(pid) or bool(
        expected_identity and not _pid_matches_identity(pid, expected_identity)
    )


def _launch_server(settings: Settings, restart_id: str) -> subprocess.Popen[bytes]:
    env = {
        **os.environ,
        "PATH": normalized_tool_path(),
        "COPILOT_HUB_RESTART_TOKEN": restart_id,
        "COPILOT_HUB_STATE_DIR": str(settings.state_dir),
        "COPILOT_HUB_DB": str(settings.db_path),
        "COPILOT_HUB_LOG_DIR": str(settings.log_dir),
        "COPILOT_HUB_HOST": settings.host,
        "COPILOT_HUB_PORT": str(settings.port),
    }
    log_path = server_log_path(settings)
    log_path.parent.mkdir(parents=True, exist_ok=True)
    with log_path.open("ab") as log:
        return subprocess.Popen(
            [
                sys.executable,
                "-m",
                "uvicorn",
                "copilot_hub.app:app",
                "--host",
                settings.host,
                "--port",
                str(settings.port),
            ],
            cwd=Path(__file__).parent.parent,
            stdin=subprocess.DEVNULL,
            stdout=log,
            stderr=subprocess.STDOUT,
            env=env,
            close_fds=True,
            start_new_session=True,
        )


def run_restart_helper(
    restart_id: str,
    *,
    handoff_timeout_seconds: float = RESTART_HANDOFF_TIMEOUT_SECONDS,
    stop_timeout_seconds: float = SERVER_STOP_TIMEOUT_SECONDS,
    health_timeout_seconds: float = SERVER_HEALTH_TIMEOUT_SECONDS,
) -> None:
    settings = Settings.from_env()
    settings.ensure_directories()
    repository = Repository(settings.db_path)
    repository.initialize()
    helper_pid = os.getpid()
    if not _claim_helper(repository, restart_id, helper_pid):
        return
    try:
        restart = _wait_for_handoff(
            repository,
            restart_id,
            handoff_timeout_seconds,
        )
        source_generation = str(restart["source_server_generation"])
        source_pid = int(restart["source_server_pid"])
        current = repository.current_hub_server()
        if (
            current is None
            or current["generation"] != source_generation
            or int(current["pid"]) != source_pid
        ):
            repository.finish_hub_restart(
                restart_id,
                status="superseded",
                error="A newer Hub server generation is already active.",
            )
            return
        source_identity = _source_process_identity(settings, restart)
        if not repository.begin_hub_restart_stop(
            restart_id,
            source_server_generation=source_generation,
            source_server_pid=source_pid,
        ):
            raise RuntimeError("Restart handoff was no longer ready to stop the source server")
        if not source_identity or not _pid_matches_identity(source_pid, source_identity):
            raise RuntimeError("Source Hub server process identity changed before stop")
        if _pid_is_alive(source_pid):
            os.kill(source_pid, signal.SIGTERM)
            if not _wait_for_process_exit(
                source_pid,
                stop_timeout_seconds,
                expected_identity=source_identity,
            ):
                if not _pid_matches_identity(source_pid, source_identity):
                    raise RuntimeError("Source Hub server process identity changed before kill")
                os.kill(source_pid, signal.SIGKILL)
                if not _wait_for_process_exit(
                    source_pid,
                    5,
                    expected_identity=source_identity,
                ):
                    raise RuntimeError("Source Hub server did not exit")
        if not repository.mark_hub_restart_starting(restart_id):
            raise RuntimeError("Restart handoff was no longer ready to start a server")

        existing = _health(settings)
        if existing and existing.get("generation") != source_generation:
            repository.finish_hub_restart(
                restart_id,
                status="superseded",
                error="Another Hub server generation started before the helper.",
            )
            return

        process = _launch_server(settings, restart_id)
        deadline = time.monotonic() + health_timeout_seconds
        while time.monotonic() < deadline:
            health = _health(settings)
            if (
                health
                and health.get("restart_token") == restart_id
                and health.get("generation") != source_generation
            ):
                repository.mark_hub_restart_healthy(
                    restart_id,
                    replacement_server_generation=str(health["generation"]),
                    replacement_server_pid=int(health["pid"]),
                )
                return
            if process.poll() is not None:
                raise RuntimeError(
                    f"Replacement Hub server exited with status {process.returncode}"
                )
            time.sleep(0.2)
        raise RuntimeError("Replacement Hub server did not pass health checks")
    except (OSError, RuntimeError, ValueError, sqlite3.Error) as exc:
        repository.finish_hub_restart(
            restart_id,
            status="failed",
            error=str(exc),
        )


def main() -> None:
    if len(sys.argv) != 2:
        raise SystemExit("usage: python -m copilot_hub.restart RESTART_ID")
    run_restart_helper(sys.argv[1])


if __name__ == "__main__":
    main()
