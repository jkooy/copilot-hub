from __future__ import annotations

import argparse
import ipaddress
import json
import os
import sys
import urllib.error
import urllib.request
import webbrowser

import uvicorn

from . import dispatch_inbox
from .config import (
    DEFAULT_AGENT_MAX_WORKERS,
    Settings,
    total_copilot_sessions,
)
from .db import Repository
from .restart import normalized_tool_path
from .worker_types import TASK_TYPE_ALIASES, WORKER_TYPES, worker_type_of


def validate_bind_host(host: str, *, allow_remote: bool) -> str:
    normalized = host.strip().strip("[]")
    if normalized.lower() == "localhost":
        return host
    try:
        loopback = ipaddress.ip_address(normalized).is_loopback
    except ValueError:
        loopback = False
    if not loopback and not allow_remote:
        raise ValueError(
            "Refusing to expose Copilot Hub beyond loopback without "
            "COPILOT_HUB_ALLOW_REMOTE=1"
        )
    return host


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="copilot-hub",
        description=(
            "Operate a personal Copilot Hub. The default pool allows "
            f"{DEFAULT_AGENT_MAX_WORKERS} worker sessions "
            f"({total_copilot_sessions(DEFAULT_AGENT_MAX_WORKERS)} total "
            "including the manager); workers are created only as needed."
        ),
    )
    subparsers = parser.add_subparsers(dest="command", required=True)
    subparsers.add_parser("init", help="Initialize local state")

    serve = subparsers.add_parser("serve", help="Run the web server")
    serve.add_argument("--host")
    serve.add_argument("--port", type=int)

    dispatch = subparsers.add_parser(
        "dispatch",
        help="Dispatch a task to a persistent worker",
    )
    dispatch.add_argument("--title", required=True)
    dispatch.add_argument(
        "--prompt",
        required=True,
        help="Task prompt, or - to read it from stdin",
    )
    dispatch.add_argument(
        "--task-type",
        default="code",
        choices=[*WORKER_TYPES, *TASK_TYPE_ALIASES],
        help="Worker type that should take the task",
    )
    dispatch.add_argument("--requires-approval", action="store_true")

    subparsers.add_parser("status", help="Show session and task state")
    subparsers.add_parser(
        "restart",
        help="Hand restart to the detached generation-fenced controller",
    )
    subparsers.add_parser("open", help="Open the browser workspace")
    return parser


def post_json(url: str, payload: dict[str, object]) -> dict[str, object]:
    request = urllib.request.Request(
        url,
        data=json.dumps(payload).encode(),
        headers={"Content-Type": "application/json"},
    )
    try:
        with urllib.request.urlopen(request, timeout=10) as response:
            return json.load(response)
    except urllib.error.URLError as exc:
        raise RuntimeError("Copilot Hub server is not reachable") from exc


def get_json(url: str) -> dict[str, object]:
    try:
        with urllib.request.urlopen(url, timeout=10) as response:
            return json.load(response)
    except urllib.error.URLError as exc:
        raise RuntimeError("Copilot Hub server is not reachable") from exc


def in_managed_session() -> bool:
    return bool(os.environ.get("COPILOT_HUB_WORKER_ID"))


def local_status(repository: Repository) -> dict[str, object]:
    """Worker and task summary read straight from the Hub database."""
    workers = repository.list_agent_workers()
    names = {worker["id"]: worker["name"] for worker in workers}
    return {
        "workers": [
            {
                "name": worker["name"],
                "role": worker["role"],
                "worker_type": (
                    worker_type_of(worker).key if worker_type_of(worker) else None
                ),
                "state": worker["state"],
                "current_task_id": worker.get("current_task_id"),
            }
            for worker in workers
        ],
        "recent_tasks": [
            {
                "id": task["id"],
                "title": task["title"],
                "task_type": task["task_type"],
                "status": task["status"],
                "worker": names.get(task.get("assigned_worker_id")),
            }
            for task in repository.list_agent_tasks(limit=20, include_result=False)
            if task["task_type"] != "manager"
        ],
    }


def main() -> None:
    args = build_parser().parse_args()
    settings = Settings.from_env()
    settings.ensure_directories()
    repository = Repository(settings.db_path)
    repository.initialize()
    base_url = f"http://{settings.host}:{settings.port}"

    if args.command == "init":
        print(settings.db_path)
        return
    if args.command == "serve":
        os.environ["PATH"] = normalized_tool_path()
        host = validate_bind_host(
            args.host or settings.host,
            allow_remote=settings.allow_remote,
        )
        uvicorn.run(
            "copilot_hub.app:app",
            host=host,
            port=args.port or settings.port,
        )
        return
    if args.command == "dispatch":
        prompt = sys.stdin.read() if args.prompt == "-" else args.prompt
        if not prompt.strip():
            raise SystemExit("copilot-hub dispatch: the prompt is empty")
        payload: dict[str, object] = {
            "title": args.title,
            "prompt": prompt.strip(),
            "task_type": args.task_type,
            "requires_approval": args.requires_approval,
        }
        # Managed sessions run shell commands in a sandbox without loopback network
        # access, so they hand the request to the server through the state directory.
        if in_managed_session():
            result = dispatch_inbox.submit_request(settings.state_dir, payload)
        else:
            try:
                result = post_json(f"{base_url}/api/agents/tasks", payload)
            except RuntimeError:
                result = dispatch_inbox.submit_request(settings.state_dir, payload)
        print(json.dumps(result, indent=2, ensure_ascii=False))
        if result.get("status") == "error":
            raise SystemExit(1)
        return
    if args.command == "status":
        state = None
        if not in_managed_session():
            try:
                state = get_json(f"{base_url}/api/agents/state?compact=true")
            except RuntimeError:
                state = None
        print(json.dumps(state or local_status(repository), indent=2, ensure_ascii=False))
        return
    if args.command == "restart":
        worker_id = os.environ.get("COPILOT_HUB_WORKER_ID")
        payload: dict[str, object] = {}
        if worker_id:
            payload = {
                "worker_id": worker_id,
                "copilot_session_id": os.environ.get(
                    "COPILOT_HUB_SESSION_ID",
                    "",
                ),
                "terminal_generation": os.environ.get(
                    "COPILOT_HUB_TERMINAL_GENERATION",
                    "",
                ),
            }
        print(
            json.dumps(
                post_json(f"{base_url}/api/control/restart", payload),
                indent=2,
            )
        )
        return
    if args.command == "open":
        webbrowser.open(base_url)


if __name__ == "__main__":
    main()
