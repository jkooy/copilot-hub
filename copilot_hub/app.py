from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any
from urllib.parse import urlsplit

from fastapi import FastAPI, HTTPException, Request, WebSocket
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

from .agent_runtime import (
    CopilotAgentRuntime,
    ManagerResultDispatchBlockedError,
    agent_task_result_content,
)
from .config import (
    MAX_AGENT_MAX_WORKERS,
    MIN_AGENT_MAX_WORKERS,
    Settings,
    total_copilot_sessions,
)
from .db import Repository, utc_now
from .memory import MemoryMonitor
from .presentation import (
    WORKER_STATUS_PRESENTATIONS,
    active_work_items,
    concise_current_work_title,
    worker_display_name,
    worker_status_presentation,
)
from .restart import (
    activate_server_runtime,
    deactivate_server_runtime,
    spawn_restart_helper,
)
from .terminal_runtime import TerminalRuntime
from .usage import CopilotUsageMonitor

RESULT_TERMINAL_STATUSES = frozenset(
    {"succeeded", "failed", "cancelled", "interrupted"}
)
RESULT_PAGE_SIZE = 100
USAGE_COLLECTION_BYTES = 32 * 1024 * 1024

settings = Settings.from_env()
settings.ensure_directories()
repository = Repository(settings.db_path)
agent_runtime = CopilotAgentRuntime(repository, settings)
terminal_runtime = TerminalRuntime(repository, settings)
memory_monitor = MemoryMonitor()
usage_monitor = CopilotUsageMonitor()
agent_runtime.set_terminal_runtime(terminal_runtime)
server_runtime_state: dict[str, Any] | None = None
package_dir = Path(__file__).parent
templates = Jinja2Templates(directory=package_dir / "templates")
templates.env.auto_reload = False
templates.env.globals["worker_display_name"] = worker_display_name
templates.env.globals["worker_status_presentation"] = worker_status_presentation
templates.env.globals["worker_status_presentations"] = WORKER_STATUS_PRESENTATIONS


def origin_matches_host(origin: str, host: str) -> bool:
    try:
        parsed = urlsplit(origin)
    except ValueError:
        return False
    return (
        parsed.scheme in {"http", "https"}
        and bool(parsed.netloc)
        and parsed.netloc.casefold() == host.casefold()
    )


def agent_worker_state(
    worker: dict[str, Any],
    task: dict[str, Any] | None = None,
) -> dict[str, Any]:
    value = dict(worker)
    value["display_name"] = worker_display_name(
        worker.get("name"),
        role=worker.get("role"),
    )
    current_title = None
    if worker.get("current_task_id"):
        current_title = concise_current_work_title(
            task.get("title") if task else None,
            fallback="Dispatched task",
        )
    elif worker.get("activity_kind") == "direct":
        current_title = concise_current_work_title(
            worker.get("direct_context"),
            fallback="Direct terminal work",
        )
    if worker.get("needs_user_input"):
        value["activity_label"] = (
            f"Needs your input: {current_title or 'Direct terminal work'}"
        )
        value["input_detail"] = (
            worker.get("input_reason")
            or "This session is waiting for your response."
        )
    elif worker.get("state") == "recovering":
        attempt = int(worker.get("recovery_attempt") or 0)
        value["activity_label"] = (
            f"Terminal recovery · attempt {attempt}"
            if attempt
            else "Terminal recovery"
        )
        value["recovery_detail"] = (
            worker.get("recovery_error")
            or "Waiting for Copilot to become ready for input"
        )
    else:
        value["activity_label"] = current_title
    return value


def agent_task_state(task: dict[str, Any]) -> dict[str, Any]:
    value = dict(task)
    worker_name = task.get("worker_name")
    value["worker_display_name"] = (
        worker_display_name(worker_name) if worker_name else None
    )
    return value


def agent_worker_states(
    tasks: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    tasks_by_id = {task["id"]: task for task in tasks if task.get("id")}
    return [
        agent_worker_state(
            worker,
            tasks_by_id.get(worker.get("current_task_id")),
        )
        for worker in repository.list_agent_workers()
    ]


def reporting_state(workers: list[dict[str, Any]]) -> dict[str, Any]:
    workers_by_id = {worker["id"]: worker for worker in workers}
    handoffs = [
        {
            **update,
            "worker_display_name": worker_display_name(
                update.get("worker_name"),
                role=update.get("worker_role"),
            ),
        }
        for update in repository.list_worker_handoffs(limit=12)
    ]
    usage_reports = []
    usage_budget = USAGE_COLLECTION_BYTES
    for metrics in repository.list_agent_usage_metrics():
        completed = sum(
            int(metrics.get(key) or 0)
            for key in (
                "succeeded_count",
                "failed_count",
                "cancelled_count",
                "interrupted_count",
            )
        )
        usage, consumed = usage_monitor.session_usage_with_budget(
            metrics["copilot_session_id"],
            usage_budget,
        )
        usage_budget = max(0, usage_budget - consumed)
        active_worker = workers_by_id.get(metrics["id"])
        usage_reports.append(
            {
                **metrics,
                "display_name": worker_display_name(
                    metrics["name"],
                    role=metrics["role"],
                ),
                "completed_count": completed,
                "success_rate": (
                    round(
                        100
                        * int(metrics["succeeded_count"] or 0)
                        / completed
                    )
                    if completed
                    else None
                ),
                "rss_bytes": (
                    active_worker.get("rss_bytes") if active_worker else None
                ),
                "usage": usage,
            }
        )
    return {"handoffs": handoffs, "usage": usage_reports}


def initialize_result_inbox() -> str:
    started_at = repository.get_state("agent_results_started_at")
    if not started_at:
        started_at = utc_now()
        repository.set_state("agent_results_started_at", started_at)
    return started_at


@asynccontextmanager
async def lifespan(_: FastAPI):
    global server_runtime_state
    repository.initialize()
    agent_runtime.initialize_auto_manager_replies()
    runtime = activate_server_runtime(repository, settings)
    server_runtime_state = runtime
    repository.recover_hub_restarts(
        current_server_generation=str(runtime["generation"]),
    )
    initialize_result_inbox()
    agent_runtime.ensure_manager()
    terminal_runtime.cleanup_orphaned_terminals()
    try:
        terminal_runtime.recover()
        agent_runtime.recover()
        yield
    finally:
        agent_runtime.shutdown()
        terminal_runtime.shutdown()
        deactivate_server_runtime(repository, settings, runtime)
        server_runtime_state = None


app = FastAPI(title="Copilot Hub", lifespan=lifespan)
app.mount("/static", StaticFiles(directory=package_dir / "static"), name="static")


@app.middleware("http")
async def reject_cross_origin_mutations(request: Request, call_next: Any):
    if request.method in {"POST", "PUT", "PATCH", "DELETE"}:
        origin = request.headers.get("origin")
        host = request.headers.get("host")
        if origin and (not host or not origin_matches_host(origin, host)):
            return JSONResponse(
                {"detail": "Cross-origin mutation rejected"},
                status_code=403,
            )
    return await call_next(request)


@app.get("/api/health")
def health_api():
    runtime = server_runtime_state or repository.current_hub_server()
    if not runtime:
        raise HTTPException(503, "Hub server runtime is not registered")
    return {
        "status": "healthy",
        "generation": runtime["generation"],
        "pid": runtime["pid"],
        "started_at": runtime.get("started_at"),
        "restart_token": runtime.get("restart_token"),
    }


@app.get("/", response_class=HTMLResponse)
def workspace(request: Request, worker: str | None = None):
    tasks = repository.list_agent_tasks(
        limit=100,
        exclude_terminal_manager_updates=True,
    )
    workers = agent_worker_states(tasks)
    manager = next(
        (item for item in workers if item.get("role") == "manager"),
        None,
    )
    if manager is None:
        manager = agent_worker_state(agent_runtime.ensure_manager())
        workers.insert(0, manager)
    selected = next(
        (item for item in workers if item["id"] == worker),
        manager,
    )
    result_started_at = initialize_result_inbox()
    results, result_summary = repository.agent_result_inbox_page(
        started_at=result_started_at,
        limit=RESULT_PAGE_SIZE,
    )
    memory = memory_monitor.snapshot(workers)
    for item in workers:
        item["rss_bytes"] = memory["worker_rss_bytes"].get(item["id"])
    reporting = reporting_state(workers)
    return templates.TemplateResponse(
        request=request,
        name="terminal.html",
        context={
            "workers": workers,
            "selected_worker": selected,
            "tasks": [agent_task_state(task) for task in tasks],
            "active_work": active_work_items(tasks, workers),
            "results": [agent_task_state(task) for task in results],
            "result_summary": result_summary,
            "reporting": reporting,
            "memory": {
                key: value
                for key, value in memory.items()
                if key != "worker_rss_bytes"
            },
            "max_workers": agent_runtime.max_workers,
            "max_sessions": total_copilot_sessions(agent_runtime.max_workers),
        },
    )


@app.websocket("/ws/agents/terminal/{worker_id}")
async def agent_terminal_socket(websocket: WebSocket, worker_id: str):
    origin = websocket.headers.get("origin")
    host = websocket.headers.get("host")
    if origin and (not host or not origin_matches_host(origin, host)):
        await websocket.close(code=4403)
        return
    worker = repository.get_agent_worker(worker_id)
    if worker is None or worker.get("retired_at"):
        await websocket.close(code=4404)
        return
    client_id = websocket.query_params.get("client_id") or ""
    requested_stream_id = websocket.query_params.get("stream_id")
    requested_offset_raw = websocket.query_params.get("offset")
    try:
        requested_offset = (
            int(requested_offset_raw)
            if requested_offset_raw is not None
            else None
        )
    except ValueError:
        requested_offset = None
    await terminal_runtime.attach(
        websocket,
        worker,
        client_id,
        requested_stream_id,
        requested_offset,
    )


@app.get("/api/agents/terminal/{worker_id}/history")
def agent_terminal_history(worker_id: str):
    worker = repository.get_agent_worker(worker_id)
    if worker is None or worker.get("retired_at"):
        raise HTTPException(404, "Session was not found")
    return {
        "worker_id": worker_id,
        "session_id": worker["copilot_session_id"],
        "history": terminal_runtime.history(worker["copilot_session_id"]),
    }


@app.post("/api/agents/chat")
async def agent_chat_api(request: Request):
    payload = await request.json()
    message = str(payload.get("message") or "").strip()
    if not message:
        raise HTTPException(400, "Message is required")
    try:
        return agent_runtime.submit_manager_message(message)
    except ManagerResultDispatchBlockedError as exc:
        raise HTTPException(409, str(exc)) from exc


@app.post("/api/agents/manager/prompt")
async def manager_terminal_prompt_api(request: Request):
    payload = await request.json()
    message = str(payload.get("message") or "").strip()
    if not message:
        raise HTTPException(400, "Message is required")
    manager = agent_runtime.ensure_manager()
    try:
        terminal_runtime.send_prompt(manager, message)
    except RuntimeError as exc:
        raise HTTPException(409, str(exc)) from exc
    return {"status": "submitted", "worker_id": manager["id"]}


@app.post("/api/agents/tasks")
async def create_agent_task_api(request: Request):
    payload = await request.json()
    title = str(payload.get("title") or "").strip()
    prompt = str(payload.get("prompt") or "").strip()
    if not title or not prompt:
        raise HTTPException(400, "title and prompt are required")
    try:
        return agent_runtime.submit_worker_task(
            title=title,
            prompt=prompt,
            parent_task_id=(
                str(payload["parent_task_id"])
                if payload.get("parent_task_id")
                else None
            ),
            requires_approval=bool(payload.get("requires_approval")),
            task_type=str(payload.get("task_type") or "work"),
        )
    except ManagerResultDispatchBlockedError as exc:
        raise HTTPException(409, str(exc)) from exc


@app.post("/api/agents/tasks/{task_id}/approve")
def approve_agent_task_api(task_id: str):
    try:
        return agent_runtime.approve_task(task_id)
    except KeyError as exc:
        raise HTTPException(404, "Task was not found") from exc
    except ValueError as exc:
        raise HTTPException(409, str(exc)) from exc


@app.post("/api/agents/tasks/{task_id}/cancel")
def cancel_agent_task_api(task_id: str):
    try:
        return agent_runtime.cancel_task(task_id)
    except KeyError as exc:
        raise HTTPException(404, "Task was not found") from exc
    except ValueError as exc:
        raise HTTPException(409, str(exc)) from exc


@app.post("/api/agents/tasks/{task_id}/view-result")
def view_agent_task_result_api(task_id: str):
    task = repository.get_agent_task(task_id)
    if task is None or task["status"] not in RESULT_TERMINAL_STATUSES:
        raise HTTPException(404, "Task result was not found")
    repository.merge_agent_task_result_metadata(
        task_id,
        {},
        viewed_at=utc_now(),
    )
    return {"task_id": task_id, "result": agent_task_result_content(task)}


@app.post("/api/agents/tasks/{task_id}/clear-result")
def clear_agent_task_result_api(task_id: str):
    task = repository.get_agent_task(task_id)
    if task is None or task["status"] not in RESULT_TERMINAL_STATUSES:
        raise HTTPException(404, "Task result was not found")
    if not repository.clear_agent_task_result(task_id):
        raise HTTPException(409, "Task result was already cleared")
    return {"task_id": task_id, "cleared": True}


@app.post("/api/agents/tasks/{task_id}/send-to-manager")
async def send_agent_task_result_to_manager_api(task_id: str):
    task = repository.get_agent_task(task_id)
    if task is None:
        raise HTTPException(404, "Task was not found")
    followup = await asyncio.to_thread(
        agent_runtime.submit_worker_result_to_manager,
        task_id,
        automatic=False,
    )
    if followup is None:
        raise HTTPException(409, "Task result cannot be sent to the Manager")
    return {"task_id": task_id, "manager_task_id": followup["id"]}


@app.post("/api/agents/config")
async def agent_config_api(request: Request):
    payload = await request.json()
    try:
        value = int(payload["max_workers"])
    except (KeyError, TypeError, ValueError) as exc:
        raise HTTPException(400, "max_workers must be an integer") from exc
    if not MIN_AGENT_MAX_WORKERS <= value <= MAX_AGENT_MAX_WORKERS:
        raise HTTPException(
            400,
            (
                f"max_workers must be between {MIN_AGENT_MAX_WORKERS} "
                f"and {MAX_AGENT_MAX_WORKERS}"
            ),
        )
    return {
        "max_workers": agent_runtime.set_max_workers(value),
        "max_sessions": total_copilot_sessions(value),
    }


@app.post("/api/agents/workers")
def create_agent_worker_api():
    try:
        return agent_worker_state(agent_runtime.add_worker())
    except ValueError as exc:
        raise HTTPException(409, str(exc)) from exc


@app.delete("/api/agents/workers/{worker_id}")
def retire_agent_worker_api(worker_id: str):
    try:
        return agent_worker_state(agent_runtime.retire_worker(worker_id))
    except KeyError as exc:
        raise HTTPException(404, "Session was not found") from exc
    except ValueError as exc:
        raise HTTPException(409, str(exc)) from exc


@app.get("/api/agents/state")
def agent_state_api(compact: bool = False):
    tasks = repository.list_agent_tasks(
        limit=100,
        include_result=not compact,
        exclude_terminal_manager_updates=True,
    )
    workers = agent_worker_states(tasks)
    manager = next(
        (item for item in workers if item.get("role") == "manager"),
        None,
    )
    memory = memory_monitor.snapshot(workers)
    for worker in workers:
        worker["rss_bytes"] = memory["worker_rss_bytes"].get(worker["id"])
    return {
        "manager": manager,
        "workers": workers,
        "tasks": [agent_task_state(task) for task in tasks],
        "active_work": active_work_items(tasks, workers),
        "messages": repository.list_agent_messages(limit=100),
        "manager_updates": repository.list_agent_manager_updates(limit=20),
        "manager_update_count": repository.agent_manager_update_count(),
        "result_summary": repository.agent_result_summary(
            started_at=initialize_result_inbox(),
        ),
        "max_workers": agent_runtime.max_workers,
        "max_sessions": total_copilot_sessions(agent_runtime.max_workers),
        "memory": {
            key: value
            for key, value in memory.items()
            if key != "worker_rss_bytes"
        },
        "reporting": reporting_state(workers),
    }


@app.post("/api/agents/manager-updates/{message_id}/dismiss")
def dismiss_agent_manager_update_api(message_id: str):
    if not repository.dismiss_agent_manager_update(message_id):
        raise HTTPException(404, "Manager update was not found")
    return {"message_id": message_id, "dismissed": True}


@app.get("/api/agents/results")
def agent_results_api(before: str | None = None):
    try:
        results, summary = repository.agent_result_inbox_page(
            started_at=initialize_result_inbox(),
            limit=RESULT_PAGE_SIZE,
            before_task_id=before,
        )
    except KeyError as exc:
        raise HTTPException(404, "Result cursor was not found") from exc
    return {
        "results": [agent_task_state(task) for task in results],
        "summary": summary,
        "next_before": results[-1]["id"] if len(results) == RESULT_PAGE_SIZE else None,
    }


@app.get("/api/agents/tasks/{task_id}")
def agent_task_api(task_id: str):
    task = repository.get_agent_task(task_id)
    if task is None:
        raise HTTPException(404, "Task was not found")
    return {
        "task": agent_task_state(task),
        "events": repository.list_agent_events(task_id),
    }


@app.post("/api/control/restart")
async def restart_hub_api(request: Request):
    runtime = server_runtime_state
    if runtime is None:
        raise HTTPException(503, "Hub server runtime is not registered")
    payload = await request.json()
    worker_id = str(payload.get("worker_id") or "").strip() or None
    worker = repository.get_agent_worker(worker_id) if worker_id else None
    activity_kind = "external"
    task_id = None
    direct_generation = None
    interaction_id = None
    copilot_session_id = None
    terminal_generation = None
    if worker_id:
        if worker is None:
            raise HTTPException(404, "Restart initiator session was not found")
        worker = terminal_runtime.reconcile_pending_direct_activity(worker)
        copilot_session_id = str(payload.get("copilot_session_id") or "")
        try:
            terminal_generation = int(payload["terminal_generation"])
        except (KeyError, TypeError, ValueError) as exc:
            raise HTTPException(
                400,
                "Restart terminal generation is required",
            ) from exc
        if worker.get("direct_interaction_id"):
            direct_generation = int(worker.get("direct_generation") or 0)
            interaction_id = str(worker["direct_interaction_id"])
            activity_kind = "direct"
        elif worker.get("current_task_id"):
            task_id = str(worker["current_task_id"])
            task = repository.get_agent_task(task_id)
            interaction_id = (
                str(task.get("dispatch_interaction_id") or "") if task else ""
            )
            activity_kind = "task"
        else:
            raise HTTPException(409, "Restart initiator has no active interaction")
        if not interaction_id:
            raise HTTPException(
                409,
                "Restart interaction is not authoritatively accepted yet",
            )
    try:
        restart = repository.begin_hub_restart(
            source_server_generation=str(runtime["generation"]),
            source_server_pid=int(runtime["pid"]),
            worker_id=worker_id,
            copilot_session_id=copilot_session_id,
            terminal_generation=terminal_generation,
            activity_kind=activity_kind,
            task_id=task_id,
            direct_generation=direct_generation,
            interaction_id=interaction_id,
        )
    except RuntimeError as exc:
        raise HTTPException(409, str(exc)) from exc
    helper_pid = spawn_restart_helper(settings, restart["id"])
    return {
        "restart_id": restart["id"],
        "generation": restart["generation"],
        "status": restart["status"],
        "created": restart["created"],
        "initiator_added": restart["initiator_added"],
        "helper_pid": helper_pid,
        "message": (
            "Restart handoff accepted. The detached helper will restart the "
            "Hub after the initiating interaction reaches terminal state."
        ),
    }


@app.get("/api/control/restart/{restart_id}")
def restart_hub_status_api(restart_id: str):
    restart = repository.get_hub_restart(restart_id)
    if restart is None:
        raise HTTPException(404, "Restart handoff was not found")
    return restart


@app.get("/terminal")
def terminal_redirect() -> RedirectResponse:
    return RedirectResponse("/", status_code=308)
