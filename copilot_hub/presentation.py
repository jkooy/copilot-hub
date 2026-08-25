from __future__ import annotations

import re
from typing import Any, TypedDict


class StatusPresentation(TypedDict):
    css_class: str
    label: str


WORKER_STATUS_PRESENTATIONS: dict[str, StatusPresentation] = {
    "idle": {"css_class": "idle", "label": "Ready"},
    "leased": {"css_class": "working", "label": "Starting"},
    "busy": {"css_class": "working", "label": "Working"},
    "needs_user_input": {
        "css_class": "needs-user-input",
        "label": "Needs your input",
    },
    "recovering": {"css_class": "recovering", "label": "Recovering"},
    "blocked": {"css_class": "blocked", "label": "Blocked"},
    "retired": {"css_class": "retired", "label": "Retired"},
}
ACTIVE_AGENT_TASK_STATUSES = frozenset(
    {"awaiting_approval", "queued", "dispatching", "running", "cancelling"}
)
CONTEXT_DETAIL_LIMIT = 2000
CURRENT_WORK_TITLE_LIMIT = 120
_NUMBERED_WORKER_NAME = re.compile(r"(?:worker-)(?P<number>\d+)", re.IGNORECASE)
_CONTROL_CHARACTERS = re.compile(r"[\x00-\x08\x0b\x0c\x0e-\x1f\x7f]")
_PRIVATE_KEY = re.compile(
    r"-----BEGIN [A-Z0-9 ]*PRIVATE KEY-----.*?"
    r"-----END [A-Z0-9 ]*PRIVATE KEY-----",
    re.DOTALL,
)
_BEARER_TOKEN = re.compile(r"(?i)\bbearer\s+[A-Za-z0-9._~+/=-]{8,}")
_SECRET_ASSIGNMENT = re.compile(
    r"(?i)\b(api[_-]?key|access[_-]?token|auth[_-]?token|password|secret)"
    r"(\s*[:=]\s*)([^\s,;]+)"
)
_SECRET_QUERY_PARAMETER = re.compile(
    r"(?i)([?&](?:token|key|secret|password)=)[^&#\s]+"
)
_KNOWN_TOKEN = re.compile(
    r"\b(?:gh[pousr]_[A-Za-z0-9]{20,}|sk-[A-Za-z0-9_-]{20,})\b"
)


def worker_status_presentation(value: object) -> StatusPresentation:
    normalized = str(value or "unknown").strip().lower()
    return WORKER_STATUS_PRESENTATIONS.get(
        normalized,
        {
            "css_class": normalized,
            "label": normalized.replace("_", " ").replace("-", " ").title(),
        },
    )


def worker_display_name(name: object, *, role: object = None) -> str:
    if str(role or "").strip().lower() == "manager":
        return "Manager"
    value = str(name or "").strip()
    match = _NUMBERED_WORKER_NAME.fullmatch(value)
    if match:
        return f"worker-{match.group('number')}"
    return value or "worker"


def sanitize_context_text(
    value: object,
    *,
    limit: int = CONTEXT_DETAIL_LIMIT,
) -> str:
    text = str(value or "").replace("\r\n", "\n").replace("\r", "\n")
    text = _CONTROL_CHARACTERS.sub("", text)
    text = _PRIVATE_KEY.sub("[private key redacted]", text)
    text = _BEARER_TOKEN.sub("Bearer [redacted]", text)
    text = _SECRET_ASSIGNMENT.sub(r"\1\2[redacted]", text)
    text = _SECRET_QUERY_PARAMETER.sub(r"\1[redacted]", text)
    text = _KNOWN_TOKEN.sub("[token redacted]", text)
    text = "\n".join(line.rstrip() for line in text.splitlines()).strip()
    if len(text) <= limit:
        return text
    return f"{text[: limit - 1].rstrip()}…"


def concise_current_work_title(value: object, *, fallback: str) -> str:
    text = sanitize_context_text(value, limit=CURRENT_WORK_TITLE_LIMIT)
    return " ".join(text.split()) or fallback


def _task_context(task: dict[str, Any]) -> str:
    source = next(
        (
            str(task.get(field) or "").strip()
            for field in ("description", "normalized_prompt", "requested_prompt")
            if str(task.get(field) or "").strip()
        ),
        "",
    )
    return sanitize_context_text(source)


def _active_task_status(task: dict[str, Any]) -> tuple[str, str]:
    if task.get("status") == "awaiting_approval":
        return "awaiting-approval", "Awaiting approval"
    if task.get("needs_user_input"):
        return "needs-user-input", "Needs your input"
    normalized = str(task.get("status") or "").strip().lower()
    if normalized in {"dispatching", "running"}:
        return "working", "Working"
    return normalized, normalized.replace("_", " ").replace("-", " ").title()


def active_work_items(
    tasks: list[dict[str, Any]],
    workers: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    items: list[dict[str, Any]] = []
    workers_by_id = {
        str(worker["id"]): worker for worker in workers if worker.get("id")
    }
    for task in tasks:
        if (
            task.get("task_type") == "manager"
            or task.get("status") not in ACTIVE_AGENT_TASK_STATUSES
        ):
            continue
        title = str(task.get("title") or "Task")
        context = _task_context(task)
        if " ".join(context.split()).casefold() == " ".join(title.split()).casefold():
            context = ""
        worker = workers_by_id.get(str(task.get("assigned_worker_id") or ""))
        worker_id = str(worker["id"]) if worker else None
        worker_name = (
            worker_display_name(worker.get("name"), role=worker.get("role"))
            if worker
            else "waiting for worker"
        )
        visual_status, status_label = _active_task_status(task)
        attention_kind = None
        attention_reason = None
        action_label = None
        action_url = None
        if task.get("status") == "awaiting_approval":
            attention_kind = "approval"
            attention_reason = "Approval is required before this task can start."
            action_label = "Review approval"
            action_url = f"/?focus_task={task['id']}#approval-{task['id']}"
        elif task.get("needs_user_input"):
            attention_kind = "user_input"
            attention_reason = (
                sanitize_context_text(task.get("input_reason"), limit=500)
                or "The worker is waiting for your response."
            )
            action_label = "Respond in terminal"
            if worker_id:
                action_url = f"/?worker={worker_id}&focus_work=task-{task['id']}"
        items.append(
            {
                "key": f"task-{task['id']}",
                "kind": "task",
                "task_id": task["id"],
                "title": title,
                "context": context,
                "status": visual_status,
                "status_label": status_label,
                "worker_id": worker_id,
                "worker_name": worker_name,
                "worker_url": f"/?worker={worker_id}" if worker_id else None,
                "attention_kind": attention_kind,
                "attention_reason": attention_reason,
                "attention_requested_at": task.get("input_requested_at"),
                "action_label": action_label,
                "action_url": action_url,
            }
        )
    return items
