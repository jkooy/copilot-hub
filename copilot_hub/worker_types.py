"""Worker specialties the Manager can route tasks to."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class WorkerType:
    key: str
    label: str
    summary: str
    instructions: str


WORKER_TYPES: dict[str, WorkerType] = {
    worker_type.key: worker_type
    for worker_type in (
        WorkerType(
            key="code",
            label="coder",
            summary="implements features and fixes, writes and runs tests",
            instructions=(
                "You are the coder. Implement the requested change end to end: read the "
                "relevant code first, keep the diff focused, follow the surrounding style, "
                "add or update tests, and run them. Report the files changed and the test "
                "results."
            ),
        ),
        WorkerType(
            key="research",
            label="researcher",
            summary="investigates code, docs, logs, and data without changing them",
            instructions=(
                "You are the researcher. Investigate and answer the question with evidence: "
                "cite files with line numbers, commands, and outputs. Do not modify files "
                "or system state unless the task explicitly asks for it. Separate verified "
                "facts from inferences."
            ),
        ),
        WorkerType(
            key="review",
            label="reviewer",
            summary="reviews diffs and designs for bugs, security issues, and test gaps",
            instructions=(
                "You are the reviewer. Examine the requested change or design for "
                "correctness bugs, security problems, edge cases, and missing tests. Run "
                "the tests where possible. Report findings ranked by severity, each with "
                "a concrete failure scenario. Do not rewrite the code unless asked."
            ),
        ),
        WorkerType(
            key="operate",
            label="operator",
            summary="runs commands, builds, deployments, and environment setup",
            instructions=(
                "You are the operator. Run the requested commands, builds, deployments, or "
                "environment changes step by step, verify the resulting state after each "
                "step, and stop at the first unexpected failure. Report the exact commands "
                "run and their relevant output."
            ),
        ),
    )
}
DEFAULT_WORKER_TYPE = "code"
# Task types used before worker specialties existed.
TASK_TYPE_ALIASES = {"read": "research", "work": "code"}


def worker_type_for_task(task_type: str | None) -> WorkerType:
    key = TASK_TYPE_ALIASES.get(str(task_type or ""), str(task_type or ""))
    return WORKER_TYPES.get(key, WORKER_TYPES[DEFAULT_WORKER_TYPE])


def worker_type_of(worker: dict[str, Any]) -> WorkerType | None:
    for capability in worker.get("capabilities") or []:
        if capability in WORKER_TYPES:
            return WORKER_TYPES[capability]
    return None


def manager_worker_type_guide() -> list[dict[str, str]]:
    return [
        {"task_type": item.key, "worker": item.label, "use_for": item.summary}
        for item in WORKER_TYPES.values()
    ]
