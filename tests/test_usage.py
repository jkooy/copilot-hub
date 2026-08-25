from __future__ import annotations

import json

from copilot_hub.db import Repository, utc_now
from copilot_hub.usage import CopilotUsageMonitor


def write_events(path, events):
    path.parent.mkdir(parents=True)
    path.write_text(
        "".join(json.dumps(event) + "\n" for event in events),
        encoding="utf-8",
    )


def test_usage_monitor_reports_completed_and_live_usage(tmp_path):
    sessions = tmp_path / "sessions"
    events = sessions / "session-1" / "events.jsonl"
    write_events(
        events,
        [
            {
                "type": "session.shutdown",
                "data": {
                    "currentModel": "model-a",
                    "totalNanoAiu": 12,
                    "totalPremiumRequests": 1.5,
                    "modelMetrics": {
                        "model-a": {
                            "usage": {
                                "inputTokens": 100,
                                "outputTokens": 40,
                                "reasoningTokens": 15,
                                "cacheReadTokens": 5,
                                "cacheWriteTokens": 2,
                            }
                        }
                    },
                },
            },
            {
                "type": "session.usage_checkpoint",
                "data": {
                    "totalNanoAiu": 15,
                    "totalPremiumRequests": 2,
                    "modelCacheState": [{"modelId": "model-b"}],
                },
            },
        ],
    )
    monitor = CopilotUsageMonitor(sessions)

    usage = monitor.session_usage("session-1")

    assert usage["recorded_tokens"] == 140
    assert usage["reasoning_tokens"] == 15
    assert usage["nano_aiu"] == 15
    assert usage["premium_requests"] == 2
    assert usage["models"] == ["model-a", "model-b"]


def test_worker_handoff_and_usage_metrics_are_local(tmp_path):
    repository = Repository(tmp_path / "hub.db")
    repository.initialize()
    worker = repository.create_agent_worker(
        copilot_session_id="session-1",
        name="worker-1",
        role="worker",
        model="model",
        reasoning_effort="high",
        context_tier="long",
        cwd=str(tmp_path),
    )
    task = repository.create_agent_task(
        title="Create local file",
        task_type="code",
        requested_prompt="Create a local file.",
        status="queued",
    )
    assert repository.try_lease_agent_worker(worker["id"], task["id"])
    dispatched = repository.begin_agent_task_dispatch(
        task["id"],
        worker["id"],
        deadline_at=utc_now(),
    )
    repository.update_agent_task(
        task["id"],
        status="succeeded",
        assigned_worker_id=worker["id"],
        result={"response": "Created the file."},
        started_at=utc_now(),
        finished_at=utc_now(),
    )

    updates = repository.list_worker_handoffs()
    metrics = repository.list_agent_usage_metrics()

    assert dispatched is not None
    assert updates[0]["summary"] == "Created the file."
    assert metrics[0]["task_count"] == 1
    assert metrics[0]["succeeded_count"] == 1
