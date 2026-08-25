from __future__ import annotations

import importlib
import sys

import pytest
from fastapi.testclient import TestClient


@pytest.fixture
def app_module(tmp_path, monkeypatch):
    monkeypatch.setenv("HOME", str(tmp_path))
    monkeypatch.setenv("COPILOT_HUB_STATE_DIR", str(tmp_path / "state"))
    monkeypatch.setenv("COPILOT_HUB_CWD", str(tmp_path / "work"))
    monkeypatch.setenv("COPILOT_HUB_TERMINALS", "0")
    sys.modules.pop("copilot_hub.app", None)
    module = importlib.import_module("copilot_hub.app")
    monkeypatch.setattr(
        module,
        "activate_server_runtime",
        lambda repository, settings: {
            "generation": "server-generation",
            "pid": 12345,
            "started_at": "2026-01-01T00:00:00+00:00",
            "restart_token": None,
        },
    )
    monkeypatch.setattr(
        module,
        "deactivate_server_runtime",
        lambda repository, settings, runtime: None,
    )
    monkeypatch.setattr(module.terminal_runtime, "cleanup_orphaned_terminals", list)
    monkeypatch.setattr(module.terminal_runtime, "recover", lambda: None)
    monkeypatch.setattr(module.terminal_runtime, "shutdown", lambda: None)
    monkeypatch.setattr(module.agent_runtime, "recover", lambda: None)
    monkeypatch.setattr(module.agent_runtime, "shutdown", lambda: None)
    return module


def test_workspace_health_and_generic_state(app_module):
    with TestClient(app_module.app) as client:
        health = client.get("/api/health")
        workspace = client.get("/")
        state = client.get("/api/agents/state")

    assert health.status_code == 200
    assert health.json()["generation"] == "server-generation"
    assert workspace.status_code == 200
    assert "Personal Manager and Worker sessions" in workspace.text
    assert "Active work" in workspace.text
    payload = state.json()
    assert payload["manager"]["display_name"] == "Manager"
    assert payload["max_sessions"] == payload["max_workers"] + 1
    assert set(payload) >= {
        "workers",
        "tasks",
        "active_work",
        "results" if False else "result_summary",
        "memory",
        "reporting",
    }


def test_task_result_and_worker_apis(app_module):
    with TestClient(app_module.app) as client:
        worker = client.post("/api/agents/workers")
        assert worker.status_code == 200
        assert worker.json()["name"] == "worker-1"

        configured = client.post(
            "/api/agents/config",
            json={"max_workers": 4},
        )
        assert configured.json() == {"max_workers": 4, "max_sessions": 5}

        created = client.post(
            "/api/agents/tasks",
            json={
                "title": "Review local files",
                "prompt": "Review the files and report findings.",
                "task_type": "read",
                "requires_approval": True,
            },
        )
        assert created.status_code == 200
        task_id = created.json()["id"]
        details = client.get(f"/api/agents/tasks/{task_id}")
        assert details.json()["task"]["status"] == "awaiting_approval"
        cancelled = client.post(f"/api/agents/tasks/{task_id}/cancel")
        assert cancelled.json()["status"] == "cancelled"

        result_task = app_module.repository.create_agent_task(
            title="Finished review",
            task_type="read",
            requested_prompt="Review.",
            status="succeeded",
        )
        app_module.repository.update_agent_task(
            result_task["id"],
            result={"response": "Review complete."},
            finished_at="2999-01-01T00:00:00+00:00",
        )
        inbox = client.get("/api/agents/results")
        assert inbox.status_code == 200
        assert inbox.json()["results"][0]["result"]["response"] == "Review complete."
        viewed = client.post(
            f"/api/agents/tasks/{result_task['id']}/view-result"
        )
        assert viewed.json()["result"] == "Review complete."
        cleared = client.post(
            f"/api/agents/tasks/{result_task['id']}/clear-result"
        )
        assert cleared.json()["cleared"]


def test_cross_origin_mutation_is_rejected(app_module):
    with TestClient(app_module.app) as client:
        response = client.post(
            "/api/agents/config",
            json={"max_workers": 2},
            headers={
                "origin": "https://untrusted.invalid",
                "host": "testserver",
            },
        )
    assert response.status_code == 403


def test_terminal_history_and_redirect(app_module):
    with TestClient(app_module.app) as client:
        state = client.get("/api/agents/state").json()
        manager_id = state["manager"]["id"]
        history = client.get(f"/api/agents/terminal/{manager_id}/history")
        redirect = client.get("/terminal", follow_redirects=False)

    assert history.status_code == 200
    assert history.json()["worker_id"] == manager_id
    assert redirect.status_code == 308
    assert redirect.headers["location"] == "/"
