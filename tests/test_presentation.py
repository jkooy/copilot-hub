from __future__ import annotations

from copilot_hub.presentation import (
    active_work_items,
    concise_current_work_title,
    sanitize_context_text,
    worker_display_name,
    worker_status_presentation,
)


def test_worker_names_and_unknown_statuses_are_generic():
    assert worker_display_name("copilot-hub-manager", role="manager") == "Manager"
    assert worker_display_name("worker-12") == "worker-12"
    assert worker_display_name("personal-helper") == "personal-helper"
    assert worker_status_presentation("waiting_for_sync") == {
        "css_class": "waiting_for_sync",
        "label": "Waiting For Sync",
    }


def test_context_sanitization_redacts_secrets_and_controls():
    source = (
        "api_" "key=placeholder-value\n"
        "ordinary header\n"
        "https://local.invalid/?token=value123\x00\n"
        "ghp_" "abcdefghijklmnopqrstuvwxyz123456"
    )
    sanitized = sanitize_context_text(source)
    assert "placeholder-value" not in sanitized
    assert "abcdefghijklmnopqrstuvwxyz" not in sanitized
    assert "\x00" not in sanitized
    assert "[redacted]" in sanitized
    assert concise_current_work_title("  update\nlocal files  ", fallback="Task") == (
        "update local files"
    )


def test_active_work_keeps_attention_and_session_navigation():
    workers = [
        {
            "id": "w1",
            "name": "worker-1",
            "role": "worker",
        }
    ]
    tasks = [
        {
            "id": "t1",
            "task_type": "code",
            "status": "running",
            "title": "Implement feature",
            "requested_prompt": "Implement feature with tests",
            "assigned_worker_id": "w1",
        },
        {
            "id": "t2",
            "task_type": "operate",
            "status": "awaiting_approval",
            "title": "Run command",
            "requested_prompt": "Run the migration command",
            "assigned_worker_id": None,
        },
        {
            "id": "t3",
            "task_type": "manager",
            "status": "running",
            "title": "Manager turn",
            "requested_prompt": "Answer",
        },
    ]

    items = active_work_items(tasks, workers)

    assert [item["task_id"] for item in items] == ["t1", "t2"]
    assert items[0]["worker_url"] == "/?worker=w1"
    assert items[1]["attention_kind"] == "approval"
    assert items[1]["action_label"] == "Review approval"
