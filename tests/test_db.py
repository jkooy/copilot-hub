from __future__ import annotations

import sqlite3

from copilot_hub.db import Repository, utc_now


def create_repository(tmp_path):
    repository = Repository(tmp_path / "hub.db")
    repository.initialize()
    return repository


def create_worker(repository, *, name="worker-1", role="worker", session="s1"):
    return repository.create_agent_worker(
        copilot_session_id=session,
        name=name,
        role=role,
        model="model",
        reasoning_effort="high",
        context_tier="long",
        cwd="/home/user",
    )


def test_schema_contains_only_generic_persistent_tables(tmp_path):
    repository = create_repository(tmp_path)
    with sqlite3.connect(repository.db_path) as connection:
        tables = {
            row[0]
            for row in connection.execute(
                """
                SELECT name FROM sqlite_master
                WHERE type='table' AND name NOT LIKE 'sqlite_%'
                """
            )
        }
        task_columns = {
            row[1] for row in connection.execute("PRAGMA table_info(agent_tasks)")
        }
    assert tables == {
        "app_state",
        "agent_workers",
        "terminal_input_sequences",
        "agent_tasks",
        "agent_events",
        "agent_messages",
        "worker_handoffs",
        "hub_restarts",
        "hub_restart_initiators",
    }
    assert "parent_task_id" in task_columns
    assert "dispatch_generation" in task_columns
    assert "interruption_recovery_generation" in task_columns


def test_terminal_input_sequence_acceptance_survives_repository_restart(tmp_path):
    db_path = tmp_path / "hub.db"
    first = Repository(db_path)
    first.initialize()
    worker = create_worker(first)

    assert first.accept_terminal_input_sequence(worker["id"], "browser", 1)

    restarted = Repository(db_path)
    restarted.initialize()
    assert not restarted.accept_terminal_input_sequence(
        worker["id"],
        "browser",
        1,
    )
    assert restarted.accept_terminal_input_sequence(worker["id"], "browser", 2)
    assert not restarted.accept_terminal_input_sequence(
        worker["id"],
        "browser",
        1,
    )
    with sqlite3.connect(db_path) as connection:
        assert connection.execute(
            """
            SELECT client_id, last_sequence
            FROM terminal_input_sequences
            WHERE worker_id=?
            """,
            (worker["id"],),
        ).fetchall() == [("browser", 2)]


def test_task_dispatch_generation_result_trigger_and_idempotent_handoff(tmp_path):
    repository = create_repository(tmp_path)
    manager = create_worker(
        repository,
        name="copilot-hub-manager",
        role="manager",
        session="manager",
    )
    repository.set_state("agent_manager_session_id", manager["copilot_session_id"])
    worker = create_worker(repository)
    task = repository.create_agent_task(
        title="Build local tool",
        task_type="code",
        requested_prompt="Build and verify the tool.",
        status="queued",
        manager_session_id=manager["copilot_session_id"],
    )
    assert repository.try_lease_agent_worker(worker["id"], task["id"])
    dispatched = repository.begin_agent_task_dispatch(
        task["id"],
        worker["id"],
        deadline_at="2999-01-01T00:00:00+00:00",
    )
    assert dispatched["dispatch_generation"] == 1
    assert repository.claim_agent_task_dispatch_attempt(
        task["id"],
        worker["id"],
        generation=1,
        attempt_id="attempt-1",
        session_id=worker["copilot_session_id"],
        event_cursor=7,
        terminal_generation=0,
        attempt_limit=4,
    )
    assert repository.accept_agent_task_dispatch(
        task["id"],
        worker["id"],
        generation=1,
        attempt_id="attempt-1",
        session_id=worker["copilot_session_id"],
        event_cursor=7,
        terminal_generation=0,
        interaction_id="interaction-1",
    )
    assert not repository.finish_agent_task_assignment(
        task["id"],
        worker["id"],
        generation=2,
        status="succeeded",
        result={"response": "wrong generation"},
    )
    assert repository.finish_agent_task_assignment(
        task["id"],
        worker["id"],
        generation=1,
        status="succeeded",
        result={"response": "Tool built."},
    )

    source = repository.get_agent_task(task["id"])
    manager_task_id = source["result"]["sent_to_manager_task_id"]
    manager_task = repository.get_agent_task(manager_task_id)
    assert manager_task["normalized_prompt"] == "worker_result_followup"
    assert manager_task["result"]["response"] == "Tool built."
    followup, created = repository.create_agent_result_manager_task(
        task["id"],
        title="Worker result",
        requested_prompt="{}",
        manager_response="Tool built.",
        manager_session_id=manager["copilot_session_id"],
        automatic=True,
        mark_viewed=False,
    )
    assert not created
    assert followup["id"] == manager_task_id


def test_direct_activity_generation_and_input_fences(tmp_path):
    repository = create_repository(tmp_path)
    worker = create_worker(repository)
    direct = repository.register_direct_activity(
        worker["id"],
        event_cursor=10,
        prompt_hash="hash",
    )
    generation = direct["direct_generation"]
    assert repository.accept_direct_activity(
        worker["id"],
        generation=generation,
        interaction_id="turn-1",
        context="Run a local check",
    )
    assert repository.set_direct_input_request(
        worker["id"],
        generation=generation,
        reason="Choose a file",
        source="assistant",
        requested_at=utc_now(),
        event_cursor=12,
        interaction_id="turn-1",
        tool_call_id=None,
    )
    assert repository.get_agent_worker(worker["id"])["needs_user_input"]
    assert not repository.finish_direct_activity(
        worker["id"],
        generation=generation + 1,
        interaction_id="turn-1",
    )
    assert repository.clear_direct_input_request(
        worker["id"],
        generation=generation,
    )
    assert repository.finish_direct_activity(
        worker["id"],
        generation=generation,
        interaction_id="turn-1",
    )
    finished = repository.get_agent_worker(worker["id"])
    assert finished["state"] == "idle"
    assert finished["direct_submitted_at"] is None


def test_automatic_manager_handoff_is_bounded_with_head_and_tail(tmp_path):
    repository = create_repository(tmp_path)
    worker = create_worker(repository)
    task = repository.create_agent_task(
        title="Produce detailed result",
        task_type="research",
        requested_prompt="Produce a detailed result.",
        status="queued",
    )
    content = "important-head\n" + ("detail " * 5000) + "\nimportant-tail"

    repository.update_agent_task(
        task["id"],
        status="succeeded",
        assigned_worker_id=worker["id"],
        result={"response": content},
        finished_at=utc_now(),
    )

    source = repository.get_agent_task(task["id"])
    handoff = repository.get_agent_task(
        source["result"]["sent_to_manager_task_id"]
    )
    response = handoff["result"]["response"]
    assert len(response) <= 16_000
    assert response.startswith("important-head")
    assert response.endswith("important-tail")
    assert "middle omitted" in response


def test_restart_recovery_never_replays_active_initiator(tmp_path):
    repository = create_repository(tmp_path)
    worker = create_worker(repository)
    direct = repository.register_direct_activity(
        worker["id"],
        event_cursor=1,
        prompt_hash="hash",
    )
    repository.accept_direct_activity(
        worker["id"],
        generation=direct["direct_generation"],
        interaction_id="turn",
        context="Restart the Hub",
    )
    source = repository.register_hub_server(pid=123, process_identity="identity")
    restart = repository.begin_hub_restart(
        source_server_generation=source["generation"],
        source_server_pid=123,
        worker_id=worker["id"],
        copilot_session_id=worker["copilot_session_id"],
        terminal_generation=worker["terminal_generation"],
        activity_kind="direct",
        direct_generation=direct["direct_generation"],
        interaction_id="turn",
    )
    repository.recover_hub_restarts(current_server_generation="replacement")

    recovered = repository.get_hub_restart(restart["id"])
    recovered_worker = repository.get_agent_worker(worker["id"])
    assert recovered["status"] == "failed"
    assert recovered["initiators"][0]["state"] == "aborted"
    assert recovered_worker["state"] == "idle"
    assert recovered_worker["copilot_session_id"] != worker["copilot_session_id"]
