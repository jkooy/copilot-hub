from __future__ import annotations

from pathlib import Path

from copilot_hub.db import Repository


def test_start_script_parses_health_pid():
    script = (
        Path(__file__).parents[1] / "scripts" / "start.sh"
    ).read_text(encoding="utf-8")

    assert (
        """sed -n 's/.*"pid":\\([0-9][0-9]*\\).*/\\1/p'"""
        in script
    )


def test_external_restart_is_generation_fenced_and_idempotent(tmp_path):
    repository = Repository(tmp_path / "hub.db")
    repository.initialize()
    source = repository.register_hub_server(
        pid=123,
        process_identity="source-identity",
    )
    restart = repository.begin_hub_restart(
        source_server_generation=source["generation"],
        source_server_pid=123,
    )

    assert restart["status"] == "handed_off"
    assert restart["created"]
    assert repository.claim_hub_restart_helper(
        restart["id"],
        helper_pid=456,
    )
    assert not repository.claim_hub_restart_helper(
        restart["id"],
        helper_pid=789,
    )
    assert repository.begin_hub_restart_stop(
        restart["id"],
        source_server_generation=source["generation"],
        source_server_pid=123,
    )
    assert repository.mark_hub_restart_starting(restart["id"])
    replacement = repository.register_hub_server(
        pid=789,
        restart_token=restart["id"],
        process_identity="replacement-identity",
    )
    assert replacement["generation"] != source["generation"]
    assert repository.mark_hub_restart_healthy(
        restart["id"],
        replacement_server_generation=replacement["generation"],
        replacement_server_pid=789,
    )
    assert repository.get_hub_restart(restart["id"])["status"] == "healthy"


def test_direct_restart_waits_for_terminal_generation(tmp_path):
    repository = Repository(tmp_path / "hub.db")
    repository.initialize()
    worker = repository.create_agent_worker(
        copilot_session_id="session",
        name="worker-1",
        role="worker",
        model="model",
        reasoning_effort="high",
        context_tier="long",
        cwd=str(tmp_path),
    )
    direct = repository.register_direct_activity(
        worker["id"],
        event_cursor=1,
        prompt_hash="hash",
    )
    repository.accept_direct_activity(
        worker["id"],
        generation=direct["direct_generation"],
        interaction_id="turn",
        context="Restart",
    )
    source = repository.register_hub_server(pid=111, process_identity="identity")
    restart = repository.begin_hub_restart(
        source_server_generation=source["generation"],
        source_server_pid=111,
        worker_id=worker["id"],
        copilot_session_id=worker["copilot_session_id"],
        terminal_generation=worker["terminal_generation"],
        activity_kind="direct",
        direct_generation=direct["direct_generation"],
        interaction_id="turn",
    )
    assert restart["status"] == "requested"
    assert repository.reconcile_hub_restart_handoff(restart["id"])["status"] == (
        "requested"
    )
    assert repository.finish_direct_activity(
        worker["id"],
        generation=direct["direct_generation"],
        interaction_id="turn",
    )
    assert repository.reconcile_hub_restart_handoff(restart["id"])["status"] == (
        "handed_off"
    )
