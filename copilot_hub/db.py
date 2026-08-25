from __future__ import annotations

import json
import sqlite3
import uuid
from collections.abc import Iterator
from contextlib import contextmanager, nullcontext
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any


def utc_now() -> str:
    return datetime.now(UTC).isoformat()


MANAGER_UPDATE_MAX_CHARS = 16_000
MANAGER_UPDATE_MARKER = "\n\n[... middle omitted by the Hub worker handoff ...]\n\n"
TERMINAL_TASK_STATUSES = frozenset(
    {"succeeded", "failed", "cancelled", "interrupted"}
)


class AgentTaskCreationBlockedError(RuntimeError):
    pass


def agent_manager_update_content(result_json: str, status: str) -> str:
    try:
        result = json.loads(result_json or "{}")
    except json.JSONDecodeError:
        result = {}
    if not isinstance(result, dict):
        result = {}
    content = str(
        result.get("response")
        or result.get("error")
        or f"Task ended with status {status} without result text."
    )
    if len(content) <= MANAGER_UPDATE_MAX_CHARS:
        return content
    available = MANAGER_UPDATE_MAX_CHARS - len(MANAGER_UPDATE_MARKER)
    tail_length = available // 3
    return (
        content[: available - tail_length].rstrip()
        + MANAGER_UPDATE_MARKER
        + content[-tail_length:].lstrip()
    )


JSON_COLUMNS = {
    "capabilities_json": "capabilities",
    "result_json": "result",
    "payload_json": "payload",
    "snapshot_json": "snapshot",
}

AGENT_TASK_COLUMNS_WITHOUT_RESULT = (
    "id",
    "parent_task_id",
    "title",
    "task_type",
    "requested_prompt",
    "normalized_prompt",
    "status",
    "requires_approval",
    "approval_state",
    "assigned_worker_id",
    "manager_session_id",
    "result_cleared_at",
    "input_state",
    "input_reason",
    "input_source",
    "input_requested_at",
    "input_updated_at",
    "input_event_cursor",
    "input_interaction_id",
    "input_tool_call_id",
    "dispatch_generation",
    "dispatch_attempt",
    "dispatch_requeue_count",
    "dispatch_started_at",
    "dispatch_deadline_at",
    "dispatch_phase",
    "dispatch_attempt_id",
    "dispatch_session_id",
    "dispatch_event_cursor",
    "dispatch_terminal_generation",
    "dispatch_interaction_id",
    "dispatch_rendered_at",
    "dispatch_submitted_at",
    "dispatch_accepted_at",
    "dispatch_error",
    "dispatch_excluded_worker_id",
    "interruption_recovery_phase",
    "interruption_recovery_session_id",
    "interruption_recovery_event_cursor",
    "interruption_recovery_terminal_generation",
    "interruption_recovery_generation",
    "interruption_recovery_interaction_id",
    "interruption_recovery_prompt",
    "interruption_recovery_started_at",
    "interruption_recovery_submitted_at",
    "interruption_recovery_accepted_at",
    "interruption_recovery_error",
    "log_path",
    "pid",
    "pgid",
    "created_at",
    "updated_at",
    "started_at",
    "finished_at",
)


SCHEMA = """
PRAGMA foreign_keys = ON;

CREATE TABLE IF NOT EXISTS app_state (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS agent_workers (
    id TEXT PRIMARY KEY,
    copilot_session_id TEXT NOT NULL UNIQUE,
    name TEXT NOT NULL,
    role TEXT NOT NULL DEFAULT 'worker',
    state TEXT NOT NULL DEFAULT 'idle',
    capabilities_json TEXT NOT NULL DEFAULT '[]',
    cwd TEXT,
    model TEXT NOT NULL,
    reasoning_effort TEXT NOT NULL,
    context_tier TEXT NOT NULL,
    current_task_id TEXT,
    activity_kind TEXT,
    activity_started_at TEXT,
    activity_updated_at TEXT,
    direct_event_cursor INTEGER,
    direct_interaction_id TEXT,
    direct_submitted_at TEXT,
    direct_prompt_hash TEXT,
    direct_context TEXT,
    direct_generation INTEGER NOT NULL DEFAULT 0,
    terminal_generation INTEGER NOT NULL DEFAULT 0,
    recovery_generation INTEGER NOT NULL DEFAULT 0,
    recovery_attempt INTEGER NOT NULL DEFAULT 0,
    recovery_started_at TEXT,
    recovery_error TEXT,
    input_state TEXT,
    input_reason TEXT,
    input_source TEXT,
    input_requested_at TEXT,
    input_updated_at TEXT,
    input_event_cursor INTEGER,
    input_interaction_id TEXT,
    input_tool_call_id TEXT,
    pid INTEGER,
    pgid INTEGER,
    turn_count INTEGER NOT NULL DEFAULT 0,
    last_error TEXT,
    retired_at TEXT,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    heartbeat_at TEXT
);

CREATE INDEX IF NOT EXISTS idx_agent_workers_state
ON agent_workers(state, role);

CREATE TABLE IF NOT EXISTS terminal_input_sequences (
    worker_id TEXT NOT NULL REFERENCES agent_workers(id) ON DELETE CASCADE,
    client_id TEXT NOT NULL,
    last_sequence INTEGER NOT NULL CHECK (last_sequence > 0),
    accepted_at TEXT NOT NULL,
    PRIMARY KEY (worker_id, client_id)
);

CREATE TABLE IF NOT EXISTS agent_tasks (
    id TEXT PRIMARY KEY,
    parent_task_id TEXT REFERENCES agent_tasks(id) ON DELETE SET NULL,
    title TEXT NOT NULL,
    task_type TEXT NOT NULL,
    requested_prompt TEXT NOT NULL,
    normalized_prompt TEXT,
    status TEXT NOT NULL,
    requires_approval INTEGER NOT NULL DEFAULT 0,
    approval_state TEXT NOT NULL DEFAULT 'not_required',
    assigned_worker_id TEXT REFERENCES agent_workers(id) ON DELETE SET NULL,
    manager_session_id TEXT,
    result_json TEXT NOT NULL DEFAULT '{}',
    result_cleared_at TEXT,
    input_state TEXT,
    input_reason TEXT,
    input_source TEXT,
    input_requested_at TEXT,
    input_updated_at TEXT,
    input_event_cursor INTEGER,
    input_interaction_id TEXT,
    input_tool_call_id TEXT,
    dispatch_generation INTEGER NOT NULL DEFAULT 0,
    dispatch_attempt INTEGER NOT NULL DEFAULT 0,
    dispatch_requeue_count INTEGER NOT NULL DEFAULT 0,
    dispatch_started_at TEXT,
    dispatch_deadline_at TEXT,
    dispatch_phase TEXT,
    dispatch_attempt_id TEXT,
    dispatch_session_id TEXT,
    dispatch_event_cursor INTEGER,
    dispatch_terminal_generation INTEGER,
    dispatch_interaction_id TEXT,
    dispatch_rendered_at TEXT,
    dispatch_submitted_at TEXT,
    dispatch_accepted_at TEXT,
    dispatch_error TEXT,
    dispatch_excluded_worker_id TEXT,
    interruption_recovery_phase TEXT,
    interruption_recovery_session_id TEXT,
    interruption_recovery_event_cursor INTEGER,
    interruption_recovery_terminal_generation INTEGER,
    interruption_recovery_generation INTEGER,
    interruption_recovery_interaction_id TEXT,
    interruption_recovery_prompt TEXT,
    interruption_recovery_started_at TEXT,
    interruption_recovery_submitted_at TEXT,
    interruption_recovery_accepted_at TEXT,
    interruption_recovery_error TEXT,
    log_path TEXT,
    pid INTEGER,
    pgid INTEGER,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    started_at TEXT,
    finished_at TEXT
);

CREATE INDEX IF NOT EXISTS idx_agent_tasks_status
ON agent_tasks(status, created_at);

CREATE TABLE IF NOT EXISTS agent_events (
    id TEXT PRIMARY KEY,
    task_id TEXT NOT NULL REFERENCES agent_tasks(id) ON DELETE CASCADE,
    event_type TEXT NOT NULL,
    payload_json TEXT NOT NULL DEFAULT '{}',
    created_at TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_agent_events_task
ON agent_events(task_id, created_at);

CREATE TABLE IF NOT EXISTS agent_messages (
    id TEXT PRIMARY KEY,
    role TEXT NOT NULL,
    content TEXT NOT NULL,
    task_id TEXT REFERENCES agent_tasks(id) ON DELETE SET NULL,
    created_at TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_agent_messages_created
ON agent_messages(created_at);

CREATE TABLE IF NOT EXISTS worker_handoffs (
    id TEXT PRIMARY KEY,
    worker_id TEXT REFERENCES agent_workers(id) ON DELETE SET NULL,
    task_id TEXT UNIQUE REFERENCES agent_tasks(id) ON DELETE SET NULL,
    kind TEXT NOT NULL,
    title TEXT NOT NULL,
    summary TEXT NOT NULL,
    status TEXT NOT NULL,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_worker_handoffs_created
ON worker_handoffs(created_at DESC);

CREATE TABLE IF NOT EXISTS hub_restarts (
    id TEXT PRIMARY KEY,
    generation INTEGER NOT NULL UNIQUE,
    source_server_generation TEXT NOT NULL,
    source_server_pid INTEGER NOT NULL,
    source_process_identity TEXT,
    status TEXT NOT NULL,
    snapshot_json TEXT NOT NULL DEFAULT '{}',
    helper_pid INTEGER,
    replacement_server_generation TEXT,
    replacement_server_pid INTEGER,
    result_json TEXT NOT NULL DEFAULT '{}',
    requested_at TEXT NOT NULL,
    handed_off_at TEXT,
    stopping_at TEXT,
    starting_at TEXT,
    healthy_at TEXT,
    finished_at TEXT,
    updated_at TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_hub_restarts_status
ON hub_restarts(status, generation DESC);

CREATE TABLE IF NOT EXISTS hub_restart_initiators (
    id TEXT PRIMARY KEY,
    restart_id TEXT NOT NULL REFERENCES hub_restarts(id) ON DELETE CASCADE,
    worker_id TEXT,
    copilot_session_id TEXT,
    terminal_generation INTEGER,
    activity_kind TEXT NOT NULL,
    task_id TEXT,
    direct_generation INTEGER,
    interaction_id TEXT,
    state TEXT NOT NULL,
    result_json TEXT NOT NULL DEFAULT '{}',
    created_at TEXT NOT NULL,
    terminal_at TEXT,
    updated_at TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_hub_restart_initiators_restart
ON hub_restart_initiators(restart_id, state);

INSERT OR IGNORE INTO app_state (key, value, updated_at)
VALUES (
  'agent_auto_manager_replies_started_at',
  strftime('%Y-%m-%dT%H:%M:%f+00:00', 'now'),
  strftime('%Y-%m-%dT%H:%M:%f+00:00', 'now')
);

DROP TRIGGER IF EXISTS trg_agent_task_manager_update;

CREATE TRIGGER trg_agent_task_manager_update
AFTER UPDATE OF status ON agent_tasks
WHEN NEW.task_type != 'manager'
  AND (
    NEW.assigned_worker_id IS NOT NULL
    OR NEW.dispatch_excluded_worker_id IS NOT NULL
  )
  AND NEW.status IN ('succeeded', 'failed', 'cancelled', 'interrupted')
  AND NEW.finished_at IS NOT NULL
  AND julianday(NEW.finished_at) >= julianday(
    (
      SELECT value FROM app_state
      WHERE key='agent_auto_manager_replies_started_at'
    )
  )
  AND json_extract(NEW.result_json, '$.sent_to_manager_task_id') IS NULL
BEGIN
  UPDATE agent_tasks
  SET result_json=json_set(
        CASE WHEN json_valid(result_json) THEN result_json ELSE '{}' END,
        '$.sent_to_manager_at',
        strftime('%Y-%m-%dT%H:%M:%f+00:00', 'now'),
        '$.sent_to_manager_task_id',
        lower(hex(randomblob(16))),
        '$.sent_to_manager_automatically',
        json('true')
      ),
      updated_at=strftime('%Y-%m-%dT%H:%M:%f+00:00', 'now')
  WHERE id=NEW.id;

  INSERT INTO agent_tasks (
    id, parent_task_id, title, task_type, requested_prompt,
    normalized_prompt, status, requires_approval, approval_state,
    manager_session_id, result_json, created_at, updated_at
  )
  SELECT
    json_extract(source.result_json, '$.sent_to_manager_task_id'),
    source.id,
    substr('Worker result · ' || source.title, 1, 160),
    'manager',
    json_object(
      'source_task_id', source.id,
      'title', source.title,
      'status', source.status
    ),
    'worker_result_followup',
    'queued',
    0,
    'not_required',
    COALESCE(
      source.manager_session_id,
      (SELECT value FROM app_state WHERE key='agent_manager_session_id')
    ),
    json_object(
      'response',
      CASE
        WHEN length(source.manager_content) <= 16000
          THEN source.manager_content
        ELSE
          substr(source.manager_content, 1, 10600)
          || char(10) || char(10)
          || '[... middle omitted by the Hub worker handoff ...]'
          || char(10) || char(10)
          || substr(source.manager_content, -5346)
      END,
      'created_task_ids',
      json('[]'),
      'delivery',
      'inline'
    ),
    strftime('%Y-%m-%dT%H:%M:%f+00:00', 'now'),
    strftime('%Y-%m-%dT%H:%M:%f+00:00', 'now')
  FROM (
    SELECT task.*,
           COALESCE(
             NULLIF(
               CAST(json_extract(task.result_json, '$.response') AS TEXT),
               ''
             ),
             NULLIF(
               CAST(json_extract(task.result_json, '$.error') AS TEXT),
               ''
             ),
             'Task ended with status ' || task.status
               || ' without result text.'
           ) AS manager_content
    FROM agent_tasks task
    WHERE task.id=NEW.id
  ) source;

  INSERT INTO agent_events (id, task_id, event_type, payload_json, created_at)
  VALUES (
    lower(hex(randomblob(16))),
    NEW.id,
    'result.auto_sent_to_manager',
    json_object(
      'manager_task_id',
      (SELECT json_extract(result_json, '$.sent_to_manager_task_id')
       FROM agent_tasks WHERE id=NEW.id)
    ),
    strftime('%Y-%m-%dT%H:%M:%f+00:00', 'now')
  );
END;
"""


class Repository:
    def __init__(self, db_path: Path):
        self.db_path = db_path

    @contextmanager
    def connect(self) -> Iterator[sqlite3.Connection]:
        connection = sqlite3.connect(
            self.db_path,
            timeout=30,
            check_same_thread=False,
        )
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA journal_mode=WAL")
        connection.execute("PRAGMA foreign_keys=ON")
        try:
            yield connection
            connection.commit()
        finally:
            connection.close()

    def initialize(self) -> None:
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        with self.connect() as connection:
            connection.executescript(SCHEMA)

    @staticmethod
    def _decode(row: sqlite3.Row | None) -> dict[str, Any] | None:
        if row is None:
            return None
        value = dict(row)
        for source, target in JSON_COLUMNS.items():
            if source not in value:
                continue
            raw = value.pop(source)
            try:
                value[target] = json.loads(raw or "{}")
            except json.JSONDecodeError:
                value[target] = {}
        if "requires_approval" in value:
            value["requires_approval"] = bool(value["requires_approval"])
        if "input_state" in value:
            value["needs_user_input"] = value["input_state"] == "needs_user_input"
        return value

    @staticmethod
    def _json(value: Any) -> str:
        return json.dumps(value, ensure_ascii=False)

    @staticmethod
    def _mark_restart_initiators_terminal(
        connection: sqlite3.Connection,
        *,
        worker_id: str,
        activity_kind: str,
        now: str,
        task_id: str | None = None,
        direct_generation: int | None = None,
        interaction_id: str | None = None,
    ) -> None:
        if activity_kind == "task":
            rows = connection.execute(
                """
                SELECT id, restart_id FROM hub_restart_initiators
                WHERE worker_id=? AND activity_kind='task' AND task_id=?
                  AND state='accepted'
                """,
                (worker_id, task_id),
            ).fetchall()
        else:
            rows = connection.execute(
                """
                SELECT id, restart_id FROM hub_restart_initiators
                WHERE worker_id=? AND activity_kind='direct'
                  AND direct_generation=? AND interaction_id=?
                  AND state='accepted'
                """,
                (worker_id, direct_generation, interaction_id),
            ).fetchall()
        for row in rows:
            connection.execute(
                """
                UPDATE hub_restart_initiators
                SET state='terminal', terminal_at=?, updated_at=?,
                    result_json=?
                WHERE id=? AND state='accepted'
                """,
                (
                    now,
                    now,
                    json.dumps(
                        {
                            "response": (
                                "Restart control was handed off after the "
                                "initiating interaction reached terminal state."
                            )
                        }
                    ),
                    row["id"],
                ),
            )
            pending = connection.execute(
                """
                SELECT 1 FROM hub_restart_initiators
                WHERE restart_id=? AND state='accepted' LIMIT 1
                """,
                (row["restart_id"],),
            ).fetchone()
            if pending is None:
                connection.execute(
                    """
                    UPDATE hub_restarts
                    SET status='handed_off',
                        handed_off_at=COALESCE(handed_off_at, ?),
                        updated_at=?
                    WHERE id=? AND status='requested'
                    """,
                    (now, now, row["restart_id"]),
                )

    def create_agent_worker(
        self,
        *,
        copilot_session_id: str,
        name: str,
        role: str,
        model: str,
        reasoning_effort: str,
        context_tier: str,
        cwd: str | None,
        capabilities: list[str] | None = None,
        state: str = "idle",
        turn_count: int = 0,
    ) -> dict[str, Any]:
        worker_id = uuid.uuid4().hex
        now = utc_now()
        with self.connect() as connection:
            connection.execute(
                """
                INSERT INTO agent_workers (
                  id, copilot_session_id, name, role, state, capabilities_json,
                  cwd, model, reasoning_effort, context_tier, turn_count,
                  created_at, updated_at, heartbeat_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    worker_id,
                    copilot_session_id,
                    name,
                    role,
                    state,
                    self._json(capabilities or []),
                    cwd,
                    model,
                    reasoning_effort,
                    context_tier,
                    turn_count,
                    now,
                    now,
                    now,
                ),
            )
        return self.get_agent_worker(worker_id)  # type: ignore[return-value]

    def get_agent_worker(self, worker_id: str) -> dict[str, Any] | None:
        with self.connect() as connection:
            return self._decode(
                connection.execute(
                    "SELECT * FROM agent_workers WHERE id=?",
                    (worker_id,),
                ).fetchone()
            )

    def get_agent_worker_by_session(
        self,
        copilot_session_id: str,
    ) -> dict[str, Any] | None:
        with self.connect() as connection:
            return self._decode(
                connection.execute(
                    "SELECT * FROM agent_workers WHERE copilot_session_id=?",
                    (copilot_session_id,),
                ).fetchone()
            )

    def list_agent_workers(
        self,
        *,
        role: str | None = None,
        include_retired: bool = False,
    ) -> list[dict[str, Any]]:
        conditions: list[str] = []
        parameters: list[Any] = []
        if not include_retired:
            conditions.append("retired_at IS NULL")
        if role:
            conditions.append("role=?")
            parameters.append(role)
        where = f" WHERE {' AND '.join(conditions)}" if conditions else ""
        with self.connect() as connection:
            return [
                self._decode(row)  # type: ignore[arg-type]
                for row in connection.execute(
                    f"SELECT * FROM agent_workers{where} ORDER BY role, created_at",
                    parameters,
                ).fetchall()
            ]

    def retire_agent_worker(
        self,
        worker_id: str,
        *,
        expected_pid: int | None,
        expected_pgid: int | None,
    ) -> dict[str, Any] | None:
        now = utc_now()
        with self.connect() as connection:
            cursor = connection.execute(
                """
                UPDATE agent_workers
                SET state='retired', retired_at=?, pid=NULL, pgid=NULL,
                    updated_at=?, heartbeat_at=?
                WHERE id=? AND role='worker' AND retired_at IS NULL
                  AND state IN ('idle', 'blocked')
                  AND current_task_id IS NULL AND activity_kind IS NULL
                  AND direct_submitted_at IS NULL AND input_state IS NULL
                  AND pid IS ? AND pgid IS ?
                """,
                (
                    now,
                    now,
                    now,
                    worker_id,
                    expected_pid,
                    expected_pgid,
                ),
            )
            if cursor.rowcount != 1:
                return None
        return self.get_agent_worker(worker_id)

    def update_agent_worker(self, worker_id: str, **fields: Any) -> None:
        if not fields:
            return
        updates: dict[str, Any] = {}
        for key, value in fields.items():
            column = "capabilities_json" if key == "capabilities" else key
            updates[column] = self._json(value) if column == "capabilities_json" else value
        updates["updated_at"] = utc_now()
        assignments = ", ".join(f"{key}=?" for key in updates)
        with self.connect() as connection:
            connection.execute(
                f"UPDATE agent_workers SET {assignments} WHERE id=?",
                (*updates.values(), worker_id),
            )

    def accept_terminal_input_sequence(
        self,
        worker_id: str,
        client_id: str,
        sequence: int,
    ) -> bool:
        if sequence <= 0:
            raise ValueError("Terminal input sequence must be positive")
        now = utc_now()
        with self.connect() as connection:
            cursor = connection.execute(
                """
                INSERT INTO terminal_input_sequences (
                  worker_id, client_id, last_sequence, accepted_at
                ) VALUES (?, ?, ?, ?)
                ON CONFLICT(worker_id, client_id) DO UPDATE SET
                  last_sequence=excluded.last_sequence,
                  accepted_at=excluded.accepted_at
                WHERE excluded.last_sequence >
                      terminal_input_sequences.last_sequence
                """,
                (worker_id, client_id, sequence, now),
            )
            return cursor.rowcount == 1

    def set_agent_worker_process(
        self,
        worker_id: str,
        *,
        copilot_session_id: str,
        terminal_generation: int,
        pid: int,
        pgid: int,
        expected_task_id: str | None = None,
        require_task_owner: bool = False,
    ) -> bool:
        now = utc_now()
        owner = " AND current_task_id IS ?" if require_task_owner else ""
        parameters: list[Any] = [
            pid,
            pgid,
            now,
            now,
            worker_id,
            copilot_session_id,
            terminal_generation,
        ]
        if require_task_owner:
            parameters.append(expected_task_id)
        with self.connect() as connection:
            cursor = connection.execute(
                f"""
                UPDATE agent_workers
                SET pid=?, pgid=?, updated_at=?, heartbeat_at=?
                WHERE id=? AND copilot_session_id=?
                  AND terminal_generation=? AND retired_at IS NULL{owner}
                """,
                parameters,
            )
            return cursor.rowcount == 1

    def clear_agent_worker_process(
        self,
        worker_id: str,
        *,
        pid: int,
        copilot_session_id: str | None = None,
        terminal_generation: int | None = None,
    ) -> bool:
        clauses = ["id=?", "pid=?"]
        parameters: list[Any] = [utc_now(), utc_now(), worker_id, pid]
        if copilot_session_id is not None:
            clauses.append("copilot_session_id=?")
            parameters.append(copilot_session_id)
        if terminal_generation is not None:
            clauses.append("terminal_generation=?")
            parameters.append(terminal_generation)
        with self.connect() as connection:
            cursor = connection.execute(
                f"""
                UPDATE agent_workers
                SET pid=NULL, pgid=NULL, updated_at=?, heartbeat_at=?
                WHERE {' AND '.join(clauses)}
                """,
                parameters,
            )
            return cursor.rowcount == 1

    def try_lease_agent_worker(self, worker_id: str, task_id: str) -> bool:
        now = utc_now()
        with self.connect() as connection:
            cursor = connection.execute(
                """
                UPDATE agent_workers
                SET state='leased', current_task_id=?, activity_kind='task',
                    activity_started_at=?, activity_updated_at=?,
                    recovery_attempt=0, recovery_started_at=NULL,
                    recovery_error=NULL, input_state=NULL, input_reason=NULL,
                    input_source=NULL, input_requested_at=NULL,
                    input_updated_at=NULL, input_event_cursor=NULL,
                    input_interaction_id=NULL, input_tool_call_id=NULL,
                    updated_at=?, heartbeat_at=?
                WHERE id=? AND state='idle' AND current_task_id IS NULL
                  AND direct_submitted_at IS NULL AND retired_at IS NULL
                """,
                (task_id, now, now, now, now, worker_id),
            )
            return cursor.rowcount == 1

    def begin_manager_task_dispatch(
        self,
        task_id: str,
        manager_id: str,
    ) -> dict[str, Any] | None:
        if not self.try_lease_agent_worker(manager_id, task_id):
            return None
        now = utc_now()
        with self.connect() as connection:
            cursor = connection.execute(
                """
                UPDATE agent_tasks
                SET status='dispatching',
                    dispatch_generation=dispatch_generation + 1,
                    dispatch_phase=CASE
                      WHEN dispatch_interaction_id IS NULL THEN 'preparing'
                      ELSE dispatch_phase
                    END,
                    dispatch_started_at=COALESCE(dispatch_started_at, ?),
                    dispatch_error=NULL, updated_at=?
                WHERE id=? AND task_type='manager' AND status='queued'
                """,
                (now, now, task_id),
            )
        if cursor.rowcount != 1:
            self.release_agent_worker_task(
                manager_id,
                task_id,
                increment_turn_count=False,
            )
            return None
        return self.get_agent_task(task_id)

    def manager_task_can_submit_prompt(self, manager_id: str, task_id: str) -> bool:
        with self.connect() as connection:
            return (
                connection.execute(
                    """
                    SELECT 1 FROM agent_workers w
                    JOIN agent_tasks t ON t.id=w.current_task_id
                    WHERE w.id=? AND w.role='manager' AND w.current_task_id=?
                      AND w.activity_kind='task'
                      AND t.status IN ('dispatching', 'running')
                    """,
                    (manager_id, task_id),
                ).fetchone()
                is not None
            )

    def begin_agent_task_dispatch(
        self,
        task_id: str,
        worker_id: str,
        *,
        deadline_at: str,
    ) -> dict[str, Any] | None:
        now = utc_now()
        with self.connect() as connection:
            cursor = connection.execute(
                """
                UPDATE agent_tasks
                SET status='dispatching', assigned_worker_id=?,
                    dispatch_generation=dispatch_generation + 1,
                    dispatch_attempt=dispatch_attempt + 1,
                    dispatch_started_at=COALESCE(dispatch_started_at, ?),
                    dispatch_deadline_at=COALESCE(dispatch_deadline_at, ?),
                    dispatch_phase='preparing', dispatch_attempt_id=NULL,
                    dispatch_session_id=NULL, dispatch_event_cursor=NULL,
                    dispatch_terminal_generation=NULL,
                    dispatch_interaction_id=NULL,
                    dispatch_rendered_at=NULL, dispatch_submitted_at=NULL,
                    dispatch_accepted_at=NULL, dispatch_error=NULL,
                    updated_at=?
                WHERE id=? AND status='queued' AND assigned_worker_id IS NULL
                """,
                (worker_id, now, deadline_at, now, task_id),
            )
            if cursor.rowcount != 1:
                return None
        return self.get_agent_task(task_id)

    def claim_agent_task_dispatch_attempt(
        self,
        task_id: str,
        worker_id: str,
        *,
        generation: int,
        attempt_id: str,
        session_id: str,
        event_cursor: int,
        terminal_generation: int,
        attempt_limit: int,
    ) -> bool:
        now = utc_now()
        with self.connect() as connection:
            cursor = connection.execute(
                """
                UPDATE agent_tasks
                SET dispatch_attempt_id=?, dispatch_session_id=?,
                    dispatch_event_cursor=?, dispatch_terminal_generation=?,
                    dispatch_phase='not_rendered', dispatch_error=NULL,
                    updated_at=?
                WHERE id=? AND assigned_worker_id=?
                  AND dispatch_generation=?
                  AND status='dispatching' AND dispatch_attempt <= ?
                  AND dispatch_interaction_id IS NULL
                """,
                (
                    attempt_id,
                    session_id,
                    event_cursor,
                    terminal_generation,
                    now,
                    task_id,
                    worker_id,
                    generation,
                    attempt_limit,
                ),
            )
            return cursor.rowcount == 1

    def update_agent_task_dispatch_phase(
        self,
        task_id: str,
        worker_id: str,
        *,
        generation: int,
        attempt_id: str,
        phase: str,
        error: str | None = None,
    ) -> bool:
        now = utc_now()
        timestamps = {
            "rendered": "dispatch_rendered_at",
            "submitted": "dispatch_submitted_at",
        }
        timestamp = timestamps.get(phase)
        assignment = f", {timestamp}=?" if timestamp else ""
        parameters: list[Any] = [phase, error]
        if timestamp:
            parameters.append(now)
        parameters.extend((now, task_id, worker_id, generation, attempt_id))
        with self.connect() as connection:
            cursor = connection.execute(
                f"""
                UPDATE agent_tasks
                SET dispatch_phase=?, dispatch_error=?{assignment}, updated_at=?
                WHERE id=? AND assigned_worker_id=? AND dispatch_generation=?
                  AND dispatch_attempt_id=? AND status='dispatching'
                """,
                parameters,
            )
            return cursor.rowcount == 1

    def retarget_agent_task_dispatch_attempt(
        self,
        task_id: str,
        worker_id: str,
        *,
        generation: int,
        attempt_id: str,
        session_id: str,
        event_cursor: int,
        terminal_generation: int,
    ) -> bool:
        with self.connect() as connection:
            cursor = connection.execute(
                """
                UPDATE agent_tasks
                SET dispatch_session_id=?, dispatch_event_cursor=?,
                    dispatch_terminal_generation=?, updated_at=?
                WHERE id=? AND assigned_worker_id=? AND dispatch_generation=?
                  AND dispatch_attempt_id=? AND dispatch_interaction_id IS NULL
                """,
                (
                    session_id,
                    event_cursor,
                    terminal_generation,
                    utc_now(),
                    task_id,
                    worker_id,
                    generation,
                    attempt_id,
                ),
            )
            return cursor.rowcount == 1

    def accept_agent_task_dispatch(
        self,
        task_id: str,
        worker_id: str,
        *,
        generation: int,
        attempt_id: str,
        session_id: str,
        event_cursor: int,
        terminal_generation: int,
        interaction_id: str,
    ) -> bool:
        now = utc_now()
        with self.connect() as connection:
            cursor = connection.execute(
                """
                UPDATE agent_tasks
                SET status='running', started_at=COALESCE(started_at, ?),
                    dispatch_phase='accepted', dispatch_attempt_id=?,
                    dispatch_session_id=?, dispatch_event_cursor=?,
                    dispatch_terminal_generation=?,
                    dispatch_interaction_id=?, dispatch_accepted_at=?,
                    updated_at=?
                WHERE id=? AND assigned_worker_id=? AND dispatch_generation=?
                  AND status IN ('dispatching', 'running')
                  AND (
                    dispatch_interaction_id IS NULL
                    OR dispatch_interaction_id=?
                  )
                """,
                (
                    now,
                    attempt_id,
                    session_id,
                    event_cursor,
                    terminal_generation,
                    interaction_id,
                    now,
                    now,
                    task_id,
                    worker_id,
                    generation,
                    interaction_id,
                ),
            )
            if cursor.rowcount:
                connection.execute(
                    """
                    UPDATE agent_workers
                    SET state='busy', activity_kind='task',
                        activity_updated_at=?, updated_at=?, heartbeat_at=?
                    WHERE id=? AND current_task_id=?
                    """,
                    (now, now, now, worker_id, task_id),
                )
            return cursor.rowcount == 1

    def finish_agent_task_assignment(
        self,
        task_id: str,
        worker_id: str,
        *,
        generation: int,
        status: str,
        result: dict[str, Any],
    ) -> bool:
        if status not in TERMINAL_TASK_STATUSES:
            raise ValueError("Task status is not terminal")
        now = utc_now()
        with self.connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            cursor = connection.execute(
                """
                UPDATE agent_tasks
                SET status=?, result_json=?, finished_at=?, updated_at=?,
                    input_state=NULL, input_reason=NULL, input_source=NULL,
                    input_requested_at=NULL, input_updated_at=?,
                    input_event_cursor=NULL, input_interaction_id=NULL,
                    input_tool_call_id=NULL
                WHERE id=? AND assigned_worker_id=? AND dispatch_generation=?
                  AND status IN ('dispatching', 'running', 'cancelling')
                """,
                (
                    status,
                    self._json(result),
                    now,
                    now,
                    now,
                    task_id,
                    worker_id,
                    generation,
                ),
            )
            if cursor.rowcount:
                self._mark_restart_initiators_terminal(
                    connection,
                    worker_id=worker_id,
                    activity_kind="task",
                    task_id=task_id,
                    now=now,
                )
            return cursor.rowcount == 1

    def register_direct_activity(
        self,
        worker_id: str,
        *,
        event_cursor: int,
        prompt_hash: str,
    ) -> dict[str, Any] | None:
        now = utc_now()
        with self.connect() as connection:
            cursor = connection.execute(
                """
                UPDATE agent_workers
                SET state='busy', activity_kind='direct',
                    activity_started_at=?, activity_updated_at=?,
                    direct_event_cursor=?, direct_interaction_id=NULL,
                    direct_submitted_at=?, direct_prompt_hash=?,
                    direct_context=NULL,
                    direct_generation=direct_generation + 1,
                    input_state=NULL, input_reason=NULL, input_source=NULL,
                    input_requested_at=NULL, input_updated_at=NULL,
                    input_event_cursor=NULL, input_interaction_id=NULL,
                    input_tool_call_id=NULL, updated_at=?, heartbeat_at=?
                WHERE id=? AND retired_at IS NULL
                  AND current_task_id IS NULL
                  AND direct_submitted_at IS NULL
                  AND state IN ('idle', 'busy')
                """,
                (
                    now,
                    now,
                    event_cursor,
                    now,
                    prompt_hash,
                    now,
                    now,
                    worker_id,
                ),
            )
            if cursor.rowcount != 1:
                return None
        return self.get_agent_worker(worker_id)

    def accept_direct_activity(
        self,
        worker_id: str,
        *,
        generation: int,
        interaction_id: str,
        context: str | None,
    ) -> bool:
        now = utc_now()
        with self.connect() as connection:
            cursor = connection.execute(
                """
                UPDATE agent_workers
                SET direct_interaction_id=?, direct_context=COALESCE(?, direct_context),
                    activity_updated_at=?, updated_at=?, heartbeat_at=?
                WHERE id=? AND direct_generation=?
                  AND direct_submitted_at IS NOT NULL
                  AND (
                    direct_interaction_id IS NULL
                    OR direct_interaction_id=?
                  )
                """,
                (
                    interaction_id,
                    context,
                    now,
                    now,
                    now,
                    worker_id,
                    generation,
                    interaction_id,
                ),
            )
            return cursor.rowcount == 1

    def cancel_pending_direct_activity(
        self,
        worker_id: str,
        *,
        generation: int | None = None,
    ) -> bool:
        clauses = [
            "id=?",
            "current_task_id IS NULL",
            "direct_submitted_at IS NOT NULL",
            "direct_interaction_id IS NULL",
        ]
        parameters: list[Any] = [utc_now(), utc_now(), worker_id]
        if generation is not None:
            clauses.append("direct_generation=?")
            parameters.append(generation)
        with self.connect() as connection:
            cursor = connection.execute(
                f"""
                UPDATE agent_workers
                SET state='idle', activity_kind=NULL,
                    activity_started_at=NULL, activity_updated_at=NULL,
                    direct_event_cursor=NULL, direct_submitted_at=NULL,
                    direct_prompt_hash=NULL, direct_context=NULL,
                    updated_at=?, heartbeat_at=?
                WHERE {' AND '.join(clauses)}
                """,
                parameters,
            )
            return cursor.rowcount == 1

    def touch_direct_activity(
        self,
        worker_id: str,
        *,
        generation: int,
        interaction_id: str | None = None,
        context: str | None = None,
    ) -> bool:
        clauses = ["id=?", "direct_generation=?", "direct_submitted_at IS NOT NULL"]
        parameters: list[Any] = [
            context,
            utc_now(),
            utc_now(),
            utc_now(),
            worker_id,
            generation,
        ]
        if interaction_id is not None:
            clauses.append("direct_interaction_id=?")
            parameters.append(interaction_id)
        with self.connect() as connection:
            cursor = connection.execute(
                f"""
                UPDATE agent_workers
                SET direct_context=COALESCE(?, direct_context),
                    activity_updated_at=?, updated_at=?, heartbeat_at=?
                WHERE {' AND '.join(clauses)}
                """,
                parameters,
            )
            return cursor.rowcount == 1

    def set_agent_task_input_request(
        self,
        task_id: str,
        worker_id: str,
        *,
        reason: str,
        source: str,
        requested_at: str | None,
        event_cursor: int,
        interaction_id: str | None,
        tool_call_id: str | None,
    ) -> bool:
        now = utc_now()
        values = (
            reason,
            source,
            requested_at or now,
            now,
            event_cursor,
            interaction_id,
            tool_call_id,
            now,
        )
        with self.connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            cursor = connection.execute(
                """
                UPDATE agent_tasks
                SET input_state='needs_user_input', input_reason=?,
                    input_source=?, input_requested_at=?,
                    input_updated_at=?, input_event_cursor=?,
                    input_interaction_id=?, input_tool_call_id=?, updated_at=?
                WHERE id=? AND assigned_worker_id=?
                  AND status IN ('dispatching', 'running')
                """,
                (*values, task_id, worker_id),
            )
            if cursor.rowcount:
                connection.execute(
                    """
                    UPDATE agent_workers
                    SET state='needs_user_input',
                        input_state='needs_user_input', input_reason=?,
                        input_source=?, input_requested_at=?,
                        input_updated_at=?, input_event_cursor=?,
                        input_interaction_id=?, input_tool_call_id=?,
                        updated_at=?, heartbeat_at=?
                    WHERE id=? AND current_task_id=?
                    """,
                    (*values, now, worker_id, task_id),
                )
            return cursor.rowcount == 1

    def clear_agent_task_input_request(self, task_id: str, worker_id: str) -> bool:
        now = utc_now()
        with self.connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            cursor = connection.execute(
                """
                UPDATE agent_tasks
                SET input_state=NULL, input_reason=NULL, input_source=NULL,
                    input_requested_at=NULL, input_updated_at=?,
                    input_event_cursor=NULL, input_interaction_id=NULL,
                    input_tool_call_id=NULL, updated_at=?
                WHERE id=? AND assigned_worker_id=?
                """,
                (now, now, task_id, worker_id),
            )
            if cursor.rowcount:
                connection.execute(
                    """
                    UPDATE agent_workers
                    SET state='busy', input_state=NULL, input_reason=NULL,
                        input_source=NULL, input_requested_at=NULL,
                        input_updated_at=?, input_event_cursor=NULL,
                        input_interaction_id=NULL, input_tool_call_id=NULL,
                        updated_at=?, heartbeat_at=?
                    WHERE id=? AND current_task_id=?
                    """,
                    (now, now, now, worker_id, task_id),
                )
            return cursor.rowcount == 1

    def set_direct_input_request(
        self,
        worker_id: str,
        *,
        generation: int,
        reason: str,
        source: str,
        requested_at: str | None,
        event_cursor: int,
        interaction_id: str | None,
        tool_call_id: str | None,
    ) -> bool:
        now = utc_now()
        with self.connect() as connection:
            cursor = connection.execute(
                """
                UPDATE agent_workers
                SET state='needs_user_input', input_state='needs_user_input',
                    input_reason=?, input_source=?, input_requested_at=?,
                    input_updated_at=?, input_event_cursor=?,
                    input_interaction_id=?, input_tool_call_id=?,
                    updated_at=?, heartbeat_at=?
                WHERE id=? AND direct_generation=?
                  AND direct_submitted_at IS NOT NULL
                """,
                (
                    reason,
                    source,
                    requested_at or now,
                    now,
                    event_cursor,
                    interaction_id,
                    tool_call_id,
                    now,
                    now,
                    worker_id,
                    generation,
                ),
            )
            return cursor.rowcount == 1

    def clear_direct_input_request(self, worker_id: str, *, generation: int) -> bool:
        now = utc_now()
        with self.connect() as connection:
            cursor = connection.execute(
                """
                UPDATE agent_workers
                SET state='busy', input_state=NULL, input_reason=NULL,
                    input_source=NULL, input_requested_at=NULL,
                    input_updated_at=?, input_event_cursor=NULL,
                    input_interaction_id=NULL, input_tool_call_id=NULL,
                    updated_at=?, heartbeat_at=?
                WHERE id=? AND direct_generation=?
                  AND direct_submitted_at IS NOT NULL
                """,
                (now, now, now, worker_id, generation),
            )
            return cursor.rowcount == 1

    def cancel_direct_activity(
        self,
        worker_id: str,
        *,
        generation: int,
        interaction_id: str | None = None,
    ) -> bool:
        return self._finish_direct(
            worker_id,
            generation=generation,
            interaction_id=interaction_id,
            message="Direct terminal activity was cancelled.",
        )

    def finish_direct_activity(
        self,
        worker_id: str,
        *,
        generation: int,
        interaction_id: str | None = None,
    ) -> bool:
        return self._finish_direct(
            worker_id,
            generation=generation,
            interaction_id=interaction_id,
            message="Direct terminal activity completed.",
        )

    def _finish_direct(
        self,
        worker_id: str,
        *,
        generation: int,
        interaction_id: str | None,
        message: str,
    ) -> bool:
        now = utc_now()
        clauses = [
            "id=?",
            "direct_generation=?",
            "direct_submitted_at IS NOT NULL",
        ]
        parameters: list[Any] = [now, now, worker_id, generation]
        if interaction_id is not None:
            clauses.append("direct_interaction_id=?")
            parameters.append(interaction_id)
        with self.connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            worker = connection.execute(
                """
                SELECT direct_interaction_id FROM agent_workers
                WHERE id=? AND direct_generation=?
                  AND direct_submitted_at IS NOT NULL
                """,
                (worker_id, generation),
            ).fetchone()
            cursor = connection.execute(
                f"""
                UPDATE agent_workers
                SET state='idle', activity_kind=NULL,
                    activity_started_at=NULL, activity_updated_at=NULL,
                    direct_event_cursor=NULL, direct_interaction_id=NULL,
                    direct_submitted_at=NULL, direct_prompt_hash=NULL,
                    direct_context=NULL, input_state=NULL, input_reason=NULL,
                    input_source=NULL, input_requested_at=NULL,
                    input_updated_at=NULL, input_event_cursor=NULL,
                    input_interaction_id=NULL, input_tool_call_id=NULL,
                    updated_at=?, heartbeat_at=?
                WHERE {' AND '.join(clauses)}
                """,
                parameters,
            )
            if cursor.rowcount:
                self._mark_restart_initiators_terminal(
                    connection,
                    worker_id=worker_id,
                    activity_kind="direct",
                    direct_generation=generation,
                    interaction_id=(
                        interaction_id
                        or (worker["direct_interaction_id"] if worker else None)
                    ),
                    now=now,
                )
                connection.execute(
                    """
                    INSERT INTO agent_messages
                    (id, role, content, task_id, created_at)
                    VALUES (?, 'system', ?, NULL, ?)
                    """,
                    (uuid.uuid4().hex, message, now),
                )
            return cursor.rowcount == 1

    def mark_direct_activity_recovering(
        self,
        worker_id: str,
        *,
        generation: int,
        error: str,
    ) -> bool:
        now = utc_now()
        with self.connect() as connection:
            cursor = connection.execute(
                """
                UPDATE agent_workers
                SET state='recovering', activity_kind='recovery',
                    recovery_generation=recovery_generation + 1,
                    recovery_attempt=0, recovery_started_at=?,
                    recovery_error=?, updated_at=?, heartbeat_at=?
                WHERE id=? AND direct_generation=?
                  AND direct_submitted_at IS NOT NULL
                """,
                (now, error, now, now, worker_id, generation),
            )
            return cursor.rowcount == 1

    def release_agent_worker_task(
        self,
        worker_id: str,
        task_id: str,
        *,
        increment_turn_count: bool = True,
        clear_error: bool = True,
        recovering_reason: str | None = None,
        recovering_event_payload: dict[str, Any] | None = None,
    ) -> dict[str, Any] | None:
        now = utc_now()
        state = "recovering" if recovering_reason else "idle"
        activity_kind = "recovery" if recovering_reason else None
        with self.connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            cursor = connection.execute(
                """
                UPDATE agent_workers
                SET state=?, current_task_id=NULL, activity_kind=?,
                    activity_started_at=CASE WHEN ? IS NULL THEN NULL ELSE ? END,
                    activity_updated_at=CASE WHEN ? IS NULL THEN NULL ELSE ? END,
                    recovery_generation=recovery_generation + CASE
                      WHEN ? IS NULL THEN 0 ELSE 1 END,
                    recovery_attempt=0,
                    recovery_started_at=CASE WHEN ? IS NULL THEN NULL ELSE ? END,
                    recovery_error=?,
                    input_state=NULL, input_reason=NULL, input_source=NULL,
                    input_requested_at=NULL, input_updated_at=NULL,
                    input_event_cursor=NULL, input_interaction_id=NULL,
                    input_tool_call_id=NULL,
                    turn_count=turn_count + ?, last_error=?,
                    updated_at=?, heartbeat_at=?
                WHERE id=? AND current_task_id=?
                """,
                (
                    state,
                    activity_kind,
                    recovering_reason,
                    now,
                    recovering_reason,
                    now,
                    recovering_reason,
                    recovering_reason,
                    now,
                    recovering_reason,
                    int(increment_turn_count),
                    None if clear_error else recovering_reason,
                    now,
                    now,
                    worker_id,
                    task_id,
                ),
            )
            if cursor.rowcount != 1:
                return None
            if recovering_event_payload:
                connection.execute(
                    """
                    INSERT INTO agent_events
                    (id, task_id, event_type, payload_json, created_at)
                    VALUES (?, ?, 'terminal.recovering', ?, ?)
                    """,
                    (
                        uuid.uuid4().hex,
                        task_id,
                        self._json(recovering_event_payload),
                        now,
                    ),
                )
        return self.get_agent_worker(worker_id)

    def cancel_agent_task_assignment(self, task_id: str) -> dict[str, Any] | None:
        now = utc_now()
        with self.connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            task = connection.execute(
                """
                SELECT * FROM agent_tasks
                WHERE id=? AND status IN ('dispatching', 'running', 'cancelling')
                """,
                (task_id,),
            ).fetchone()
            if task is None:
                return None
            worker_id = task["assigned_worker_id"]
            worker = connection.execute(
                "SELECT * FROM agent_workers WHERE id=?",
                (worker_id,),
            ).fetchone()
            connection.execute(
                """
                UPDATE agent_tasks
                SET status='cancelled', result_json=?,
                    finished_at=?, updated_at=?
                WHERE id=?
                """,
                (self._json({"error": "Task was cancelled."}), now, now, task_id),
            )
            recovery_generation = None
            if worker is not None:
                recovery_generation = int(worker["recovery_generation"]) + 1
                connection.execute(
                    """
                    UPDATE agent_workers
                    SET state='recovering', current_task_id=NULL,
                        activity_kind='recovery',
                        recovery_generation=?, recovery_started_at=?,
                        recovery_error='Recovering after task cancellation',
                        updated_at=?, heartbeat_at=?
                    WHERE id=? AND current_task_id=?
                    """,
                    (
                        recovery_generation,
                        now,
                        now,
                        now,
                        worker_id,
                        task_id,
                    ),
                )
                self._mark_restart_initiators_terminal(
                    connection,
                    worker_id=worker_id,
                    activity_kind="task",
                    task_id=task_id,
                    now=now,
                )
            return {
                "worker_id": worker_id,
                "pid": worker["pid"] if worker else None,
                "pgid": worker["pgid"] if worker else None,
                "recovery_generation": recovery_generation,
            }

    def fail_agent_task_dispatch_assignment(
        self,
        task_id: str,
        worker_id: str,
        *,
        generation: int,
        error: str,
        requeue_limit: int,
    ) -> dict[str, Any] | None:
        now = utc_now()
        with self.connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            task = connection.execute(
                """
                SELECT * FROM agent_tasks
                WHERE id=? AND assigned_worker_id=? AND dispatch_generation=?
                  AND status IN ('dispatching', 'running')
                """,
                (task_id, worker_id, generation),
            ).fetchone()
            if task is None:
                return None
            worker = connection.execute(
                "SELECT pid, pgid FROM agent_workers WHERE id=?",
                (worker_id,),
            ).fetchone()
            requeue = int(task["dispatch_requeue_count"]) < requeue_limit
            if requeue:
                connection.execute(
                    """
                    UPDATE agent_tasks
                    SET status='queued', assigned_worker_id=NULL,
                        dispatch_requeue_count=dispatch_requeue_count + 1,
                        dispatch_excluded_worker_id=?, dispatch_phase='requeued',
                        dispatch_error=?, updated_at=?
                    WHERE id=?
                    """,
                    (worker_id, error, now, task_id),
                )
            else:
                connection.execute(
                    """
                    UPDATE agent_tasks
                    SET status='failed', result_json=?, dispatch_phase='failed',
                        dispatch_error=?, finished_at=?, updated_at=?
                    WHERE id=?
                    """,
                    (
                        self._json({"error": error}),
                        error,
                        now,
                        now,
                        task_id,
                    ),
                )
            connection.execute(
                """
                UPDATE agent_workers
                SET state='blocked', current_task_id=NULL, activity_kind=NULL,
                    last_error=?, updated_at=?, heartbeat_at=?
                WHERE id=? AND current_task_id=?
                """,
                (error, now, now, worker_id, task_id),
            )
            return {
                "outcome": "requeued" if requeue else "failed",
                "pid": worker["pid"] if worker else None,
                "pgid": worker["pgid"] if worker else None,
            }

    def fail_queued_agent_task_dispatch(
        self,
        task_id: str,
        *,
        generation: int,
        error: str,
    ) -> bool:
        now = utc_now()
        with self.connect() as connection:
            cursor = connection.execute(
                """
                UPDATE agent_tasks
                SET status='failed', result_json=?, dispatch_phase='failed',
                    dispatch_error=?, finished_at=?, updated_at=?
                WHERE id=? AND status='queued' AND dispatch_generation=?
                """,
                (
                    self._json({"error": error}),
                    error,
                    now,
                    now,
                    task_id,
                    generation,
                ),
            )
            return cursor.rowcount == 1

    def mark_agent_worker_task_recovering(
        self,
        worker_id: str,
        task_id: str,
        *,
        error: str,
    ) -> bool:
        return self._mark_worker_recovery(worker_id, task_id, error, "recovering")

    def mark_agent_worker_task_rebinding(
        self,
        worker_id: str,
        task_id: str,
        *,
        error: str,
    ) -> bool:
        return self._mark_worker_recovery(worker_id, task_id, error, "rebinding")

    def _mark_worker_recovery(
        self,
        worker_id: str,
        task_id: str,
        error: str,
        phase: str,
    ) -> bool:
        now = utc_now()
        with self.connect() as connection:
            cursor = connection.execute(
                """
                UPDATE agent_workers
                SET state='recovering', activity_kind='recovery',
                    recovery_generation=recovery_generation + 1,
                    recovery_started_at=?, recovery_error=?,
                    activity_updated_at=?, updated_at=?, heartbeat_at=?
                WHERE id=? AND current_task_id=?
                """,
                (now, f"{phase}: {error}", now, now, now, worker_id, task_id),
            )
            return cursor.rowcount == 1

    def mark_agent_worker_task_busy(self, worker_id: str, task_id: str) -> bool:
        now = utc_now()
        with self.connect() as connection:
            cursor = connection.execute(
                """
                UPDATE agent_workers
                SET state='busy', activity_kind='task',
                    recovery_attempt=0, recovery_started_at=NULL,
                    recovery_error=NULL, updated_at=?, heartbeat_at=?
                WHERE id=? AND current_task_id=?
                """,
                (now, now, worker_id, task_id),
            )
            return cursor.rowcount == 1

    def update_agent_worker_recovery(
        self,
        worker_id: str,
        *,
        generation: int,
        attempt: int,
        error: str,
    ) -> bool:
        with self.connect() as connection:
            cursor = connection.execute(
                """
                UPDATE agent_workers
                SET recovery_attempt=?, recovery_error=?,
                    updated_at=?, heartbeat_at=?
                WHERE id=? AND recovery_generation=? AND state='recovering'
                """,
                (attempt, error, utc_now(), utc_now(), worker_id, generation),
            )
            return cursor.rowcount == 1

    def update_agent_worker_task_rebind(
        self,
        worker_id: str,
        task_id: str,
        *,
        generation: int,
        attempt: int,
        error: str,
    ) -> bool:
        return self.update_agent_worker_recovery(
            worker_id,
            generation=generation,
            attempt=attempt,
            error=error,
        )

    def recover_blocked_agent_worker(
        self,
        worker_id: str,
        *,
        expected_error: str,
    ) -> bool:
        with self.connect() as connection:
            cursor = connection.execute(
                """
                UPDATE agent_workers
                SET state='idle', last_error=NULL,
                    updated_at=?, heartbeat_at=?
                WHERE id=? AND state='blocked' AND last_error=?
                  AND current_task_id IS NULL
                """,
                (utc_now(), utc_now(), worker_id, expected_error),
            )
            return cursor.rowcount == 1

    def finish_agent_worker_recovery(
        self,
        worker_id: str,
        *,
        generation: int,
    ) -> bool:
        now = utc_now()
        with self.connect() as connection:
            cursor = connection.execute(
                """
                UPDATE agent_workers
                SET state=CASE
                      WHEN current_task_id IS NULL THEN 'idle' ELSE 'busy'
                    END,
                    activity_kind=CASE
                      WHEN current_task_id IS NULL THEN NULL ELSE 'task'
                    END,
                    activity_started_at=CASE
                      WHEN current_task_id IS NULL THEN NULL
                      ELSE activity_started_at END,
                    activity_updated_at=CASE
                      WHEN current_task_id IS NULL THEN NULL ELSE ? END,
                    recovery_attempt=0, recovery_started_at=NULL,
                    recovery_error=NULL, last_error=NULL,
                    updated_at=?, heartbeat_at=?
                WHERE id=? AND recovery_generation=?
                  AND state='recovering'
                """,
                (now, now, now, worker_id, generation),
            )
            return cursor.rowcount == 1

    def finish_agent_worker_recovery_interaction(
        self,
        worker_id: str,
        *,
        recovery_generation: int,
        copilot_session_id: str,
        terminal_generation: int,
        direct_generation: int,
    ) -> bool:
        with self.connect() as connection:
            cursor = connection.execute(
                """
                UPDATE agent_workers
                SET state='idle', activity_kind=NULL,
                    activity_started_at=NULL, activity_updated_at=NULL,
                    recovery_attempt=0, recovery_started_at=NULL,
                    recovery_error=NULL, direct_generation=?,
                    updated_at=?, heartbeat_at=?
                WHERE id=? AND recovery_generation=?
                  AND copilot_session_id=? AND terminal_generation=?
                """,
                (
                    direct_generation,
                    utc_now(),
                    utc_now(),
                    worker_id,
                    recovery_generation,
                    copilot_session_id,
                    terminal_generation,
                ),
            )
            return cursor.rowcount == 1

    def fail_agent_worker_recovery(
        self,
        worker_id: str,
        *,
        generation: int,
        error: str,
    ) -> bool:
        with self.connect() as connection:
            cursor = connection.execute(
                """
                UPDATE agent_workers
                SET state='blocked', activity_kind=NULL,
                    recovery_error=?, last_error=?,
                    updated_at=?, heartbeat_at=?
                WHERE id=? AND recovery_generation=?
                """,
                (error, error, utc_now(), utc_now(), worker_id, generation),
            )
            return cursor.rowcount == 1

    def rebind_agent_worker_session(
        self,
        worker_id: str,
        *,
        expected_terminal_generation: int,
        expected_task_id: str | None,
        copilot_session_id: str,
        recovery_attempt: int,
        recovery_error: str,
        expected_task_generation: int | None = None,
        expected_recovery_generation: int | None = None,
    ) -> dict[str, Any] | None:
        clauses = ["id=?", "terminal_generation=?"]
        parameters: list[Any] = [
            copilot_session_id,
            recovery_attempt,
            recovery_error,
            utc_now(),
            utc_now(),
            worker_id,
            expected_terminal_generation,
        ]
        if expected_task_id is None:
            clauses.append("current_task_id IS NULL")
        else:
            clauses.append("current_task_id=?")
            parameters.append(expected_task_id)
        if expected_recovery_generation is not None:
            clauses.append("recovery_generation=?")
            parameters.append(expected_recovery_generation)
        with self.connect() as connection:
            cursor = connection.execute(
                f"""
                UPDATE agent_workers
                SET copilot_session_id=?,
                    terminal_generation=terminal_generation + 1,
                    turn_count=0, pid=NULL, pgid=NULL,
                    recovery_attempt=?, recovery_error=?,
                    updated_at=?, heartbeat_at=?
                WHERE {' AND '.join(clauses)}
                """,
                parameters,
            )
            if cursor.rowcount != 1:
                return None
            if expected_task_id and expected_task_generation is not None:
                task = connection.execute(
                    """
                    SELECT 1 FROM agent_tasks
                    WHERE id=? AND dispatch_generation=?
                    """,
                    (expected_task_id, expected_task_generation),
                ).fetchone()
                if task is None:
                    raise RuntimeError("Task generation changed during session rebind")
        return self.get_agent_worker(worker_id)

    def begin_agent_task_terminal_recovery(
        self,
        worker_id: str,
        task_id: str,
        *,
        dispatch_generation: int,
        expected_session_id: str,
        expected_terminal_generation: int,
        expected_recovery_generation: int,
        recovery_attempt: int,
        recovery_error: str,
    ) -> dict[str, Any] | None:
        worker = self.rebind_agent_worker_session(
            worker_id,
            expected_terminal_generation=expected_terminal_generation,
            expected_task_id=task_id,
            copilot_session_id=str(uuid.uuid4()),
            recovery_attempt=recovery_attempt,
            recovery_error=recovery_error,
            expected_task_generation=dispatch_generation,
            expected_recovery_generation=expected_recovery_generation,
        )
        if worker and worker.get("copilot_session_id") == expected_session_id:
            raise RuntimeError("Recovery did not rotate the session")
        return worker

    def claim_agent_task_interruption_recovery(
        self,
        task_id: str,
        worker_id: str,
        *,
        dispatch_generation: int,
        source_session_id: str,
        source_interaction_id: str,
        session_id: str,
        event_cursor: int,
        terminal_generation: int,
        recovery_generation: int,
        prompt: str,
        interruption: dict[str, Any],
    ) -> bool:
        now = utc_now()
        with self.connect() as connection:
            cursor = connection.execute(
                """
                UPDATE agent_tasks
                SET interruption_recovery_phase='claimed',
                    interruption_recovery_session_id=?,
                    interruption_recovery_event_cursor=?,
                    interruption_recovery_terminal_generation=?,
                    interruption_recovery_generation=?,
                    interruption_recovery_interaction_id=NULL,
                    interruption_recovery_prompt=?,
                    interruption_recovery_started_at=?,
                    interruption_recovery_submitted_at=NULL,
                    interruption_recovery_accepted_at=NULL,
                    interruption_recovery_error=?,
                    updated_at=?
                WHERE id=? AND assigned_worker_id=?
                  AND dispatch_generation=?
                  AND dispatch_session_id=? AND dispatch_interaction_id=?
                  AND status IN ('dispatching', 'running')
                  AND interruption_recovery_phase IS NULL
                """,
                (
                    session_id,
                    event_cursor,
                    terminal_generation,
                    recovery_generation,
                    prompt,
                    now,
                    self._json(interruption),
                    now,
                    task_id,
                    worker_id,
                    dispatch_generation,
                    source_session_id,
                    source_interaction_id,
                ),
            )
            return cursor.rowcount == 1

    def mark_agent_task_interruption_recovery_submitted(
        self,
        task_id: str,
        worker_id: str,
        *,
        dispatch_generation: int,
        session_id: str,
        event_cursor: int,
        terminal_generation: int,
        recovery_generation: int,
    ) -> bool:
        return self._update_interruption_recovery(
            task_id,
            worker_id,
            dispatch_generation=dispatch_generation,
            session_id=session_id,
            event_cursor=event_cursor,
            terminal_generation=terminal_generation,
            recovery_generation=recovery_generation,
            phase="submitted",
        )

    def accept_agent_task_interruption_recovery(
        self,
        task_id: str,
        worker_id: str,
        *,
        dispatch_generation: int,
        session_id: str,
        event_cursor: int,
        terminal_generation: int,
        recovery_generation: int,
        interaction_id: str,
    ) -> bool:
        return self._update_interruption_recovery(
            task_id,
            worker_id,
            dispatch_generation=dispatch_generation,
            session_id=session_id,
            event_cursor=event_cursor,
            terminal_generation=terminal_generation,
            recovery_generation=recovery_generation,
            phase="accepted",
            interaction_id=interaction_id,
        )

    def _update_interruption_recovery(
        self,
        task_id: str,
        worker_id: str,
        *,
        dispatch_generation: int,
        session_id: str,
        event_cursor: int,
        terminal_generation: int,
        recovery_generation: int,
        phase: str,
        interaction_id: str | None = None,
    ) -> bool:
        now = utc_now()
        column = (
            "interruption_recovery_accepted_at"
            if phase == "accepted"
            else "interruption_recovery_submitted_at"
        )
        with self.connect() as connection:
            cursor = connection.execute(
                f"""
                UPDATE agent_tasks
                SET interruption_recovery_phase=?,
                    interruption_recovery_interaction_id=
                      COALESCE(?, interruption_recovery_interaction_id),
                    {column}=?, updated_at=?
                WHERE id=? AND assigned_worker_id=? AND dispatch_generation=?
                  AND interruption_recovery_session_id=?
                  AND interruption_recovery_event_cursor=?
                  AND interruption_recovery_terminal_generation=?
                  AND interruption_recovery_generation=?
                """,
                (
                    phase,
                    interaction_id,
                    now,
                    now,
                    task_id,
                    worker_id,
                    dispatch_generation,
                    session_id,
                    event_cursor,
                    terminal_generation,
                    recovery_generation,
                ),
            )
            return cursor.rowcount == 1

    def finish_agent_task_interruption_recovery(
        self,
        task_id: str,
        worker_id: str,
        *,
        dispatch_generation: int,
        phase: str,
        error: str | None = None,
    ) -> bool:
        with self.connect() as connection:
            cursor = connection.execute(
                """
                UPDATE agent_tasks
                SET interruption_recovery_phase=?,
                    interruption_recovery_error=?, updated_at=?
                WHERE id=? AND assigned_worker_id=? AND dispatch_generation=?
                """,
                (
                    phase,
                    error,
                    utc_now(),
                    task_id,
                    worker_id,
                    dispatch_generation,
                ),
            )
            return cursor.rowcount == 1

    def finish_agent_task_terminal_recovery(
        self,
        worker_id: str,
        task_id: str,
        *,
        dispatch_generation: int,
        terminal_generation: int,
        recovery_generation: int,
    ) -> bool:
        now = utc_now()
        with self.connect() as connection:
            cursor = connection.execute(
                """
                UPDATE agent_workers
                SET state='busy', activity_kind='task',
                    recovery_attempt=0, recovery_started_at=NULL,
                    recovery_error=NULL, updated_at=?, heartbeat_at=?
                WHERE id=? AND current_task_id=?
                  AND terminal_generation=? AND recovery_generation=?
                  AND EXISTS (
                    SELECT 1 FROM agent_tasks
                    WHERE id=? AND dispatch_generation=?
                  )
                """,
                (
                    now,
                    now,
                    worker_id,
                    task_id,
                    terminal_generation,
                    recovery_generation,
                    task_id,
                    dispatch_generation,
                ),
            )
            return cursor.rowcount == 1

    def create_agent_task(
        self,
        *,
        title: str,
        task_type: str,
        requested_prompt: str,
        status: str,
        requires_approval: bool = False,
        approval_state: str = "not_required",
        parent_task_id: str | None = None,
        normalized_prompt: str | None = None,
        manager_session_id: str | None = None,
        log_path: str | None = None,
        blocked_manager_task_marker: str | None = None,
    ) -> dict[str, Any]:
        task_id = uuid.uuid4().hex
        now = utc_now()
        with self.connect() as connection:
            if blocked_manager_task_marker:
                connection.execute("BEGIN IMMEDIATE")
                active = connection.execute(
                    """
                    SELECT 1 FROM agent_workers w
                    JOIN agent_tasks t ON t.id=w.current_task_id
                    WHERE w.role='manager' AND t.normalized_prompt=?
                      AND t.status NOT IN (
                        'succeeded', 'failed', 'cancelled', 'interrupted'
                      )
                    LIMIT 1
                    """,
                    (blocked_manager_task_marker,),
                ).fetchone()
                if active is not None:
                    raise AgentTaskCreationBlockedError(
                        "Worker dispatch is disabled during an automatic result handoff"
                    )
            connection.execute(
                """
                INSERT INTO agent_tasks (
                  id, parent_task_id, title, task_type, requested_prompt,
                  normalized_prompt, status, requires_approval,
                  approval_state, manager_session_id, log_path,
                  created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    task_id,
                    parent_task_id,
                    title,
                    task_type,
                    requested_prompt,
                    normalized_prompt,
                    status,
                    int(requires_approval),
                    approval_state,
                    manager_session_id,
                    log_path,
                    now,
                    now,
                ),
            )
        return self.get_agent_task(task_id)  # type: ignore[return-value]

    def create_agent_result_manager_task(
        self,
        source_task_id: str,
        *,
        title: str,
        requested_prompt: str,
        manager_response: str,
        manager_session_id: str,
        automatic: bool,
        mark_viewed: bool,
    ) -> tuple[dict[str, Any] | None, bool]:
        now = utc_now()
        created = False
        with self.connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            source = connection.execute(
                "SELECT * FROM agent_tasks WHERE id=?",
                (source_task_id,),
            ).fetchone()
            if (
                source is None
                or source["task_type"] == "manager"
                or source["status"] not in TERMINAL_TASK_STATUSES
            ):
                return None, False
            result = json.loads(source["result_json"] or "{}")
            existing_id = result.get("sent_to_manager_task_id")
            existing = (
                connection.execute(
                    "SELECT * FROM agent_tasks WHERE id=?",
                    (existing_id,),
                ).fetchone()
                if existing_id
                else None
            )
            if existing is None:
                manager_task_id = uuid.uuid4().hex
                connection.execute(
                    """
                    INSERT INTO agent_tasks (
                      id, parent_task_id, title, task_type,
                      requested_prompt, normalized_prompt, status,
                      requires_approval, approval_state, manager_session_id,
                      result_json, created_at, updated_at
                    ) VALUES (?, ?, ?, 'manager', ?, 'worker_result_followup',
                              'queued', 0, 'not_required', ?, ?, ?, ?)
                    """,
                    (
                        manager_task_id,
                        source_task_id,
                        title,
                        requested_prompt,
                        manager_session_id,
                        self._json(
                            {
                                "response": manager_response,
                                "created_task_ids": [],
                                "delivery": "inline",
                            }
                        ),
                        now,
                        now,
                    ),
                )
                result.update(
                    {
                        "sent_to_manager_at": result.get("sent_to_manager_at") or now,
                        "sent_to_manager_task_id": manager_task_id,
                        "sent_to_manager_automatically": automatic,
                    }
                )
                created = True
            else:
                manager_task_id = str(existing["id"])
                if existing["status"] in {"failed", "cancelled", "interrupted"}:
                    connection.execute(
                        """
                        UPDATE agent_tasks
                        SET status='queued', result_json=?,
                            dispatch_phase='waiting_for_manager',
                            dispatch_session_id=NULL,
                            dispatch_event_cursor=NULL,
                            dispatch_terminal_generation=NULL,
                            dispatch_interaction_id=NULL,
                            dispatch_started_at=NULL, started_at=NULL,
                            finished_at=NULL, updated_at=?
                        WHERE id=?
                        """,
                        (
                            self._json(
                                {
                                    "response": manager_response,
                                    "created_task_ids": [],
                                    "delivery": "inline",
                                }
                            ),
                            now,
                            manager_task_id,
                        ),
                    )
                    created = True
            if mark_viewed and not result.get("viewed_at"):
                result["viewed_at"] = now
            connection.execute(
                "UPDATE agent_tasks SET result_json=?, updated_at=? WHERE id=?",
                (self._json(result), now, source_task_id),
            )
        return self.get_agent_task(manager_task_id), created

    def get_agent_task(self, task_id: str) -> dict[str, Any] | None:
        with self.connect() as connection:
            return self._decode(
                connection.execute(
                    """
                    SELECT t.*, w.name AS worker_name,
                           w.copilot_session_id AS worker_session_id
                    FROM agent_tasks t
                    LEFT JOIN agent_workers w ON w.id=t.assigned_worker_id
                    WHERE t.id=?
                    """,
                    (task_id,),
                ).fetchone()
            )

    def list_agent_tasks(
        self,
        *,
        statuses: tuple[str, ...] | None = None,
        limit: int = 100,
        include_result: bool = True,
        exclude_terminal_manager_updates: bool = False,
    ) -> list[dict[str, Any]]:
        columns = (
            "t.*"
            if include_result
            else ", ".join(f"t.{name}" for name in AGENT_TASK_COLUMNS_WITHOUT_RESULT)
        )
        conditions: list[str] = []
        parameters: list[Any] = []
        if statuses:
            conditions.append(
                "t.status IN (" + ",".join("?" for _ in statuses) + ")"
            )
            parameters.extend(statuses)
        if exclude_terminal_manager_updates:
            conditions.append(
                """NOT (
                  t.task_type='manager'
                  AND COALESCE(t.normalized_prompt, '')='worker_result_followup'
                )"""
            )
        where = f" WHERE {' AND '.join(conditions)}" if conditions else ""
        parameters.append(limit)
        with self.connect() as connection:
            return [
                self._decode(row)  # type: ignore[arg-type]
                for row in connection.execute(
                    f"""
                    SELECT {columns}, w.name AS worker_name,
                           w.copilot_session_id AS worker_session_id
                    FROM agent_tasks t
                    LEFT JOIN agent_workers w ON w.id=t.assigned_worker_id
                    {where}
                    ORDER BY t.created_at DESC LIMIT ?
                    """,
                    parameters,
                ).fetchall()
            ]

    def list_pending_agent_manager_update_tasks(
        self,
        *,
        started_at: str,
        limit: int = 100,
    ) -> list[dict[str, Any]]:
        with self.connect() as connection:
            return [
                self._decode(row)  # type: ignore[arg-type]
                for row in connection.execute(
                    """
                    SELECT t.*, w.name AS worker_name,
                           w.copilot_session_id AS worker_session_id
                    FROM agent_tasks t
                    LEFT JOIN agent_workers w ON w.id=t.assigned_worker_id
                    WHERE t.task_type != 'manager'
                      AND (
                        t.assigned_worker_id IS NOT NULL
                        OR t.dispatch_excluded_worker_id IS NOT NULL
                      )
                      AND t.status IN (
                        'succeeded', 'failed', 'cancelled', 'interrupted'
                      )
                      AND t.finished_at >= ?
                      AND json_extract(
                        t.result_json,
                        '$.sent_to_manager_task_id'
                      ) IS NULL
                    ORDER BY t.finished_at, t.id LIMIT ?
                    """,
                    (started_at, limit),
                ).fetchall()
            ]

    @staticmethod
    def _result_conditions(
        *,
        include_cleared: bool,
        started_at: str | None,
    ) -> tuple[list[str], list[Any]]:
        conditions = [
            "t.task_type != 'manager'",
            "t.status IN ('succeeded', 'failed', 'cancelled', 'interrupted')",
            """(
              COALESCE(json_extract(t.result_json, '$.response'), '') != ''
              OR COALESCE(json_extract(t.result_json, '$.error'), '') != ''
            )""",
        ]
        parameters: list[Any] = []
        if not include_cleared:
            conditions.append("t.result_cleared_at IS NULL")
        if started_at:
            conditions.append("t.finished_at >= ?")
            parameters.append(started_at)
        return conditions, parameters

    def list_agent_result_tasks(
        self,
        *,
        include_cleared: bool = False,
        started_at: str | None = None,
        limit: int = 100,
        offset: int = 0,
        before_task_id: str | None = None,
        _connection: sqlite3.Connection | None = None,
    ) -> list[dict[str, Any]]:
        if limit < 1 or offset < 0:
            raise ValueError("Invalid result page")
        conditions, parameters = self._result_conditions(
            include_cleared=include_cleared,
            started_at=started_at,
        )
        context = self.connect() if _connection is None else nullcontext(_connection)
        with context as connection:
            if before_task_id:
                cursor = connection.execute(
                    """
                    SELECT COALESCE(finished_at, updated_at, created_at) AS sort_time,
                           id
                    FROM agent_tasks WHERE id=?
                    """,
                    (before_task_id,),
                ).fetchone()
                if cursor is None:
                    raise KeyError(before_task_id)
                conditions.append(
                    """(
                      COALESCE(t.finished_at, t.updated_at, t.created_at) < ?
                      OR (
                        COALESCE(t.finished_at, t.updated_at, t.created_at) = ?
                        AND t.id < ?
                      )
                    )"""
                )
                parameters.extend(
                    (cursor["sort_time"], cursor["sort_time"], cursor["id"])
                )
            parameters.extend((limit, offset))
            return [
                self._decode(row)  # type: ignore[arg-type]
                for row in connection.execute(
                    f"""
                    SELECT t.*, w.name AS worker_name,
                           w.copilot_session_id AS worker_session_id
                    FROM agent_tasks t
                    LEFT JOIN agent_workers w ON w.id=t.assigned_worker_id
                    WHERE {' AND '.join(conditions)}
                    ORDER BY
                      COALESCE(t.finished_at, t.updated_at, t.created_at) DESC,
                      t.id DESC
                    LIMIT ? OFFSET ?
                    """,
                    parameters,
                ).fetchall()
            ]

    def agent_result_summary(
        self,
        *,
        started_at: str | None = None,
        _connection: sqlite3.Connection | None = None,
    ) -> dict[str, int]:
        conditions, parameters = self._result_conditions(
            include_cleared=False,
            started_at=started_at,
        )
        context = self.connect() if _connection is None else nullcontext(_connection)
        with context as connection:
            row = connection.execute(
                f"""
                SELECT
                  SUM(CASE WHEN json_extract(result_json, '$.viewed_at') IS NULL
                    THEN 1 ELSE 0 END) AS unread,
                  SUM(CASE WHEN json_extract(result_json, '$.viewed_at') IS NOT NULL
                    THEN 1 ELSE 0 END) AS read
                FROM agent_tasks t
                WHERE {' AND '.join(conditions)}
                """,
                parameters,
            ).fetchone()
        unread = int(row["unread"] or 0)
        read = int(row["read"] or 0)
        return {"unread": unread, "read": read, "total": unread + read}

    def agent_result_inbox_page(
        self,
        *,
        started_at: str | None = None,
        limit: int = 100,
        before_task_id: str | None = None,
    ) -> tuple[list[dict[str, Any]], dict[str, int]]:
        with self.connect() as connection:
            connection.execute("BEGIN")
            summary = self.agent_result_summary(
                started_at=started_at,
                _connection=connection,
            )
            results = self.list_agent_result_tasks(
                started_at=started_at,
                limit=limit,
                before_task_id=before_task_id,
                _connection=connection,
            )
        return results, summary

    def merge_agent_task_result_metadata(
        self,
        task_id: str,
        metadata: dict[str, Any],
        *,
        viewed_at: str | None = None,
    ) -> dict[str, Any]:
        now = utc_now()
        with self.connect() as connection:
            row = connection.execute(
                "SELECT result_json FROM agent_tasks WHERE id=?",
                (task_id,),
            ).fetchone()
            if row is None:
                raise KeyError(task_id)
            result = json.loads(row["result_json"] or "{}")
            if viewed_at and not result.get("viewed_at"):
                result["viewed_at"] = viewed_at
            result.update(metadata)
            connection.execute(
                "UPDATE agent_tasks SET result_json=?, updated_at=? WHERE id=?",
                (self._json(result), now, task_id),
            )
        return result

    def clear_agent_task_result(self, task_id: str) -> bool:
        now = utc_now()
        with self.connect() as connection:
            cursor = connection.execute(
                """
                UPDATE agent_tasks
                SET result_cleared_at=?, updated_at=?
                WHERE id=? AND result_cleared_at IS NULL
                """,
                (now, now, task_id),
            )
            if cursor.rowcount:
                connection.execute(
                    """
                    INSERT INTO agent_events
                    (id, task_id, event_type, payload_json, created_at)
                    VALUES (?, ?, 'result.cleared', ?, ?)
                    """,
                    (
                        uuid.uuid4().hex,
                        task_id,
                        self._json({"cleared_at": now}),
                        now,
                    ),
                )
            return cursor.rowcount == 1

    def sync_worker_handoffs(self, *, limit: int = 200) -> int:
        now = utc_now()
        created = 0
        with self.connect() as connection:
            rows = connection.execute(
                """
                SELECT t.* FROM agent_tasks t
                LEFT JOIN worker_handoffs u ON u.task_id=t.id
                WHERE u.id IS NULL AND t.assigned_worker_id IS NOT NULL
                  AND t.status IN (
                    'succeeded', 'failed', 'cancelled', 'interrupted'
                  )
                ORDER BY COALESCE(t.finished_at, t.updated_at) DESC LIMIT ?
                """,
                (limit,),
            ).fetchall()
            for row in rows:
                result = json.loads(row["result_json"] or "{}")
                summary = str(
                    result.get("response")
                    or result.get("error")
                    or f"Task ended with status {row['status']}."
                ).strip()
                if len(summary) > 1200:
                    summary = summary[:1197].rstrip() + "..."
                cursor = connection.execute(
                    """
                    INSERT OR IGNORE INTO worker_handoffs (
                      id, worker_id, task_id, kind, title, summary, status,
                      created_at, updated_at
                    ) VALUES (?, ?, ?, 'task_handoff', ?, ?, ?, ?, ?)
                    """,
                    (
                        uuid.uuid4().hex,
                        row["assigned_worker_id"],
                        row["id"],
                        row["title"],
                        summary,
                        row["status"],
                        row["finished_at"] or now,
                        now,
                    ),
                )
                created += cursor.rowcount
        return created

    def list_worker_handoffs(self, *, limit: int = 20) -> list[dict[str, Any]]:
        self.sync_worker_handoffs()
        with self.connect() as connection:
            return [
                self._decode(row)  # type: ignore[arg-type]
                for row in connection.execute(
                    """
                    SELECT u.*, w.name AS worker_name, w.role AS worker_role
                    FROM worker_handoffs u
                    LEFT JOIN agent_workers w ON w.id=u.worker_id
                    ORDER BY u.created_at DESC LIMIT ?
                    """,
                    (limit,),
                ).fetchall()
            ]

    def list_agent_usage_metrics(self, *, limit: int = 100) -> list[dict[str, Any]]:
        with self.connect() as connection:
            return [
                dict(row)
                for row in connection.execute(
                    """
                    SELECT
                      w.id, w.name, w.role, w.copilot_session_id, w.retired_at,
                      COUNT(t.id) AS task_count,
                      SUM(CASE WHEN t.status='succeeded' THEN 1 ELSE 0 END)
                        AS succeeded_count,
                      SUM(CASE WHEN t.status='failed' THEN 1 ELSE 0 END)
                        AS failed_count,
                      SUM(CASE WHEN t.status='cancelled' THEN 1 ELSE 0 END)
                        AS cancelled_count,
                      SUM(CASE WHEN t.status='interrupted' THEN 1 ELSE 0 END)
                        AS interrupted_count,
                      SUM(CASE WHEN t.status IN (
                        'queued', 'dispatching', 'running', 'cancelling',
                        'awaiting_approval'
                      ) THEN 1 ELSE 0 END) AS active_count,
                      ROUND(AVG(
                        CASE
                          WHEN t.started_at IS NOT NULL
                           AND t.finished_at IS NOT NULL
                          THEN (
                            julianday(t.finished_at) - julianday(t.started_at)
                          ) * 86400
                        END
                      )) AS average_duration_seconds
                    FROM agent_workers w
                    LEFT JOIN agent_tasks t ON t.assigned_worker_id=w.id
                    GROUP BY w.id
                    ORDER BY
                      CASE WHEN w.retired_at IS NULL THEN 0 ELSE 1 END,
                      COALESCE(w.retired_at, w.created_at) DESC
                    LIMIT ?
                    """,
                    (limit,),
                ).fetchall()
            ]

    def update_agent_task(self, task_id: str, **fields: Any) -> None:
        if not fields:
            return
        if fields.get("status") in TERMINAL_TASK_STATUSES:
            fields = {
                **fields,
                "input_state": None,
                "input_reason": None,
                "input_source": None,
                "input_requested_at": None,
                "input_updated_at": utc_now(),
                "input_event_cursor": None,
                "input_interaction_id": None,
                "input_tool_call_id": None,
            }
        updates: dict[str, Any] = {}
        for key, value in fields.items():
            column = "result_json" if key == "result" else key
            updates[column] = self._json(value) if column == "result_json" else value
        updates["updated_at"] = utc_now()
        assignments = ", ".join(f"{key}=?" for key in updates)
        with self.connect() as connection:
            connection.execute(
                f"UPDATE agent_tasks SET {assignments} WHERE id=?",
                (*updates.values(), task_id),
            )

    def add_agent_event(
        self,
        *,
        task_id: str,
        event_type: str,
        payload: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        event_id = uuid.uuid4().hex
        now = utc_now()
        with self.connect() as connection:
            connection.execute(
                """
                INSERT INTO agent_events
                (id, task_id, event_type, payload_json, created_at)
                VALUES (?, ?, ?, ?, ?)
                """,
                (event_id, task_id, event_type, self._json(payload or {}), now),
            )
        return {
            "id": event_id,
            "task_id": task_id,
            "event_type": event_type,
            "payload": payload or {},
            "created_at": now,
        }

    def list_agent_events(
        self,
        task_id: str,
        limit: int = 500,
    ) -> list[dict[str, Any]]:
        with self.connect() as connection:
            return [
                self._decode(row)  # type: ignore[arg-type]
                for row in connection.execute(
                    """
                    SELECT * FROM agent_events
                    WHERE task_id=? ORDER BY created_at, id LIMIT ?
                    """,
                    (task_id, limit),
                ).fetchall()
            ]

    def add_agent_message(
        self,
        *,
        role: str,
        content: str,
        task_id: str | None = None,
    ) -> dict[str, Any]:
        message_id = uuid.uuid4().hex
        now = utc_now()
        with self.connect() as connection:
            connection.execute(
                """
                INSERT INTO agent_messages
                (id, role, content, task_id, created_at)
                VALUES (?, ?, ?, ?, ?)
                """,
                (message_id, role, content, task_id, now),
            )
        return {
            "id": message_id,
            "role": role,
            "content": content,
            "task_id": task_id,
            "created_at": now,
        }

    def list_agent_messages(self, limit: int = 100) -> list[dict[str, Any]]:
        with self.connect() as connection:
            rows = connection.execute(
                """
                SELECT * FROM (
                  SELECT * FROM agent_messages
                  ORDER BY created_at DESC, id DESC LIMIT ?
                ) ORDER BY created_at, id
                """,
                (limit,),
            ).fetchall()
        return [dict(row) for row in rows]

    def list_agent_manager_updates(self, limit: int = 20) -> list[dict[str, Any]]:
        with self.connect() as connection:
            return [
                dict(row)
                for row in connection.execute(
                    """
                    SELECT m.*, t.parent_task_id
                    FROM agent_messages m
                    JOIN agent_tasks t ON t.id=m.task_id
                    WHERE m.role='assistant'
                      AND t.normalized_prompt='worker_result_followup'
                      AND json_extract(t.result_json, '$.dismissed_at') IS NULL
                    ORDER BY m.created_at DESC LIMIT ?
                    """,
                    (limit,),
                ).fetchall()
            ]

    def agent_manager_update_count(self) -> int:
        with self.connect() as connection:
            row = connection.execute(
                """
                SELECT COUNT(*) AS count
                FROM agent_messages m
                JOIN agent_tasks t ON t.id=m.task_id
                WHERE m.role='assistant'
                  AND t.normalized_prompt='worker_result_followup'
                  AND json_extract(t.result_json, '$.dismissed_at') IS NULL
                """
            ).fetchone()
        return int(row["count"])

    def dismiss_agent_manager_update(self, message_id: str) -> bool:
        now = utc_now()
        with self.connect() as connection:
            cursor = connection.execute(
                """
                UPDATE agent_tasks
                SET result_json=json_set(result_json, '$.dismissed_at', ?),
                    updated_at=?
                WHERE id=(
                  SELECT task_id FROM agent_messages WHERE id=?
                )
                AND normalized_prompt='worker_result_followup'
                """,
                (now, now, message_id),
            )
            return cursor.rowcount == 1

    def recover_agent_runtime(
        self,
        *,
        dispatch_timeout_seconds: float = 90,
        dispatch_attempt_limit: int = 4,
        resumable_task_ids: tuple[str, ...] = (),
    ) -> None:
        now = utc_now()
        stale_before = (
            datetime.now(UTC) - timedelta(seconds=dispatch_timeout_seconds)
        ).isoformat()
        preserve = ""
        parameters: tuple[Any, ...] = ()
        if resumable_task_ids:
            preserve = " AND id NOT IN (" + ",".join("?" for _ in resumable_task_ids) + ")"
            parameters = tuple(resumable_task_ids)
        with self.connect() as connection:
            connection.execute(
                f"""
                UPDATE agent_tasks
                SET status='failed', result_json=?, finished_at=?,
                    dispatch_phase='failed',
                    dispatch_error='Dispatch recovery exceeded its persisted bound',
                    updated_at=?
                WHERE status='dispatching'
                  AND dispatch_interaction_id IS NULL
                  AND (
                    dispatch_attempt >= ?
                    OR dispatch_deadline_at <= ?
                    OR (
                      dispatch_deadline_at IS NULL
                      AND COALESCE(
                        dispatch_started_at, started_at, updated_at, created_at
                      ) <= ?
                    )
                  ){preserve}
                """,
                (
                    self._json(
                        {
                            "error": (
                                "Dispatch recovery exceeded its persisted "
                                "attempt or wall-clock bound"
                            )
                        }
                    ),
                    now,
                    now,
                    dispatch_attempt_limit,
                    now,
                    stale_before,
                    *parameters,
                ),
            )
            connection.execute(
                f"""
                UPDATE agent_tasks
                SET status='interrupted', finished_at=?, updated_at=?
                WHERE status IN ('running', 'cancelling'){preserve}
                """,
                (now, now, *parameters),
            )
            connection.execute(
                f"""
                UPDATE agent_tasks
                SET status='queued', assigned_worker_id=NULL,
                    dispatch_phase='recovering', updated_at=?
                WHERE status='dispatching'
                  AND dispatch_interaction_id IS NULL
                  AND dispatch_attempt < ?
                  AND (dispatch_deadline_at IS NULL OR dispatch_deadline_at > ?)
                  {preserve}
                """,
                (now, dispatch_attempt_limit, now, *parameters),
            )
            connection.execute(
                """
                UPDATE agent_workers
                SET state=CASE
                      WHEN direct_submitted_at IS NOT NULL THEN 'busy'
                      ELSE 'idle'
                    END,
                    current_task_id=NULL,
                    activity_kind=CASE
                      WHEN direct_submitted_at IS NOT NULL THEN 'direct'
                      ELSE NULL
                    END,
                    pid=NULL, pgid=NULL, updated_at=?, heartbeat_at=?
                WHERE current_task_id IS NOT NULL
                  AND current_task_id NOT IN (
                    SELECT id FROM agent_tasks
                    WHERE status IN ('dispatching', 'running', 'cancelling')
                  )
                """,
                (now, now),
            )
            connection.execute(
                """
                UPDATE agent_workers
                SET pid=NULL, pgid=NULL, updated_at=?, heartbeat_at=?
                WHERE retired_at IS NULL
                """,
                (now, now),
            )

    def get_state(self, key: str) -> str | None:
        with self.connect() as connection:
            row = connection.execute(
                "SELECT value FROM app_state WHERE key=?",
                (key,),
            ).fetchone()
            return str(row["value"]) if row else None

    def set_state(self, key: str, value: str) -> None:
        with self.connect() as connection:
            self._set_state_in_connection(connection, key, value, utc_now())

    @staticmethod
    def _set_state_in_connection(
        connection: sqlite3.Connection,
        key: str,
        value: str,
        now: str,
    ) -> None:
        connection.execute(
            """
            INSERT INTO app_state (key, value, updated_at)
            VALUES (?, ?, ?)
            ON CONFLICT(key) DO UPDATE SET
              value=excluded.value,
              updated_at=excluded.updated_at
            """,
            (key, value, now),
        )

    @classmethod
    def _hub_restart_value(
        cls,
        connection: sqlite3.Connection,
        restart_id: str,
    ) -> dict[str, Any] | None:
        value = cls._decode(
            connection.execute(
                "SELECT * FROM hub_restarts WHERE id=?",
                (restart_id,),
            ).fetchone()
        )
        if value is None:
            return None
        value["initiators"] = [
            cls._decode(row)
            for row in connection.execute(
                """
                SELECT * FROM hub_restart_initiators
                WHERE restart_id=? ORDER BY created_at, id
                """,
                (restart_id,),
            ).fetchall()
        ]
        return value

    def current_hub_server(self) -> dict[str, Any] | None:
        generation = self.get_state("hub_server_generation")
        pid = self.get_state("hub_server_pid")
        if not generation or not pid:
            return None
        try:
            parsed_pid = int(pid)
        except ValueError:
            return None
        return {
            "generation": generation,
            "pid": parsed_pid,
            "started_at": self.get_state("hub_server_started_at"),
            "restart_token": self.get_state("hub_server_restart_token"),
            "process_identity": self.get_state("hub_server_process_identity"),
        }

    def register_hub_server(
        self,
        *,
        pid: int,
        restart_token: str | None = None,
        process_identity: str | None = None,
    ) -> dict[str, Any]:
        now = utc_now()
        generation = uuid.uuid4().hex
        with self.connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            if restart_token:
                ready = connection.execute(
                    """
                    SELECT 1 FROM hub_restarts
                    WHERE id=? AND status='starting'
                    """,
                    (restart_token,),
                ).fetchone()
                if ready is None:
                    raise RuntimeError("Restart token is not ready to start a Hub server")
            for key, value in (
                ("hub_server_generation", generation),
                ("hub_server_pid", str(pid)),
                ("hub_server_started_at", now),
                ("hub_server_restart_token", restart_token or ""),
                ("hub_server_process_identity", process_identity or ""),
            ):
                self._set_state_in_connection(connection, key, value, now)
            if restart_token:
                connection.execute(
                    """
                    UPDATE hub_restarts
                    SET replacement_server_generation=?,
                        replacement_server_pid=?, updated_at=?
                    WHERE id=? AND status='starting'
                    """,
                    (generation, pid, now, restart_token),
                )
        return {
            "generation": generation,
            "pid": pid,
            "started_at": now,
            "restart_token": restart_token,
            "process_identity": process_identity,
        }

    def unregister_hub_server(self, *, generation: str, pid: int) -> bool:
        with self.connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            current = connection.execute(
                """
                SELECT
                  (SELECT value FROM app_state
                   WHERE key='hub_server_generation') AS generation,
                  (SELECT value FROM app_state
                   WHERE key='hub_server_pid') AS pid
                """
            ).fetchone()
            if (
                current is None
                or current["generation"] != generation
                or current["pid"] != str(pid)
            ):
                return False
            connection.execute(
                """
                DELETE FROM app_state
                WHERE key IN (
                  'hub_server_generation', 'hub_server_pid',
                  'hub_server_started_at', 'hub_server_restart_token',
                  'hub_server_process_identity'
                )
                """
            )
            return True

    def get_hub_restart(self, restart_id: str) -> dict[str, Any] | None:
        with self.connect() as connection:
            return self._hub_restart_value(connection, restart_id)

    def reconcile_hub_restart_handoff(
        self,
        restart_id: str,
    ) -> dict[str, Any] | None:
        now = utc_now()
        with self.connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            restart = connection.execute(
                "SELECT status FROM hub_restarts WHERE id=?",
                (restart_id,),
            ).fetchone()
            if restart is None or restart["status"] != "requested":
                return self._hub_restart_value(connection, restart_id)
            initiators = connection.execute(
                """
                SELECT * FROM hub_restart_initiators
                WHERE restart_id=? AND state='accepted'
                """,
                (restart_id,),
            ).fetchall()
            for initiator in initiators:
                worker = connection.execute(
                    "SELECT * FROM agent_workers WHERE id=?",
                    (initiator["worker_id"],),
                ).fetchone()
                if initiator["activity_kind"] == "task":
                    task = connection.execute(
                        "SELECT status FROM agent_tasks WHERE id=?",
                        (initiator["task_id"],),
                    ).fetchone()
                    terminal = bool(
                        task
                        and task["status"] in TERMINAL_TASK_STATUSES
                        and (
                            worker is None
                            or worker["current_task_id"] != initiator["task_id"]
                        )
                    )
                else:
                    terminal = bool(
                        worker is None
                        or worker["direct_submitted_at"] is None
                        or worker["direct_generation"]
                        != initiator["direct_generation"]
                        or worker["direct_interaction_id"]
                        != initiator["interaction_id"]
                    )
                if terminal:
                    connection.execute(
                        """
                        UPDATE hub_restart_initiators
                        SET state='terminal', terminal_at=?, updated_at=?
                        WHERE id=? AND state='accepted'
                        """,
                        (now, now, initiator["id"]),
                    )
            pending = connection.execute(
                """
                SELECT 1 FROM hub_restart_initiators
                WHERE restart_id=? AND state='accepted' LIMIT 1
                """,
                (restart_id,),
            ).fetchone()
            if pending is None:
                connection.execute(
                    """
                    UPDATE hub_restarts
                    SET status='handed_off',
                        handed_off_at=COALESCE(handed_off_at, ?),
                        updated_at=?
                    WHERE id=? AND status='requested'
                    """,
                    (now, now, restart_id),
                )
            return self._hub_restart_value(connection, restart_id)

    def begin_hub_restart(
        self,
        *,
        source_server_generation: str,
        source_server_pid: int,
        worker_id: str | None = None,
        copilot_session_id: str | None = None,
        terminal_generation: int | None = None,
        activity_kind: str = "external",
        task_id: str | None = None,
        direct_generation: int | None = None,
        interaction_id: str | None = None,
    ) -> dict[str, Any]:
        now = utc_now()
        with self.connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            current = connection.execute(
                """
                SELECT
                  (SELECT value FROM app_state
                   WHERE key='hub_server_generation') AS generation,
                  (SELECT value FROM app_state
                   WHERE key='hub_server_pid') AS pid,
                  (SELECT value FROM app_state
                   WHERE key='hub_server_process_identity') AS identity
                """
            ).fetchone()
            if (
                current is None
                or current["generation"] != source_server_generation
                or current["pid"] != str(source_server_pid)
            ):
                raise RuntimeError("Hub server generation changed before restart handoff")
            if worker_id:
                worker = connection.execute(
                    "SELECT * FROM agent_workers WHERE id=?",
                    (worker_id,),
                ).fetchone()
                if (
                    worker is None
                    or worker["copilot_session_id"] != copilot_session_id
                    or terminal_generation is None
                    or int(worker["terminal_generation"]) != terminal_generation
                ):
                    raise RuntimeError("Restart initiator terminal changed")
                if activity_kind == "direct":
                    valid = (
                        int(worker["direct_generation"])
                        == int(direct_generation or -1)
                        and worker["direct_interaction_id"] == interaction_id
                        and worker["direct_submitted_at"] is not None
                    )
                elif activity_kind == "task":
                    task = connection.execute(
                        "SELECT * FROM agent_tasks WHERE id=?",
                        (task_id,),
                    ).fetchone()
                    valid = bool(
                        task
                        and worker["current_task_id"] == task_id
                        and task["assigned_worker_id"] == worker_id
                        and task["dispatch_interaction_id"] == interaction_id
                    )
                else:
                    valid = False
                if not valid:
                    raise RuntimeError("Restart interaction is not active")
            existing = connection.execute(
                """
                SELECT * FROM hub_restarts
                WHERE source_server_generation=? AND status='requested'
                ORDER BY generation DESC LIMIT 1
                """,
                (source_server_generation,),
            ).fetchone()
            created = existing is None
            if existing is None:
                active = connection.execute(
                    """
                    SELECT status FROM hub_restarts
                    WHERE source_server_generation=?
                      AND status IN ('handed_off', 'stopping', 'starting')
                    ORDER BY generation DESC LIMIT 1
                    """,
                    (source_server_generation,),
                ).fetchone()
                if active:
                    raise RuntimeError(
                        f"Hub restart is already {active['status']}; "
                        "a late request cannot join this generation"
                    )
                sequence = connection.execute(
                    """
                    SELECT value FROM app_state
                    WHERE key='hub_restart_generation'
                    """
                ).fetchone()
                generation = int(sequence["value"]) + 1 if sequence else 1
                self._set_state_in_connection(
                    connection,
                    "hub_restart_generation",
                    str(generation),
                    now,
                )
                restart_id = uuid.uuid4().hex
                snapshot = {
                    "workers": [
                        {
                            "id": row["id"],
                            "role": row["role"],
                            "state": row["state"],
                            "current_task_id": row["current_task_id"],
                            "activity_kind": row["activity_kind"],
                            "direct_generation": row["direct_generation"],
                            "direct_interaction_id": row["direct_interaction_id"],
                        }
                        for row in connection.execute(
                            """
                            SELECT * FROM agent_workers
                            WHERE state != 'idle'
                               OR current_task_id IS NOT NULL
                               OR direct_submitted_at IS NOT NULL
                            ORDER BY role, created_at
                            """
                        ).fetchall()
                    ],
                    "tasks": [
                        {
                            "id": row["id"],
                            "title": row["title"],
                            "status": row["status"],
                            "assigned_worker_id": row["assigned_worker_id"],
                            "dispatch_generation": row["dispatch_generation"],
                            "dispatch_interaction_id": row[
                                "dispatch_interaction_id"
                            ],
                        }
                        for row in connection.execute(
                            """
                            SELECT * FROM agent_tasks
                            WHERE status IN (
                              'queued', 'awaiting_approval', 'dispatching',
                              'running', 'cancelling'
                            )
                            ORDER BY created_at
                            """
                        ).fetchall()
                    ],
                }
                status = "requested" if worker_id else "handed_off"
                connection.execute(
                    """
                    INSERT INTO hub_restarts (
                      id, generation, source_server_generation,
                      source_server_pid, source_process_identity, status,
                      snapshot_json, requested_at, handed_off_at, updated_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        restart_id,
                        generation,
                        source_server_generation,
                        source_server_pid,
                        current["identity"],
                        status,
                        self._json(snapshot),
                        now,
                        now if status == "handed_off" else None,
                        now,
                    ),
                )
            else:
                restart_id = str(existing["id"])
            initiator_added = False
            if worker_id:
                duplicate = connection.execute(
                    """
                    SELECT 1 FROM hub_restart_initiators
                    WHERE restart_id=? AND worker_id=?
                      AND copilot_session_id=? AND terminal_generation=?
                      AND activity_kind=? AND task_id IS ?
                      AND direct_generation IS ? AND interaction_id IS ?
                    """,
                    (
                        restart_id,
                        worker_id,
                        copilot_session_id,
                        terminal_generation,
                        activity_kind,
                        task_id,
                        direct_generation,
                        interaction_id,
                    ),
                ).fetchone()
                if duplicate is None:
                    connection.execute(
                        """
                        INSERT INTO hub_restart_initiators (
                          id, restart_id, worker_id, copilot_session_id,
                          terminal_generation, activity_kind, task_id,
                          direct_generation, interaction_id, state,
                          created_at, updated_at
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 'accepted', ?, ?)
                        """,
                        (
                            uuid.uuid4().hex,
                            restart_id,
                            worker_id,
                            copilot_session_id,
                            terminal_generation,
                            activity_kind,
                            task_id,
                            direct_generation,
                            interaction_id,
                            now,
                            now,
                        ),
                    )
                    connection.execute(
                        """
                        UPDATE hub_restarts
                        SET status='requested', handed_off_at=NULL, updated_at=?
                        WHERE id=? AND status IN ('requested', 'handed_off')
                        """,
                        (now, restart_id),
                    )
                    initiator_added = True
            value = self._hub_restart_value(connection, restart_id)
            assert value is not None
            value["created"] = created
            value["initiator_added"] = initiator_added
            return value

    def claim_hub_restart_helper(
        self,
        restart_id: str,
        *,
        helper_pid: int,
        replace_helper_pid: int | None = None,
    ) -> bool:
        clauses = ["helper_pid IS NULL", "helper_pid=?"]
        parameters: list[Any] = [helper_pid]
        if replace_helper_pid is not None:
            clauses.append("helper_pid=?")
            parameters.append(replace_helper_pid)
        with self.connect() as connection:
            cursor = connection.execute(
                f"""
                UPDATE hub_restarts
                SET helper_pid=?, updated_at=?
                WHERE id=? AND status IN ('requested', 'handed_off')
                  AND ({' OR '.join(clauses)})
                """,
                (helper_pid, utc_now(), restart_id, *parameters),
            )
            return cursor.rowcount == 1

    def begin_hub_restart_stop(
        self,
        restart_id: str,
        *,
        source_server_generation: str,
        source_server_pid: int,
    ) -> bool:
        now = utc_now()
        with self.connect() as connection:
            pending = connection.execute(
                """
                SELECT 1 FROM hub_restart_initiators
                WHERE restart_id=? AND state='accepted' LIMIT 1
                """,
                (restart_id,),
            ).fetchone()
            if pending:
                return False
            cursor = connection.execute(
                """
                UPDATE hub_restarts
                SET status='stopping', stopping_at=?, updated_at=?
                WHERE id=? AND status='handed_off'
                  AND source_server_generation=? AND source_server_pid=?
                """,
                (
                    now,
                    now,
                    restart_id,
                    source_server_generation,
                    source_server_pid,
                ),
            )
            return cursor.rowcount == 1

    def mark_hub_restart_starting(self, restart_id: str) -> bool:
        now = utc_now()
        with self.connect() as connection:
            cursor = connection.execute(
                """
                UPDATE hub_restarts
                SET status='starting', starting_at=?, updated_at=?
                WHERE id=? AND status='stopping'
                """,
                (now, now, restart_id),
            )
            return cursor.rowcount == 1

    def mark_hub_restart_healthy(
        self,
        restart_id: str,
        *,
        replacement_server_generation: str,
        replacement_server_pid: int,
    ) -> bool:
        now = utc_now()
        with self.connect() as connection:
            cursor = connection.execute(
                """
                UPDATE hub_restarts
                SET status='healthy', healthy_at=?, finished_at=?, updated_at=?,
                    replacement_server_generation=?,
                    replacement_server_pid=?, result_json=?
                WHERE id=? AND status='starting'
                """,
                (
                    now,
                    now,
                    now,
                    replacement_server_generation,
                    replacement_server_pid,
                    self._json(
                        {
                            "response": (
                                "Copilot Hub restarted and passed health checks."
                            )
                        }
                    ),
                    restart_id,
                ),
            )
            return cursor.rowcount == 1

    def finish_hub_restart(
        self,
        restart_id: str,
        *,
        status: str,
        error: str,
    ) -> bool:
        if status not in {"failed", "superseded"}:
            raise ValueError("Restart terminal status must be failed or superseded")
        now = utc_now()
        with self.connect() as connection:
            cursor = connection.execute(
                """
                UPDATE hub_restarts
                SET status=?, result_json=?, finished_at=?, updated_at=?
                WHERE id=? AND status IN (
                  'requested', 'handed_off', 'stopping', 'starting'
                )
                """,
                (status, self._json({"error": error}), now, now, restart_id),
            )
            return cursor.rowcount == 1

    def recover_hub_restarts(self, *, current_server_generation: str) -> None:
        now = utc_now()
        with self.connect() as connection:
            interrupted = connection.execute(
                """
                SELECT * FROM hub_restarts
                WHERE status='requested' AND source_server_generation != ?
                """,
                (current_server_generation,),
            ).fetchall()
            for restart in interrupted:
                initiators = connection.execute(
                    """
                    SELECT * FROM hub_restart_initiators
                    WHERE restart_id=? AND state='accepted'
                    """,
                    (restart["id"],),
                ).fetchall()
                for initiator in initiators:
                    if not initiator["worker_id"]:
                        continue
                    worker_id = str(initiator["worker_id"])
                    if initiator["activity_kind"] == "task":
                        connection.execute(
                            """
                            UPDATE agent_tasks
                            SET status='interrupted', result_json=?,
                                finished_at=?, updated_at=?
                            WHERE id=? AND assigned_worker_id=?
                              AND status IN (
                                'dispatching', 'running', 'cancelling'
                              )
                            """,
                            (
                                self._json(
                                    {
                                        "response": (
                                            "Restart control was accepted before "
                                            "the prior server exited; the active "
                                            "interaction was not replayed."
                                        )
                                    }
                                ),
                                now,
                                now,
                                initiator["task_id"],
                                worker_id,
                            ),
                        )
                    connection.execute(
                        """
                        UPDATE agent_workers
                        SET state='idle', current_task_id=NULL,
                            activity_kind=NULL, activity_started_at=NULL,
                            activity_updated_at=NULL,
                            direct_event_cursor=NULL,
                            direct_interaction_id=NULL,
                            direct_submitted_at=NULL,
                            direct_prompt_hash=NULL, direct_context=NULL,
                            copilot_session_id=?,
                            terminal_generation=terminal_generation + 1,
                            turn_count=0, pid=NULL, pgid=NULL,
                            updated_at=?, heartbeat_at=?
                        WHERE id=?
                        """,
                        (str(uuid.uuid4()), now, now, worker_id),
                    )
                    connection.execute(
                        """
                        UPDATE hub_restart_initiators
                        SET state='aborted', terminal_at=?, updated_at=?,
                            result_json=?
                        WHERE id=? AND state='accepted'
                        """,
                        (
                            now,
                            now,
                            self._json(
                                {
                                    "error": (
                                        "Source server exited before the restart "
                                        "interaction became terminal; replay was "
                                        "suppressed."
                                    )
                                }
                            ),
                            initiator["id"],
                        ),
                    )
                connection.execute(
                    """
                    UPDATE hub_restarts
                    SET status='failed', finished_at=?, updated_at=?,
                        result_json=?
                    WHERE id=? AND status='requested'
                    """,
                    (
                        now,
                        now,
                        self._json(
                            {
                                "error": (
                                    "Source server exited before restart handoff "
                                    "completed."
                                )
                            }
                        ),
                        restart["id"],
                    ),
                )
