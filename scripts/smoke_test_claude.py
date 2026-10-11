"""Smoke-test the Hub's Claude Code integration against the real `claude` CLI.

Starts one managed Claude session in a PTY through TerminalRuntime, waits for
the SessionStart hook, sends a prompt, and waits for the Stop hook's final
answer. Uses throwaway state and workspace directories.

    uv run python scripts/smoke_test_claude.py [--model haiku] [--permission-mode default]
"""

from __future__ import annotations

import argparse
import json
import re
import tempfile
import time
import uuid
from pathlib import Path

from copilot_hub.config import Settings
from copilot_hub.db import Repository
from copilot_hub.terminal_runtime import TerminalRuntime

EXPECTED = "HUB-SMOKE-OK"


def visible_tail(data: bytes, limit: int = 2500) -> str:
    text = data.decode(errors="ignore")
    text = re.sub(r"\x1b\][^\x07]*(?:\x07|\x1b\\)", "", text)
    text = re.sub(r"\x1b\[[0-?]*[ -/]*[@-~]", " ", text)
    return text[-limit:]


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--model", default="haiku")
    parser.add_argument("--permission-mode", default="default")
    parser.add_argument("--claude", default="claude")
    parser.add_argument("--timeout", type=float, default=180)
    args = parser.parse_args()

    base = Path(tempfile.mkdtemp(prefix="copilot-hub-smoke-"))
    workspace = base / "work"
    workspace.mkdir()
    settings = Settings(
        state_dir=base / "state",
        db_path=base / "state" / "hub.db",
        log_dir=base / "state" / "logs",
        host="127.0.0.1",
        port=0,
        claude_executable=args.claude,
        permission_mode=args.permission_mode,
        agent_default_cwd=str(workspace),
        agent_model=args.model,
        agent_reasoning_effort="low",
        agent_context_tier="standard",
        agent_max_workers=1,
        manager_session_id=None,
        terminals_enabled=True,
        allow_remote=False,
    )
    settings.ensure_directories()
    repository = Repository(settings.db_path)
    repository.initialize()
    runtime = TerminalRuntime(repository, settings)
    worker = repository.create_agent_worker(
        copilot_session_id=str(uuid.uuid4()),
        name="smoke",
        role="worker",
        model=args.model,
        reasoning_effort="low",
        context_tier="standard",
        cwd=str(workspace),
    )
    session_id = worker["copilot_session_id"]
    print(f"state: {base}")
    session = None
    try:
        step = "start Claude and receive SessionStart"
        started = time.monotonic()
        session = runtime.ensure_terminal(worker, wait_for_ready=False)
        runtime._wait_until_ready(session, timeout_seconds=args.timeout)
        print(f"ok   {step} ({time.monotonic() - started:.1f}s)")

        step = "paste and submit a prompt"
        prompt = f"Reply with exactly this text and nothing else: {EXPECTED}"
        cursor = runtime.event_cursor(session_id)
        runtime.send_prompt(worker, prompt)
        interaction_id = runtime.wait_for_prompt_after(session_id, cursor, prompt)
        print(f"ok   {step} (interaction {interaction_id})")

        step = "receive the final answer from the Stop hook"
        deadline = time.monotonic() + args.timeout
        response = None
        while time.monotonic() < deadline and response is None:
            response = runtime.response_after_cursor(session_id, cursor, interaction_id)
            time.sleep(0.2)
        if response is None:
            raise RuntimeError("timed out")
        print(f"ok   {step}: {response!r}")
        if EXPECTED not in response:
            print("WARN the model did not echo the expected text exactly")

        step = "become ready for the next prompt"
        runtime._wait_until_ready(session, timeout_seconds=30)
        print(f"ok   {step}")
        print("PASS")
        return 0
    except Exception as exc:  # noqa: BLE001 - report any failure with diagnostics.
        print(f"FAIL {step}: {exc}")
        if session is not None:
            data, _ = session.read_from(0)
            print("---- terminal (tail) ----")
            print(visible_tail(data))
        log = settings.sessions_dir / session_id / "events.jsonl"
        if log.exists():
            print("---- hub events ----")
            for line in log.read_text().splitlines()[-20:]:
                event = json.loads(line)
                print(event["type"], json.dumps(event.get("data"))[:160])
        hook_log = settings.log_dir / "claude-hooks.log"
        if hook_log.exists():
            print("---- hook errors ----")
            print(hook_log.read_text()[-2000:])
        return 1
    finally:
        runtime.shutdown()


if __name__ == "__main__":
    raise SystemExit(main())
