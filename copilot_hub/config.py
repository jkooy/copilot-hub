from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

DEFAULT_AGENT_MAX_WORKERS = 7
MIN_AGENT_MAX_WORKERS = 1
MAX_AGENT_MAX_WORKERS = 16
MANAGER_SESSION_COUNT = 1


def _env(name: str, default: str) -> str:
    return os.environ.get(name, default)


def total_copilot_sessions(max_workers: int) -> int:
    return max_workers + MANAGER_SESSION_COUNT


@dataclass(frozen=True)
class Settings:
    state_dir: Path
    db_path: Path
    log_dir: Path
    host: str
    port: int
    copilot_executable: str
    agent_default_cwd: str
    agent_model: str
    agent_reasoning_effort: str
    agent_context_tier: str
    agent_max_workers: int
    manager_session_id: str | None
    terminals_enabled: bool
    allow_remote: bool

    @classmethod
    def from_env(cls) -> Settings:
        home = Path.home()
        state_dir = Path(
            _env("COPILOT_HUB_STATE_DIR", "~/.copilot-hub")
        ).expanduser()
        return cls(
            state_dir=state_dir,
            db_path=Path(
                _env("COPILOT_HUB_DB", str(state_dir / "copilot_hub.db"))
            ).expanduser(),
            log_dir=Path(
                _env("COPILOT_HUB_LOG_DIR", str(state_dir / "logs"))
            ).expanduser(),
            host=_env("COPILOT_HUB_HOST", "127.0.0.1"),
            port=int(_env("COPILOT_HUB_PORT", "8767")),
            copilot_executable=_env("COPILOT_HUB_COPILOT", "copilot"),
            agent_default_cwd=_env(
                "COPILOT_HUB_CWD",
                str(home / "copilot_hub_workspace"),
            ),
            agent_model=_env("COPILOT_HUB_MODEL", "gpt-5.6-sol"),
            agent_reasoning_effort=_env("COPILOT_HUB_EFFORT", "xhigh"),
            agent_context_tier=_env("COPILOT_HUB_CONTEXT", "long_context"),
            agent_max_workers=int(
                _env(
                    "COPILOT_HUB_MAX_WORKERS",
                    str(DEFAULT_AGENT_MAX_WORKERS),
                )
            ),
            manager_session_id=(
                _env("COPILOT_HUB_MANAGER_SESSION_ID", "") or None
            ),
            terminals_enabled=_env("COPILOT_HUB_TERMINALS", "1")
            not in {"0", "false", "False"},
            allow_remote=_env("COPILOT_HUB_ALLOW_REMOTE", "0")
            in {"1", "true", "True"},
        )

    def ensure_directories(self) -> None:
        self.state_dir.mkdir(parents=True, exist_ok=True)
        self.log_dir.mkdir(parents=True, exist_ok=True)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        Path(self.agent_default_cwd).expanduser().mkdir(parents=True, exist_ok=True)
