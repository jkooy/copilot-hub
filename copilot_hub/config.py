from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

DEFAULT_AGENT_MAX_WORKERS = 7
MIN_AGENT_MAX_WORKERS = 1
MAX_AGENT_MAX_WORKERS = 16
MANAGER_SESSION_COUNT = 1


def read_env_file(path: Path) -> dict[str, str]:
    """``KEY=VALUE`` lines from a local settings file; blank lines and ``#`` comments skipped."""
    try:
        lines = path.read_text(encoding="utf-8").splitlines()
    except OSError:
        return {}
    values: dict[str, str] = {}
    for line in lines:
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        values[key.strip().removeprefix("export ").strip()] = value.strip().strip("'\"")
    return values


PERMISSION_MODES = (
    "bypassPermissions",
    "acceptEdits",
    "auto",
    "default",
    "dontAsk",
    "plan",
)


def total_copilot_sessions(max_workers: int) -> int:
    return max_workers + MANAGER_SESSION_COUNT


@dataclass(frozen=True)
class Settings:
    state_dir: Path
    db_path: Path
    log_dir: Path
    host: str
    port: int
    claude_executable: str
    permission_mode: str
    agent_default_cwd: str
    agent_model: str
    agent_reasoning_effort: str
    agent_context_tier: str
    agent_max_workers: int
    manager_session_id: str | None
    terminals_enabled: bool
    allow_remote: bool
    daily_budget_usd: float = 300.0
    budget_timezone: str | None = None
    spend_ledger: str | None = None

    @classmethod
    def from_env(cls) -> Settings:
        home = Path.home()
        state_dir = Path(
            os.environ.get("COPILOT_HUB_STATE_DIR", "~/.copilot-hub")
        ).expanduser()
        # Machine-local settings that stay out of the repository; the environment wins.
        local = read_env_file(state_dir / "hub.env")

        def _env(name: str, default: str) -> str:
            return os.environ.get(name, local.get(name, default))

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
            claude_executable=_env("COPILOT_HUB_CLAUDE", "claude"),
            permission_mode=_env("COPILOT_HUB_PERMISSION_MODE", "bypassPermissions"),
            agent_default_cwd=_env(
                "COPILOT_HUB_CWD",
                str(home / "copilot_hub_workspace"),
            ),
            agent_model=_env("COPILOT_HUB_MODEL", "opus"),
            agent_reasoning_effort=_env("COPILOT_HUB_EFFORT", "xhigh"),
            agent_context_tier=_env("COPILOT_HUB_CONTEXT", "standard"),
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
            daily_budget_usd=float(_env("COPILOT_HUB_DAILY_BUDGET_USD", "300")),
            budget_timezone=_env("COPILOT_HUB_BUDGET_TIMEZONE", "") or None,
            spend_ledger=_env("COPILOT_HUB_SPEND_LEDGER", "") or None,
        )

    def __post_init__(self) -> None:
        if self.permission_mode not in PERMISSION_MODES:
            raise ValueError(
                "COPILOT_HUB_PERMISSION_MODE must be one of: " + ", ".join(PERMISSION_MODES)
            )

    @property
    def sessions_dir(self) -> Path:
        return self.state_dir / "sessions"

    def ensure_directories(self) -> None:
        self.state_dir.mkdir(parents=True, exist_ok=True)
        self.log_dir.mkdir(parents=True, exist_ok=True)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self.sessions_dir.mkdir(parents=True, exist_ok=True)
        Path(self.agent_default_cwd).expanduser().mkdir(parents=True, exist_ok=True)
