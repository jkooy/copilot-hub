from __future__ import annotations

from pathlib import Path

import pytest

from copilot_hub.cli import build_parser, validate_bind_host
from copilot_hub.config import Settings, total_copilot_sessions


def test_generic_defaults_and_environment(tmp_path, monkeypatch):
    monkeypatch.setenv("HOME", str(tmp_path))
    monkeypatch.setenv("COPILOT_HUB_MAX_WORKERS", "3")
    monkeypatch.setenv("COPILOT_HUB_PORT", "9911")
    monkeypatch.setenv("COPILOT_HUB_CWD", str(tmp_path / "work"))

    settings = Settings.from_env()

    assert settings.state_dir == tmp_path / ".copilot-hub"
    assert settings.db_path == tmp_path / ".copilot-hub" / "copilot_hub.db"
    assert settings.agent_max_workers == 3
    assert settings.port == 9911
    assert total_copilot_sessions(3) == 4
    settings.ensure_directories()
    assert settings.log_dir.is_dir()
    assert Path(settings.agent_default_cwd).is_dir()


def test_personal_isolation_defaults(tmp_path, monkeypatch):
    monkeypatch.setenv("HOME", str(tmp_path))
    monkeypatch.delenv("COPILOT_HUB_PORT", raising=False)
    monkeypatch.delenv("COPILOT_HUB_CWD", raising=False)

    settings = Settings.from_env()

    assert settings.port == 8767
    assert settings.agent_default_cwd == str(tmp_path / "copilot_hub_workspace")


def test_cli_exposes_only_generic_commands():
    parser = build_parser()
    help_text = parser.format_help()
    assert "copilot-hub" in help_text
    choices = parser._subparsers._group_actions[0].choices
    assert set(choices) == {"init", "serve", "dispatch", "status", "restart", "open"}


def test_remote_bind_requires_explicit_opt_in():
    assert validate_bind_host("127.0.0.1", allow_remote=False) == "127.0.0.1"
    assert validate_bind_host("localhost", allow_remote=False) == "localhost"
    with pytest.raises(ValueError, match="loopback"):
        validate_bind_host("0.0.0.0", allow_remote=False)
    assert validate_bind_host("0.0.0.0", allow_remote=True) == "0.0.0.0"
