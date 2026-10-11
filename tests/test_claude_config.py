from __future__ import annotations

import json
from pathlib import Path

import pytest

from copilot_hub.claude_config import trust_folders


def test_trust_folders_preserves_other_state_permissions_and_is_idempotent(tmp_path):
    config = tmp_path / ".claude.json"
    config.write_text(
        json.dumps({"theme": "dark", "projects": {"/elsewhere": {"allowedTools": []}}}),
        encoding="utf-8",
    )
    config.chmod(0o640)
    first = tmp_path / "first"
    second = tmp_path / "second"
    first.mkdir()
    second.mkdir()

    trusted = trust_folders(config, [first, second, first])
    repeated = trust_folders(config, [second])

    assert trusted == [str(first.resolve()), str(second.resolve())]
    assert repeated == [str(second.resolve())]
    data = json.loads(config.read_text(encoding="utf-8"))
    assert data["theme"] == "dark"
    assert data["projects"]["/elsewhere"] == {"allowedTools": []}
    for folder in trusted:
        assert data["projects"][folder]["hasTrustDialogAccepted"] is True
    assert config.stat().st_mode & 0o777 == 0o640
    assert not list(tmp_path.glob("..claude.json.*"))


def test_trust_folders_rejects_invalid_existing_value(tmp_path):
    config = tmp_path / ".claude.json"
    config.write_text('{"projects": "all"}', encoding="utf-8")
    with pytest.raises(ValueError, match="object of objects"):
        trust_folders(config, [tmp_path])


def test_setup_trusts_only_project_and_managed_workspace():
    script = Path("scripts/setup.sh").read_text(encoding="utf-8")

    invocation = script.split("-m copilot_hub.claude_config", 1)[1].split(
        ">/dev/null",
        1,
    )[0]
    assert '"$ROOT"' in invocation
    assert '"$MANAGED_CWD"' in invocation
    assert '"${HOME}"' not in invocation
    assert "copilot_hub_workspace" in script
