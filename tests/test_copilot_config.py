from __future__ import annotations

import json
from pathlib import Path

import pytest

from copilot_hub.copilot_config import trust_folders


def test_trust_folders_preserves_preamble_permissions_and_is_idempotent(tmp_path):
    config = tmp_path / "config.json"
    config.write_text('// local settings\n{"theme": "dark"}\n', encoding="utf-8")
    config.chmod(0o640)
    first = tmp_path / "first"
    second = tmp_path / "second"
    first.mkdir()
    second.mkdir()

    trusted = trust_folders(config, [first, second, first])
    repeated = trust_folders(config, [second])

    assert trusted == [str(first.resolve()), str(second.resolve())]
    assert repeated == trusted
    rendered = config.read_text(encoding="utf-8")
    assert rendered.startswith("// local settings\n")
    body = "\n".join(rendered.splitlines()[1:])
    assert json.loads(body)["trustedFolders"] == trusted
    assert config.stat().st_mode & 0o777 == 0o640
    assert not list(tmp_path.glob(".config.json.*"))


def test_trust_folders_rejects_invalid_existing_value(tmp_path):
    config = tmp_path / "config.json"
    config.write_text('{"trustedFolders": "all"}', encoding="utf-8")
    with pytest.raises(ValueError, match="list of strings"):
        trust_folders(config, [tmp_path])


def test_setup_trusts_only_project_and_managed_workspace():
    script = Path("scripts/setup.sh").read_text(encoding="utf-8")

    invocation = script.split("-m copilot_hub.copilot_config", 1)[1].split(
        ">/dev/null",
        1,
    )[0]
    assert '"$ROOT"' in invocation
    assert '"$MANAGED_CWD"' in invocation
    assert '"${HOME}"' not in invocation
    assert "copilot_hub_workspace" in script
