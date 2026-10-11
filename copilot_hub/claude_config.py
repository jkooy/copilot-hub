from __future__ import annotations

import argparse
import json
import os
import tempfile
from pathlib import Path
from typing import Any


def default_config_path() -> Path:
    config_dir = os.environ.get("CLAUDE_CONFIG_DIR")
    if config_dir:
        return Path(config_dir).expanduser() / ".claude.json"
    return Path("~/.claude.json").expanduser()


def trust_folders(config_path: Path, folders: list[Path]) -> list[str]:
    """Pre-accept Claude Code's folder trust dialog for exactly these folders."""
    config_path = config_path.expanduser()
    original = config_path.read_text(encoding="utf-8") if config_path.exists() else ""
    data: dict[str, Any] = json.loads(original) if original.strip() else {}
    projects = data.setdefault("projects", {})
    if not isinstance(projects, dict) or not all(
        isinstance(item, dict) for item in projects.values()
    ):
        raise ValueError("Claude projects must be an object of objects")

    trusted: list[str] = []
    for folder in folders:
        value = str(folder.expanduser().resolve())
        projects.setdefault(value, {})["hasTrustDialogAccepted"] = True
        if value not in trusted:
            trusted.append(value)

    config_path.parent.mkdir(parents=True, exist_ok=True)
    mode = config_path.stat().st_mode & 0o777 if config_path.exists() else 0o600
    rendered = f"{json.dumps(data, indent=2, ensure_ascii=False)}\n"
    fd, temporary_name = tempfile.mkstemp(
        prefix=f".{config_path.name}.",
        dir=config_path.parent,
    )
    temporary_path = Path(temporary_name)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as handle:
            handle.write(rendered)
            handle.flush()
            os.fsync(handle.fileno())
        os.chmod(temporary_path, mode)
        os.replace(temporary_path, config_path)
    finally:
        temporary_path.unlink(missing_ok=True)
    return trusted


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", type=Path, default=default_config_path())
    parser.add_argument("folders", nargs="+", type=Path)
    args = parser.parse_args()
    for folder in trust_folders(args.config, args.folders):
        print(folder)


if __name__ == "__main__":
    main()
