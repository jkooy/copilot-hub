from __future__ import annotations

import argparse
import json
import os
import tempfile
from pathlib import Path
from typing import Any


def _split_jsonc_preamble(text: str) -> tuple[str, str]:
    lines = text.splitlines(keepends=True)
    index = 0
    while index < len(lines):
        stripped = lines[index].strip()
        if stripped and not stripped.startswith("//"):
            break
        index += 1
    return "".join(lines[:index]), "".join(lines[index:])


def trust_folders(config_path: Path, folders: list[Path]) -> list[str]:
    config_path = config_path.expanduser()
    original = config_path.read_text(encoding="utf-8") if config_path.exists() else ""
    preamble, body = _split_jsonc_preamble(original)
    data: dict[str, Any] = json.loads(body) if body.strip() else {}
    trusted = data.get("trustedFolders", [])
    if not isinstance(trusted, list) or not all(isinstance(item, str) for item in trusted):
        raise ValueError("Copilot trustedFolders must be a list of strings")

    normalized = list(dict.fromkeys(str(Path(item).expanduser().resolve()) for item in trusted))
    for folder in folders:
        value = str(folder.expanduser().resolve())
        if value not in normalized:
            normalized.append(value)
    data["trustedFolders"] = normalized

    config_path.parent.mkdir(parents=True, exist_ok=True)
    mode = config_path.stat().st_mode & 0o777 if config_path.exists() else 0o600
    rendered = f"{preamble}{json.dumps(data, indent=2, ensure_ascii=False)}\n"
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
    return normalized


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--config",
        type=Path,
        default=Path("~/.copilot/config.json").expanduser(),
    )
    parser.add_argument("folders", nargs="+", type=Path)
    args = parser.parse_args()
    for folder in trust_folders(args.config, args.folders):
        print(folder)


if __name__ == "__main__":
    main()
