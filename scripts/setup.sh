#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
source "$ROOT/scripts/environment.sh"

for command in uv copilot; do
  if ! command -v "$command" >/dev/null 2>&1; then
    echo "Missing required command: $command" >&2
    exit 1
  fi
done

cd "$ROOT"
MANAGED_CWD="${COPILOT_HUB_CWD:-${HOME}/copilot_hub_workspace}"
mkdir -p "$MANAGED_CWD"
uv sync --dev
uv run copilot-hub init >/dev/null
"$ROOT/.venv/bin/python" -m copilot_hub.copilot_config \
  "$ROOT" \
  "$MANAGED_CWD" \
  >/dev/null

SKILL_SOURCE="$ROOT/skills/copilot-hub"
SKILL_DEST="${HOME}/.copilot/skills/copilot-hub"
mkdir -p "${HOME}/.copilot/skills"
if [[ -e "$SKILL_DEST" && ! -L "$SKILL_DEST" ]]; then
  if diff -qr "$SKILL_SOURCE" "$SKILL_DEST" >/dev/null; then
    rm -rf "$SKILL_DEST"
  else
    echo "Refusing to replace modified skill directory: $SKILL_DEST" >&2
    exit 1
  fi
fi
ln -sfn "$SKILL_SOURCE" "$SKILL_DEST"

echo "Copilot Hub is installed."
echo "Start it with: $ROOT/scripts/start.sh"
