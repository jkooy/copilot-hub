#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
source "$ROOT/scripts/environment.sh"

cd "$ROOT"
if [[ -x "$ROOT/.venv/bin/copilot-hub" ]]; then
  exec "$ROOT/.venv/bin/copilot-hub" restart
fi
exec "$(command -v uv)" run copilot-hub restart
