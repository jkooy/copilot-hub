#!/usr/bin/env bash
set -u

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
source "$ROOT/scripts/environment.sh"

HOST="${COPILOT_HUB_HOST:-127.0.0.1}"
PORT="${COPILOT_HUB_PORT:-8767}"
FAILED=0

check_command() {
  if command -v "$1" >/dev/null 2>&1; then
    echo "ok   $1: $(command -v "$1")"
  else
    echo "FAIL $1 is not installed"
    FAILED=1
  fi
}

check_command uv
check_command copilot
check_command curl

if [[ -x "$ROOT/.venv/bin/copilot-hub" ]]; then
  echo "ok   Python environment exists"
else
  echo "FAIL Python environment is missing; run ./scripts/setup.sh"
  FAILED=1
fi

if [[ -L "${HOME}/.copilot/skills/copilot-hub" ]] \
  && [[ -f "${HOME}/.copilot/skills/copilot-hub/SKILL.md" ]]; then
  echo "ok   skill link: copilot-hub"
else
  echo "FAIL missing copilot-hub skill link"
  FAILED=1
fi

if curl --connect-timeout 1 --max-time 5 -fsS \
  "http://${HOST}:${PORT}/api/agents/state" >/dev/null 2>&1; then
  echo "ok   Hub is responding at http://${HOST}:${PORT}/"
else
  echo "WARN Hub is not responding at http://${HOST}:${PORT}/"
fi

exit "$FAILED"
