#!/usr/bin/env bash
set -euo pipefail

STATE_DIR="${COPILOT_HUB_STATE_DIR:-${HOME}/.copilot-hub}"
PID_FILE="$STATE_DIR/server.pid"

hub_process_signature() {
  local pid="$1"
  local command
  [[ "$pid" =~ ^[1-9][0-9]*$ ]] || return 1
  command="$(ps -p "$pid" -o command= 2>/dev/null)" || return 1
  if [[ "$command" != *"copilot-hub serve"* ]] \
    && [[ "$command" != *"-m uvicorn copilot_hub.app:app"* ]]; then
    return 1
  fi
  ps -p "$pid" -o lstart= -o command= 2>/dev/null
}

if [[ -n "${COPILOT_HUB_WORKER_ID:-}" ]]; then
  echo "Refusing to stop the Hub synchronously from a Hub-owned terminal." >&2
  echo "Use: ./scripts/restart.sh" >&2
  exit 2
fi

if [[ ! -f "$PID_FILE" ]]; then
  echo "Copilot Hub is not running."
  exit 0
fi

PID="$(tr -d '[:space:]' <"$PID_FILE")"
SIGNATURE="$(hub_process_signature "$PID" || true)"
if [[ -z "$SIGNATURE" ]]; then
  rm -f "$PID_FILE"
  echo "Removed stale Copilot Hub PID file; no owned server was running."
  exit 0
fi

kill "$PID"
for _ in {1..20}; do
  [[ "$(hub_process_signature "$PID" || true)" != "$SIGNATURE" ]] && break
  sleep 0.25
done
if [[ "$(hub_process_signature "$PID" || true)" == "$SIGNATURE" ]]; then
  kill -KILL "$PID"
  for _ in {1..20}; do
    [[ "$(hub_process_signature "$PID" || true)" != "$SIGNATURE" ]] && break
    sleep 0.1
  done
fi
if [[ "$(hub_process_signature "$PID" || true)" == "$SIGNATURE" ]]; then
  echo "Copilot Hub did not stop; retaining $PID_FILE for safe retry." >&2
  exit 1
fi

rm -f "$PID_FILE"
echo "Copilot Hub stopped."
