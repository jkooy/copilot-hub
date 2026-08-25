#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
source "$ROOT/scripts/environment.sh"

STATE_DIR="${COPILOT_HUB_STATE_DIR:-${HOME}/.copilot-hub}"
HOST="${COPILOT_HUB_HOST:-127.0.0.1}"
PORT="${COPILOT_HUB_PORT:-8767}"
PID_FILE="$STATE_DIR/server.pid"
LOG_FILE="$STATE_DIR/server.log"

process_signature() {
  local pid="$1"
  [[ "$pid" =~ ^[1-9][0-9]*$ ]] || return 1
  ps -p "$pid" -o lstart= -o command= 2>/dev/null
}

hub_process_signature() {
  local pid="$1"
  local command
  command="$(ps -p "$pid" -o command= 2>/dev/null)" || return 1
  if [[ "$command" != *"copilot-hub serve"* ]] \
    && [[ "$command" != *"-m uvicorn copilot_hub.app:app"* ]]; then
    return 1
  fi
  process_signature "$pid"
}

health_pid() {
  curl --connect-timeout 1 --max-time 2 -fsS \
    "http://${HOST}:${PORT}/api/health" 2>/dev/null \
    | sed -n 's/.*"pid":\([0-9][0-9]*\).*/\1/p'
}

stop_owned_process() {
  local pid="$1"
  local signature="$2"
  local current
  [[ -n "$signature" ]] || return 0
  kill "$pid" 2>/dev/null || true
  for _ in {1..20}; do
    current="$(process_signature "$pid" || true)"
    [[ "$current" != "$signature" ]] && return 0
    sleep 0.25
  done
  current="$(process_signature "$pid" || true)"
  if [[ "$current" == "$signature" ]]; then
    kill -KILL "$pid" 2>/dev/null || true
  fi
  for _ in {1..20}; do
    current="$(process_signature "$pid" || true)"
    [[ "$current" != "$signature" ]] && return 0
    sleep 0.1
  done
  return 1
}

mkdir -p "$STATE_DIR"
for command in uv copilot curl; do
  if ! command -v "$command" >/dev/null 2>&1; then
    echo "Missing required command: $command" >&2
    exit 1
  fi
done
if [[ -x "$ROOT/.venv/bin/copilot-hub" ]]; then
  HUB_COMMAND=("$ROOT/.venv/bin/copilot-hub")
else
  HUB_COMMAND=(uv run copilot-hub)
fi

if [[ -f "$PID_FILE" ]]; then
  PID="$(tr -d '[:space:]' <"$PID_FILE")"
  SIGNATURE="$(hub_process_signature "$PID" || true)"
  if [[ -n "$SIGNATURE" ]]; then
    if [[ "$(health_pid)" == "$PID" ]]; then
      echo "Copilot Hub is already running at http://${HOST}:${PORT}/"
      exit 0
    fi
    echo "Copilot Hub PID $PID is running but is not responding." >&2
    echo "Refusing to start a second server; inspect $LOG_FILE." >&2
    exit 1
  fi
  rm -f "$PID_FILE"
fi

if [[ -n "$(health_pid)" ]]; then
  echo "Port ${PORT} is already occupied by another service." >&2
  echo "Set COPILOT_HUB_PORT to an unused loopback port." >&2
  exit 1
fi

cd "$ROOT"
nohup "${HUB_COMMAND[@]}" serve --host "$HOST" --port "$PORT" \
  >>"$LOG_FILE" 2>&1 &
PID=$!
echo "$PID" >"$PID_FILE"
SIGNATURE="$(hub_process_signature "$PID" || true)"

for _ in {1..30}; do
  if [[ "$(health_pid)" == "$PID" ]]; then
    echo "Copilot Hub started at http://${HOST}:${PORT}/"
    exit 0
  fi
  if [[ -z "$SIGNATURE" ]]; then
    SIGNATURE="$(hub_process_signature "$PID" || true)"
  fi
  if [[ -z "$SIGNATURE" ]] && ! kill -0 "$PID" 2>/dev/null; then
    echo "Copilot Hub exited during startup. See $LOG_FILE" >&2
    rm -f "$PID_FILE"
    exit 1
  fi
  sleep 1
done

echo "Copilot Hub did not become ready. See $LOG_FILE" >&2
if ! stop_owned_process "$PID" "$SIGNATURE"; then
  echo "The owned Hub process did not stop; retaining $PID_FILE." >&2
  exit 1
fi
rm -f "$PID_FILE"
exit 1
