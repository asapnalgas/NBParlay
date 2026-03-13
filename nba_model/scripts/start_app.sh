#!/bin/zsh
set -euo pipefail

SCRIPT_DIR=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
APP_DIR=$(CDPATH= cd -- "$SCRIPT_DIR/.." && pwd)
ROOT=$(CDPATH= cd -- "$APP_DIR/.." && pwd)
ENV_FILE="$APP_DIR/config/providers.env"
LOG_DIR="$APP_DIR/logs"
PORT="${PORT:-8010}"
HOST="${APP_HOST:-127.0.0.1}"
DAEMON="${DAEMONIZE:-0}"
PID_FILE="$APP_DIR/app.pid"
SINGLE_INSTANCE="${SINGLE_INSTANCE:-1}"
PYTHON_BIN="${PYTHON_BIN:-}"

mkdir -p "$LOG_DIR"
cd "$ROOT"

if [ -f "$ENV_FILE" ]; then
  source "$ENV_FILE"
fi

resolve_python_bin() {
  if [ -n "${PYTHON_BIN:-}" ] && [ -x "$PYTHON_BIN" ]; then
    echo "$PYTHON_BIN"
    return 0
  fi

  local candidates=(
    "$ROOT/venv311/bin/python"
    "$ROOT/venv/bin/python"
    "$(command -v python3 2>/dev/null || true)"
    "$(command -v python 2>/dev/null || true)"
  )
  local candidate=""
  for candidate in "${candidates[@]}"; do
    if [ -n "$candidate" ] && [ -x "$candidate" ]; then
      echo "$candidate"
      return 0
    fi
  done

  return 1
}

stop_existing_instances() {
  local stopped=0

  if [ -f "$PID_FILE" ]; then
    local pid_from_file
    pid_from_file="$(cat "$PID_FILE" 2>/dev/null || true)"
    if [ -n "$pid_from_file" ] && kill -0 "$pid_from_file" >/dev/null 2>&1; then
      kill "$pid_from_file" >/dev/null 2>&1 || true
      sleep 0.25
      stopped=1
    fi
  fi

  local pids
  pids="$(ps -axo pid=,command= | awk '
    tolower($0) ~ /python/ && $0 ~ /app.py --host/ { print $1 }
  ')"
  if [ -n "$pids" ]; then
    while IFS= read -r pid; do
      if [ -n "$pid" ] && [ "$pid" != "$$" ]; then
        local process_command
        process_command="$(ps -p "$pid" -o command= 2>/dev/null || true)"
        if [[ "$process_command" != *"app.py --host"* ]]; then
          continue
        fi
        local cwd_path
        cwd_path="$( (lsof -a -p "$pid" -d cwd -Fn 2>/dev/null || true) | awk 'NR==3 && substr($0,1,1)=="n" {print substr($0,2)}' )"
        if [ "$cwd_path" = "$ROOT" ] || [ "$cwd_path" = "$APP_DIR" ] || [[ "$process_command" == *"$APP_DIR/app.py"* ]]; then
          kill "$pid" >/dev/null 2>&1 || true
          stopped=1
        fi
      fi
    done <<< "$pids"
    sleep 0.25
  fi

  if [ "$stopped" -eq 1 ]; then
    rm -f "$PID_FILE"
  fi
}

find_free_port() {
  local start_port="$1"
  local candidate="$start_port"
  local attempts=0

  while [ "$attempts" -lt 10 ]; do
    if ! lsof -iTCP:"$candidate" -sTCP:LISTEN -nP >/dev/null 2>&1; then
      echo "$candidate"
      return 0
    fi
    candidate=$((candidate + 1))
    attempts=$((attempts + 1))
  done

  echo "$start_port"
}

if [ "$SINGLE_INSTANCE" = "1" ]; then
  stop_existing_instances
fi

PORT="$(find_free_port "$PORT")"
PYTHON_BIN="$(resolve_python_bin || true)"
if [ -z "$PYTHON_BIN" ]; then
  echo "Could not find a usable Python interpreter."
  echo "Set PYTHON_BIN=/absolute/path/to/python and retry."
  exit 1
fi

echo "Starting NBA prediction engine at http://${HOST}:$PORT"
echo "PID file: $PID_FILE"
echo "Log file: $LOG_DIR/app.log"
echo "Python: $PYTHON_BIN"

export PYTHONUNBUFFERED=1

if [ "$DAEMON" = "1" ]; then
  nohup "$PYTHON_BIN" "$APP_DIR/app.py" --host "$HOST" --port "$PORT" \
    </dev/null >"$LOG_DIR/app.log" 2>&1 &
  app_pid=$!
  echo "$app_pid" > "$PID_FILE"

  server_ready=0
  for _ in {1..20}; do
    if lsof -iTCP:"$PORT" -sTCP:LISTEN -nP >/dev/null 2>&1; then
      server_ready=1
      break
    fi
    sleep 0.25
  done

  if [ "$server_ready" -ne 1 ]; then
    echo "App failed to bind to port $PORT. Check $LOG_DIR/app.log for details."
    if [ -s "$LOG_DIR/app.log" ]; then
      echo "Last log output:"
      tail -n 40 "$LOG_DIR/app.log"
    else
      echo "(No log output captured.)"
    fi
    exit 1
  fi

  echo "Started in background (pid=$app_pid)."
  echo "Open: http://${HOST}:$PORT"
  exit 0
fi

exec "$PYTHON_BIN" "$APP_DIR/app.py" --host "$HOST" --port "$PORT"
