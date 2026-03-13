#!/bin/zsh
set -euo pipefail

LABEL="com.josephdelallera.nbapredictionengine"
PLIST_TARGET="$HOME/Library/LaunchAgents/${LABEL}.plist"
PROJECT_ROOT="/Users/josephdelallera/Documents/Playground 2"
APP_PID_FILE="$PROJECT_ROOT/nba_model/app.pid"
RUNTIME_PID_FILE="$HOME/nba_prediction_engine_runtime/nba_model/app.pid"

echo "Stopping user LaunchAgent: $LABEL"
launchctl bootout "gui/$(id -u)/$LABEL" >/dev/null 2>&1 || true
launchctl bootout "gui/$(id -u)" "$PLIST_TARGET" >/dev/null 2>&1 || true

for pid_file in "$APP_PID_FILE" "$RUNTIME_PID_FILE"; do
  if [ -f "$pid_file" ]; then
    pid="$(cat "$pid_file" 2>/dev/null || true)"
    if [ -n "${pid:-}" ] && kill -0 "$pid" >/dev/null 2>&1; then
      kill "$pid" >/dev/null 2>&1 || true
    fi
    rm -f "$pid_file"
  fi
done

pkill -f "nba_model/app.py --host 127.0.0.1 --port 8010" >/dev/null 2>&1 || true

echo "Stopped. You can restart with:"
echo "  /Users/josephdelallera/Documents/Playground 2/nba_model/scripts/install_launch_agent.sh"
