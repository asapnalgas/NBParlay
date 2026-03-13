#!/bin/zsh
set -euo pipefail

WORKSPACE_ROOT="/Users/josephdelallera/Documents/Playground 2"
RUNTIME_ROOT="$HOME/nba_prediction_engine_runtime"
APP_NAME="nba_model"
PLIST_NAME="com.josephdelallera.nbapredictionengine.plist"
LABEL="com.josephdelallera.nbapredictionengine"
PLIST_TARGET="$HOME/Library/LaunchAgents/$PLIST_NAME"
APP_PORT="${APP_PORT:-8010}"

mkdir -p "$HOME/Library/LaunchAgents"
mkdir -p "$RUNTIME_ROOT"

ditto "$WORKSPACE_ROOT/$APP_NAME" "$RUNTIME_ROOT/$APP_NAME"
PYTHON_VERSION=$("$WORKSPACE_ROOT/venv/bin/python" -c 'import sys; print(f"{sys.version_info.major}.{sys.version_info.minor}")')
"$WORKSPACE_ROOT/venv/bin/python" -m venv --clear "$RUNTIME_ROOT/venv"
ditto "$WORKSPACE_ROOT/venv/lib/python$PYTHON_VERSION/site-packages" "$RUNTIME_ROOT/venv/lib/python$PYTHON_VERSION/site-packages"
"$WORKSPACE_ROOT/venv/bin/python" - <<EOF
import json
from pathlib import Path
import joblib

runtime_root = Path("$RUNTIME_ROOT")
config_path = runtime_root / "$APP_NAME" / "config" / "live_sync.json"
config = json.loads(config_path.read_text(encoding="utf-8"))
data_dir = runtime_root / "$APP_NAME" / "data"
config["training_data_path"] = str(data_dir / "training_data.csv")
config["upcoming_data_path"] = str(data_dir / "upcoming_slate.csv")
config["context_updates_path"] = str(data_dir / "context_updates.csv")
config["provider_context_path"] = str(data_dir / "provider_context_updates.csv")
config_path.write_text(json.dumps(config, indent=2), encoding="utf-8")

bundle_path = runtime_root / "$APP_NAME" / "models" / "engine_bundle.joblib"
if bundle_path.exists():
    bundle = joblib.load(bundle_path)
    bundle["data_path"] = str(data_dir / "training_data.csv")
    joblib.dump(bundle, bundle_path)

metrics_path = runtime_root / "$APP_NAME" / "models" / "engine_metrics.json"
if metrics_path.exists():
    metrics = json.loads(metrics_path.read_text(encoding="utf-8"))
    metrics["training_data_path"] = str(data_dir / "training_data.csv")
    metrics_path.write_text(json.dumps(metrics, indent=2), encoding="utf-8")
EOF

ODDS_API_KEY=""
BALLDONTLIE_API_KEY=""
if [ -f "$RUNTIME_ROOT/$APP_NAME/config/providers.env" ]; then
  source "$RUNTIME_ROOT/$APP_NAME/config/providers.env"
fi

cat > "$PLIST_TARGET" <<EOF
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
  <key>Label</key>
  <string>$LABEL</string>
  <key>ProgramArguments</key>
  <array>
    <string>$RUNTIME_ROOT/venv/bin/python</string>
    <string>$RUNTIME_ROOT/$APP_NAME/app.py</string>
    <string>--host</string>
    <string>127.0.0.1</string>
    <string>--port</string>
    <string>$APP_PORT</string>
  </array>
  <key>WorkingDirectory</key>
  <string>$RUNTIME_ROOT</string>
  <key>EnvironmentVariables</key>
  <dict>
    <key>PYTHONUNBUFFERED</key>
    <string>1</string>
    <key>MPLCONFIGDIR</key>
    <string>$RUNTIME_ROOT/$APP_NAME/.matplotlib</string>
    <key>ODDS_API_KEY</key>
    <string>$ODDS_API_KEY</string>
    <key>BALLDONTLIE_API_KEY</key>
    <string>$BALLDONTLIE_API_KEY</string>
  </dict>
  <key>RunAtLoad</key>
  <true/>
  <key>KeepAlive</key>
  <true/>
  <key>ThrottleInterval</key>
  <integer>30</integer>
  <key>StandardOutPath</key>
  <string>$RUNTIME_ROOT/$APP_NAME/logs/launchd.stdout.log</string>
  <key>StandardErrorPath</key>
  <string>$RUNTIME_ROOT/$APP_NAME/logs/launchd.stderr.log</string>
</dict>
</plist>
EOF

launchctl bootout "gui/$(id -u)" "$PLIST_TARGET" >/dev/null 2>&1 || true
launchctl bootstrap "gui/$(id -u)" "$PLIST_TARGET"
launchctl kickstart -k "gui/$(id -u)/$LABEL"

echo "LaunchAgent installed: $PLIST_TARGET"
echo "Service label: $LABEL"
echo "App URL: http://127.0.0.1:$APP_PORT"
