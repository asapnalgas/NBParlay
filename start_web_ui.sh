#!/bin/bash

# NBA Self-Learning System - Web UI Launcher
# Starts the Flask web server for the dashboard

set -e

PROJECT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
APP_DIR="$PROJECT_DIR/nba_model"

echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "🏀 NBA SELF-LEARNING SYSTEM - WEB UI LAUNCHER"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo ""

# Activate virtual environment
echo "🔧 Activating Python environment..."
source "$PROJECT_DIR/venv311/bin/activate"
echo "✓ Environment activated"
echo ""

# Create necessary directories
echo "📁 Setting up directories..."
mkdir -p "$APP_DIR/templates"
mkdir -p "$APP_DIR/static"
mkdir -p "$APP_DIR/logs"
echo "✓ Directories ready"
echo ""

# Check if Flask is installed
echo "📦 Checking dependencies..."
python3 -c "import flask" 2>/dev/null && echo "✓ Flask found" || {
    echo "⚠️  Installing Flask..."
    pip install flask -q
    echo "✓ Flask installed"
}
echo ""

# Start the web server
cd "$APP_DIR"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "🚀 STARTING WEB SERVER"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo ""
echo "📱 Open in Safari or Browser:"
echo ""
echo "   ▶️  http://localhost:5000"
echo ""
echo "Features:"
echo "   • Real-time system monitoring"
echo "   • Learning progress tracking"
echo "   • Accuracy trends and analysis"
echo "   • System controls (start/stop/backtest/learn)"
echo "   • Performance metrics"
echo ""
echo "Press Ctrl+C to stop the server"
echo ""
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo ""

# Run the Flask app
python3 web_app.py
