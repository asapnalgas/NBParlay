#!/bin/bash
# Quick Start Script for Self-Learning System
# Run this file to start the complete system

cd /Users/josephdelallera/Desktop/NBParlay

# Activate virtual environment
echo "🔧 Activating Python environment..."
source venv311/bin/activate

cd nba_model

# Create required directories
echo "📁 Creating directories..."
mkdir -p logs data/self_learning/monitoring data/self_learning/orchestrator

# Start the learning daemon
echo "🚀 Starting continuous learning daemon..."
python self_learning_launcher.py --mode hybrid --daemon > learning_daemon.log 2>&1 &
DAEMON_PID=$!
echo "   ✓ Daemon started (PID: $DAEMON_PID)"

# Give daemon time to initialize
sleep 5

# Start the monitoring agent
echo "📊 Starting monitoring agent..."
nohup python monitor.py --interval 60 > monitoring_daemon.log 2>&1 &
MONITOR_PID=$!
echo "   ✓ Monitor started (PID: $MONITOR_PID)"

# Show system status
echo ""
echo "════════════════════════════════════════════"
echo "   Self-Learning System Started! 🎉"
echo "════════════════════════════════════════════"
echo ""
echo "Active Processes:"
echo "  Learning Daemon (PID: $DAEMON_PID) - learning_daemon.log"
echo "  Monitoring Agent (PID: $MONITOR_PID) - monitoring_daemon.log"
echo ""
echo "Quick Commands:"
echo "  Status:      python self_learning_launcher.py --status"
echo "  Backtest:    python self_learning_launcher.py --backtest"
echo "  Report:      python self_learning_launcher.py --report"
echo "  Check once:  python monitor.py --check-once"
echo ""
echo "Log Files:"
echo "  Learning:    tail -f learning_daemon.log"
echo "  Monitoring:  tail -f monitoring_daemon.log"
echo "  Metrics:     data/self_learning/monitoring/metrics_log.jsonl"
echo ""
echo "⏰ Continuous cycles run at:"
echo "  • Data Sync:    Every 10 minutes"
echo "  • Backtest:     Every 1 hour"
echo "  • Learning:     Every 4 hours"
echo "  • Projections:  Every 24 hours"
echo "  • Optimization: Every 12 hours"
echo ""
echo "💡 The system is now learning continuously!"
echo "   Check monitoring_daemon.log for real-time status updates."
echo ""
