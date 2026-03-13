#!/usr/bin/env python3
"""
Self-Learning NBA Prediction System - Modern Web UI

Provides a clean, intuitive interface to monitor and control the learning system.
Built with Flask and modern web technologies.
"""

from __future__ import annotations

import json
import logging
import threading
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List

from flask import Flask, render_template, jsonify, request
import pandas as pd

try:
    from src.self_learning_integration import get_self_learning_system
    from src.features import DEFAULT_PROJECT_DIR
except ImportError:
    from self_learning_integration import get_self_learning_system
    from features import DEFAULT_PROJECT_DIR

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Create Flask app
app = Flask(__name__, template_folder="templates", static_folder="static")
app.config['JSON_SORT_KEYS'] = False
app.config['MAX_CONTENT_LENGTH'] = 50 * 1024 * 1024  # 50MB max

# System reference
system = None

def init_system():
    """Initialize the learning system"""
    global system
    try:
        system = get_self_learning_system()
        logger.info("✓ Self-learning system initialized")
    except Exception as e:
        logger.error(f"✗ Failed to initialize system: {e}")
        system = None


@app.route('/')
def index():
    """Main dashboard page"""
    return render_template('dashboard.html')


@app.route('/api/status')
def get_status():
    """Get current system status"""
    try:
        if not system:
            return jsonify({"error": "System not initialized"}), 500
        
        status = system.orchestrator.get_status() if system.orchestrator else {}
        
        return jsonify({
            "status": "success",
            "timestamp": datetime.now().isoformat(),
            "system": {
                "is_running": system.orchestrator.is_running if system.orchestrator else False,
                "learning_phase": system.orchestrator.state.get("learning_phase") if system.orchestrator else None,
            },
            "orchestrator": status,
            "brain": system.cloud_brain.get_brain_summary() if system.cloud_brain else {},
        })
    except Exception as e:
        logger.error(f"Error getting status: {e}")
        return jsonify({"error": str(e)}), 500


@app.route('/api/metrics')
def get_metrics():
    """Get latest metrics"""
    try:
        metrics_path = DEFAULT_PROJECT_DIR / "data" / "self_learning" / "monitoring" / "metrics_log.jsonl"
        
        metrics = []
        if metrics_path.exists():
            with open(metrics_path, 'r') as f:
                for line in f.readlines()[-100:]:  # Last 100 metrics
                    try:
                        metrics.append(json.loads(line))
                    except:
                        pass
        
        # Calculate stats
        if metrics:
            accuracies = [m.get('accuracy', 0) for m in metrics if 'accuracy' in m]
            return jsonify({
                "status": "success",
                "metrics": metrics[-32:],  # Return last 32 for chart
                "count": len(metrics),
                "current_accuracy": accuracies[-1] if accuracies else 0,
                "average_accuracy": sum(accuracies) / len(accuracies) if accuracies else 0,
            })
        
        return jsonify({
            "status": "success",
            "metrics": [],
            "count": 0,
            "current_accuracy": 0,
            "average_accuracy": 0,
        })
    except Exception as e:
        logger.error(f"Error getting metrics: {e}")
        return jsonify({"error": str(e)}), 500


@app.route('/api/learning-progress')
def get_learning_progress():
    """Get learning progress data"""
    try:
        if not system:
            return jsonify({"error": "System not initialized"}), 500
        
        brain = system.cloud_brain
        timestamp = datetime.now()
        
        # Get error analysis
        errors = brain.analyze_errors()
        
        return jsonify({
            "status": "success",
            "timestamp": timestamp.isoformat(),
            "predictions_total": len(brain.prediction_records),
            "predictions_completed": sum(1 for r in brain.prediction_records if r.actual_result_timestamp),
            "starter_accuracy": errors.get("starter_prediction_accuracy", 0),
            "error_by_stat": {k: v.get("mae", 0) if isinstance(v, dict) else 0 
                             for k, v in errors.get("by_stat", {}).items()},
            "high_error_predictions": len(errors.get("high_error_predictions", [])),
            "recommendations": brain.get_improvement_recommendations(),
        })
    except Exception as e:
        logger.error(f"Error getting learning progress: {e}")
        return jsonify({"error": str(e)}), 500


@app.route('/api/controls/start', methods=['POST'])
def start_system():
    """Start the learning daemon"""
    try:
        if system and system.orchestrator:
            if not system.orchestrator.is_running:
                system.orchestrator.start()
                return jsonify({"status": "success", "message": "✓ System started"}), 200
            else:
                return jsonify({"status": "info", "message": "System already running"}), 200
        return jsonify({"error": "System not initialized"}), 500
    except Exception as e:
        logger.error(f"Error starting system: {e}")
        return jsonify({"error": str(e)}), 500


@app.route('/api/controls/stop', methods=['POST'])
def stop_system():
    """Stop the learning daemon"""
    try:
        if system and system.orchestrator:
            if system.orchestrator.is_running:
                system.orchestrator.stop()
                return jsonify({"status": "success", "message": "✓ System stopped"}), 200
            else:
                return jsonify({"status": "info", "message": "System already stopped"}), 200
        return jsonify({"error": "System not initialized"}), 500
    except Exception as e:
        logger.error(f"Error stopping system: {e}")
        return jsonify({"error": str(e)}), 500


@app.route('/api/controls/backtest', methods=['POST'])
def run_backtest():
    """Run backtest cycle"""
    try:
        if system and system.self_learner:
            def backtest_thread():
                try:
                    system.self_learner.backtest_historical_games()
                    logger.info("✓ Backtest cycle completed")
                except Exception as e:
                    logger.error(f"✗ Backtest error: {e}")
            
            thread = threading.Thread(target=backtest_thread, daemon=True)
            thread.start()
            
            return jsonify({"status": "success", "message": "✓ Backtest started"}), 200
        return jsonify({"error": "System not initialized"}), 500
    except Exception as e:
        logger.error(f"Error running backtest: {e}")
        return jsonify({"error": str(e)}), 500


@app.route('/api/controls/learn', methods=['POST'])
def run_learning():
    """Run learning cycle"""
    try:
        if system and system.self_learner:
            def learning_thread():
                try:
                    system.self_learner.run_full_historical_learning()
                    logger.info("✓ Learning cycle completed")
                except Exception as e:
                    logger.error(f"✗ Learning error: {e}")
            
            thread = threading.Thread(target=learning_thread, daemon=True)
            thread.start()
            
            return jsonify({"status": "success", "message": "✓ Learning started"}), 200
        return jsonify({"error": "System not initialized"}), 500
    except Exception as e:
        logger.error(f"Error running learning: {e}")
        return jsonify({"error": str(e)}), 500


@app.route('/api/brain-state')
def get_brain_state():
    """Get cloud brain state"""
    try:
        if not system:
            return jsonify({"error": "System not initialized"}), 500
        
        brain = system.cloud_brain
        return jsonify({
            "status": "success",
            "timestamp": datetime.now().isoformat(),
            "state": brain.state,
            "prediction_log_count": len(brain.prediction_records),
        })
    except Exception as e:
        logger.error(f"Error getting brain state: {e}")
        return jsonify({"error": str(e)}), 500


@app.route('/api/system-info')
def get_system_info():
    """Get system information"""
    try:
        return jsonify({
            "status": "success",
            "timestamp": datetime.now().isoformat(),
            "version": "1.0.0",
            "components": {
                "depth_chart_features": "✓ Enabled",
                "cloud_brain": "✓ Enabled",
                "simulation_engine": "✓ Enabled",
                "self_learner": "✓ Enabled",
                "continuous_learning": "✓ Enabled",
                "monitoring": "✓ Enabled",
                "depth_chart_integration": "✓ Active",
            },
            "data_paths": {
                "training_data": str(DEFAULT_PROJECT_DIR / "data" / "training_data.csv"),
                "cloud_brain": str(DEFAULT_PROJECT_DIR / "data" / "cloud_brain"),
                "metrics": str(DEFAULT_PROJECT_DIR / "data" / "self_learning" / "monitoring"),
            }
        })
    except Exception as e:
        logger.error(f"Error getting system info: {e}")
        return jsonify({"error": str(e)}), 500


@app.errorhandler(404)
def not_found(error):
    """Handle 404 errors"""
    return jsonify({"error": "Not found"}), 404


@app.errorhandler(500)
def server_error(error):
    """Handle 500 errors"""
    logger.error(f"Server error: {error}")
    return jsonify({"error": "Internal server error"}), 500


if __name__ == '__main__':
    # Initialize system
    logger.info("=" * 60)
    logger.info("NBA SELF-LEARNING PREDICTION SYSTEM - WEB UI")
    logger.info("=" * 60)
    
    init_system()
    
    # Start Flask app
    logger.info("\n🎯 LAUNCHING WEB SERVER")
    logger.info("📱 Open in Safari/Browser: http://localhost:5000")
    logger.info("=" * 60 + "\n")
    
    try:
        app.run(host='0.0.0.0', port=8000, debug=False, use_reloader=False, threaded=True)
    except Exception as e:
        logger.error(f"Failed to start web server: {e}")
        raise
