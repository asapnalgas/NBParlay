#!/usr/bin/env python3
"""
Self-Learning System Setup & Launcher

This script sets up and launches the complete self-learning NBA prediction system.
It handles:
1. System initialization
2. Starting continuous learning
3. Monitoring and logging
4. Graceful shutdown
"""

import argparse
import json
import logging
import signal
import sys
import time
from datetime import datetime
from pathlib import Path

try:
    from src.self_learning_integration import (
        get_self_learning_system,
        initialize_self_learning_system,
        integrate_with_existing_app,
    )
    from src.features import DEFAULT_PROJECT_DIR
except ImportError:
    from self_learning_integration import (
        get_self_learning_system,
        initialize_self_learning_system,
        integrate_with_existing_app,
    )
    from features import DEFAULT_PROJECT_DIR


# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - [%(levelname)s] - %(name)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(DEFAULT_PROJECT_DIR / "logs" / "self_learning.log"),
    ]
)
logger = logging.getLogger(__name__)

# Ensure logs directory exists
(DEFAULT_PROJECT_DIR / "logs").mkdir(parents=True, exist_ok=True)

# Global system instance for signal handling
_system = None


def signal_handler(signum, frame):
    """Handle shutdown signals gracefully"""
    logger.info(f"Received signal {signum}, shutting down gracefully...")
    if _system:
        _system.stop_learning()
    sys.exit(0)


def main():
    """Main entry point for self-learning system"""
    global _system
    
    parser = argparse.ArgumentParser(
        description="NBA Self-Learning Prediction System",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Start continuous learning in background
  python self_learning_launcher.py --mode hybrid
  
  # Run historical learning until target accuracy
  python self_learning_launcher.py --mode learning-only --target 0.95
  
  # Check system status
  python self_learning_launcher.py --status
  
  # Generate forward projections
  python self_learning_launcher.py --generate-projections
  
  # Run single backtest cycle
  python self_learning_launcher.py --backtest
        """
    )
    
    parser.add_argument(
        "--mode",
        choices=["hybrid", "learning-only", "production"],
        help="Running mode for the system",
    )
    
    parser.add_argument(
        "--target",
        type=float,
        default=0.95,
        help="Target accuracy for historical learning (default: 0.95)",
    )
    
    parser.add_argument(
        "--status",
        action="store_true",
        help="Show current system status",
    )
    
    parser.add_argument(
        "--backtest",
        action="store_true",
        help="Run a single backtest cycle",
    )
    
    parser.add_argument(
        "--generate-projections",
        action="store_true",
        help="Generate forward projections",
    )
    
    parser.add_argument(
        "--report",
        action="store_true",
        help="Generate and show learning report",
    )
    
    parser.add_argument(
        "--sync",
        action="store_true",
        help="Manually sync ESPN data",
    )
    
    parser.add_argument(
        "--analyze",
        action="store_true",
        help="Analyze improvements and show recommendations",
    )
    
    parser.add_argument(
        "--daemon",
        action="store_true",
        help="Run as daemon (background process)",
    )
    
    args = parser.parse_args()
    
    logger.info("=" * 80)
    logger.info("NBA SELF-LEARNING PREDICTION SYSTEM LAUNCHER")
    logger.info("=" * 80)
    
    # Initialize system
    logger.info("Initializing self-learning system...")
    _system = initialize_self_learning_system()
    logger.info("✓ System initialized")
    
    # Set up signal handlers for graceful shutdown
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    # Handle different command modes
    if args.mode:
        logger.info(f"Starting system in {args.mode} mode...")
        result = integrate_with_existing_app(app_mode=args.mode)
        logger.info(f"Result: {json.dumps(result, indent=2)}")
        
        if args.daemon:
            logger.info("Running as daemon, press Ctrl+C to stop...")
            try:
                while True:
                    time.sleep(1)
            except KeyboardInterrupt:
                logger.info("Shutting down...")
                _system.stop_learning()
        else:
            # Give user time to see output before returning
            time.sleep(2)
    
    elif args.status:
        logger.info("Checking system status...")
        status = _system.get_system_status()
        logger.info(json.dumps(status, indent=2, default=str))
    
    elif args.backtest:
        logger.info("Running backtest cycle...")
        result = _system.run_backtest_cycle()
        logger.info(f"Backtest result: {json.dumps(result, indent=2, default=str)}")
    
    elif args.generate_projections:
        logger.info("Generating forward projections...")
        projections = _system.generate_forward_projections()
        logger.info(f"Generated projections for {projections['summary']['total_games']} games")
        logger.info(f"Total predictions: {projections['summary']['total_predictions']}")
    
    elif args.report:
        logger.info("Generating learning report...")
        report = _system.get_learning_report()
        
        # Print key metrics
        state = report["orchestrator_state"]
        logger.info("\n" + "=" * 80)
        logger.info("LEARNING REPORT SUMMARY")
        logger.info("=" * 80)
        logger.info(f"Learning Phase: {state['learning_phase']}")
        logger.info(f"Total Runs: {state['total_runs']}")
        logger.info(f"Failed Runs: {state['failed_runs']}")
        logger.info(f"Last Sync: {state['last_sync']}")
        logger.info(f"Last Backtest: {state['last_backtest']}")
        logger.info(f"Last Learning Cycle: {state['last_learning_cycle']}")
        logger.info(f"Last Projection Update: {state['last_projection_update']}")
        
        # Print recent events
        logger.info("\nRecent Events:")
        for event in report["recent_events"][-10:]:
            logger.info(f"  [{event['event_type']}] {event['message']}")
    
    elif args.sync:
        logger.info("Syncing ESPN data...")
        result = _system.sync_espn_data()
        logger.info(f"Sync result: {result['message']}")
    
    elif args.analyze:
        logger.info("Analyzing improvements...")
        analysis = _system.analyze_improvements()
        
        logger.info("\n" + "=" * 80)
        logger.info("IMPROVEMENT ANALYSIS")
        logger.info("=" * 80)
        
        recommendations = analysis.get("recommendations", [])
        if recommendations:
            logger.info(f"\nFound {len(recommendations)} improvement opportunities:")
            for rec in recommendations:
                priority = rec.get("priority", "medium").upper()
                logger.info(f"  [{priority}] {rec['reason']}")
                logger.info(f"           Action: {rec['action']}")
        else:
            logger.info("No immediate improvement recommendations")
        
        # Show error analysis
        errors = analysis.get("error_analysis", {})
        logger.info(f"\nError Analysis:")
        logger.info(f"  Starter Prediction Accuracy: {errors.get('starter_prediction_accuracy', 0):.2%}")
        for stat, metrics in errors.get("by_stat", {}).items():
            if isinstance(metrics, dict):
                logger.info(f"  {stat.upper()}: MAE={metrics.get('mae', 0):.2f}")
    
    else:
        # Default: show help and status
        parser.print_help()
        logger.info("\n" + "=" * 80)
        logger.info("CURRENT SYSTEM STATUS")
        logger.info("=" * 80)
        status = _system.get_system_status()
        logger.info(f"Running: {status['is_running']}")
        logger.info(f"Learning Phase: {status['state']['learning_phase']}")
        logger.info(f"Total Runs: {status['state']['total_runs']}")
        
        logger.info("\nTo start the system, use:")
        logger.info("  python self_learning_launcher.py --mode hybrid --daemon")
        logger.info("\nFor more options, run:")
        logger.info("  python self_learning_launcher.py --help")


if __name__ == "__main__":
    main()
