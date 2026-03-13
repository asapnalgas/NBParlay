#!/usr/bin/env python3
"""
Continuous Learning Monitor: Watches system health, accuracy, and resolves issues

This agent:
- Monitors system status every minute
- Tracks accuracy improvements across learning cycles
- Detects and alerts on performance issues
- Logs comprehensive metrics
- Auto-detects and suggests fixes for common problems
"""

from __future__ import annotations

import json
import logging
import sys
import time
import subprocess
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
from enum import Enum

import pandas as pd
import numpy as np

try:
    from src.self_learning_integration import get_self_learning_system
except ImportError:
    from self_learning_integration import get_self_learning_system

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('monitoring.log'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)


class SystemHealthStatus(Enum):
    """Health status indicators"""
    HEALTHY = "healthy"
    WARNING = "warning"
    CRITICAL = "critical"
    ERROR = "error"


class PerformanceMetricsTracker:
    """Tracks system performance metrics over time"""
    
    def __init__(self, metrics_dir: Optional[Path] = None):
        """Initialize metrics tracker"""
        self.metrics_dir = metrics_dir or Path("data/self_learning/monitoring")
        self.metrics_dir.mkdir(parents=True, exist_ok=True)
        self.metrics_path = self.metrics_dir / "metrics_log.jsonl"
        self.metrics_history: List[Dict[str, Any]] = []
        
    def record_checkpoint(self, metrics: Dict[str, Any]) -> None:
        """Record a checkpoint of system metrics"""
        checkpoint = {
            "timestamp": datetime.now().isoformat(),
            **metrics
        }
        
        self.metrics_history.append(checkpoint)
        
        # Append to JSONL log
        with open(self.metrics_path, 'a') as f:
            f.write(json.dumps(checkpoint) + '\n')
    
    def get_recent_metrics(self, minutes: int = 60) -> List[Dict[str, Any]]:
        """Get metrics from the last N minutes"""
        cutoff = datetime.now() - timedelta(minutes=minutes)
        
        recent = []
        if self.metrics_path.exists():
            with open(self.metrics_path, 'r') as f:
                for line in f:
                    try:
                        record = json.loads(line)
                        record_time = datetime.fromisoformat(record['timestamp'])
                        if record_time >= cutoff:
                            recent.append(record)
                    except (json.JSONDecodeError, ValueError):
                        continue
        
        return recent
    
    def calculate_trending(self, metric_name: str, minutes: int = 60) -> Dict[str, float]:
        """Calculate trending for a metric"""
        recent = self.get_recent_metrics(minutes)
        
        if not recent:
            return {"trend": None, "current": None, "change": None}
        
        values = [m.get(metric_name) for m in recent if m.get(metric_name) is not None]
        
        if len(values) < 2:
            return {"trend": None, "current": values[-1] if values else None, "change": None}
        
        current = values[-1]
        previous = values[0]
        change = current - previous if previous else 0
        
        trend = "improving" if change > 0 else ("declining" if change < 0 else "stable")
        
        return {
            "trend": trend,
            "current": current,
            "change": change,
            "percent_change": (change / previous * 100) if previous and previous != 0 else None,
        }


class SystemHealthMonitor:
    """Monitors overall system health and detects issues"""
    
    def __init__(self):
        """Initialize health monitor"""
        self.metrics_tracker = PerformanceMetricsTracker()
        self.check_history: List[Dict[str, Any]] = []
        self.alerts: List[Dict[str, Any]] = []
        
        # Thresholds for alerts
        self.thresholds = {
            "starter_accuracy_min": 0.55,  # Alert if below 55%
            "stat_mae_max": 3.0,  # Alert if mean absolute error > 3
            "error_rate_max": 0.05,  # Alert if errors exceed 5%
            "response_time_max": 30,  # Alert if checks take > 30s
            "restart_needed": False,
        }
    
    def check_system_health(self) -> Dict[str, Any]:
        """
        Run comprehensive health check
        
        Returns:
            Health check results with status and details
        """
        check_time = datetime.now()
        logger.info("=" * 60)
        logger.info("SYSTEM HEALTH CHECK")
        logger.info("=" * 60)
        
        start_time = time.time()
        health_results = {
            "timestamp": check_time.isoformat(),
            "status": SystemHealthStatus.HEALTHY.value,
            "checks": {},
            "warnings": [],
            "recommendations": [],
        }
        
        try:
            system = get_self_learning_system()
            
            # Check 1: System running status
            is_running = system.orchestrator.is_running if system.orchestrator else False
            health_results["checks"]["system_running"] = {
                "status": "ok" if is_running else "warning",
                "value": is_running,
                "message": "System is running" if is_running else "System is not currently running",
            }
            
            if not is_running:
                health_results["warnings"].append("System is not running - learning is paused")
            
            # Check 2: Cloud Brain state
            brain_summary = system.cloud_brain.get_brain_summary()
            prediction_count = brain_summary.get("prediction_count", 0)
            completed_predictions = brain_summary.get("completed_predictions", 0)
            
            health_results["checks"]["predictions"] = {
                "status": "ok" if prediction_count > 0 else "warning",
                "total_predictions": prediction_count,
                "completed": completed_predictions,
                "pending": prediction_count - completed_predictions,
            }
            
            # Check 3: Accuracy metrics
            error_summary = brain_summary.get("error_summary", {})
            starter_accuracy = error_summary.get("starter_accuracy", 0.0)
            
            accuracy_status = "ok" if starter_accuracy >= self.thresholds["starter_accuracy_min"] else "warning"
            health_results["checks"]["accuracy"] = {
                "status": accuracy_status,
                "starter_accuracy": round(starter_accuracy * 100, 2),
                "target": self.thresholds["starter_accuracy_min"] * 100,
            }
            
            if starter_accuracy < self.thresholds["starter_accuracy_min"]:
                health_results["warnings"].append(
                    f"Starter accuracy ({starter_accuracy:.1%}) below target ({self.thresholds['starter_accuracy_min']:.1%})"
                )
            
            # Check 4: Error analysis
            by_stat = error_summary.get("by_stat", {})
            high_errors = []
            for stat, metrics in by_stat.items():
                if isinstance(metrics, dict):
                    mae = metrics.get("mae", 0)
                    if mae > self.thresholds["stat_mae_max"]:
                        high_errors.append(f"{stat} (MAE: {mae:.2f})")
            
            if high_errors:
                health_results["checks"]["errors"] = {
                    "status": "warning",
                    "high_error_stats": high_errors,
                }
                health_results["warnings"].append(f"High prediction errors: {', '.join(high_errors)}")
            else:
                health_results["checks"]["errors"] = {
                    "status": "ok",
                    "message": "Error rates within acceptable ranges",
                }
            
            # Check 5: Recommendations from brain
            recommendations = brain_summary.get("improvement_recommendations", [])
            if recommendations:
                health_results["checks"]["recommendations"] = {
                    "count": len(recommendations),
                    "items": [r.get("reason") for r in recommendations],
                }
                health_results["recommendations"].extend(recommendations)
            
            # Check 6: Learning session activity
            learning_phase = system.orchestrator.state.get("learning_phase") if system.orchestrator else None
            health_results["checks"]["learning_phase"] = {
                "current_phase": learning_phase,
                "message": f"Currently in {learning_phase} phase" if learning_phase else "No active phase",
            }
            
            # Determine overall status
            if health_results["warnings"]:
                health_results["status"] = SystemHealthStatus.WARNING.value
            
            if high_errors or starter_accuracy < 0.5:
                health_results["status"] = SystemHealthStatus.CRITICAL.value
        
        except Exception as e:
            logger.exception(f"Error during health check: {e}")
            health_results["status"] = SystemHealthStatus.ERROR.value
            health_results["checks"]["error"] = {
                "status": "error",
                "message": str(e),
            }
        
        # Record check time
        check_duration = time.time() - start_time
        health_results["check_duration_seconds"] = check_duration
        
        # Log results
        self._log_health_check(health_results)
        
        # Record metrics
        metrics_to_record = {
            "starter_accuracy": health_results["checks"].get("accuracy", {}).get("starter_accuracy", 0),
            "total_predictions": health_results["checks"].get("predictions", {}).get("total_predictions", 0),
            "system_health_status": health_results["status"],
            "check_duration_seconds": check_duration,
        }
        self.metrics_tracker.record_checkpoint(metrics_to_record)
        
        self.check_history.append(health_results)
        
        return health_results
    
    def _log_health_check(self, results: Dict[str, Any]) -> None:
        """Log health check results in readable format"""
        status = results["status"].upper()
        logger.info(f"Status: {status}")
        
        for check_name, check_result in results.get("checks", {}).items():
            if isinstance(check_result, dict):
                logger.info(f"  {check_name}: {check_result.get('status', 'unknown')}")
                if "value" in check_result:
                    logger.info(f"    -> {check_result['value']}")
                if "message" in check_result:
                    logger.info(f"    -> {check_result['message']}")
        
        for warning in results.get("warnings", []):
            logger.warning(f"  ⚠️  {warning}")
        
        for rec in results.get("recommendations", []):
            logger.info(f"  💡 {rec.get('action', rec.get('reason'))}")
        
        logger.info(f"Check completed in {results.get('check_duration_seconds', 0):.2f}s")
    
    def suggest_fixes(self, health_results: Dict[str, Any]) -> List[Dict[str, Any]]:
        """
        Suggest fixes based on health check results
        
        Returns:
            List of suggested fixes
        """
        fixes = []
        
        status = health_results.get("status")
        
        if status == SystemHealthStatus.CRITICAL.value:
            fixes.append({
                "severity": "critical",
                "issue": "System health is critical",
                "suggestion": "Recommend restarting learning system and checking configuration",
                "command": "python self_learning_launcher.py --mode hybrid --daemon",
            })
        
        if not health_results["checks"].get("system_running", {}).get("value"):
            fixes.append({
                "severity": "high",
                "issue": "System is not running",
                "suggestion": "Start the continuous learning daemon",
                "command": "python self_learning_launcher.py --mode hybrid --daemon",
            })
        
        accuracy = health_results["checks"].get("accuracy", {}).get("starter_accuracy", 0)
        if accuracy < self.thresholds["starter_accuracy_min"]:
            fixes.append({
                "severity": "medium",
                "issue": f"Starter accuracy low ({accuracy:.1%})",
                "suggestion": "Run backtest cycle to recalibrate starter prediction model",
                "command": "python self_learning_launcher.py --backtest",
            })
        
        recommendations = health_results.get("recommendations", [])
        for rec in recommendations:
            if rec.get("priority") == "high":
                fixes.append({
                    "severity": "medium",
                    "issue": rec.get("reason", "Unknown"),
                    "suggestion": rec.get("action", "Review recommendations"),
                    "command": None,
                })
        
        return fixes


class ContinuousMonitor:
    """Runs continuous monitoring loop"""
    
    def __init__(self, check_interval_seconds: int = 60):
        """Initialize continuous monitor"""
        self.check_interval = check_interval_seconds
        self.health_monitor = SystemHealthMonitor()
        self.is_running = False
        self.iteration = 0
        
    def start(self) -> None:
        """Start monitoring loop"""
        self.is_running = True
        self.iteration = 0
        
        logger.info("Starting continuous monitor...")
        logger.info(f"Check interval: {self.check_interval} seconds")
        logger.info("Press Ctrl+C to stop")
        
        try:
            while self.is_running:
                self.iteration += 1
                logger.info(f"\n--- Monitor Iteration {self.iteration} ---")
                
                # Run health check
                health = self.health_monitor.check_system_health()
                
                # Get suggested fixes if there are warnings
                if health.get("status") != SystemHealthStatus.HEALTHY.value:
                    fixes = self.health_monitor.suggest_fixes(health)
                    
                    if fixes:
                        logger.info("\nSuggested Actions:")
                        for fix in fixes:
                            logger.info(f"  [{fix['severity'].upper()}] {fix['issue']}")
                            logger.info(f"  -> {fix['suggestion']}")
                            if fix['command']:
                                logger.info(f"  -> Run: {fix['command']}")
                
                # Get trending
                trending = self.health_monitor.metrics_tracker.calculate_trending("starter_accuracy", minutes=60)
                if trending.get("trend"):
                    logger.info(f"\nAccuracy Trend (60min): {trending['trend']}")
                    logger.info(f"  Current: {trending['current']:.2%}")
                    if trending.get("percent_change"):
                        logger.info(f"  Change: {trending['percent_change']:+.1f}%")
                
                # Wait for next check
                logger.info(f"Next check in {self.check_interval} seconds...")
                time.sleep(self.check_interval)
        
        except KeyboardInterrupt:
            logger.info("\nMonitor stopped by user")
            self.stop()
        
        except Exception as e:
            logger.exception(f"Monitor error: {e}")
            self.stop()
    
    def stop(self) -> None:
        """Stop monitoring"""
        self.is_running = False
        logger.info("Monitor shutdown complete")


def main():
    """Entry point for monitor"""
    import argparse
    
    parser = argparse.ArgumentParser(description="Monitor continuous learning system health")
    parser.add_argument(
        "--interval",
        type=int,
        default=60,
        help="Check interval in seconds (default: 60)",
    )
    parser.add_argument(
        "--check-once",
        action="store_true",
        help="Run single health check and exit",
    )
    parser.add_argument(
        "--log-level",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        default="INFO",
        help="Logging level",
    )
    
    args = parser.parse_args()
    
    # Set log level
    logging.getLogger().setLevel(getattr(logging, args.log_level))
    
    if args.check_once:
        # Single check mode
        monitor = SystemHealthMonitor()
        health = monitor.check_system_health()
        fixes = monitor.suggest_fixes(health)
        
        if fixes:
            logger.info("\nSuggested Fixes:")
            for fix in fixes:
                logger.info(f"  [{fix['severity'].upper()}] {fix['issue']}")
                if fix['command']:
                    logger.info(f"  -> {fix['command']}")
    else:
        # Continuous monitoring mode
        monitor = ContinuousMonitor(check_interval_seconds=args.interval)
        monitor.start()


if __name__ == "__main__":
    main()
