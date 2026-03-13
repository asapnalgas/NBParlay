"""
Self-Learning Orchestrator: Manages continuous learning and projections

This module:
1. Coordinates the complete self-learning pipeline
2. Manages scheduled runs of backtest, learning, and projection cycles
3. Handles continuous syncing of ESPN data
4. Updates models based on new game results
5. Provides monitoring and logging
"""

from __future__ import annotations

import json
import logging
import threading
import time
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional, Callable
from collections import deque

try:
    from .cloud_brain import CloudBrain, create_cloud_brain
    from .simulation_engine import SimulationEngine, create_simulation_engine
    from .self_learner import SelfLearner, create_self_learner
    from .live_sync import LiveSyncManager
    from .features import DEFAULT_PROJECT_DIR
except ImportError:
    from cloud_brain import CloudBrain, create_cloud_brain
    from simulation_engine import SimulationEngine, create_simulation_engine
    from self_learner import SelfLearner, create_self_learner
    from live_sync import LiveSyncManager
    from features import DEFAULT_PROJECT_DIR


logger = logging.getLogger(__name__)

DEFAULT_ORCHESTRATOR_DIR = DEFAULT_PROJECT_DIR / "data" / "orchestrator"
DEFAULT_ORCHESTRATOR_STATE_PATH = DEFAULT_ORCHESTRATOR_DIR / "orchestrator_state.json"
DEFAULT_RUN_LOG_PATH = DEFAULT_ORCHESTRATOR_DIR / "run_log.jsonl"

# Scheduling intervals (in seconds)
CONTINUOUS_SYNC_INTERVAL = 600  # 10 minutes
BACKTEST_INTERVAL = 3600  # 1 hour
LEARNING_INTERVAL = 14400  # 4 hours
PROJECTION_INTERVAL = 86400  # 24 hours
OPTIMIZATION_INTERVAL = 43200  # 12 hours


class ContinuousLearningOrchestrator:
    """
    Orchestrates the entire self-learning system:
    - Continuous ESPN data sync
    - Periodic backtesting
    - Learning cycle management
    - Forward projections
    - Model optimization
    """
    
    def __init__(self, orchestrator_dir: Optional[Path] = None):
        """Initialize orchestrator"""
        self.orchestrator_dir = orchestrator_dir or DEFAULT_ORCHESTRATOR_DIR
        self.orchestrator_dir.mkdir(parents=True, exist_ok=True)
        
        # Initialize core components
        self.cloud_brain = create_cloud_brain()
        self.simulation_engine = create_simulation_engine(self.cloud_brain)
        self.self_learner = create_self_learner()
        self.live_sync_manager = LiveSyncManager()
        
        # State tracking
        self.state_path = self.orchestrator_dir / "orchestrator_state.json"
        self.state = self._load_state()
        self.run_log_path = self.orchestrator_dir / "run_log.jsonl"
        
        # Thread management
        self.threads = {}
        self.should_stop = False
        self.is_running = False
        
        # Event tracking for monitoring
        self.event_queue = deque(maxlen=1000)  # Last 1000 events
        
        logger.info("Continuous Learning Orchestrator initialized")
    
    def _load_state(self) -> Dict[str, Any]:
        """Load or initialize orchestrator state"""
        if self.state_path.exists():
            with open(self.state_path, 'r') as f:
                return json.load(f)
        
        return {
            "created_at": datetime.now().isoformat(),
            "last_sync": None,
            "last_backtest": None,
            "last_learning_cycle": None,
            "last_projection_update": None,
            "last_optimization": None,
            "total_runs": 0,
            "failed_runs": 0,
            "is_running": False,
            "learning_phase": "historical_learning",  # historical_learning, forward_projections
            "status_messages": [],
        }
    
    def save_state(self) -> None:
        """Save orchestrator state"""
        self.state["last_updated"] = datetime.now().isoformat()
        with open(self.state_path, 'w') as f:
            json.dump(self.state, f, indent=2)
    
    def log_event(self, event_type: str, level: str, message: str, 
                  context: Optional[Dict] = None) -> None:
        """Log an event for monitoring"""
        event = {
            "timestamp": datetime.now().isoformat(),
            "event_type": event_type,
            "level": level,
            "message": message,
            "context": context or {},
        }
        
        self.event_queue.append(event)
        
        # Also write to run log
        with open(self.run_log_path, 'a') as f:
            f.write(json.dumps(event) + '\n')
        
        # Log at appropriate level
        log_func = getattr(logger, level.lower(), logger.info)
        log_func(f"[{event_type}] {message}")
    
    def start(self) -> None:
        """Start the continuous learning system"""
        if self.is_running:
            logger.warning("Orchestrator already running")
            return
        
        logger.info("Starting Continuous Learning Orchestrator")
        self.is_running = True
        self.should_stop = False
        self.state["is_running"] = True
        
        # Start background threads
        self._start_sync_thread()
        self._start_backtest_thread()
        self._start_learning_thread()
        self._start_projection_thread()
        self._start_optimization_thread()
        
        self.log_event("orchestrator_start", "info", "Continuous learning system started")
        self.save_state()
    
    def stop(self) -> None:
        """Stop all continuous learning processes"""
        logger.info("Stopping Continuous Learning Orchestrator")
        self.should_stop = True
        self.is_running = False
        self.state["is_running"] = False
        
        # Wait for threads to stop
        for thread_name, thread in self.threads.items():
            if thread and thread.is_alive():
                logger.info(f"Waiting for {thread_name} to stop")
                thread.join(timeout=10)
        
        self.log_event("orchestrator_stop", "info", "Continuous learning system stopped")
        self.save_state()
    
    def _start_sync_thread(self) -> None:
        """Start ESPN data sync thread"""
        def sync_loop():
            logger.info("ESPN sync thread started")
            while not self.should_stop:
                try:
                    self._run_sync_cycle()
                    time.sleep(CONTINUOUS_SYNC_INTERVAL)
                except Exception as e:
                    logger.error(f"Error in sync loop: {e}")
                    self.log_event("sync_error", "error", str(e))
                    self.state["failed_runs"] += 1
                    time.sleep(30)  # Wait before retry
        
        thread = threading.Thread(target=sync_loop, daemon=True, name="sync_thread")
        thread.start()
        self.threads["sync"] = thread
    
    def _start_backtest_thread(self) -> None:
        """Start backtest thread"""
        def backtest_loop():
            logger.info("Backtest thread started")
            while not self.should_stop:
                try:
                    # Only run backtest if in historical learning phase
                    if self.state["learning_phase"] == "historical_learning":
                        self._run_backtest_cycle()
                    time.sleep(BACKTEST_INTERVAL)
                except Exception as e:
                    logger.error(f"Error in backtest loop: {e}")
                    self.log_event("backtest_error", "error", str(e))
                    self.state["failed_runs"] += 1
                    time.sleep(30)
        
        thread = threading.Thread(target=backtest_loop, daemon=True, name="backtest_thread")
        thread.start()
        self.threads["backtest"] = thread
    
    def _start_learning_thread(self) -> None:
        """Start learning cycle thread"""
        def learning_loop():
            logger.info("Learning thread started")
            while not self.should_stop:
                try:
                    # Only run full learning if in historical phase
                    if self.state["learning_phase"] == "historical_learning":
                        self._run_learning_cycle()
                    time.sleep(LEARNING_INTERVAL)
                except Exception as e:
                    logger.error(f"Error in learning loop: {e}")
                    self.log_event("learning_error", "error", str(e))
                    self.state["failed_runs"] += 1
                    time.sleep(30)
        
        thread = threading.Thread(target=learning_loop, daemon=True, name="learning_thread")
        thread.start()
        self.threads["learning"] = thread
    
    def _start_projection_thread(self) -> None:
        """Start forward projection thread"""
        def projection_loop():
            logger.info("Projection thread started")
            while not self.should_stop:
                try:
                    self._run_projection_cycle()
                    time.sleep(PROJECTION_INTERVAL)
                except Exception as e:
                    logger.error(f"Error in projection loop: {e}")
                    self.log_event("projection_error", "error", str(e))
                    self.state["failed_runs"] += 1
                    time.sleep(30)
        
        thread = threading.Thread(target=projection_loop, daemon=True, name="projection_thread")
        thread.start()
        self.threads["projection"] = thread
    
    def _start_optimization_thread(self) -> None:
        """Start model optimization thread"""
        def optimization_loop():
            logger.info("Optimization thread started")
            while not self.should_stop:
                try:
                    self._run_optimization_cycle()
                    time.sleep(OPTIMIZATION_INTERVAL)
                except Exception as e:
                    logger.error(f"Error in optimization loop: {e}")
                    self.log_event("optimization_error", "error", str(e))
                    self.state["failed_runs"] += 1
                    time.sleep(30)
        
        thread = threading.Thread(target=optimization_loop, daemon=True, name="optimization_thread")
        thread.start()
        self.threads["optimization"] = thread
    
    def _run_sync_cycle(self) -> None:
        """Run ESPN data synchronization"""
        logger.info("Running ESPN sync cycle")
        start_time = datetime.now()
        
        try:
            # Sync latest games and upcoming games from ESPN
            self.live_sync_manager.sync_scoreboard()
            self.live_sync_manager.sync_upcoming_games()
            self.live_sync_manager.sync_lineups()
            
            self.state["last_sync"] = datetime.now().isoformat()
            self.state["total_runs"] += 1
            
            self.log_event(
                "sync_complete",
                "info",
                f"ESPN sync completed in {(datetime.now() - start_time).total_seconds():.1f}s"
            )
        except Exception as e:
            logger.error(f"Sync cycle failed: {e}")
            raise
    
    def _run_backtest_cycle(self) -> None:
        """Run backtest cycle"""
        logger.info("Running backtest cycle")
        start_time = datetime.now()
        
        try:
            result = self.self_learner.backtest_historical_games(
                lookback_days=365,
                games_per_cycle=20
            )
            
            self.state["last_backtest"] = datetime.now().isoformat()
            self.state["total_runs"] += 1
            
            self.log_event(
                "backtest_complete",
                "info",
                f"Backtest: {result['games_processed']} games, "
                f"improvement: {result['improvement']:+.2%}",
                context={"accuracy_before": result["accuracy_before"],
                        "accuracy_after": result["accuracy_after"]}
            )
        except Exception as e:
            logger.error(f"Backtest cycle failed: {e}")
            raise
    
    def _run_learning_cycle(self) -> None:
        """Run full learning cycle"""
        logger.info("Running learning cycle")
        start_time = datetime.now()
        
        try:
            result = self.self_learner.run_full_historical_learning(target_accuracy=0.95)
            
            self.state["last_learning_cycle"] = datetime.now().isoformat()
            self.state["total_runs"] += 1
            
            if result["target_achieved"]:
                self.state["learning_phase"] = "forward_projections"
                logger.info("Historical learning complete, switching to forward projections")
                self.log_event(
                    "learning_complete",
                    "info",
                    f"Historical learning complete! Target accuracy achieved: {result['final_accuracy']:.2%}",
                    context={"cycles": len(result["cycles"]),
                            "games_tested": result["total_games_tested"]}
                )
            else:
                self.log_event(
                    "learning_cycle_complete",
                    "info",
                    f"Learning cycle: {len(result['cycles'])} cycles, "
                    f"final accuracy: {result['final_accuracy']:.2%}",
                    context={"target_achieved": False}
                )
        except Exception as e:
            logger.error(f"Learning cycle failed: {e}")
            raise
    
    def _run_projection_cycle(self) -> None:
        """Run forward projection generation"""
        logger.info("Running projection cycle")
        start_time = datetime.now()
        
        try:
            result = self.self_learner.generate_forward_projections()
            
            self.state["last_projection_update"] = datetime.now().isoformat()
            self.state["total_runs"] += 1
            
            self.log_event(
                "projection_update",
                "info",
                f"Forward projections updated: {result['summary']['total_games']} games, "
                f"{result['summary']['total_predictions']} predictions",
                context={"start_date": result["projection_start"],
                        "end_date": result["projection_end"]}
            )
        except Exception as e:
            logger.error(f"Projection cycle failed: {e}")
            raise
    
    def _run_optimization_cycle(self) -> None:
        """Run model optimization based on new results"""
        logger.info("Running optimization cycle")
        start_time = datetime.now()
        
        try:
            # Check for new game results and optimize if needed
            improvements = self.cloud_brain.get_improvement_recommendations()
            
            if improvements:
                logger.info(f"Found {len(improvements)} optimization opportunities")
                
                # Apply high priority improvements
                for improvement in improvements:
                    if improvement.get("priority") == "high":
                        logger.info(f"Applying: {improvement['action']}")
                
                self.state["last_optimization"] = datetime.now().isoformat()
            
            self.log_event(
                "optimization_complete",
                "info",
                f"Optimization cycle: {len(improvements)} recommendations generated",
                context={"improvements": len(improvements)}
            )
        except Exception as e:
            logger.error(f"Optimization cycle failed: {e}")
            raise
    
    def get_status(self) -> Dict[str, Any]:
        """Get current orchestrator status"""
        return {
            "is_running": self.is_running,
            "state": self.state,
            "threads": {
                name: {"alive": thread.is_alive() if thread else False}
                for name, thread in self.threads.items()
            },
            "recent_events": list(self.event_queue)[-10:],  # Last 10 events
            "cloud_brain": self.cloud_brain.get_brain_summary(),
        }
    
    def get_learning_report(self) -> Dict[str, Any]:
        """Generate comprehensive learning report"""
        return {
            "generated_at": datetime.now().isoformat(),
            "orchestrator_state": self.state,
            "learning_summary": self.self_learner.get_learning_summary(),
            "recent_events": list(self.event_queue)[-50:],
        }


def create_orchestrator(orchestrator_dir: Optional[Path] = None) -> ContinuousLearningOrchestrator:
    """Factory function to create continuous learning orchestrator"""
    return ContinuousLearningOrchestrator(orchestrator_dir)
