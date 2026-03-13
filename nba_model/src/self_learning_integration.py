"""
Self-Learning System Integration: Integrates all components into your NBA prediction app

This module provides the glue that connects:
- ESPN data sync (live_sync)
- Cloud Brain (persistent knowledge)
- Simulation Engine (prediction vs actual comparison)
- Self-Learner (error analysis and correction)
- Continuous Orchestrator (automated scheduling)

Use this to initialize and manage the entire self-learning system.
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Any, Dict, Optional

try:
    from .cloud_brain import CloudBrain, create_cloud_brain
    from .simulation_engine import SimulationEngine, create_simulation_engine
    from .self_learner import SelfLearner, create_self_learner
    from .continuous_learning import ContinuousLearningOrchestrator, create_orchestrator
    from .engine import predict_engine, train_engine
    from .live_sync import LiveSyncManager
    from .features import DEFAULT_PROJECT_DIR
except ImportError:
    from cloud_brain import CloudBrain, create_cloud_brain
    from simulation_engine import SimulationEngine, create_simulation_engine
    from self_learner import SelfLearner, create_self_learner
    from continuous_learning import ContinuousLearningOrchestrator, create_orchestrator
    from engine import predict_engine, train_engine
    from live_sync import LiveSyncManager
    from features import DEFAULT_PROJECT_DIR


logger = logging.getLogger(__name__)


class SelfLearningSystem:
    """
    Complete self-learning system for NBA prediction model
    
    Features:
    - Continuous ESPN data synchronization
    - Persistent knowledge storage (Cloud Brain)
    - Simulation and error analysis
    - Self-correction learning loops
    - Automatic model improvement
    - Forward projections for future games
    """
    
    def __init__(self):
        """Initialize the complete self-learning system"""
        logger.info("Initializing Self-Learning System")
        
        # Core components
        self.cloud_brain = create_cloud_brain()
        self.simulation_engine = create_simulation_engine(self.cloud_brain)
        self.self_learner = create_self_learner()
        self.orchestrator = create_orchestrator()
        
        logger.info("Self-Learning System initialized successfully")
    
    def start_learning(self) -> None:
        """Start the complete automated learning system"""
        logger.info("Starting Self-Learning System")
        self.orchestrator.start()
        logger.info("Self-Learning System is running")
    
    def stop_learning(self) -> None:
        """Stop the learning system"""
        logger.info("Stopping Self-Learning System")
        self.orchestrator.stop()
        logger.info("Self-Learning System stopped")
    
    def run_backtest_cycle(self) -> Dict[str, Any]:
        """
        Run a single backtest cycle on historical games
        
        This tests the model on past games without looking at actual results first,
        then compares predictions to actual outcomes.
        
        Returns:
            Backtest results including accuracy metrics
        """
        logger.info("Running manual backtest cycle")
        return self.self_learner.backtest_historical_games()
    
    def run_full_historical_learning(self, target_accuracy: float = 0.95) -> Dict[str, Any]:
        """
        Run complete historical learning to reach target accuracy
        
        Args:
            target_accuracy: Target accuracy before moving to forward projections
            
        Returns:
            Learning session results
        """
        logger.info(f"Starting full historical learning (target: {target_accuracy:.2%})")
        return self.self_learner.run_full_historical_learning(target_accuracy=target_accuracy)
    
    def generate_forward_projections(self, start_date: str = None, 
                                   end_date: str = None) -> Dict[str, Any]:
        """
        Generate forward projections for upcoming games
        
        Args:
            start_date: Start date (default: today)
            end_date: End date (default: 30 days ahead)
            
        Returns:
            Forward projections with confidence scores
        """
        logger.info("Generating forward projections")
        return self.self_learner.generate_forward_projections(start_date, end_date)
    
    def sync_espn_data(self) -> Dict[str, Any]:
        """
        Manually trigger ESPN data synchronization
        
        Returns:
            Sync results
        """
        logger.info("Manually syncing ESPN data")
        try:
            self.orchestrator.live_sync_manager.sync_scoreboard()
            self.orchestrator.live_sync_manager.sync_upcoming_games()
            self.orchestrator.live_sync_manager.sync_lineups()
            
            return {
                "status": "success",
                "message": "ESPN data synced successfully",
                "timestamp": str(__import__('datetime').datetime.now()),
            }
        except Exception as e:
            logger.error(f"ESPN sync failed: {e}")
            return {
                "status": "error",
                "message": str(e),
                "timestamp": str(__import__('datetime').datetime.now()),
            }
    
    def get_brain_summary(self) -> Dict[str, Any]:
        """Get summary of Cloud Brain knowledge"""
        return self.cloud_brain.get_brain_summary()
    
    def get_learning_progress(self) -> Dict[str, Any]:
        """Get current learning progress"""
        return self.self_learner.get_learning_summary()
    
    def get_system_status(self) -> Dict[str, Any]:
        """Get complete system status"""
        return self.orchestrator.get_status()
    
    def get_learning_report(self) -> Dict[str, Any]:
        """Get comprehensive learning report"""
        return self.orchestrator.get_learning_report()
    
    def analyze_improvements(self) -> Dict[str, Any]:
        """Get recommendations for model improvements"""
        return {
            "recommendations": self.cloud_brain.get_improvement_recommendations(),
            "error_analysis": self.cloud_brain.analyze_errors(),
            "brain_summary": self.cloud_brain.get_brain_summary(),
        }


# Global instance for easy access
_system_instance: Optional[SelfLearningSystem] = None


def initialize_self_learning_system() -> SelfLearningSystem:
    """Initialize the global self-learning system"""
    global _system_instance
    if _system_instance is None:
        _system_instance = SelfLearningSystem()
    return _system_instance


def get_self_learning_system() -> SelfLearningSystem:
    """Get the global self-learning system instance"""
    global _system_instance
    if _system_instance is None:
        return initialize_self_learning_system()
    return _system_instance


# Example usage and integration points
def example_complete_workflow() -> None:
    """
    Example of the complete self-learning workflow
    
    This shows how to:
    1. Initialize the system
    2. Run historical learning
    3. Generate forward projections
    4. Keep system running continuously
    """
    logger.info("=== Self-Learning System Complete Workflow ===")
    
    # Initialize
    system = initialize_self_learning_system()
    
    # Phase 1: Historical Learning
    logger.info("\n--- PHASE 1: Historical Learning ---")
    logger.info("The system will now learn from all historical games...")
    learning_result = system.run_full_historical_learning(target_accuracy=0.95)
    logger.info(f"Learning complete: {learning_result['target_achieved']}")
    logger.info(f"Final accuracy: {learning_result['final_accuracy']:.2%}")
    
    # Phase 2: Forward Projections
    logger.info("\n--- PHASE 2: Forward Projections ---")
    projections = system.generate_forward_projections()
    logger.info(f"Generated projections for {projections['summary']['total_games']} games")
    
    # Phase 3: Continuous Learning
    logger.info("\n--- PHASE 3: Continuous Learning ---")
    logger.info("Starting continuous learning system...")
    system.start_learning()
    
    # The system will now:
    # - Continuously sync ESPN data
    # - Make predictions for upcoming games
    # - Compare with actual results when games complete
    # - Learn from errors automatically
    # - Update projections continuously
    # - Optimize models based on new data
    
    logger.info("System is now running continuously. Check status with:")
    logger.info("  system.get_system_status()")
    logger.info("  system.get_learning_progress()")
    logger.info("  system.get_learning_report()")


# Integration point with your existing app.py
def integrate_with_existing_app(app_mode: str = "hybrid") -> Dict[str, Any]:
    """
    Integration guide for adding self-learning to existing app
    
    Args:
        app_mode: 
            "hybrid" - Existing app + self-learning running in background
            "learning_only" - Run only self-learning for research/training
            "production" - Full production setup with monitoring
    
    Returns:
        Integration status
    """
    logger.info(f"Integrating self-learning with app mode: {app_mode}")
    
    system = get_self_learning_system()
    
    if app_mode == "hybrid":
        # Keep existing app running, add learning in background
        logger.info("Running in hybrid mode: existing app + background learning")
        system.start_learning()
        return {
            "mode": "hybrid",
            "status": "running",
            "message": "Self-learning system is running in background"
        }
    
    elif app_mode == "learning_only":
        # Focus on learning from historical data
        logger.info("Running in learning-only mode: focusing on historical training")
        result = system.run_full_historical_learning()
        return {
            "mode": "learning_only",
            "status": "complete" if result["target_achieved"] else "incomplete",
            "accuracy": result["final_accuracy"],
        }
    
    elif app_mode == "production":
        # Full production setup
        logger.info("Running in production mode")
        system.start_learning()
        status = system.get_system_status()
        return {
            "mode": "production",
            "status": "running",
            "components": status
        }
    
    return {"error": "Unknown app mode"}


# Data export functions for visualization/analysis
def export_predictions_for_analysis(output_path: Optional[Path] = None) -> Path:
    """Export prediction log for analysis/visualization"""
    system = get_self_learning_system()
    predictions = system.cloud_brain.load_prediction_log()
    
    if output_path is None:
        output_path = DEFAULT_PROJECT_DIR / "data" / "exported_predictions.csv"
    
    predictions.to_csv(output_path, index=False)
    logger.info(f"Predictions exported to {output_path}")
    return output_path


def export_learning_report(output_path: Optional[Path] = None) -> Path:
    """Export comprehensive learning report for review"""
    import json
    
    system = get_self_learning_system()
    report = system.get_learning_report()
    
    if output_path is None:
        output_path = DEFAULT_PROJECT_DIR / "data" / "learning_report.json"
    
    with open(output_path, 'w') as f:
        json.dump(report, f, indent=2, default=str)
    
    logger.info(f"Learning report exported to {output_path}")
    return output_path


# Quick start function
def quick_start() -> None:
    """Quick start the self-learning system"""
    logger.info("Starting Self-Learning System (Quick Start)")
    
    system = initialize_self_learning_system()
    
    # Show current status
    status = system.get_system_status()
    logger.info(f"System Status: {status['is_running']}")
    
    # Start if not running
    if not status['is_running']:
        system.start_learning()
        logger.info("✓ Self-Learning System started")
    
    # Show learning progress
    progress = system.get_learning_progress()
    logger.info(f"Learning Progress: {progress['state']['learning_iterations']} iterations")
    
    logger.info("\nSystem is ready! Use system.get_learning_report() to check progress.")


if __name__ == "__main__":
    # Configure logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    # Run quick start
    quick_start()
