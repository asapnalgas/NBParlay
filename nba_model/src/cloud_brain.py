"""
Cloud Brain: Persistent Knowledge Store for Self-Learning System

The Cloud Brain maintains:
- Historical predictions vs actual outcomes
- Error patterns and corrections
- Model performance metrics by player/team/date
- Learning sessions and their results
- Confidence scores and prediction quality metrics
"""

from __future__ import annotations

import json
import os
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
from dataclasses import dataclass, asdict
from collections import defaultdict

import pandas as pd
import numpy as np

try:
    from .features import DEFAULT_PROJECT_DIR
except ImportError:
    from features import DEFAULT_PROJECT_DIR


DEFAULT_CLOUD_BRAIN_DIR = DEFAULT_PROJECT_DIR / "data" / "cloud_brain"
DEFAULT_BRAIN_STATE_PATH = DEFAULT_CLOUD_BRAIN_DIR / "brain_state.json"
DEFAULT_BRAIN_MEMORY_PATH = DEFAULT_CLOUD_BRAIN_DIR / "brain_memory.csv"
DEFAULT_PREDICTION_LOG_PATH = DEFAULT_CLOUD_BRAIN_DIR / "prediction_log.csv"
DEFAULT_ERROR_ANALYSIS_PATH = DEFAULT_CLOUD_BRAIN_DIR / "error_analysis.json"
DEFAULT_LEARNING_SESSIONS_PATH = DEFAULT_CLOUD_BRAIN_DIR / "learning_sessions.csv"
DEFAULT_SIMULATION_RESULTS_PATH = DEFAULT_CLOUD_BRAIN_DIR / "simulation_results.csv"
DEFAULT_STARTER_PREDICTIONS_PATH = DEFAULT_CLOUD_BRAIN_DIR / "starter_predictions.csv"
DEFAULT_STATLINE_PREDICTIONS_PATH = DEFAULT_CLOUD_BRAIN_DIR / "statline_predictions.csv"

# Error thresholds for adaptive learning
ERROR_CORRECTION_THRESHOLDS = {
    "points": 2.5,
    "rebounds": 1.5,
    "assists": 1.5,
    "pra": 4.0,
}

PREDICTION_METRICS_TARGETS = ["points", "rebounds", "assists", "steals", "blocks", "turnovers"]


@dataclass
class PredictionRecord:
    """Record of a single prediction vs actual outcome"""
    game_id: str
    game_date: str
    player_key: str
    player_name: str
    team_key: str
    predicted_starter: bool
    actual_starter: bool | None
    starter_prediction_confidence: float
    
    predicted_points: float
    actual_points: Optional[float]
    points_error: Optional[float]
    
    predicted_rebounds: float
    actual_rebounds: Optional[float]
    rebounds_error: Optional[float]
    
    predicted_assists: float
    actual_assists: Optional[float]
    assists_error: Optional[float]
    
    predicted_pra: float
    actual_pra: Optional[float]
    pra_error: Optional[float]
    
    prediction_timestamp: str
    actual_result_timestamp: Optional[str]
    
    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


class CloudBrain:
    """
    Persistent knowledge store that learns from predictions and actual results.
    Maintains state about what the model knows and how it's improving.
    """

    def __init__(self, brain_dir: Optional[Path] = None):
        """Initialize or load existing cloud brain"""
        self.brain_dir = brain_dir or DEFAULT_CLOUD_BRAIN_DIR
        self.brain_dir.mkdir(parents=True, exist_ok=True)
        
        # Ensure all data paths exist
        self.state_path = self.brain_dir / "brain_state.json"
        self.memory_path = self.brain_dir / "brain_memory.csv"
        self.prediction_log_path = self.brain_dir / "prediction_log.csv"
        self.error_analysis_path = self.brain_dir / "error_analysis.json"
        self.learning_sessions_path = self.brain_dir / "learning_sessions.csv"
        self.simulation_results_path = self.brain_dir / "simulation_results.csv"
        self.starter_predictions_path = self.brain_dir / "starter_predictions.csv"
        self.statline_predictions_path = self.brain_dir / "statline_predictions.csv"
        
        # In-memory cache
        self.state = self._load_state()
        self.prediction_records: List[PredictionRecord] = []
        self.error_corrections: Dict[str, List[Dict]] = defaultdict(list)
        
    def _load_state(self) -> Dict[str, Any]:
        """Load or initialize brain state"""
        if self.state_path.exists():
            with open(self.state_path, 'r') as f:
                return json.load(f)
        
        return {
            "created_at": datetime.now().isoformat(),
            "last_updated": datetime.now().isoformat(),
            "total_predictions": 0,
            "total_actual_results": 0,
            "learning_enabled": True,
            "sessions_completed": 0,
            "historical_games_processed": 0,
            "forward_projection_date": None,
            "model_version": "1.0",
            "accuracy_metrics": {},
            "player_error_profiles": {},
        }
    
    def save_state(self) -> None:
        """Save brain state to disk"""
        self.state["last_updated"] = datetime.now().isoformat()
        with open(self.state_path, 'w') as f:
            json.dump(self.state, f, indent=2)
    
    def record_prediction(self, record: PredictionRecord) -> None:
        """Record a prediction made by the model"""
        self.prediction_records.append(record)
        self.state["total_predictions"] += 1
        
        # Track prediction type
        if record.predicted_starter:
            if "starter_predictions" not in self.state:
                self.state["starter_predictions"] = 0
            self.state["starter_predictions"] += 1
    
    def record_actual_result(self, game_id: str, player_key: str, 
                            actual_starter: bool, **actual_stats) -> None:
        """Record actual game result for comparison with prediction"""
        # Find matching prediction
        for record in self.prediction_records:
            if record.game_id == game_id and record.player_key == player_key:
                record.actual_starter = actual_starter
                record.actual_result_timestamp = datetime.now().isoformat()
                
                # Update stat lines
                if "points" in actual_stats:
                    record.actual_points = actual_stats["points"]
                    record.points_error = abs(record.predicted_points - actual_stats["points"])
                
                if "rebounds" in actual_stats:
                    record.actual_rebounds = actual_stats["rebounds"]
                    record.rebounds_error = abs(record.predicted_rebounds - actual_stats["rebounds"])
                
                if "assists" in actual_stats:
                    record.actual_assists = actual_stats["assists"]
                    record.assists_error = abs(record.predicted_assists - actual_stats["assists"])
                
                if "pra" in actual_stats:
                    record.actual_pra = actual_stats["pra"]
                    record.pra_error = abs(record.predicted_pra - actual_stats["pra"])
                
                self.state["total_actual_results"] += 1
                break
    
    def analyze_errors(self) -> Dict[str, Any]:
        """Analyze prediction errors and identify patterns"""
        errors = {
            "by_stat": {},
            "by_player": defaultdict(list),
            "by_team": defaultdict(list),
            "by_date": defaultdict(list),
            "high_error_predictions": [],
            "starter_prediction_accuracy": 0.0,
            "improvement_opportunities": [],
        }
        
        total_starter_predictions = 0
        correct_starter_predictions = 0
        
        for record in self.prediction_records:
            if record.actual_result_timestamp is None:
                continue  # Skip if actual result not recorded yet
            
            # Analyze by stat
            if "points" in errors["by_stat"]:
                errors["by_stat"]["points"].append(record.points_error or 0)
            else:
                errors["by_stat"]["points"] = [record.points_error or 0]
            
            # Starter prediction accuracy
            if record.predicted_starter or record.actual_starter:
                total_starter_predictions += 1
                if record.predicted_starter == record.actual_starter:
                    correct_starter_predictions += 1
            
            # Track high errors
            if (record.points_error or 0) > ERROR_CORRECTION_THRESHOLDS.get("points", 2.5):
                errors["high_error_predictions"].append({
                    "game_id": record.game_id,
                    "player_key": record.player_key,
                    "stat": "points",
                    "predicted": record.predicted_points,
                    "actual": record.actual_points,
                    "error": record.points_error,
                })
            
            # Track by player
            errors["by_player"][record.player_key].append({
                "points_error": record.points_error,
                "rebounds_error": record.rebounds_error,
                "assists_error": record.assists_error,
            })
        
        # Calculate aggregate metrics
        for stat, error_list in errors["by_stat"].items():
            if error_list:
                errors["by_stat"][stat] = {
                    "mae": float(np.mean(error_list)),
                    "mape": float(np.mean(np.abs(error_list))) if error_list else 0,
                    "max_error": float(max(error_list)) if error_list else 0,
                }
        
        if total_starter_predictions > 0:
            errors["starter_prediction_accuracy"] = correct_starter_predictions / total_starter_predictions
        
        return errors
    
    def get_player_error_profile(self, player_key: str) -> Dict[str, Any]:
        """Get error profile for a specific player"""
        player_records = [r for r in self.prediction_records 
                         if r.player_key == player_key and r.actual_result_timestamp]
        
        if not player_records:
            return {"player_key": player_key, "sample_size": 0}
        
        errors = [r.points_error for r in player_records if r.points_error is not None]
        
        return {
            "player_key": player_key,
            "sample_size": len(player_records),
            "avg_error_points": float(np.mean(errors)) if errors else 0,
            "max_error_points": float(max(errors)) if errors else 0,
            "starter_prediction_accuracy": float(
                sum(1 for r in player_records if r.predicted_starter == r.actual_starter) 
                / len(player_records)
            ) if player_records else 0,
        }
    
    def save_prediction_log(self) -> None:
        """Save prediction records to CSV"""
        if not self.prediction_records:
            return
        
        df = pd.DataFrame([r.to_dict() for r in self.prediction_records])
        df.to_csv(self.prediction_log_path, index=False)
    
    def load_prediction_log(self) -> pd.DataFrame:
        """Load prediction log from CSV"""
        if self.prediction_log_path.exists():
            return pd.read_csv(self.prediction_log_path)
        return pd.DataFrame()
    
    def record_learning_session(self, session_data: Dict[str, Any]) -> None:
        """Record a learning session and its results"""
        session_df = pd.DataFrame([session_data])
        
        if self.learning_sessions_path.exists():
            existing = pd.read_csv(self.learning_sessions_path)
            session_df = pd.concat([existing, session_df], ignore_index=True)
        
        session_df.to_csv(self.learning_sessions_path, index=False)
        self.state["sessions_completed"] += 1
    
    def record_simulation_result(self, simulation_data: Dict[str, Any]) -> None:
        """Record results from a simulation run"""
        sim_df = pd.DataFrame([simulation_data])
        
        if self.simulation_results_path.exists():
            existing = pd.read_csv(self.simulation_results_path)
            sim_df = pd.concat([existing, sim_df], ignore_index=True)
        
        sim_df.to_csv(self.simulation_results_path, index=False)
    
    def get_improvement_recommendations(self) -> List[Dict[str, Any]]:
        """Get recommendations for model improvements based on error analysis"""
        errors = self.analyze_errors()
        recommendations = []
        
        for stat, metrics in errors["by_stat"].items():
            if isinstance(metrics, dict) and metrics.get("mae", 0) > ERROR_CORRECTION_THRESHOLDS.get(stat, 2.0):
                recommendations.append({
                    "stat": stat,
                    "reason": f"High MAE for {stat}: {metrics['mae']:.2f}",
                    "action": f"Retrain model with focus on {stat} feature importance",
                    "priority": "high" if metrics["mae"] > ERROR_CORRECTION_THRESHOLDS.get(stat, 2.0) * 1.5 else "medium",
                })
        
        if errors["starter_prediction_accuracy"] < 0.85:
            recommendations.append({
                "stat": "starter",
                "reason": f"Starter prediction accuracy: {errors['starter_prediction_accuracy']:.2%}",
                "action": "Add more features related to depth chart and player status",
                "priority": "high",
            })
        
        return recommendations
    
    def get_brain_summary(self) -> Dict[str, Any]:
        """Get high-level summary of brain state and learning progress"""
        errors = self.analyze_errors()
        
        return {
            "state": self.state,
            "prediction_count": len(self.prediction_records),
            "completed_predictions": sum(1 for r in self.prediction_records if r.actual_result_timestamp),
            "error_summary": {
                "by_stat": errors["by_stat"],
                "starter_accuracy": errors["starter_prediction_accuracy"],
                "high_error_count": len(errors["high_error_predictions"]),
            },
            "improvement_recommendations": self.get_improvement_recommendations(),
            "brain_dir": str(self.brain_dir),
        }


def create_cloud_brain(brain_dir: Optional[Path] = None) -> CloudBrain:
    """Factory function to create or load cloud brain"""
    return CloudBrain(brain_dir)
