"""
Simulation Engine: Run predictions and compare with actual results

This module handles:
- Predicting starting lineups before they're revealed
- Predicting stat lines for starter projections
- Comparing predictions with actual game results
- Analyzing prediction errors
- Tracking simulation progress through historical games
"""

from __future__ import annotations

import json
import logging
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
from dataclasses import dataclass

import pandas as pd
import numpy as np

try:
    from .cloud_brain import CloudBrain, PredictionRecord
    from .features import DEFAULT_PROJECT_DIR
    from .engine import predict_engine
    from .player_matching import normalize_player_name
    from .depth_chart_features import DepthChartFeatures, create_depth_chart_features
except ImportError:
    from cloud_brain import CloudBrain, PredictionRecord
    from features import DEFAULT_PROJECT_DIR
    from engine import predict_engine
    from player_matching import normalize_player_name
    from depth_chart_features import DepthChartFeatures, create_depth_chart_features


logger = logging.getLogger(__name__)

DEFAULT_SIMULATION_DIR = DEFAULT_PROJECT_DIR / "data" / "simulations"
DEFAULT_SIMULATION_STATE_PATH = DEFAULT_SIMULATION_DIR / "simulation_state.json"


@dataclass
class StarterPrediction:
    """Prediction for whether a player will start"""
    game_id: str
    game_date: str
    player_key: str
    player_name: str
    team_key: str
    team_name: str
    position: str
    predicted_starter: bool
    confidence: float
    confidence_reasoning: str
    prediction_timestamp: str


@dataclass
class StatlinePrediction:
    """Prediction for a player's stats in a game"""
    game_id: str
    game_date: str
    player_key: str
    player_name: str
    team_key: str
    is_starter: bool
    
    predicted_minutes: float
    predicted_points: float
    predicted_rebounds: float
    predicted_assists: float
    predicted_steals: float
    predicted_blocks: float
    predicted_turnovers: float
    predicted_pra: float
    
    confidence: float
    prediction_timestamp: str


class SimulationEngine:
    """Runs simulations comparing predictions to actual results"""
    
    def __init__(self, cloud_brain: CloudBrain, sim_dir: Optional[Path] = None):
        """Initialize simulation engine"""
        self.cloud_brain = cloud_brain
        self.sim_dir = sim_dir or DEFAULT_SIMULATION_DIR
        self.sim_dir.mkdir(parents=True, exist_ok=True)
        self.state_path = self.sim_dir / "simulation_state.json"
        
        # Initialize depth chart features
        try:
            self.depth_chart_features = create_depth_chart_features()
            logger.info("Depth chart features initialized successfully")
        except Exception as e:
            logger.warning(f"Could not initialize depth chart features: {e}")
            self.depth_chart_features = None
        
        self.state = self._load_state()
        self.starter_predictions: List[StarterPrediction] = []
        self.statline_predictions: List[StatlinePrediction] = []
        self.comparison_results: List[Dict[str, Any]] = []
        
    def _load_state(self) -> Dict[str, Any]:
        """Load or initialize simulation state"""
        if self.state_path.exists():
            with open(self.state_path, 'r') as f:
                return json.load(f)
        
        return {
            "created_at": datetime.now().isoformat(),
            "simulations_run": 0,
            "games_processed": 0,
            "predictions_made": 0,
            "comparisons_completed": 0,
            "current_game_index": 0,
            "total_games": 0,
            "error_corrections_applied": 0,
        }
    
    def save_state(self) -> None:
        """Save simulation state"""
        self.state["last_updated"] = datetime.now().isoformat()
        with open(self.state_path, 'w') as f:
            json.dump(self.state, f, indent=2)
    
    def predict_starters(self, game_date: str, game_id: str, team_key: str, 
                        historical_depth_chart: Optional[pd.DataFrame] = None) -> List[StarterPrediction]:
        """
        Predict which players will be starting in an upcoming game
        
        Args:
            game_date: Date of the game (YYYY-MM-DD)
            game_id: Unique game identifier
            team_key: Team code
            historical_depth_chart: Historical depth chart data for reference
            
        Returns:
            List of StarterPrediction objects
        """
        predictions = []
        timestamp = datetime.now().isoformat()
        
        # Load team roster for the date
        # This would pull from your existing data
        # For now, using placeholder logic
        
        logger.info(f"Predicting starters for {game_date} - {game_id} - {team_key}")
        
        # You would implement logic to:
        # 1. Load historical starter patterns for this team
        # 2. Check player injury/status information
        # 3. Apply model predictions about starting probability
        # 4. Use depth chart information
        
        # Placeholder: would be replaced with actual prediction logic
        starter_prob_model = self._get_starter_probability_model(team_key)
        
        # Query player roster for this team/date
        team_roster = self._get_team_roster(game_date, team_key)
        
        for _, player in team_roster.iterrows():
            player_key = player.get("player_key")
            confidence = starter_prob_model.get(player_key, 0.0) if starter_prob_model else 0.5
            
            pred = StarterPrediction(
                game_id=game_id,
                game_date=game_date,
                player_key=player_key,
                player_name=player.get("player_name", ""),
                team_key=team_key,
                team_name=player.get("team_name", ""),
                position=player.get("position", ""),
                predicted_starter=confidence > 0.5,
                confidence=confidence,
                confidence_reasoning=f"Depth chart and historical starter patterns ({confidence:.2%})",
                prediction_timestamp=timestamp,
            )
            predictions.append(pred)
            self.starter_predictions.append(pred)
        
        self.state["predictions_made"] += len(predictions)
        return predictions
    
    def predict_statlines(self, starters: List[StarterPrediction], 
                         game_date: str, game_id: str) -> List[StatlinePrediction]:
        """
        Predict stat lines for predicted starters
        
        Args:
            starters: List of starter predictions
            game_date: Game date
            game_id: Game ID
            
        Returns:
            List of StatlinePrediction objects
        """
        predictions = []
        timestamp = datetime.now().isoformat()
        
        logger.info(f"Predicting stat lines for {game_id}")
        
        for starter in starters:
            if not starter.predicted_starter:
                continue  # Only predict stat lines for predicted starters
            
            # Use your existing predict_engine to generate predictions
            try:
                # This would call your existing prediction model
                stat_pred = {
                    "minutes": 30.0,  # Placeholder
                    "points": 15.0,
                    "rebounds": 5.0,
                    "assists": 3.0,
                    "steals": 1.0,
                    "blocks": 1.0,
                    "turnovers": 2.0,
                }
                
                pra = stat_pred["points"] + stat_pred["rebounds"] + stat_pred["assists"]
                
                pred = StatlinePrediction(
                    game_id=game_id,
                    game_date=game_date,
                    player_key=starter.player_key,
                    player_name=starter.player_name,
                    team_key=starter.team_key,
                    is_starter=True,
                    predicted_minutes=stat_pred["minutes"],
                    predicted_points=stat_pred["points"],
                    predicted_rebounds=stat_pred["rebounds"],
                    predicted_assists=stat_pred["assists"],
                    predicted_steals=stat_pred["steals"],
                    predicted_blocks=stat_pred["blocks"],
                    predicted_turnovers=stat_pred["turnovers"],
                    predicted_pra=pra,
                    confidence=0.75,  # Would be dynamic
                    prediction_timestamp=timestamp,
                )
                predictions.append(pred)
                self.statline_predictions.append(pred)
            except Exception as e:
                logger.error(f"Error predicting statline for {starter.player_key}: {e}")
                continue
        
        self.state["predictions_made"] += len(predictions)
        return predictions
    
    def compare_with_actual(self, game_id: str, actual_starters: pd.DataFrame,
                           actual_stats: pd.DataFrame) -> Dict[str, Any]:
        """
        Compare predictions with actual game results
        
        Args:
            game_id: Game ID
            actual_starters: DataFrame with actual starting lineup
            actual_stats: DataFrame with actual box score stats
            
        Returns:
            Comparison results dictionary
        """
        logger.info(f"Comparing predictions with actual results for {game_id}")
        
        comparison = {
            "game_id": game_id,
            "timestamp": datetime.now().isoformat(),
            "starter_predictions": [],
            "statline_predictions": [],
            "summary": {
                "starter_accuracy": 0.0,
                "points_mae": 0.0,
                "rebounds_mae": 0.0,
                "assists_mae": 0.0,
                "pra_mae": 0.0,
            }
        }
        
        # Get game predictions
        game_starter_preds = [p for p in self.starter_predictions if p.game_id == game_id]
        game_statline_preds = [p for p in self.statline_predictions if p.game_id == game_id]
        
        # Compare starters
        starter_matches = 0
        for pred in game_starter_preds:
            actual_is_starter = (actual_starters["player_key"] == pred.player_key).any()
            match = pred.predicted_starter == actual_is_starter
            starter_matches += int(match)
            
            comparison["starter_predictions"].append({
                "player_key": pred.player_key,
                "predicted_starter": pred.predicted_starter,
                "actual_starter": actual_is_starter,
                "correct": match,
            })
            
            # Record in cloud brain
            record = PredictionRecord(
                game_id=game_id,
                game_date=pred.game_date,
                player_key=pred.player_key,
                player_name=pred.player_name,
                team_key=pred.team_key,
                predicted_starter=pred.predicted_starter,
                actual_starter=actual_is_starter,
                starter_prediction_confidence=pred.confidence,
                predicted_points=0.0,
                actual_points=None,
                points_error=None,
                predicted_rebounds=0.0,
                actual_rebounds=None,
                rebounds_error=None,
                predicted_assists=0.0,
                actual_assists=None,
                assists_error=None,
                predicted_pra=0.0,
                actual_pra=None,
                pra_error=None,
                prediction_timestamp=pred.prediction_timestamp,
                actual_result_timestamp=datetime.now().isoformat(),
            )
            self.cloud_brain.record_prediction(record)
        
        if game_starter_preds:
            comparison["summary"]["starter_accuracy"] = starter_matches / len(game_starter_preds)
        
        # Compare stat lines
        stat_errors = {"points": [], "rebounds": [], "assists": [], "pra": []}
        
        for pred in game_statline_preds:
            player_actual = actual_stats[actual_stats["player_key"] == pred.player_key]
            
            if player_actual.empty:
                logger.warning(f"No actual stats found for {pred.player_key} in {game_id}")
                continue
            
            actual_row = player_actual.iloc[0]
            
            # Calculate errors
            points_error = abs(pred.predicted_points - actual_row.get("points", 0))
            rebounds_error = abs(pred.predicted_rebounds - actual_row.get("rebounds", 0))
            assists_error = abs(pred.predicted_assists - actual_row.get("assists", 0))
            actual_pra = actual_row.get("points", 0) + actual_row.get("rebounds", 0) + actual_row.get("assists", 0)
            pra_error = abs(pred.predicted_pra - actual_pra)
            
            stat_errors["points"].append(points_error)
            stat_errors["rebounds"].append(rebounds_error)
            stat_errors["assists"].append(assists_error)
            stat_errors["pra"].append(pra_error)
            
            comparison["statline_predictions"].append({
                "player_key": pred.player_key,
                "predicted_points": pred.predicted_points,
                "actual_points": actual_row.get("points", 0),
                "points_error": points_error,
                "predicted_rebounds": pred.predicted_rebounds,
                "actual_rebounds": actual_row.get("rebounds", 0),
                "rebounds_error": rebounds_error,
                "predicted_assists": pred.predicted_assists,
                "actual_assists": actual_row.get("assists", 0),
                "assists_error": assists_error,
                "predicted_pra": pred.predicted_pra,
                "actual_pra": actual_pra,
                "pra_error": pra_error,
            })
        
        # Calculate summary statistics
        for stat in stat_errors:
            if stat_errors[stat]:
                comparison["summary"][f"{stat}_mae"] = float(np.mean(stat_errors[stat]))
        
        self.comparison_results.append(comparison)
        self.state["comparisons_completed"] += 1
        self.state["games_processed"] += 1
        
        return comparison
    
    def _get_starter_probability_model(self, team_key: str) -> Dict[str, float]:
        """Get model for predicting starter probabilities from depth chart and historical data"""
        try:
            # Try to use depth chart features first
            if self.depth_chart_features:
                starter_probs = {}
                depth_chart = self.depth_chart_features.builder.get_depth_chart_for_position(team_key, "PG")
                if depth_chart:
                    logger.info(f"Using depth chart features for {team_key}")
                
                # Get all depth charts for this team
                team_depth_charts = [
                    self.depth_chart_features.builder.get_depth_chart_for_position(team_key, pos)
                    for pos in ["PG", "SG", "SF", "PF", "C"]
                ]
                
                for position_depth_chart in team_depth_charts:
                    for entry in position_depth_chart:
                        starter_probs[entry.player_name] = entry.starter_probability
                
                if starter_probs:
                    return starter_probs
            
            # Fallback to historical data if depth chart not available
            training_path = DEFAULT_PROJECT_DIR / "data" / "training_data.csv"
            if not training_path.exists():
                return {}
            
            df = pd.read_csv(training_path)
            
            # Get team data
            team_data = df[df['team'] == team_key.upper()]
            
            if team_data.empty:
                return {}
            
            # Calculate starter probability for each player
            starter_probs = {}
            for player_name in team_data['player_name'].unique():
                player_data = team_data[team_data['player_name'] == player_name]
                
                # Count games started vs total appearances
                games_started = (player_data['starter'] == 1).sum()
                total_games = len(player_data)
                
                if total_games > 0:
                    starter_probs[player_name] = games_started / total_games
            
            return starter_probs
        except Exception as e:
            logger.error(f"Error computing starter probability model: {e}")
            return {}
    
    def _get_team_roster(self, game_date: str, team_key: str) -> pd.DataFrame:
        """Get team roster for a specific date"""
        try:
            training_path = DEFAULT_PROJECT_DIR / "data" / "training_data.csv"
            if not training_path.exists():
                return pd.DataFrame()
            
            df = pd.read_csv(training_path)
            
            # Convert dates
            df['game_date'] = pd.to_datetime(df['game_date'])
            game_date_dt = pd.to_datetime(game_date)
            
            # Get team data around the game date (within 30 days before)
            team_data = df[
                (df['team'] == team_key.upper()) &
                (df['game_date'] <= game_date_dt) &
                (df['game_date'] >= game_date_dt - pd.Timedelta(days=30))
            ].copy()
            
            if team_data.empty:
                return pd.DataFrame()
            
            # Get unique players
            roster = team_data.groupby('player_name').agg({
                'position': 'first',
                'starter': 'mean',  # Average starter percentage
                'game_date': 'max',  # Most recent game
            }).reset_index()
            
            roster['player_key'] = roster['player_name'].str.lower().str.replace(' ', '_')
            roster['team_name'] = team_key
            roster['team_key'] = team_key.upper()
            
            return roster
        except Exception as e:
            logger.error(f"Error loading team roster: {e}")
            return pd.DataFrame()
    
    def get_simulation_summary(self) -> Dict[str, Any]:
        """Get summary of simulation progress"""
        return {
            "state": self.state,
            "total_starter_predictions": len(self.starter_predictions),
            "total_statline_predictions": len(self.statline_predictions),
            "total_comparisons": len(self.comparison_results),
            "comparison_results": self.comparison_results,
        }


def create_simulation_engine(cloud_brain: CloudBrain) -> SimulationEngine:
    """Factory function to create simulation engine"""
    return SimulationEngine(cloud_brain)
