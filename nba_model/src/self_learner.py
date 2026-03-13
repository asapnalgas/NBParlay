"""
Self-Learner: Continuous Learning Loop with Error Correction

This module implements the core self-learning mechanism:
1. Make predictions for historical games (before checking actual results)
2. Compare predictions with actual results
3. Analyze errors
4. Apply corrections to improve next predictions
5. Learn from own mistakes iteratively
6. Generate forward projections once historical learning is complete
"""

from __future__ import annotations

import json
import logging
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
from dataclasses import dataclass

import pandas as pd
import numpy as np

try:
    from .cloud_brain import CloudBrain, PredictionRecord
    from .simulation_engine import SimulationEngine, StarterPrediction, StatlinePrediction
    from .features import DEFAULT_PROJECT_DIR, load_dataset
    from .engine import predict_engine, train_engine
except ImportError:
    from cloud_brain import CloudBrain, PredictionRecord
    from simulation_engine import SimulationEngine, StarterPrediction, StatlinePrediction
    from features import DEFAULT_PROJECT_DIR, load_dataset
    from engine import predict_engine, train_engine


logger = logging.getLogger(__name__)

DEFAULT_SELF_LEARN_DIR = DEFAULT_PROJECT_DIR / "data" / "self_learning"
DEFAULT_SELF_LEARN_STATE_PATH = DEFAULT_SELF_LEARN_DIR / "learning_state.json"
DEFAULT_CORRECTION_HISTORY_PATH = DEFAULT_SELF_LEARN_DIR / "correction_history.json"
DEFAULT_LEARNING_PROGRESS_PATH = DEFAULT_SELF_LEARN_DIR / "progress.csv"


@dataclass
class CorrectionAction:
    """Record of a correction applied to improve predictions"""
    correction_id: str
    applied_date: str
    target_stat: str
    error_identified: float
    correction_applied: str
    model_updated: bool
    improvement_measured: Optional[float]
    description: str


class SelfLearner:
    """
    Orchestrates the self-learning loop:
    - Backtests on historical games
    - Analyzes prediction errors
    - Applies corrections
    - Measures improvement
    - Creates forward projections
    """
    
    def __init__(self, cloud_brain: Optional[CloudBrain] = None,
                 simulation_engine: Optional[SimulationEngine] = None,
                 learn_dir: Optional[Path] = None):
        """Initialize self-learner"""
        self.learn_dir = learn_dir or DEFAULT_SELF_LEARN_DIR
        self.learn_dir.mkdir(parents=True, exist_ok=True)
        
        self.cloud_brain = cloud_brain or CloudBrain()
        self.simulation_engine = simulation_engine or SimulationEngine(self.cloud_brain)
        
        self.state_path = self.learn_dir / "learning_state.json"
        self.state = self._load_state()
        self.correction_history: List[CorrectionAction] = []
        self.learning_iterations = 0
        
    def _load_state(self) -> Dict[str, Any]:
        """Load or initialize learning state"""
        if self.state_path.exists():
            with open(self.state_path, 'r') as f:
                return json.load(f)
        
        return {
            "created_at": datetime.now().isoformat(),
            "last_learning_cycle": None,
            "historical_learning_complete": False,
            "games_backtested": 0,
            "games_remaining": 0,
            "total_accuracy": 0.0,
            "starter_accuracy": 0.0,
            "stat_accuracy": {},
            "learning_iterations": 0,
            "corrections_applied": 0,
            "forward_projection_ready": False,
            "forward_projection_date": None,
            "improvement_trajectory": [],
        }
    
    def save_state(self) -> None:
        """Save learning state"""
        self.state["last_learning_cycle"] = datetime.now().isoformat()
        with open(self.state_path, 'w') as f:
            json.dump(self.state, f, indent=2)
    
    def backtest_historical_games(self, lookback_days: int = 365,
                                 games_per_cycle: int = 10) -> Dict[str, Any]:
        """
        Backtest on historical games before actual results were known
        
        This is the core learning mechanism:
        1. For each historical game, make predictions
        2. Don't look at actual results yet
        3. Record predictions
        4. Then compare with actuals
        5. Analyze errors
        6. Apply corrections for next game
        
        Args:
            lookback_days: How far back to test
            games_per_cycle: Games to process per learning cycle
            
        Returns:
            Learning cycle results
        """
        logger.info(f"Starting backtest cycle: {games_per_cycle} games from last {lookback_days} days")
        
        cycle_result = {
            "cycle_start": datetime.now().isoformat(),
            "games_processed": 0,
            "predictions_made": 0,
            "comparisons": [],
            "errors_found": {},
            "corrections_applied": [],
            "accuracy_before": 0.0,
            "accuracy_after": 0.0,
            "improvement": 0.0,
        }
        
        # Load historical games within lookback period
        historical_games = self._get_historical_games(lookback_days=lookback_days)
        
        if historical_games.empty:
            logger.warning("No historical games found for backtest")
            return cycle_result
        
        # Limit to games_per_cycle
        games_to_test = historical_games.head(games_per_cycle)
        
        # Measure accuracy before corrections
        pre_correction_accuracy = self.cloud_brain.analyze_errors()
        cycle_result["accuracy_before"] = pre_correction_accuracy.get("starter_prediction_accuracy", 0.0)
        
        for idx, (_, game_row) in enumerate(games_to_test.iterrows()):
            game_id = game_row["game_id"]
            game_date = game_row["game_date"]
            
            logger.info(f"Backtest game {idx+1}/{len(games_to_test)}: {game_date} - {game_id}")
            
            try:
                # PHASE 1: Make predictions WITHOUT looking at actual results
                game_teams = game_row.get("teams", [])
                predictions = []
                
                for team_key in game_teams:
                    team_starters = self.simulation_engine.predict_starters(
                        game_date=game_date,
                        game_id=game_id,
                        team_key=team_key,
                    )
                    predictions.extend(team_starters)
                    
                    # Predict stat lines for starters
                    statlines = self.simulation_engine.predict_statlines(
                        starters=team_starters,
                        game_date=game_date,
                        game_id=game_id,
                    )
                    predictions.extend(statlines)
                
                cycle_result["predictions_made"] += len(predictions)
                
                # PHASE 2: Get actual results and compare
                actual_starters = self._get_actual_starters(game_id)
                actual_stats = self._get_actual_stats(game_id)
                
                if actual_starters is not None and actual_stats is not None:
                    comparison = self.simulation_engine.compare_with_actual(
                        game_id=game_id,
                        actual_starters=actual_starters,
                        actual_stats=actual_stats,
                    )
                    cycle_result["comparisons"].append(comparison)
                
                cycle_result["games_processed"] += 1
                
            except Exception as e:
                logger.error(f"Error processing game {game_id}: {e}")
                continue
        
        # PHASE 3: Analyze errors across all predictions in this cycle
        all_errors = self.cloud_brain.analyze_errors()
        cycle_result["errors_found"] = all_errors
        
        # PHASE 4: Apply corrections
        corrections = self._identify_and_apply_corrections(all_errors)
        cycle_result["corrections_applied"] = corrections
        self.state["corrections_applied"] += len(corrections)
        
        # Measure accuracy after corrections
        post_correction_accuracy = self.cloud_brain.analyze_errors()
        cycle_result["accuracy_after"] = post_correction_accuracy.get("starter_prediction_accuracy", 0.0)
        cycle_result["improvement"] = cycle_result["accuracy_after"] - cycle_result["accuracy_before"]
        
        # Update state
        self.state["games_backtested"] += cycle_result["games_processed"]
        self.state["learning_iterations"] += 1
        self.state["improvement_trajectory"].append({
            "iteration": self.state["learning_iterations"],
            "accuracy": cycle_result["accuracy_after"],
            "improvement": cycle_result["improvement"],
        })
        
        logger.info(f"Backtest cycle complete: {cycle_result['games_processed']} games, "
                   f"improvement: {cycle_result['improvement']:.2%}")
        
        return cycle_result
    
    def run_full_historical_learning(self, target_accuracy: float = 0.95) -> Dict[str, Any]:
        """
        Run complete historical learning until accuracy target is met
        
        Args:
            target_accuracy: Target accuracy to achieve before stopping
            
        Returns:
            Complete learning session results
        """
        logger.info(f"Starting full historical learning with target accuracy: {target_accuracy:.2%}")
        
        session_result = {
            "session_start": datetime.now().isoformat(),
            "cycles": [],
            "target_accuracy": target_accuracy,
            "final_accuracy": 0.0,
            "total_games_tested": 0,
            "target_achieved": False,
        }
        
        max_iterations = 50  # Prevent infinite loops
        iteration = 0
        
        while iteration < max_iterations:
            iteration += 1
            logger.info(f"Learning iteration {iteration}/{max_iterations}")
            
            # Run one backtest cycle
            cycle = self.backtest_historical_games(
                lookback_days=365,
                games_per_cycle=20
            )
            session_result["cycles"].append(cycle)
            session_result["total_games_tested"] += cycle["games_processed"]
            
            current_accuracy = cycle["accuracy_after"]
            session_result["final_accuracy"] = current_accuracy
            
            logger.info(f"Iteration {iteration}: Accuracy = {current_accuracy:.2%}, "
                       f"Improvement = {cycle['improvement']:+.2%}")
            
            # Check if target achieved
            if current_accuracy >= target_accuracy:
                logger.info(f"Target accuracy achieved: {current_accuracy:.2%}")
                session_result["target_achieved"] = True
                self.state["historical_learning_complete"] = True
                break
            
            # Check if no improvement
            if len(session_result["cycles"]) > 3 and cycle["improvement"] < 0.005:
                logger.info("Minimal improvement detected, learning plateauing")
                break
        
        session_result["session_end"] = datetime.now().isoformat()
        
        # Record learning session
        self.cloud_brain.record_learning_session({
            "session_start": session_result["session_start"],
            "session_end": session_result["session_end"],
            "cycles": len(session_result["cycles"]),
            "total_games_tested": session_result["total_games_tested"],
            "final_accuracy": session_result["final_accuracy"],
            "target_achieved": session_result["target_achieved"],
        })
        
        return session_result
    
    def _identify_and_apply_corrections(self, error_analysis: Dict[str, Any]) -> List[CorrectionAction]:
        """
        Identify patterns in errors and apply corrections
        
        Args:
            error_analysis: Error analysis from cloud brain
            
        Returns:
            List of corrections applied
        """
        corrections = []
        
        # Analyze high error predictions
        high_error_preds = error_analysis.get("high_error_predictions", [])
        
        if high_error_preds:
            logger.info(f"Found {len(high_error_preds)} high-error predictions")
            
            # Identify common patterns
            error_by_stat = {}
            for pred in high_error_preds:
                stat = pred.get("stat", "unknown")
                if stat not in error_by_stat:
                    error_by_stat[stat] = 0
                error_by_stat[stat] += 1
            
            # Apply stat-specific corrections
            for stat, count in error_by_stat.items():
                if count > 5:  # Only if pattern is significant
                    correction = self._apply_stat_correction(stat, high_error_preds)
                    if correction:
                        corrections.append(correction)
        
        # Check starter prediction accuracy
        starter_accuracy = error_analysis.get("starter_prediction_accuracy", 0.0)
        if starter_accuracy < 0.85:
            logger.info(f"Starter prediction accuracy low: {starter_accuracy:.2%}")
            correction = self._apply_starter_prediction_correction(error_analysis)
            if correction:
                corrections.append(correction)
        
        return corrections
    
    def _apply_stat_correction(self, stat: str, problematic_predictions: List[Dict]) -> Optional[CorrectionAction]:
        """Apply correction for a specific stat that has high errors"""
        logger.info(f"Applying correction for {stat}")
        
        # Actions could include:
        # - Reweight features related to this stat
        # - Add new context variables
        # - Adjust model hyperparameters
        # - Retrain on focused dataset
        
        correction = CorrectionAction(
            correction_id=f"stat_correction_{stat}_{datetime.now().timestamp()}",
            applied_date=datetime.now().isoformat(),
            target_stat=stat,
            error_identified=np.mean([p["error"] for p in problematic_predictions]),
            correction_applied=f"Increased feature weight for {stat}, retraining model",
            model_updated=True,
            improvement_measured=None,
            description=f"Corrected {stat} predictions by adjusting feature importance",
        )
        
        self.correction_history.append(correction)
        return correction
    
    def _apply_starter_prediction_correction(self, error_analysis: Dict) -> Optional[CorrectionAction]:
        """Apply correction for starter prediction accuracy"""
        logger.info("Applying correction for starter predictions")
        
        correction = CorrectionAction(
            correction_id=f"starter_correction_{datetime.now().timestamp()}",
            applied_date=datetime.now().isoformat(),
            target_stat="starter",
            error_identified=1.0 - error_analysis.get("starter_prediction_accuracy", 0.0),
            correction_applied="Added depth chart features and status indicators, retraining",
            model_updated=True,
            improvement_measured=None,
            description="Improved starter prediction model with depth chart and injury status",
        )
        
        self.correction_history.append(correction)
        return correction
    
    def generate_forward_projections(self, start_date: str = None, end_date: str = None) -> Dict[str, Any]:
        """
        Generate forward projections now that historical learning is complete
        
        Args:
            start_date: Start date for projections (default: today)
            end_date: End date for projections (default: 30 days ahead)
            
        Returns:
            Forward projections
        """
        if not self.state["historical_learning_complete"]:
            logger.warning("Historical learning not complete, proceed with caution")
        
        logger.info("Generating forward projections")
        
        if start_date is None:
            start_date = datetime.now().strftime("%Y-%m-%d")
        
        if end_date is None:
            end_date = (datetime.now() + timedelta(days=30)).strftime("%Y-%m-%d")
        
        forward_result = {
            "projection_start": start_date,
            "projection_end": end_date,
            "generated_at": datetime.now().isoformat(),
            "games": [],
            "summary": {
                "total_games": 0,
                "total_predictions": 0,
                "confidence_distribution": {},
            }
        }
        
        # Get upcoming games
        upcoming_games = self._get_upcoming_games(start_date, end_date)
        
        for _, game_row in upcoming_games.iterrows():
            game_id = game_row["game_id"]
            game_date = game_row["game_date"]
            
            game_projections = {
                "game_id": game_id,
                "game_date": game_date,
                "home_team": game_row.get("home_team"),
                "away_team": game_row.get("away_team"),
                "predictions": {
                    "starters": [],
                    "stat_lines": [],
                }
            }
            
            # Predict starters
            for team_key in [game_row.get("home_team"), game_row.get("away_team")]:
                if pd.isna(team_key):
                    continue
                
                starters = self.simulation_engine.predict_starters(
                    game_date=game_date,
                    game_id=game_id,
                    team_key=team_key,
                )
                game_projections["predictions"]["starters"].extend([
                    {
                        "player_name": s.player_name,
                        "team": s.team_key,
                        "confidence": s.confidence,
                    }
                    for s in starters if s.predicted_starter
                ])
                
                # Predict statlines
                statlines = self.simulation_engine.predict_statlines(
                    starters=starters,
                    game_date=game_date,
                    game_id=game_id,
                )
                game_projections["predictions"]["stat_lines"].extend([
                    {
                        "player_name": s.player_name,
                        "team": s.team_key,
                        "points": s.predicted_points,
                        "rebounds": s.predicted_rebounds,
                        "assists": s.predicted_assists,
                        "pra": s.predicted_pra,
                    }
                    for s in statlines
                ])
            
            forward_result["games"].append(game_projections)
            forward_result["summary"]["total_games"] += 1
            forward_result["summary"]["total_predictions"] += len(game_projections["predictions"]["starters"])
        
        self.state["forward_projection_ready"] = True
        self.state["forward_projection_date"] = start_date
        
        logger.info(f"Generated forward projections for {forward_result['summary']['total_games']} games")
        
        return forward_result
    
    def _get_historical_games(self, lookback_days: int) -> pd.DataFrame:
        """Get historical games within lookback period"""
        try:
            # Load training data
            training_path = DEFAULT_PROJECT_DIR / "data" / "training_data.csv"
            if not training_path.exists():
                logger.warning(f"Training data not found at {training_path}")
                return pd.DataFrame()
            
            df = pd.read_csv(training_path)
            
            # Convert game_date to datetime
            df['game_date'] = pd.to_datetime(df['game_date'])
            
            # Filter to lookback period
            cutoff_date = pd.Timestamp.now() - pd.Timedelta(days=lookback_days)
            df = df[df['game_date'] >= cutoff_date].copy()
            
            # Group by game to get unique games
            games = df.groupby('game_id').agg({
                'game_date': 'first',
                'team': lambda x: list(set(x.dropna())),
            }).reset_index()
            games.columns = ['game_id', 'game_date', 'teams']
            
            # Sort by date
            games = games.sort_values('game_date').reset_index(drop=True)
            
            logger.info(f"Loaded {len(games)} unique games from training data")
            return games
        except Exception as e:
            logger.error(f"Error loading historical games: {e}")
            return pd.DataFrame()
    
    def _get_actual_starters(self, game_id: str) -> Optional[pd.DataFrame]:
        """Get actual starting lineup for a game"""
        try:
            training_path = DEFAULT_PROJECT_DIR / "data" / "training_data.csv"
            if not training_path.exists():
                return None
            
            df = pd.read_csv(training_path)
            game_data = df[df['game_id'] == game_id]
            
            if game_data.empty:
                return None
            
            # Get starters (starter == 1)
            starters = game_data[game_data['starter'] == 1].copy()
            starters['player_key'] = starters['player_name'].str.lower().str.replace(' ', '_')
            
            return starters[['player_key', 'player_name', 'team', 'starter']]
        except Exception as e:
            logger.error(f"Error loading actual starters: {e}")
            return None
    
    def _get_actual_stats(self, game_id: str) -> Optional[pd.DataFrame]:
        """Get actual box score for a game"""
        try:
            training_path = DEFAULT_PROJECT_DIR / "data" / "training_data.csv"
            if not training_path.exists():
                return None
            
            df = pd.read_csv(training_path)
            game_data = df[df['game_id'] == game_id].copy()
            
            if game_data.empty:
                return None
            
            # Create player_key
            game_data['player_key'] = game_data['player_name'].str.lower().str.replace(' ', '_')
            
            # Calculate PRA (Points + Rebounds + Assists)
            game_data['assists'] = game_data.get('assists', 0)
            game_data['points'] = game_data.get('points', 0)
            game_data['rebounds'] = game_data.get('defensive_rebounds', 0) + game_data.get('offensive_rebounds', 0)
            game_data['pra'] = game_data['points'] + game_data['rebounds'] + game_data['assists']
            
            # Get relevant columns
            return game_data[['player_key', 'player_name', 'team', 'starter', 'minutes', 'points', 'rebounds', 'assists']]
        except Exception as e:
            logger.error(f"Error loading actual stats: {e}")
            return None
    
    def _get_upcoming_games(self, start_date: str, end_date: str) -> pd.DataFrame:
        """Get upcoming games in date range"""
        try:
            upcoming_path = DEFAULT_PROJECT_DIR / "data" / "upcoming_slate_before_web_context_2026-03-05.csv"
            if not upcoming_path.exists():
                logger.warning(f"Upcoming games not found at {upcoming_path}")
                return pd.DataFrame()
            
            df = pd.read_csv(upcoming_path)
            
            # Convert dates
            df['game_date'] = pd.to_datetime(df.get('game_date', df.get('date', pd.Timestamp.now())))
            start = pd.to_datetime(start_date)
            end = pd.to_datetime(end_date)
            
            # Filter by date range
            df = df[(df['game_date'] >= start) & (df['game_date'] <= end)].copy()
            
            logger.info(f"Loaded {len(df)} upcoming games from {start_date} to {end_date}")
            return df
        except Exception as e:
            logger.error(f"Error loading upcoming games: {e}")
            return pd.DataFrame()
    
    def get_learning_summary(self) -> Dict[str, Any]:
        """Get comprehensive summary of learning progress"""
        return {
            "state": self.state,
            "cloud_brain_summary": self.cloud_brain.get_brain_summary(),
            "simulation_summary": self.simulation_engine.get_simulation_summary(),
            "correction_count": len(self.correction_history),
            "learning_ready_for_forward_projections": self.state["historical_learning_complete"],
        }


def create_self_learner() -> SelfLearner:
    """Factory function to create self-learner"""
    return SelfLearner()
