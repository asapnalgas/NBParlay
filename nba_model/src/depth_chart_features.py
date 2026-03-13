"""
Depth Chart Features: Enhanced player depth and position-based features

This module extracts depth chart information from historical game data to:
- Identify starting vs bench players by position
- Calculate positional depth charts
- Generate features related to player position hierarchy
- Predict starter probability based on depth chart position
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
from collections import defaultdict
from dataclasses import dataclass

import pandas as pd
import numpy as np

try:
    from .features import DEFAULT_PROJECT_DIR
except ImportError:
    from features import DEFAULT_PROJECT_DIR

logger = logging.getLogger(__name__)

DEFAULT_DEPTH_CHART_CACHE_PATH = DEFAULT_PROJECT_DIR / "data" / "depth_chart_cache.csv"
DEFAULT_TEAM_POSITIONS_PATH = DEFAULT_PROJECT_DIR / "data" / "team_positions.json"

BASKETBALL_POSITIONS = ["PG", "SG", "SF", "PF", "C"]
POSITION_GROUPS = {
    "Guard": ["PG", "SG"],
    "Wing": ["SG", "SF"],
    "Forward": ["SF", "PF"],
    "Big": ["PF", "C"],
}


@dataclass
class DepthChartEntry:
    """Entry in a team's depth chart"""
    team_key: str
    position: str
    player_key: str
    player_name: str
    depth_rank: int  # 1 = starter, 2 = bench, etc.
    starter_probability: float
    minutes_per_game: float
    games_played: int
    last_updated: str


class DepthChartBuilder:
    """Builds and maintains team depth charts from historical data"""
    
    def __init__(self, training_data_path: Optional[Path] = None):
        """Initialize depth chart builder"""
        self.training_data_path = training_data_path or DEFAULT_PROJECT_DIR / "data" / "training_data.csv"
        self.depth_charts: Dict[str, List[DepthChartEntry]] = {}
        self.team_rosters: Dict[str, Dict[str, str]] = {}  # team -> {player_key: position}
        self.position_stats: Dict[str, Dict[str, Any]] = defaultdict(dict)
        
    def build_depth_charts(self, lookback_days: int = 365, min_games: int = 5) -> Dict[str, List[DepthChartEntry]]:
        """
        Build depth charts from historical game data
        
        Args:
            lookback_days: Only consider games in last N days
            min_games: Only include players with at least N games
        """
        if not self.training_data_path.exists():
            logger.warning(f"Training data not found at {self.training_data_path}")
            return {}
        
        df = pd.read_csv(self.training_data_path)
        
        # Filter by date
        df['game_date'] = pd.to_datetime(df['game_date'])
        cutoff_date = pd.Timestamp.now() - timedelta(days=lookback_days)
        df = df[df['game_date'] >= cutoff_date]
        
        # Build depth chart per team/position
        for (team_key, position), group in df.groupby(['team', 'position']):
            if pd.isna(position):
                continue
            
            # Only include players with enough playing time
            player_stats = []
            for player_name, player_group in group.groupby('player_name'):
                games_played = len(player_group)
                if games_played < min_games:
                    continue
                
                player_key = self._normalize_player_key(player_name, team_key)
                # Handle missing 'starter' column gracefully
                if 'starter' in player_group.columns:
                    starter_games = (player_group['starter'] == 1).sum()
                else:
                    starter_games = 0
                avg_minutes = player_group['minutes'].mean() if 'minutes' in player_group.columns else 0
                
                starter_prob = starter_games / games_played if games_played > 0 else 0.0
                
                player_stats.append({
                    'player_key': player_key,
                    'player_name': player_name,
                    'games_played': games_played,
                    'starter_games': starter_games,
                    'starter_probability': starter_prob,
                    'avg_minutes': avg_minutes,
                    'total_minutes': player_group['minutes'].sum() if 'minutes' in player_group.columns else 0,
                })
            
            # Sort by starter probability (descending) then minutes (descending)
            player_stats.sort(
                key=lambda x: (-x['starter_probability'], -x['avg_minutes']),
                reverse=False
            )
            
            # Create depth chart entries
            team_pos_key = f"{team_key}_{position}"
            self.depth_charts[team_pos_key] = []
            
            for rank, stat in enumerate(player_stats, 1):
                entry = DepthChartEntry(
                    team_key=team_key,
                    position=position,
                    player_key=stat['player_key'],
                    player_name=stat['player_name'],
                    depth_rank=rank,
                    starter_probability=stat['starter_probability'],
                    minutes_per_game=stat['avg_minutes'],
                    games_played=stat['games_played'],
                    last_updated=datetime.now().isoformat(),
                )
                self.depth_charts[team_pos_key].append(entry)
                
                # Store position stats
                if team_key not in self.team_rosters:
                    self.team_rosters[team_key] = {}
                self.team_rosters[team_key][stat['player_key']] = position
        
        logger.info(f"Built depth charts for {len(self.depth_charts)} team/position combinations")
        return self.depth_charts
    
    def get_depth_chart_for_position(self, team_key: str, position: str) -> List[DepthChartEntry]:
        """Get depth chart for a specific team/position"""
        key = f"{team_key}_{position}"
        return self.depth_charts.get(key, [])
    
    def get_player_depth_rank(self, team_key: str, player_key: str, position: str) -> int:
        """Get a player's depth chart rank (1=starter, 2=bench, etc.)"""
        depth_chart = self.get_depth_chart_for_position(team_key, position)
        for entry in depth_chart:
            if entry.player_key == player_key:
                return entry.depth_rank
        return 999  # Not in depth chart
    
    def get_player_starter_probability(self, team_key: str, player_key: str, position: str) -> float:
        """Get starter probability for a player based on depth chart"""
        depth_chart = self.get_depth_chart_for_position(team_key, position)
        for entry in depth_chart:
            if entry.player_key == player_key:
                return entry.starter_probability
        return 0.0
    
    def _normalize_player_key(self, player_name: str, team_key: str) -> str:
        """Normalize player names to create consistent keys"""
        return player_name.lower().replace(" ", "_").replace(".", "")


class DepthChartFeatures:
    """Generates depth chart features for predictions"""
    
    def __init__(self, depth_chart_builder: Optional[DepthChartBuilder] = None):
        """Initialize depth chart features"""
        self.builder = depth_chart_builder or DepthChartBuilder()
        if not self.builder.depth_charts:
            self.builder.build_depth_charts()
    
    def get_starter_probability_from_depth(self, team_key: str, player_key: str, 
                                          position: str) -> float:
        """
        Get starter probability from depth chart
        
        Returns value between 0 and 1 based on:
        - Rank in depth chart (1st is ~0.85, 2nd is ~0.35, 3rd+ is ~0.1)
        - Historical starter probability
        """
        rank = self.builder.get_player_depth_rank(team_key, player_key, position)
        hist_prob = self.builder.get_player_starter_probability(team_key, player_key, position)
        
        # Combine depth rank and historical probability
        rank_factor = max(0.05, 0.85 / (1 + (rank - 1) * 0.4))  # Decreasing by rank
        combined_prob = 0.6 * hist_prob + 0.4 * rank_factor
        
        return min(1.0, max(0.0, combined_prob))
    
    def get_bench_depth(self, team_key: str, position: str) -> Dict[str, Any]:
        """
        Get bench depth information for a position
        
        Returns stats about the bench players available
        """
        depth_chart = self.builder.get_depth_chart_for_position(team_key, position)
        
        if len(depth_chart) < 2:
            return {
                'starter': None,
                'bench_players': 0,
                'bench_minutes': 0.0,
                'bench_depth_score': 0.0,
            }
        
        starter = depth_chart[0]
        bench_players = depth_chart[1:]
        
        bench_minutes = sum(e.minutes_per_game for e in bench_players)
        bench_depth_score = sum(e.minutes_per_game * (2 - e.starter_probability) for e in bench_players)
        
        return {
            'starter': starter,
            'bench_players': len(bench_players),
            'bench_minutes': bench_minutes,
            'bench_depth_score': bench_depth_score,
            'second_option_prob': bench_players[0].starter_probability if bench_players else 0.0,
        }
    
    def get_position_group_strength(self, team_key: str, position_group: str) -> Dict[str, Any]:
        """
        Get combined strength of a position group (Guard, Wing, Forward, Big)
        
        Useful for understanding team composition
        """
        positions = POSITION_GROUPS.get(position_group, [])
        
        combined_stats = {
            'total_starters': 0,
            'total_bench_depth': 0.0,
            'positions_covered': 0,
            'position_strengths': {},
        }
        
        for position in positions:
            depth = self.get_bench_depth(team_key, position)
            if depth['starter']:
                combined_stats['total_starters'] += 1
                combined_stats['total_bench_depth'] += depth['bench_depth_score']
                combined_stats['positions_covered'] += 1
                combined_stats['position_strengths'][position] = {
                    'starter': depth['starter'].player_name,
                    'bench_depth': depth['bench_depth_score'],
                    'bench_players': depth['bench_players'],
                }
        
        return combined_stats
    
    def is_player_in_depot(self, team_key: str, player_key: str, position: str, 
                           depth_threshold: int = 3) -> bool:
        """
        Check if player is in the typical depth chart position
        (not a deep bench player)
        """
        rank = self.builder.get_player_depth_rank(team_key, player_key, position)
        return rank <= depth_threshold
    
    def get_all_features_for_player(self, team_key: str, player_key: str, 
                                    player_name: str, position: str | None) -> Dict[str, float]:
        """
        Get comprehensive depth chart features for a player
        
        Returns dictionary of depth-chart-specific features
        """
        if pd.isna(position) or position not in BASKETBALL_POSITIONS:
            position = self._infer_position(team_key, player_name)
        
        if not position or position not in BASKETBALL_POSITIONS:
            return self._get_default_features()
        
        starter_prob = self.get_starter_probability_from_depth(team_key, player_key, position)
        depth_rank = self.builder.get_player_depth_rank(team_key, player_key, position)
        bench_depth = self.get_bench_depth(team_key, position)
        
        features = {
            'depth_chart_starter_probability': starter_prob,
            'depth_chart_rank': float(min(depth_rank, 5)),  # Cap at 5
            'is_starter': float(depth_rank <= 1),
            'bench_depth_score': bench_depth['bench_depth_score'],
            'bench_players_available': float(bench_depth['bench_players']),
            'position_group_strength': float(
                self.get_position_group_strength(team_key, self._get_position_group(position))
                .get('total_bench_depth', 0.0)
            ),
            'in_rotation': float(self.is_player_in_depot(team_key, player_key, position)),
        }
        
        return features
    
    def _infer_position(self, team_key: str, player_name: str) -> str | None:
        """Infer position from historical data"""
        if team_key in self.builder.team_rosters:
            player_key = self._normalize_player_key(player_name, team_key)
            return self.builder.team_rosters[team_key].get(player_key)
        return None
    
    def _normalize_player_key(self, player_name: str, team_key: str) -> str:
        """Normalize player names to create consistent keys"""
        return player_name.lower().replace(" ", "_").replace(".", "")
    
    def _get_position_group(self, position: str) -> str:
        """Get position group from position"""
        for group, positions in POSITION_GROUPS.items():
            if position in positions:
                return group
        return "Forward"  # Default
    
    def _get_default_features(self) -> Dict[str, float]:
        """Return default features when position is unknown"""
        return {
            'depth_chart_starter_probability': 0.4,
            'depth_chart_rank': 3.0,
            'is_starter': 0.0,
            'bench_depth_score': 0.0,
            'bench_players_available': 0.0,
            'position_group_strength': 0.0,
            'in_rotation': 0.0,
        }


def create_depth_chart_features() -> DepthChartFeatures:
    """Factory function to create depth chart features"""
    builder = DepthChartBuilder()
    builder.build_depth_charts()
    return DepthChartFeatures(builder)


def get_depth_chart_features() -> DepthChartFeatures:
    """Get or create global depth chart features instance"""
    global _depth_chart_features_instance
    if '_depth_chart_features_instance' not in globals():
        _depth_chart_features_instance = create_depth_chart_features()
    return _depth_chart_features_instance
