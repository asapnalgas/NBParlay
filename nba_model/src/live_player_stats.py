"""
Live Player Stats Pipeline
Continuously pulls live player performance data and projected stats
"""

import logging
import json
from datetime import datetime, timedelta
from typing import Dict, List, Optional
import threading
import time
import random

logger = logging.getLogger(__name__)


class LivePlayerStatsCollector:
    """Collects live player statistics and projections"""
    
    def __init__(self, update_interval: int = 30):
        """
        Initialize stats collector
        Args:
            update_interval: Seconds between updates
        """
        self.update_interval = update_interval
        self.is_running = False
        self.collector_thread = None
        self.stats_cache = {}
        self.callbacks = []
        
    def register_callback(self, callback):
        """Register callback for stat updates"""
        self.callbacks.append(callback)
        
    def start(self):
        """Start collecting stats"""
        if self.is_running:
            logger.warning("Stats collector already running")
            return
            
        self.is_running = True
        self.collector_thread = threading.Thread(target=self._collection_loop, daemon=True)
        self.collector_thread.start()
        logger.info("✓ Live player stats collector started")
        
    def stop(self):
        """Stop collecting stats"""
        self.is_running = False
        if self.collector_thread:
            self.collector_thread.join(timeout=5)
        logger.info("✓ Live player stats collector stopped")
        
    def _collection_loop(self):
        """Main collection loop"""
        while self.is_running:
            try:
                stats = self._fetch_player_stats()
                self._process_stats_updates(stats)
                time.sleep(self.update_interval)
            except Exception as e:
                logger.error(f"Error in collection loop: {e}")
                time.sleep(self.update_interval)
                
    def _fetch_player_stats(self) -> List[Dict]:
        """Fetch current player stats from data sources"""
        # This would integrate with actual NBA stats APIs
        stats = []
        
        try:
            # Placeholder for real API integration
            # In production, would fetch from:
            # - NBA Stats API (stats.nba.com)
            # - ESPN API
            # - RealGM
            # - Other sports data providers
            
            stats = self._get_sample_player_stats()
            
        except Exception as e:
            logger.error(f"Error fetching stats: {e}")
            
        return stats
        
    def _get_sample_player_stats(self) -> List[Dict]:
        """Get sample player data (placeholder for real API)"""
        # This will be replaced with actual API calls
        sample_players = [
            "Jayson Tatum", "Ja Morant", "Luka Doncic", "Kevin Durant",
            "Stephen Curry", "Giannis Antetokounmpo", "Joel Embiid",
            "Cooper Flagg", "Donovan Mitchell", "Kawhi Leonard"
        ]
        
        stats = []
        for player_name in sample_players:
            stats.append({
                "player_name": player_name,
                "team": self._get_team_for_player(player_name),
                "points": round(random.uniform(15, 35), 1),
                "rebounds": round(random.uniform(4, 15), 1),
                "assists": round(random.uniform(3, 12), 1),
                "steals": round(random.uniform(1, 5), 1),
                "blocks": round(random.uniform(0.5, 4), 1),
                "turnovers": round(random.uniform(1, 5), 1),
                "field_goal_pct": round(random.uniform(0.35, 0.55), 2),
                "three_point_pct": round(random.uniform(0.20, 0.45), 2),
                "free_throw_pct": round(random.uniform(0.70, 0.95), 2),
                "minutes_played": round(random.uniform(20, 40), 1),
                "plus_minus": round(random.uniform(-15, 25), 1),
                "timestamp": datetime.now().isoformat(),
                "game_status": random.choice(["live", "upcoming", "completed"]),
            })
            
        return stats
        
    def _get_team_for_player(self, player_name: str) -> str:
        """Get team code for player"""
        player_teams = {
            "Jayson Tatum": "BOS",
            "Ja Morant": "MEM",
            "Luka Doncic": "DAL",
            "Kevin Durant": "PHX",
            "Stephen Curry": "GSW",
            "Giannis Antetokounmpo": "MIL",
            "Joel Embiid": "PHI",
            "Cooper Flagg": "POR",
            "Donovan Mitchell": "CLE",
            "Kawhi Leonard": "LAC",
        }
        return player_teams.get(player_name, "TBA")
        
    def _process_stats_updates(self, stats: List[Dict]):
        """Process stat updates and call callbacks"""
        for stat_entry in stats:
            player_key = f"{stat_entry['player_name']}_{stat_entry['team']}"
            
            # Check if stats changed
            if player_key in self.stats_cache:
                old_stats = self.stats_cache[player_key]
                if self._stats_changed(old_stats, stat_entry):
                    self._notify_update(stat_entry)
            else:
                self._notify_update(stat_entry)
                
            self.stats_cache[player_key] = stat_entry
            
    def _stats_changed(self, old: Dict, new: Dict) -> bool:
        """Check if player stats have changed"""
        return (old.get('points') != new.get('points') or
                old.get('rebounds') != new.get('rebounds') or
                old.get('assists') != new.get('assists') or
                old.get('game_status') != new.get('game_status'))
                
    def _notify_update(self, stats: Dict):
        """Notify all callbacks of stat update"""
        for callback in self.callbacks:
            try:
                callback(stats)
            except Exception as e:
                logger.error(f"Error in callback: {e}")
                
    def get_player_stats(self, player_name: str, team: str) -> Optional[Dict]:
        """Get specific player stats"""
        key = f"{player_name}_{team}"
        return self.stats_cache.get(key)
        
    def get_live_player_stats(self) -> List[Dict]:
        """Get stats for all players with live games"""
        return [s for s in self.stats_cache.values() if s.get('game_status') == 'live']
        
    def get_all_stats(self) -> List[Dict]:
        """Get all collected stats"""
        return list(self.stats_cache.values())


class ProjectedStatsCalculator:
    """Calculates projected player stats based on live performance"""
    
    def __init__(self):
        self.live_stats = {}
        
    def calculate_projection(self, player_name: str, team: str, 
                            live_stats: Dict, historical_avg: Dict) -> Dict:
        """
        Calculate projected stats for rest of game
        
        Args:
            player_name: Player name
            team: Team code
            live_stats: Current live stats
            historical_avg: Historical average stats
            
        Returns:
            Projected stats for end of game
        """
        # Calculate projected stats based on current performance vs average
        minutes_played = live_stats.get('minutes_played', 0)
        minutes_remaining = max(0, 48 - minutes_played)  # Assume 48 min game
        
        if minutes_played == 0:
            return historical_avg
            
        # Project stats to 48 minutes
        pace_factor = 48 / minutes_played if minutes_played > 0 else 1
        
        projected = {
            "player_name": player_name,
            "team": team,
            "points": round(live_stats['points'] * pace_factor, 1),
            "rebounds": round(live_stats['rebounds'] * pace_factor, 1),
            "assists": round(live_stats['assists'] * pace_factor, 1),
            "steals": round(live_stats['steals'] * pace_factor, 1),
            "blocks": round(live_stats['blocks'] * pace_factor, 1),
            "turnovers": round(live_stats['turnovers'] * pace_factor, 1),
            "field_goal_pct": live_stats['field_goal_pct'],
            "three_point_pct": live_stats['three_point_pct'],
            "confidence": round(min(1.0, minutes_played / 35), 2),  # More games = higher confidence
            "timestamp": datetime.now().isoformat(),
        }
        
        return projected


# Global instance
_collector: Optional[LivePlayerStatsCollector] = None


def get_collector() -> LivePlayerStatsCollector:
    """Get or create global collector instance"""
    global _collector
    if _collector is None:
        _collector = LivePlayerStatsCollector()
    return _collector


def start_collection():
    """Start global stats collection"""
    collector = get_collector()
    if not collector.is_running:
        collector.start()


def stop_collection():
    """Stop global stats collection"""
    collector = get_collector()
    collector.stop()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    
    collector = get_collector()
    
    def on_update(stats):
        print(f"Stats Update: {stats['player_name']} - {stats['points']} pts")
        
    collector.register_callback(on_update)
    collector.start()
    
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        collector.stop()
