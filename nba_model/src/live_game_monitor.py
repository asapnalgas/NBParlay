"""
Live Game Status Monitor
Continuously monitors NBA game status and updates CloudBrain with real-time data
"""

import logging
import json
from datetime import datetime, timedelta
from typing import Dict, List, Optional
import threading
import time

logger = logging.getLogger(__name__)


class LiveGameMonitor:
    """Monitors live NBA games and streams data to CloudBrain"""
    
    def __init__(self, update_interval: int = 60):
        """
        Initialize game monitor
        Args:
            update_interval: Seconds between updates
        """
        self.update_interval = update_interval
        self.is_running = False
        self.monitor_thread = None
        self.game_cache = {}
        self.callbacks = []
        
    def register_callback(self, callback):
        """Register callback for game updates"""
        self.callbacks.append(callback)
        
    def start(self):
        """Start monitoring games"""
        if self.is_running:
            logger.warning("Game monitor already running")
            return
            
        self.is_running = True
        self.monitor_thread = threading.Thread(target=self._monitor_loop, daemon=True)
        self.monitor_thread.start()
        logger.info("✓ Live game monitor started")
        
    def stop(self):
        """Stop monitoring games"""
        self.is_running = False
        if self.monitor_thread:
            self.monitor_thread.join(timeout=5)
        logger.info("✓ Live game monitor stopped")
        
    def _monitor_loop(self):
        """Main monitoring loop"""
        while self.is_running:
            try:
                games = self._fetch_live_games()
                self._process_game_updates(games)
                time.sleep(self.update_interval)
            except Exception as e:
                logger.error(f"Error in monitor loop: {e}")
                time.sleep(self.update_interval)
                
    def _fetch_live_games(self) -> List[Dict]:
        """Fetch current game status from data sources"""
        # This would integrate with actual NBA data sources (ESPN, NBA Stats API, etc.)
        games = []
        
        try:
            # Placeholder for real API integration
            # In production, would fetch from:
            # - ESPN API
            # - NBA.com Stats API
            # - RealGM
            # - Other sports data providers
            
            games = self._get_sample_games()
            
        except Exception as e:
            logger.error(f"Error fetching games: {e}")
            
        return games
        
    def _get_sample_games(self) -> List[Dict]:
        """Get sample game data (placeholder for real API)"""
        # This will be replaced with actual API calls
        return [
            {
                "game_id": "0022500XXX",
                "date": datetime.now().isoformat(),
                "home_team": "BOS",
                "away_team": "MIA",
                "home_score": 105,
                "away_score": 102,
                "status": "live",
                "quarter": 4,
                "time_remaining": "5:30",
                "last_updated": datetime.now().isoformat(),
            }
        ]
        
    def _process_game_updates(self, games: List[Dict]):
        """Process game updates and call callbacks"""
        for game in games:
            game_id = game.get('game_id')
            
            # Check if game state changed
            if game_id in self.game_cache:
                old_game = self.game_cache[game_id]
                if self._game_changed(old_game, game):
                    self._notify_update(game)
            else:
                self._notify_update(game)
                
            self.game_cache[game_id] = game
            
    def _game_changed(self, old: Dict, new: Dict) -> bool:
        """Check if game state has changed"""
        return (old.get('status') != new.get('status') or
                old.get('home_score') != new.get('home_score') or
                old.get('away_score') != new.get('away_score'))
                
    def _notify_update(self, game: Dict):
        """Notify all callbacks of game update"""
        for callback in self.callbacks:
            try:
                callback(game)
            except Exception as e:
                logger.error(f"Error in callback: {e}")
                
    def get_live_games(self) -> List[Dict]:
        """Get current live games"""
        return [g for g in self.game_cache.values() if g.get('status') == 'live']
        
    def get_upcoming_games(self) -> List[Dict]:
        """Get upcoming games"""
        return [g for g in self.game_cache.values() if g.get('status') == 'upcoming']
        
    def get_game(self, game_id: str) -> Optional[Dict]:
        """Get specific game by ID"""
        return self.game_cache.get(game_id)


# Global instance
_monitor: Optional[LiveGameMonitor] = None


def get_monitor() -> LiveGameMonitor:
    """Get or create global monitor instance"""
    global _monitor
    if _monitor is None:
        _monitor = LiveGameMonitor()
    return _monitor


def start_monitoring():
    """Start global monitoring"""
    monitor = get_monitor()
    if not monitor.is_running:
        monitor.start()


def stop_monitoring():
    """Stop global monitoring"""
    monitor = get_monitor()
    monitor.stop()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    
    monitor = get_monitor()
    
    def on_update(game):
        print(f"Game Update: {game}")
        
    monitor.register_callback(on_update)
    monitor.start()
    
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        monitor.stop()
