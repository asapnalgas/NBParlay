"""
NBA Season Stats Fetcher - Pulls real player stats via Ball Don't Lie API
Falls back to position-based estimates when API key is unavailable.
"""

import json
import logging
import os
import ssl
import time
from datetime import datetime, timedelta
from typing import Dict, List, Optional
from urllib.request import Request, urlopen
from urllib.error import HTTPError, URLError

logger = logging.getLogger(__name__)

ssl._create_default_https_context = ssl._create_unverified_context

BDL_BASE = "https://api.balldontlie.io/v1"
_stats_cache: Dict[str, object] = {"data": None, "fetched_at": None}
CACHE_TTL = 3600  # 1 hour


def _fetch_json(url: str, headers: Optional[Dict] = None, timeout: int = 10) -> Optional[Dict]:
    h = {"User-Agent": "Mozilla/5.0"}
    if headers:
        h.update(headers)
    for attempt in range(2):
        try:
            req = Request(url, headers=h)
            with urlopen(req, timeout=timeout) as resp:
                return json.loads(resp.read())
        except (HTTPError, URLError) as e:
            logger.warning(f"Stats fetch attempt {attempt + 1} failed: {e}")
            if attempt < 1:
                time.sleep(1)
        except json.JSONDecodeError:
            return None
    return None


def fetch_season_averages_bdl(season: int = 2025) -> Dict[str, Dict]:
    """Fetch season averages from Ball Don't Lie API.

    Returns dict keyed by player name -> stat dict.
    Requires BALLDONTLIE_API_KEY env var.
    """
    api_key = os.getenv("BALLDONTLIE_API_KEY", "").strip()
    if not api_key:
        return {}

    now = datetime.now()
    if (
        _stats_cache["data"]
        and _stats_cache["fetched_at"]
        and (now - _stats_cache["fetched_at"]).total_seconds() < CACHE_TTL
    ):
        return _stats_cache["data"]

    result = {}
    page = 1
    while page <= 20:  # Safety limit
        url = f"{BDL_BASE}/season_averages?season={season}&per_page=100&page={page}"
        data = _fetch_json(url, headers={"Authorization": api_key})
        if not data or not data.get("data"):
            break
        for entry in data["data"]:
            player = entry.get("player", {})
            name = f"{player.get('first_name', '')} {player.get('last_name', '')}".strip()
            if name:
                result[name] = {
                    "pts": entry.get("pts", 0),
                    "reb": entry.get("reb", 0),
                    "ast": entry.get("ast", 0),
                    "stl": entry.get("stl", 0),
                    "blk": entry.get("blk", 0),
                    "fg3m": entry.get("fg3m", 0),
                    "min": entry.get("min", "0"),
                    "games_played": entry.get("games_played", 0),
                    "turnover": entry.get("turnover", 0),
                }
        meta = data.get("meta", {})
        if page >= meta.get("total_pages", 1):
            break
        page += 1
        time.sleep(0.25)  # Rate limit courtesy

    if result:
        _stats_cache["data"] = result
        _stats_cache["fetched_at"] = now
        logger.info(f"Fetched season averages for {len(result)} players from BDL")
    return result


# Enhanced position-based stat baselines (more realistic NBA averages)
POSITION_BASELINES = {
    "PG": {"pts": 16.5, "reb": 3.8, "ast": 6.2, "stl": 1.2, "blk": 0.3, "fg3m": 2.1},
    "SG": {"pts": 15.8, "reb": 3.5, "ast": 3.0, "stl": 1.0, "blk": 0.3, "fg3m": 2.0},
    "SF": {"pts": 15.2, "reb": 5.5, "ast": 2.8, "stl": 0.9, "blk": 0.5, "fg3m": 1.5},
    "PF": {"pts": 14.5, "reb": 7.2, "ast": 2.2, "stl": 0.7, "blk": 0.8, "fg3m": 1.0},
    "C":  {"pts": 13.8, "reb": 9.5, "ast": 1.8, "stl": 0.6, "blk": 1.4, "fg3m": 0.5},
}


def get_player_stats(player_name: str, position: str = "SF") -> Dict[str, float]:
    """Get season stats for a player. Tries BDL first, falls back to baselines."""
    bdl_data = _stats_cache.get("data") or {}
    if player_name in bdl_data:
        return bdl_data[player_name]

    baseline = POSITION_BASELINES.get(position, POSITION_BASELINES["SF"])
    return dict(baseline)


def get_all_cached_stats() -> Dict[str, Dict]:
    """Return all cached BDL stats (empty dict if none fetched)."""
    return _stats_cache.get("data") or {}
