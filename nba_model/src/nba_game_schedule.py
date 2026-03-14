"""
NBA Game Schedule - Fetches real game schedules from ESPN API
"""

import json
import logging
import ssl
import time
from datetime import datetime, timedelta
from typing import Dict, List, Optional
from urllib.request import Request, urlopen
from urllib.error import HTTPError, URLError

logger = logging.getLogger(__name__)

# ESPN API for schedule by date
ESPN_SCOREBOARD_URL = "https://site.api.espn.com/apis/site/v2/sports/basketball/nba/scoreboard?dates={yyyymmdd}"

# Bypass SSL verification on macOS
ssl._create_default_https_context = ssl._create_unverified_context

# ESPN uses different abbreviations than standard NBA codes
ESPN_TO_NBA_ABBR = {
    "NY": "NYK", "GS": "GSW", "NO": "NOP", "UTAH": "UTA",
    "SA": "SAS", "WSH": "WAS", "BKN": "BRK", "PHO": "PHX",
}

# In-memory cache: date_str -> list of games
_schedule_cache: Dict[str, List[Dict]] = {}


def _fetch_json(url: str, timeout: int = 10) -> Optional[Dict]:
    """Fetch JSON from URL with retry"""
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
    }
    for attempt in range(3):
        try:
            req = Request(url, headers=headers)
            with urlopen(req, timeout=timeout) as resp:
                return json.loads(resp.read())
        except (HTTPError, URLError) as e:
            logger.warning(f"ESPN fetch attempt {attempt + 1} failed: {e}")
            if attempt < 2:
                time.sleep(1)
        except json.JSONDecodeError:
            return None
    return None


def _fetch_espn_games_for_date(date_str: str) -> List[Dict]:
    """Fetch real games from ESPN for a given date (YYYY-MM-DD)"""
    yyyymmdd = date_str.replace("-", "")
    data = _fetch_json(ESPN_SCOREBOARD_URL.format(yyyymmdd=yyyymmdd))
    if not data:
        return []

    games = []
    game_times = []

    for event in data.get("events", []):
        comp = event.get("competitions", [{}])[0]
        competitors = comp.get("competitors", [])
        if len(competitors) < 2:
            continue

        # ESPN: competitors[0] is home, competitors[1] is away
        home = competitors[0]
        away = competitors[1]

        home_abbr = home.get("team", {}).get("abbreviation", "???")
        away_abbr = away.get("team", {}).get("abbreviation", "???")

        # Normalize ESPN abbreviations to standard NBA codes
        home_abbr = ESPN_TO_NBA_ABBR.get(home_abbr, home_abbr)
        away_abbr = ESPN_TO_NBA_ABBR.get(away_abbr, away_abbr)

        # Game status
        status_obj = event.get("status", {}).get("type", {})
        state = status_obj.get("state", "pre")  # pre, in, post
        if state == "pre":
            status = "upcoming"
        elif state == "in":
            status = "live"
        else:
            status = "final"

        # Game time
        game_dt_str = event.get("date", "")  # ISO format from ESPN
        try:
            game_dt = datetime.fromisoformat(game_dt_str.replace("Z", "+00:00"))
            # Convert to local-ish display
            time_12 = game_dt.strftime("%-I:%M %p")
            time_24 = game_dt.strftime("%H:%M")
        except Exception:
            time_12 = "TBD"
            time_24 = "00:00"
            game_dt = None

        game_info = {
            "game_id": event.get("id", f"{date_str}_{len(games)}"),
            "date": date_str,
            "time_24": time_24,
            "time_12": time_12,
            "datetime": game_dt_str,
            "home_team": home_abbr,
            "away_team": away_abbr,
            "home_score": int(home.get("score", 0) or 0),
            "away_score": int(away.get("score", 0) or 0),
            "status": status,
        }
        games.append(game_info)

    logger.info(f"ESPN schedule for {date_str}: {len(games)} games")
    return games


def get_nba_game_schedule(start_date: datetime = None, num_days: int = 14) -> List[Dict]:
    """
    Fetch real NBA game schedule from ESPN API for the given date range.
    Results are cached in memory to avoid repeated API calls.
    """
    if start_date is None:
        start_date = datetime.now()

    all_games = []

    for day_offset in range(num_days):
        game_date = start_date + timedelta(days=day_offset)
        date_str = game_date.strftime("%Y-%m-%d")

        # Check cache first
        if date_str in _schedule_cache:
            all_games.extend(_schedule_cache[date_str])
            continue

        # Fetch from ESPN
        games = _fetch_espn_games_for_date(date_str)
        _schedule_cache[date_str] = games
        all_games.extend(games)

        # Brief pause between API calls to be respectful
        if day_offset < num_days - 1:
            time.sleep(0.3)

    logger.info(f"Total schedule: {len(all_games)} games over {num_days} days")
    return all_games


def get_player_games(team_code: str, games: List[Dict]) -> List[Dict]:
    """Get all games for a specific team, with opponent info added."""
    team_games = []
    for game in games:
        if game["home_team"] == team_code:
            team_games.append({**game, "opponent": game["away_team"], "is_home": True})
        elif game["away_team"] == team_code:
            team_games.append({**game, "opponent": game["home_team"], "is_home": False})
    return team_games


def clear_schedule_cache():
    """Clear the cached schedule data"""
    _schedule_cache.clear()


def get_game_for_team_date(team_code: str, game_date: str, games: List[Dict]) -> Optional[Dict]:
    """Get the specific game for a team on a given date"""
    team_games = get_player_games(team_code, games)
    for game in team_games:
        if game['date'] == game_date:
            return game
    return None


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    schedule = get_nba_game_schedule(num_days=3)

    print(f"Fetched {len(schedule)} games\n")

    current_date = None
    for game in schedule:
        if game['date'] != current_date:
            current_date = game['date']
            print(f"\n  {current_date}")
        print(f"  {game['time_12']} - {game['away_team']} @ {game['home_team']} ({game['status']})")
