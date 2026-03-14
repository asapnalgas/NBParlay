"""
Live Scoreboard Sync - Fetches real-time game data from NBA official API
Provides today's games and player availability
"""

import json
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Set
from urllib.request import Request, urlopen
from urllib.error import HTTPError, URLError
import ssl
import time

logger = logging.getLogger(__name__)

# Official NBA API endpoints
SCOREBOARD_URL = "https://cdn.nba.com/static/json/liveData/scoreboard/todaysScoreboard_00.json"
SCHEDULE_URL = "https://cdn.nba.com/static/json/staticData/scheduleLeagueV2_9.json"
ESPN_SCOREBOARD_URL_TEMPLATE = "https://site.api.espn.com/apis/site/v2/sports/basketball/nba/scoreboard?dates={yyyymmdd}"

# Create SSL context that doesn't verify (necessary for some macOS environments)
ssl._create_default_https_context = ssl._create_unverified_context


def fetch_json(url: str, timeout: int = 10) -> Optional[Dict]:
    """Fetch JSON from URL with retry logic"""
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
    }
    
    for attempt in range(3):
        try:
            req = Request(url, headers=headers)
            with urlopen(req, timeout=timeout) as response:
                data = response.read()
                return json.loads(data)
        except (HTTPError, URLError) as e:
            logger.warning(f"Attempt {attempt + 1} failed for {url}: {e}")
            if attempt < 2:
                time.sleep(2 ** attempt)  # Exponential backoff
        except json.JSONDecodeError as e:
            logger.error(f"JSON decode error for {url}: {e}")
            return None
    
    logger.error(f"Failed to fetch data from {url} after 3 attempts")
    return None


def get_todays_scoreboard() -> Optional[Dict]:
    """Fetch today's NBA scoreboard from official API"""
    return fetch_json(SCOREBOARD_URL)


def get_nba_schedule() -> Optional[Dict]:
    """Fetch full NBA schedule"""
    return fetch_json(SCHEDULE_URL)


def get_espn_scoreboard(date_str: str = None) -> Optional[Dict]:
    """Fetch ESPN scoreboard for specific date (YYYYMMDD format)"""
    if not date_str:
        date_str = datetime.now().strftime("%Y%m%d")
    
    url = ESPN_SCOREBOARD_URL_TEMPLATE.format(yyyymmdd=date_str)
    return fetch_json(url)


def extract_today_games() -> List[Dict]:
    """Extract today's games from official NBA scoreboard"""
    scoreboard = get_todays_scoreboard()
    
    if not scoreboard:
        logger.warning("Failed to fetch scoreboard, trying ESPN")
        espn_data = get_espn_scoreboard()
        return extract_espn_games(espn_data) if espn_data else []
    
    games = []
    
    try:
        nba_games = scoreboard.get("scoreboard", {}).get("games", [])
        
        for game in nba_games:
            game_id = game.get("gameId")
            home_team = game.get("homeTeam", {})
            away_team = game.get("awayTeam", {})
            game_status = int(game.get("gameStatus", 0))
            
            # Map status: 1 = upcoming, 2 = live, 3 = final
            if game_status == 1:
                status = "upcoming"
            elif game_status == 2:
                status = "live"
            elif game_status == 3:
                status = "final"
            else:
                status = "unknown"
            
            game_info = {
                "game_id": game_id,
                "home_team": home_team.get("teamTricode"),
                "away_team": away_team.get("teamTricode"),
                "home_score": game.get("homeTeamScore", 0),
                "away_score": game.get("awayTeamScore", 0),
                "status": status,
                "game_datetime": game.get("gameTimeUTC"),
                "game_status": game_status,
                "home_players": extract_team_players(home_team),
                "away_players": extract_team_players(away_team),
            }
            
            games.append(game_info)
    
    except Exception as e:
        logger.error(f"Error extracting games: {e}")
    
    return games


def extract_espn_games(espn_data: Dict) -> List[Dict]:
    """Extract games from ESPN API format"""
    games = []
    
    try:
        events = espn_data.get("events", [])
        
        for event in events:
            competition = event.get("competitions", [{}])[0]
            competitors = competition.get("competitors", [])
            
            if len(competitors) < 2:
                continue
            
            home_team = competitors[0]
            away_team = competitors[1]
            
            game_info = {
                "game_id": event.get("id"),
                "home_team": home_team.get("team", {}).get("abbreviation"),
                "away_team": away_team.get("team", {}).get("abbreviation"),
                "home_score": int(home_team.get("score", 0)),
                "away_score": int(away_team.get("score", 0)),
                "status": event.get("status", {}).get("type"),
                "game_datetime": event.get("date"),
                "home_players": [],
                "away_players": [],
            }
            
            games.append(game_info)
    
    except Exception as e:
        logger.error(f"Error extracting ESPN games: {e}")
    
    return games


def extract_team_players(team_data: Dict) -> List[Dict]:
    """Extract players from team data"""
    players = []
    
    try:
        player_stats = team_data.get("players", [])
        
        for player in player_stats:
            player_info = {
                "player_id": player.get("personId"),
                "name": player.get("firstName", "") + " " + player.get("lastName", ""),
                "jersey": player.get("jerseyNum"),
                "position": player.get("position", ""),
                "points": player.get("points", 0),
                "rebounds": player.get("rebounded", {}).get("total", 0),
                "assists": player.get("assists", 0),
                "minutes": player.get("minutesCalculated", 0),
                "status": player.get("status", "active"),
            }
            
            # Only include players with activity
            if player_info["status"] != "inactive":
                players.append(player_info)
    
    except Exception as e:
        logger.error(f"Error extracting players: {e}")
    
    return players


def get_today_playing_teams() -> Set[str]:
    """Get set of team codes playing today"""
    games = extract_today_games()
    teams = set()
    
    for game in games:
        if game["status"] in ["upcoming", "live"]:
            teams.add(game["home_team"])
            teams.add(game["away_team"])
    
    logger.info(f"Teams playing today: {teams}")
    return teams


def get_today_games_detail() -> Dict[str, Dict]:
    """Get detailed game information indexed by team"""
    games = extract_today_games()
    team_games = {}
    
    for game in games:
        if game["status"] in ["upcoming", "live"]:
            home_team = game["home_team"]
            away_team = game["away_team"]
            
            # Extract date from game_datetime (format: YYYY-MM-DD)
            game_datetime_str = game["game_datetime"]
            game_date = game_datetime_str.split('T')[0] if game_datetime_str else ""
            
            # Add game info for home team
            team_games[home_team] = {
                "game_id": game["game_id"],
                "opponent": away_team,
                "is_home": True,
                "status": game["status"],
                "home_score": game["home_score"],
                "away_score": game["away_score"],
                "game_datetime": game["game_datetime"],
                "game_date": game_date,
            }
            
            # Add game info for away team
            team_games[away_team] = {
                "game_id": game["game_id"],
                "opponent": home_team,
                "is_home": False,
                "status": game["status"],
                "home_score": game["home_score"],
                "away_score": game["away_score"],
                "game_datetime": game["game_datetime"],
                "game_date": game_date,
            }
    
    return team_games


def get_today_playing_players(complete_roster: List[Dict]) -> List[Dict]:
    """Filter roster to only include players playing today"""
    playing_teams = get_today_playing_teams()
    team_games = get_today_games_detail()
    
    playing_players = []
    
    for player in complete_roster:
        team = player.get("team")
        
        if team in playing_teams:
            # Add game information
            game_info = team_games.get(team, {})
            player_with_game = {**player, **game_info}
            playing_players.append(player_with_game)
    
    logger.info(f"Found {len(playing_players)} players playing today out of {len(complete_roster)} total")
    
    return playing_players


def sync_all_todays_data() -> Dict:
    """Complete sync of today's game and player data"""
    result = {
        "timestamp": datetime.now().isoformat(),
        "games": extract_today_games(),
        "teams_playing": list(get_today_playing_teams()),
        "team_games": get_today_games_detail(),
    }
    
    logger.info(f"Synced data for {len(result['teams_playing'])} teams playing today")
    
    return result


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    
    print("Fetching today's games...")
    games = extract_today_games()
    print(f"Found {len(games)} games")
    
    for game in games:
        print(f"  {game['away_team']} @ {game['home_team']} - {game['status']}")
    
    teams = get_today_playing_teams()
    print(f"Teams playing today: {teams}")
