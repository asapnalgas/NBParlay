"""
NBA Injury Report Fetcher - Pulls real injury data from ESPN API
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

# ESPN free injury endpoint
ESPN_INJURIES_URL = "https://site.api.espn.com/apis/site/v2/sports/basketball/nba/injuries"

# Bypass SSL on macOS
ssl._create_default_https_context = ssl._create_unverified_context

# Cache: timestamp -> injury data
_injury_cache: Dict[str, object] = {"data": None, "fetched_at": None}
CACHE_TTL_SECONDS = 300  # 5 minutes


# ESPN team abbreviation mapping
ESPN_TO_NBA = {
    "NY": "NYK", "GS": "GSW", "NO": "NOP", "UTAH": "UTA",
    "SA": "SAS", "WSH": "WAS", "BKN": "BRK", "PHO": "PHX",
}

# Map full team display names to tricode abbreviations
TEAM_NAME_TO_ABBR = {
    "Atlanta Hawks": "ATL", "Boston Celtics": "BOS", "Brooklyn Nets": "BKN",
    "Charlotte Hornets": "CHA", "Chicago Bulls": "CHI", "Cleveland Cavaliers": "CLE",
    "Dallas Mavericks": "DAL", "Denver Nuggets": "DEN", "Detroit Pistons": "DET",
    "Golden State Warriors": "GSW", "Houston Rockets": "HOU", "Indiana Pacers": "IND",
    "LA Clippers": "LAC", "Los Angeles Clippers": "LAC",
    "Los Angeles Lakers": "LAL", "LA Lakers": "LAL",
    "Memphis Grizzlies": "MEM", "Miami Heat": "MIA", "Milwaukee Bucks": "MIL",
    "Minnesota Timberwolves": "MIN", "New Orleans Pelicans": "NOP",
    "New York Knicks": "NYK", "Oklahoma City Thunder": "OKC",
    "Orlando Magic": "ORL", "Philadelphia 76ers": "PHI", "Phoenix Suns": "PHX",
    "Portland Trail Blazers": "POR", "Sacramento Kings": "SAC",
    "San Antonio Spurs": "SAS", "Toronto Raptors": "TOR",
    "Utah Jazz": "UTA", "Washington Wizards": "WAS",
}

# Risk scores and minutes multipliers
INJURY_RISK = {
    "out": {"risk": 1.0, "minutes_mult": 0.0},
    "inactive": {"risk": 1.0, "minutes_mult": 0.0},
    "suspended": {"risk": 1.0, "minutes_mult": 0.0},
    "doubtful": {"risk": 0.72, "minutes_mult": 0.45},
    "questionable": {"risk": 0.48, "minutes_mult": 0.82},
    "day-to-day": {"risk": 0.48, "minutes_mult": 0.82},
    "probable": {"risk": 0.22, "minutes_mult": 0.96},
}


def _fetch_json(url: str, timeout: int = 10) -> Optional[Dict]:
    """Fetch JSON with retry."""
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
    }
    for attempt in range(3):
        try:
            req = Request(url, headers=headers)
            with urlopen(req, timeout=timeout) as resp:
                return json.loads(resp.read())
        except (HTTPError, URLError) as e:
            logger.warning(f"ESPN injury fetch attempt {attempt + 1} failed: {e}")
            if attempt < 2:
                time.sleep(1)
        except json.JSONDecodeError:
            return None
    return None


def fetch_injuries() -> List[Dict]:
    """Fetch current NBA injury report from ESPN.

    Returns list of dicts with keys:
        player_name, team, status, detail, injury_risk_score, injury_minutes_multiplier
    """
    now = datetime.now()

    # Check cache
    if (
        _injury_cache["data"] is not None
        and _injury_cache["fetched_at"]
        and (now - _injury_cache["fetched_at"]).total_seconds() < CACHE_TTL_SECONDS
    ):
        return _injury_cache["data"]

    data = _fetch_json(ESPN_INJURIES_URL)
    if not data:
        logger.warning("Could not fetch ESPN injury data")
        return _injury_cache.get("data") or []

    injuries: List[Dict] = []

    for team_entry in data.get("injuries", []):
        team_display = team_entry.get("displayName", "")
        team_abbr = TEAM_NAME_TO_ABBR.get(team_display, "")
        if not team_abbr:
            # Fallback: try abbreviation field or ESPN mapping
            espn_abbr = team_entry.get("team", {}).get("abbreviation", "")
            team_abbr = ESPN_TO_NBA.get(espn_abbr, espn_abbr)

        for injury in team_entry.get("injuries", []):
            athlete = injury.get("athlete", {})
            first = athlete.get("firstName", "")
            last = athlete.get("lastName", "")
            name = f"{first} {last}".strip()
            if not name:
                name = athlete.get("displayName", "Unknown")

            status_raw = (injury.get("status", "") or "").lower().strip()
            detail = injury.get("longComment", "") or injury.get("shortComment", "")

            risk_info = INJURY_RISK.get(status_raw, {"risk": 0.12, "minutes_mult": 1.0})

            injuries.append({
                "player_name": name,
                "team": team_abbr,
                "status": status_raw,
                "detail": detail,
                "injury_risk_score": risk_info["risk"],
                "injury_minutes_multiplier": risk_info["minutes_mult"],
            })

    _injury_cache["data"] = injuries
    _injury_cache["fetched_at"] = now
    logger.info(f"Fetched {len(injuries)} injury records from ESPN")
    return injuries


def get_injury_map() -> Dict[str, Dict]:
    """Return dict keyed by 'TEAM|PlayerName' -> injury info."""
    injuries = fetch_injuries()
    result = {}
    for inj in injuries:
        key = f"{inj['team']}|{inj['player_name']}"
        result[key] = inj
    return result


def clear_injury_cache():
    """Force refresh on next call."""
    _injury_cache["data"] = None
    _injury_cache["fetched_at"] = None
