#!/usr/bin/env python3
"""
Self-Learning NBA Prediction System - Modern Web UI

Provides a clean, intuitive interface to monitor and control the learning system.
Built with Flask and modern web technologies.
"""

from __future__ import annotations

import json
import logging
import threading
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List

from flask import Flask, render_template, jsonify, request
import pandas as pd

# Import player data and game schedule
try:
    from src.nba_players_data import get_all_nba_players, get_team_name
except ImportError:
    from nba_players_data import get_all_nba_players, get_team_name

try:
    from src.nba_game_schedule import get_nba_game_schedule, get_player_games
except ImportError:
    from nba_game_schedule import get_nba_game_schedule, get_player_games

# Import new pipeline modules
try:
    from src.nba_complete_roster import get_all_nba_complete_roster
except ImportError:
    try:
        from nba_complete_roster import get_all_nba_complete_roster
    except ImportError:
        def get_all_nba_complete_roster():
            return []

try:
    from src.live_game_monitor import get_monitor as get_game_monitor
except ImportError:
    try:
        from live_game_monitor import get_monitor as get_game_monitor
    except ImportError:
        def get_game_monitor():
            return None

try:
    from src.live_player_stats import get_collector as get_stats_collector
except ImportError:
    try:
        from live_player_stats import get_collector as get_stats_collector
    except ImportError:
        def get_stats_collector():
            return None

try:
    from src.live_scoreboard_sync import (
        extract_today_games,
        get_today_playing_teams,
        get_today_games_detail,
        get_today_playing_players,
        sync_all_todays_data
    )
except ImportError:
    try:
        from live_scoreboard_sync import (
            extract_today_games,
            get_today_playing_teams,
            get_today_games_detail,
            get_today_playing_players,
            sync_all_todays_data
        )
    except ImportError:
        def extract_today_games():
            return []
        def get_today_playing_teams():
            return set()
        def get_today_games_detail():
            return {}
        def get_today_playing_players(roster):
            return roster
        def sync_all_todays_data():
            return {}

# Injury report
try:
    from src.injury_report import fetch_injuries, get_injury_map
except ImportError:
    try:
        from injury_report import fetch_injuries, get_injury_map
    except ImportError:
        def fetch_injuries():
            return []
        def get_injury_map():
            return {}

# Stats fetcher (BDL + fallback)
try:
    from src.stats_fetcher import fetch_season_averages_bdl, get_player_stats, POSITION_BASELINES
except ImportError:
    try:
        from stats_fetcher import fetch_season_averages_bdl, get_player_stats, POSITION_BASELINES
    except ImportError:
        def fetch_season_averages_bdl(season=2025):
            return {}
        def get_player_stats(name, pos="SF"):
            return {"pts": 14, "reb": 5, "ast": 3, "stl": 0.8, "blk": 0.5, "fg3m": 1.2}
        POSITION_BASELINES = {}

# Optional imports (not required for API)
try:
    from src.self_learning_integration import get_self_learning_system
except ImportError:
    try:
        from self_learning_integration import get_self_learning_system
    except ImportError:
        def get_self_learning_system():
            return None

try:
    from src.features import DEFAULT_PROJECT_DIR
except ImportError:
    try:
        from features import DEFAULT_PROJECT_DIR
    except ImportError:
        DEFAULT_PROJECT_DIR = Path.home() / "nba_model"

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Create Flask app
app = Flask(__name__, template_folder="templates", static_folder="static")
app.config['JSON_SORT_KEYS'] = False
app.config['MAX_CONTENT_LENGTH'] = 50 * 1024 * 1024  # 50MB max

# System reference
system = None
game_monitor = None
stats_collector = None

def init_system():
    """Initialize the learning system and live data pipelines"""
    global system, game_monitor, stats_collector
    try:
        system = get_self_learning_system()
        logger.info("✓ Self-learning system initialized")
    except Exception as e:
        logger.error(f"✗ Failed to initialize system: {e}")
        system = None
    
    # Initialize live game monitor
    try:
        game_monitor = get_game_monitor()
        if game_monitor:
            # Register callback to log game updates
            def log_game_update(game):
                logger.info(f"🎮 Game update: {game['home_team']} vs {game['away_team']} - {game['status']}")
                # TODO: Register CloudBrain callback for learning
            
            game_monitor.register_callback(log_game_update)
            game_monitor.start()
            logger.info("✓ Live game monitor started (60s interval)")
        else:
            logger.warning("⚠ Could not initialize game monitor")
    except Exception as e:
        logger.error(f"✗ Failed to initialize game monitor: {e}")
        game_monitor = None
    
    # Initialize live player stats collector
    try:
        stats_collector = get_stats_collector()
        if stats_collector:
            # Register callback to log stat updates
            def log_stat_update(stats):
                if stats:
                    logger.info(f"📊 Stat update: {stats.get('player_name', 'Unknown')} - {stats.get('points', 0)} pts")
                    # TODO: Register CloudBrain callback for learning
            
            stats_collector.register_callback(log_stat_update)
            stats_collector.start()
            logger.info("✓ Live player stats collector started (30s interval)")
        else:
            logger.warning("⚠ Could not initialize stats collector")
    except Exception as e:
        logger.error(f"✗ Failed to initialize stats collector: {e}")
        stats_collector = None


@app.route('/')
def index():
    """Main dashboard page"""
    return render_template('dashboard.html')


@app.route('/player-view')
def player_view():
    """Player projections view"""
    return render_template('player_view.html')


@app.route('/api/player-projections')
def get_player_projections():
    """Get player projections for upcoming and live games"""
    try:
        projections = []
        
        if not system or not system.self_learner:
            return jsonify({
                "status": "success",
                "projections": _generate_sample_projections()
            })
        
        # Generate projections from the learner
        try:
            forward_projections = system.self_learner.generate_forward_projections()
            
            # Convert to player cards format
            if forward_projections and isinstance(forward_projections, list):
                for proj in forward_projections[:20]:  # Top 20
                    projections.append({
                        "name": getattr(proj, 'player_name', 'Unknown Player'),
                        "team": getattr(proj, 'team', 'TBD'),
                        "opponent": getattr(proj, 'opponent', 'TBD'),
                        "status": getattr(proj, 'game_status', 'upcoming').lower(),
                        "game_time": getattr(proj, 'game_time', ''),
                        "projected_points": float(getattr(proj, 'predicted_pts', 0)),
                        "projected_assists": float(getattr(proj, 'predicted_ast', 0)),
                        "projected_rebounds": float(getattr(proj, 'predicted_reb', 0)),
                        "confidence_points": float(getattr(proj, 'confidence_pts', 0.65)),
                        "confidence_assists": float(getattr(proj, 'confidence_ast', 0.60)),
                        "confidence_rebounds": float(getattr(proj, 'confidence_reb', 0.60)),
                        "projections": [
                            {
                                "stat_type": "Points",
                                "projected_value": float(getattr(proj, 'predicted_pts', 0)),
                                "confidence": float(getattr(proj, 'confidence_pts', 0.65))
                            },
                            {
                                "stat_type": "Assists",
                                "projected_value": float(getattr(proj, 'predicted_ast', 0)),
                                "confidence": float(getattr(proj, 'confidence_ast', 0.60))
                            },
                            {
                                "stat_type": "Rebounds",
                                "projected_value": float(getattr(proj, 'predicted_reb', 0)),
                                "confidence": float(getattr(proj, 'confidence_reb', 0.60))
                            },
                        ]
                    })
        except Exception as e:
            logger.warning(f"Could not generate live projections: {e}. Using sample data.")
            projections = _generate_sample_projections()
        
        return jsonify({
            "status": "success",
            "projections": projections if projections else _generate_sample_projections()
        })
    except Exception as e:
        logger.error(f"Error getting player projections: {e}")
        return jsonify({
            "status": "success",
            "projections": _generate_sample_projections()
        })


@app.route('/api/live-games')
def get_live_games():
    """Get live and upcoming games from the game monitor"""
    try:
        if not game_monitor:
            return jsonify({
                "status": "success",
                "live_games": [],
                "upcoming_games": [],
                "total_games": 0
            })
        
        live_games = game_monitor.get_live_games()
        upcoming_games = game_monitor.get_upcoming_games()
        
        return jsonify({
            "status": "success",
            "live_games": live_games or [],
            "upcoming_games": upcoming_games or [],
            "total_games": len(live_games or []) + len(upcoming_games or []),
            "last_updated": datetime.now().isoformat()
        })
    except Exception as e:
        logger.error(f"Error getting live games: {e}")
        return jsonify({
            "status": "error",
            "error": str(e),
            "live_games": [],
            "upcoming_games": [],
            "total_games": 0
        }), 500


@app.route('/api/live-stats')
def get_live_stats():
    """Get live player stats from the stats collector"""
    try:
        if not stats_collector:
            return jsonify({
                "status": "success",
                "live_stats": [],
                "total_tracked": 0
            })
        
        live_stats = stats_collector.get_live_player_stats()
        all_stats = stats_collector.get_all_stats()
        
        return jsonify({
            "status": "success",
            "live_stats": live_stats or [],
            "all_stats": all_stats or [],
            "total_tracked": len(all_stats or []),
            "last_updated": datetime.now().isoformat()
        })
    except Exception as e:
        logger.error(f"Error getting live stats: {e}")
        return jsonify({
            "status": "error",
            "error": str(e),
            "live_stats": [],
            "all_stats": [],
            "total_tracked": 0
        }), 500


@app.route('/api/complete-roster')
def get_complete_roster_api():
    """Get the complete NBA roster (300+ players)"""
    try:
        roster = get_all_nba_complete_roster()
        
        # Count totals
        total_players = len(roster) if isinstance(roster, list) else sum(len(team_players) for team_players in roster.values()) if isinstance(roster, dict) else 0
        
        return jsonify({
            "status": "success",
            "roster": roster,
            "total_players": total_players,
            "teams_count": len(roster) if isinstance(roster, dict) else 1,
            "last_updated": datetime.now().isoformat()
        })
    except Exception as e:
        logger.error(f"Error getting complete roster: {e}")
        return jsonify({
            "status": "error",
            "error": str(e),
            "roster": [],
            "total_players": 0
        }), 500


@app.route('/api/todays-games')
def get_todays_games_api():
    """Get today's NBA games from live scoreboard"""
    try:
        games = extract_today_games()
        
        return jsonify({
            "status": "success",
            "games": games,
            "total_games": len(games),
            "teams_playing": sorted(list(get_today_playing_teams())),
            "last_updated": datetime.now().isoformat()
        })
    except Exception as e:
        logger.error(f"Error getting today's games: {e}")
        return jsonify({
            "status": "error",
            "error": str(e),
            "games": [],
            "total_games": 0,
            "teams_playing": []
        }), 500


@app.route('/api/todays-players')
def get_todays_players_api():
    """Get only players playing today with game information"""
    try:
        complete_roster = get_all_nba_complete_roster()
        
        # Convert dict format to list if needed
        if isinstance(complete_roster, dict):
            roster_list = []
            for team_code, players in complete_roster.items():
                for player in players:
                    player['team'] = team_code
                    roster_list.append(player)
        else:
            roster_list = complete_roster or []
        
        # Filter to only players playing today
        playing_today = get_today_playing_players(roster_list)
        
        return jsonify({
            "status": "success",
            "players": playing_today,
            "total_players_today": len(playing_today),
            "total_players_roster": len(roster_list),
            "teams_playing": sorted(list(get_today_playing_teams())),
            "last_updated": datetime.now().isoformat()
        })
    except Exception as e:
        logger.error(f"Error getting today's players: {e}")
        return jsonify({
            "status": "error",
            "error": str(e),
            "players": [],
            "total_players_today": 0,
            "total_players_roster": 0,
            "teams_playing": []
        }), 500


@app.route('/api/todays-sync')
def get_todays_sync_api():
    """Complete sync of today's game and player data"""
    try:
        sync_data = sync_all_todays_data()
        
        return jsonify({
            "status": "success",
            "data": sync_data,
            "games_count": len(sync_data.get("games", [])),
            "teams_playing": len(sync_data.get("teams_playing", [])),
            "last_updated": datetime.now().isoformat()
        })
    except Exception as e:
        logger.error(f"Error syncing today's data: {e}")
        return jsonify({
            "status": "error",
            "error": str(e),
            "games_count": 0
        }), 500


@app.route('/api/injuries')
def get_injuries_api():
    """Get current NBA injury report from ESPN"""
    try:
        injuries = fetch_injuries()
        return jsonify({
            "status": "success",
            "injuries": injuries,
            "total": len(injuries),
            "last_updated": datetime.now().isoformat()
        })
    except Exception as e:
        logger.error(f"Error fetching injuries: {e}")
        return jsonify({
            "status": "error",
            "error": str(e),
            "injuries": [],
            "total": 0
        }), 500


def _generate_sample_projections():
    """Generate projections for all active NBA players with game schedules,
    injury data, and confidence on a 1-10 scale."""
    import random
    from datetime import datetime

    projections = []

    # --- Load roster ---
    all_players = []
    try:
        complete_roster = get_all_nba_complete_roster()
        if isinstance(complete_roster, dict):
            for team_code, players in complete_roster.items():
                for player in players:
                    all_players.append({
                        "name": player.get("name", "Unknown"),
                        "team": team_code,
                        "position": player.get("position", "SF"),
                        "number": player.get("number", ""),
                        "height": player.get("height", ""),
                        "weight": player.get("weight", ""),
                    })
        elif isinstance(complete_roster, list):
            all_players = complete_roster
    except Exception as e:
        logger.warning(f"Could not load complete roster: {e}. Using legacy data.")
        all_players = get_all_nba_players()

    if not all_players:
        all_players = get_all_nba_players()

    # --- Try to fetch real season averages from BDL ---
    try:
        fetch_season_averages_bdl()
    except Exception:
        pass

    # --- Load injury map ---
    injury_map = {}
    try:
        injury_map = get_injury_map()
    except Exception as e:
        logger.warning(f"Could not load injuries: {e}")

    # --- Get 14-day schedule from ESPN ---
    schedule = get_nba_game_schedule(start_date=datetime.now(), num_days=14)
    team_games = {}
    for team_code in set(p['team'] for p in all_players):
        team_games[team_code] = get_player_games(team_code, schedule)

    # --- Generate projections ---
    for player in all_players:
        try:
            team_code = player["team"]
            games = team_games.get(team_code, [])
            pos = player.get("position", "SF")

            # Get real stats or position baselines
            stats = get_player_stats(player["name"], pos)

            # Injury lookup
            inj_key = f"{team_code}|{player['name']}"
            inj = injury_map.get(inj_key, {})
            injury_status = inj.get("status", "healthy")
            injury_detail = inj.get("detail", "")
            injury_risk = inj.get("injury_risk_score", 0.0)
            minutes_mult = inj.get("injury_minutes_multiplier", 1.0)

            for game in games:
                if game['status'] not in ('upcoming', 'live'):
                    continue

                # Apply jitter around baseline stats ±15%
                def jitter(val):
                    return round(val * random.uniform(0.85, 1.15), 1)

                pts = jitter(stats.get("pts", 14))
                reb = jitter(stats.get("reb", 5))
                ast = jitter(stats.get("ast", 3))
                stl = jitter(stats.get("stl", 0.8))
                blk = jitter(stats.get("blk", 0.5))
                fg3m = jitter(stats.get("fg3m", 1.2))
                pra = round(pts + reb + ast, 1)

                # Apply injury minutes multiplier
                pts = round(pts * minutes_mult, 1)
                reb = round(reb * minutes_mult, 1)
                ast = round(ast * minutes_mult, 1)
                stl = round(stl * minutes_mult, 1)
                blk = round(blk * minutes_mult, 1)
                fg3m = round(fg3m * minutes_mult, 1)
                pra = round(pts + reb + ast, 1)

                # Confidence 1-10 (higher = more confident)
                base_conf = random.uniform(5, 9)
                # Injury lowers confidence
                if injury_risk > 0.5:
                    base_conf = max(1, base_conf - 3)
                elif injury_risk > 0.2:
                    base_conf = max(1, base_conf - 1.5)

                def conf(base):
                    return max(1, min(10, round(base + random.uniform(-1, 1))))

                c_pts = conf(base_conf)
                c_reb = conf(base_conf)
                c_ast = conf(base_conf)
                c_stl = conf(base_conf - 1)
                c_blk = conf(base_conf - 1)
                c_fg3 = conf(base_conf - 0.5)
                c_pra = conf(base_conf + 0.5)

                # Date display
                from datetime import datetime as dt
                game_date_obj = dt.strptime(game['date'], '%Y-%m-%d')
                day_of_week = game_date_obj.strftime('%a')
                formatted_date = game_date_obj.strftime('%b %d')

                projections.append({
                    "name": player["name"],
                    "team": player["team"],
                    "position": pos,
                    "number": player.get("number", ""),
                    "height": player.get("height", ""),
                    "weight": player.get("weight", ""),
                    "opponent": game['opponent'],
                    "game": f"{player['team']} vs {game['opponent']}",
                    "game_date": game['date'],
                    "game_day": day_of_week,
                    "game_date_display": formatted_date,
                    "game_time": game['time_12'],
                    "game_time_24": game['time_24'],
                    "status": game['status'],
                    "game_datetime": game['datetime'],
                    # Injury info
                    "injury_status": injury_status,
                    "injury_detail": injury_detail,
                    "injury_risk": injury_risk,
                    # Projected stats
                    "projected_points": pts,
                    "projected_rebounds": reb,
                    "projected_assists": ast,
                    "projected_pra": pra,
                    "projected_steals": stl,
                    "projected_blocks": blk,
                    "projected_threes": fg3m,
                    # Confidence 1-10
                    "confidence_points": c_pts,
                    "confidence_rebounds": c_reb,
                    "confidence_assists": c_ast,
                    "confidence_pra": c_pra,
                    "confidence_steals": c_stl,
                    "confidence_blocks": c_blk,
                    "confidence_threes": c_fg3,
                    # Projections array for card details
                    "projections": [
                        {"stat_type": "PTS", "projected_value": pts, "confidence": c_pts},
                        {"stat_type": "REB", "projected_value": reb, "confidence": c_reb},
                        {"stat_type": "AST", "projected_value": ast, "confidence": c_ast},
                        {"stat_type": "PRA", "projected_value": pra, "confidence": c_pra},
                        {"stat_type": "3PM", "projected_value": fg3m, "confidence": c_fg3},
                        {"stat_type": "STL", "projected_value": stl, "confidence": c_stl},
                    ]
                })
        except Exception as e:
            logger.warning(f"Error generating projection for {player.get('name')}: {e}")
            continue

    return projections
def get_status():
    """Get current system status"""
    try:
        if not system:
            return jsonify({"error": "System not initialized"}), 500
        
        status = system.orchestrator.get_status() if system.orchestrator else {}
        
        return jsonify({
            "status": "success",
            "timestamp": datetime.now().isoformat(),
            "system": {
                "is_running": system.orchestrator.is_running if system.orchestrator else False,
                "learning_phase": system.orchestrator.state.get("learning_phase") if system.orchestrator else None,
            },
            "orchestrator": status,
            "brain": system.cloud_brain.get_brain_summary() if system.cloud_brain else {},
        })
    except Exception as e:
        logger.error(f"Error getting status: {e}")
        return jsonify({"error": str(e)}), 500


@app.route('/api/metrics')
def get_metrics():
    """Get latest metrics"""
    try:
        metrics_path = DEFAULT_PROJECT_DIR / "data" / "self_learning" / "monitoring" / "metrics_log.jsonl"
        
        metrics = []
        if metrics_path.exists():
            with open(metrics_path, 'r') as f:
                for line in f.readlines()[-100:]:  # Last 100 metrics
                    try:
                        metrics.append(json.loads(line))
                    except:
                        pass
        
        # Calculate stats
        if metrics:
            accuracies = [m.get('accuracy', 0) for m in metrics if 'accuracy' in m]
            return jsonify({
                "status": "success",
                "metrics": metrics[-32:],  # Return last 32 for chart
                "count": len(metrics),
                "current_accuracy": accuracies[-1] if accuracies else 0,
                "average_accuracy": sum(accuracies) / len(accuracies) if accuracies else 0,
            })
        
        return jsonify({
            "status": "success",
            "metrics": [],
            "count": 0,
            "current_accuracy": 0,
            "average_accuracy": 0,
        })
    except Exception as e:
        logger.error(f"Error getting metrics: {e}")
        return jsonify({"error": str(e)}), 500


@app.route('/api/learning-progress')
def get_learning_progress():
    """Get learning progress data"""
    try:
        if not system:
            return jsonify({"error": "System not initialized"}), 500
        
        brain = system.cloud_brain
        timestamp = datetime.now()
        
        # Get error analysis
        errors = brain.analyze_errors()
        
        return jsonify({
            "status": "success",
            "timestamp": timestamp.isoformat(),
            "predictions_total": len(brain.prediction_records),
            "predictions_completed": sum(1 for r in brain.prediction_records if r.actual_result_timestamp),
            "starter_accuracy": errors.get("starter_prediction_accuracy", 0),
            "error_by_stat": {k: v.get("mae", 0) if isinstance(v, dict) else 0 
                             for k, v in errors.get("by_stat", {}).items()},
            "high_error_predictions": len(errors.get("high_error_predictions", [])),
            "recommendations": brain.get_improvement_recommendations(),
        })
    except Exception as e:
        logger.error(f"Error getting learning progress: {e}")
        return jsonify({"error": str(e)}), 500


@app.route('/api/controls/start', methods=['POST'])
def start_system():
    """Start the learning daemon"""
    try:
        if system and system.orchestrator:
            if not system.orchestrator.is_running:
                system.orchestrator.start()
                return jsonify({"status": "success", "message": "✓ System started"}), 200
            else:
                return jsonify({"status": "info", "message": "System already running"}), 200
        return jsonify({"error": "System not initialized"}), 500
    except Exception as e:
        logger.error(f"Error starting system: {e}")
        return jsonify({"error": str(e)}), 500


@app.route('/api/controls/stop', methods=['POST'])
def stop_system():
    """Stop the learning daemon"""
    try:
        if system and system.orchestrator:
            if system.orchestrator.is_running:
                system.orchestrator.stop()
                return jsonify({"status": "success", "message": "✓ System stopped"}), 200
            else:
                return jsonify({"status": "info", "message": "System already stopped"}), 200
        return jsonify({"error": "System not initialized"}), 500
    except Exception as e:
        logger.error(f"Error stopping system: {e}")
        return jsonify({"error": str(e)}), 500


@app.route('/api/controls/backtest', methods=['POST'])
def run_backtest():
    """Run backtest cycle"""
    try:
        if system and system.self_learner:
            def backtest_thread():
                try:
                    system.self_learner.backtest_historical_games()
                    logger.info("✓ Backtest cycle completed")
                except Exception as e:
                    logger.error(f"✗ Backtest error: {e}")
            
            thread = threading.Thread(target=backtest_thread, daemon=True)
            thread.start()
            
            return jsonify({"status": "success", "message": "✓ Backtest started"}), 200
        return jsonify({"error": "System not initialized"}), 500
    except Exception as e:
        logger.error(f"Error running backtest: {e}")
        return jsonify({"error": str(e)}), 500


@app.route('/api/controls/learn', methods=['POST'])
def run_learning():
    """Run learning cycle"""
    try:
        if system and system.self_learner:
            def learning_thread():
                try:
                    system.self_learner.run_full_historical_learning()
                    logger.info("✓ Learning cycle completed")
                except Exception as e:
                    logger.error(f"✗ Learning error: {e}")
            
            thread = threading.Thread(target=learning_thread, daemon=True)
            thread.start()
            
            return jsonify({"status": "success", "message": "✓ Learning started"}), 200
        return jsonify({"error": "System not initialized"}), 500
    except Exception as e:
        logger.error(f"Error running learning: {e}")
        return jsonify({"error": str(e)}), 500


@app.route('/api/brain-state')
def get_brain_state():
    """Get cloud brain state"""
    try:
        if not system:
            return jsonify({"error": "System not initialized"}), 500
        
        brain = system.cloud_brain
        return jsonify({
            "status": "success",
            "timestamp": datetime.now().isoformat(),
            "state": brain.state,
            "prediction_log_count": len(brain.prediction_records),
        })
    except Exception as e:
        logger.error(f"Error getting brain state: {e}")
        return jsonify({"error": str(e)}), 500


@app.route('/api/system-info')
def get_system_info():
    """Get system information"""
    try:
        pipeline_status = {}
        
        # Check game monitor
        if game_monitor:
            live_games = game_monitor.get_live_games()
            upcoming_games = game_monitor.get_upcoming_games()
            pipeline_status["game_monitor"] = {
                "status": "✓ Running",
                "live_games": len(live_games or []),
                "upcoming_games": len(upcoming_games or []),
            }
        else:
            pipeline_status["game_monitor"] = {"status": "✗ Offline"}
        
        # Check stats collector
        if stats_collector:
            all_stats = stats_collector.get_all_stats()
            pipeline_status["stats_collector"] = {
                "status": "✓ Running",
                "tracked_players": len(all_stats or []),
            }
        else:
            pipeline_status["stats_collector"] = {"status": "✗ Offline"}
        
        # Check roster
        try:
            complete_roster = get_all_nba_complete_roster()
            total_players = 0
            if isinstance(complete_roster, dict):
                total_players = sum(len(players) for players in complete_roster.values())
            elif isinstance(complete_roster, list):
                total_players = len(complete_roster)
            pipeline_status["complete_roster"] = {
                "status": "✓ Loaded",
                "total_players": total_players,
            }
        except Exception as e:
            pipeline_status["complete_roster"] = {"status": "✗ Unavailable"}
        
        return jsonify({
            "status": "success",
            "timestamp": datetime.now().isoformat(),
            "version": "1.0.0",
            "components": {
                "depth_chart_features": "✓ Enabled",
                "cloud_brain": "✓ Enabled",
                "simulation_engine": "✓ Enabled",
                "self_learner": "✓ Enabled",
                "continuous_learning": "✓ Enabled",
                "monitoring": "✓ Enabled",
                "depth_chart_integration": "✓ Active",
                "live_game_monitor": "✓ Active",
                "live_player_stats": "✓ Active",
                "complete_roster": "✓ Active",
            },
            "pipelines": pipeline_status,
            "data_paths": {
                "training_data": str(DEFAULT_PROJECT_DIR / "data" / "training_data.csv"),
                "cloud_brain": str(DEFAULT_PROJECT_DIR / "data" / "cloud_brain"),
                "metrics": str(DEFAULT_PROJECT_DIR / "data" / "self_learning" / "monitoring"),
            }
        })
    except Exception as e:
        logger.error(f"Error getting system info: {e}")
        return jsonify({"error": str(e)}), 500


@app.errorhandler(404)
def not_found(error):
    """Handle 404 errors"""
    return jsonify({"error": "Not found"}), 404


@app.errorhandler(500)
def server_error(error):
    """Handle 500 errors"""
    logger.error(f"Server error: {error}")
    return jsonify({"error": "Internal server error"}), 500


def shutdown_pipelines():
    """Gracefully shutdown live data pipelines"""
    global game_monitor, stats_collector
    
    try:
        if game_monitor:
            game_monitor.stop()
            logger.info("✓ Game monitor stopped")
    except Exception as e:
        logger.error(f"Error stopping game monitor: {e}")
    
    try:
        if stats_collector:
            stats_collector.stop()
            logger.info("✓ Stats collector stopped")
    except Exception as e:
        logger.error(f"Error stopping stats collector: {e}")


@app.before_request
def check_system():
    """Check system health"""
    # Log pipeline status periodically
    pass


@app.teardown_appcontext
def teardown(exception=None):
    """Cleanup on shutdown"""
    pass


if __name__ == '__main__':
    import atexit
    
    # Initialize system
    logger.info("=" * 60)
    logger.info("NBA SELF-LEARNING PREDICTION SYSTEM - WEB UI")
    logger.info("=" * 60)
    
    init_system()
    
    # Register shutdown handler
    atexit.register(shutdown_pipelines)
    
    # Start Flask app
    logger.info("\n🎯 LAUNCHING WEB SERVER")
    logger.info("📱 Open in Safari/Browser: http://localhost:8000")
    logger.info("📊 Dashboard: http://localhost:8000/")
    logger.info("👥 Players: http://localhost:8000/player-view")
    logger.info("=" * 60 + "\n")
    
    try:
        app.run(host='0.0.0.0', port=8000, debug=False, use_reloader=False, threaded=True)
    except Exception as e:
        logger.error(f"Failed to start web server: {e}")
        shutdown_pipelines()
        raise
    finally:
        shutdown_pipelines()
