from __future__ import annotations

import argparse
import gzip
import hashlib
import html
import io
import json
import os
import random
import re
import shutil
import subprocess
import threading
import time
from datetime import datetime, timedelta, timezone
from email.utils import parsedate_to_datetime
from pathlib import Path
from urllib.parse import quote_plus, urlencode, urljoin, urlparse
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen
import xml.etree.ElementTree as ET
from zoneinfo import ZoneInfo

import numpy as np
import pandas as pd
from pypdf import PdfReader

try:
    from .benchmark import (
        DEFAULT_REPORT_PATH as DEFAULT_BENCHMARK_REPORT_PATH,
        DEFAULT_SNAPSHOT_PATH as DEFAULT_BENCHMARK_SNAPSHOT_PATH,
        capture_rotowire_benchmark_snapshot,
        run_rotowire_benchmark,
    )
    from .engine import (
        DEFAULT_ADAPTIVE_LEARNING_PROFILE_PATH,
        DEFAULT_BUNDLE_PATH,
        DEFAULT_PREDICTIONS_PATH,
        DEFAULT_PREDICTION_MISS_LOG_PATH,
        app_status,
        predict_engine,
        refresh_adaptive_learning_from_benchmark,
        recheck_past_predictions,
        train_engine,
    )
    from .features import (
        ALL_TARGETS,
        DEFAULT_CONTEXT_UPDATES_PATH,
        DEFAULT_DATA_PATH,
        DEFAULT_PROVIDER_CONTEXT_PATH,
        DEFAULT_PROJECT_DIR,
        DEFAULT_TRAINING_UPLOAD_PATH,
        DEFAULT_UPCOMING_PATH,
        KNOWN_ROLLING_STAT_COLUMNS,
        OPTIONAL_CONTEXT_COLUMNS,
        SUPPORT_TARGETS,
        load_dataset,
        load_season_priors,
        refresh_season_priors_from_history,
    )
    from .scoring import calculate_draftkings_points, calculate_fanduel_points
    from .data_pipeline import (
        check_and_register_idempotency,
        compute_frame_fingerprint,
        record_ingestion_event,
        run_contract_drift_audit,
    )
    from .support_modules import (
        build_support_module_snapshot,
        default_support_module_config,
        module_enabled,
        normalize_support_module_config,
        support_module_specs,
        summarize_module_alerts,
    )
except ImportError:
    from benchmark import (
        DEFAULT_REPORT_PATH as DEFAULT_BENCHMARK_REPORT_PATH,
        DEFAULT_SNAPSHOT_PATH as DEFAULT_BENCHMARK_SNAPSHOT_PATH,
        capture_rotowire_benchmark_snapshot,
        run_rotowire_benchmark,
    )
    from engine import (
        DEFAULT_ADAPTIVE_LEARNING_PROFILE_PATH,
        DEFAULT_BUNDLE_PATH,
        DEFAULT_PREDICTIONS_PATH,
        DEFAULT_PREDICTION_MISS_LOG_PATH,
        app_status,
        predict_engine,
        refresh_adaptive_learning_from_benchmark,
        recheck_past_predictions,
        train_engine,
    )
    from features import (
        ALL_TARGETS,
        DEFAULT_CONTEXT_UPDATES_PATH,
        DEFAULT_DATA_PATH,
        DEFAULT_PROVIDER_CONTEXT_PATH,
        DEFAULT_PROJECT_DIR,
        DEFAULT_TRAINING_UPLOAD_PATH,
        DEFAULT_UPCOMING_PATH,
        KNOWN_ROLLING_STAT_COLUMNS,
        OPTIONAL_CONTEXT_COLUMNS,
        SUPPORT_TARGETS,
        load_dataset,
        load_season_priors,
        refresh_season_priors_from_history,
    )
    from scoring import calculate_draftkings_points, calculate_fanduel_points
    from data_pipeline import (
        check_and_register_idempotency,
        compute_frame_fingerprint,
        record_ingestion_event,
        run_contract_drift_audit,
    )
    from support_modules import (
        build_support_module_snapshot,
        default_support_module_config,
        module_enabled,
        normalize_support_module_config,
        support_module_specs,
        summarize_module_alerts,
    )


CONFIG_DIR = DEFAULT_PROJECT_DIR / "config"
DEFAULT_LIVE_CONFIG_PATH = CONFIG_DIR / "live_sync.json"
DEFAULT_LIVE_STATE_PATH = DEFAULT_PROJECT_DIR / "data" / "live_sync_state.json"
DEFAULT_PROVIDERS_ENV_PATH = CONFIG_DIR / "providers.env"
DEFAULT_PROFILE_CACHE_PATH = DEFAULT_PROJECT_DIR / "data" / "player_profile_cache.csv"
DEFAULT_PLAYSTYLE_CACHE_PATH = DEFAULT_PROJECT_DIR / "data" / "player_playstyle_cache.csv"
DEFAULT_LIVE_GAME_ACTIONS_PATH = DEFAULT_PROJECT_DIR / "data" / "live_game_actions.csv"
DEFAULT_POSTGAME_REVIEWS_PATH = DEFAULT_PROJECT_DIR / "data" / "postgame_reviews.csv"
DEFAULT_GAME_NOTES_DAILY_PATH = DEFAULT_PROJECT_DIR / "data" / "game_notes_daily.csv"
DEFAULT_ESPN_LIVE_GAMES_PATH = DEFAULT_PROJECT_DIR / "data" / "espn_live_games.csv"
DEFAULT_CLOUD_ARCHIVE_PATH = Path.home() / "Library" / "Mobile Documents" / "com~apple~CloudDocs" / "nba_model_cloud_archive"

SCOREBOARD_URL = "https://cdn.nba.com/static/json/liveData/scoreboard/todaysScoreboard_00.json"
BOXSCORE_URL_TEMPLATE = "https://cdn.nba.com/static/json/liveData/boxscore/boxscore_{game_id}.json"
SCHEDULE_URL = "https://cdn.nba.com/static/json/staticData/scheduleLeagueV2_9.json"
DAILY_LINEUPS_URL_TEMPLATE = "https://stats.nba.com/js/data/leaders/00_daily_lineups_{yyyymmdd}.json"
ESPN_SCOREBOARD_URL_TEMPLATE = "https://site.api.espn.com/apis/site/v2/sports/basketball/nba/scoreboard?dates={yyyymmdd}"
ESPN_SUMMARY_URL_TEMPLATE = "https://site.api.espn.com/apis/site/v2/sports/basketball/nba/summary?event={event_id}"
ROTOWIRE_PRIZEPICKS_LINES_URL = "https://www.rotowire.com/picks/api/lines.php"

# Operational interval guardrails. Fast UI/data refresh is useful, but expensive
# background tasks must run on slower cadences to avoid stalling live sync.
MIN_SYNC_INTERVAL_SECONDS = 10
MIN_PROJECTION_INTERVAL_SECONDS = 10
MIN_IN_GAME_REFRESH_INTERVAL_SECONDS = 10
MIN_RETRAIN_INTERVAL_SECONDS = 900
MIN_OPTIMIZATION_INTERVAL_SECONDS = 1800
MIN_BENCHMARK_INTERVAL_SECONDS = 300
MIN_PLAYSTYLE_REFRESH_INTERVAL_SECONDS = 300
MIN_PROFILE_REFRESH_INTERVAL_SECONDS = 900
MIN_NEWS_REFRESH_INTERVAL_SECONDS = 10
MIN_PROVIDER_REFRESH_INTERVAL_SECONDS = 10
DEFAULT_PROVIDER_REQUEST_TIMEOUT_SECONDS = 8
DEFAULT_BACKFILL_MAX_RUNTIME_SECONDS = 20
DEFAULT_LINEUPS_REFRESH_INTERVAL_SECONDS = 10
DEFAULT_ODDS_REFRESH_INTERVAL_SECONDS = 10
DEFAULT_PLAYER_PROPS_REFRESH_INTERVAL_SECONDS = 10
DEFAULT_ROTOWIRE_REFRESH_INTERVAL_SECONDS = 10
DEFAULT_INJURY_REFRESH_INTERVAL_SECONDS = 10
DEFAULT_LIVE_ROSTER_REFRESH_INTERVAL_SECONDS = 10
DEFAULT_NEWS_MAX_RUNTIME_SECONDS = 10
DEFAULT_NEON_SYNC_INTERVAL_SECONDS = 300
DEFAULT_NEON_MAX_DATASET_BYTES = 8_000_000

DEFAULT_LIVE_CONFIG = {
    "enabled": True,
    "auto_start_on_app_launch": True,
    "poll_interval_seconds": MIN_SYNC_INTERVAL_SECONDS,
    "force_provider_refresh_every_poll": True,
    "fetch_retry_attempts": 1,
    "fetch_retry_base_delay_seconds": 0.3,
    "fetch_retry_jitter_seconds": 0.1,
    "auto_backfill_recent_history": True,
    "history_backfill_days": 42,
    "history_backfill_max_games_per_cycle": 180,
    "history_backfill_max_runtime_seconds": DEFAULT_BACKFILL_MAX_RUNTIME_SECONDS,
    "history_backfill_min_interval_hours": 12,
    "auto_build_upcoming_slate": True,
    "auto_estimate_expected_minutes": True,
    "expected_minutes_refresh_interval_seconds": 1800,
    "teammate_context_refresh_interval_seconds": 1800,
    "shot_style_context_refresh_interval_seconds": 300,
    "pregame_slate_lookahead_hours": 96,
    "live_projection_horizon_hours": 48,
    "max_upcoming_rows_per_cycle": 250,
    "auto_retrain_on_new_results": True,
    "auto_retrain_each_interval": True,
    "retrain_interval_seconds": MIN_RETRAIN_INTERVAL_SECONDS,
    "auto_predict_after_sync": False,
    "capture_benchmark_snapshot_on_projection_refresh": True,
    "auto_run_rotowire_benchmark": True,
    "benchmark_run_interval_minutes": 10,
    "benchmark_run_interval_seconds": MIN_BENCHMARK_INTERVAL_SECONDS,
    "benchmark_run_lookback_days": 28,
    "auto_contract_drift_audit": True,
    "contract_drift_interval_hours": 24,
    "contract_drift_alert_on_unexpected_columns": True,
    "contract_drift_max_missing_required_columns": 0,
    "contract_drift_max_unexpected_columns": 0,
    "projection_refresh_interval_minutes": 1,
    "projection_refresh_interval_seconds": MIN_PROJECTION_INTERVAL_SECONDS,
    "prediction_min_interval_seconds": MIN_PROJECTION_INTERVAL_SECONDS,
    "prediction_on_context_change_only": False,
    "prediction_max_rows_per_cycle": 400,
    "auto_refresh_in_game_projections": True,
    "in_game_projection_refresh_interval_seconds": MIN_IN_GAME_REFRESH_INTERVAL_SECONDS,
    "in_game_projection_blend_live_weight": 0.6,
    "in_game_projection_blend_pregame_weight": 0.4,
    "run_heavy_model_tasks_in_live_sync": False,
    "support_modules": default_support_module_config(),
    "auto_self_optimize_hourly": True,
    "optimization_interval_minutes": 60,
    "optimization_interval_seconds": MIN_OPTIMIZATION_INTERVAL_SECONDS,
    "optimization_recheck_sample_rows": 2000,
    "optimization_candidate_lookbacks_days": [21, 28, 35, 42, 56],
    "model_training_lookback_days": 35,
    "include_live_games_in_upcoming": False,
    "always_include_live_games_for_context": True,
    "force_projection_refresh_on_context_change": True,
    "training_data_path": str(DEFAULT_TRAINING_UPLOAD_PATH),
    "upcoming_data_path": str(DEFAULT_UPCOMING_PATH),
    "context_updates_path": str(DEFAULT_CONTEXT_UPDATES_PATH),
    "provider_context_path": str(DEFAULT_PROVIDER_CONTEXT_PATH),
    "providers": {
        "odds": {
            "enabled": True,
            "api_key_env": "ODDS_API_KEY",
            "base_url": "https://api.the-odds-api.com/v4",
            "sport": "basketball_nba",
            "refresh_interval_seconds": DEFAULT_ODDS_REFRESH_INTERVAL_SECONDS,
            "request_timeout_seconds": DEFAULT_PROVIDER_REQUEST_TIMEOUT_SECONDS,
            "regions": "us",
            "markets": "spreads,totals",
            "bookmakers": "draftkings,fanduel",
            "odds_format": "american",
            "date_format": "iso",
            "enable_espn_fallback": True,
            "espn_scoreboard_url_template": ESPN_SCOREBOARD_URL_TEMPLATE,
            "espn_summary_url_template": ESPN_SUMMARY_URL_TEMPLATE,
        },
        "player_props": {
            "enabled": True,
            "api_key_env": "ODDS_API_KEY",
            "base_url": "https://api.the-odds-api.com/v4",
            "sport": "basketball_nba",
            "refresh_interval_seconds": DEFAULT_PLAYER_PROPS_REFRESH_INTERVAL_SECONDS,
            "request_timeout_seconds": DEFAULT_PROVIDER_REQUEST_TIMEOUT_SECONDS,
            "max_events_per_cycle": 8,
            "regions": "us",
            "bookmakers": "draftkings,fanduel",
            "markets": (
                "player_points,player_rebounds,player_assists,player_points_rebounds_assists,"
                "player_points_rebounds,player_points_assists,player_rebounds_assists,"
                "player_steals,player_blocks,player_turnovers,player_steals_blocks,"
                "player_threes,player_three_points_made"
            ),
            "odds_format": "american",
            "date_format": "iso",
        },
        "rotowire_prizepicks": {
            "enabled": True,
            "lines_url": ROTOWIRE_PRIZEPICKS_LINES_URL,
            "book": "prizepicks",
            "referer": "https://www.rotowire.com/picks/prizepicks/",
            "prefer_non_promo_lines": True,
            "refresh_interval_seconds": DEFAULT_ROTOWIRE_REFRESH_INTERVAL_SECONDS,
            "request_timeout_seconds": DEFAULT_PROVIDER_REQUEST_TIMEOUT_SECONDS,
        },
        "betr": {
            "enabled": False,
            "board_url": "https://www.betr.app/",
            "note": "No stable public BETR props API is available; use manual line import for BETR entries.",
        },
        "lineups": {
            "enabled": True,
            "url_template": DAILY_LINEUPS_URL_TEMPLATE,
            "include_expected_as_starters": True,
            "include_confirmed_as_starters": True,
            "refresh_interval_seconds": DEFAULT_LINEUPS_REFRESH_INTERVAL_SECONDS,
            "request_timeout_seconds": DEFAULT_PROVIDER_REQUEST_TIMEOUT_SECONDS,
            "max_dates_per_cycle": 2,
        },
        "live_rosters": {
            "enabled": True,
            "refresh_interval_seconds": DEFAULT_LIVE_ROSTER_REFRESH_INTERVAL_SECONDS,
        },
        "player_profiles": {
            "enabled": True,
            "refresh_interval_seconds": MIN_PROFILE_REFRESH_INTERVAL_SECONDS,
            "refresh_interval_hours": 24,
            "max_players_per_cycle": 120,
            "cache_path": str(DEFAULT_PROFILE_CACHE_PATH),
            "wikipedia_summary_template": "https://en.wikipedia.org/api/rest_v1/page/summary/{title}",
        },
        "playstyle": {
            "enabled": True,
            "refresh_interval_seconds": MIN_PLAYSTYLE_REFRESH_INTERVAL_SECONDS,
            "remote_refresh_interval_seconds": 1800,
            "max_players_per_cycle": 700,
            "remote_fetch_mode": "light",
            "cache_path": str(DEFAULT_PLAYSTYLE_CACHE_PATH),
            "base_url": "https://stats.nba.com/stats",
            "season": "",
            "season_type": "Regular Season",
            "per_mode": "PerGame",
            "timeout_seconds": DEFAULT_PROVIDER_REQUEST_TIMEOUT_SECONDS,
        },
        "injuries": {
            "enabled": True,
            "provider": "balldontlie",
            "api_key_env": "BALLDONTLIE_API_KEY",
            "base_url": "https://api.balldontlie.io/v1",
            "refresh_interval_seconds": DEFAULT_INJURY_REFRESH_INTERVAL_SECONDS,
            "request_timeout_seconds": 10,
            "official_report_page": "https://official.nba.com/nba-injury-report-2025-26-season/",
            "csv_url": "",
            "csv_url_env": "NBA_INJURY_CSV_URL",
            "json_url": "",
            "json_url_env": "NBA_INJURY_JSON_URL",
            "json_records_path": "",
        },
        "news": {
            "enabled": True,
            "refresh_interval_minutes": 5,
            "refresh_interval_seconds": MIN_NEWS_REFRESH_INTERVAL_SECONDS,
            "lookback_hours": 24,
            "max_queries_per_cycle": 8,
            "max_articles_per_query": 25,
            "request_timeout_seconds": DEFAULT_PROVIDER_REQUEST_TIMEOUT_SECONDS,
            "max_runtime_seconds": DEFAULT_NEWS_MAX_RUNTIME_SECONDS,
            "google_news_enabled": True,
            "google_news_template": "https://news.google.com/rss/search?q={query}&hl=en-US&gl=US&ceid=US:en",
            "espn_rss_enabled": True,
            "espn_rss_url": "https://www.espn.com/espn/rss/nba/news",
            "rotowire_rss_enabled": True,
            "rotowire_rss_url": "https://www.rotowire.com/rss/news.php?sport=NBA",
        },
        "game_notes": {
            "enabled": True,
            "refresh_interval_seconds": MIN_IN_GAME_REFRESH_INTERVAL_SECONDS,
            "postgame_refresh_interval_seconds": 300,
            "daily_compile_interval_seconds": 900,
            "max_live_rows_retained": 250_000,
            "max_postgame_rows_retained": 120_000,
            "max_daily_rows_retained": 250_000,
            "request_timeout_seconds": DEFAULT_PROVIDER_REQUEST_TIMEOUT_SECONDS,
            "live_actions_path": str(DEFAULT_LIVE_GAME_ACTIONS_PATH),
            "postgame_reviews_path": str(DEFAULT_POSTGAME_REVIEWS_PATH),
            "daily_notes_path": str(DEFAULT_GAME_NOTES_DAILY_PATH),
            "espn_scoreboard_url_template": ESPN_SCOREBOARD_URL_TEMPLATE,
            "espn_summary_url_template": ESPN_SUMMARY_URL_TEMPLATE,
        },
        "espn_live": {
            "enabled": True,
            "refresh_interval_seconds": MIN_IN_GAME_REFRESH_INTERVAL_SECONDS,
            "request_timeout_seconds": DEFAULT_PROVIDER_REQUEST_TIMEOUT_SECONDS,
            "scoreboard_url_template": ESPN_SCOREBOARD_URL_TEMPLATE,
            "summary_url_template": ESPN_SUMMARY_URL_TEMPLATE,
            "max_dates_per_cycle": 2,
            "max_events_per_cycle": 20,
            "max_rows_retained": 500_000,
            "store_path": str(DEFAULT_ESPN_LIVE_GAMES_PATH),
            "include_pregame_events": False,
            "mirror_into_game_notes_live_actions": True,
        },
        "cloud_archive": {
            "enabled": True,
            "sync_interval_seconds": 60,
            "hydrate_training_from_cloud": True,
            "hydrate_interval_seconds": 3600,
            "archive_path": str(DEFAULT_CLOUD_ARCHIVE_PATH),
        },
        "neon_sync": {
            "enabled": False,
            "database_url_env": "NEON_DATABASE_URL",
            "database_url": "",
            "schema": "public",
            "table_prefix": "nba_live",
            "sync_interval_seconds": DEFAULT_NEON_SYNC_INTERVAL_SECONDS,
            "max_dataset_bytes": DEFAULT_NEON_MAX_DATASET_BYTES,
            "max_rows_per_dataset": 250_000,
            "compress_payloads": True,
        },
    },
}

UPCOMING_CONTEXT_COLUMNS = [
    "team",
    "position",
    "starter",
    "starter_probability",
    "starter_certainty",
    "lineup_status_label",
    "lineup_status_confidence",
    "age",
    "height_inches",
    "weight_lbs",
    "injury_status",
    "health_status",
    "suspension_status",
    "injury_risk_score",
    "injury_minutes_multiplier",
    "home_court_points_boost",
    "home_court_minutes_boost",
    "hometown_game_flag",
    "hometown_advantage_score",
    "teammate_active_core_count",
    "teammate_out_core_count",
    "teammate_usage_vacancy",
    "teammate_continuity_score",
    "teammate_star_out_flag",
    "teammate_synergy_points",
    "teammate_synergy_rebounds",
    "teammate_synergy_assists",
    "teammate_on_off_points_delta",
    "teammate_on_off_rebounds_delta",
    "teammate_on_off_assists_delta",
    "shot_style_arc_label",
    "shot_style_arc_score",
    "shot_style_release_label",
    "shot_style_release_score",
    "shot_style_volume_index",
    "shot_style_miss_pressure",
    "team_shot_miss_pressure",
    "opponent_shot_miss_pressure",
    "opponent_avg_height_inches",
    "opponent_height_advantage_inches",
    "shot_style_tall_mismatch_penalty",
    "shot_style_pace_bonus",
    "shot_style_rebound_environment",
    "playstyle_shot_profile_source",
    "playstyle_primary_role",
    "playstyle_scoring_mode",
    "playstyle_rim_rate",
    "playstyle_mid_range_rate",
    "playstyle_three_rate",
    "playstyle_catch_shoot_rate",
    "playstyle_pull_up_rate",
    "playstyle_drive_rate",
    "playstyle_assist_potential",
    "playstyle_paint_touch_rate",
    "playstyle_post_touch_rate",
    "playstyle_elbow_touch_rate",
    "playstyle_rebound_chance_rate",
    "playstyle_offball_activity_rate",
    "playstyle_usage_proxy",
    "playstyle_defensive_event_rate",
    "playstyle_context_confidence",
    "news_article_count_24h",
    "news_injury_mentions_24h",
    "news_starting_mentions_24h",
    "news_minutes_limit_mentions_24h",
    "news_positive_mentions_24h",
    "news_negative_mentions_24h",
    "news_risk_score",
    "news_confidence_score",
    "notes_recent_points_mean_5",
    "notes_recent_rebounds_mean_5",
    "notes_recent_assists_mean_5",
    "notes_recent_minutes_mean_5",
    "notes_recent_points_std_5",
    "notes_recent_minutes_std_5",
    "notes_live_points_per_minute",
    "notes_live_rebounds_per_minute",
    "notes_live_assists_per_minute",
    "notes_live_usage_proxy",
    "notes_live_foul_pressure",
    "notes_live_minutes_current",
    "notes_postgame_positive_mentions_14d",
    "notes_postgame_negative_mentions_14d",
    "notes_postgame_minutes_limit_mentions_14d",
    "notes_postgame_rotation_change_mentions_14d",
    "notes_postgame_risk_score",
    "game_notes_confidence",
    "family_context",
    "expected_minutes",
    "expected_minutes_confidence",
    "minutes_projection_error_estimate",
    "pregame_lock_confidence",
    "pregame_lock_tier",
    "pregame_lock_window_stage",
    "pregame_lock_minutes_to_tipoff",
    "pregame_lock_window_weight",
    "pregame_line_freshness_score",
    "pregame_min_line_age_minutes",
    "commence_time_utc",
    "salary_dk",
    "salary_fd",
    "implied_team_total",
    "game_total",
    "spread",
    "line_points",
    "line_points_consensus",
    "line_points_stddev",
    "line_points_books_count",
    "line_points_snapshot_age_minutes",
    "line_points_open",
    "line_points_close",
    "line_points_movement",
    "line_rebounds",
    "line_rebounds_consensus",
    "line_rebounds_stddev",
    "line_rebounds_books_count",
    "line_rebounds_snapshot_age_minutes",
    "line_rebounds_open",
    "line_rebounds_close",
    "line_rebounds_movement",
    "line_assists",
    "line_assists_consensus",
    "line_assists_stddev",
    "line_assists_books_count",
    "line_assists_snapshot_age_minutes",
    "line_assists_open",
    "line_assists_close",
    "line_assists_movement",
    "line_pra",
    "line_pra_consensus",
    "line_pra_stddev",
    "line_pra_books_count",
    "line_pra_snapshot_age_minutes",
    "line_pra_open",
    "line_pra_close",
    "line_pra_movement",
    "line_three_points_made",
    "line_points_rebounds",
    "line_points_assists",
    "line_rebounds_assists",
    "line_steals",
    "line_blocks",
    "line_turnovers",
    "line_steals_blocks",
    "rest_days",
    "travel_miles",
]

PROP_LINE_COLUMNS = [
    "line_points",
    "line_rebounds",
    "line_assists",
    "line_pra",
    "line_three_points_made",
    "line_points_rebounds",
    "line_points_assists",
    "line_rebounds_assists",
    "line_steals",
    "line_blocks",
    "line_turnovers",
    "line_steals_blocks",
]

LINE_CONSENSUS_COLUMNS = {
    "line_points": {
        "consensus": "line_points_consensus",
        "stddev": "line_points_stddev",
        "books": "line_points_books_count",
        "age": "line_points_snapshot_age_minutes",
    },
    "line_rebounds": {
        "consensus": "line_rebounds_consensus",
        "stddev": "line_rebounds_stddev",
        "books": "line_rebounds_books_count",
        "age": "line_rebounds_snapshot_age_minutes",
    },
    "line_assists": {
        "consensus": "line_assists_consensus",
        "stddev": "line_assists_stddev",
        "books": "line_assists_books_count",
        "age": "line_assists_snapshot_age_minutes",
    },
    "line_pra": {
        "consensus": "line_pra_consensus",
        "stddev": "line_pra_stddev",
        "books": "line_pra_books_count",
        "age": "line_pra_snapshot_age_minutes",
    },
}
LINE_MOVEMENT_COLUMNS = {
    "line_points": {
        "open": "line_points_open",
        "close": "line_points_close",
        "movement": "line_points_movement",
    },
    "line_rebounds": {
        "open": "line_rebounds_open",
        "close": "line_rebounds_close",
        "movement": "line_rebounds_movement",
    },
    "line_assists": {
        "open": "line_assists_open",
        "close": "line_assists_close",
        "movement": "line_assists_movement",
    },
    "line_pra": {
        "open": "line_pra_open",
        "close": "line_pra_close",
        "movement": "line_pra_movement",
    },
}
PROP_LINE_CONTEXT_COLUMNS = PROP_LINE_COLUMNS + sorted(
    {
        column_name
        for metadata in LINE_CONSENSUS_COLUMNS.values()
        for column_name in metadata.values()
    }
    | {
        column_name
        for metadata in LINE_MOVEMENT_COLUMNS.values()
        for column_name in metadata.values()
    }
)

LINEUPS_CONTEXT_COLUMNS = [
    "position",
    "starter",
    "starter_probability",
    "starter_certainty",
    "lineup_status_label",
    "lineup_status_confidence",
    "injury_status",
    "health_status",
]

LIVE_ROSTER_CONTEXT_COLUMNS = [
    "position",
    "starter",
    "starter_probability",
    "starter_certainty",
    "lineup_status_label",
    "lineup_status_confidence",
    "injury_status",
    "health_status",
    "injury_risk_score",
    "injury_minutes_multiplier",
]

INJURY_CONTEXT_COLUMNS = [
    "injury_status",
    "health_status",
    "injury_risk_score",
    "injury_minutes_multiplier",
]

ODDS_CONTEXT_COLUMNS = [
    "spread",
    "game_total",
    "implied_team_total",
    "salary_dk",
    "salary_fd",
]

CONTEXT_KEY_COLUMNS = ["player_name", "game_date", "team"]
NBA_SCHEDULE_TIMEZONE = ZoneInfo("America/New_York")
MINUTES_UNAVAILABLE_PATTERN = re.compile(
    r"\b(out|inactive|suspended|suspension|out for season|g league|two-way)\b",
    flags=re.IGNORECASE,
)
MINUTES_DOUBTFUL_PATTERN = re.compile(r"\bdoubtful\b", flags=re.IGNORECASE)
MINUTES_QUESTIONABLE_PATTERN = re.compile(
    r"\b(?:questionable|gtd|game[-\s]?time decision|day[-\s]?to[-\s]?day|dtd)\b",
    flags=re.IGNORECASE,
)
MINUTES_PROBABLE_PATTERN = re.compile(r"\bprobable\b", flags=re.IGNORECASE)
NEWS_INJURY_PATTERN = re.compile(
    r"\b(?:injury|injured|out|questionable|doubtful|probable|inactive|sidelined|ankle|knee|hamstring|illness|rest)\b",
    flags=re.IGNORECASE,
)
NEWS_STARTING_PATTERN = re.compile(
    r"\b(?:starting lineup|starting five|starting|will start|benched|bench unit|rotation)\b",
    flags=re.IGNORECASE,
)
NEWS_MINUTES_LIMIT_PATTERN = re.compile(
    r"\b(?:minutes restriction|minutes limit|load management|snap count|limited minutes)\b",
    flags=re.IGNORECASE,
)
NEWS_POSITIVE_PATTERN = re.compile(
    r"\b(?:available|cleared|returns|returning|full go|healthy|active)\b",
    flags=re.IGNORECASE,
)
NEWS_NEGATIVE_PATTERN = re.compile(
    r"\b(?:out|inactive|sidelined|ruled out|won't play|unlikely|setback|questionable|doubtful)\b",
    flags=re.IGNORECASE,
)
POSTGAME_ROTATION_PATTERN = re.compile(
    r"\b(?:rotation|bench unit|second unit|stagger|starting five|lineup change|role change)\b",
    flags=re.IGNORECASE,
)
POSTGAME_MINUTES_PATTERN = re.compile(
    r"\b(?:minutes restriction|minutes limit|limited minutes|load management)\b",
    flags=re.IGNORECASE,
)
POSTGAME_POSITIVE_PATTERN = re.compile(
    r"\b(?:healthy|cleared|available|returned|breakout|career[- ]high|strong|dominant)\b",
    flags=re.IGNORECASE,
)
POSTGAME_NEGATIVE_PATTERN = re.compile(
    r"\b(?:injury|setback|struggle|struggled|cold shooting|foul trouble|turnovers?)\b",
    flags=re.IGNORECASE,
)
TEAMMATE_CONTEXT_COLUMNS = [
    "teammate_active_core_count",
    "teammate_out_core_count",
    "teammate_usage_vacancy",
    "teammate_continuity_score",
    "teammate_star_out_flag",
    "teammate_synergy_points",
    "teammate_synergy_rebounds",
    "teammate_synergy_assists",
    "teammate_on_off_points_delta",
    "teammate_on_off_rebounds_delta",
    "teammate_on_off_assists_delta",
]
SHOT_STYLE_CONTEXT_COLUMNS = [
    "shot_style_arc_label",
    "shot_style_arc_score",
    "shot_style_release_label",
    "shot_style_release_score",
    "shot_style_volume_index",
    "shot_style_miss_pressure",
    "team_shot_miss_pressure",
    "opponent_shot_miss_pressure",
    "opponent_avg_height_inches",
    "opponent_height_advantage_inches",
    "shot_style_tall_mismatch_penalty",
    "shot_style_pace_bonus",
    "shot_style_rebound_environment",
]
PLAYSTYLE_CONTEXT_COLUMNS = [
    "playstyle_shot_profile_source",
    "playstyle_primary_role",
    "playstyle_scoring_mode",
    "playstyle_rim_rate",
    "playstyle_mid_range_rate",
    "playstyle_three_rate",
    "playstyle_catch_shoot_rate",
    "playstyle_pull_up_rate",
    "playstyle_drive_rate",
    "playstyle_assist_potential",
    "playstyle_paint_touch_rate",
    "playstyle_post_touch_rate",
    "playstyle_elbow_touch_rate",
    "playstyle_rebound_chance_rate",
    "playstyle_offball_activity_rate",
    "playstyle_usage_proxy",
    "playstyle_defensive_event_rate",
    "playstyle_context_confidence",
]
PLAYSTYLE_NUMERIC_COLUMNS = [
    "playstyle_rim_rate",
    "playstyle_mid_range_rate",
    "playstyle_three_rate",
    "playstyle_catch_shoot_rate",
    "playstyle_pull_up_rate",
    "playstyle_drive_rate",
    "playstyle_assist_potential",
    "playstyle_paint_touch_rate",
    "playstyle_post_touch_rate",
    "playstyle_elbow_touch_rate",
    "playstyle_rebound_chance_rate",
    "playstyle_offball_activity_rate",
    "playstyle_usage_proxy",
    "playstyle_defensive_event_rate",
    "playstyle_context_confidence",
]
NEWS_CONTEXT_COLUMNS = [
    "news_article_count_24h",
    "news_injury_mentions_24h",
    "news_starting_mentions_24h",
    "news_minutes_limit_mentions_24h",
    "news_positive_mentions_24h",
    "news_negative_mentions_24h",
    "news_risk_score",
    "news_confidence_score",
]
GAME_NOTES_CONTEXT_COLUMNS = [
    "notes_recent_points_mean_5",
    "notes_recent_rebounds_mean_5",
    "notes_recent_assists_mean_5",
    "notes_recent_minutes_mean_5",
    "notes_recent_points_std_5",
    "notes_recent_minutes_std_5",
    "notes_live_points_per_minute",
    "notes_live_rebounds_per_minute",
    "notes_live_assists_per_minute",
    "notes_live_usage_proxy",
    "notes_live_foul_pressure",
    "notes_live_minutes_current",
    "notes_postgame_positive_mentions_14d",
    "notes_postgame_negative_mentions_14d",
    "notes_postgame_minutes_limit_mentions_14d",
    "notes_postgame_rotation_change_mentions_14d",
    "notes_postgame_risk_score",
    "game_notes_confidence",
]
ESPN_LIVE_LOG_COLUMNS = [
    "captured_at",
    "captured_at_bucket",
    "source",
    "event_id",
    "game_status",
    "game_status_detail",
    "commence_time_utc",
    "game_date",
    "home_team",
    "away_team",
    "player_name",
    "team",
    "opponent",
    "home",
    "starter",
    "did_not_play",
    "minutes",
    "points",
    "rebounds",
    "assists",
    "steals",
    "blocks",
    "turnovers",
    "three_points_made",
    "field_goals_made",
    "field_goals_attempted",
    "free_throws_made",
    "free_throws_attempted",
    "personal_fouls",
    "plus_minus",
    "notes_live_points_per_minute",
    "notes_live_rebounds_per_minute",
    "notes_live_assists_per_minute",
    "notes_live_usage_proxy",
    "notes_live_foul_pressure",
    "notes_live_minutes_current",
    "summary_url",
]
CONTEXT_CHANGE_SENSITIVE_COLUMNS = [
    "starter",
    "starter_probability",
    "starter_certainty",
    "lineup_status_label",
    "lineup_status_confidence",
    "injury_status",
    "health_status",
    "suspension_status",
    "expected_minutes",
    "pregame_lock_window_stage",
    "pregame_lock_minutes_to_tipoff",
    "pregame_lock_window_weight",
    "injury_risk_score",
    "injury_minutes_multiplier",
    "home_court_points_boost",
    "home_court_minutes_boost",
    "hometown_game_flag",
    "hometown_advantage_score",
] + TEAMMATE_CONTEXT_COLUMNS + SHOT_STYLE_CONTEXT_COLUMNS + PLAYSTYLE_CONTEXT_COLUMNS + NEWS_CONTEXT_COLUMNS + GAME_NOTES_CONTEXT_COLUMNS
PREDICTION_TRIGGER_COLUMNS = [
    "starter",
    "starter_probability",
    "starter_certainty",
    "lineup_status_label",
    "lineup_status_confidence",
    "injury_status",
    "health_status",
    "suspension_status",
    "expected_minutes",
    "expected_minutes_confidence",
    "injury_minutes_multiplier",
    "teammate_star_out_flag",
    "teammate_usage_vacancy",
    "teammate_continuity_score",
    "teammate_on_off_points_delta",
    "teammate_on_off_rebounds_delta",
    "teammate_on_off_assists_delta",
    "shot_style_tall_mismatch_penalty",
    "shot_style_rebound_environment",
    "playstyle_primary_role",
    "playstyle_context_confidence",
    "spread",
    "game_total",
    "implied_team_total",
    "line_points_consensus",
    "line_rebounds_consensus",
    "line_assists_consensus",
    "line_pra_consensus",
    "line_points_movement",
    "line_rebounds_movement",
    "line_assists_movement",
    "line_pra_movement",
    "pregame_lock_window_stage",
    "pregame_lock_minutes_to_tipoff",
    "pregame_lock_window_weight",
    "notes_recent_points_mean_5",
    "notes_recent_minutes_mean_5",
    "notes_live_points_per_minute",
    "notes_live_usage_proxy",
    "notes_postgame_risk_score",
    "game_notes_confidence",
]
CONTEXT_OVERRIDE_COLUMNS = {
    "starter",
    "starter_probability",
    "starter_certainty",
    "lineup_status_label",
    "lineup_status_confidence",
    "injury_status",
    "health_status",
    "suspension_status",
    "injury_risk_score",
    "injury_minutes_multiplier",
    "home_court_points_boost",
    "home_court_minutes_boost",
    "hometown_game_flag",
    "hometown_advantage_score",
    "teammate_active_core_count",
    "teammate_out_core_count",
    "teammate_usage_vacancy",
    "teammate_continuity_score",
    "teammate_star_out_flag",
    "teammate_synergy_points",
    "teammate_synergy_rebounds",
    "teammate_synergy_assists",
    "teammate_on_off_points_delta",
    "teammate_on_off_rebounds_delta",
    "teammate_on_off_assists_delta",
    "shot_style_arc_label",
    "shot_style_arc_score",
    "shot_style_release_label",
    "shot_style_release_score",
    "shot_style_volume_index",
    "shot_style_miss_pressure",
    "team_shot_miss_pressure",
    "opponent_shot_miss_pressure",
    "opponent_avg_height_inches",
    "opponent_height_advantage_inches",
    "shot_style_tall_mismatch_penalty",
    "shot_style_pace_bonus",
    "shot_style_rebound_environment",
    "news_article_count_24h",
    "news_injury_mentions_24h",
    "news_starting_mentions_24h",
    "news_minutes_limit_mentions_24h",
    "news_positive_mentions_24h",
    "news_negative_mentions_24h",
    "news_risk_score",
    "news_confidence_score",
    "expected_minutes",
    "expected_minutes_confidence",
    "minutes_projection_error_estimate",
    "pregame_lock_window_stage",
    "pregame_lock_minutes_to_tipoff",
    "pregame_lock_window_weight",
    "commence_time_utc",
    "spread",
    "game_total",
    "implied_team_total",
    "line_points",
    "line_points_consensus",
    "line_points_stddev",
    "line_points_books_count",
    "line_points_snapshot_age_minutes",
    "line_points_open",
    "line_points_close",
    "line_points_movement",
    "line_rebounds",
    "line_rebounds_consensus",
    "line_rebounds_stddev",
    "line_rebounds_books_count",
    "line_rebounds_snapshot_age_minutes",
    "line_rebounds_open",
    "line_rebounds_close",
    "line_rebounds_movement",
    "line_assists",
    "line_assists_consensus",
    "line_assists_stddev",
    "line_assists_books_count",
    "line_assists_snapshot_age_minutes",
    "line_assists_open",
    "line_assists_close",
    "line_assists_movement",
    "line_pra",
    "line_pra_consensus",
    "line_pra_stddev",
    "line_pra_books_count",
    "line_pra_snapshot_age_minutes",
    "line_pra_open",
    "line_pra_close",
    "line_pra_movement",
    "line_three_points_made",
    "line_points_rebounds",
    "line_points_assists",
    "line_rebounds_assists",
    "line_steals",
    "line_blocks",
    "line_turnovers",
    "line_steals_blocks",
}

TEAM_ALIAS_LOOKUP = {
    "atl": "ATL",
    "atlanta hawks": "ATL",
    "hawks": "ATL",
    "bos": "BOS",
    "boston celtics": "BOS",
    "celtics": "BOS",
    "bkn": "BKN",
    "brooklyn nets": "BKN",
    "nets": "BKN",
    "cha": "CHA",
    "charlotte hornets": "CHA",
    "hornets": "CHA",
    "chi": "CHI",
    "chicago bulls": "CHI",
    "bulls": "CHI",
    "cle": "CLE",
    "cleveland cavaliers": "CLE",
    "cavaliers": "CLE",
    "cavs": "CLE",
    "dal": "DAL",
    "dallas mavericks": "DAL",
    "mavericks": "DAL",
    "mavs": "DAL",
    "den": "DEN",
    "denver nuggets": "DEN",
    "nuggets": "DEN",
    "det": "DET",
    "detroit pistons": "DET",
    "pistons": "DET",
    "gsw": "GSW",
    "golden state warriors": "GSW",
    "warriors": "GSW",
    "hou": "HOU",
    "houston rockets": "HOU",
    "rockets": "HOU",
    "ind": "IND",
    "indiana pacers": "IND",
    "pacers": "IND",
    "lac": "LAC",
    "la clippers": "LAC",
    "los angeles clippers": "LAC",
    "clippers": "LAC",
    "lal": "LAL",
    "la lakers": "LAL",
    "los angeles lakers": "LAL",
    "lakers": "LAL",
    "mem": "MEM",
    "memphis grizzlies": "MEM",
    "grizzlies": "MEM",
    "mia": "MIA",
    "miami heat": "MIA",
    "heat": "MIA",
    "mil": "MIL",
    "milwaukee bucks": "MIL",
    "bucks": "MIL",
    "min": "MIN",
    "minnesota timberwolves": "MIN",
    "timberwolves": "MIN",
    "wolves": "MIN",
    "nop": "NOP",
    "new orleans pelicans": "NOP",
    "pelicans": "NOP",
    "no": "NOP",
    "nyk": "NYK",
    "new york knicks": "NYK",
    "knicks": "NYK",
    "okc": "OKC",
    "oklahoma city thunder": "OKC",
    "thunder": "OKC",
    "orl": "ORL",
    "orlando magic": "ORL",
    "magic": "ORL",
    "phi": "PHI",
    "philadelphia 76ers": "PHI",
    "philadelphia sixers": "PHI",
    "76ers": "PHI",
    "sixers": "PHI",
    "phx": "PHX",
    "phoenix suns": "PHX",
    "suns": "PHX",
    "por": "POR",
    "portland trail blazers": "POR",
    "trail blazers": "POR",
    "blazers": "POR",
    "sac": "SAC",
    "sacramento kings": "SAC",
    "kings": "SAC",
    "sas": "SAS",
    "san antonio spurs": "SAS",
    "spurs": "SAS",
    "tor": "TOR",
    "toronto raptors": "TOR",
    "raptors": "TOR",
    "uta": "UTA",
    "utah jazz": "UTA",
    "jazz": "UTA",
    "was": "WAS",
    "washington wizards": "WAS",
    "wizards": "WAS",
}

TEAM_ID_BY_TRICODE = {
    "ATL": 1,
    "BOS": 2,
    "BKN": 3,
    "CHA": 4,
    "CHI": 5,
    "CLE": 6,
    "DAL": 7,
    "DEN": 8,
    "DET": 9,
    "GSW": 10,
    "HOU": 11,
    "IND": 12,
    "LAC": 13,
    "LAL": 14,
    "MEM": 15,
    "MIA": 16,
    "MIL": 17,
    "MIN": 18,
    "NOP": 19,
    "NYK": 20,
    "OKC": 21,
    "ORL": 22,
    "PHI": 23,
    "PHX": 24,
    "POR": 25,
    "SAC": 26,
    "SAS": 27,
    "TOR": 28,
    "UTA": 29,
    "WAS": 30,
}

TEAM_FULL_NAMES_BY_CODE = {
    "ATL": "Atlanta Hawks",
    "BOS": "Boston Celtics",
    "BKN": "Brooklyn Nets",
    "CHA": "Charlotte Hornets",
    "CHI": "Chicago Bulls",
    "CLE": "Cleveland Cavaliers",
    "DAL": "Dallas Mavericks",
    "DEN": "Denver Nuggets",
    "DET": "Detroit Pistons",
    "GSW": "Golden State Warriors",
    "HOU": "Houston Rockets",
    "IND": "Indiana Pacers",
    "LAC": "Los Angeles Clippers",
    "LAL": "Los Angeles Lakers",
    "MEM": "Memphis Grizzlies",
    "MIA": "Miami Heat",
    "MIL": "Milwaukee Bucks",
    "MIN": "Minnesota Timberwolves",
    "NOP": "New Orleans Pelicans",
    "NYK": "New York Knicks",
    "OKC": "Oklahoma City Thunder",
    "ORL": "Orlando Magic",
    "PHI": "Philadelphia 76ers",
    "PHX": "Phoenix Suns",
    "POR": "Portland Trail Blazers",
    "SAC": "Sacramento Kings",
    "SAS": "San Antonio Spurs",
    "TOR": "Toronto Raptors",
    "UTA": "Utah Jazz",
    "WAS": "Washington Wizards",
}

TEAM_CITY_TERMS_BY_CODE = {
    "ATL": ["atlanta", "georgia"],
    "BOS": ["boston", "massachusetts"],
    "BKN": ["brooklyn", "new york", "nyc"],
    "CHA": ["charlotte", "north carolina"],
    "CHI": ["chicago", "illinois"],
    "CLE": ["cleveland", "ohio"],
    "DAL": ["dallas", "texas"],
    "DEN": ["denver", "colorado"],
    "DET": ["detroit", "michigan"],
    "GSW": ["san francisco", "oakland", "bay area", "california"],
    "HOU": ["houston", "texas"],
    "IND": ["indiana", "indianapolis"],
    "LAC": ["los angeles", "la", "california"],
    "LAL": ["los angeles", "la", "california"],
    "MEM": ["memphis", "tennessee"],
    "MIA": ["miami", "florida"],
    "MIL": ["milwaukee", "wisconsin"],
    "MIN": ["minnesota", "minneapolis", "saint paul"],
    "NOP": ["new orleans", "louisiana"],
    "NYK": ["new york", "manhattan", "nyc"],
    "OKC": ["oklahoma city", "oklahoma"],
    "ORL": ["orlando", "florida"],
    "PHI": ["philadelphia", "pennsylvania"],
    "PHX": ["phoenix", "arizona"],
    "POR": ["portland", "oregon"],
    "SAC": ["sacramento", "california"],
    "SAS": ["san antonio", "texas"],
    "TOR": ["toronto", "ontario", "canada"],
    "UTA": ["utah", "salt lake city"],
    "WAS": ["washington", "dc", "district of columbia"],
}

PROVIDER_COLUMN_ALIASES = {
    "player": "player_name",
    "playername": "player_name",
    "player_name": "player_name",
    "name": "player_name",
    "fullname": "player_name",
    "team": "team",
    "teamcode": "team",
    "teamtricode": "team",
    "teamabbr": "team",
    "teamabbreviation": "team",
    "teamname": "team",
    "team_name": "team",
    "gamedate": "game_date",
    "date": "game_date",
    "game_date": "game_date",
    "status": "injury_status",
    "injury": "injury_status",
    "injurystatus": "injury_status",
    "injury_status": "injury_status",
    "health": "health_status",
    "healthstatus": "health_status",
    "health_status": "health_status",
    "suspension": "suspension_status",
    "suspensionstatus": "suspension_status",
    "suspension_status": "suspension_status",
    "family": "family_context",
    "familycontext": "family_context",
    "family_context": "family_context",
    "expectedminutes": "expected_minutes",
    "projectedminutes": "expected_minutes",
    "minutes_projection": "expected_minutes",
    "minutesprojection": "expected_minutes",
    "expected_minutes": "expected_minutes",
    "starterprobability": "starter_probability",
    "starter_probability": "starter_probability",
    "startercertainty": "starter_certainty",
    "starter_certainty": "starter_certainty",
    "lineupstatuslabel": "lineup_status_label",
    "lineup_status_label": "lineup_status_label",
    "lineupstatusconfidence": "lineup_status_confidence",
    "lineup_status_confidence": "lineup_status_confidence",
    "injuryriskscore": "injury_risk_score",
    "injury_risk_score": "injury_risk_score",
    "injuryminutesmultiplier": "injury_minutes_multiplier",
    "injury_minutes_multiplier": "injury_minutes_multiplier",
    "homecourtpointsboost": "home_court_points_boost",
    "home_court_points_boost": "home_court_points_boost",
    "homecourtminutesboost": "home_court_minutes_boost",
    "home_court_minutes_boost": "home_court_minutes_boost",
    "hometowngameflag": "hometown_game_flag",
    "hometown_game_flag": "hometown_game_flag",
    "hometownadvantagescore": "hometown_advantage_score",
    "hometown_advantage_score": "hometown_advantage_score",
    "teammateactivecorecount": "teammate_active_core_count",
    "teammate_active_core_count": "teammate_active_core_count",
    "teammateoutcorecount": "teammate_out_core_count",
    "teammate_out_core_count": "teammate_out_core_count",
    "teammateusagevacancy": "teammate_usage_vacancy",
    "teammate_usage_vacancy": "teammate_usage_vacancy",
    "teammatecontinuityscore": "teammate_continuity_score",
    "teammate_continuity_score": "teammate_continuity_score",
    "teammatestaroutflag": "teammate_star_out_flag",
    "teammate_star_out_flag": "teammate_star_out_flag",
    "teammatesynergypoints": "teammate_synergy_points",
    "teammate_synergy_points": "teammate_synergy_points",
    "teammatesynergyrebounds": "teammate_synergy_rebounds",
    "teammate_synergy_rebounds": "teammate_synergy_rebounds",
    "teammatesynergyassists": "teammate_synergy_assists",
    "teammate_synergy_assists": "teammate_synergy_assists",
    "teammateonoffpointsdelta": "teammate_on_off_points_delta",
    "teammate_on_off_points_delta": "teammate_on_off_points_delta",
    "teammateonoffreboundsdelta": "teammate_on_off_rebounds_delta",
    "teammate_on_off_rebounds_delta": "teammate_on_off_rebounds_delta",
    "teammateonoffassistsdelta": "teammate_on_off_assists_delta",
    "teammate_on_off_assists_delta": "teammate_on_off_assists_delta",
    "newsarticlecount24h": "news_article_count_24h",
    "news_article_count_24h": "news_article_count_24h",
    "newsinjurymentions24h": "news_injury_mentions_24h",
    "news_injury_mentions_24h": "news_injury_mentions_24h",
    "newsstartingmentions24h": "news_starting_mentions_24h",
    "news_starting_mentions_24h": "news_starting_mentions_24h",
    "newsminuteslimitmentions24h": "news_minutes_limit_mentions_24h",
    "news_minutes_limit_mentions_24h": "news_minutes_limit_mentions_24h",
    "newspositivementions24h": "news_positive_mentions_24h",
    "news_positive_mentions_24h": "news_positive_mentions_24h",
    "newsnegativementions24h": "news_negative_mentions_24h",
    "news_negative_mentions_24h": "news_negative_mentions_24h",
    "newsriskscore": "news_risk_score",
    "news_risk_score": "news_risk_score",
    "newsconfidencescore": "news_confidence_score",
    "news_confidence_score": "news_confidence_score",
    "expectedminutesconfidence": "expected_minutes_confidence",
    "expected_minutes_confidence": "expected_minutes_confidence",
    "minutesprojectionerrorestimate": "minutes_projection_error_estimate",
    "minutes_projection_error_estimate": "minutes_projection_error_estimate",
    "pregamelockwindowstage": "pregame_lock_window_stage",
    "pregame_lock_window_stage": "pregame_lock_window_stage",
    "pregamelockminutestotipoff": "pregame_lock_minutes_to_tipoff",
    "pregame_lock_minutes_to_tipoff": "pregame_lock_minutes_to_tipoff",
    "pregamelockwindowweight": "pregame_lock_window_weight",
    "pregame_lock_window_weight": "pregame_lock_window_weight",
    "commencetimeutc": "commence_time_utc",
    "commence_time_utc": "commence_time_utc",
    "dksalary": "salary_dk",
    "dk_salary": "salary_dk",
    "salarydk": "salary_dk",
    "salary_dk": "salary_dk",
    "fdsalary": "salary_fd",
    "fd_salary": "salary_fd",
    "salaryfd": "salary_fd",
    "salary_fd": "salary_fd",
    "impliedteamtotal": "implied_team_total",
    "implied_team_total": "implied_team_total",
    "gametotal": "game_total",
    "total": "game_total",
    "game_total": "game_total",
    "spread": "spread",
    "line": "spread",
    "linepoints": "line_points",
    "pointsline": "line_points",
    "line_points": "line_points",
    "linepointsconsensus": "line_points_consensus",
    "line_points_consensus": "line_points_consensus",
    "linepointsstddev": "line_points_stddev",
    "line_points_stddev": "line_points_stddev",
    "linepointsbookscount": "line_points_books_count",
    "line_points_books_count": "line_points_books_count",
    "linepointssnapshotageminutes": "line_points_snapshot_age_minutes",
    "line_points_snapshot_age_minutes": "line_points_snapshot_age_minutes",
    "linepointsopen": "line_points_open",
    "line_points_open": "line_points_open",
    "linepointsclose": "line_points_close",
    "line_points_close": "line_points_close",
    "linepointsmovement": "line_points_movement",
    "line_points_movement": "line_points_movement",
    "linerebounds": "line_rebounds",
    "reboundsline": "line_rebounds",
    "line_rebounds": "line_rebounds",
    "linereboundsconsensus": "line_rebounds_consensus",
    "line_rebounds_consensus": "line_rebounds_consensus",
    "linereboundsstddev": "line_rebounds_stddev",
    "line_rebounds_stddev": "line_rebounds_stddev",
    "linereboundsbookscount": "line_rebounds_books_count",
    "line_rebounds_books_count": "line_rebounds_books_count",
    "linereboundssnapshotageminutes": "line_rebounds_snapshot_age_minutes",
    "line_rebounds_snapshot_age_minutes": "line_rebounds_snapshot_age_minutes",
    "linereboundsopen": "line_rebounds_open",
    "line_rebounds_open": "line_rebounds_open",
    "linereboundsclose": "line_rebounds_close",
    "line_rebounds_close": "line_rebounds_close",
    "linereboundsmovement": "line_rebounds_movement",
    "line_rebounds_movement": "line_rebounds_movement",
    "lineassists": "line_assists",
    "assistsline": "line_assists",
    "line_assists": "line_assists",
    "lineassistsconsensus": "line_assists_consensus",
    "line_assists_consensus": "line_assists_consensus",
    "lineassistsstddev": "line_assists_stddev",
    "line_assists_stddev": "line_assists_stddev",
    "lineassistsbookscount": "line_assists_books_count",
    "line_assists_books_count": "line_assists_books_count",
    "lineassistssnapshotageminutes": "line_assists_snapshot_age_minutes",
    "line_assists_snapshot_age_minutes": "line_assists_snapshot_age_minutes",
    "lineassistsopen": "line_assists_open",
    "line_assists_open": "line_assists_open",
    "lineassistsclose": "line_assists_close",
    "line_assists_close": "line_assists_close",
    "lineassistsmovement": "line_assists_movement",
    "line_assists_movement": "line_assists_movement",
    "linepra": "line_pra",
    "praline": "line_pra",
    "line_pra": "line_pra",
    "linepraconsensus": "line_pra_consensus",
    "line_pra_consensus": "line_pra_consensus",
    "lineprastddev": "line_pra_stddev",
    "line_pra_stddev": "line_pra_stddev",
    "lineprabookscount": "line_pra_books_count",
    "line_pra_books_count": "line_pra_books_count",
    "lineprasnapshotageminutes": "line_pra_snapshot_age_minutes",
    "line_pra_snapshot_age_minutes": "line_pra_snapshot_age_minutes",
    "linepraopen": "line_pra_open",
    "line_pra_open": "line_pra_open",
    "linepraclose": "line_pra_close",
    "line_pra_close": "line_pra_close",
    "linepramovement": "line_pra_movement",
    "line_pra_movement": "line_pra_movement",
    "linethreepointsmade": "line_three_points_made",
    "line_3pm": "line_three_points_made",
    "line_three_points_made": "line_three_points_made",
    "linepointsrebounds": "line_points_rebounds",
    "line_points_rebounds": "line_points_rebounds",
    "linepointsassists": "line_points_assists",
    "line_points_assists": "line_points_assists",
    "linereboundsassists": "line_rebounds_assists",
    "line_rebounds_assists": "line_rebounds_assists",
    "linesteals": "line_steals",
    "line_steals": "line_steals",
    "lineblocks": "line_blocks",
    "line_blocks": "line_blocks",
    "lineturnovers": "line_turnovers",
    "line_turnovers": "line_turnovers",
    "linestealsblocks": "line_steals_blocks",
    "line_steals_blocks": "line_steals_blocks",
    "restdays": "rest_days",
    "rest_days": "rest_days",
    "travelmiles": "travel_miles",
    "travel_miles": "travel_miles",
    "home": "home",
    "opponent": "opponent",
}


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _deep_merge_dict(base: dict, override: dict) -> dict:
    merged = dict(base)
    for key, value in override.items():
        if isinstance(value, dict) and isinstance(base.get(key), dict):
            merged[key] = _deep_merge_dict(base[key], value)
        else:
            merged[key] = value
    return merged


def _normalize_lookup_key(value: object) -> str:
    return re.sub(r"[^a-z0-9]+", " ", str(value or "").strip().lower()).strip()


def _normalize_team_code(value: object) -> str | None:
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return None
    normalized = _normalize_lookup_key(value)
    if not normalized:
        return None
    if normalized in TEAM_ALIAS_LOOKUP:
        return TEAM_ALIAS_LOOKUP[normalized]
    compact = normalized.replace(" ", "")
    if compact in TEAM_ALIAS_LOOKUP:
        return TEAM_ALIAS_LOOKUP[compact]
    if len(compact) == 3 and compact.isalpha():
        return compact.upper()
    return None


def _normalize_player_key(value: object) -> str:
    return _normalize_lookup_key(value)


def _normalize_provider_column_name(column: str) -> str:
    return re.sub(r"[^a-z0-9]+", "", column.strip().lower())


def _sanitize_error_message(message: object) -> str:
    sanitized = str(message)
    sanitized = re.sub(r"(Authorization:\s*)([^'\"\]\s]+)", r"\1[redacted]", sanitized)
    sanitized = re.sub(r"(apiKey=)([^&'\"\s]+)", r"\1[redacted]", sanitized)
    return sanitized


def _merge_optional_frames(base: pd.DataFrame, override: pd.DataFrame) -> pd.DataFrame:
    if base.empty:
        return override.copy()
    if override.empty:
        return base.copy()
    combined = base.copy()
    for column in override.columns:
        if column not in combined.columns:
            combined[column] = pd.NA
    for column in combined.columns:
        if column not in override.columns:
            override[column] = pd.NA

    aligned_override = override[combined.columns]
    combined = pd.concat([combined, aligned_override], ignore_index=True, sort=False)
    combined = combined.drop_duplicates(subset=CONTEXT_KEY_COLUMNS, keep="last")
    return combined


def _load_provider_env_file(env_path: Path = DEFAULT_PROVIDERS_ENV_PATH) -> dict[str, bool]:
    loaded: dict[str, bool] = {}
    if not env_path.exists():
        return loaded

    for raw_line in env_path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        if line.startswith("export "):
            line = line[len("export ") :].strip()
        if "=" not in line:
            continue

        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip().strip("\"'")
        if not key:
            continue

        if key not in os.environ and value:
            os.environ[key] = value
        loaded[key] = bool(os.environ.get(key))

    return loaded


def _clamp_interval(value: object, fallback: int, minimum: int) -> int:
    return max(int(minimum), _coerce_positive_int(value, max(int(fallback), int(minimum))))


def _normalize_live_config_intervals(config: dict) -> tuple[dict, bool]:
    normalized = _deep_merge_dict(DEFAULT_LIVE_CONFIG, config)
    changed = False

    modules_before = normalized.get("support_modules")
    modules_after = normalize_support_module_config(modules_before)
    if modules_before != modules_after:
        normalized["support_modules"] = modules_after
        changed = True

    def _set_interval(target: dict, key: str, fallback: int, minimum: int) -> None:
        nonlocal changed
        current = target.get(key)
        clamped = _clamp_interval(current, fallback=fallback, minimum=minimum)
        if current != clamped:
            changed = True
        target[key] = clamped

    _set_interval(normalized, "poll_interval_seconds", MIN_SYNC_INTERVAL_SECONDS, MIN_SYNC_INTERVAL_SECONDS)
    _set_interval(
        normalized,
        "history_backfill_max_runtime_seconds",
        DEFAULT_BACKFILL_MAX_RUNTIME_SECONDS,
        5,
    )
    _set_interval(
        normalized,
        "projection_refresh_interval_seconds",
        MIN_PROJECTION_INTERVAL_SECONDS,
        MIN_PROJECTION_INTERVAL_SECONDS,
    )
    _set_interval(
        normalized,
        "prediction_min_interval_seconds",
        MIN_PROJECTION_INTERVAL_SECONDS,
        MIN_PROJECTION_INTERVAL_SECONDS,
    )
    _set_interval(
        normalized,
        "prediction_max_rows_per_cycle",
        400,
        50,
    )
    _set_interval(
        normalized,
        "live_projection_horizon_hours",
        48,
        6,
    )
    _set_interval(
        normalized,
        "max_upcoming_rows_per_cycle",
        250,
        100,
    )
    _set_interval(
        normalized,
        "expected_minutes_refresh_interval_seconds",
        1800,
        60,
    )
    _set_interval(
        normalized,
        "teammate_context_refresh_interval_seconds",
        1800,
        60,
    )
    _set_interval(
        normalized,
        "shot_style_context_refresh_interval_seconds",
        300,
        60,
    )
    _set_interval(
        normalized,
        "in_game_projection_refresh_interval_seconds",
        MIN_IN_GAME_REFRESH_INTERVAL_SECONDS,
        MIN_IN_GAME_REFRESH_INTERVAL_SECONDS,
    )
    _set_interval(normalized, "retrain_interval_seconds", MIN_RETRAIN_INTERVAL_SECONDS, MIN_RETRAIN_INTERVAL_SECONDS)
    _set_interval(
        normalized,
        "optimization_interval_seconds",
        MIN_OPTIMIZATION_INTERVAL_SECONDS,
        MIN_OPTIMIZATION_INTERVAL_SECONDS,
    )
    _set_interval(
        normalized,
        "benchmark_run_interval_seconds",
        MIN_BENCHMARK_INTERVAL_SECONDS,
        MIN_BENCHMARK_INTERVAL_SECONDS,
    )

    providers = normalized.get("providers", {})
    if isinstance(providers, dict):
        profiles = providers.get("player_profiles", {})
        if isinstance(profiles, dict):
            _set_interval(
                profiles,
                "refresh_interval_seconds",
                MIN_PROFILE_REFRESH_INTERVAL_SECONDS,
                MIN_PROFILE_REFRESH_INTERVAL_SECONDS,
            )

        playstyle = providers.get("playstyle", {})
        if isinstance(playstyle, dict):
            _set_interval(
                playstyle,
                "refresh_interval_seconds",
                MIN_PLAYSTYLE_REFRESH_INTERVAL_SECONDS,
                MIN_PLAYSTYLE_REFRESH_INTERVAL_SECONDS,
            )
            _set_interval(
                playstyle,
                "remote_refresh_interval_seconds",
                max(MIN_PLAYSTYLE_REFRESH_INTERVAL_SECONDS * 2, 900),
                max(MIN_PLAYSTYLE_REFRESH_INTERVAL_SECONDS * 2, 900),
            )
            _set_interval(
                playstyle,
                "timeout_seconds",
                DEFAULT_PROVIDER_REQUEST_TIMEOUT_SECONDS,
                3,
            )

        news = providers.get("news", {})
        if isinstance(news, dict):
            _set_interval(
                news,
                "refresh_interval_seconds",
                MIN_NEWS_REFRESH_INTERVAL_SECONDS,
                MIN_NEWS_REFRESH_INTERVAL_SECONDS,
            )
            refresh_minutes = _clamp_interval(
                news.get("refresh_interval_minutes", 5),
                fallback=5,
                minimum=max(1, int(MIN_NEWS_REFRESH_INTERVAL_SECONDS // 60)),
            )
            if news.get("refresh_interval_minutes") != refresh_minutes:
                changed = True
            news["refresh_interval_minutes"] = refresh_minutes

        lineups = providers.get("lineups", {})
        if isinstance(lineups, dict):
            _set_interval(
                lineups,
                "refresh_interval_seconds",
                DEFAULT_LINEUPS_REFRESH_INTERVAL_SECONDS,
                MIN_PROVIDER_REFRESH_INTERVAL_SECONDS,
            )
            _set_interval(
                lineups,
                "request_timeout_seconds",
                DEFAULT_PROVIDER_REQUEST_TIMEOUT_SECONDS,
                3,
            )
            _set_interval(
                lineups,
                "max_dates_per_cycle",
                2,
                1,
            )

        live_rosters = providers.get("live_rosters", {})
        if isinstance(live_rosters, dict):
            _set_interval(
                live_rosters,
                "refresh_interval_seconds",
                DEFAULT_LIVE_ROSTER_REFRESH_INTERVAL_SECONDS,
                MIN_PROVIDER_REFRESH_INTERVAL_SECONDS,
            )

        odds = providers.get("odds", {})
        if isinstance(odds, dict):
            _set_interval(
                odds,
                "refresh_interval_seconds",
                DEFAULT_ODDS_REFRESH_INTERVAL_SECONDS,
                MIN_PROVIDER_REFRESH_INTERVAL_SECONDS,
            )
            _set_interval(
                odds,
                "request_timeout_seconds",
                DEFAULT_PROVIDER_REQUEST_TIMEOUT_SECONDS,
                3,
            )

        player_props = providers.get("player_props", {})
        if isinstance(player_props, dict):
            _set_interval(
                player_props,
                "refresh_interval_seconds",
                DEFAULT_PLAYER_PROPS_REFRESH_INTERVAL_SECONDS,
                MIN_PROVIDER_REFRESH_INTERVAL_SECONDS,
            )
            _set_interval(
                player_props,
                "request_timeout_seconds",
                DEFAULT_PROVIDER_REQUEST_TIMEOUT_SECONDS,
                3,
            )
            _set_interval(
                player_props,
                "max_events_per_cycle",
                8,
                1,
            )

        rotowire = providers.get("rotowire_prizepicks", {})
        if isinstance(rotowire, dict):
            _set_interval(
                rotowire,
                "refresh_interval_seconds",
                DEFAULT_ROTOWIRE_REFRESH_INTERVAL_SECONDS,
                MIN_PROVIDER_REFRESH_INTERVAL_SECONDS,
            )
            _set_interval(
                rotowire,
                "request_timeout_seconds",
                DEFAULT_PROVIDER_REQUEST_TIMEOUT_SECONDS,
                3,
            )

        injuries = providers.get("injuries", {})
        if isinstance(injuries, dict):
            _set_interval(
                injuries,
                "refresh_interval_seconds",
                DEFAULT_INJURY_REFRESH_INTERVAL_SECONDS,
                MIN_PROVIDER_REFRESH_INTERVAL_SECONDS,
            )
            _set_interval(
                injuries,
                "request_timeout_seconds",
                10,
                3,
            )

        game_notes = providers.get("game_notes", {})
        if isinstance(game_notes, dict):
            _set_interval(
                game_notes,
                "refresh_interval_seconds",
                MIN_IN_GAME_REFRESH_INTERVAL_SECONDS,
                MIN_IN_GAME_REFRESH_INTERVAL_SECONDS,
            )
            _set_interval(
                game_notes,
                "postgame_refresh_interval_seconds",
                300,
                30,
            )
            _set_interval(
                game_notes,
                "daily_compile_interval_seconds",
                900,
                60,
            )
            _set_interval(
                game_notes,
                "max_live_rows_retained",
                250_000,
                1_000,
            )
            _set_interval(
                game_notes,
                "max_postgame_rows_retained",
                120_000,
                1_000,
            )
            _set_interval(
                game_notes,
                "max_daily_rows_retained",
                250_000,
                1_000,
            )
            _set_interval(
                game_notes,
                "request_timeout_seconds",
                DEFAULT_PROVIDER_REQUEST_TIMEOUT_SECONDS,
                3,
            )

        espn_live = providers.get("espn_live", {})
        if isinstance(espn_live, dict):
            _set_interval(
                espn_live,
                "refresh_interval_seconds",
                MIN_IN_GAME_REFRESH_INTERVAL_SECONDS,
                MIN_PROVIDER_REFRESH_INTERVAL_SECONDS,
            )
            _set_interval(
                espn_live,
                "request_timeout_seconds",
                DEFAULT_PROVIDER_REQUEST_TIMEOUT_SECONDS,
                3,
            )
            _set_interval(
                espn_live,
                "max_dates_per_cycle",
                2,
                1,
            )
            _set_interval(
                espn_live,
                "max_events_per_cycle",
                20,
                1,
            )
            _set_interval(
                espn_live,
                "max_rows_retained",
                500_000,
                1_000,
            )

        cloud_archive = providers.get("cloud_archive", {})
        if isinstance(cloud_archive, dict):
            _set_interval(
                cloud_archive,
                "sync_interval_seconds",
                60,
                10,
            )
            _set_interval(
                cloud_archive,
                "hydrate_interval_seconds",
                3600,
                60,
            )

        neon_sync = providers.get("neon_sync", {})
        if isinstance(neon_sync, dict):
            _set_interval(
                neon_sync,
                "sync_interval_seconds",
                DEFAULT_NEON_SYNC_INTERVAL_SECONDS,
                30,
            )
            _set_interval(
                neon_sync,
                "max_dataset_bytes",
                DEFAULT_NEON_MAX_DATASET_BYTES,
                50_000,
            )
            _set_interval(
                neon_sync,
                "max_rows_per_dataset",
                250_000,
                1_000,
            )

        if isinstance(news, dict):
            _set_interval(
                news,
                "request_timeout_seconds",
                DEFAULT_PROVIDER_REQUEST_TIMEOUT_SECONDS,
                3,
            )
            _set_interval(
                news,
                "max_runtime_seconds",
                DEFAULT_NEWS_MAX_RUNTIME_SECONDS,
                2,
            )

    return normalized, changed


def load_live_config(config_path: Path = DEFAULT_LIVE_CONFIG_PATH) -> dict:
    _load_provider_env_file()
    config_path.parent.mkdir(parents=True, exist_ok=True)
    if not config_path.exists():
        config_path.write_text(json.dumps(DEFAULT_LIVE_CONFIG, indent=2), encoding="utf-8")
        return dict(DEFAULT_LIVE_CONFIG)

    loaded = json.loads(config_path.read_text(encoding="utf-8"))
    normalized, changed = _normalize_live_config_intervals(loaded)
    if changed:
        save_live_config(normalized, config_path=config_path)
    return normalized


def save_live_config(config: dict, config_path: Path = DEFAULT_LIVE_CONFIG_PATH) -> None:
    config_path.parent.mkdir(parents=True, exist_ok=True)
    config_path.write_text(json.dumps(config, indent=2), encoding="utf-8")


def load_live_state(state_path: Path = DEFAULT_LIVE_STATE_PATH) -> dict:
    if not state_path.exists():
        return {
            "last_sync_at": None,
            "last_error": None,
            "games_seen": 0,
            "games_scheduled": 0,
            "games_live": 0,
            "games_final": 0,
            "completed_rows_appended": 0,
            "upcoming_rows_generated": 0,
            "provider_context_rows": 0,
            "providers": {},
            "last_train_triggered": False,
            "last_predict_triggered": False,
            "scoreboard_rows_appended": 0,
            "backfill_rows_appended": 0,
            "backfill_games_scanned": 0,
            "backfill_games_fetched": 0,
            "backfill_games_failed": 0,
            "backfill_window_start": None,
            "backfill_window_end": None,
            "backfill_note": None,
            "backfill_last_run_at": None,
            "model_training_lookback_days": None,
            "last_projection_refresh_at": None,
            "last_in_game_projection_refresh_at": None,
            "last_optimization_at": None,
            "last_retrain_refresh_at": None,
            "last_benchmark_run_at": None,
            "next_projection_refresh_due_at": None,
            "next_in_game_projection_refresh_due_at": None,
            "next_optimization_due_at": None,
            "next_retrain_due_at": None,
            "next_benchmark_run_due_at": None,
            "last_news_refresh_at": None,
            "next_news_refresh_due_at": None,
            "last_expected_minutes_refresh_at": None,
            "next_expected_minutes_refresh_due_at": None,
            "last_teammate_context_refresh_at": None,
            "next_teammate_context_refresh_due_at": None,
            "last_shot_style_context_refresh_at": None,
            "next_shot_style_context_refresh_due_at": None,
            "last_game_notes_live_refresh_at": None,
            "next_game_notes_live_refresh_due_at": None,
            "last_postgame_review_refresh_at": None,
            "next_postgame_review_refresh_due_at": None,
            "last_game_notes_daily_compile_at": None,
            "next_game_notes_daily_compile_due_at": None,
            "game_notes_live_rows": 0,
            "postgame_review_rows": 0,
            "game_notes_daily_rows": 0,
            "last_espn_live_refresh_at": None,
            "next_espn_live_refresh_due_at": None,
            "espn_live_rows": 0,
            "espn_live_rows_appended": 0,
            "espn_live_rows_appended_to_game_notes": 0,
            "espn_live_events_loaded": 0,
            "last_cloud_archive_sync_at": None,
            "next_cloud_archive_sync_due_at": None,
            "last_cloud_hydrate_at": None,
            "next_cloud_hydrate_due_at": None,
            "cloud_archive_rows_synced": 0,
            "cloud_archive_enabled": False,
            "cloud_archive_path": None,
            "cloud_archive_note": None,
            "cloud_archive_last_error": None,
            "last_neon_sync_at": None,
            "next_neon_sync_due_at": None,
            "neon_sync_rows_synced": 0,
            "neon_sync_enabled": False,
            "neon_sync_database_host": None,
            "neon_sync_note": None,
            "neon_sync_last_error": None,
            "pregame_lock_window_rows": 0,
            "rows_in_lock_windows": 0,
            "benchmark_rows_evaluated": 0,
            "benchmark_last_generated_at": None,
            "benchmark_last_error": None,
            "benchmark_last_note": None,
            "contract_drift_last_run_at": None,
            "contract_drift_next_due_at": None,
            "contract_drift_interval_seconds": 86400,
            "contract_drift_summary": {},
            "contract_drift_warning_count": 0,
            "contract_drift_warnings": [],
            "contract_drift_last_error": None,
            "projection_context_signature": None,
            "projection_context_changed": False,
            "last_context_change_detected_at": None,
            "lineup_rows_matched": 0,
            "live_roster_rows_matched": 0,
            "live_roster_games_loaded": 0,
            "player_props_rows_matched": 0,
            "rotowire_prizepicks_rows_matched": 0,
            "playstyle_rows_matched": 0,
            "shot_style_context_rows": 0,
            "shot_style_opponent_rows": 0,
            "shot_style_rebound_rows": 0,
            "playstyle_context_rows": 0,
            "news_rows_matched": 0,
            "news_articles_loaded": 0,
            "home_context_rows": 0,
            "hometown_context_rows": 0,
            "teammate_context_rows": 0,
            "profile_cache_rows": 0,
            "profiles_fetched": 0,
            "projection_refresh_interval_seconds": MIN_PROJECTION_INTERVAL_SECONDS,
            "in_game_projection_refresh_interval_seconds": MIN_IN_GAME_REFRESH_INTERVAL_SECONDS,
            "optimization_interval_seconds": MIN_OPTIMIZATION_INTERVAL_SECONDS,
            "retrain_interval_seconds": MIN_RETRAIN_INTERVAL_SECONDS,
            "benchmark_run_interval_seconds": MIN_BENCHMARK_INTERVAL_SECONDS,
            "in_game_projection_rows_updated": 0,
            "in_game_projection_players_tracked": 0,
            "in_game_projection_games_tracked": 0,
            "in_game_projection_live_games_active": 0,
            "in_game_projection_note": None,
            "in_game_projection_last_error": None,
            "optimization_summary": {},
            "last_sync_duration_seconds": 0.0,
            "prediction_rows_used": 0,
            "support_modules": {},
            "module_alerts": [],
            "module_alerts_count": 0,
        }
    loaded = json.loads(state_path.read_text(encoding="utf-8"))
    normalized = dict(loaded)
    changed = False

    default_state = {
        "last_expected_minutes_refresh_at": None,
        "next_expected_minutes_refresh_due_at": None,
        "last_teammate_context_refresh_at": None,
        "next_teammate_context_refresh_due_at": None,
        "last_shot_style_context_refresh_at": None,
        "next_shot_style_context_refresh_due_at": None,
        "last_game_notes_live_refresh_at": None,
        "next_game_notes_live_refresh_due_at": None,
        "last_postgame_review_refresh_at": None,
        "next_postgame_review_refresh_due_at": None,
        "last_game_notes_daily_compile_at": None,
        "next_game_notes_daily_compile_due_at": None,
        "game_notes_live_rows": 0,
        "postgame_review_rows": 0,
        "game_notes_daily_rows": 0,
        "last_espn_live_refresh_at": None,
        "next_espn_live_refresh_due_at": None,
        "espn_live_rows": 0,
        "espn_live_rows_appended": 0,
        "espn_live_rows_appended_to_game_notes": 0,
        "espn_live_events_loaded": 0,
        "last_cloud_archive_sync_at": None,
        "next_cloud_archive_sync_due_at": None,
        "last_cloud_hydrate_at": None,
        "next_cloud_hydrate_due_at": None,
        "cloud_archive_rows_synced": 0,
        "cloud_archive_enabled": False,
        "cloud_archive_path": None,
        "cloud_archive_note": None,
        "cloud_archive_last_error": None,
        "last_neon_sync_at": None,
        "next_neon_sync_due_at": None,
        "neon_sync_rows_synced": 0,
        "neon_sync_enabled": False,
        "neon_sync_database_host": None,
        "neon_sync_note": None,
        "neon_sync_last_error": None,
        "support_modules": {},
        "module_alerts": [],
        "module_alerts_count": 0,
        "pregame_lock_window_rows": 0,
        "rows_in_lock_windows": 0,
        "last_sync_duration_seconds": 0.0,
        "prediction_rows_used": 0,
    }
    for key, value in default_state.items():
        if key not in normalized:
            normalized[key] = value
            changed = True

    interval_fields = [
        ("projection_refresh_interval_seconds", MIN_PROJECTION_INTERVAL_SECONDS, MIN_PROJECTION_INTERVAL_SECONDS),
        ("in_game_projection_refresh_interval_seconds", MIN_IN_GAME_REFRESH_INTERVAL_SECONDS, MIN_IN_GAME_REFRESH_INTERVAL_SECONDS),
        ("optimization_interval_seconds", MIN_OPTIMIZATION_INTERVAL_SECONDS, MIN_OPTIMIZATION_INTERVAL_SECONDS),
        ("retrain_interval_seconds", MIN_RETRAIN_INTERVAL_SECONDS, MIN_RETRAIN_INTERVAL_SECONDS),
        ("benchmark_run_interval_seconds", MIN_BENCHMARK_INTERVAL_SECONDS, MIN_BENCHMARK_INTERVAL_SECONDS),
    ]
    for key, fallback, minimum in interval_fields:
        clamped = _clamp_interval(normalized.get(key), fallback=fallback, minimum=minimum)
        if normalized.get(key) != clamped:
            normalized[key] = clamped
            changed = True

    if changed:
        save_live_state(normalized, state_path)
    return normalized


def save_live_state(state: dict, state_path: Path = DEFAULT_LIVE_STATE_PATH) -> None:
    state_path.parent.mkdir(parents=True, exist_ok=True)
    state_path.write_text(json.dumps(state, indent=2), encoding="utf-8")


def _configured_training_path(config: dict) -> Path:
    configured = Path(config.get("training_data_path") or DEFAULT_TRAINING_UPLOAD_PATH)
    if configured.exists():
        return configured
    if DEFAULT_TRAINING_UPLOAD_PATH.exists():
        return DEFAULT_TRAINING_UPLOAD_PATH
    return DEFAULT_DATA_PATH


def _configured_upcoming_path(config: dict) -> Path:
    return Path(config.get("upcoming_data_path") or DEFAULT_UPCOMING_PATH)


def _configured_context_path(config: dict) -> Path:
    return Path(config.get("context_updates_path") or DEFAULT_CONTEXT_UPDATES_PATH)


def _configured_provider_context_path(config: dict) -> Path:
    return Path(config.get("provider_context_path") or DEFAULT_PROVIDER_CONTEXT_PATH)


FETCH_RETRY_SETTINGS = {
    "attempts": int(DEFAULT_LIVE_CONFIG.get("fetch_retry_attempts", 1)),
    "base_delay_seconds": float(DEFAULT_LIVE_CONFIG.get("fetch_retry_base_delay_seconds", 0.3)),
    "jitter_seconds": float(DEFAULT_LIVE_CONFIG.get("fetch_retry_jitter_seconds", 0.1)),
}

TEAMMATE_SYNERGY_CACHE: dict[str, object] = {
    "signature": None,
    "index": {},
    "last_built_at": None,
}


def _update_fetch_retry_settings_from_config(config: dict) -> None:
    FETCH_RETRY_SETTINGS["attempts"] = max(1, _coerce_positive_int(config.get("fetch_retry_attempts", 1), 1))
    base_delay_raw = pd.to_numeric(
        pd.Series([config.get("fetch_retry_base_delay_seconds", 0.3)]),
        errors="coerce",
    ).iloc[0]
    base_delay = float(base_delay_raw) if pd.notna(base_delay_raw) else 0.3
    FETCH_RETRY_SETTINGS["base_delay_seconds"] = float(max(0.1, base_delay))
    jitter_value = pd.to_numeric(pd.Series([config.get("fetch_retry_jitter_seconds", 0.1)]), errors="coerce").iloc[0]
    FETCH_RETRY_SETTINGS["jitter_seconds"] = float(max(0.0, jitter_value if pd.notna(jitter_value) else 0.1))


def _retry_sleep_seconds(attempt: int) -> float:
    base_delay = float(FETCH_RETRY_SETTINGS.get("base_delay_seconds", 1.5))
    jitter = float(FETCH_RETRY_SETTINGS.get("jitter_seconds", 0.4))
    exponential = base_delay * (attempt + 1)
    return max(0.1, exponential + random.uniform(0.0, jitter))


def _default_request_headers(headers: dict[str, str] | None = None) -> dict[str, str]:
    request_headers = {"User-Agent": "NBAPredictionEngine/1.0"}
    if headers:
        request_headers.update(headers)
    return request_headers


def _run_curl_request(
    *,
    url: str,
    request_headers: dict[str, str],
    timeout: int,
    binary: bool,
) -> bytes | str:
    command = [
        "curl",
        "-fsSL",
        "--retry",
        "0",
        "--connect-timeout",
        str(max(2, int(timeout))),
        "--max-time",
        str(max(3, int(timeout))),
        "--http1.1",
        "-A",
        request_headers["User-Agent"],
    ]
    for key, value in request_headers.items():
        if key.lower() == "user-agent":
            continue
        command.extend(["-H", f"{key}: {value}"])
    command.append(url)
    response = subprocess.run(
        command,
        check=True,
        capture_output=True,
        text=not binary,
        timeout=timeout,
    )
    return response.stdout


def _fetch_bytes_with_retry(
    *,
    url: str,
    timeout: int = DEFAULT_PROVIDER_REQUEST_TIMEOUT_SECONDS,
    headers: dict[str, str] | None = None,
) -> bytes:
    request_headers = _default_request_headers(headers)
    attempts = max(1, int(FETCH_RETRY_SETTINGS.get("attempts", 3)))
    last_error: Exception | None = None
    for attempt in range(attempts):
        request = Request(url, headers=request_headers)
        try:
            with urlopen(request, timeout=timeout) as response:  # noqa: S310
                return response.read()
        except (HTTPError, URLError, OSError, ValueError) as primary_error:
            last_error = primary_error
            try:
                curl_payload = _run_curl_request(
                    url=url,
                    request_headers=request_headers,
                    timeout=timeout,
                    binary=True,
                )
                if isinstance(curl_payload, str):
                    return curl_payload.encode("utf-8", errors="ignore")
                return curl_payload
            except subprocess.SubprocessError as curl_error:
                last_error = curl_error
        if attempt < (attempts - 1):
            time.sleep(_retry_sleep_seconds(attempt))
    if last_error is not None:
        raise last_error
    raise ValueError("Failed to fetch payload and no error details were captured.")


def fetch_json(
    url: str,
    timeout: int = DEFAULT_PROVIDER_REQUEST_TIMEOUT_SECONDS,
    headers: dict[str, str] | None = None,
) -> dict:
    payload = _fetch_bytes_with_retry(url=url, timeout=timeout, headers=headers)
    return json.loads(payload.decode("utf-8"))


def fetch_text(
    url: str,
    timeout: int = DEFAULT_PROVIDER_REQUEST_TIMEOUT_SECONDS,
    headers: dict[str, str] | None = None,
) -> str:
    payload = _fetch_bytes_with_retry(url=url, timeout=timeout, headers=headers)
    return payload.decode("utf-8")


def fetch_binary(
    url: str,
    timeout: int = DEFAULT_PROVIDER_REQUEST_TIMEOUT_SECONDS,
    headers: dict[str, str] | None = None,
) -> bytes:
    return _fetch_bytes_with_retry(url=url, timeout=timeout, headers=headers)


def fetch_scoreboard() -> dict:
    return fetch_json(SCOREBOARD_URL)


def fetch_schedule() -> dict:
    return fetch_json(SCHEDULE_URL)


def fetch_boxscore(game_id: str) -> dict:
    return fetch_json(BOXSCORE_URL_TEMPLATE.format(game_id=game_id))


def _parse_iso_datetime(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _coerce_positive_int(value: object, fallback: int) -> int:
    numeric = pd.to_numeric(pd.Series([value]), errors="coerce").iloc[0]
    if pd.isna(numeric):
        return int(fallback)
    coerced = int(numeric)
    return coerced if coerced > 0 else int(fallback)


def _is_due(last_run_iso: str | None, interval_minutes: int, now_utc: datetime | None = None) -> tuple[bool, str | None]:
    if interval_minutes <= 0:
        return False, None
    now_value = now_utc or datetime.now(timezone.utc)
    last_run = _parse_iso_datetime(last_run_iso)
    if last_run is None:
        return True, now_value.isoformat()
    next_due = last_run + timedelta(minutes=interval_minutes)
    return now_value >= next_due, next_due.isoformat()


def _is_due_seconds(last_run_iso: str | None, interval_seconds: int, now_utc: datetime | None = None) -> tuple[bool, str | None]:
    if interval_seconds <= 0:
        return False, None
    now_value = now_utc or datetime.now(timezone.utc)
    last_run = _parse_iso_datetime(last_run_iso)
    if last_run is None:
        return True, now_value.isoformat()
    next_due = last_run + timedelta(seconds=interval_seconds)
    return now_value >= next_due, next_due.isoformat()


def _safe_float_value(value: object, default: float = 0.0) -> float:
    if value is None:
        return float(default)
    if isinstance(value, (int, float, np.integer, np.floating)):
        if pd.isna(value):
            return float(default)
        return float(value)
    if isinstance(value, str):
        stripped = value.strip()
        if not stripped or stripped.lower() in {"nan", "na", "none", "null"}:
            return float(default)
        try:
            return float(stripped)
        except ValueError:
            return float(default)
    try:
        numeric = pd.to_numeric(value, errors="coerce")
    except Exception:  # noqa: BLE001
        return float(default)
    if pd.isna(numeric):
        return float(default)
    return float(numeric)


def _stable_frame_signature(frame: pd.DataFrame, key_columns: list[str], value_columns: list[str]) -> str:
    if frame.empty:
        return ""
    relevant_keys = [column for column in key_columns if column in frame.columns]
    if not relevant_keys:
        return ""
    relevant_values = [column for column in value_columns if column in frame.columns]
    if not relevant_values:
        return ""

    signature_frame = frame[relevant_keys + relevant_values].copy()
    for column in signature_frame.columns:
        if column in relevant_keys:
            signature_frame[column] = signature_frame[column].astype("string").fillna("").str.strip().str.lower()
        else:
            if pd.api.types.is_numeric_dtype(signature_frame[column]):
                signature_frame[column] = pd.to_numeric(signature_frame[column], errors="coerce").round(4)
            else:
                signature_frame[column] = signature_frame[column].astype("string").fillna("").str.strip().str.lower()
    signature_frame = signature_frame.sort_values(relevant_keys).drop_duplicates(subset=relevant_keys, keep="last")
    payload = signature_frame.to_csv(index=False).encode("utf-8")
    return hashlib.sha256(payload).hexdigest()


def _cached_provider_rows(
    upcoming_frame: pd.DataFrame,
    provider_context_path: Path,
    context_columns: list[str],
) -> pd.DataFrame:
    if upcoming_frame.empty or not provider_context_path.exists():
        return pd.DataFrame(columns=CONTEXT_KEY_COLUMNS + context_columns)
    try:
        raw = pd.read_csv(provider_context_path)
    except (OSError, pd.errors.EmptyDataError):
        return pd.DataFrame(columns=CONTEXT_KEY_COLUMNS + context_columns)
    if raw.empty:
        return pd.DataFrame(columns=CONTEXT_KEY_COLUMNS + context_columns)

    available_columns = [column for column in context_columns if column in raw.columns]
    if not available_columns:
        return pd.DataFrame(columns=CONTEXT_KEY_COLUMNS + context_columns)
    standardized = _standardize_provider_frame(raw[CONTEXT_KEY_COLUMNS + available_columns])
    if standardized.empty:
        return pd.DataFrame(columns=CONTEXT_KEY_COLUMNS + context_columns)
    return _align_provider_rows_to_upcoming(upcoming_frame, standardized)


def _metric_from_recheck_payload(payload: dict) -> float | None:
    if not isinstance(payload, dict):
        return None
    operational = payload.get("operational_subset", {})
    if isinstance(operational, dict):
        candidate = pd.to_numeric(
            pd.Series([operational.get("mean_abs_pct_error_floor")]),
            errors="coerce",
        ).iloc[0]
        if pd.notna(candidate):
            return float(candidate)
    overall = payload.get("overall", {})
    if isinstance(overall, dict):
        candidate = pd.to_numeric(pd.Series([overall.get("mean_abs_pct_error")]), errors="coerce").iloc[0]
        if pd.notna(candidate):
            return float(candidate)
    return None


def _run_lookback_optimization(
    training_path: Path,
    candidate_lookbacks: list[int],
    recheck_sample_rows: int | None = None,
) -> dict:
    tested: list[dict] = []
    best_metric: float | None = None
    best_lookback: int | None = None

    unique_candidates: list[int] = []
    for value in candidate_lookbacks:
        numeric = pd.to_numeric(pd.Series([value]), errors="coerce").iloc[0]
        if pd.isna(numeric):
            continue
        candidate = int(numeric)
        if candidate > 0 and candidate not in unique_candidates:
            unique_candidates.append(candidate)
    unique_candidates = sorted(unique_candidates)
    if not unique_candidates:
        return {
            "ran": False,
            "reason": "No valid lookback candidates were configured.",
            "candidate_count": 0,
            "tested": [],
            "best_lookback_days": None,
            "best_metric": None,
        }

    for lookback in unique_candidates:
        try:
            train_engine(data_path=training_path, lookback_days=lookback)
            recheck_payload = recheck_past_predictions(
                data_path=training_path,
                lookback_days=lookback,
                sample_rows=recheck_sample_rows if recheck_sample_rows and recheck_sample_rows > 0 else None,
            )
            metric = _metric_from_recheck_payload(recheck_payload)
            tested.append(
                {
                    "lookback_days": int(lookback),
                    "metric": metric,
                    "evaluated_rows": int(recheck_payload.get("evaluated_rows", 0)),
                    "sample_rows": int(recheck_payload.get("sample_rows", 0)),
                }
            )
            if metric is not None and (best_metric is None or metric < best_metric):
                best_metric = metric
                best_lookback = int(lookback)
        except Exception as exc:  # pragma: no cover
            tested.append({"lookback_days": int(lookback), "metric": None, "error": _sanitize_error_message(exc)})

    if best_lookback is not None:
        train_engine(data_path=training_path, lookback_days=best_lookback)
        final_recheck = recheck_past_predictions(
            data_path=training_path,
            lookback_days=best_lookback,
            sample_rows=recheck_sample_rows if recheck_sample_rows and recheck_sample_rows > 0 else None,
        )
        final_metric = _metric_from_recheck_payload(final_recheck)
        if final_metric is not None:
            best_metric = final_metric

    return {
        "ran": True,
        "candidate_count": int(len(unique_candidates)),
        "tested": tested,
        "best_lookback_days": best_lookback,
        "best_metric": best_metric,
        "recheck_sample_rows": int(recheck_sample_rows) if recheck_sample_rows else None,
    }


def _game_date_from_tipoff(tipoff_utc: datetime | None) -> str | None:
    if tipoff_utc is None:
        return None
    return tipoff_utc.astimezone(NBA_SCHEDULE_TIMEZONE).strftime("%Y-%m-%d")


def _iso_duration_to_minutes(value: str | None) -> float | None:
    if not value:
        return None
    if not value.startswith("PT"):
        return None

    hours = 0.0
    minutes = 0.0
    seconds = 0.0
    token = value[2:]

    if "H" in token:
        hours_str, token = token.split("H", 1)
        hours = float(hours_str or 0)
    if "M" in token:
        minutes_str, token = token.split("M", 1)
        minutes = float(minutes_str or 0)
    if "S" in token:
        seconds_str = token.replace("S", "")
        seconds = float(seconds_str or 0)

    return hours * 60 + minutes + (seconds / 60)


def _boxscore_players_to_rows(game_payload: dict) -> pd.DataFrame:
    game = game_payload["game"]
    game_date = (game.get("gameEt") or game.get("gameTimeUTC") or "")[:10]
    rows: list[dict] = []

    def collect_team_rows(team_payload: dict, opponent_payload: dict, home_flag: int) -> None:
        for player in team_payload.get("players", []):
            if player.get("played") != "1":
                continue

            stats = player.get("statistics", {})
            rows.append(
                {
                    "game_id": game.get("gameId"),
                    "player_name": player.get("name"),
                    "game_date": game_date,
                    "home": home_flag,
                    "opponent": opponent_payload.get("teamTricode"),
                    "team": team_payload.get("teamTricode"),
                    "position": player.get("position"),
                    "starter": 1 if player.get("starter") == "1" else 0,
                    "minutes": _iso_duration_to_minutes(stats.get("minutes") or stats.get("minutesCalculated")),
                    "field_goals_made": stats.get("fieldGoalsMade"),
                    "field_goals_attempted": stats.get("fieldGoalsAttempted"),
                    "three_points_made": stats.get("threePointersMade"),
                    "rebounds": stats.get("reboundsTotal"),
                    "offensive_rebounds": stats.get("reboundsOffensive"),
                    "defensive_rebounds": stats.get("reboundsDefensive"),
                    "assists": stats.get("assists"),
                    "steals": stats.get("steals"),
                    "blocks": stats.get("blocks"),
                    "turnovers": stats.get("turnovers"),
                    "points": stats.get("points"),
                    "free_throws_made": stats.get("freeThrowsMade"),
                    "free_throws_attempted": stats.get("freeThrowsAttempted"),
                    "personal_fouls": stats.get("foulsPersonal"),
                    "plus_minus": stats.get("plusMinusPoints"),
                }
            )

    collect_team_rows(game["homeTeam"], game["awayTeam"], 1)
    collect_team_rows(game["awayTeam"], game["homeTeam"], 0)
    return pd.DataFrame(rows)


def _append_completed_rows(training_path: Path, completed_rows: pd.DataFrame) -> int:
    if completed_rows.empty:
        return 0

    training_path.parent.mkdir(parents=True, exist_ok=True)
    if training_path.exists():
        existing = pd.read_csv(training_path)
        combined = pd.concat([existing, completed_rows], ignore_index=True, sort=False)
    else:
        combined = completed_rows.copy()

    dedupe_keys = [column for column in ["game_id", "player_name"] if column in combined.columns]
    fallback_keys = [column for column in ["player_name", "game_date", "team", "opponent"] if column in combined.columns]

    before = 0
    if training_path.exists():
        before = len(pd.read_csv(training_path))
    if len(dedupe_keys) >= 2:
        combined = combined.drop_duplicates(subset=dedupe_keys, keep="last")
    if len(fallback_keys) >= 4:
        combined = combined.drop_duplicates(subset=fallback_keys, keep="last")
    combined = combined.drop_duplicates().sort_values(["player_name", "game_date"])
    combined.to_csv(training_path, index=False)
    return max(0, len(combined) - before)


def _next_due_iso_from_seconds(
    last_run_iso: str | None,
    interval_seconds: int,
    now_utc: datetime | None = None,
) -> str | None:
    if interval_seconds <= 0:
        return None
    now_value = now_utc or datetime.now(timezone.utc)
    last_run = _parse_iso_datetime(last_run_iso)
    if last_run is None:
        return now_value.isoformat()
    return (last_run + timedelta(seconds=interval_seconds)).isoformat()


def _trim_upcoming_frame_for_live_window(
    frame: pd.DataFrame,
    horizon_hours: int,
    max_rows: int,
) -> pd.DataFrame:
    if frame.empty:
        return frame

    working = frame.copy()
    horizon_hours = max(6, int(horizon_hours))
    max_rows = max(100, int(max_rows))
    today_local = datetime.now(NBA_SCHEDULE_TIMEZONE).date()
    latest_date = today_local + timedelta(days=max(1, int(np.ceil(horizon_hours / 24.0))))

    if "game_date" in working.columns:
        game_dates = pd.to_datetime(working["game_date"], errors="coerce").dt.date
        filtered = working.loc[(game_dates.notna()) & (game_dates <= latest_date)].copy()
        if not filtered.empty:
            working = filtered

    sort_columns: list[str] = []
    ascending: list[bool] = []
    if "game_date" in working.columns:
        working["__game_date_sort"] = pd.to_datetime(working["game_date"], errors="coerce")
        sort_columns.append("__game_date_sort")
        ascending.append(True)
    if "starter_probability" in working.columns:
        working["__starter_probability_sort"] = pd.to_numeric(working["starter_probability"], errors="coerce").fillna(0.0)
        sort_columns.append("__starter_probability_sort")
        ascending.append(False)
    if "expected_minutes" in working.columns:
        working["__expected_minutes_sort"] = pd.to_numeric(working["expected_minutes"], errors="coerce").fillna(0.0)
        sort_columns.append("__expected_minutes_sort")
        ascending.append(False)
    if sort_columns:
        working = working.sort_values(sort_columns, ascending=ascending, na_position="last")
    if len(working) > max_rows:
        working = working.head(max_rows)
    return working.drop(columns=["__game_date_sort", "__starter_probability_sort", "__expected_minutes_sort"], errors="ignore").reset_index(drop=True)


def _collect_live_boxscore_rows(scoreboard_payload: dict) -> tuple[pd.DataFrame, dict]:
    status = {
        "live_games_active": 0,
        "games_fetched": 0,
        "players_tracked": 0,
        "rows": 0,
        "last_error": None,
    }
    games = ((scoreboard_payload or {}).get("scoreboard", {}) or {}).get("games", []) or []
    live_games = [game for game in games if int((game or {}).get("gameStatus", 0) or 0) == 2]
    status["live_games_active"] = int(len(live_games))
    if not live_games:
        return pd.DataFrame(), status

    live_frames: list[pd.DataFrame] = []
    for game in live_games:
        game_id = str(game.get("gameId") or "").strip()
        if not game_id:
            continue
        try:
            payload = fetch_boxscore(game_id)
        except (HTTPError, URLError, ValueError, OSError, subprocess.SubprocessError) as exc:
            status["last_error"] = _sanitize_error_message(exc)
            continue
        frame = _boxscore_players_to_rows(payload)
        if frame.empty:
            continue
        frame = frame.copy()
        frame["game_id"] = frame.get("game_id", game_id).fillna(game_id)
        frame["game_status"] = int(game.get("gameStatus", 0) or 0)
        live_frames.append(frame)
        status["games_fetched"] = int(status["games_fetched"] + 1)

    if not live_frames:
        return pd.DataFrame(), status

    live_rows = pd.concat(live_frames, ignore_index=True, sort=False)
    for column in [
        "minutes",
        "points",
        "rebounds",
        "assists",
        "steals",
        "blocks",
        "turnovers",
        "three_points_made",
    ]:
        if column not in live_rows.columns:
            live_rows[column] = pd.NA
        live_rows[column] = pd.to_numeric(live_rows[column], errors="coerce")
    live_rows["team"] = live_rows.get("team", pd.Series("", index=live_rows.index)).map(_normalize_team_code).fillna(
        live_rows.get("team", pd.Series("", index=live_rows.index))
    )
    live_rows["player_key"] = live_rows.get("player_name", pd.Series("", index=live_rows.index)).map(_normalize_player_key)
    live_rows["team_key"] = live_rows.get("team", pd.Series("", index=live_rows.index)).map(_normalize_team_code)
    live_rows["game_date"] = pd.to_datetime(live_rows.get("game_date"), errors="coerce").dt.strftime("%Y-%m-%d")
    live_rows = live_rows.dropna(subset=["player_key", "team_key", "game_date"])
    if live_rows.empty:
        return pd.DataFrame(), status

    live_rows = live_rows.sort_values(["minutes"], ascending=[True]).drop_duplicates(
        subset=["player_key", "team_key", "game_date"],
        keep="last",
    )
    status["rows"] = int(len(live_rows))
    status["players_tracked"] = int(live_rows["player_key"].nunique())
    return live_rows, status


def _blend_live_projection_component(
    pregame_projection: pd.Series,
    live_current: pd.Series,
    live_minutes: pd.Series,
    expected_minutes: pd.Series,
    live_weight: float,
    pregame_weight: float,
) -> pd.Series:
    pregame = pd.to_numeric(pregame_projection, errors="coerce").fillna(0.0)
    current = pd.to_numeric(live_current, errors="coerce").fillna(0.0)
    minutes = pd.to_numeric(live_minutes, errors="coerce").fillna(0.0).clip(lower=0.0)
    expected = pd.to_numeric(expected_minutes, errors="coerce").fillna(30.0).clip(lower=12.0, upper=44.0)
    expected_final = expected.where(minutes <= expected, (minutes + 4.0).clip(upper=48.0)).clip(lower=12.0, upper=48.0)
    remaining = (expected_final - minutes).clip(lower=0.0)

    pre_rate = pregame / expected_final.replace(0.0, 1.0)
    live_rate = (current / minutes.where(minutes > 0.0, 1.0)).where(minutes > 0.0, pre_rate)

    blended_rate = (live_weight * live_rate) + (pregame_weight * pre_rate)
    rate_floor = pre_rate * 0.45
    rate_ceiling = (pre_rate * 1.9).where((pre_rate * 1.9) > (pre_rate + 0.12), pre_rate + 0.12)
    bounded_rate = blended_rate.clip(lower=rate_floor, upper=rate_ceiling)

    pace_projection = current + remaining * bounded_rate
    progress = (minutes / expected_final.replace(0.0, 1.0)).clip(lower=0.0, upper=1.0)
    blend_weight = (0.3 + 0.6 * progress).clip(lower=0.22, upper=0.9)
    blend_weight = blend_weight.where(minutes >= 3.0, 0.18)

    adjusted = (1.0 - blend_weight) * pregame + blend_weight * pace_projection
    return adjusted.where(adjusted >= current, current)


def _apply_in_game_projection_adjustments(
    predictions_path: Path,
    live_rows: pd.DataFrame,
    config: dict,
) -> dict:
    status = {
        "rows_updated": 0,
        "players_updated": 0,
        "games_updated": 0,
        "note": None,
        "last_error": None,
    }
    if live_rows.empty:
        status["note"] = "No live player rows available for in-game projection refresh."
        return status
    if not predictions_path.exists():
        status["note"] = "Predictions file does not exist yet."
        return status

    try:
        predictions = pd.read_csv(predictions_path)
    except (OSError, pd.errors.EmptyDataError, ValueError) as exc:
        status["last_error"] = _sanitize_error_message(exc)
        status["note"] = "Predictions file could not be loaded."
        return status

    if predictions.empty:
        status["note"] = "Predictions file is empty."
        return status
    required_prediction_columns = {"player_name", "team", "game_date"}
    if not required_prediction_columns.issubset(set(predictions.columns)):
        status["note"] = "Predictions file is missing required identity columns."
        return status

    working = predictions.copy()
    working["__row_id"] = pd.RangeIndex(start=0, stop=len(working), step=1)
    working["player_key"] = working["player_name"].map(_normalize_player_key)
    working["team_key"] = working["team"].map(_normalize_team_code)
    working["game_date"] = pd.to_datetime(working["game_date"], errors="coerce").dt.strftime("%Y-%m-%d")

    live = live_rows.copy()
    live = live.rename(
        columns={
            "game_id": "live_game_id",
            "minutes": "live_minutes_played",
            "points": "live_points_current",
            "rebounds": "live_rebounds_current",
            "assists": "live_assists_current",
            "steals": "live_steals_current",
            "blocks": "live_blocks_current",
            "turnovers": "live_turnovers_current",
            "three_points_made": "live_three_points_current",
        }
    )
    live_match_columns = [
        "player_key",
        "team_key",
        "game_date",
        "live_game_id",
        "live_minutes_played",
        "live_points_current",
        "live_rebounds_current",
        "live_assists_current",
        "live_steals_current",
        "live_blocks_current",
        "live_turnovers_current",
        "live_three_points_current",
    ]
    live = live[live_match_columns].drop_duplicates(subset=["player_key", "team_key", "game_date"], keep="last")
    existing_live_columns = [
        column
        for column in live_match_columns
        if column not in {"player_key", "team_key", "game_date"} and column in working.columns
    ]
    if existing_live_columns:
        working = working.drop(columns=existing_live_columns)

    merged = working.merge(live, on=["player_key", "team_key", "game_date"], how="left")
    fallback_live = (
        live.sort_values(["live_minutes_played"], ascending=[True])
        .drop_duplicates(subset=["player_key", "team_key"], keep="last")
        .rename(columns={column: f"{column}_fallback" for column in live_match_columns if column not in {"player_key", "team_key"}})
    )
    merged = merged.merge(fallback_live, on=["player_key", "team_key"], how="left")

    for column in [column for column in live_match_columns if column not in {"player_key", "team_key", "game_date"}]:
        fallback_column = f"{column}_fallback"
        if fallback_column in merged.columns:
            if column not in merged.columns:
                merged[column] = pd.NA
            merged[column] = merged[column].combine_first(merged[fallback_column])

    live_minutes = pd.to_numeric(merged.get("live_minutes_played"), errors="coerce").fillna(0.0)
    matched_mask = live_minutes.gt(0.0)
    if not matched_mask.any():
        status["note"] = "Live games are active, but no player rows matched the current predictions slate."
        return status

    blend_live_weight = _safe_float_value(config.get("in_game_projection_blend_live_weight"), default=0.6)
    blend_pregame_weight = _safe_float_value(config.get("in_game_projection_blend_pregame_weight"), default=0.4)
    weight_total = blend_live_weight + blend_pregame_weight
    if weight_total <= 0.0:
        blend_live_weight, blend_pregame_weight = 0.6, 0.4
    else:
        blend_live_weight = blend_live_weight / weight_total
        blend_pregame_weight = blend_pregame_weight / weight_total

    expected_minutes = pd.to_numeric(merged.get("predicted_minutes"), errors="coerce")
    expected_minutes = expected_minutes.where(expected_minutes.notna() & expected_minutes.gt(0.0))
    expected_minutes = expected_minutes.combine_first(pd.to_numeric(merged.get("expected_minutes"), errors="coerce"))

    component_map = {
        "live_points_current": "predicted_points",
        "live_rebounds_current": "predicted_rebounds",
        "live_assists_current": "predicted_assists",
        "live_steals_current": "predicted_steals",
        "live_blocks_current": "predicted_blocks",
        "live_turnovers_current": "predicted_turnovers",
        "live_three_points_current": "predicted_three_points_made",
    }
    for live_column, prediction_column in component_map.items():
        if prediction_column not in merged.columns or live_column not in merged.columns:
            continue
        adjusted_values = _blend_live_projection_component(
            merged[prediction_column],
            merged[live_column],
            live_minutes,
            expected_minutes,
            live_weight=blend_live_weight,
            pregame_weight=blend_pregame_weight,
        )
        merged.loc[matched_mask, prediction_column] = adjusted_values.loc[matched_mask].round(3)

    for required in ["predicted_points", "predicted_rebounds", "predicted_assists"]:
        if required not in merged.columns:
            merged[required] = 0.0
    merged["predicted_pra"] = (
        pd.to_numeric(merged.get("predicted_points"), errors="coerce").fillna(0.0)
        + pd.to_numeric(merged.get("predicted_rebounds"), errors="coerce").fillna(0.0)
        + pd.to_numeric(merged.get("predicted_assists"), errors="coerce").fillna(0.0)
    ).round(3)

    dk_input = pd.DataFrame(
        {
            "points": pd.to_numeric(merged.get("predicted_points"), errors="coerce").fillna(0.0),
            "rebounds": pd.to_numeric(merged.get("predicted_rebounds"), errors="coerce").fillna(0.0),
            "assists": pd.to_numeric(merged.get("predicted_assists"), errors="coerce").fillna(0.0),
            "steals": pd.to_numeric(merged.get("predicted_steals"), errors="coerce").fillna(0.0),
            "blocks": pd.to_numeric(merged.get("predicted_blocks"), errors="coerce").fillna(0.0),
            "turnovers": pd.to_numeric(merged.get("predicted_turnovers"), errors="coerce").fillna(0.0),
            "three_points_made": pd.to_numeric(merged.get("predicted_three_points_made"), errors="coerce").fillna(0.0),
        }
    )
    merged["predicted_draftkings_points"] = calculate_draftkings_points(dk_input).round(3)
    merged["predicted_fanduel_points"] = calculate_fanduel_points(dk_input).round(3)

    refresh_stamp = _now_iso()
    merged["live_projection_in_game_flag"] = matched_mask.astype(int)
    merged["live_projection_source"] = merged["live_projection_in_game_flag"].map(
        lambda value: "nba_live_boxscore" if int(value) == 1 else ""
    )
    merged["live_projection_updated_at"] = merged["live_projection_in_game_flag"].map(
        lambda value: refresh_stamp if int(value) == 1 else ""
    )

    output_columns = list(predictions.columns)
    additional_columns = [
        "live_projection_in_game_flag",
        "live_projection_source",
        "live_projection_updated_at",
        "live_game_id",
        "live_minutes_played",
        "live_points_current",
        "live_rebounds_current",
        "live_assists_current",
        "live_steals_current",
        "live_blocks_current",
        "live_turnovers_current",
        "live_three_points_current",
    ]
    for column in additional_columns:
        if column not in output_columns:
            output_columns.append(column)

    output_frame = merged[output_columns].copy()
    tmp_path = predictions_path.with_suffix(".tmp.csv")
    output_frame.to_csv(tmp_path, index=False)
    tmp_path.replace(predictions_path)

    status["rows_updated"] = int(matched_mask.sum())
    status["players_updated"] = int(merged.loc[matched_mask, "player_key"].nunique())
    if "live_game_id" in merged.columns:
        status["games_updated"] = int(merged.loc[matched_mask, "live_game_id"].astype(str).str.strip().nunique())
    else:
        status["games_updated"] = 0
    return status


def run_live_in_game_projection_refresh(
    config_path: Path = DEFAULT_LIVE_CONFIG_PATH,
    state_path: Path = DEFAULT_LIVE_STATE_PATH,
) -> dict:
    config = load_live_config(config_path)
    support_modules_config = normalize_support_module_config(config.get("support_modules"))
    state = load_live_state(state_path)
    refresh_interval_seconds = _clamp_interval(
        config.get("in_game_projection_refresh_interval_seconds", MIN_IN_GAME_REFRESH_INTERVAL_SECONDS),
        fallback=MIN_IN_GAME_REFRESH_INTERVAL_SECONDS,
        minimum=MIN_IN_GAME_REFRESH_INTERVAL_SECONDS,
    )
    state["in_game_projection_refresh_interval_seconds"] = refresh_interval_seconds

    if not module_enabled(support_modules_config, "live_ingest", default=True):
        state["in_game_projection_note"] = "In-game refresh skipped because live_ingest module is disabled."
        state["in_game_projection_rows_updated"] = 0
        state["in_game_projection_players_tracked"] = 0
        state["in_game_projection_games_tracked"] = 0
        state["in_game_projection_live_games_active"] = 0
        state["in_game_projection_last_error"] = None
        state["next_in_game_projection_refresh_due_at"] = _next_due_iso_from_seconds(
            state.get("last_in_game_projection_refresh_at"),
            refresh_interval_seconds,
        )
        save_live_state(state, state_path)
        return state

    if not module_enabled(support_modules_config, "model_trainer", default=True):
        state["in_game_projection_note"] = "In-game refresh skipped because model_trainer module is disabled."
        state["in_game_projection_rows_updated"] = 0
        state["in_game_projection_players_tracked"] = 0
        state["in_game_projection_games_tracked"] = 0
        state["in_game_projection_live_games_active"] = 0
        state["in_game_projection_last_error"] = None
        state["next_in_game_projection_refresh_due_at"] = _next_due_iso_from_seconds(
            state.get("last_in_game_projection_refresh_at"),
            refresh_interval_seconds,
        )
        save_live_state(state, state_path)
        return state

    if not bool(config.get("auto_refresh_in_game_projections", True)):
        state["in_game_projection_note"] = "In-game projection refresh is disabled."
        state["in_game_projection_rows_updated"] = 0
        state["in_game_projection_players_tracked"] = 0
        state["in_game_projection_games_tracked"] = 0
        state["in_game_projection_live_games_active"] = 0
        state["in_game_projection_last_error"] = None
        state["next_in_game_projection_refresh_due_at"] = _next_due_iso_from_seconds(
            state.get("last_in_game_projection_refresh_at"),
            refresh_interval_seconds,
        )
        save_live_state(state, state_path)
        return state

    try:
        scoreboard_payload = fetch_scoreboard()
        live_rows, live_status = _collect_live_boxscore_rows(scoreboard_payload)
        update_status = _apply_in_game_projection_adjustments(DEFAULT_PREDICTIONS_PATH, live_rows, config)

        state["last_in_game_projection_refresh_at"] = _now_iso()
        state["next_in_game_projection_refresh_due_at"] = _next_due_iso_from_seconds(
            state.get("last_in_game_projection_refresh_at"),
            refresh_interval_seconds,
        )
        state["in_game_projection_rows_updated"] = int(update_status.get("rows_updated", 0))
        state["in_game_projection_players_tracked"] = int(update_status.get("players_updated", 0))
        state["in_game_projection_games_tracked"] = int(update_status.get("games_updated", 0))
        state["in_game_projection_live_games_active"] = int(live_status.get("live_games_active", 0))
        state["in_game_projection_note"] = update_status.get("note")
        state["in_game_projection_last_error"] = update_status.get("last_error") or live_status.get("last_error")
    except Exception as exc:
        state["last_in_game_projection_refresh_at"] = _now_iso()
        state["next_in_game_projection_refresh_due_at"] = _next_due_iso_from_seconds(
            state.get("last_in_game_projection_refresh_at"),
            refresh_interval_seconds,
        )
        state["in_game_projection_rows_updated"] = 0
        state["in_game_projection_players_tracked"] = 0
        state["in_game_projection_games_tracked"] = 0
        state["in_game_projection_live_games_active"] = 0
        state["in_game_projection_note"] = "In-game refresh failed."
        state["in_game_projection_last_error"] = _sanitize_error_message(exc)

    save_live_state(state, state_path)
    return state


def _latest_player_context(training_path: Path) -> pd.DataFrame:
    history = load_dataset(training_path)
    if "team" not in history.columns:
        return pd.DataFrame()

    latest = history.sort_values(["player_name", "game_date"]).groupby("player_name", as_index=False).tail(1).copy()
    latest["last_game_date"] = latest["game_date"]
    return latest


def _opponent_difficulty_index(history: pd.DataFrame) -> tuple[dict[str, float], float, float]:
    if history.empty or "opponent" not in history.columns:
        return {}, 0.0, 1.0

    if "opp_drtg" in history.columns and pd.to_numeric(history["opp_drtg"], errors="coerce").notna().sum() >= 50:
        source = (
            history[["opponent", "opp_drtg"]]
            .assign(opp_drtg=lambda df: pd.to_numeric(df["opp_drtg"], errors="coerce"))
            .dropna(subset=["opponent", "opp_drtg"])
        )
        if source.empty:
            return {}, 0.0, 1.0
        per_opponent = source.groupby("opponent", as_index=False)["opp_drtg"].mean()
        league_mean = float(per_opponent["opp_drtg"].mean())
        league_std = float(per_opponent["opp_drtg"].std(ddof=0) or 1.0)
        index = {
            str(row["opponent"]): float((league_mean - float(row["opp_drtg"])) / league_std)
            for _, row in per_opponent.iterrows()
        }
        return index, league_mean, league_std

    if "points" in history.columns:
        source = (
            history[["opponent", "points"]]
            .assign(points=lambda df: pd.to_numeric(df["points"], errors="coerce"))
            .dropna(subset=["opponent", "points"])
        )
        if source.empty:
            return {}, 0.0, 1.0
        per_opponent = source.groupby("opponent", as_index=False)["points"].mean()
        league_mean = float(per_opponent["points"].mean())
        league_std = float(per_opponent["points"].std(ddof=0) or 1.0)
        index = {
            str(row["opponent"]): float((league_mean - float(row["points"])) / league_std)
            for _, row in per_opponent.iterrows()
        }
        return index, league_mean, league_std

    return {}, 0.0, 1.0


def _coach_mindset_profile(history: pd.DataFrame, opponent_index: dict[str, float]) -> dict[str, dict[str, float]]:
    required = {"team", "opponent", "minutes"}
    if history.empty or not required.issubset(set(history.columns)):
        return {}

    working = history.copy()
    working["minutes"] = pd.to_numeric(working["minutes"], errors="coerce")
    working["starter"] = pd.to_numeric(working.get("starter"), errors="coerce").fillna(0)
    working = working.dropna(subset=["minutes", "team", "opponent"])
    if working.empty:
        return {}

    grouped = working.groupby(["player_name", "team"], sort=False)
    working["minutes_avg_last_5"] = grouped["minutes"].transform(lambda series: series.shift(1).rolling(5, min_periods=1).mean())
    working["minutes_avg_last_10"] = grouped["minutes"].transform(lambda series: series.shift(1).rolling(10, min_periods=1).mean())
    working["minutes_baseline"] = working["minutes_avg_last_5"].combine_first(working["minutes_avg_last_10"])
    working["minutes_delta"] = working["minutes"] - working["minutes_baseline"]
    working["opponent_difficulty_z"] = working["opponent"].map(opponent_index).fillna(0.0)

    def _role_bucket(row: pd.Series) -> str:
        baseline = pd.to_numeric(pd.Series([row.get("minutes_baseline")]), errors="coerce").iloc[0]
        starter = pd.to_numeric(pd.Series([row.get("starter")]), errors="coerce").iloc[0]
        baseline = 0.0 if pd.isna(baseline) else float(baseline)
        starter = 0.0 if pd.isna(starter) else float(starter)
        if baseline >= 30 or (starter >= 0.5 and baseline >= 27):
            return "star"
        if baseline >= 24 or starter >= 0.5:
            return "core"
        if baseline >= 16:
            return "rotation"
        return "bench"

    working["role_bucket"] = working.apply(_role_bucket, axis=1)
    working = working.dropna(subset=["minutes_delta"])
    if working.empty:
        return {}

    profiles: dict[str, dict[str, float]] = {}
    for team, team_frame in working.groupby("team"):
        tough = team_frame["opponent_difficulty_z"] >= 0.5
        easy = team_frame["opponent_difficulty_z"] <= -0.5
        star_or_core = team_frame["role_bucket"].isin(["star", "core"])
        bench_or_rotation = team_frame["role_bucket"].isin(["bench", "rotation"])

        star_tough = team_frame.loc[tough & star_or_core, "minutes_delta"].mean()
        star_easy = team_frame.loc[easy & star_or_core, "minutes_delta"].mean()
        bench_tough = team_frame.loc[tough & bench_or_rotation, "minutes_delta"].mean()
        bench_easy = team_frame.loc[easy & bench_or_rotation, "minutes_delta"].mean()

        star_tough = 0.0 if pd.isna(star_tough) else float(star_tough)
        star_easy = 0.0 if pd.isna(star_easy) else float(star_easy)
        bench_tough = 0.0 if pd.isna(bench_tough) else float(bench_tough)
        bench_easy = 0.0 if pd.isna(bench_easy) else float(bench_easy)

        tightness = (star_tough - star_easy) - (bench_tough - bench_easy)
        tightness = max(-2.5, min(2.5, tightness))
        profiles[str(team)] = {
            "tightness": float(tightness),
            "sample_rows": float(len(team_frame)),
        }
    return profiles


def _injury_minutes_multiplier_from_status_text(text: str) -> float:
    normalized = str(text or "").lower()
    if not normalized.strip():
        return 1.0
    if MINUTES_UNAVAILABLE_PATTERN.search(normalized):
        return 0.0
    if MINUTES_DOUBTFUL_PATTERN.search(normalized):
        return 0.45
    if MINUTES_QUESTIONABLE_PATTERN.search(normalized):
        return 0.82
    if MINUTES_PROBABLE_PATTERN.search(normalized):
        return 0.96
    return 1.0


def _injury_risk_score_from_status_text(text: str) -> float:
    normalized = str(text or "").lower()
    if not normalized.strip():
        return 0.05
    if MINUTES_UNAVAILABLE_PATTERN.search(normalized):
        return 1.0
    if MINUTES_DOUBTFUL_PATTERN.search(normalized):
        return 0.72
    if MINUTES_QUESTIONABLE_PATTERN.search(normalized):
        return 0.48
    if MINUTES_PROBABLE_PATTERN.search(normalized):
        return 0.22
    if re.search(r"\b(rest|maintenance|monitor)\b", normalized):
        return 0.28
    return 0.12


def _starter_probability_from_signals(
    starter_numeric: float | None,
    starter_rate_last_10: float | None,
    baseline_minutes: float,
    injury_multiplier: float,
) -> float:
    if starter_numeric is not None and not pd.isna(starter_numeric):
        starter_prob = 0.9 if float(starter_numeric) >= 0.5 else 0.1
    elif starter_rate_last_10 is not None and not pd.isna(starter_rate_last_10):
        starter_prob = float(max(0.05, min(0.95, starter_rate_last_10)))
    elif baseline_minutes >= 27:
        starter_prob = 0.78
    elif baseline_minutes >= 22:
        starter_prob = 0.58
    elif baseline_minutes >= 16:
        starter_prob = 0.34
    else:
        starter_prob = 0.14

    if injury_multiplier <= 0:
        return 0.0
    if injury_multiplier < 0.5:
        starter_prob *= 0.35
    elif injury_multiplier < 0.9:
        starter_prob *= 0.78
    return float(max(0.0, min(1.0, starter_prob)))


def _refresh_pregame_lock_window_fields(
    frame: pd.DataFrame,
    now_utc: datetime | None = None,
) -> pd.DataFrame:
    if frame.empty:
        return frame

    working = frame.copy()
    if now_utc is None:
        now_utc = datetime.now(timezone.utc)
    now_timestamp = pd.Timestamp(now_utc)
    if now_timestamp.tzinfo is None:
        now_timestamp = now_timestamp.tz_localize("UTC")
    else:
        now_timestamp = now_timestamp.tz_convert("UTC")

    tipoff = pd.to_datetime(working.get("commence_time_utc"), errors="coerce", utc=True)
    if not isinstance(tipoff, pd.Series):
        tipoff = pd.Series(pd.NaT, index=working.index, dtype="datetime64[ns, UTC]")
    minutes_to_tipoff = (tipoff - now_timestamp).dt.total_seconds() / 60.0

    stage = pd.Series("unknown", index=working.index, dtype="object")
    has_tipoff = tipoff.notna()
    stage.loc[has_tipoff & minutes_to_tipoff.gt(90.0)] = "pre_t_minus_90"
    stage.loc[has_tipoff & minutes_to_tipoff.le(90.0) & minutes_to_tipoff.gt(30.0)] = "t_minus_90"
    stage.loc[has_tipoff & minutes_to_tipoff.le(30.0) & minutes_to_tipoff.gt(5.0)] = "t_minus_30"
    stage.loc[has_tipoff & minutes_to_tipoff.le(5.0) & minutes_to_tipoff.ge(-30.0)] = "t_minus_5"
    stage.loc[has_tipoff & minutes_to_tipoff.lt(-30.0)] = "post_tipoff"

    weight = pd.Series(0.0, index=working.index, dtype=float)
    weight.loc[stage.eq("t_minus_90")] = 0.09
    weight.loc[stage.eq("t_minus_30")] = 0.15
    weight.loc[stage.eq("t_minus_5")] = 0.22

    working["pregame_lock_window_stage"] = stage
    working["pregame_lock_minutes_to_tipoff"] = minutes_to_tipoff.round(2)
    working["pregame_lock_window_weight"] = weight.round(3)
    return working


def _estimate_expected_minutes(upcoming_frame: pd.DataFrame, training_path: Path) -> tuple[pd.DataFrame, dict]:
    if upcoming_frame.empty:
        return upcoming_frame, {"rows_estimated": 0, "rows_unavailable": 0}

    history = load_dataset(training_path)
    required = {"player_name", "team", "game_date", "minutes", "opponent"}
    if history.empty or not required.issubset(set(history.columns)):
        return upcoming_frame, {"rows_estimated": 0, "rows_unavailable": 0, "note": "insufficient_history_columns"}

    history = history.copy()
    history["team"] = history["team"].map(_normalize_team_code)
    history["opponent"] = history["opponent"].map(_normalize_team_code)
    nba_team_codes = set(TEAM_ID_BY_TRICODE.keys())
    history = history[history["team"].isin(nba_team_codes) & history["opponent"].isin(nba_team_codes)].copy()
    history["minutes"] = pd.to_numeric(history["minutes"], errors="coerce")
    history["starter"] = pd.to_numeric(history.get("starter"), errors="coerce").fillna(0)
    history = history.dropna(subset=["player_name", "team", "game_date", "minutes"]).sort_values(
        ["player_name", "team", "game_date"]
    )
    if history.empty:
        return upcoming_frame, {"rows_estimated": 0, "rows_unavailable": 0, "note": "empty_history_after_clean"}

    player_group = history.groupby(["player_name", "team"], sort=False)
    history["minutes_avg_last_3"] = player_group["minutes"].transform(lambda series: series.shift(1).rolling(3, min_periods=1).mean())
    history["minutes_avg_last_5"] = player_group["minutes"].transform(lambda series: series.shift(1).rolling(5, min_periods=1).mean())
    history["minutes_avg_last_10"] = player_group["minutes"].transform(lambda series: series.shift(1).rolling(10, min_periods=1).mean())
    history["minutes_std_last_10"] = player_group["minutes"].transform(lambda series: series.shift(1).rolling(10, min_periods=3).std())
    history["minutes_season_avg"] = player_group["minutes"].transform(lambda series: series.expanding().mean().shift(1))
    history["minutes_games_played"] = player_group.cumcount()
    history["starter_rate_last_10"] = player_group["starter"].transform(lambda series: series.shift(1).rolling(10, min_periods=1).mean())

    latest = (
        history.groupby(["player_name", "team"], as_index=False)
        .tail(1)[
            [
                "player_name",
                "team",
                "minutes_avg_last_3",
                "minutes_avg_last_5",
                "minutes_avg_last_10",
                "minutes_std_last_10",
                "minutes_season_avg",
                "minutes_games_played",
                "starter_rate_last_10",
            ]
        ]
        .copy()
    )
    latest["player_key"] = latest["player_name"].map(_normalize_player_key)
    latest["team_key"] = latest["team"].map(_normalize_team_code)

    team_median_minutes = history.groupby("team")["minutes"].median().to_dict()
    team_median_starter_minutes = history.loc[history["starter"] >= 0.5].groupby("team")["minutes"].median().to_dict()
    team_median_bench_minutes = history.loc[history["starter"] < 0.5].groupby("team")["minutes"].median().to_dict()

    opponent_index, _, _ = _opponent_difficulty_index(history)
    coach_profiles = _coach_mindset_profile(history, opponent_index)

    projected = upcoming_frame.copy()
    projected["player_key"] = projected["player_name"].map(_normalize_player_key)
    projected["team_key"] = projected["team"].map(_normalize_team_code)
    projected = projected.merge(
        latest.drop(columns=["player_name", "team"]),
        on=["player_key", "team_key"],
        how="left",
        suffixes=("", "__hist"),
    )

    projected["expected_minutes_existing"] = pd.to_numeric(projected.get("expected_minutes"), errors="coerce")
    projected["rest_days"] = pd.to_numeric(projected.get("rest_days"), errors="coerce")
    projected["starter_numeric"] = pd.to_numeric(projected.get("starter"), errors="coerce")
    projected["spread_numeric"] = pd.to_numeric(projected.get("spread"), errors="coerce")
    projected["commence_time_utc"] = pd.to_datetime(projected.get("commence_time_utc"), errors="coerce", utc=True)
    now_utc = datetime.now(timezone.utc)

    estimated_rows = 0
    unavailable_rows = 0

    def _baseline_minutes(row: pd.Series) -> float:
        values = [
            pd.to_numeric(pd.Series([row.get("minutes_avg_last_5")]), errors="coerce").iloc[0],
            pd.to_numeric(pd.Series([row.get("minutes_avg_last_10")]), errors="coerce").iloc[0],
            pd.to_numeric(pd.Series([row.get("minutes_season_avg")]), errors="coerce").iloc[0],
        ]
        weights = [0.5, 0.3, 0.2]
        weighted_sum = 0.0
        weight_sum = 0.0
        for value, weight in zip(values, weights):
            if pd.notna(value):
                weighted_sum += float(value) * weight
                weight_sum += weight
        if weight_sum > 0:
            return weighted_sum / weight_sum

        team_code = str(row.get("team") or "")
        starter_flag = pd.to_numeric(pd.Series([row.get("starter_numeric")]), errors="coerce").iloc[0]
        if pd.notna(starter_flag) and float(starter_flag) >= 0.5 and team_code in team_median_starter_minutes:
            return float(team_median_starter_minutes[team_code])
        if pd.notna(starter_flag) and float(starter_flag) < 0.5 and team_code in team_median_bench_minutes:
            return float(team_median_bench_minutes[team_code])
        return float(team_median_minutes.get(team_code, 20.0))

    def _row_role(baseline: float, starter_probability: float) -> str:
        starter_prob = 0.0 if pd.isna(starter_probability) else float(starter_probability)
        if baseline >= 30.0 or (starter_prob >= 0.72 and baseline >= 27.0):
            return "star"
        if baseline >= 24.0 or starter_prob >= 0.55:
            return "core"
        if baseline >= 16.0:
            return "rotation"
        return "bench"

    def _status_multiplier(row: pd.Series) -> float:
        explicit = pd.to_numeric(pd.Series([row.get("injury_minutes_multiplier")]), errors="coerce").iloc[0]
        if pd.notna(explicit):
            return float(max(0.0, min(1.0, explicit)))
        text = " ".join(
            str(row.get(column) or "")
            for column in ["injury_status", "health_status", "suspension_status"]
        ).lower()
        return _injury_minutes_multiplier_from_status_text(text)

    estimated_minutes: list[float] = []
    minutes_baseline_values: list[float] = []
    difficulty_values: list[float] = []
    coach_tightness_values: list[float] = []
    starter_probability_values: list[float] = []
    starter_certainty_values: list[float] = []
    injury_risk_values: list[float] = []
    injury_multiplier_values: list[float] = []
    minutes_confidence_values: list[float] = []
    minutes_error_estimate_values: list[float] = []
    pregame_lock_confidence_values: list[float] = []
    pregame_lock_tier_values: list[str] = []
    pregame_lock_window_stage_values: list[str] = []
    pregame_lock_window_minutes_values: list[float] = []
    pregame_lock_window_weight_values: list[float] = []
    pregame_line_freshness_values: list[float] = []
    pregame_min_line_age_values: list[float] = []
    home_hometown_adjust_values: list[float] = []
    teammate_adjust_values: list[float] = []
    news_adjust_values: list[float] = []
    model_source: list[str] = []

    for _, row in projected.iterrows():
        existing = pd.to_numeric(pd.Series([row.get("expected_minutes_existing")]), errors="coerce").iloc[0]
        baseline = _baseline_minutes(row)
        starter_numeric = pd.to_numeric(pd.Series([row.get("starter_numeric")]), errors="coerce").iloc[0]
        starter_rate = pd.to_numeric(pd.Series([row.get("starter_rate_last_10")]), errors="coerce").iloc[0]
        status_text = " ".join(
            str(row.get(column) or "")
            for column in ["injury_status", "health_status", "suspension_status"]
        ).lower()
        injury_multiplier_raw = pd.to_numeric(pd.Series([row.get("injury_minutes_multiplier")]), errors="coerce").iloc[0]
        injury_risk_raw = pd.to_numeric(pd.Series([row.get("injury_risk_score")]), errors="coerce").iloc[0]
        injury_multiplier = (
            float(max(0.0, min(1.0, injury_multiplier_raw)))
            if pd.notna(injury_multiplier_raw)
            else _injury_minutes_multiplier_from_status_text(status_text)
        )
        injury_risk_score = (
            float(max(0.0, min(1.0, injury_risk_raw)))
            if pd.notna(injury_risk_raw)
            else _injury_risk_score_from_status_text(status_text)
        )
        tipoff_raw = row.get("commence_time_utc")
        tipoff_timestamp = pd.to_datetime(pd.Series([tipoff_raw]), errors="coerce", utc=True).iloc[0]
        minutes_to_tipoff = float("nan")
        lock_window_stage = "unknown"
        lock_window_confidence_boost = 0.0
        lock_window_pull_boost = 0.0
        lock_window_starter_hint_boost = 0.0
        if pd.notna(tipoff_timestamp):
            minutes_to_tipoff = float((tipoff_timestamp.to_pydatetime() - now_utc).total_seconds() / 60.0)
            if minutes_to_tipoff <= 5.0 and minutes_to_tipoff >= -30.0:
                lock_window_stage = "t_minus_5"
                lock_window_confidence_boost = 0.22
                lock_window_pull_boost = 0.22
                lock_window_starter_hint_boost = 0.16
            elif minutes_to_tipoff <= 30.0 and minutes_to_tipoff >= -30.0:
                lock_window_stage = "t_minus_30"
                lock_window_confidence_boost = 0.15
                lock_window_pull_boost = 0.15
                lock_window_starter_hint_boost = 0.11
            elif minutes_to_tipoff <= 90.0 and minutes_to_tipoff >= -30.0:
                lock_window_stage = "t_minus_90"
                lock_window_confidence_boost = 0.09
                lock_window_pull_boost = 0.08
                lock_window_starter_hint_boost = 0.07
            elif minutes_to_tipoff > 90.0:
                lock_window_stage = "pre_t_minus_90"
            else:
                lock_window_stage = "post_tipoff"
        starter_prob = _starter_probability_from_signals(starter_numeric, starter_rate, baseline, injury_multiplier)
        starter_certainty_hint = pd.to_numeric(pd.Series([row.get("starter_certainty")]), errors="coerce").iloc[0]
        lineup_confidence_hint = pd.to_numeric(pd.Series([row.get("lineup_status_confidence")]), errors="coerce").iloc[0]
        if pd.notna(starter_certainty_hint):
            blend_weight = (0.62 if pd.notna(starter_numeric) else 0.48) + lock_window_starter_hint_boost
            blend_weight = float(max(0.18, min(0.9, blend_weight)))
            starter_prob = float(
                max(
                    0.0,
                    min(
                        1.0,
                        starter_prob * (1.0 - blend_weight) + float(starter_certainty_hint) * blend_weight,
                    ),
                )
            )
        if pd.notna(lineup_confidence_hint) and pd.notna(starter_numeric):
            confidence_direction = 1.0 if float(starter_numeric) >= 0.5 else -1.0
            lineup_hint_gain = 0.12 + 0.5 * lock_window_starter_hint_boost
            starter_prob = float(
                max(
                    0.0,
                    min(1.0, starter_prob + confidence_direction * (float(lineup_confidence_hint) - 0.5) * lineup_hint_gain),
                )
            )

        line_age_candidates = [
            pd.to_numeric(pd.Series([row.get("line_points_snapshot_age_minutes")]), errors="coerce").iloc[0],
            pd.to_numeric(pd.Series([row.get("line_rebounds_snapshot_age_minutes")]), errors="coerce").iloc[0],
            pd.to_numeric(pd.Series([row.get("line_assists_snapshot_age_minutes")]), errors="coerce").iloc[0],
            pd.to_numeric(pd.Series([row.get("line_pra_snapshot_age_minutes")]), errors="coerce").iloc[0],
        ]
        observed_line_ages = [float(value) for value in line_age_candidates if pd.notna(value)]
        min_line_age_minutes = float(min(observed_line_ages)) if observed_line_ages else 720.0
        pregame_line_freshness = float(max(0.0, min(1.0, 1.0 - (min_line_age_minutes / 720.0))))

        lineup_conf_component = (
            float(max(0.0, min(1.0, lineup_confidence_hint)))
            if pd.notna(lineup_confidence_hint)
            else 0.0
        )
        starter_cert_component = (
            float(max(0.0, min(1.0, starter_certainty_hint)))
            if pd.notna(starter_certainty_hint)
            else float(max(0.0, min(1.0, starter_prob)))
        )
        news_conf_component = float(max(0.0, min(1.0, _safe_float_value(row.get("news_confidence_score"), 0.0))))
        news_risk_component = float(max(0.0, min(1.0, _safe_float_value(row.get("news_risk_score"), 0.0))))
        pregame_lock_confidence = (
            0.12
            + (lineup_conf_component * 0.42)
            + (starter_cert_component * 0.2)
            + (pregame_line_freshness * 0.18)
            + (news_conf_component * (1.0 - news_risk_component) * 0.16)
            + (lock_window_confidence_boost * (0.55 + 0.45 * lineup_conf_component))
        )
        pregame_lock_confidence = float(max(0.0, min(1.0, pregame_lock_confidence)))
        if pd.notna(starter_numeric):
            target_probability = 1.0 if float(starter_numeric) >= 0.5 else 0.0
            lock_pull = 0.08 + (0.24 * pregame_lock_confidence) + lock_window_pull_boost
            lock_pull = float(max(0.08, min(0.75, lock_pull)))
            starter_prob = float(
                max(
                    0.0,
                    min(
                        1.0,
                        starter_prob * (1.0 - lock_pull) + target_probability * lock_pull,
                    ),
                )
            )
        if pregame_lock_confidence >= 0.8:
            pregame_lock_tier = "high"
        elif pregame_lock_confidence >= 0.55:
            pregame_lock_tier = "medium"
        else:
            pregame_lock_tier = "low"
        role = _row_role(baseline, starter_prob)

        trend = pd.to_numeric(pd.Series([row.get("minutes_avg_last_3")]), errors="coerce").iloc[0]
        trend_ref = pd.to_numeric(pd.Series([row.get("minutes_avg_last_10")]), errors="coerce").iloc[0]
        trend_delta = 0.0
        if pd.notna(trend) and pd.notna(trend_ref):
            trend_delta = max(-5.0, min(5.0, float(trend - trend_ref))) * 0.35

        rest_days = pd.to_numeric(pd.Series([row.get("rest_days")]), errors="coerce").iloc[0]
        rest_adjust = 0.0
        if pd.notna(rest_days):
            if rest_days <= 0:
                rest_adjust = -1.6
            elif rest_days == 1:
                rest_adjust = -0.3
            elif rest_days == 2:
                rest_adjust = 0.35
            elif rest_days >= 3:
                rest_adjust = 0.75

        implied_total = pd.to_numeric(pd.Series([row.get("implied_team_total")]), errors="coerce").iloc[0]
        game_total = pd.to_numeric(pd.Series([row.get("game_total")]), errors="coerce").iloc[0]
        environment_adjust = 0.0
        if pd.notna(implied_total):
            environment_adjust += max(-1.2, min(1.6, (float(implied_total) - 112.0) * 0.05))
        if pd.notna(game_total):
            environment_adjust += max(-0.8, min(0.8, (float(game_total) - 228.0) * 0.015))

        spread = pd.to_numeric(pd.Series([row.get("spread_numeric")]), errors="coerce").iloc[0]
        spread_adjust = 0.0
        if pd.notna(spread):
            abs_spread = abs(float(spread))
            if abs_spread <= 4:
                spread_adjust = 1.0 if role in {"star", "core"} else 0.3
            elif abs_spread >= 10:
                spread_adjust = -1.0 if role in {"star", "core"} else -0.2

        home_court_minutes_boost = _safe_float_value(row.get("home_court_minutes_boost"), default=0.0)
        hometown_advantage_score = _safe_float_value(row.get("hometown_advantage_score"), default=0.0)
        home_hometown_adjust = max(-3.0, min(3.0, home_court_minutes_boost + 0.65 * hometown_advantage_score))

        opponent = str(row.get("opponent") or "")
        difficulty = float(opponent_index.get(opponent, 0.0))
        role_difficulty_weight = {
            "star": 1.25,
            "core": 0.8,
            "rotation": 0.15,
            "bench": -0.55,
        }[role]
        opponent_adjust = max(-2.5, min(2.5, difficulty * role_difficulty_weight))

        team = str(row.get("team") or "")
        coach_tightness = float(coach_profiles.get(team, {}).get("tightness", 0.0))
        coach_role_weight = {
            "star": 1.0,
            "core": 0.7,
            "rotation": -0.2,
            "bench": -0.7,
        }[role]
        coach_adjust = max(-2.0, min(2.0, coach_tightness * difficulty * coach_role_weight))

        starter_adjust = float((starter_prob - 0.5) * 2.2)

        teammate_vacancy = _safe_float_value(row.get("teammate_usage_vacancy"), default=0.0)
        teammate_continuity = _safe_float_value(row.get("teammate_continuity_score"), default=0.6)
        teammate_star_out = _safe_float_value(row.get("teammate_star_out_flag"), default=0.0)
        teammate_synergy_points = _safe_float_value(row.get("teammate_synergy_points"), default=0.0)
        teammate_synergy_rebounds = _safe_float_value(row.get("teammate_synergy_rebounds"), default=0.0)
        teammate_synergy_assists = _safe_float_value(row.get("teammate_synergy_assists"), default=0.0)
        teammate_on_off_points = _safe_float_value(row.get("teammate_on_off_points_delta"), default=0.0)
        teammate_on_off_rebounds = _safe_float_value(row.get("teammate_on_off_rebounds_delta"), default=0.0)
        teammate_on_off_assists = _safe_float_value(row.get("teammate_on_off_assists_delta"), default=0.0)
        teammate_synergy_blend = teammate_synergy_points + 0.45 * teammate_synergy_rebounds + 0.55 * teammate_synergy_assists
        teammate_on_off_blend = teammate_on_off_points + 0.35 * teammate_on_off_rebounds + 0.5 * teammate_on_off_assists
        teammate_adjust = (
            2.1 * max(0.0, min(1.8, teammate_vacancy))
            - 1.0 * max(0.0, min(1.0, 1.0 - teammate_continuity))
            + 0.7 * max(0.0, min(1.0, teammate_star_out))
            + 0.3 * max(-6.0, min(6.0, teammate_synergy_blend))
            + 0.15 * max(-8.0, min(8.0, teammate_on_off_blend))
        )
        teammate_adjust = max(-2.8, min(3.2, teammate_adjust))

        news_risk_score = _safe_float_value(row.get("news_risk_score"), default=0.0)
        news_confidence_score = _safe_float_value(row.get("news_confidence_score"), default=0.0)
        news_article_count = _safe_float_value(row.get("news_article_count_24h"), default=0.0)
        news_positive_mentions = _safe_float_value(row.get("news_positive_mentions_24h"), default=0.0)
        news_negative_mentions = _safe_float_value(row.get("news_negative_mentions_24h"), default=0.0)
        news_starting_mentions = _safe_float_value(row.get("news_starting_mentions_24h"), default=0.0)
        news_balance = (news_positive_mentions - news_negative_mentions) / max(1.0, news_article_count)
        news_adjust = (
            -2.2 * news_risk_score * max(0.35, min(1.0, news_confidence_score))
            + 0.5 * max(-1.0, min(1.0, news_balance))
            + 0.2 * max(0.0, min(3.0, news_starting_mentions))
        )
        news_adjust = max(-2.5, min(1.8, news_adjust))

        projected_minutes = (
            baseline
            + trend_delta
            + rest_adjust
            + environment_adjust
            + spread_adjust
            + home_hometown_adjust
            + opponent_adjust
            + coach_adjust
            + starter_adjust
            + teammate_adjust
            + news_adjust
        )
        projected_minutes = max(0.0, min(44.0, projected_minutes))

        if lock_window_stage in {"t_minus_30", "t_minus_5"}:
            if role in {"rotation", "bench"} and starter_prob < 0.45:
                reduction = 0.9 if lock_window_stage == "t_minus_30" else 0.82
                projected_minutes = projected_minutes * reduction
            if role in {"star", "core"} and starter_prob >= 0.65 and baseline >= 20.0:
                floor_scale = 0.78 if lock_window_stage == "t_minus_30" else 0.84
                projected_minutes = max(projected_minutes, baseline * floor_scale)

        season_avg = pd.to_numeric(pd.Series([row.get("minutes_season_avg")]), errors="coerce").iloc[0]
        if pd.notna(season_avg):
            lower = max(0.0, float(season_avg) * 0.35 - 1.0)
            upper = min(44.0, float(season_avg) * 1.45 + 5.0)
            if upper > lower:
                projected_minutes = max(lower, min(upper, projected_minutes))

        multiplier = _status_multiplier(row)
        if multiplier == 0.0:
            unavailable_rows += 1
        projected_minutes = projected_minutes * multiplier
        if starter_prob < 0.2:
            projected_minutes = min(projected_minutes, 24.0)
        if pregame_lock_confidence >= 0.75 and starter_prob >= 0.6 and baseline >= 20.0:
            projected_minutes = max(projected_minutes, baseline * 0.72)

        games_played = pd.to_numeric(pd.Series([row.get("minutes_games_played")]), errors="coerce").fillna(0).iloc[0]
        minutes_volatility = pd.to_numeric(pd.Series([row.get("minutes_std_last_10")]), errors="coerce").fillna(7.0).iloc[0]
        starter_signal = max(0.0, min(1.0, abs(starter_prob - 0.5) * 2.0))
        starter_certainty = (
            float(max(0.0, min(1.0, starter_prob)))
            if pd.isna(starter_certainty_hint)
            else float(max(0.0, min(1.0, float(starter_certainty_hint))))
        )
        confidence = (
            0.22
            + (min(float(games_played), 24.0) / 30.0)
            + 0.22 * starter_signal
            + 0.18 * (1.0 - min(float(minutes_volatility), 12.0) / 12.0)
            + 0.18 * (1.0 - float(injury_risk_score))
            + 0.08 * starter_certainty
            + 0.14 * pregame_lock_confidence
        )
        confidence = float(max(0.05, min(0.98, confidence)))
        minutes_error_estimate = float(max(1.0, min(12.0, (1.0 - confidence) * (7.0 + 0.45 * float(minutes_volatility)))))
        minutes_error_estimate = float(
            max(
                0.8,
                min(12.0, minutes_error_estimate * (1.0 - (0.35 * pregame_lock_confidence))),
            )
        )

        final_minutes = float(round(projected_minutes, 2))
        if pd.isna(existing):
            estimated_rows += 1
            model_source.append("history_model")
            estimated_minutes.append(final_minutes)
        else:
            model_source.append("manual_or_provider")
            estimated_minutes.append(float(existing))

        minutes_baseline_values.append(round(float(baseline), 2))
        difficulty_values.append(round(float(difficulty), 3))
        coach_tightness_values.append(round(float(coach_tightness), 3))
        starter_probability_values.append(round(float(starter_prob), 3))
        starter_certainty_values.append(round(float(starter_certainty), 3))
        injury_risk_values.append(round(float(injury_risk_score), 3))
        injury_multiplier_values.append(round(float(injury_multiplier), 3))
        minutes_confidence_values.append(round(float(confidence), 3))
        minutes_error_estimate_values.append(round(float(minutes_error_estimate), 3))
        pregame_lock_confidence_values.append(round(float(pregame_lock_confidence), 3))
        pregame_lock_tier_values.append(str(pregame_lock_tier))
        pregame_lock_window_stage_values.append(str(lock_window_stage))
        pregame_lock_window_minutes_values.append(round(float(minutes_to_tipoff), 2) if np.isfinite(minutes_to_tipoff) else float("nan"))
        pregame_lock_window_weight_values.append(round(float(lock_window_confidence_boost), 3))
        pregame_line_freshness_values.append(round(float(pregame_line_freshness), 3))
        pregame_min_line_age_values.append(round(float(min_line_age_minutes), 2))
        home_hometown_adjust_values.append(round(float(home_hometown_adjust), 3))
        teammate_adjust_values.append(round(float(teammate_adjust), 3))
        news_adjust_values.append(round(float(news_adjust), 3))

    projected["expected_minutes"] = estimated_minutes
    projected["minutes_model_baseline"] = minutes_baseline_values
    projected["minutes_model_opponent_difficulty_z"] = difficulty_values
    projected["minutes_model_coach_tightness"] = coach_tightness_values
    projected["starter_probability"] = starter_probability_values
    projected["starter_certainty"] = starter_certainty_values
    projected["injury_risk_score"] = injury_risk_values
    projected["injury_minutes_multiplier"] = injury_multiplier_values
    projected["expected_minutes_confidence"] = minutes_confidence_values
    projected["minutes_projection_error_estimate"] = minutes_error_estimate_values
    projected["pregame_lock_confidence"] = pregame_lock_confidence_values
    projected["pregame_lock_tier"] = pregame_lock_tier_values
    projected["pregame_lock_window_stage"] = pregame_lock_window_stage_values
    projected["pregame_lock_minutes_to_tipoff"] = pregame_lock_window_minutes_values
    projected["pregame_lock_window_weight"] = pregame_lock_window_weight_values
    projected["pregame_line_freshness_score"] = pregame_line_freshness_values
    projected["pregame_min_line_age_minutes"] = pregame_min_line_age_values
    projected["minutes_model_home_hometown_adjust"] = home_hometown_adjust_values
    projected["minutes_model_teammate_adjust"] = teammate_adjust_values
    projected["minutes_model_news_adjust"] = news_adjust_values
    if "starter" in projected.columns:
        starter_existing = pd.to_numeric(projected["starter"], errors="coerce")
        projected["starter"] = starter_existing.combine_first(
            (pd.to_numeric(projected["starter_probability"], errors="coerce").fillna(0.0) >= 0.6).astype(int)
        )
    else:
        projected["starter"] = (pd.to_numeric(projected["starter_probability"], errors="coerce").fillna(0.0) >= 0.6).astype(int)
    projected["expected_minutes_source"] = model_source

    projected = projected.drop(columns=["player_key", "team_key", "expected_minutes_existing", "starter_numeric", "spread_numeric"], errors="ignore")
    summary = {
        "rows_estimated": int(estimated_rows),
        "rows_unavailable": int(unavailable_rows),
        "rows_with_history": int(pd.to_numeric(projected.get("minutes_games_played"), errors="coerce").fillna(0).gt(0).sum()),
        "rows_in_lock_windows": int(
            pd.Series(pregame_lock_window_stage_values, dtype="object")
            .isin(["t_minus_90", "t_minus_30", "t_minus_5"])
            .sum()
        ),
    }
    return projected, summary


def _scheduled_games_from_scoreboard(scoreboard_payload: dict, include_live_games: bool) -> list[dict]:
    scoreboard = scoreboard_payload["scoreboard"]
    scheduled_games: list[dict] = []
    for game in scoreboard.get("games", []):
        game_status = int(game.get("gameStatus", 0))
        if game_status == 3:
            continue
        if game_status == 2 and not include_live_games:
            continue

        game_date = (game.get("gameEt") or scoreboard.get("gameDate") or "")[:10]
        home_team = _normalize_team_code(game["homeTeam"].get("teamTricode"))
        away_team = _normalize_team_code(game["awayTeam"].get("teamTricode"))
        if not game_date or not home_team or not away_team:
            continue

        scheduled_games.append(
            {
                "game_id": game.get("gameId"),
                "game_date": game_date,
                "home_team": home_team,
                "away_team": away_team,
                "source": "scoreboard",
                "game_status": game_status,
            }
        )
    return scheduled_games


def _scheduled_games_from_schedule(schedule_payload: dict, lookahead_hours: int, include_live_games: bool) -> list[dict]:
    scheduled_games: list[dict] = []
    now_utc = datetime.now(timezone.utc)
    cutoff_utc = now_utc + timedelta(hours=max(1, int(lookahead_hours)))

    for game_date_entry in schedule_payload.get("leagueSchedule", {}).get("gameDates", []):
        for game in game_date_entry.get("games", []):
            if str(game.get("gameLabel", "")).strip().lower() == "preseason":
                continue

            game_status = int(game.get("gameStatus", 0) or 0)
            if game_status == 3:
                continue
            if game_status == 2 and not include_live_games:
                continue

            # Use actual tipoff timestamp first; gameDateUTC is date-midnight and can incorrectly drop same-day games.
            tipoff_utc = _parse_iso_datetime(
                game.get("gameDateTimeUTC")
                or game.get("gameDateUTC")
                or game.get("gameDateTimeEst")
                or game.get("gameDateEst")
            )
            if tipoff_utc is None:
                continue
            if tipoff_utc < now_utc:
                continue
            if tipoff_utc > cutoff_utc:
                continue

            home_team = _normalize_team_code((game.get("homeTeam") or {}).get("teamTricode"))
            away_team = _normalize_team_code((game.get("awayTeam") or {}).get("teamTricode"))
            game_date = _game_date_from_tipoff(tipoff_utc)
            if not game_date or not home_team or not away_team:
                continue

            scheduled_games.append(
                {
                    "game_id": game.get("gameId"),
                    "game_date": game_date,
                    "home_team": home_team,
                    "away_team": away_team,
                    "source": "schedule",
                    "commence_time_utc": tipoff_utc.isoformat(),
                }
            )

    return scheduled_games


def _existing_game_ids(training_path: Path) -> set[str]:
    if not training_path.exists():
        return set()
    try:
        game_id_frame = pd.read_csv(training_path, usecols=["game_id"])
    except (ValueError, OSError, pd.errors.EmptyDataError):
        return set()

    return {
        str(value).strip()
        for value in game_id_frame["game_id"].dropna().astype(str).tolist()
        if str(value).strip()
    }


def _completed_games_from_schedule(schedule_payload: dict, lookback_days: int) -> list[dict]:
    now_utc = datetime.now(timezone.utc)
    lookback_days = max(1, int(lookback_days))
    window_start_utc = now_utc - timedelta(days=lookback_days)
    games: list[dict] = []

    for game_date_entry in schedule_payload.get("leagueSchedule", {}).get("gameDates", []):
        for game in game_date_entry.get("games", []):
            if str(game.get("gameLabel", "")).strip().lower() == "preseason":
                continue
            if int(game.get("gameStatus", 0) or 0) != 3:
                continue

            # Use actual tipoff timestamp first; gameDateUTC is date-midnight and can skew window checks.
            tipoff_utc = _parse_iso_datetime(
                game.get("gameDateTimeUTC")
                or game.get("gameDateUTC")
                or game.get("gameDateTimeEst")
                or game.get("gameDateEst")
            )
            if tipoff_utc is None:
                game_date_value = pd.to_datetime(game_date_entry.get("gameDate"), errors="coerce")
                if pd.notna(game_date_value):
                    tipoff_utc = game_date_value.to_pydatetime().replace(tzinfo=NBA_SCHEDULE_TIMEZONE).astimezone(timezone.utc)
            if tipoff_utc is None:
                continue
            if tipoff_utc < window_start_utc or tipoff_utc > now_utc:
                continue

            game_id = str(game.get("gameId") or "").strip()
            if not game_id:
                continue

            game_date = _game_date_from_tipoff(tipoff_utc)
            if not game_date:
                continue

            games.append(
                {
                    "game_id": game_id,
                    "game_date": game_date,
                    "tipoff_utc": tipoff_utc.isoformat(),
                    "home_team": _normalize_team_code((game.get("homeTeam") or {}).get("teamTricode")),
                    "away_team": _normalize_team_code((game.get("awayTeam") or {}).get("teamTricode")),
                }
            )

    games.sort(key=lambda item: item.get("tipoff_utc", ""), reverse=True)
    return games


def _backfill_recent_history(
    schedule_payload: dict,
    training_path: Path,
    lookback_days: int,
    max_games_per_cycle: int,
    max_runtime_seconds: int | None = None,
) -> dict:
    lookback_days = max(1, int(lookback_days))
    max_games_per_cycle = max(1, int(max_games_per_cycle))
    runtime_cap_seconds = max(5, int(max_runtime_seconds)) if max_runtime_seconds else 0
    completed_games = _completed_games_from_schedule(schedule_payload, lookback_days=lookback_days)
    existing_ids = _existing_game_ids(training_path)
    games_to_fetch = [game for game in completed_games if str(game["game_id"]) not in existing_ids]
    games_to_fetch = games_to_fetch[:max_games_per_cycle]

    frames: list[pd.DataFrame] = []
    errors: list[str] = []
    fetched_games = 0
    timed_out = False
    started_at_monotonic = time.monotonic()
    for game in games_to_fetch:
        if runtime_cap_seconds and (time.monotonic() - started_at_monotonic) >= runtime_cap_seconds:
            timed_out = True
            break
        game_id = str(game["game_id"])
        try:
            boxscore_payload = fetch_boxscore(game_id)
            frame = _boxscore_players_to_rows(boxscore_payload)
            if not frame.empty:
                frames.append(frame)
            fetched_games += 1
        except (HTTPError, URLError, ValueError, OSError, KeyError, subprocess.SubprocessError) as exc:
            errors.append(f"{game_id}: {_sanitize_error_message(exc)}")

    rows_before_append = 0
    if frames:
        completed_rows = pd.concat(frames, ignore_index=True, sort=False)
        rows_before_append = int(len(completed_rows))
        appended_rows = _append_completed_rows(training_path, completed_rows)
    else:
        appended_rows = 0

    window_end = datetime.now(timezone.utc)
    window_start = window_end - timedelta(days=lookback_days)
    summary = {
        "games_scanned": int(len(completed_games)),
        "games_missing_before_fetch": int(len([game for game in completed_games if str(game["game_id"]) not in existing_ids])),
        "games_attempted": int(len(games_to_fetch)),
        "games_fetched": int(fetched_games),
        "games_failed": int(len(errors)),
        "rows_collected": int(rows_before_append),
        "rows_appended": int(appended_rows),
        "window_start": window_start.date().isoformat(),
        "window_end": window_end.date().isoformat(),
        "errors": errors[:12],
        "timed_out": bool(timed_out),
        "note": (
            f"Backfill runtime cap reached ({runtime_cap_seconds}s); continuing in later cycles."
            if timed_out
            else (None if games_to_fetch else "No missing final games detected in the configured backfill window.")
        ),
    }
    return summary


def _fetch_odds_events(provider_config: dict, lookahead_hours: int) -> tuple[list[dict], dict]:
    status = {
        "enabled": bool(provider_config.get("enabled", True)),
        "api_key_present": False,
        "events_loaded": 0,
        "event_dates": [],
        "last_error": None,
        "note": None,
        "source": "The Odds API",
    }
    if not status["enabled"]:
        status["note"] = "Odds provider is disabled."
        return [], status

    api_key = os.getenv(provider_config.get("api_key_env", "ODDS_API_KEY"), "").strip()
    status["api_key_present"] = bool(api_key)
    if not api_key:
        status["note"] = "Set ODDS_API_KEY to auto-build pregame slates from future NBA events."
        return [], status
    timeout_seconds = _coerce_positive_int(
        provider_config.get("request_timeout_seconds", DEFAULT_PROVIDER_REQUEST_TIMEOUT_SECONDS),
        DEFAULT_PROVIDER_REQUEST_TIMEOUT_SECONDS,
    )

    try:
        query = urlencode(
            {
                "apiKey": api_key,
                "regions": provider_config.get("regions", "us"),
                "markets": provider_config.get("markets", "spreads,totals"),
                "bookmakers": provider_config.get("bookmakers", "draftkings,fanduel"),
                "oddsFormat": provider_config.get("odds_format", "american"),
                "dateFormat": provider_config.get("date_format", "iso"),
            }
        )
        base_url = provider_config.get("base_url", "https://api.the-odds-api.com/v4").rstrip("/")
        sport = provider_config.get("sport", "basketball_nba")
        events = fetch_json(
            f"{base_url}/sports/{sport}/odds/?{query}",
            timeout=timeout_seconds,
        )
        if not isinstance(events, list):
            raise ValueError("Odds provider returned an unexpected payload.")

        now_utc = datetime.now(timezone.utc)
        cutoff_utc = now_utc + timedelta(hours=max(1, int(lookahead_hours)))
        filtered_events = []
        for event in events:
            tipoff_utc = _parse_iso_datetime(event.get("commence_time"))
            if tipoff_utc is None or tipoff_utc < now_utc or tipoff_utc > cutoff_utc:
                continue
            filtered_events.append(event)

        status["events_loaded"] = int(len(filtered_events))
        status["event_dates"] = sorted(
            {
                game_date
                for game_date in (_game_date_from_tipoff(_parse_iso_datetime(event.get("commence_time"))) for event in filtered_events)
                if game_date
            }
        )
        if not filtered_events:
            status["note"] = f"No future NBA odds events were found in the next {int(lookahead_hours)} hours."
        return filtered_events, status
    except (HTTPError, URLError, ValueError, OSError, subprocess.SubprocessError) as exc:
        status["last_error"] = _sanitize_error_message(exc)
        return [], status


def _scheduled_games_from_odds_events(events: list[dict]) -> list[dict]:
    scheduled_games: list[dict] = []
    for event in events:
        tipoff_utc = _parse_iso_datetime(event.get("commence_time"))
        game_date = _game_date_from_tipoff(tipoff_utc)
        home_team = _normalize_team_code(event.get("home_team"))
        away_team = _normalize_team_code(event.get("away_team"))
        if not game_date or not home_team or not away_team:
            continue

        scheduled_games.append(
            {
                "game_id": event.get("id"),
                "game_date": game_date,
                "home_team": home_team,
                "away_team": away_team,
                "source": "odds",
                "commence_time_utc": tipoff_utc.isoformat() if tipoff_utc else None,
            }
        )
    return scheduled_games


def _apply_context_frame(upcoming_frame: pd.DataFrame, context_frame: pd.DataFrame) -> pd.DataFrame:
    if upcoming_frame.empty or context_frame.empty:
        return upcoming_frame

    working_context = context_frame.copy()
    if "game_date" in working_context.columns:
        working_context["game_date"] = pd.to_datetime(working_context["game_date"], errors="coerce").dt.strftime("%Y-%m-%d")
    if "team" in working_context.columns:
        working_context["team"] = working_context["team"].map(_normalize_team_code).fillna(working_context["team"])

    join_keys = [column for column in CONTEXT_KEY_COLUMNS if column in working_context.columns and column in upcoming_frame.columns]
    if len(join_keys) < 2:
        join_keys = [column for column in ["player_name", "game_date"] if column in working_context.columns and column in upcoming_frame.columns]
    if len(join_keys) < 2:
        return upcoming_frame

    update_columns = [column for column in working_context.columns if column not in join_keys]
    merged = upcoming_frame.merge(working_context, on=join_keys, how="left", suffixes=("", "__context"))

    for column in update_columns:
        override_column = f"{column}__context"
        if override_column not in merged.columns:
            continue
        if column in merged.columns:
            if column in CONTEXT_OVERRIDE_COLUMNS:
                merged[column] = merged[override_column].where(merged[override_column].notna(), merged[column])
            else:
                merged[column] = merged[column].where(merged[column].notna(), merged[override_column])
        else:
            merged[column] = merged[override_column]
        merged = merged.drop(columns=[override_column])

    return merged


def _merge_context_updates(upcoming_frame: pd.DataFrame, context_paths: Path | list[Path]) -> pd.DataFrame:
    if upcoming_frame.empty:
        return upcoming_frame

    paths = context_paths if isinstance(context_paths, list) else [context_paths]
    merged = upcoming_frame.copy()
    for path in paths:
        if not path.exists():
            continue
        context_frame = pd.read_csv(path)
        if context_frame.empty:
            continue
        merged = _apply_context_frame(merged, context_frame)
    return merged


def _build_base_upcoming_frame(
    scoreboard_payload: dict,
    training_path: Path,
    include_live_games: bool,
    scheduled_games: list[dict] | None = None,
) -> tuple[pd.DataFrame, str | None, dict]:
    latest = _latest_player_context(training_path)
    if latest.empty:
        return pd.DataFrame(columns=["player_name", "game_date", "home", "opponent", "team", "commence_time_utc"]), (
            "Historical data needs a team column before automatic upcoming slate generation can work."
        ), {"scheduled_games_found": 0, "scheduled_game_dates": [], "scheduled_sources": []}

    raw_games = list(scheduled_games or _scheduled_games_from_scoreboard(scoreboard_payload, include_live_games))
    unique_games: list[dict] = []
    seen_game_keys: set[tuple[str, str, str]] = set()
    for game in raw_games:
        home_team = _normalize_team_code(game.get("home_team"))
        away_team = _normalize_team_code(game.get("away_team"))
        game_date = str(game.get("game_date") or "")
        if not game_date or not home_team or not away_team:
            continue
        game_key = (game_date, home_team, away_team)
        if game_key in seen_game_keys:
            continue
        seen_game_keys.add(game_key)
        normalized_game = dict(game)
        normalized_game["game_date"] = game_date
        normalized_game["home_team"] = home_team
        normalized_game["away_team"] = away_team
        normalized_game["commence_time_utc"] = game.get("commence_time_utc")
        unique_games.append(normalized_game)

    slate_rows: list[dict] = []
    for game in unique_games:
        game_date = game["game_date"]
        for team_tricode, opponent_tricode, home_flag in [
            (game["home_team"], game["away_team"], 1),
            (game["away_team"], game["home_team"], 0),
        ]:
            team_players = latest[latest["team"] == team_tricode].copy()
            if team_players.empty:
                continue

            for _, row in team_players.iterrows():
                slate_row = {
                    "player_name": row["player_name"],
                    "game_date": game_date,
                    "home": home_flag,
                    "opponent": opponent_tricode,
                    "team": team_tricode,
                    "commence_time_utc": game.get("commence_time_utc"),
                }
                for column in UPCOMING_CONTEXT_COLUMNS:
                    if column in row.index and column not in slate_row:
                        slate_row[column] = row[column]

                last_game_date = row.get("last_game_date")
                if pd.notna(last_game_date):
                    slate_row["rest_days"] = max(0, (pd.to_datetime(game_date) - pd.to_datetime(last_game_date)).days)

                slate_rows.append(slate_row)

    upcoming_frame = pd.DataFrame(slate_rows)
    if not upcoming_frame.empty:
        upcoming_frame["game_date"] = pd.to_datetime(upcoming_frame["game_date"]).dt.strftime("%Y-%m-%d")
        upcoming_frame["team"] = upcoming_frame["team"].map(_normalize_team_code).fillna(upcoming_frame["team"])
        upcoming_frame["opponent"] = upcoming_frame["opponent"].map(_normalize_team_code).fillna(upcoming_frame["opponent"])
        if "commence_time_utc" in upcoming_frame.columns:
            upcoming_frame["commence_time_utc"] = pd.to_datetime(
                upcoming_frame["commence_time_utc"],
                errors="coerce",
                utc=True,
            ).dt.strftime("%Y-%m-%dT%H:%M:%SZ")
        upcoming_frame = upcoming_frame.drop_duplicates(subset=CONTEXT_KEY_COLUMNS, keep="last")

    note = None
    if not unique_games:
        note = "No upcoming NBA games were found in the current live feeds."

    return upcoming_frame, note, {
        "scheduled_games_found": int(len(unique_games)),
        "scheduled_game_dates": sorted({game["game_date"] for game in unique_games}),
        "scheduled_sources": sorted({str(game.get("source", "unknown")) for game in unique_games}),
    }


def _write_csv_frame(path: Path, frame: pd.DataFrame, default_columns: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    ordered_columns: list[str] = []
    for column in default_columns:
        if column not in ordered_columns:
            ordered_columns.append(column)

    if frame.empty:
        empty_payload = pd.DataFrame(columns=ordered_columns).to_csv(index=False)
        if path.exists():
            try:
                if path.read_text(encoding="utf-8") == empty_payload:
                    return
            except OSError:
                pass
        path.write_text(empty_payload, encoding="utf-8")
        return

    clean_frame = frame.loc[:, ~frame.columns.duplicated()].copy()
    for column in clean_frame.columns:
        if column not in ordered_columns:
            ordered_columns.append(column)
    for column in ordered_columns:
        if column not in clean_frame.columns:
            clean_frame[column] = pd.NA
    serialized = clean_frame[ordered_columns].to_csv(index=False)
    if path.exists():
        try:
            if path.read_text(encoding="utf-8") == serialized:
                return
        except OSError:
            pass
    path.write_text(serialized, encoding="utf-8")


def _csv_has_rows(path: Path) -> bool:
    if not path.exists():
        return False
    try:
        frame = pd.read_csv(path, nrows=1)
    except (OSError, pd.errors.EmptyDataError):
        return False
    return not frame.empty


def _csv_row_count(path: Path) -> int:
    if not path.exists():
        return 0
    try:
        return int(len(pd.read_csv(path)))
    except (OSError, pd.errors.EmptyDataError):
        return 0


def _average_or_none(values: list[float]) -> float | None:
    if not values:
        return None
    return round(sum(values) / len(values), 2)


def _coerce_float(value: object) -> float | None:
    numeric = pd.to_numeric(pd.Series([value]), errors="coerce").iloc[0]
    if pd.isna(numeric):
        return None
    return float(numeric)


def _coerce_spread_from_string(value: object) -> float | None:
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None
    cleaned = re.sub(r"[^0-9+\-.]", "", text)
    if not cleaned:
        return None
    return _coerce_float(cleaned)


def _coerce_espn_stat_value(value: object) -> float | None:
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None
    if ":" in text:
        parts = text.split(":")
        if len(parts) == 2:
            mins = _coerce_float(parts[0])
            secs = _coerce_float(parts[1])
            if mins is not None and secs is not None:
                return round(float(mins) + float(secs) / 60.0, 3)
    fraction_match = re.match(r"^\s*(-?\d+(?:\.\d+)?)\s*-\s*(-?\d+(?:\.\d+)?)\s*$", text)
    if fraction_match:
        return _coerce_float(fraction_match.group(1))
    return _coerce_float(text)


def _parse_made_attempt_stat(value: object) -> tuple[float | None, float | None]:
    if value is None:
        return None, None
    text = str(value).strip()
    if not text:
        return None, None
    fraction_match = re.match(r"^\s*(-?\d+(?:\.\d+)?)\s*-\s*(-?\d+(?:\.\d+)?)\s*$", text)
    if fraction_match:
        return _coerce_float(fraction_match.group(1)), _coerce_float(fraction_match.group(2))
    numeric = _coerce_float(text)
    if numeric is None:
        return None, None
    return numeric, None


def _espn_game_status_code(status_type: dict) -> int:
    state_value = str(status_type.get("state") or "").strip().lower()
    if bool(status_type.get("completed")) or state_value == "post":
        return 3
    if state_value == "in":
        return 2
    return 1


def _extract_espn_event_metadata(event: dict) -> dict | None:
    event_id = str(event.get("id") or "").strip()
    if not event_id:
        return None
    competition = (event.get("competitions") or [{}])[0] if isinstance(event.get("competitions"), list) else {}
    competitors = competition.get("competitors") or []
    home_code = None
    away_code = None
    for competitor in competitors:
        team_payload = competitor.get("team") or {}
        abbreviation = (
            team_payload.get("abbreviation")
            or team_payload.get("shortDisplayName")
            or team_payload.get("displayName")
        )
        team_code = _normalize_team_code(abbreviation)
        if not team_code:
            continue
        side = str(competitor.get("homeAway") or "").strip().lower()
        if side == "home":
            home_code = team_code
        elif side == "away":
            away_code = team_code
    if not home_code or not away_code:
        return None

    status_type = ((competition.get("status") or {}).get("type") or {})
    commence_time = _parse_iso_datetime(event.get("date"))
    game_date = _game_date_from_tipoff(commence_time)
    if not game_date:
        game_date = pd.to_datetime(event.get("date"), errors="coerce")
        game_date = game_date.strftime("%Y-%m-%d") if pd.notna(game_date) else None
    return {
        "event_id": event_id,
        "home_team": home_code,
        "away_team": away_code,
        "game_status": _espn_game_status_code(status_type),
        "game_status_detail": str(status_type.get("shortDetail") or status_type.get("detail") or "").strip(),
        "commence_time_utc": commence_time.isoformat() if commence_time else None,
        "game_date": game_date,
    }


def _extract_espn_summary_player_rows(
    summary_payload: dict,
    event_meta: dict,
    *,
    captured_at: str,
    captured_at_bucket: str,
    summary_url: str,
) -> pd.DataFrame:
    rows: list[dict] = []
    boxscore = (summary_payload or {}).get("boxscore") or {}
    team_player_groups = boxscore.get("players") or []
    for team_group in team_player_groups:
        team_payload = team_group.get("team") or {}
        team_code = _normalize_team_code(
            team_payload.get("abbreviation")
            or team_payload.get("shortDisplayName")
            or team_payload.get("displayName")
        )
        if not team_code:
            continue
        home_team = str(event_meta.get("home_team") or "")
        away_team = str(event_meta.get("away_team") or "")
        if team_code not in {home_team, away_team}:
            continue
        opponent_code = away_team if team_code == home_team else home_team
        home_flag = 1 if team_code == home_team else 0

        stat_block = None
        for candidate in team_group.get("statistics") or []:
            candidate_keys = {
                _normalize_provider_column_name(key)
                for key in (candidate.get("keys") or [])
                if str(key).strip()
            }
            if not candidate_keys:
                continue
            if "points" in candidate_keys and ("assists" in candidate_keys or "rebounds" in candidate_keys):
                stat_block = candidate
                break
        if not stat_block:
            continue
        keys = [str(key).strip() for key in (stat_block.get("keys") or [])]
        key_index = {_normalize_provider_column_name(key): idx for idx, key in enumerate(keys)}
        athletes = stat_block.get("athletes") or []
        for athlete_row in athletes:
            athlete_payload = athlete_row.get("athlete") or {}
            player_name = str(
                athlete_payload.get("displayName")
                or athlete_payload.get("shortName")
                or athlete_row.get("displayName")
                or ""
            ).strip()
            if not player_name:
                continue
            raw_stats = athlete_row.get("stats") or athlete_row.get("statistics") or []
            values_by_key: dict[str, object] = {}
            if isinstance(raw_stats, dict):
                values_by_key = {
                    _normalize_provider_column_name(k): v
                    for k, v in raw_stats.items()
                    if str(k).strip()
                }
            elif isinstance(raw_stats, list):
                for key, idx in key_index.items():
                    if 0 <= idx < len(raw_stats):
                        values_by_key[key] = raw_stats[idx]

            def value_for(*candidate_keys: str) -> object:
                for candidate_key in candidate_keys:
                    normalized_key = _normalize_provider_column_name(candidate_key)
                    if normalized_key in values_by_key:
                        return values_by_key.get(normalized_key)
                return None

            points = _coerce_espn_stat_value(value_for("points", "pts"))
            rebounds = _coerce_espn_stat_value(value_for("rebounds", "reb"))
            assists = _coerce_espn_stat_value(value_for("assists", "ast"))
            steals = _coerce_espn_stat_value(value_for("steals", "stl"))
            blocks = _coerce_espn_stat_value(value_for("blocks", "blk"))
            turnovers = _coerce_espn_stat_value(value_for("turnovers", "to", "tov"))
            three_points_made = _coerce_espn_stat_value(
                value_for(
                    "threePointFieldGoalsMade-threePointFieldGoalsAttempted",
                    "threePointFieldGoalsMade",
                    "threePointsMade",
                    "3pt",
                    "3pm",
                )
            )
            minutes = _coerce_espn_stat_value(value_for("minutes", "min"))
            personal_fouls = _coerce_espn_stat_value(value_for("fouls", "personalFouls", "pf"))
            plus_minus = _coerce_espn_stat_value(value_for("plusMinus", "+/-", "plus_minus"))
            field_goals_made, field_goals_attempted = _parse_made_attempt_stat(
                value_for("fieldGoalsMade-fieldGoalsAttempted", "fg")
            )
            free_throws_made, free_throws_attempted = _parse_made_attempt_stat(
                value_for("freeThrowsMade-freeThrowsAttempted", "ft")
            )
            did_not_play = bool(
                athlete_row.get("didNotPlay")
                or athlete_row.get("didNotParticipate")
                or athlete_row.get("ejected")
            )

            row_payload = {
                "captured_at": captured_at,
                "captured_at_bucket": captured_at_bucket,
                "source": "espn_summary",
                "event_id": str(event_meta.get("event_id") or ""),
                "game_status": int(event_meta.get("game_status") or 0),
                "game_status_detail": str(event_meta.get("game_status_detail") or ""),
                "commence_time_utc": event_meta.get("commence_time_utc"),
                "game_date": event_meta.get("game_date"),
                "home_team": home_team,
                "away_team": away_team,
                "player_name": player_name,
                "team": team_code,
                "opponent": opponent_code,
                "home": home_flag,
                "starter": 1 if bool(athlete_row.get("starter")) else 0,
                "did_not_play": 1 if did_not_play else 0,
                "minutes": minutes,
                "points": points,
                "rebounds": rebounds,
                "assists": assists,
                "steals": steals,
                "blocks": blocks,
                "turnovers": turnovers,
                "three_points_made": three_points_made,
                "field_goals_made": field_goals_made,
                "field_goals_attempted": field_goals_attempted,
                "free_throws_made": free_throws_made,
                "free_throws_attempted": free_throws_attempted,
                "personal_fouls": personal_fouls,
                "plus_minus": plus_minus,
                "summary_url": summary_url,
            }
            if did_not_play and all(
                pd.isna(value)
                for value in [row_payload.get("minutes"), row_payload.get("points"), row_payload.get("rebounds"), row_payload.get("assists")]
            ):
                continue
            rows.append(row_payload)

    frame = pd.DataFrame(rows, columns=ESPN_LIVE_LOG_COLUMNS)
    if frame.empty:
        return frame
    for stat_column in [
        "minutes",
        "points",
        "rebounds",
        "assists",
        "turnovers",
        "personal_fouls",
        "field_goals_attempted",
        "free_throws_attempted",
    ]:
        frame[stat_column] = pd.to_numeric(frame.get(stat_column), errors="coerce")
    minutes_denominator = frame["minutes"].clip(lower=0.5).fillna(0.5)
    usage_proxy = (
        pd.to_numeric(frame.get("field_goals_attempted"), errors="coerce").fillna(0.0)
        + 0.44 * pd.to_numeric(frame.get("free_throws_attempted"), errors="coerce").fillna(0.0)
        + pd.to_numeric(frame.get("turnovers"), errors="coerce").fillna(0.0)
        + pd.to_numeric(frame.get("assists"), errors="coerce").fillna(0.0)
    )
    frame["notes_live_points_per_minute"] = (pd.to_numeric(frame.get("points"), errors="coerce").fillna(0.0) / minutes_denominator).round(4)
    frame["notes_live_rebounds_per_minute"] = (pd.to_numeric(frame.get("rebounds"), errors="coerce").fillna(0.0) / minutes_denominator).round(4)
    frame["notes_live_assists_per_minute"] = (pd.to_numeric(frame.get("assists"), errors="coerce").fillna(0.0) / minutes_denominator).round(4)
    frame["notes_live_usage_proxy"] = (usage_proxy / minutes_denominator).round(4)
    frame["notes_live_foul_pressure"] = (
        pd.to_numeric(frame.get("personal_fouls"), errors="coerce").fillna(0.0) / minutes_denominator
    ).round(4)
    frame["notes_live_minutes_current"] = pd.to_numeric(frame.get("minutes"), errors="coerce").round(3)
    return frame[ESPN_LIVE_LOG_COLUMNS].copy()


def _fetch_espn_live_rows(
    upcoming_frame: pd.DataFrame,
    provider_config: dict,
    game_notes_provider_config: dict,
) -> tuple[pd.DataFrame, dict]:
    status = {
        "enabled": bool(provider_config.get("enabled", True)),
        "source": "ESPN scoreboard + summary",
        "rows": 0,
        "rows_appended": 0,
        "rows_appended_to_game_notes": 0,
        "events_loaded": 0,
        "summaries_loaded": 0,
        "live_events": 0,
        "final_events": 0,
        "dates_loaded": [],
        "path": None,
        "last_error": None,
        "note": None,
    }
    empty_frame = pd.DataFrame(columns=ESPN_LIVE_LOG_COLUMNS)
    if not status["enabled"]:
        status["note"] = "ESPN live provider is disabled."
        return empty_frame, status

    timeout_seconds = _coerce_positive_int(
        provider_config.get("request_timeout_seconds", DEFAULT_PROVIDER_REQUEST_TIMEOUT_SECONDS),
        DEFAULT_PROVIDER_REQUEST_TIMEOUT_SECONDS,
    )
    scoreboard_template = str(provider_config.get("scoreboard_url_template") or ESPN_SCOREBOARD_URL_TEMPLATE).strip()
    summary_template = str(provider_config.get("summary_url_template") or ESPN_SUMMARY_URL_TEMPLATE).strip()
    max_dates_per_cycle = _coerce_positive_int(provider_config.get("max_dates_per_cycle", 2), 2)
    max_events_per_cycle = _coerce_positive_int(provider_config.get("max_events_per_cycle", 20), 20)
    max_rows_retained = _coerce_positive_int(provider_config.get("max_rows_retained", 500_000), 500_000)
    include_pregame_events = bool(provider_config.get("include_pregame_events", False))
    mirror_into_game_notes = bool(provider_config.get("mirror_into_game_notes_live_actions", True))
    store_path = Path(str(provider_config.get("store_path") or DEFAULT_ESPN_LIVE_GAMES_PATH))
    status["path"] = str(store_path)

    if not scoreboard_template or not summary_template:
        status["note"] = "ESPN templates are not configured."
        return empty_frame, status

    requested_dates: list[str] = []
    if not upcoming_frame.empty and "game_date" in upcoming_frame.columns:
        requested_dates = sorted(
            {
                str(value)
                for value in pd.to_datetime(upcoming_frame["game_date"], errors="coerce")
                .dropna()
                .dt.strftime("%Y-%m-%d")
                .unique()
                .tolist()
            }
        )
    if not requested_dates:
        requested_dates = [datetime.now(NBA_SCHEDULE_TIMEZONE).strftime("%Y-%m-%d")]
    requested_dates = requested_dates[:max_dates_per_cycle]

    events: list[dict] = []
    for game_date in requested_dates:
        yyyymmdd = game_date.replace("-", "")
        try:
            scoreboard_payload = fetch_json(
                scoreboard_template.format(yyyymmdd=yyyymmdd),
                timeout=timeout_seconds,
            )
        except (HTTPError, URLError, OSError, ValueError, subprocess.SubprocessError) as exc:
            status["last_error"] = _sanitize_error_message(exc)
            continue
        for event in scoreboard_payload.get("events", []) or []:
            event_meta = _extract_espn_event_metadata(event)
            if not event_meta:
                continue
            if not include_pregame_events and int(event_meta.get("game_status", 0) or 0) < 2:
                continue
            events.append(event_meta)

    if not events:
        status["dates_loaded"] = requested_dates
        status["note"] = "No ESPN live/final events were found for the requested dates."
        return empty_frame, status

    events = events[:max_events_per_cycle]
    status["events_loaded"] = int(len(events))
    status["dates_loaded"] = sorted({str(event_meta.get("game_date") or "") for event_meta in events if event_meta.get("game_date")})
    status["live_events"] = int(sum(1 for event_meta in events if int(event_meta.get("game_status", 0) or 0) == 2))
    status["final_events"] = int(sum(1 for event_meta in events if int(event_meta.get("game_status", 0) or 0) == 3))

    captured_at = _now_iso()
    captured_at_bucket = pd.Timestamp.now(tz=timezone.utc).floor("10s").isoformat()
    frames: list[pd.DataFrame] = []
    for event_meta in events:
        event_id = str(event_meta.get("event_id") or "").strip()
        if not event_id:
            continue
        summary_url = summary_template.format(event_id=event_id)
        try:
            summary_payload = fetch_json(summary_url, timeout=timeout_seconds)
        except (HTTPError, URLError, OSError, ValueError, subprocess.SubprocessError) as exc:
            status["last_error"] = _sanitize_error_message(exc)
            continue
        event_frame = _extract_espn_summary_player_rows(
            summary_payload,
            event_meta,
            captured_at=captured_at,
            captured_at_bucket=captured_at_bucket,
            summary_url=summary_url,
        )
        if event_frame.empty:
            continue
        frames.append(event_frame)
        status["summaries_loaded"] = int(status["summaries_loaded"] + 1)

    if not frames:
        status["note"] = "ESPN events were loaded, but no player summary rows were available."
        return empty_frame, status

    merged = pd.concat(frames, ignore_index=True, sort=False)
    try:
        rows_appended, total_rows = _append_dataset_log(
            store_path,
            merged,
            dedupe_keys=["captured_at_bucket", "event_id", "player_name", "team"],
            max_rows=max_rows_retained,
            sort_columns=["captured_at", "game_date", "event_id", "player_name"],
        )
        status["rows_appended"] = int(rows_appended)
        status["rows"] = int(total_rows)
    except Exception as exc:  # noqa: BLE001
        status["rows"] = int(len(merged))
        status["last_error"] = _sanitize_error_message(exc)

    if mirror_into_game_notes and bool(game_notes_provider_config.get("enabled", True)):
        live_actions_path = Path(str(game_notes_provider_config.get("live_actions_path") or DEFAULT_LIVE_GAME_ACTIONS_PATH))
        max_live_rows = _coerce_positive_int(game_notes_provider_config.get("max_live_rows_retained", 250_000), 250_000)
        game_notes_columns = [
            "captured_at",
            "captured_at_bucket",
            "game_id",
            "game_date",
            "player_name",
            "team",
            "opponent",
            "minutes",
            "points",
            "rebounds",
            "assists",
            "steals",
            "blocks",
            "turnovers",
            "three_points_made",
            "field_goals_attempted",
            "free_throws_attempted",
            "personal_fouls",
            "notes_live_points_per_minute",
            "notes_live_rebounds_per_minute",
            "notes_live_assists_per_minute",
            "notes_live_usage_proxy",
            "notes_live_foul_pressure",
            "notes_live_minutes_current",
        ]
        game_notes_frame = merged.rename(columns={"event_id": "game_id"}).copy()
        game_notes_frame = game_notes_frame[game_notes_columns]
        try:
            rows_appended_notes, _ = _append_dataset_log(
                live_actions_path,
                game_notes_frame,
                dedupe_keys=["captured_at_bucket", "game_id", "player_name", "team"],
                max_rows=max_live_rows,
                sort_columns=["captured_at", "game_date", "game_id", "player_name"],
            )
            status["rows_appended_to_game_notes"] = int(rows_appended_notes)
        except Exception as exc:  # noqa: BLE001
            status["last_error"] = _sanitize_error_message(exc)

    if status["rows_appended"] == 0:
        status["note"] = "ESPN live rows were checked with no changes this cycle."
    else:
        status["note"] = f"Stored {status['rows_appended']} ESPN live player rows."
    return merged[ESPN_LIVE_LOG_COLUMNS].copy(), status


def _extract_pickcenter_market_summary(payload: dict) -> dict[str, float | None]:
    entries = payload.get("pickcenter")
    if not isinstance(entries, list) or not entries:
        return {}
    pickcenter = entries[0] if isinstance(entries[0], dict) else {}
    if not pickcenter:
        return {}

    home_spread = None
    away_spread = None
    total = _coerce_float(pickcenter.get("overUnder"))

    point_spread = pickcenter.get("pointSpread") if isinstance(pickcenter.get("pointSpread"), dict) else {}
    if point_spread:
        home_spread = _coerce_spread_from_string(
            ((point_spread.get("home") or {}).get("close") or {}).get("line"),
        )
        away_spread = _coerce_spread_from_string(
            ((point_spread.get("away") or {}).get("close") or {}).get("line"),
        )

    if total is None:
        total = _coerce_spread_from_string(
            (((pickcenter.get("total") or {}).get("over") or {}).get("close") or {}).get("line"),
        )
    if total is None:
        total = _coerce_spread_from_string(
            (((pickcenter.get("total") or {}).get("under") or {}).get("close") or {}).get("line"),
        )

    if home_spread is None and away_spread is None:
        spread = _coerce_float(pickcenter.get("spread"))
        home_favorite = bool(((pickcenter.get("homeTeamOdds") or {}).get("favorite")))
        away_favorite = bool(((pickcenter.get("awayTeamOdds") or {}).get("favorite")))
        if spread is not None:
            if home_favorite:
                home_spread = float(spread)
                away_spread = -float(spread)
            elif away_favorite:
                away_spread = float(spread)
                home_spread = -float(spread)

    if home_spread is None and away_spread is not None:
        home_spread = round(-away_spread, 2)
    if away_spread is None and home_spread is not None:
        away_spread = round(-home_spread, 2)

    return {
        "home_spread": round(home_spread, 2) if home_spread is not None else None,
        "away_spread": round(away_spread, 2) if away_spread is not None else None,
        "game_total": round(total, 2) if total is not None else None,
    }


def _build_provider_rows_from_game_markets(
    upcoming_frame: pd.DataFrame,
    game_markets: dict[tuple[str, str], dict[str, float | None]],
) -> tuple[pd.DataFrame, set[tuple[str, str]]]:
    provider_rows: list[dict] = []
    matched_games: set[tuple[str, str]] = set()
    for _, row in upcoming_frame.iterrows():
        is_home = int(row.get("home", 0)) == 1
        home_team = row["team"] if is_home else row["opponent"]
        away_team = row["opponent"] if is_home else row["team"]
        game_key = (_normalize_team_code(home_team) or str(home_team), _normalize_team_code(away_team) or str(away_team))
        market = game_markets.get(game_key)
        if not market:
            continue

        team_spread = market["home_spread"] if is_home else market["away_spread"]
        provider_row = {
            "player_name": row["player_name"],
            "game_date": row["game_date"],
            "team": row["team"],
        }
        if market["game_total"] is not None:
            provider_row["game_total"] = market["game_total"]
        if team_spread is not None:
            provider_row["spread"] = team_spread
        if market["game_total"] is not None and team_spread is not None:
            provider_row["implied_team_total"] = round((market["game_total"] - team_spread) / 2, 2)
        provider_rows.append(provider_row)
        matched_games.add(game_key)

    provider_frame = pd.DataFrame(provider_rows)
    if not provider_frame.empty:
        prop_value_columns = [
            column
            for column in [
                "line_points",
                "line_rebounds",
                "line_assists",
                "line_pra",
                "line_three_points_made",
                "line_points_rebounds",
                "line_points_assists",
                "line_rebounds_assists",
                "line_steals",
                "line_blocks",
                "line_turnovers",
                "line_steals_blocks",
            ]
            if column in provider_frame.columns
        ]
        provider_frame = (
            provider_frame
            .groupby(CONTEXT_KEY_COLUMNS, as_index=False)[prop_value_columns]
            .mean(numeric_only=True)
        )
    return provider_frame, matched_games


def _fetch_espn_pickcenter_markets(
    upcoming_frame: pd.DataFrame,
    provider_config: dict,
) -> tuple[dict[tuple[str, str], dict[str, float | None]], dict]:
    status = {
        "enabled": bool(provider_config.get("enable_espn_fallback", True)),
        "events_loaded": 0,
        "event_dates": [],
        "games_matched": 0,
        "last_error": None,
        "note": None,
        "source": "ESPN pickcenter fallback",
    }
    if not status["enabled"]:
        status["note"] = "ESPN fallback is disabled."
        return {}, status
    if upcoming_frame.empty:
        status["note"] = "No upcoming slate rows are available for ESPN fallback matching."
        return {}, status

    try:
        game_dates = sorted(
            {
                str(value)
                for value in pd.to_datetime(upcoming_frame["game_date"], errors="coerce")
                .dropna()
                .dt.strftime("%Y-%m-%d")
                .unique()
                .tolist()
            }
        )
        if not game_dates:
            status["note"] = "Upcoming slate has no valid game dates for ESPN fallback."
            return {}, status

        scoreboard_template = str(provider_config.get("espn_scoreboard_url_template") or ESPN_SCOREBOARD_URL_TEMPLATE)
        summary_template = str(provider_config.get("espn_summary_url_template") or ESPN_SUMMARY_URL_TEMPLATE)

        event_lookup: dict[tuple[str, str, str], str] = {}
        for game_date in game_dates:
            yyyymmdd = game_date.replace("-", "")
            scoreboard_payload = fetch_json(scoreboard_template.format(yyyymmdd=yyyymmdd))
            for event in scoreboard_payload.get("events", []):
                event_id = str(event.get("id") or "").strip()
                if not event_id:
                    continue
                competition = (event.get("competitions") or [{}])[0]
                competitors = competition.get("competitors") or []
                home_code = None
                away_code = None
                for competitor in competitors:
                    team_payload = competitor.get("team") or {}
                    abbreviation = (
                        team_payload.get("abbreviation")
                        or team_payload.get("shortDisplayName")
                        or team_payload.get("displayName")
                    )
                    team_code = _normalize_team_code(abbreviation)
                    if not team_code:
                        continue
                    side = str(competitor.get("homeAway") or "").lower()
                    if side == "home":
                        home_code = team_code
                    elif side == "away":
                        away_code = team_code
                if not home_code or not away_code:
                    continue
                event_date = _game_date_from_tipoff(_parse_iso_datetime(event.get("date"))) or game_date
                event_lookup[(event_date, home_code, away_code)] = event_id

        status["events_loaded"] = int(len(event_lookup))
        status["event_dates"] = sorted({key[0] for key in event_lookup.keys()})
        if not event_lookup:
            status["note"] = "ESPN fallback loaded, but no NBA events were mapped for the upcoming slate dates."
            return {}, status

        game_markets: dict[tuple[str, str], dict[str, float | None]] = {}
        for game_key, event_id in event_lookup.items():
            _, home_code, away_code = game_key
            summary_payload = fetch_json(summary_template.format(event_id=event_id))
            market_summary = _extract_pickcenter_market_summary(summary_payload)
            if not market_summary:
                continue
            game_markets[(home_code, away_code)] = market_summary

        status["games_matched"] = int(len(game_markets))
        if not game_markets:
            status["note"] = "ESPN fallback loaded events, but pickcenter spreads/totals were unavailable for those games."
        return game_markets, status
    except (HTTPError, URLError, ValueError, OSError, subprocess.SubprocessError, KeyError) as exc:
        status["last_error"] = _sanitize_error_message(exc)
        return {}, status


def _extract_odds_market_summary(event: dict) -> dict[str, float | None]:
    home_code = _normalize_team_code(event.get("home_team"))
    away_code = _normalize_team_code(event.get("away_team"))
    if not home_code or not away_code:
        return {}

    home_spreads: list[float] = []
    away_spreads: list[float] = []
    totals: list[float] = []

    for bookmaker in event.get("bookmakers", []):
        for market in bookmaker.get("markets", []):
            outcomes = market.get("outcomes", [])
            if market.get("key") == "spreads":
                for outcome in outcomes:
                    point = outcome.get("point")
                    outcome_team = _normalize_team_code(outcome.get("name"))
                    if point is None or outcome_team is None:
                        continue
                    if outcome_team == home_code:
                        home_spreads.append(float(point))
                    elif outcome_team == away_code:
                        away_spreads.append(float(point))
            elif market.get("key") == "totals":
                for outcome in outcomes:
                    point = outcome.get("point")
                    if point is not None:
                        totals.append(float(point))

    home_spread = _average_or_none(home_spreads)
    away_spread = _average_or_none(away_spreads)
    total = _average_or_none(totals)

    if home_spread is None and away_spread is not None:
        home_spread = round(-away_spread, 2)
    if away_spread is None and home_spread is not None:
        away_spread = round(-home_spread, 2)

    return {
        "home_spread": home_spread,
        "away_spread": away_spread,
        "game_total": total,
    }


def _fetch_odds_provider_rows(
    upcoming_frame: pd.DataFrame,
    provider_config: dict,
    events: list[dict] | None = None,
    base_status: dict | None = None,
) -> tuple[pd.DataFrame, dict]:
    status = {
        "enabled": bool(provider_config.get("enabled", True)),
        "rows": 0,
        "games_matched": 0,
        "api_key_present": False,
        "events_loaded": 0,
        "event_dates": [],
        "last_error": None,
        "note": None,
        "source": "The Odds API",
        "fallback_source": None,
    }
    if base_status:
        status.update(base_status)
    if not status["enabled"]:
        status["note"] = "Odds provider is disabled."
        return pd.DataFrame(columns=CONTEXT_KEY_COLUMNS + ["game_total", "spread", "implied_team_total"]), status

    api_key = os.getenv(provider_config.get("api_key_env", "ODDS_API_KEY"), "").strip()
    status["api_key_present"] = bool(api_key)
    if upcoming_frame.empty:
        status["note"] = "No upcoming slate rows are available for odds matching."
        return pd.DataFrame(columns=CONTEXT_KEY_COLUMNS + ["game_total", "spread", "implied_team_total"]), status

    def _fallback_from_espn(note_prefix: str | None = None) -> tuple[pd.DataFrame, dict]:
        fallback_markets, fallback_status = _fetch_espn_pickcenter_markets(upcoming_frame, provider_config)
        if fallback_status.get("last_error"):
            status["last_error"] = fallback_status.get("last_error")
        if fallback_status.get("event_dates"):
            status["event_dates"] = fallback_status.get("event_dates", [])
        if pd.to_numeric(pd.Series([fallback_status.get("events_loaded")]), errors="coerce").fillna(0).iloc[0] > 0:
            status["events_loaded"] = int(pd.to_numeric(pd.Series([fallback_status.get("events_loaded")]), errors="coerce").fillna(0).iloc[0])

        if fallback_markets:
            provider_frame, matched_games = _build_provider_rows_from_game_markets(upcoming_frame, fallback_markets)
            status["rows"] = int(len(provider_frame))
            status["games_matched"] = int(len(matched_games))
            status["source"] = "ESPN pickcenter fallback"
            status["fallback_source"] = "ESPN pickcenter"
            fallback_note = fallback_status.get("note")
            if note_prefix and fallback_note:
                status["note"] = f"{note_prefix} {fallback_note}".strip()
            elif note_prefix:
                status["note"] = note_prefix
            else:
                status["note"] = fallback_note
            return provider_frame, status

        empty = pd.DataFrame(columns=CONTEXT_KEY_COLUMNS + ["game_total", "spread", "implied_team_total"])
        fallback_note = fallback_status.get("note")
        if note_prefix and fallback_note:
            status["note"] = f"{note_prefix} {fallback_note}".strip()
        elif note_prefix:
            status["note"] = note_prefix
        elif fallback_note:
            status["note"] = fallback_note
        return empty, status

    if not api_key:
        return _fallback_from_espn("ODDS_API_KEY was not set; using ESPN pregame markets.")

    try:
        odds_events = events
        if odds_events is None:
            odds_events, fetch_status = _fetch_odds_events(
                provider_config,
                int(provider_config.get("lookahead_hours", DEFAULT_LIVE_CONFIG["pregame_slate_lookahead_hours"])),
            )
            status.update(fetch_status)

        game_markets: dict[tuple[str, str], dict[str, float | None]] = {}
        for event in odds_events:
            home_code = _normalize_team_code(event.get("home_team"))
            away_code = _normalize_team_code(event.get("away_team"))
            if not home_code or not away_code:
                continue
            summary = _extract_odds_market_summary(event)
            if summary:
                game_markets[(home_code, away_code)] = summary

        provider_frame, matched_games = _build_provider_rows_from_game_markets(upcoming_frame, game_markets)
        status["rows"] = int(len(provider_frame))
        status["games_matched"] = int(len(matched_games))
        if not provider_frame.empty:
            return provider_frame, status

        note = "Odds provider returned data, but none of the events matched the current slate."
        return _fallback_from_espn(note)
    except (HTTPError, URLError, ValueError, OSError, subprocess.SubprocessError) as exc:
        status["last_error"] = _sanitize_error_message(exc)
        return _fallback_from_espn("The Odds API failed; using ESPN pregame markets.")


def _player_props_market_column(market_key: str) -> str | None:
    mapping = {
        "player_points": "line_points",
        "player_points_alternate": "line_points",
        "player_rebounds": "line_rebounds",
        "player_rebounds_alternate": "line_rebounds",
        "player_assists": "line_assists",
        "player_assists_alternate": "line_assists",
        "player_points_rebounds_assists": "line_pra",
        "player_points_rebounds_assists_alternate": "line_pra",
        "player_points_rebounds": "line_points_rebounds",
        "player_points_rebounds_alternate": "line_points_rebounds",
        "player_points_assists": "line_points_assists",
        "player_points_assists_alternate": "line_points_assists",
        "player_rebounds_assists": "line_rebounds_assists",
        "player_rebounds_assists_alternate": "line_rebounds_assists",
        "player_steals": "line_steals",
        "player_steals_alternate": "line_steals",
        "player_blocks": "line_blocks",
        "player_blocks_alternate": "line_blocks",
        "player_turnovers": "line_turnovers",
        "player_turnovers_alternate": "line_turnovers",
        "player_steals_blocks": "line_steals_blocks",
        "player_steals_blocks_alternate": "line_steals_blocks",
        "player_threes": "line_three_points_made",
        "player_three_points_made": "line_three_points_made",
        "player_threes_alternate": "line_three_points_made",
        "player_three_points_made_alternate": "line_three_points_made",
    }
    return mapping.get(str(market_key or "").strip().lower())


def _rotowire_market_column(market_name: object) -> str | None:
    normalized = re.sub(r"[^a-z0-9]+", "", str(market_name or "").strip().lower())
    mapping = {
        "points": "line_points",
        "rebounds": "line_rebounds",
        "assists": "line_assists",
        "ptsrebast": "line_pra",
        "3ptmade": "line_three_points_made",
        "ptsreb": "line_points_rebounds",
        "ptsast": "line_points_assists",
        "rebast": "line_rebounds_assists",
        "steals": "line_steals",
        "blocks": "line_blocks",
        "turnovers": "line_turnovers",
        "blkstl": "line_steals_blocks",
        "stlblk": "line_steals_blocks",
    }
    return mapping.get(normalized)


def _select_rotowire_prizepicks_line_meta(
    line_entries: object,
    book: str = "prizepicks",
    prefer_non_promo: bool = True,
) -> dict[str, float] | None:
    if not isinstance(line_entries, list):
        return None

    candidates: list[dict[str, object]] = []
    target_book = str(book or "prizepicks").strip().lower()
    for entry in line_entries:
        if not isinstance(entry, dict):
            continue
        entry_book = str(entry.get("book") or "").strip().lower()
        if entry_book != target_book:
            continue
        line_value = pd.to_numeric(pd.Series([entry.get("line")]), errors="coerce").iloc[0]
        if pd.isna(line_value):
            continue
        line_time = pd.to_numeric(pd.Series([entry.get("lineTime")]), errors="coerce").iloc[0]
        candidates.append(
            {
                "line": float(line_value),
                "line_time": float(line_time) if pd.notna(line_time) else -1.0,
                "promo": bool(entry.get("promo", False)),
            }
        )

    if not candidates:
        return None

    usable = candidates
    if prefer_non_promo:
        non_promo = [entry for entry in candidates if not bool(entry.get("promo", False))]
        if non_promo:
            usable = non_promo

    best = max(usable, key=lambda entry: float(entry.get("line_time", -1.0)))
    lines = pd.Series([float(entry.get("line", 0.0)) for entry in usable], dtype=float)
    consensus = float(lines.median()) if not lines.empty else float(best["line"])
    stddev = float(lines.std(ddof=0)) if len(lines) > 1 else 0.0
    books = 1 if usable else 0
    age_minutes = pd.NA
    if float(best.get("line_time", -1.0)) > 0:
        age_minutes = max(0.0, (time.time() - float(best["line_time"])) / 60.0)
    return {
        "line": float(best["line"]),
        "consensus": float(consensus),
        "stddev": float(stddev),
        "books": int(books),
        "age_minutes": float(age_minutes) if pd.notna(age_minutes) else pd.NA,
    }


def _select_rotowire_prizepicks_line(
    line_entries: object,
    book: str = "prizepicks",
    prefer_non_promo: bool = True,
) -> float | None:
    payload = _select_rotowire_prizepicks_line_meta(
        line_entries,
        book=book,
        prefer_non_promo=prefer_non_promo,
    )
    if payload is None:
        return None
    return float(payload["line"])


def _enrich_prop_line_columns(provider_frame: pd.DataFrame) -> pd.DataFrame:
    if provider_frame.empty:
        return provider_frame

    enriched = provider_frame.copy()
    for column in PROP_LINE_COLUMNS:
        if column not in enriched.columns:
            enriched[column] = pd.NA
    for metadata in LINE_CONSENSUS_COLUMNS.values():
        for column in metadata.values():
            if column not in enriched.columns:
                enriched[column] = pd.NA

    derived_pra = (
        pd.to_numeric(enriched["line_points"], errors="coerce")
        + pd.to_numeric(enriched["line_rebounds"], errors="coerce")
        + pd.to_numeric(enriched["line_assists"], errors="coerce")
    )
    enriched["line_pra"] = pd.to_numeric(enriched["line_pra"], errors="coerce").combine_first(derived_pra)

    derived_points_rebounds = pd.to_numeric(enriched["line_points"], errors="coerce") + pd.to_numeric(
        enriched["line_rebounds"], errors="coerce"
    )
    enriched["line_points_rebounds"] = pd.to_numeric(enriched["line_points_rebounds"], errors="coerce").combine_first(
        derived_points_rebounds
    )

    derived_points_assists = pd.to_numeric(enriched["line_points"], errors="coerce") + pd.to_numeric(
        enriched["line_assists"], errors="coerce"
    )
    enriched["line_points_assists"] = pd.to_numeric(enriched["line_points_assists"], errors="coerce").combine_first(
        derived_points_assists
    )

    derived_rebounds_assists = pd.to_numeric(enriched["line_rebounds"], errors="coerce") + pd.to_numeric(
        enriched["line_assists"], errors="coerce"
    )
    enriched["line_rebounds_assists"] = pd.to_numeric(
        enriched["line_rebounds_assists"], errors="coerce"
    ).combine_first(derived_rebounds_assists)

    derived_steals_blocks = pd.to_numeric(enriched["line_steals"], errors="coerce") + pd.to_numeric(
        enriched["line_blocks"], errors="coerce"
    )
    enriched["line_steals_blocks"] = pd.to_numeric(enriched["line_steals_blocks"], errors="coerce").combine_first(
        derived_steals_blocks
    )

    for line_column, metadata in LINE_CONSENSUS_COLUMNS.items():
        consensus_column = metadata["consensus"]
        stddev_column = metadata["stddev"]
        books_column = metadata["books"]
        age_column = metadata["age"]
        line_values = pd.to_numeric(enriched[line_column], errors="coerce")
        enriched[consensus_column] = pd.to_numeric(enriched[consensus_column], errors="coerce").combine_first(line_values)
        enriched[stddev_column] = pd.to_numeric(enriched[stddev_column], errors="coerce").fillna(0.0).clip(lower=0.0)
        enriched[books_column] = pd.to_numeric(enriched[books_column], errors="coerce").fillna(1).clip(lower=0)
        enriched[age_column] = pd.to_numeric(enriched[age_column], errors="coerce")

        movement_meta = LINE_MOVEMENT_COLUMNS.get(line_column, {})
        open_column = movement_meta.get("open")
        close_column = movement_meta.get("close")
        movement_column = movement_meta.get("movement")
        if open_column and close_column and movement_column:
            close_series = (
                pd.to_numeric(enriched[close_column], errors="coerce")
                if close_column in enriched.columns
                else pd.Series(np.nan, index=enriched.index, dtype=float)
            )
            open_series = (
                pd.to_numeric(enriched[open_column], errors="coerce")
                if open_column in enriched.columns
                else pd.Series(np.nan, index=enriched.index, dtype=float)
            )
            movement_series = (
                pd.to_numeric(enriched[movement_column], errors="coerce")
                if movement_column in enriched.columns
                else pd.Series(np.nan, index=enriched.index, dtype=float)
            )
            close_values = close_series.combine_first(
                pd.to_numeric(enriched[consensus_column], errors="coerce")
            ).combine_first(line_values)
            open_values = open_series.combine_first(close_values)
            movement_values = movement_series.combine_first(close_values - open_values)
            enriched[open_column] = open_values
            enriched[close_column] = close_values
            enriched[movement_column] = movement_values

    consensus_pra = (
        pd.to_numeric(enriched["line_points_consensus"], errors="coerce")
        + pd.to_numeric(enriched["line_rebounds_consensus"], errors="coerce")
        + pd.to_numeric(enriched["line_assists_consensus"], errors="coerce")
    )
    enriched["line_pra_consensus"] = pd.to_numeric(enriched["line_pra_consensus"], errors="coerce").combine_first(consensus_pra)
    pra_variance = (
        pd.to_numeric(enriched["line_points_stddev"], errors="coerce").fillna(0.0) ** 2
        + pd.to_numeric(enriched["line_rebounds_stddev"], errors="coerce").fillna(0.0) ** 2
        + pd.to_numeric(enriched["line_assists_stddev"], errors="coerce").fillna(0.0) ** 2
    )
    enriched["line_pra_stddev"] = pd.to_numeric(enriched["line_pra_stddev"], errors="coerce").combine_first(
        pra_variance.pow(0.5)
    )
    enriched["line_pra_books_count"] = pd.to_numeric(enriched["line_pra_books_count"], errors="coerce").combine_first(
        pd.concat(
            [
                pd.to_numeric(enriched["line_points_books_count"], errors="coerce"),
                pd.to_numeric(enriched["line_rebounds_books_count"], errors="coerce"),
                pd.to_numeric(enriched["line_assists_books_count"], errors="coerce"),
            ],
            axis=1,
        ).min(axis=1)
    )
    enriched["line_pra_snapshot_age_minutes"] = pd.to_numeric(
        enriched["line_pra_snapshot_age_minutes"], errors="coerce"
    ).combine_first(
        pd.concat(
            [
                pd.to_numeric(enriched["line_points_snapshot_age_minutes"], errors="coerce"),
                pd.to_numeric(enriched["line_rebounds_snapshot_age_minutes"], errors="coerce"),
                pd.to_numeric(enriched["line_assists_snapshot_age_minutes"], errors="coerce"),
            ],
            axis=1,
        ).max(axis=1)
    )
    return enriched


def _extract_rotowire_prizepicks_rows(
    payload: object,
    book: str = "prizepicks",
    prefer_non_promo: bool = True,
) -> pd.DataFrame:
    if not isinstance(payload, dict):
        return pd.DataFrame(columns=CONTEXT_KEY_COLUMNS + PROP_LINE_CONTEXT_COLUMNS)

    markets = payload.get("markets", [])
    entities = payload.get("entities", [])
    events = payload.get("events", [])
    props = payload.get("props", [])
    if not all(isinstance(value, list) for value in [markets, entities, events, props]):
        return pd.DataFrame(columns=CONTEXT_KEY_COLUMNS + PROP_LINE_CONTEXT_COLUMNS)

    market_to_column: dict[object, str] = {}
    for market in markets:
        if not isinstance(market, dict):
            continue
        market_id = market.get("marketID", market.get("id"))
        sport = str(market.get("sport") or "").strip().upper()
        if sport != "NBA":
            continue
        market_name = market.get("marketName", market.get("name"))
        column = _rotowire_market_column(market_name)
        if market_id is None or not column:
            continue
        market_to_column[market_id] = column

    entity_lookup: dict[object, dict] = {}
    for entity in entities:
        if not isinstance(entity, dict):
            continue
        entity_id = entity.get("entityID", entity.get("id"))
        sport = str(entity.get("sport") or "").strip().upper()
        player_name = str(entity.get("name") or "").strip()
        team_code = _normalize_team_code(entity.get("team"))
        if entity_id is None or sport != "NBA" or not player_name or not team_code:
            continue
        event_id = entity.get("eventID", entity.get("eventId"))
        entity_lookup[entity_id] = {
            "player_name": player_name,
            "team": team_code,
            "event_id": event_id,
        }

    event_lookup: dict[object, dict] = {}
    for event in events:
        if not isinstance(event, dict):
            continue
        event_id = event.get("eventID", event.get("id"))
        if event_id is None:
            continue
        event_lookup[event_id] = event

    aggregated: dict[tuple[str, str, str], dict] = {}
    for prop in props:
        if not isinstance(prop, dict):
            continue
        market_id = prop.get("marketID", prop.get("marketId", prop.get("market")))
        context_column = market_to_column.get(market_id)
        if not context_column:
            continue

        entity_ids = prop.get("entities")
        if isinstance(entity_ids, list):
            resolved_entity_ids = entity_ids
        elif entity_ids is None:
            resolved_entity_ids = []
        else:
            resolved_entity_ids = [entity_ids]
        if len(resolved_entity_ids) != 1:
            continue
        entity_info = entity_lookup.get(resolved_entity_ids[0])
        if not entity_info:
            continue

        event = event_lookup.get(entity_info["event_id"], {})
        event_time_raw = event.get("eventTime", event.get("date"))
        tipoff_utc: datetime | None = None
        if isinstance(event_time_raw, (int, float)) and not pd.isna(event_time_raw):
            tipoff_utc = datetime.fromtimestamp(float(event_time_raw), tz=timezone.utc)
        else:
            tipoff_utc = _parse_iso_datetime(str(event_time_raw or ""))
        game_date = _game_date_from_tipoff(tipoff_utc)
        if not game_date:
            continue

        line_payload = _select_rotowire_prizepicks_line_meta(
            prop.get("lines"),
            book=book,
            prefer_non_promo=prefer_non_promo,
        )
        if line_payload is None:
            continue

        key = (str(entity_info["player_name"]), str(entity_info["team"]), game_date)
        row = aggregated.setdefault(
            key,
            {
                "player_name": str(entity_info["player_name"]),
                "team": str(entity_info["team"]),
                "game_date": game_date,
            },
        )
        row[context_column] = float(line_payload["line"])
        consensus_meta = LINE_CONSENSUS_COLUMNS.get(context_column)
        if consensus_meta:
            row[consensus_meta["consensus"]] = float(line_payload.get("consensus", line_payload["line"]))
            row[consensus_meta["stddev"]] = float(line_payload.get("stddev", 0.0))
            row[consensus_meta["books"]] = int(line_payload.get("books", 1))
            row[consensus_meta["age"]] = (
                float(line_payload["age_minutes"])
                if pd.notna(line_payload.get("age_minutes"))
                else pd.NA
            )

    frame = pd.DataFrame(list(aggregated.values()))
    if frame.empty:
        return pd.DataFrame(columns=CONTEXT_KEY_COLUMNS + PROP_LINE_CONTEXT_COLUMNS)
    frame = _enrich_prop_line_columns(frame)
    frame = frame.drop_duplicates(subset=CONTEXT_KEY_COLUMNS, keep="last")
    return frame


def _compare_prop_line_frames(primary: pd.DataFrame, comparison: pd.DataFrame) -> dict[str, float | int]:
    if primary.empty or comparison.empty:
        return {
            "compared_rows": 0,
            "compared_values": 0,
            "changed_values": 0,
            "median_abs_delta": 0.0,
            "max_abs_delta": 0.0,
        }

    left = primary[CONTEXT_KEY_COLUMNS + [column for column in PROP_LINE_COLUMNS if column in primary.columns]].copy()
    right = comparison[CONTEXT_KEY_COLUMNS + [column for column in PROP_LINE_COLUMNS if column in comparison.columns]].copy()
    merged = left.merge(right, on=CONTEXT_KEY_COLUMNS, how="inner", suffixes=("_base", "_compare"))
    if merged.empty:
        return {
            "compared_rows": 0,
            "compared_values": 0,
            "changed_values": 0,
            "median_abs_delta": 0.0,
            "max_abs_delta": 0.0,
        }

    abs_deltas: list[float] = []
    changed_values = 0
    compared_values = 0
    for column in PROP_LINE_COLUMNS:
        base_column = f"{column}_base"
        compare_column = f"{column}_compare"
        if base_column not in merged.columns or compare_column not in merged.columns:
            continue
        base_series = pd.to_numeric(merged[base_column], errors="coerce")
        compare_series = pd.to_numeric(merged[compare_column], errors="coerce")
        mask = base_series.notna() & compare_series.notna()
        if not mask.any():
            continue
        delta = (compare_series[mask] - base_series[mask]).abs()
        compared_values += int(mask.sum())
        changed_values += int((delta > 1e-9).sum())
        abs_deltas.extend([float(value) for value in delta.tolist() if pd.notna(value)])

    if abs_deltas:
        delta_series = pd.Series(abs_deltas, dtype=float)
        median_abs_delta = float(round(delta_series.median(), 3))
        max_abs_delta = float(round(delta_series.max(), 3))
    else:
        median_abs_delta = 0.0
        max_abs_delta = 0.0

    return {
        "compared_rows": int(len(merged)),
        "compared_values": int(compared_values),
        "changed_values": int(changed_values),
        "median_abs_delta": median_abs_delta,
        "max_abs_delta": max_abs_delta,
    }


def _fetch_rotowire_prizepicks_rows(upcoming_frame: pd.DataFrame, provider_config: dict) -> tuple[pd.DataFrame, dict]:
    status = {
        "enabled": bool(provider_config.get("enabled", True)),
        "rows": 0,
        "records_loaded": 0,
        "props_seen": 0,
        "last_error": None,
        "note": None,
        "source": "RotoWire PrizePicks lines",
        "endpoint": str(provider_config.get("lines_url") or ROTOWIRE_PRIZEPICKS_LINES_URL),
    }
    empty_columns = CONTEXT_KEY_COLUMNS + PROP_LINE_CONTEXT_COLUMNS
    if not status["enabled"]:
        status["note"] = "RotoWire PrizePicks provider is disabled."
        return pd.DataFrame(columns=empty_columns), status
    if upcoming_frame.empty:
        status["note"] = "No upcoming slate rows are available for RotoWire PrizePicks matching."
        return pd.DataFrame(columns=empty_columns), status

    url = str(provider_config.get("lines_url") or ROTOWIRE_PRIZEPICKS_LINES_URL).strip()
    if not url:
        status["note"] = "RotoWire lines_url is missing."
        return pd.DataFrame(columns=empty_columns), status
    request_timeout_seconds = _coerce_positive_int(
        provider_config.get("request_timeout_seconds", DEFAULT_PROVIDER_REQUEST_TIMEOUT_SECONDS),
        DEFAULT_PROVIDER_REQUEST_TIMEOUT_SECONDS,
    )

    try:
        payload = fetch_json(
            url,
            timeout=request_timeout_seconds,
            headers={
                "Accept": "application/json, text/plain, */*",
                "Referer": str(provider_config.get("referer") or "https://www.rotowire.com/picks/prizepicks/"),
            },
        )
        if isinstance(payload, dict):
            status["props_seen"] = int(len(payload.get("props", []) or []))

        raw_frame = _extract_rotowire_prizepicks_rows(
            payload,
            book=str(provider_config.get("book") or "prizepicks"),
            prefer_non_promo=bool(provider_config.get("prefer_non_promo_lines", True)),
        )
        status["records_loaded"] = int(len(raw_frame))
        if raw_frame.empty:
            status["note"] = "RotoWire payload loaded, but no NBA PrizePicks rows were parsed."
            return pd.DataFrame(columns=empty_columns), status

        aligned = _align_provider_rows_to_upcoming(upcoming_frame, raw_frame)
        status["rows"] = int(len(aligned))
        if aligned.empty:
            status["note"] = "RotoWire PrizePicks rows were parsed but did not align to the current slate."
        return aligned, status
    except (HTTPError, URLError, ValueError, OSError, subprocess.SubprocessError) as exc:
        status["last_error"] = _sanitize_error_message(exc)
        return pd.DataFrame(columns=empty_columns), status


def _extract_player_prop_rows(event_payload: dict) -> list[dict]:
    records: dict[tuple[str, str, str], dict[str, object]] = {}
    now_utc = datetime.now(timezone.utc)
    event_last_update = _parse_iso_datetime(event_payload.get("last_update"))

    for bookmaker in event_payload.get("bookmakers", []):
        book_key = str(bookmaker.get("key") or bookmaker.get("title") or "").strip().lower() or "unknown"
        book_last_update = _parse_iso_datetime(bookmaker.get("last_update")) or event_last_update
        for market in bookmaker.get("markets", []):
            context_column = _player_props_market_column(market.get("key"))
            if not context_column:
                continue
            market_last_update = _parse_iso_datetime(market.get("last_update")) or book_last_update
            for outcome in market.get("outcomes", []):
                point_value = pd.to_numeric(pd.Series([outcome.get("point")]), errors="coerce").iloc[0]
                if pd.isna(point_value):
                    continue
                player_name = str(outcome.get("description") or "").strip()
                outcome_name = str(outcome.get("name") or "").strip()
                if not player_name and outcome_name and outcome_name.lower() not in {"over", "under"}:
                    player_name = outcome_name
                if not player_name:
                    continue
                player_key = _normalize_player_key(player_name)
                if not player_key:
                    continue
                row_key = (player_key, player_name, context_column)
                entry = records.setdefault(
                    row_key,
                    {
                        "lines": [],
                        "books": set(),
                        "latest_update": None,
                    },
                )
                entry["lines"].append(float(point_value))
                entry["books"].add(book_key)

                outcome_last_update = _parse_iso_datetime(outcome.get("last_update")) or market_last_update
                if outcome_last_update is not None:
                    current_latest = entry.get("latest_update")
                    if current_latest is None or outcome_last_update > current_latest:
                        entry["latest_update"] = outcome_last_update

    rows: list[dict] = []
    for (player_key, player_name, context_column), payload in records.items():
        values = [float(value) for value in payload.get("lines", [])]
        if not values:
            continue
        line_series = pd.Series(values, dtype=float)
        consensus_line = float(line_series.median())
        stddev = float(line_series.std(ddof=0)) if len(values) > 1 else 0.0
        books_count = int(len(payload.get("books", set())))
        latest_update = payload.get("latest_update")
        age_minutes = pd.NA
        if isinstance(latest_update, datetime):
            age_minutes = max(0.0, (now_utc - latest_update).total_seconds() / 60.0)

        row = {
            "player_key": player_key,
            "player_name": player_name,
            context_column: round(consensus_line, 3),
        }
        consensus_meta = LINE_CONSENSUS_COLUMNS.get(context_column)
        if consensus_meta:
            row[consensus_meta["consensus"]] = round(consensus_line, 3)
            row[consensus_meta["stddev"]] = round(stddev, 4)
            row[consensus_meta["books"]] = books_count
            row[consensus_meta["age"]] = round(float(age_minutes), 3) if pd.notna(age_minutes) else pd.NA
        rows.append(row)
    return rows


def _fetch_odds_player_props_rows(
    upcoming_frame: pd.DataFrame,
    provider_config: dict,
    events: list[dict] | None = None,
) -> tuple[pd.DataFrame, dict]:
    status = {
        "enabled": bool(provider_config.get("enabled", True)),
        "rows": 0,
        "events_requested": 0,
        "events_matched": 0,
        "events_with_props": 0,
        "api_key_present": False,
        "last_error": None,
        "note": None,
        "source": "The Odds API player props",
    }
    empty_columns = CONTEXT_KEY_COLUMNS + PROP_LINE_CONTEXT_COLUMNS
    if not status["enabled"]:
        status["note"] = "Player props provider is disabled."
        return pd.DataFrame(columns=empty_columns), status
    if upcoming_frame.empty:
        status["note"] = "No upcoming slate rows are available for player-prop matching."
        return pd.DataFrame(columns=empty_columns), status

    api_key = os.getenv(provider_config.get("api_key_env", "ODDS_API_KEY"), "").strip()
    status["api_key_present"] = bool(api_key)
    if not api_key:
        status["note"] = "Set ODDS_API_KEY to load player prop market context."
        return pd.DataFrame(columns=empty_columns), status
    request_timeout_seconds = _coerce_positive_int(
        provider_config.get("request_timeout_seconds", DEFAULT_PROVIDER_REQUEST_TIMEOUT_SECONDS),
        DEFAULT_PROVIDER_REQUEST_TIMEOUT_SECONDS,
    )
    max_events_per_cycle = _coerce_positive_int(provider_config.get("max_events_per_cycle", 8), 8)

    try:
        odds_events = events
        if odds_events is None:
            lookahead_hours = _coerce_positive_int(
                provider_config.get("lookahead_hours", DEFAULT_LIVE_CONFIG["pregame_slate_lookahead_hours"]),
                DEFAULT_LIVE_CONFIG["pregame_slate_lookahead_hours"],
            )
            odds_events, _ = _fetch_odds_events(provider_config, lookahead_hours)

        if not odds_events:
            status["note"] = "No upcoming NBA odds events were available for player props."
            return pd.DataFrame(columns=empty_columns), status

        working = upcoming_frame.copy()
        working["game_date"] = pd.to_datetime(working["game_date"], errors="coerce").dt.strftime("%Y-%m-%d")
        working["team"] = working["team"].map(_normalize_team_code).fillna(working["team"])
        working["opponent"] = working["opponent"].map(_normalize_team_code).fillna(working["opponent"])
        working["home_numeric"] = pd.to_numeric(working.get("home"), errors="coerce").fillna(0).astype(int)
        working["home_team"] = working.apply(
            lambda row: row["team"] if int(row.get("home_numeric", 0)) == 1 else row["opponent"],
            axis=1,
        )
        working["away_team"] = working.apply(
            lambda row: row["opponent"] if int(row.get("home_numeric", 0)) == 1 else row["team"],
            axis=1,
        )
        working["player_key"] = working["player_name"].map(_normalize_player_key)

        game_player_lookup: dict[tuple[str, str, str], dict[str, tuple[str, str]]] = {}
        for _, row in working.iterrows():
            key = (str(row["game_date"]), str(row["home_team"]), str(row["away_team"]))
            game_player_lookup.setdefault(key, {})
            player_key = str(row["player_key"] or "")
            if player_key and player_key not in game_player_lookup[key]:
                game_player_lookup[key][player_key] = (str(row["player_name"]), str(row["team"]))

        prop_rows: list[dict] = []
        base_url = str(provider_config.get("base_url", "https://api.the-odds-api.com/v4") or "").rstrip("/")
        sport = str(provider_config.get("sport", "basketball_nba") or "basketball_nba")
        markets = str(
            provider_config.get(
                "markets",
                (
                    "player_points,player_rebounds,player_assists,player_points_rebounds_assists,"
                    "player_points_rebounds,player_points_assists,player_rebounds_assists,"
                    "player_steals,player_blocks,player_turnovers,player_steals_blocks,"
                    "player_threes,player_three_points_made"
                ),
            )
            or ""
        ).strip()
        fallback_markets = "player_points,player_rebounds,player_assists,player_points_rebounds_assists,player_threes"
        market_candidates = [markets] if markets == fallback_markets else [markets, fallback_markets]
        regions = str(provider_config.get("regions", "us") or "us")
        bookmakers = str(provider_config.get("bookmakers", "draftkings,fanduel") or "draftkings,fanduel")
        odds_format = str(provider_config.get("odds_format", "american") or "american")
        date_format = str(provider_config.get("date_format", "iso") or "iso")

        for index, event in enumerate(odds_events):
            if max_events_per_cycle > 0 and index >= max_events_per_cycle:
                break
            event_id = str(event.get("id") or "").strip()
            tipoff_utc = _parse_iso_datetime(event.get("commence_time"))
            game_date = _game_date_from_tipoff(tipoff_utc)
            home_code = _normalize_team_code(event.get("home_team"))
            away_code = _normalize_team_code(event.get("away_team"))
            if not event_id or not game_date or not home_code or not away_code:
                continue

            lookup_key = (game_date, home_code, away_code)
            roster_lookup = game_player_lookup.get(lookup_key)
            if not roster_lookup:
                continue
            status["events_matched"] += 1

            status["events_requested"] += 1
            event_payload = None
            for market_choice in market_candidates:
                query = urlencode(
                    {
                        "apiKey": api_key,
                        "regions": regions,
                        "markets": market_choice,
                        "bookmakers": bookmakers,
                        "oddsFormat": odds_format,
                        "dateFormat": date_format,
                    }
                )
                try:
                    event_payload = fetch_json(
                        f"{base_url}/sports/{sport}/events/{event_id}/odds/?{query}",
                        timeout=request_timeout_seconds,
                    )
                    break
                except (HTTPError, URLError, ValueError, OSError, subprocess.SubprocessError):
                    continue
            if event_payload is None:
                continue
            extracted_rows = _extract_player_prop_rows(event_payload)
            if extracted_rows:
                status["events_with_props"] += 1
            for row in extracted_rows:
                player_key = str(row.get("player_key") or "")
                if not player_key:
                    continue
                roster_entry = roster_lookup.get(player_key)
                if not roster_entry:
                    continue
                player_name, player_team = roster_entry
                provider_row = {
                    "player_name": player_name,
                    "team": player_team,
                    "game_date": game_date,
                }
                for market_column in PROP_LINE_CONTEXT_COLUMNS:
                    if market_column in row:
                        provider_row[market_column] = row[market_column]
                prop_rows.append(provider_row)

        provider_frame = pd.DataFrame(prop_rows)
        if provider_frame.empty:
            status["note"] = "No player-prop rows were matched to players in the current slate."
            return pd.DataFrame(columns=empty_columns), status
        provider_frame = _enrich_prop_line_columns(provider_frame)
        provider_frame = provider_frame.drop_duplicates(subset=CONTEXT_KEY_COLUMNS, keep="last")
        aligned = _align_provider_rows_to_upcoming(upcoming_frame, provider_frame)
        status["rows"] = int(len(aligned))
        if aligned.empty:
            status["note"] = "Player-prop events were fetched but could not be aligned to current upcoming rows."
        return aligned, status
    except (HTTPError, URLError, ValueError, OSError, subprocess.SubprocessError) as exc:
        status["last_error"] = _sanitize_error_message(exc)
        return pd.DataFrame(columns=empty_columns), status


def _fetch_nba_daily_lineups_rows(upcoming_frame: pd.DataFrame, provider_config: dict) -> tuple[pd.DataFrame, dict]:
    status = {
        "enabled": bool(provider_config.get("enabled", True)),
        "rows": 0,
        "dates_requested": [],
        "dates_loaded": [],
        "records_loaded": 0,
        "last_error": None,
        "note": None,
        "source": "NBA daily lineups feed",
    }
    empty_columns = CONTEXT_KEY_COLUMNS + [
        "position",
        "starter",
        "starter_probability",
        "starter_certainty",
        "lineup_status_label",
        "lineup_status_confidence",
        "injury_status",
        "health_status",
    ]
    if not status["enabled"]:
        status["note"] = "Lineups provider is disabled."
        return pd.DataFrame(columns=empty_columns), status
    if upcoming_frame.empty:
        status["note"] = "No upcoming slate rows are available for lineup matching."
        return pd.DataFrame(columns=empty_columns), status

    include_expected = bool(provider_config.get("include_expected_as_starters", True))
    include_confirmed = bool(provider_config.get("include_confirmed_as_starters", True))
    request_timeout_seconds = _coerce_positive_int(
        provider_config.get("request_timeout_seconds", DEFAULT_PROVIDER_REQUEST_TIMEOUT_SECONDS),
        DEFAULT_PROVIDER_REQUEST_TIMEOUT_SECONDS,
    )
    max_dates_per_cycle = _coerce_positive_int(provider_config.get("max_dates_per_cycle", 2), 2)
    template = str(provider_config.get("url_template", DAILY_LINEUPS_URL_TEMPLATE) or DAILY_LINEUPS_URL_TEMPLATE)

    game_dates = sorted(
        {
            str(value)
            for value in pd.to_datetime(upcoming_frame["game_date"], errors="coerce")
            .dropna()
            .dt.strftime("%Y-%m-%d")
            .unique()
            .tolist()
        }
    )
    if not game_dates:
        status["note"] = "Upcoming slate has no valid game_date values."
        return pd.DataFrame(columns=empty_columns), status
    if max_dates_per_cycle > 0:
        game_dates = game_dates[:max_dates_per_cycle]

    provider_rows: list[dict] = []
    for game_date in game_dates:
        yyyymmdd = game_date.replace("-", "")
        status["dates_requested"].append(game_date)
        url = template.format(yyyymmdd=yyyymmdd, game_date=game_date)
        try:
            payload = fetch_json(
                url,
                timeout=request_timeout_seconds,
                headers={
                    "Accept": "application/json, text/plain, */*",
                    "Referer": "https://www.nba.com/",
                },
            )
        except (HTTPError, URLError, ValueError, OSError, subprocess.SubprocessError) as exc:
            status["last_error"] = _sanitize_error_message(exc)
            continue

        games = payload.get("games", []) if isinstance(payload, dict) else []
        if not isinstance(games, list):
            continue
        status["dates_loaded"].append(game_date)

        for game in games:
            for side in ("homeTeam", "awayTeam"):
                team_payload = game.get(side, {}) or {}
                team_code = _normalize_team_code(team_payload.get("teamAbbreviation"))
                if not team_code:
                    continue
                for player in team_payload.get("players", []) or []:
                    player_name = str(player.get("playerName") or "").strip()
                    if not player_name:
                        continue

                    lineup_status = str(player.get("lineupStatus") or "").strip().lower()
                    roster_status = str(player.get("rosterStatus") or "").strip().lower()
                    position = str(player.get("position") or "").strip().upper()

                    starter_value = None
                    if position:
                        starter_value = 1
                    elif include_confirmed and lineup_status == "confirmed":
                        starter_value = 1
                    elif include_expected and lineup_status == "expected":
                        starter_value = 1
                    elif lineup_status:
                        starter_value = 0

                    row = {
                        "player_name": player_name,
                        "team": team_code,
                        "game_date": game_date,
                    }
                    starter_probability = None
                    lineup_confidence = None
                    lineup_label = None
                    if lineup_status == "confirmed":
                        starter_probability = 0.95
                        lineup_confidence = 0.98
                        lineup_label = "confirmed"
                    elif lineup_status == "expected":
                        starter_probability = 0.78
                        lineup_confidence = 0.86
                        lineup_label = "expected"
                    elif starter_value == 1:
                        starter_probability = 0.82
                        lineup_confidence = 0.74
                        lineup_label = "inferred_starter"
                    elif starter_value == 0:
                        starter_probability = 0.12
                        lineup_confidence = 0.72
                        lineup_label = "inferred_bench"
                    if position:
                        row["position"] = position
                    if starter_value is not None:
                        row["starter"] = int(starter_value)
                    if starter_probability is not None:
                        row["starter_probability"] = float(starter_probability)
                        row["starter_certainty"] = float(max(0.0, min(1.0, starter_probability)))
                    if lineup_confidence is not None:
                        row["lineup_status_confidence"] = float(max(0.0, min(1.0, lineup_confidence)))
                    if lineup_label:
                        row["lineup_status_label"] = lineup_label
                    if roster_status and roster_status != "active":
                        row["injury_status"] = roster_status.title()
                        if roster_status in {"out", "inactive"}:
                            row["starter_probability"] = 0.0
                            row["starter_certainty"] = 0.99
                            row["lineup_status_confidence"] = 0.99
                            row["lineup_status_label"] = "inactive"
                    if lineup_status:
                        row["health_status"] = f"Lineup {lineup_status.title()}"
                    provider_rows.append(row)

    raw_frame = pd.DataFrame(provider_rows)
    status["records_loaded"] = int(len(raw_frame))
    if raw_frame.empty:
        if not status.get("last_error"):
            status["note"] = "Daily lineup feed returned no rows for current slate dates."
        return pd.DataFrame(columns=empty_columns), status

    aligned = _align_provider_rows_to_upcoming(upcoming_frame, raw_frame)
    status["rows"] = int(len(aligned))
    if aligned.empty and not status.get("note"):
        status["note"] = "Daily lineups loaded but player rows did not align to the current slate."
    return aligned, status


def _boxscore_live_roster_rows(game_payload: dict) -> pd.DataFrame:
    game = (game_payload or {}).get("game", {}) or {}
    tipoff_utc = _parse_iso_datetime(game.get("gameEt") or game.get("gameTimeUTC") or game.get("gameDateTimeUTC"))
    game_date = _game_date_from_tipoff(tipoff_utc) or str(game.get("gameEt") or game.get("gameTimeUTC") or "")[:10]
    rows: list[dict] = []

    def collect_team_rows(team_payload: dict) -> None:
        team_code = _normalize_team_code(team_payload.get("teamTricode"))
        if not team_code:
            return
        for player in team_payload.get("players", []) or []:
            player_name = str(player.get("name") or "").strip()
            if not player_name:
                continue
            position = str(player.get("position") or "").strip().upper()
            played_flag = str(player.get("played") or "").strip()
            starter_flag = 1 if str(player.get("starter") or "").strip() == "1" else 0
            status_hint = str(
                player.get("status")
                or player.get("statusComment")
                or player.get("notPlayingReason")
                or ""
            ).strip()

            injury_multiplier = 1.0
            if status_hint:
                injury_multiplier = _injury_minutes_multiplier_from_status_text(status_hint)
            elif played_flag == "0":
                injury_multiplier = 0.9

            starter_probability = 0.9 if starter_flag == 1 else 0.22
            if status_hint and injury_multiplier <= 0.82:
                starter_probability = min(starter_probability, 0.18)
            if played_flag == "1" and starter_flag == 0:
                starter_probability = max(starter_probability, 0.35)

            row = {
                "player_name": player_name,
                "team": team_code,
                "game_date": game_date,
                "starter": starter_flag,
                "starter_probability": float(max(0.0, min(1.0, starter_probability))),
                "starter_certainty": float(0.99 if starter_flag == 1 else 0.84),
                "lineup_status_label": "live_starter" if starter_flag == 1 else "live_active",
                "lineup_status_confidence": float(0.99 if starter_flag == 1 else 0.82),
                "injury_minutes_multiplier": float(max(0.0, min(1.0, injury_multiplier))),
                "health_status": "Live Roster Feed",
            }
            if position:
                row["position"] = position
            if status_hint:
                row["injury_status"] = status_hint[:80]
                row["injury_risk_score"] = _injury_risk_score_from_status_text(status_hint)
            rows.append(row)

    collect_team_rows(game.get("homeTeam", {}) or {})
    collect_team_rows(game.get("awayTeam", {}) or {})
    return pd.DataFrame(rows)


def _fetch_live_roster_rows(
    scoreboard_payload: dict,
    upcoming_frame: pd.DataFrame,
    provider_config: dict,
) -> tuple[pd.DataFrame, dict]:
    status = {
        "enabled": bool(provider_config.get("enabled", True)),
        "source": "NBA live boxscore roster",
        "games_seen": 0,
        "games_loaded": 0,
        "rows_raw": 0,
        "rows": 0,
        "last_error": None,
        "note": None,
    }
    empty_columns = CONTEXT_KEY_COLUMNS + [
        "position",
        "starter",
        "starter_probability",
        "injury_status",
        "health_status",
        "injury_risk_score",
        "injury_minutes_multiplier",
    ]
    if not status["enabled"]:
        status["note"] = "Live roster provider is disabled."
        return pd.DataFrame(columns=empty_columns), status
    if upcoming_frame.empty:
        status["note"] = "No upcoming/live slate rows are available for live roster matching."
        return pd.DataFrame(columns=empty_columns), status

    games = ((scoreboard_payload or {}).get("scoreboard", {}) or {}).get("games", []) or []
    live_games = [game for game in games if int((game or {}).get("gameStatus", 0)) == 2]
    status["games_seen"] = int(len(live_games))
    if not live_games:
        status["note"] = "No live NBA games are active in this sync cycle."
        return pd.DataFrame(columns=empty_columns), status

    raw_frames: list[pd.DataFrame] = []
    for game in live_games:
        game_id = str(game.get("gameId") or "").strip()
        if not game_id:
            continue
        try:
            payload = fetch_boxscore(game_id)
        except (HTTPError, URLError, ValueError, OSError, subprocess.SubprocessError) as exc:
            status["last_error"] = _sanitize_error_message(exc)
            continue
        raw = _boxscore_live_roster_rows(payload)
        if raw.empty:
            continue
        raw_frames.append(raw)
        status["games_loaded"] = int(status.get("games_loaded", 0) + 1)

    if not raw_frames:
        if not status.get("last_error"):
            status["note"] = "Live boxscore feeds were reachable, but no roster rows were parsed."
        return pd.DataFrame(columns=empty_columns), status

    raw_frame = pd.concat(raw_frames, ignore_index=True, sort=False).drop_duplicates(
        subset=CONTEXT_KEY_COLUMNS,
        keep="last",
    )
    status["rows_raw"] = int(len(raw_frame))
    aligned = _align_provider_rows_to_upcoming(upcoming_frame, raw_frame)
    status["rows"] = int(len(aligned))
    if aligned.empty and not status.get("note"):
        status["note"] = "Live roster rows loaded but did not align to current upcoming/live rows."
    return aligned, status


def _extract_latest_injury_report(provider_config: dict) -> dict:
    page_url = provider_config.get("official_report_page", "").strip()
    if not page_url:
        return {"page_url": None, "latest_report_url": None, "report_count": 0}

    timeout_seconds = _coerce_positive_int(provider_config.get("request_timeout_seconds", 10), 10)
    html = fetch_text(
        page_url,
        timeout=timeout_seconds,
        headers={"Accept": "text/html,application/xhtml+xml"},
    )
    hrefs = re.findall(r'href=["\']([^"\']*Injury-Report[^"\']*\.pdf)["\']', html, flags=re.IGNORECASE)
    urls = [urljoin(page_url, href) for href in hrefs]
    latest_report_url = sorted(urls)[-1] if urls else None
    return {
        "page_url": page_url,
        "latest_report_url": latest_report_url,
        "report_count": len(urls),
    }


def _official_injury_line_team(line: str) -> tuple[str | None, str]:
    for team_code, team_name in TEAM_FULL_NAMES_BY_CODE.items():
        if team_name in line:
            return team_code, line.replace(team_name, " ").strip()
    return None, line.strip()


def _official_injury_player_name(raw_name: str) -> str:
    text = re.sub(r"\s+", " ", str(raw_name or "")).strip().strip(",")
    if not text:
        return ""
    text = re.sub(r"\b\d{1,2}:\d{2}\s+\(ET\)\b", " ", text, flags=re.IGNORECASE)
    text = re.sub(r"\b[A-Z]{2,3}@[A-Z]{2,3}\b", " ", text)
    text = re.sub(r"\bNOT YET SUBMITTED\b", " ", text, flags=re.IGNORECASE)
    text = re.sub(r"\bPage\s*\d+\s*of\s*\d+\b", " ", text, flags=re.IGNORECASE)
    for team_name in TEAM_FULL_NAMES_BY_CODE.values():
        text = text.replace(team_name, " ")
    text = re.sub(r"\s+", " ", text).strip().strip(",")
    if "," in text:
        last, first = [part.strip() for part in text.split(",", 1)]
        last = re.sub(r"([A-Za-z])((?:II|III|IV|Jr|Sr)\b)", r"\1 \2", last)
        return re.sub(r"\s+", " ", f"{first} {last}").strip()
    return text


def _parse_official_injury_report_rows(report_url: str) -> pd.DataFrame:
    if not report_url:
        return pd.DataFrame(columns=CONTEXT_KEY_COLUMNS + OPTIONAL_CONTEXT_COLUMNS)

    reader = PdfReader(io.BytesIO(fetch_binary(report_url)))
    status_pattern = re.compile(r"\b(Out|Questionable|Probable|Doubtful|Available|Suspended)\b", flags=re.IGNORECASE)
    rows: list[dict] = []
    current_team: str | None = None
    current_date: str | None = None

    for page in reader.pages:
        text = page.extract_text(extraction_mode="layout") or ""
        lines = [re.sub(r"\s+", " ", line).strip() for line in text.splitlines() if line.strip()]
        for line in reversed(lines):
            if any(token in line for token in ["Injury Report:", "Game Date", "Page "]):
                continue

            date_match = re.search(r"(\d{2}/\d{2}/\d{4})", line)
            if date_match:
                current_date = pd.to_datetime(date_match.group(1), format="%m/%d/%Y", errors="coerce")
                current_date = current_date.strftime("%Y-%m-%d") if pd.notna(current_date) else current_date

            status_match = status_pattern.search(line)
            if not status_match:
                if rows and line and not re.match(r"^\d{2}/\d{2}/\d{4}", line):
                    previous_value = rows[-1].get("health_status", "")
                    previous_reason = "" if pd.isna(previous_value) else str(previous_value).strip()
                    rows[-1]["health_status"] = re.sub(r"\s+", " ", f"{previous_reason} {line}".strip())
                continue

            status = status_match.group(1).title()
            before_status = line[: status_match.start()].strip()
            after_status = line[status_match.end() :].strip()
            team_code, before_status = _official_injury_line_team(before_status)
            if team_code:
                current_team = team_code

            before_status = re.sub(r"^\d{2}/\d{2}/\d{4}\s+\d{2}:\d{2}\s+\(ET\)\s+[A-Z]{2,3}@[A-Z]{2,3}\s*", "", before_status).strip()
            player_name = _official_injury_player_name(before_status)
            if not player_name:
                continue

            rows.append(
                {
                    "player_name": player_name,
                    "game_date": current_date,
                    "team": current_team,
                    "injury_status": status,
                    "health_status": after_status or "",
                    "injury_risk_score": _injury_risk_score_from_status_text(status),
                    "injury_minutes_multiplier": _injury_minutes_multiplier_from_status_text(status),
                }
            )

    return pd.DataFrame(rows)


def _resolve_provider_url(provider_config: dict, direct_key: str, env_key_key: str) -> str:
    direct = str(provider_config.get(direct_key, "") or "").strip()
    if direct:
        return direct
    env_name = str(provider_config.get(env_key_key, "") or "").strip()
    if env_name:
        return os.getenv(env_name, "").strip()
    return ""


def _extract_json_records(payload: object, records_path: str) -> list[dict]:
    current = payload
    if records_path:
        for token in [part for part in records_path.split(".") if part]:
            if isinstance(current, dict):
                current = current.get(token)
            elif isinstance(current, list) and token.isdigit():
                index = int(token)
                current = current[index] if 0 <= index < len(current) else None
            else:
                current = None
            if current is None:
                return []

    if isinstance(current, list):
        return [item for item in current if isinstance(item, dict)]
    if isinstance(current, dict):
        for key in ["data", "results", "items", "players", "injuries"]:
            value = current.get(key)
            if isinstance(value, list):
                return [item for item in value if isinstance(item, dict)]
    return []


def _height_to_inches(value: object) -> int | None:
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return None
    text = str(value).strip()
    if not text:
        return None
    match = re.match(r"^\s*(\d+)\s*[-']\s*(\d+)\s*$", text)
    if match:
        return int(match.group(1)) * 12 + int(match.group(2))
    numeric = pd.to_numeric(text, errors="coerce")
    if pd.notna(numeric):
        return int(numeric)
    return None


def _normalize_balldontlie_injury_records(records: list[dict]) -> pd.DataFrame:
    rows: list[dict] = []
    for record in records:
        player = record.get("player", {}) or {}
        first_name = str(player.get("first_name", "") or "").strip()
        last_name = str(player.get("last_name", "") or "").strip()
        player_name = " ".join(part for part in [first_name, last_name] if part).strip()
        if not player_name:
            player_name = str(record.get("player_name", "") or "").strip()
        if not player_name:
            continue

        team_code = _normalize_team_code(player.get("team"))
        if not team_code:
            team_code = _normalize_team_code(record.get("team"))
        if not team_code:
            team_id = player.get("team_id") or record.get("team_id")
            if team_id is not None:
                try:
                    inverse_lookup = {value: key for key, value in TEAM_ID_BY_TRICODE.items()}
                    team_code = inverse_lookup.get(int(team_id))
                except (TypeError, ValueError):
                    team_code = None

        rows.append(
            {
                "player_name": player_name,
                "team": team_code,
                "position": player.get("position"),
                "height_inches": _height_to_inches(player.get("height")),
                "weight_lbs": pd.to_numeric(player.get("weight"), errors="coerce"),
                "injury_status": record.get("status"),
                "injury_risk_score": _injury_risk_score_from_status_text(str(record.get("status") or "")),
                "injury_minutes_multiplier": _injury_minutes_multiplier_from_status_text(str(record.get("status") or "")),
            }
        )

    return pd.DataFrame(rows)


def _standardize_provider_frame(frame: pd.DataFrame) -> pd.DataFrame:
    if frame.empty:
        return frame

    rename_map = {}
    for column in frame.columns:
        normalized = _normalize_provider_column_name(column)
        mapped = PROVIDER_COLUMN_ALIASES.get(normalized)
        if mapped:
            rename_map[column] = mapped

    standardized = frame.rename(columns=rename_map).copy()
    standardized = standardized.loc[:, ~standardized.columns.duplicated()].copy()

    useful_columns: list[str] = []
    for column in CONTEXT_KEY_COLUMNS + OPTIONAL_CONTEXT_COLUMNS + ["home", "opponent"]:
        if column in standardized.columns and column not in useful_columns:
            useful_columns.append(column)
    if not useful_columns:
        return pd.DataFrame()
    standardized = standardized[useful_columns].copy()

    if "team" in standardized.columns:
        standardized["team"] = standardized["team"].map(_normalize_team_code).fillna(standardized["team"])
    if "opponent" in standardized.columns:
        standardized["opponent"] = standardized["opponent"].map(_normalize_team_code).fillna(standardized["opponent"])
    if "game_date" in standardized.columns:
        standardized["game_date"] = pd.to_datetime(standardized["game_date"], errors="coerce").dt.strftime("%Y-%m-%d")
    if "home" in standardized.columns:
        standardized["home"] = pd.to_numeric(standardized["home"], errors="coerce")

    status_text = (
        standardized.get("injury_status", pd.Series("", index=standardized.index)).fillna("").astype(str)
        + " "
        + standardized.get("health_status", pd.Series("", index=standardized.index)).fillna("").astype(str)
        + " "
        + standardized.get("suspension_status", pd.Series("", index=standardized.index)).fillna("").astype(str)
    ).str.lower().str.strip()

    derived_multiplier = status_text.map(_injury_minutes_multiplier_from_status_text)
    derived_risk = status_text.map(_injury_risk_score_from_status_text)
    if "injury_minutes_multiplier" in standardized.columns:
        standardized["injury_minutes_multiplier"] = (
            pd.to_numeric(standardized["injury_minutes_multiplier"], errors="coerce")
            .combine_first(derived_multiplier)
            .clip(lower=0.0, upper=1.0)
        )
    else:
        standardized["injury_minutes_multiplier"] = derived_multiplier

    if "injury_risk_score" in standardized.columns:
        standardized["injury_risk_score"] = (
            pd.to_numeric(standardized["injury_risk_score"], errors="coerce")
            .combine_first(derived_risk)
            .clip(lower=0.0, upper=1.0)
        )
    else:
        standardized["injury_risk_score"] = derived_risk

    starter_series = (
        standardized["starter"]
        if "starter" in standardized.columns
        else pd.Series(pd.NA, index=standardized.index, dtype="object")
    )
    starter_numeric = pd.to_numeric(starter_series, errors="coerce")
    derived_starter_probability = starter_numeric.map(
        lambda value: 0.9 if pd.notna(value) and float(value) >= 0.5 else (0.1 if pd.notna(value) else pd.NA)
    )
    if "starter_probability" in standardized.columns:
        standardized["starter_probability"] = (
            pd.to_numeric(standardized["starter_probability"], errors="coerce")
            .combine_first(derived_starter_probability)
            .clip(lower=0.0, upper=1.0)
        )
    else:
        standardized["starter_probability"] = pd.to_numeric(derived_starter_probability, errors="coerce")

    update_columns = [column for column in standardized.columns if column not in CONTEXT_KEY_COLUMNS and column in OPTIONAL_CONTEXT_COLUMNS]
    if not update_columns:
        return pd.DataFrame()

    return standardized


def _align_provider_rows_to_upcoming(upcoming_frame: pd.DataFrame, provider_frame: pd.DataFrame) -> pd.DataFrame:
    if upcoming_frame.empty or provider_frame.empty or "player_name" not in provider_frame.columns:
        return pd.DataFrame(columns=CONTEXT_KEY_COLUMNS)

    upcoming_keys = upcoming_frame[CONTEXT_KEY_COLUMNS].drop_duplicates().copy()
    upcoming_keys["__player_key"] = upcoming_keys["player_name"].map(_normalize_player_key)
    upcoming_keys["__team_key"] = upcoming_keys["team"].map(_normalize_team_code)

    provider_rows = provider_frame.loc[:, ~provider_frame.columns.duplicated()].copy()
    provider_rows["__player_key"] = provider_rows["player_name"].map(_normalize_player_key)

    update_columns = [
        column for column in provider_rows.columns if column in OPTIONAL_CONTEXT_COLUMNS and column not in CONTEXT_KEY_COLUMNS
    ]
    if not update_columns:
        return pd.DataFrame(columns=CONTEXT_KEY_COLUMNS)

    if "team" in provider_rows.columns and provider_rows["team"].notna().any():
        provider_rows["__team_key"] = provider_rows["team"].map(_normalize_team_code)
    if "game_date" in provider_rows.columns:
        provider_rows["game_date"] = pd.to_datetime(provider_rows["game_date"], errors="coerce").dt.strftime("%Y-%m-%d")

    candidate_key_sets = []
    if "__team_key" in provider_rows.columns and provider_rows["__team_key"].notna().any():
        if "game_date" in provider_rows.columns and provider_rows["game_date"].notna().any():
            candidate_key_sets.append(["__player_key", "__team_key", "game_date"])
        candidate_key_sets.append(["__player_key", "__team_key"])
    if "game_date" in provider_rows.columns and provider_rows["game_date"].notna().any():
        candidate_key_sets.append(["__player_key", "game_date"])
    candidate_key_sets.append(["__player_key"])

    for join_keys in candidate_key_sets:
        subset_columns = list(dict.fromkeys(join_keys + update_columns))
        joinable = provider_rows.drop_duplicates(subset=join_keys, keep="last")
        merged = upcoming_keys.merge(joinable[subset_columns], on=join_keys, how="left")
        matched = merged[update_columns].notna().any(axis=1)
        if matched.any():
            return merged.loc[matched, CONTEXT_KEY_COLUMNS + update_columns].copy()

    return pd.DataFrame(columns=CONTEXT_KEY_COLUMNS + update_columns)


def _fetch_balldontlie_injury_rows(upcoming_frame: pd.DataFrame, provider_config: dict) -> tuple[pd.DataFrame, dict]:
    api_key = os.getenv(provider_config.get("api_key_env", "BALLDONTLIE_API_KEY"), "").strip()
    status = {
        "source": "BALDONTLIE player_injuries",
        "api_key_present": bool(api_key),
        "records_loaded": 0,
        "rows": 0,
        "last_error": None,
        "note": None,
    }
    if not api_key:
        status["note"] = "Set BALLDONTLIE_API_KEY to load structured player injuries automatically."
        return pd.DataFrame(columns=CONTEXT_KEY_COLUMNS + OPTIONAL_CONTEXT_COLUMNS), status
    timeout_seconds = _coerce_positive_int(provider_config.get("request_timeout_seconds", 10), 10)

    team_ids = sorted(
        {
            TEAM_ID_BY_TRICODE[team_code]
            for team_code in {_normalize_team_code(value) for value in upcoming_frame["team"].dropna().unique()}
            if team_code in TEAM_ID_BY_TRICODE
        }
    )
    query_pairs: list[tuple[str, object]] = [("per_page", 100)]
    for team_id in team_ids:
        query_pairs.append(("team_ids[]", team_id))
    query = urlencode(query_pairs, doseq=True)
    base_url = provider_config.get("base_url", "https://api.balldontlie.io/v1").rstrip("/")
    payload = fetch_json(
        f"{base_url}/player_injuries?{query}",
        timeout=timeout_seconds,
        headers={"Authorization": api_key},
    )
    records = _extract_json_records(payload, "")
    status["records_loaded"] = int(len(records))
    standardized = _normalize_balldontlie_injury_records(records)
    aligned = _align_provider_rows_to_upcoming(upcoming_frame, standardized)
    status["rows"] = int(len(aligned))
    if aligned.empty:
        status["note"] = "BALDONTLIE returned injury data, but none of it matched the current slate."
    return aligned, status


def _fetch_injury_provider_rows(upcoming_frame: pd.DataFrame, provider_config: dict) -> tuple[pd.DataFrame, dict]:
    status = {
        "enabled": bool(provider_config.get("enabled", True)),
        "rows": 0,
        "records_loaded": 0,
        "latest_report_url": None,
        "last_error": None,
        "official_report_error": None,
        "note": None,
        "source": "Configured injury feed",
    }
    if not status["enabled"]:
        status["note"] = "Injury provider is disabled."
        return pd.DataFrame(columns=CONTEXT_KEY_COLUMNS + OPTIONAL_CONTEXT_COLUMNS), status

    try:
        report_info = _extract_latest_injury_report(provider_config)
        status["latest_report_url"] = report_info.get("latest_report_url")
        status["official_report_page"] = report_info.get("page_url")
        status["official_report_count"] = report_info.get("report_count", 0)
    except (HTTPError, URLError, OSError, ValueError, subprocess.SubprocessError) as exc:
        status["official_report_error"] = _sanitize_error_message(exc)

    if upcoming_frame.empty:
        status["note"] = "No upcoming slate rows are available for injury matching."
        return pd.DataFrame(columns=CONTEXT_KEY_COLUMNS + OPTIONAL_CONTEXT_COLUMNS), status

    provider_kind = str(provider_config.get("provider", "balldontlie") or "balldontlie").strip().lower()
    if provider_kind == "balldontlie":
        try:
            aligned, provider_status = _fetch_balldontlie_injury_rows(upcoming_frame, provider_config)
            status.update(provider_status)
            if not aligned.empty:
                return aligned, status
        except (HTTPError, URLError, ValueError, OSError, subprocess.SubprocessError) as exc:
            status["last_error"] = _sanitize_error_message(exc)
        report_url = status.get("latest_report_url")
        try:
            official_rows = _parse_official_injury_report_rows(str(report_url or ""))
            status["records_loaded"] = int(len(official_rows))
            status["source"] = "NBA official injury report PDF"
            standardized = _standardize_provider_frame(official_rows)
            aligned = _align_provider_rows_to_upcoming(upcoming_frame, standardized)
            status["rows"] = int(len(aligned))
            if status.get("last_error"):
                status["note"] = "BALDONTLIE request failed in this cycle; using official NBA injury report fallback."
            if aligned.empty and not status.get("note"):
                status["note"] = "The official NBA injury report loaded successfully, but none of its rows matched the current slate."
            return aligned, status
        except (ValueError, OSError, subprocess.SubprocessError) as exc:
            status["official_report_error"] = _sanitize_error_message(exc)
            return pd.DataFrame(columns=CONTEXT_KEY_COLUMNS + OPTIONAL_CONTEXT_COLUMNS), status

    csv_url = _resolve_provider_url(provider_config, "csv_url", "csv_url_env")
    json_url = _resolve_provider_url(provider_config, "json_url", "json_url_env")
    if not csv_url and not json_url:
        status["note"] = (
            "Set NBA_INJURY_CSV_URL or NBA_INJURY_JSON_URL to auto-load structured injury and availability updates."
        )
        return pd.DataFrame(columns=CONTEXT_KEY_COLUMNS + OPTIONAL_CONTEXT_COLUMNS), status

    try:
        timeout_seconds = _coerce_positive_int(provider_config.get("request_timeout_seconds", 10), 10)
        if csv_url:
            raw_frame = pd.read_csv(io.StringIO(fetch_text(csv_url, timeout=timeout_seconds)))
            status["source_url"] = csv_url
            status["source"] = "Remote CSV injury feed"
        else:
            payload = json.loads(fetch_text(json_url, timeout=timeout_seconds))
            records = _extract_json_records(payload, str(provider_config.get("json_records_path", "") or ""))
            raw_frame = pd.DataFrame(records)
            status["source_url"] = json_url
            status["source"] = "Remote JSON injury feed"

        status["records_loaded"] = int(len(raw_frame))
        standardized = _standardize_provider_frame(raw_frame)
        aligned = _align_provider_rows_to_upcoming(upcoming_frame, standardized)
        status["rows"] = int(len(aligned))
        if aligned.empty:
            status["note"] = "The injury feed loaded successfully, but none of its rows matched the current slate."
        return aligned, status
    except (HTTPError, URLError, ValueError, OSError, pd.errors.ParserError, subprocess.SubprocessError) as exc:
        status["last_error"] = _sanitize_error_message(exc)
        return pd.DataFrame(columns=CONTEXT_KEY_COLUMNS + OPTIONAL_CONTEXT_COLUMNS), status


def _teammate_out_signal(row: pd.Series) -> bool:
    status_text = " ".join(
        str(row.get(column) or "")
        for column in ["injury_status", "health_status", "suspension_status"]
    ).lower()
    multiplier = _safe_float_value(row.get("injury_minutes_multiplier"), default=1.0)
    starter_probability = _safe_float_value(row.get("starter_probability"), default=0.5)
    if MINUTES_UNAVAILABLE_PATTERN.search(status_text):
        return True
    if MINUTES_DOUBTFUL_PATTERN.search(status_text):
        return True
    if multiplier <= 0.45:
        return True
    if MINUTES_QUESTIONABLE_PATTERN.search(status_text) and starter_probability < 0.3:
        return True
    return False


def _build_teammate_synergy_index(history: pd.DataFrame) -> dict[str, dict[str, dict[str, float]]]:
    required = {"player_name", "team", "game_date"}
    if history.empty or not required.issubset(set(history.columns)):
        return {}

    working = history.copy()
    for column in ["points", "rebounds", "assists", "minutes"]:
        if column not in working.columns:
            working[column] = pd.NA
        working[column] = pd.to_numeric(working[column], errors="coerce")
    working["team"] = working["team"].map(_normalize_team_code)
    working = working.dropna(subset=["player_name", "team", "game_date"])
    if working.empty:
        return {}

    if "game_id" in working.columns and working["game_id"].notna().any():
        working["team_game_key"] = working["game_id"].astype(str).str.strip() + "|" + working["team"].astype(str)
    else:
        home_component = working.get("home", pd.Series(0, index=working.index)).fillna(0).astype(int).astype(str)
        opponent_component = working.get("opponent", pd.Series("", index=working.index)).fillna("").astype(str)
        working["team_game_key"] = (
            pd.to_datetime(working["game_date"], errors="coerce").dt.strftime("%Y-%m-%d").fillna("")
            + "|"
            + working["team"].astype(str)
            + "|"
            + opponent_component
            + "|"
            + home_component
        )

    working["player_key"] = working["player_name"].map(_normalize_player_key)
    working = working[working["player_key"].astype(bool)]
    if working.empty:
        return {}

    player_totals: dict[str, dict[str, float]] = {}
    pair_totals: dict[tuple[str, str], dict[str, float]] = {}

    for _, game_frame in working.groupby(["team", "team_game_key"], sort=False):
        game_players = game_frame[
            ["player_key", "points", "rebounds", "assists", "minutes"]
        ].drop_duplicates(subset=["player_key"], keep="last")
        if game_players.empty:
            continue
        records = game_players.to_dict(orient="records")
        if len(records) < 2:
            continue
        for record in records:
            player_key = str(record["player_key"])
            points = _safe_float_value(record.get("points"), default=0.0)
            rebounds = _safe_float_value(record.get("rebounds"), default=0.0)
            assists = _safe_float_value(record.get("assists"), default=0.0)
            minutes = _safe_float_value(record.get("minutes"), default=0.0)
            total_entry = player_totals.setdefault(
                player_key,
                {"games": 0.0, "points": 0.0, "rebounds": 0.0, "assists": 0.0},
            )
            total_entry["games"] += 1.0
            total_entry["points"] += points
            total_entry["rebounds"] += rebounds
            total_entry["assists"] += assists

            if minutes <= 0.0:
                continue
            for teammate in records:
                teammate_key = str(teammate["player_key"])
                if teammate_key == player_key:
                    continue
                teammate_minutes = _safe_float_value(teammate.get("minutes"), default=0.0)
                if teammate_minutes <= 0.0:
                    continue
                pair_key = (player_key, teammate_key)
                pair_entry = pair_totals.setdefault(
                    pair_key,
                    {"games": 0.0, "points": 0.0, "rebounds": 0.0, "assists": 0.0},
                )
                pair_entry["games"] += 1.0
                pair_entry["points"] += points
                pair_entry["rebounds"] += rebounds
                pair_entry["assists"] += assists

    baseline: dict[str, dict[str, float]] = {}
    for player_key, totals in player_totals.items():
        games = max(1.0, totals["games"])
        baseline[player_key] = {
            "points": totals["points"] / games,
            "rebounds": totals["rebounds"] / games,
            "assists": totals["assists"] / games,
            "games": games,
        }

    synergy_index: dict[str, dict[str, dict[str, float]]] = {}
    for (player_key, teammate_key), totals in pair_totals.items():
        games = totals.get("games", 0.0)
        if games < 4.0:
            continue
        player_base = baseline.get(player_key)
        if not player_base:
            continue
        pair_points = totals["points"] / games
        pair_rebounds = totals["rebounds"] / games
        pair_assists = totals["assists"] / games
        entry = {
            "points": pair_points - player_base["points"],
            "rebounds": pair_rebounds - player_base["rebounds"],
            "assists": pair_assists - player_base["assists"],
            "games": float(games),
        }
        per_player = synergy_index.setdefault(player_key, {})
        per_player[teammate_key] = entry

    trimmed: dict[str, dict[str, dict[str, float]]] = {}
    for player_key, teammate_entries in synergy_index.items():
        ordered = sorted(
            teammate_entries.items(),
            key=lambda item: (
                abs(item[1]["points"]) + 0.7 * abs(item[1]["rebounds"]) + 0.9 * abs(item[1]["assists"]),
                item[1]["games"],
            ),
            reverse=True,
        )
        trimmed[player_key] = {teammate_key: value for teammate_key, value in ordered[:10]}
    return trimmed


def _load_teammate_synergy_index_cached(training_path: Path) -> dict[str, dict[str, dict[str, float]]]:
    signature = None
    try:
        stat = training_path.stat()
        signature = (str(training_path.resolve()), int(stat.st_mtime_ns), int(stat.st_size))
    except OSError:
        signature = (str(training_path), None, None)

    cached_signature = TEAMMATE_SYNERGY_CACHE.get("signature")
    cached_index = TEAMMATE_SYNERGY_CACHE.get("index")
    if signature == cached_signature and isinstance(cached_index, dict):
        return cached_index

    synergy_index = _load_teammate_synergy_index_cached(training_path)
    TEAMMATE_SYNERGY_CACHE["signature"] = signature
    TEAMMATE_SYNERGY_CACHE["index"] = synergy_index
    TEAMMATE_SYNERGY_CACHE["last_built_at"] = _now_iso()
    return synergy_index


def _apply_teammate_composition_context(
    upcoming_frame: pd.DataFrame,
    training_path: Path,
) -> tuple[pd.DataFrame, dict]:
    if upcoming_frame.empty:
        return upcoming_frame, {"rows_with_context": 0, "rows_without_context": 0}

    working = upcoming_frame.copy()
    working["player_key"] = working["player_name"].map(_normalize_player_key)
    working["team"] = working["team"].map(_normalize_team_code).fillna(working["team"])
    working["starter_probability"] = pd.to_numeric(working.get("starter_probability"), errors="coerce")
    working["injury_minutes_multiplier"] = pd.to_numeric(working.get("injury_minutes_multiplier"), errors="coerce")
    working["expected_minutes"] = pd.to_numeric(working.get("expected_minutes"), errors="coerce")
    working["__out_signal"] = working.apply(_teammate_out_signal, axis=1)

    try:
        season_priors = load_season_priors()
    except Exception:
        season_priors = pd.DataFrame(columns=["player_name", "team", "player_key", "team_key"])
    if season_priors.empty:
        season_priors = pd.DataFrame(columns=["player_name", "team", "player_key", "team_key", "pts_season", "ast_season", "reb_season"])
    for column in ["pts_season", "ast_season", "reb_season", "min_season"]:
        if column not in season_priors.columns:
            season_priors[column] = pd.NA
        season_priors[column] = pd.to_numeric(season_priors[column], errors="coerce")
    season_priors["player_key"] = season_priors["player_name"].map(_normalize_player_key)
    season_priors["team"] = season_priors["team"].map(_normalize_team_code).fillna(season_priors["team"])
    season_priors["core_score"] = (
        season_priors["pts_season"].fillna(0.0) * 0.55
        + season_priors["ast_season"].fillna(0.0) * 0.3
        + season_priors["reb_season"].fillna(0.0) * 0.15
    )
    season_priors = season_priors.sort_values(["team", "core_score"], ascending=[True, False])

    try:
        history = load_dataset(training_path)
    except Exception:
        history = pd.DataFrame()
    synergy_index = _build_teammate_synergy_index(history)

    output_frame = working.copy()
    for column in TEAMMATE_CONTEXT_COLUMNS:
        output_frame[column] = pd.NA

    rows_with_context = 0
    for (_, team_code), team_frame in output_frame.groupby(["game_date", "team"], sort=False):
        team_players = team_frame.copy()
        if team_players.empty:
            continue

        team_player_keys = team_players["player_key"].astype(str).tolist()
        priors_subset = season_priors[season_priors["team"] == team_code].copy()
        priors_subset = priors_subset[priors_subset["player_key"].isin(team_player_keys)]
        if priors_subset.empty:
            priors_subset = pd.DataFrame({"player_key": team_player_keys, "core_score": [0.0] * len(team_player_keys)})

        prior_scores = {
            str(row["player_key"]): float(row["core_score"])
            for _, row in priors_subset.iterrows()
        }
        if not prior_scores:
            prior_scores = {str(player_key): 0.0 for player_key in team_player_keys}

        ranked_teammates = sorted(prior_scores.items(), key=lambda item: item[1], reverse=True)
        top_teammates = [player_key for player_key, _ in ranked_teammates[:8]]
        top_two_teammates = {player_key for player_key, _ in ranked_teammates[:2]}

        active_set: set[str] = set()
        out_set: set[str] = set()
        for _, player_row in team_players.iterrows():
            player_key = str(player_row.get("player_key") or "")
            if not player_key:
                continue
            if bool(player_row.get("__out_signal", False)):
                out_set.add(player_key)
            else:
                active_set.add(player_key)

        for row_index, player_row in team_players.iterrows():
            player_key = str(player_row.get("player_key") or "")
            if not player_key:
                continue
            teammates = [teammate_key for teammate_key in top_teammates if teammate_key != player_key]
            if not teammates:
                continue

            active_core_count = sum(1 for teammate_key in teammates if teammate_key in active_set)
            out_core_count = sum(1 for teammate_key in teammates if teammate_key in out_set)
            total_core_score = sum(max(0.0, prior_scores.get(teammate_key, 0.0)) for teammate_key in teammates)
            out_core_score = sum(max(0.0, prior_scores.get(teammate_key, 0.0)) for teammate_key in teammates if teammate_key in out_set)
            vacancy = out_core_score / total_core_score if total_core_score > 0 else min(1.0, out_core_count / max(1, len(teammates)))
            continuity = active_core_count / max(1, len(teammates))
            star_out = 1.0 if any(teammate_key in out_set for teammate_key in teammates if teammate_key in top_two_teammates) else 0.0

            synergy_points = 0.0
            synergy_rebounds = 0.0
            synergy_assists = 0.0
            player_synergy = synergy_index.get(player_key, {})
            if player_synergy:
                for teammate_key, payload in player_synergy.items():
                    if teammate_key not in prior_scores:
                        continue
                    games = _safe_float_value(payload.get("games"), default=0.0)
                    if games <= 0:
                        continue
                    weight = min(1.0, games / 18.0)
                    if teammate_key in active_set:
                        direction = 1.0
                    elif teammate_key in out_set:
                        direction = -0.45
                    else:
                        direction = 0.0
                    synergy_points += _safe_float_value(payload.get("points"), default=0.0) * weight * direction
                    synergy_rebounds += _safe_float_value(payload.get("rebounds"), default=0.0) * weight * direction
                    synergy_assists += _safe_float_value(payload.get("assists"), default=0.0) * weight * direction

            output_frame.at[row_index, "teammate_active_core_count"] = float(active_core_count)
            output_frame.at[row_index, "teammate_out_core_count"] = float(out_core_count)
            output_frame.at[row_index, "teammate_usage_vacancy"] = float(max(0.0, min(1.8, vacancy)))
            output_frame.at[row_index, "teammate_continuity_score"] = float(max(0.0, min(1.0, continuity)))
            output_frame.at[row_index, "teammate_star_out_flag"] = float(star_out)
            output_frame.at[row_index, "teammate_synergy_points"] = float(max(-6.0, min(6.0, synergy_points)))
            output_frame.at[row_index, "teammate_synergy_rebounds"] = float(max(-4.0, min(4.0, synergy_rebounds)))
            output_frame.at[row_index, "teammate_synergy_assists"] = float(max(-4.0, min(4.0, synergy_assists)))
            on_off_points = (vacancy * 2.8) + (synergy_points * 0.55) - ((1.0 - continuity) * 1.25)
            on_off_rebounds = (vacancy * 1.4) + (synergy_rebounds * 0.45) - ((1.0 - continuity) * 0.65)
            on_off_assists = (vacancy * 1.8) + (synergy_assists * 0.5) - ((1.0 - continuity) * 0.85)
            output_frame.at[row_index, "teammate_on_off_points_delta"] = float(max(-6.0, min(6.0, on_off_points)))
            output_frame.at[row_index, "teammate_on_off_rebounds_delta"] = float(max(-4.0, min(4.0, on_off_rebounds)))
            output_frame.at[row_index, "teammate_on_off_assists_delta"] = float(max(-4.0, min(4.0, on_off_assists)))
            rows_with_context += 1

    output_frame = output_frame.drop(columns=["player_key", "__out_signal"], errors="ignore")
    rows_without_context = int(len(output_frame) - rows_with_context)
    return output_frame, {
        "rows_with_context": int(rows_with_context),
        "rows_without_context": rows_without_context,
        "teams_evaluated": int(output_frame[["game_date", "team"]].drop_duplicates().shape[0]),
    }


def _default_nba_season_string(now_utc: datetime | None = None) -> str:
    now_value = now_utc or datetime.now(timezone.utc)
    start_year = now_value.year if now_value.month >= 10 else now_value.year - 1
    return f"{start_year}-{(start_year + 1) % 100:02d}"


def _nba_stats_request_headers() -> dict[str, str]:
    return {
        "User-Agent": (
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
            "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36"
        ),
        "Accept": "application/json, text/plain, */*",
        "Referer": "https://www.nba.com/",
        "Origin": "https://www.nba.com",
        "x-nba-stats-origin": "stats",
        "x-nba-stats-token": "true",
    }


def _nba_resultset_to_frame(payload: dict, preferred_result_set: str | None = None) -> pd.DataFrame:
    if not isinstance(payload, dict):
        return pd.DataFrame()
    result_sets: list[dict] = []
    candidate = payload.get("resultSets")
    if isinstance(candidate, list):
        result_sets.extend([value for value in candidate if isinstance(value, dict)])
    elif isinstance(candidate, dict):
        result_sets.append(candidate)
    single_result = payload.get("resultSet")
    if isinstance(single_result, dict):
        result_sets.append(single_result)
    if not result_sets:
        return pd.DataFrame()

    ordered_sets = result_sets
    if preferred_result_set:
        preferred = preferred_result_set.strip().lower()
        preferred_match = [
            item
            for item in result_sets
            if str(item.get("name") or item.get("resultSetName") or "").strip().lower() == preferred
        ]
        if preferred_match:
            ordered_sets = preferred_match + [item for item in result_sets if item not in preferred_match]

    for result_set in ordered_sets:
        headers = result_set.get("headers") or result_set.get("columnNames")
        row_set = result_set.get("rowSet")
        if not isinstance(headers, list) or not isinstance(row_set, list):
            continue
        if not headers:
            continue
        frame = pd.DataFrame(row_set, columns=headers)
        if not frame.empty:
            return frame
    return pd.DataFrame()


def _fetch_nba_stats_frame(
    *,
    base_url: str,
    endpoint: str,
    params: dict[str, object],
    timeout_seconds: int,
    preferred_result_set: str | None = None,
) -> pd.DataFrame:
    query = urlencode({key: value for key, value in params.items() if value is not None}, doseq=True)
    url = f"{base_url.rstrip('/')}/{endpoint.lstrip('/')}?{query}"
    payload = fetch_json(url, timeout=timeout_seconds, headers=_nba_stats_request_headers())
    return _nba_resultset_to_frame(payload, preferred_result_set=preferred_result_set)


def _safe_numeric_series_from_candidates(
    frame: pd.DataFrame,
    column_candidates: list[str],
    *,
    default: float = float("nan"),
) -> pd.Series:
    for column in column_candidates:
        if column in frame.columns:
            return pd.to_numeric(frame[column], errors="coerce")
    return pd.Series(default, index=frame.index, dtype=float)


def _load_playstyle_cache(path: Path) -> pd.DataFrame:
    cache_columns = [
        "player_key",
        "player_name",
        "team",
        "profile_updated_at",
        "profile_source",
    ] + PLAYSTYLE_CONTEXT_COLUMNS
    if not path.exists():
        return pd.DataFrame(columns=cache_columns)
    try:
        frame = pd.read_csv(path)
    except (OSError, pd.errors.EmptyDataError):
        return pd.DataFrame(columns=cache_columns)
    if frame.empty:
        return pd.DataFrame(columns=cache_columns)
    working = frame.copy()
    for column in cache_columns:
        if column not in working.columns:
            working[column] = pd.NA
    working["player_key"] = working["player_key"].fillna("").astype(str).map(_normalize_player_key)
    working["player_name"] = working["player_name"].fillna("").astype(str).str.strip()
    working["team"] = working["team"].map(_normalize_team_code).fillna(working["team"])
    working["profile_updated_at"] = working["profile_updated_at"].fillna("").astype(str).str.strip()
    working["profile_source"] = working["profile_source"].fillna("").astype(str).str.strip()
    for column in PLAYSTYLE_NUMERIC_COLUMNS:
        working[column] = pd.to_numeric(working[column], errors="coerce")
    return working[cache_columns].drop_duplicates(subset=["player_key", "team"], keep="last")


def _save_playstyle_cache(path: Path, frame: pd.DataFrame) -> None:
    cache_columns = [
        "player_key",
        "player_name",
        "team",
        "profile_updated_at",
        "profile_source",
    ] + PLAYSTYLE_CONTEXT_COLUMNS
    path.parent.mkdir(parents=True, exist_ok=True)
    if frame.empty:
        pd.DataFrame(columns=cache_columns).to_csv(path, index=False)
        return
    working = frame.copy()
    for column in cache_columns:
        if column not in working.columns:
            working[column] = pd.NA
    working = working[cache_columns].drop_duplicates(subset=["player_key", "team"], keep="last")
    working = working.sort_values(["player_name", "team", "player_key"])
    working.to_csv(path, index=False)


def _build_playstyle_profiles_from_web(
    *,
    provider_config: dict,
    player_names_filter: set[str] | None = None,
) -> tuple[pd.DataFrame, dict]:
    status = {
        "source": "NBA Stats API (leaguedashplayerstats/leaguedashptstats/leaguedashplayershotlocations)",
        "season": None,
        "records_loaded": 0,
        "endpoints_loaded": 0,
        "last_error": None,
    }
    base_url = str(provider_config.get("base_url") or "https://stats.nba.com/stats").strip()
    season = str(provider_config.get("season") or _default_nba_season_string()).strip()
    season_type = str(provider_config.get("season_type") or "Regular Season").strip()
    per_mode = str(provider_config.get("per_mode") or "PerGame").strip()
    timeout_seconds = _coerce_positive_int(provider_config.get("timeout_seconds", 18), 18)
    remote_fetch_mode = str(provider_config.get("remote_fetch_mode", "light") or "light").strip().lower()
    include_extended_endpoints = remote_fetch_mode == "full"
    status["season"] = season
    status["mode"] = remote_fetch_mode

    base_common = {
        "College": "",
        "Conference": "",
        "Country": "",
        "DateFrom": "",
        "DateTo": "",
        "Division": "",
        "DraftPick": "",
        "DraftYear": "",
        "GameScope": "",
        "GameSegment": "",
        "Height": "",
        "LastNGames": "0",
        "LeagueID": "00",
        "Location": "",
        "Month": "0",
        "OpponentTeamID": "0",
        "Outcome": "",
        "PORound": "0",
        "PlayerExperience": "",
        "PlayerPosition": "",
        "Season": season,
        "SeasonSegment": "",
        "SeasonType": season_type,
        "StarterBench": "",
        "TeamID": "0",
        "VsConference": "",
        "VsDivision": "",
        "Weight": "",
    }

    base_params = {
        **base_common,
        "MeasureType": "Base",
        "PaceAdjust": "N",
        "PerMode": per_mode,
        "Period": "0",
        "PlusMinus": "N",
        "Rank": "N",
        "ShotClockRange": "",
    }
    base_frame = _fetch_nba_stats_frame(
        base_url=base_url,
        endpoint="leaguedashplayerstats",
        params=base_params,
        timeout_seconds=timeout_seconds,
    )
    if base_frame.empty:
        return pd.DataFrame(), status
    status["endpoints_loaded"] = int(status["endpoints_loaded"]) + 1
    base_frame = base_frame.copy()
    base_frame.columns = [str(column).strip() for column in base_frame.columns]
    name_column = "PLAYER_NAME" if "PLAYER_NAME" in base_frame.columns else "PLAYER"
    team_column = "TEAM_ABBREVIATION" if "TEAM_ABBREVIATION" in base_frame.columns else "TEAM"
    if name_column not in base_frame.columns or team_column not in base_frame.columns:
        return pd.DataFrame(), status

    profiles = pd.DataFrame(
        {
            "player_name": base_frame[name_column].astype(str).str.strip(),
            "team": base_frame[team_column].map(_normalize_team_code).fillna(base_frame[team_column]),
        }
    )
    profiles["player_key"] = profiles["player_name"].map(_normalize_player_key)
    profiles = profiles[profiles["player_key"].astype(bool)]
    if player_names_filter:
        profiles = profiles[profiles["player_key"].isin({_normalize_player_key(name) for name in player_names_filter if name})]
    if profiles.empty:
        return pd.DataFrame(), status
    profiles = profiles.drop_duplicates(subset=["player_key", "team"], keep="last").reset_index(drop=True)
    profiles["player_team_key"] = profiles["player_key"] + "|" + profiles["team"].fillna("")
    base_frame["player_name"] = base_frame[name_column].astype(str).str.strip()
    base_frame["team"] = base_frame[team_column].map(_normalize_team_code).fillna(base_frame[team_column])
    base_frame["player_key"] = base_frame["player_name"].map(_normalize_player_key)
    base_frame["player_team_key"] = base_frame["player_key"] + "|" + base_frame["team"].fillna("")

    def _map_metric(measure_frame: pd.DataFrame, column_candidates: list[str]) -> pd.Series:
        if measure_frame.empty:
            return pd.Series(float("nan"), index=profiles.index, dtype=float)
        working = measure_frame.copy()
        metric = _safe_numeric_series_from_candidates(working, column_candidates)
        working = working.assign(__metric=metric)
        by_team = (
            working[["player_team_key", "__metric"]]
            .dropna(subset=["player_team_key", "__metric"])
            .drop_duplicates(subset=["player_team_key"], keep="last")
            .set_index("player_team_key")["__metric"]
        )
        by_player = (
            working[["player_key", "__metric"]]
            .dropna(subset=["player_key", "__metric"])
            .groupby("player_key", as_index=True)["__metric"]
            .mean()
        )
        mapped = profiles["player_team_key"].map(by_team)
        fallback = profiles["player_key"].map(by_player)
        return pd.to_numeric(mapped, errors="coerce").combine_first(pd.to_numeric(fallback, errors="coerce"))

    fga = _map_metric(base_frame, ["FGA"]).clip(lower=0.0)
    fg3a = _map_metric(base_frame, ["FG3A", "FG3A_PER_GAME"]).clip(lower=0.0)
    pts = _map_metric(base_frame, ["PTS"]).clip(lower=0.0)
    ast = _map_metric(base_frame, ["AST"]).clip(lower=0.0)
    reb = _map_metric(base_frame, ["REB"]).clip(lower=0.0)
    stl = _map_metric(base_frame, ["STL"]).clip(lower=0.0)
    blk = _map_metric(base_frame, ["BLK"]).clip(lower=0.0)
    minutes = _map_metric(base_frame, ["MIN", "MINUTES"]).clip(lower=0.0)

    def _fetch_pt_measure(measure_type: str) -> pd.DataFrame:
        measure_params = {
            **base_common,
            "PerMode": per_mode,
            "PlayerOrTeam": "Player",
            "PtMeasureType": measure_type,
        }
        frame = _fetch_nba_stats_frame(
            base_url=base_url,
            endpoint="leaguedashptstats",
            params=measure_params,
            timeout_seconds=timeout_seconds,
        )
        if frame.empty:
            return frame
        status["endpoints_loaded"] = int(status["endpoints_loaded"]) + 1
        frame = frame.copy()
        frame.columns = [str(column).strip() for column in frame.columns]
        pt_name_column = "PLAYER_NAME" if "PLAYER_NAME" in frame.columns else "PLAYER"
        pt_team_column = "TEAM_ABBREVIATION" if "TEAM_ABBREVIATION" in frame.columns else "TEAM"
        if pt_name_column not in frame.columns or pt_team_column not in frame.columns:
            return pd.DataFrame()
        frame["player_name"] = frame[pt_name_column].astype(str).str.strip()
        frame["team"] = frame[pt_team_column].map(_normalize_team_code).fillna(frame[pt_team_column])
        frame["player_key"] = frame["player_name"].map(_normalize_player_key)
        frame["player_team_key"] = frame["player_key"] + "|" + frame["team"].fillna("")
        return frame

    if include_extended_endpoints:
        pt_catch = _fetch_pt_measure("CatchShoot")
        pt_pull = _fetch_pt_measure("PullUpShot")
        pt_drive = _fetch_pt_measure("Drives")
        pt_pass = _fetch_pt_measure("Passing")
        pt_rebound = _fetch_pt_measure("Rebounding")
        pt_paint = _fetch_pt_measure("PaintTouch")
        pt_post = _fetch_pt_measure("PostTouch")
        pt_elbow = _fetch_pt_measure("ElbowTouch")
    else:
        pt_catch = pd.DataFrame()
        pt_pull = pd.DataFrame()
        pt_drive = pd.DataFrame()
        pt_pass = pd.DataFrame()
        pt_rebound = pd.DataFrame()
        pt_paint = pd.DataFrame()
        pt_post = pd.DataFrame()
        pt_elbow = pd.DataFrame()

    shot_frame = pd.DataFrame()
    shot_params = {
        **base_common,
        "DistanceRange": "By Zone",
        "DribbleRange": "",
        "GeneralRange": "",
        "PaceAdjust": "N",
        "PerMode": per_mode,
        "Period": "0",
        "PlusMinus": "N",
        "Rank": "N",
        "ShotClockRange": "",
    }
    if include_extended_endpoints:
        try:
            shot_frame = _fetch_nba_stats_frame(
                base_url=base_url,
                endpoint="leaguedashplayershotlocations",
                params=shot_params,
                timeout_seconds=timeout_seconds,
            )
        except (HTTPError, URLError, ValueError, OSError, subprocess.SubprocessError) as exc:
            status["last_error"] = _sanitize_error_message(exc)
            shot_frame = pd.DataFrame()
    if not shot_frame.empty:
        status["endpoints_loaded"] = int(status["endpoints_loaded"]) + 1
        shot_frame = shot_frame.copy()
        shot_frame.columns = [str(column).strip() for column in shot_frame.columns]
        shot_name_column = "PLAYER_NAME" if "PLAYER_NAME" in shot_frame.columns else "PLAYER"
        shot_team_column = "TEAM_ABBREVIATION" if "TEAM_ABBREVIATION" in shot_frame.columns else "TEAM"
        if shot_name_column in shot_frame.columns and shot_team_column in shot_frame.columns:
            shot_frame["player_name"] = shot_frame[shot_name_column].astype(str).str.strip()
            shot_frame["team"] = shot_frame[shot_team_column].map(_normalize_team_code).fillna(shot_frame[shot_team_column])
            shot_frame["player_key"] = shot_frame["player_name"].map(_normalize_player_key)
            shot_frame["player_team_key"] = shot_frame["player_key"] + "|" + shot_frame["team"].fillna("")
        else:
            shot_frame = pd.DataFrame()

    rim_fga = pd.Series(float("nan"), index=profiles.index, dtype=float)
    mid_fga = pd.Series(float("nan"), index=profiles.index, dtype=float)
    if not shot_frame.empty:
        lower_columns = {column.lower(): column for column in shot_frame.columns}
        rim_cols = [value for key, value in lower_columns.items() if ("restricted area" in key or "in the paint (non-ra)" in key) and "fga" in key]
        mid_cols = [value for key, value in lower_columns.items() if "mid-range" in key and "fga" in key]
        if rim_cols:
            shot_frame["__rim_fga"] = shot_frame[rim_cols].apply(pd.to_numeric, errors="coerce").sum(axis=1, min_count=1)
            rim_fga = _map_metric(shot_frame, ["__rim_fga"])
        if mid_cols:
            shot_frame["__mid_fga"] = shot_frame[mid_cols].apply(pd.to_numeric, errors="coerce").sum(axis=1, min_count=1)
            mid_fga = _map_metric(shot_frame, ["__mid_fga"])
    two_point_attempts = (fga - fg3a).clip(lower=0.0)
    rim_fga = rim_fga.combine_first((two_point_attempts * 0.56).clip(lower=0.0))
    mid_fga = mid_fga.combine_first((two_point_attempts - rim_fga).clip(lower=0.0))

    catch_shoot_fga = _map_metric(pt_catch, ["FGA", "CATCH_SHOOT_FGA", "CATCH_SHOOT_FGA_PER_GAME"]).clip(lower=0.0)
    pull_up_fga = _map_metric(pt_pull, ["FGA", "PULL_UP_FGA", "PULL_UP_FGA_PER_GAME"]).clip(lower=0.0)
    drives = _map_metric(pt_drive, ["DRIVES", "DRIVE", "DRIVE_FGA", "FGA"]).clip(lower=0.0)
    potential_ast = _map_metric(pt_pass, ["POTENTIAL_AST", "AST"]).clip(lower=0.0)
    rebound_chances = _map_metric(pt_rebound, ["REB_CHANCES", "REB", "CHANCES"]).clip(lower=0.0)
    paint_touches = _map_metric(pt_paint, ["PAINT_TOUCHES", "TOUCHES"]).clip(lower=0.0)
    post_touches = _map_metric(pt_post, ["POST_TOUCHES", "TOUCHES"]).clip(lower=0.0)
    elbow_touches = _map_metric(pt_elbow, ["ELBOW_TOUCHES", "TOUCHES"]).clip(lower=0.0)

    fga_den = fga.replace(0, np.nan).fillna(1.0)
    min_den = minutes.replace(0, np.nan).fillna(1.0)
    three_rate = (fg3a / fga_den).clip(lower=0.0, upper=1.0)
    rim_rate = (rim_fga / fga_den).clip(lower=0.0, upper=1.0)
    mid_rate = (mid_fga / fga_den).clip(lower=0.0, upper=1.0)
    catch_rate = (catch_shoot_fga / fga_den).clip(lower=0.0, upper=1.0)
    pull_rate = (pull_up_fga / fga_den).clip(lower=0.0, upper=1.0)
    drive_rate = (drives / min_den).clip(lower=0.0, upper=2.5)
    assist_potential = (potential_ast / min_den).clip(lower=0.0, upper=1.8)
    rebound_rate = (rebound_chances / min_den).clip(lower=0.0, upper=2.2)
    paint_rate = (paint_touches / min_den).clip(lower=0.0, upper=2.0)
    post_rate = (post_touches / min_den).clip(lower=0.0, upper=1.8)
    elbow_rate = (elbow_touches / min_den).clip(lower=0.0, upper=1.8)
    usage_proxy = (pts / min_den).clip(lower=0.0, upper=2.2)
    offball_rate = ((catch_shoot_fga.fillna(0.0) + fg3a.fillna(0.0) * 0.5) / min_den).clip(lower=0.0, upper=1.6)
    defensive_event_rate = ((stl.fillna(0.0) + blk.fillna(0.0)) / min_den).clip(lower=0.0, upper=0.45)

    coverage_components = pd.concat(
        [
            catch_shoot_fga.notna().astype(float),
            pull_up_fga.notna().astype(float),
            drives.notna().astype(float),
            potential_ast.notna().astype(float),
            rebound_chances.notna().astype(float),
        ],
        axis=1,
    )
    context_conf = (0.3 + (coverage_components.mean(axis=1) * 0.7)).clip(lower=0.2, upper=1.0)
    primary_role = np.select(
        [
            drive_rate.ge(0.45) & assist_potential.ge(0.38),
            catch_rate.ge(0.4) & pull_rate.lt(0.2),
            pull_rate.ge(0.34),
            rim_rate.ge(0.36) & rebound_rate.ge(0.34),
            assist_potential.ge(0.44),
        ],
        [
            "drive_and_kick",
            "spot_up",
            "shot_creator",
            "interior_finisher",
            "table_setter",
        ],
        default="balanced",
    )
    scoring_mode = np.select(
        [
            three_rate.ge(0.45),
            rim_rate.ge(0.38),
            mid_rate.ge(0.32),
        ],
        [
            "perimeter_volume",
            "paint_pressure",
            "mid_range",
        ],
        default="mixed",
    )

    now_iso = datetime.now(timezone.utc).isoformat()
    profiles["playstyle_shot_profile_source"] = "nba_stats_live"
    profiles["playstyle_primary_role"] = primary_role
    profiles["playstyle_scoring_mode"] = scoring_mode
    profiles["playstyle_rim_rate"] = rim_rate.round(4)
    profiles["playstyle_mid_range_rate"] = mid_rate.round(4)
    profiles["playstyle_three_rate"] = three_rate.round(4)
    profiles["playstyle_catch_shoot_rate"] = catch_rate.round(4)
    profiles["playstyle_pull_up_rate"] = pull_rate.round(4)
    profiles["playstyle_drive_rate"] = drive_rate.round(4)
    profiles["playstyle_assist_potential"] = assist_potential.round(4)
    profiles["playstyle_paint_touch_rate"] = paint_rate.round(4)
    profiles["playstyle_post_touch_rate"] = post_rate.round(4)
    profiles["playstyle_elbow_touch_rate"] = elbow_rate.round(4)
    profiles["playstyle_rebound_chance_rate"] = rebound_rate.round(4)
    profiles["playstyle_offball_activity_rate"] = offball_rate.round(4)
    profiles["playstyle_usage_proxy"] = usage_proxy.round(4)
    profiles["playstyle_defensive_event_rate"] = defensive_event_rate.round(4)
    profiles["playstyle_context_confidence"] = context_conf.round(4)
    profiles["profile_updated_at"] = now_iso
    profiles["profile_source"] = "nba_stats_api"
    if not include_extended_endpoints:
        status["note"] = "Playstyle remote fetch ran in light mode (base endpoint only)."
    status["records_loaded"] = int(len(profiles))
    return profiles, status


def _build_playstyle_profiles_from_local(
    *,
    upcoming_frame: pd.DataFrame,
    training_path: Path = DEFAULT_TRAINING_UPLOAD_PATH,
    player_names_filter: set[str] | None = None,
) -> tuple[pd.DataFrame, dict]:
    status = {
        "source": "local_inferred_playstyle",
        "records_loaded": 0,
        "history_rows_used": 0,
        "season_priors_rows_used": 0,
        "last_error": None,
    }
    if upcoming_frame.empty:
        return pd.DataFrame(), status

    keys = upcoming_frame[["player_name", "team"]].dropna(subset=["player_name"]).copy()
    keys["player_name"] = keys["player_name"].astype(str).str.strip()
    keys["team"] = keys["team"].map(_normalize_team_code).fillna(keys["team"])
    keys["player_key"] = keys["player_name"].map(_normalize_player_key)
    keys = keys[keys["player_key"].astype(bool)].drop_duplicates(subset=["player_key", "team"], keep="last")
    if player_names_filter:
        filter_keys = {_normalize_player_key(name) for name in player_names_filter if str(name).strip()}
        if filter_keys:
            keys = keys[keys["player_key"].isin(filter_keys)]
    if keys.empty:
        return pd.DataFrame(), status

    try:
        history = load_dataset(training_path)
    except Exception as exc:  # noqa: BLE001
        history = pd.DataFrame()
        status["last_error"] = _sanitize_error_message(exc)

    history_team = pd.DataFrame(columns=["player_key", "team"])
    history_player = pd.DataFrame(columns=["player_key"])
    if not history.empty and "player_name" in history.columns:
        history_working = history.copy()
        if "team" not in history_working.columns:
            history_working["team"] = pd.NA
        history_working["player_name"] = history_working["player_name"].astype(str).str.strip()
        history_working["player_key"] = history_working["player_name"].map(_normalize_player_key)
        history_working["team"] = history_working["team"].map(_normalize_team_code).fillna(history_working["team"])
        history_working = history_working[history_working["player_key"].astype(bool)]

        numeric_history_columns = [
            "points",
            "rebounds",
            "assists",
            "minutes",
            "three_points_made",
            "fga",
            "three_pa",
            "fta",
            "turnovers",
            "steals",
            "blocks",
        ]
        for column in numeric_history_columns:
            if column not in history_working.columns:
                history_working[column] = pd.NA
            history_working[column] = pd.to_numeric(history_working[column], errors="coerce")
        status["history_rows_used"] = int(len(history_working))

        history_team = (
            history_working.groupby(["player_key", "team"], dropna=False, as_index=False)
            .agg(
                points_hist=("points", "mean"),
                rebounds_hist=("rebounds", "mean"),
                assists_hist=("assists", "mean"),
                minutes_hist=("minutes", "mean"),
                three_pm_hist=("three_points_made", "mean"),
                fga_hist=("fga", "mean"),
                three_pa_hist=("three_pa", "mean"),
                fta_hist=("fta", "mean"),
                tov_hist=("turnovers", "mean"),
                stl_hist=("steals", "mean"),
                blk_hist=("blocks", "mean"),
                games_hist=("player_key", "size"),
            )
            .sort_values("games_hist", ascending=False)
            .drop_duplicates(subset=["player_key", "team"], keep="first")
        )
        history_player = (
            history_working.groupby("player_key", as_index=False)
            .agg(
                points_hist_player=("points", "mean"),
                rebounds_hist_player=("rebounds", "mean"),
                assists_hist_player=("assists", "mean"),
                minutes_hist_player=("minutes", "mean"),
                three_pm_hist_player=("three_points_made", "mean"),
                fga_hist_player=("fga", "mean"),
                three_pa_hist_player=("three_pa", "mean"),
                fta_hist_player=("fta", "mean"),
                tov_hist_player=("turnovers", "mean"),
                stl_hist_player=("steals", "mean"),
                blk_hist_player=("blocks", "mean"),
                games_hist_player=("player_key", "size"),
            )
            .sort_values("games_hist_player", ascending=False)
            .drop_duplicates(subset=["player_key"], keep="first")
        )

    try:
        season_priors = load_season_priors()
    except Exception as exc:  # noqa: BLE001
        season_priors = pd.DataFrame()
        if not status["last_error"]:
            status["last_error"] = _sanitize_error_message(exc)

    season_team = pd.DataFrame(columns=["player_key", "team"])
    season_player = pd.DataFrame(columns=["player_key"])
    if not season_priors.empty and "player_name" in season_priors.columns:
        priors = season_priors.copy()
        priors["player_name"] = priors["player_name"].astype(str).str.strip()
        priors["player_key"] = priors["player_name"].map(_normalize_player_key)
        priors["team"] = priors["team"].map(_normalize_team_code).fillna(priors["team"])
        priors = priors[priors["player_key"].astype(bool)]
        status["season_priors_rows_used"] = int(len(priors))

        season_columns = [
            "min_season",
            "pts_season",
            "reb_season",
            "ast_season",
            "three_pm_season",
            "three_pa_season",
            "fga_season",
            "fta_season",
            "tov_season",
            "stl_season",
            "blk_season",
        ]
        for column in season_columns:
            if column not in priors.columns:
                priors[column] = pd.NA
            priors[column] = pd.to_numeric(priors[column], errors="coerce")

        season_team = (
            priors[["player_key", "team"] + season_columns]
            .sort_values("min_season", ascending=False, na_position="last")
            .drop_duplicates(subset=["player_key", "team"], keep="first")
        )
        season_player = (
            priors.groupby("player_key", as_index=False)[season_columns]
            .mean(numeric_only=True)
            .rename(columns={column: f"{column}_player" for column in season_columns})
        )

    profiles = keys.copy()
    if not history_team.empty:
        profiles = profiles.merge(history_team, on=["player_key", "team"], how="left")
    if not history_player.empty:
        profiles = profiles.merge(history_player, on="player_key", how="left")
    if not season_team.empty:
        profiles = profiles.merge(season_team, on=["player_key", "team"], how="left")
    if not season_player.empty:
        profiles = profiles.merge(season_player, on="player_key", how="left")

    def _combine_pair(frame: pd.DataFrame, primary: str, fallback: str) -> pd.Series:
        if primary in frame.columns:
            primary_series = pd.to_numeric(frame[primary], errors="coerce")
        else:
            primary_series = pd.Series(np.nan, index=frame.index, dtype=float)
        if fallback in frame.columns:
            fallback_series = pd.to_numeric(frame[fallback], errors="coerce")
        else:
            fallback_series = pd.Series(np.nan, index=frame.index, dtype=float)
        return primary_series.combine_first(fallback_series)

    minutes = _combine_pair(profiles, "minutes_hist", "minutes_hist_player")
    minutes = minutes.combine_first(_combine_pair(profiles, "min_season", "min_season_player")).fillna(24.0).clip(lower=6.0, upper=42.0)
    points = _combine_pair(profiles, "points_hist", "points_hist_player")
    points = points.combine_first(_combine_pair(profiles, "pts_season", "pts_season_player")).fillna(10.0).clip(lower=0.0, upper=45.0)
    rebounds = _combine_pair(profiles, "rebounds_hist", "rebounds_hist_player")
    rebounds = rebounds.combine_first(_combine_pair(profiles, "reb_season", "reb_season_player")).fillna(4.0).clip(lower=0.0, upper=25.0)
    assists = _combine_pair(profiles, "assists_hist", "assists_hist_player")
    assists = assists.combine_first(_combine_pair(profiles, "ast_season", "ast_season_player")).fillna(2.5).clip(lower=0.0, upper=15.0)
    three_pm = _combine_pair(profiles, "three_pm_hist", "three_pm_hist_player")
    three_pm = three_pm.combine_first(_combine_pair(profiles, "three_pm_season", "three_pm_season_player")).fillna(1.2).clip(lower=0.0, upper=7.5)
    fga = _combine_pair(profiles, "fga_hist", "fga_hist_player")
    fga = fga.combine_first(_combine_pair(profiles, "fga_season", "fga_season_player"))
    fga = fga.combine_first((points / 1.28).clip(lower=2.0, upper=32.0)).clip(lower=1.0, upper=35.0)
    three_pa = _combine_pair(profiles, "three_pa_hist", "three_pa_hist_player")
    three_pa = three_pa.combine_first(_combine_pair(profiles, "three_pa_season", "three_pa_season_player"))
    three_pa = three_pa.combine_first((three_pm * 2.3).clip(lower=0.0, upper=18.0)).clip(lower=0.0, upper=20.0)
    fta = _combine_pair(profiles, "fta_hist", "fta_hist_player")
    fta = fta.combine_first(_combine_pair(profiles, "fta_season", "fta_season_player"))
    fta = fta.combine_first((points * 0.18).clip(lower=0.2, upper=14.0)).clip(lower=0.0, upper=16.0)
    tov = _combine_pair(profiles, "tov_hist", "tov_hist_player")
    tov = tov.combine_first(_combine_pair(profiles, "tov_season", "tov_season_player")).fillna(1.8).clip(lower=0.0, upper=6.5)
    stl = _combine_pair(profiles, "stl_hist", "stl_hist_player")
    stl = stl.combine_first(_combine_pair(profiles, "stl_season", "stl_season_player")).fillna(0.8).clip(lower=0.0, upper=3.5)
    blk = _combine_pair(profiles, "blk_hist", "blk_hist_player")
    blk = blk.combine_first(_combine_pair(profiles, "blk_season", "blk_season_player")).fillna(0.5).clip(lower=0.0, upper=3.8)
    games_hist = _combine_pair(profiles, "games_hist", "games_hist_player").fillna(0.0).clip(lower=0.0, upper=120.0)

    fga_den = fga.replace(0, np.nan).fillna(1.0)
    min_den = minutes.replace(0, np.nan).fillna(1.0)
    three_rate = (three_pa / fga_den).clip(lower=0.0, upper=1.0)
    usage_proxy = (points / min_den).clip(lower=0.0, upper=2.2)
    assist_potential = (assists / min_den).clip(lower=0.0, upper=1.8)
    rebound_rate = (rebounds / min_den).clip(lower=0.0, upper=2.2)
    drive_rate = ((points / min_den) * (1.0 - three_rate * 0.65)).clip(lower=0.0, upper=2.5)
    rim_rate = (0.44 + (rebound_rate - 0.2) * 0.18 - (three_rate - 0.32) * 0.42).clip(lower=0.1, upper=0.72)
    mid_rate = (1.0 - rim_rate - three_rate).clip(lower=0.05, upper=0.65)
    catch_rate = (three_rate * 0.52 + (0.24 - assist_potential).clip(lower=-0.2, upper=0.3) * 0.22).clip(lower=0.05, upper=0.8)
    pull_rate = (three_rate * 0.38 + usage_proxy * 0.18 + drive_rate * 0.06).clip(lower=0.05, upper=0.82)
    paint_rate = (drive_rate * 0.95 + rim_rate * 0.3).clip(lower=0.0, upper=2.1)
    post_rate = ((rebound_rate * 0.55) + (usage_proxy * 0.12) - (three_rate * 0.28)).clip(lower=0.0, upper=1.7)
    elbow_rate = ((assist_potential * 0.65) + (mid_rate * 0.34)).clip(lower=0.0, upper=1.6)
    offball_rate = ((catch_rate * 0.8) + (three_rate * 0.32)).clip(lower=0.0, upper=1.8)
    defensive_event_rate = ((stl + blk) / min_den).clip(lower=0.0, upper=0.55)

    role_primary = np.select(
        [
            drive_rate.ge(0.45) & assist_potential.ge(0.35),
            catch_rate.ge(0.4) & pull_rate.lt(0.24),
            pull_rate.ge(0.36),
            rim_rate.ge(0.4) & rebound_rate.ge(0.34),
            assist_potential.ge(0.45),
        ],
        ["drive_and_kick", "spot_up", "shot_creator", "interior_finisher", "table_setter"],
        default="balanced",
    )
    scoring_mode = np.select(
        [
            three_rate.ge(0.44),
            rim_rate.ge(0.42),
            mid_rate.ge(0.34),
        ],
        ["perimeter_volume", "paint_pressure", "mid_range"],
        default="mixed",
    )

    coverage_flags = pd.concat(
        [
            _combine_pair(profiles, "points_hist", "points_hist_player").notna().astype(float),
            _combine_pair(profiles, "minutes_hist", "minutes_hist_player").notna().astype(float),
            _combine_pair(profiles, "three_pm_hist", "three_pm_hist_player").notna().astype(float),
            _combine_pair(profiles, "fga_season", "fga_season_player").notna().astype(float),
            _combine_pair(profiles, "three_pa_season", "three_pa_season_player").notna().astype(float),
        ],
        axis=1,
    )
    history_scale = np.log1p(games_hist) / np.log(16.0)
    context_conf = (
        0.32
        + (coverage_flags.mean(axis=1) * 0.38)
        + history_scale.clip(lower=0.0, upper=1.0) * 0.3
    ).clip(lower=0.25, upper=0.92)

    now_iso = datetime.now(timezone.utc).isoformat()
    profiles["playstyle_shot_profile_source"] = "local_inferred_blend"
    profiles["playstyle_primary_role"] = role_primary
    profiles["playstyle_scoring_mode"] = scoring_mode
    profiles["playstyle_rim_rate"] = rim_rate.round(4)
    profiles["playstyle_mid_range_rate"] = mid_rate.round(4)
    profiles["playstyle_three_rate"] = three_rate.round(4)
    profiles["playstyle_catch_shoot_rate"] = catch_rate.round(4)
    profiles["playstyle_pull_up_rate"] = pull_rate.round(4)
    profiles["playstyle_drive_rate"] = drive_rate.round(4)
    profiles["playstyle_assist_potential"] = assist_potential.round(4)
    profiles["playstyle_paint_touch_rate"] = paint_rate.round(4)
    profiles["playstyle_post_touch_rate"] = post_rate.round(4)
    profiles["playstyle_elbow_touch_rate"] = elbow_rate.round(4)
    profiles["playstyle_rebound_chance_rate"] = rebound_rate.round(4)
    profiles["playstyle_offball_activity_rate"] = offball_rate.round(4)
    profiles["playstyle_usage_proxy"] = usage_proxy.round(4)
    profiles["playstyle_defensive_event_rate"] = defensive_event_rate.round(4)
    profiles["playstyle_context_confidence"] = context_conf.round(4)
    profiles["profile_updated_at"] = now_iso
    profiles["profile_source"] = "local_inferred"
    status["records_loaded"] = int(len(profiles))
    return profiles, status


def _fetch_playstyle_context_rows(
    upcoming_frame: pd.DataFrame,
    provider_config: dict,
) -> tuple[pd.DataFrame, dict]:
    status = {
        "enabled": bool(provider_config.get("enabled", True)),
        "source": "NBA Stats playstyle profile feed",
        "rows": 0,
        "cache_rows": 0,
        "fetched_profiles": 0,
        "reused_profiles": 0,
        "local_inferred_profiles": 0,
        "last_error": None,
        "note": None,
    }
    empty_columns = CONTEXT_KEY_COLUMNS + PLAYSTYLE_CONTEXT_COLUMNS
    if not status["enabled"]:
        status["note"] = "Playstyle provider is disabled."
        return pd.DataFrame(columns=empty_columns), status
    if upcoming_frame.empty:
        status["note"] = "No upcoming slate rows are available for playstyle matching."
        return pd.DataFrame(columns=empty_columns), status

    cache_path = Path(provider_config.get("cache_path") or DEFAULT_PLAYSTYLE_CACHE_PATH)
    refresh_seconds = _clamp_interval(
        provider_config.get("refresh_interval_seconds", MIN_PLAYSTYLE_REFRESH_INTERVAL_SECONDS),
        fallback=MIN_PLAYSTYLE_REFRESH_INTERVAL_SECONDS,
        minimum=MIN_PLAYSTYLE_REFRESH_INTERVAL_SECONDS,
    )
    remote_refresh_seconds = _clamp_interval(
        provider_config.get("remote_refresh_interval_seconds", max(refresh_seconds * 2, 900)),
        fallback=max(refresh_seconds * 2, 900),
        minimum=max(refresh_seconds * 2, 900),
    )
    cache_frame = _load_playstyle_cache(cache_path)
    status["cache_rows"] = int(len(cache_frame))

    now_utc = datetime.now(timezone.utc)
    stale_cutoff = now_utc - timedelta(seconds=remote_refresh_seconds)
    latest_cached_at = None
    if not cache_frame.empty and "profile_updated_at" in cache_frame.columns:
        parsed = cache_frame["profile_updated_at"].map(lambda value: _parse_iso_datetime(str(value or "")))
        parsed = parsed.dropna()
        if not parsed.empty:
            latest_cached_at = max(parsed.tolist())
    fetch_due = latest_cached_at is None or latest_cached_at < stale_cutoff

    if fetch_due:
        requested_players = {
            str(value).strip()
            for value in upcoming_frame.get("player_name", pd.Series(dtype=object)).dropna().tolist()
            if str(value).strip()
        }
        try:
            fetched, fetch_status = _build_playstyle_profiles_from_web(
                provider_config=provider_config,
                player_names_filter=requested_players,
            )
            if not fetched.empty:
                cache_frame = fetched
                _save_playstyle_cache(cache_path, cache_frame)
                status["fetched_profiles"] = int(len(cache_frame))
                status["cache_rows"] = int(len(cache_frame))
            else:
                fallback_profiles, fallback_status = _build_playstyle_profiles_from_local(
                    upcoming_frame=upcoming_frame,
                    training_path=Path(provider_config.get("training_data_path") or DEFAULT_TRAINING_UPLOAD_PATH),
                    player_names_filter=requested_players,
                )
                if not fallback_profiles.empty:
                    cache_frame = fallback_profiles
                    _save_playstyle_cache(cache_path, cache_frame)
                    status["local_inferred_profiles"] = int(len(cache_frame))
                    status["cache_rows"] = int(len(cache_frame))
                    status["note"] = "Using local inferred playstyle profiles after live source returned no rows."
                else:
                    status["note"] = "Playstyle source checked, but no rows were returned."
                fallback_error = fallback_status.get("last_error")
                if fallback_error and not status["last_error"]:
                    status["last_error"] = fallback_error
            fetch_error = fetch_status.get("last_error")
            if fetch_error:
                status["last_error"] = fetch_error
        except (HTTPError, URLError, ValueError, OSError, subprocess.SubprocessError) as exc:
            status["last_error"] = _sanitize_error_message(exc)
            if cache_frame.empty:
                fallback_profiles, fallback_status = _build_playstyle_profiles_from_local(
                    upcoming_frame=upcoming_frame,
                    training_path=Path(provider_config.get("training_data_path") or DEFAULT_TRAINING_UPLOAD_PATH),
                    player_names_filter=requested_players,
                )
                if not fallback_profiles.empty:
                    cache_frame = fallback_profiles
                    _save_playstyle_cache(cache_path, cache_frame)
                    status["local_inferred_profiles"] = int(len(cache_frame))
                    status["cache_rows"] = int(len(cache_frame))
                    status["note"] = "Using local inferred playstyle profiles after live fetch failure."
                else:
                    status["note"] = "Using cached playstyle profiles after live fetch failure."
                fallback_error = fallback_status.get("last_error")
                if fallback_error and not status["last_error"]:
                    status["last_error"] = fallback_error
            else:
                status["note"] = "Using cached playstyle profiles after live fetch failure."
    else:
        status["reused_profiles"] = int(len(cache_frame))
        status["note"] = (
            "Reused cached playstyle profiles until next refresh window "
            f"({remote_refresh_seconds}s remote cadence, {refresh_seconds}s merge cadence)."
        )

    if cache_frame.empty:
        return pd.DataFrame(columns=empty_columns), status

    style_columns = ["player_key", "team"] + PLAYSTYLE_CONTEXT_COLUMNS
    cache_working = cache_frame[style_columns].copy()
    cache_working["team"] = cache_working["team"].map(_normalize_team_code).fillna(cache_working["team"])
    key_columns = list(dict.fromkeys(CONTEXT_KEY_COLUMNS + ["player_name", "team"]))
    key_frame = upcoming_frame[key_columns].drop_duplicates().copy()
    key_frame["player_key"] = key_frame["player_name"].map(_normalize_player_key)
    key_frame["team"] = key_frame["team"].map(_normalize_team_code).fillna(key_frame["team"])

    merged = key_frame.merge(
        cache_working,
        on=["player_key", "team"],
        how="left",
    )
    fallback_by_player = (
        cache_working.sort_values("playstyle_context_confidence", ascending=False)
        .drop_duplicates(subset=["player_key"], keep="first")[["player_key"] + PLAYSTYLE_CONTEXT_COLUMNS]
    )
    merged = merged.merge(
        fallback_by_player,
        on="player_key",
        how="left",
        suffixes=("", "_fallback"),
    )
    for column in PLAYSTYLE_CONTEXT_COLUMNS:
        fallback_column = f"{column}_fallback"
        if fallback_column in merged.columns:
            merged[column] = merged[column].combine_first(merged[fallback_column])
            merged = merged.drop(columns=[fallback_column])
    for column in PLAYSTYLE_NUMERIC_COLUMNS:
        merged[column] = pd.to_numeric(merged[column], errors="coerce").round(4)
    for column in ["playstyle_shot_profile_source", "playstyle_primary_role", "playstyle_scoring_mode"]:
        merged[column] = merged[column].astype("string").str.strip()
        merged.loc[merged[column].isin(["", "nan", "none"]), column] = pd.NA

    provider_frame = merged[CONTEXT_KEY_COLUMNS + PLAYSTYLE_CONTEXT_COLUMNS].copy()
    numeric_presence = provider_frame[PLAYSTYLE_NUMERIC_COLUMNS].notna().any(axis=1)
    role_presence = provider_frame["playstyle_primary_role"].astype("string").str.strip().fillna("").ne("")
    has_context = numeric_presence | role_presence
    provider_frame = provider_frame.loc[has_context].drop_duplicates(subset=CONTEXT_KEY_COLUMNS, keep="last")
    aligned = _align_provider_rows_to_upcoming(upcoming_frame, provider_frame)
    status["rows"] = int(len(aligned))
    if aligned.empty and not status.get("note"):
        status["note"] = "Playstyle profiles were loaded but did not align to current slate keys."
    return aligned, status


def _apply_shot_style_context(
    upcoming_frame: pd.DataFrame,
    training_path: Path,
) -> tuple[pd.DataFrame, dict]:
    if upcoming_frame.empty:
        return upcoming_frame, {
            "rows_with_shot_profile": 0,
            "rows_with_opponent_profile": 0,
            "rows_with_rebound_environment": 0,
            "teams_evaluated": 0,
        }

    working = upcoming_frame.copy().reset_index(drop=True)
    working["player_key"] = working["player_name"].map(_normalize_player_key)
    working["team"] = working["team"].map(_normalize_team_code).fillna(working["team"])
    working["opponent"] = working["opponent"].map(_normalize_team_code).fillna(working["opponent"])
    working["height_inches"] = pd.to_numeric(working.get("height_inches"), errors="coerce")
    working["game_total"] = pd.to_numeric(working.get("game_total"), errors="coerce")
    working = working.drop(columns=SHOT_STYLE_CONTEXT_COLUMNS, errors="ignore")

    try:
        season_priors = load_season_priors()
    except Exception:
        season_priors = pd.DataFrame()
    if season_priors.empty:
        season_priors = pd.DataFrame(
            columns=[
                "player_name",
                "team",
                "fga_season",
                "three_pa_season",
                "fta_season",
                "fg_pct_season",
                "three_pct_season",
                "ft_pct_season",
                "min_season",
                "pts_season",
                "tov_season",
            ]
        )

    season_priors = season_priors.copy()
    season_priors["player_key"] = season_priors["player_name"].map(_normalize_player_key)
    season_priors["team"] = season_priors["team"].map(_normalize_team_code).fillna(season_priors["team"])
    prior_columns = [
        "player_key",
        "team",
        "fga_season",
        "three_pa_season",
        "fta_season",
        "fg_pct_season",
        "three_pct_season",
        "ft_pct_season",
        "min_season",
        "pts_season",
        "tov_season",
    ]
    for column in prior_columns:
        if column not in season_priors.columns:
            season_priors[column] = pd.NA
    season_priors = season_priors[prior_columns].drop_duplicates(subset=["player_key", "team"], keep="last")

    enriched = working.merge(
        season_priors,
        on=["player_key", "team"],
        how="left",
    )

    def _ratio(series: pd.Series, fallback: float) -> pd.Series:
        numeric = pd.to_numeric(series, errors="coerce")
        numeric = numeric.where(numeric <= 1.5, numeric / 100.0)
        return numeric.fillna(fallback).clip(lower=0.0, upper=1.0)

    def _series(column: str, default: float = np.nan) -> pd.Series:
        if column in enriched.columns:
            return pd.to_numeric(enriched[column], errors="coerce")
        return pd.Series(default, index=enriched.index, dtype=float)

    fga = pd.to_numeric(enriched.get("fga_season"), errors="coerce")
    pts = pd.to_numeric(enriched.get("pts_season"), errors="coerce")
    fga = fga.fillna((pts / 1.65).clip(lower=3.0, upper=28.0))
    fga = fga.clip(lower=1.0, upper=35.0)
    three_pa = pd.to_numeric(enriched.get("three_pa_season"), errors="coerce").fillna((fga * 0.38).clip(lower=0.2))
    three_pa = three_pa.clip(lower=0.0, upper=20.0)
    fta = pd.to_numeric(enriched.get("fta_season"), errors="coerce").fillna((fga * 0.22).clip(lower=0.2))
    fta = fta.clip(lower=0.0, upper=16.0)

    fg_pct = _ratio(enriched.get("fg_pct_season"), 0.46).clip(lower=0.32, upper=0.72)
    ft_pct = _ratio(enriched.get("ft_pct_season"), 0.78).clip(lower=0.4, upper=0.97)
    minutes = (
        pd.to_numeric(enriched.get("min_season"), errors="coerce")
        .fillna(pd.to_numeric(enriched.get("expected_minutes"), errors="coerce"))
        .fillna(26.0)
        .clip(lower=6.0, upper=42.0)
    )
    tov = pd.to_numeric(enriched.get("tov_season"), errors="coerce").fillna(1.8).clip(lower=0.0, upper=6.5)
    pts = pts.fillna((fga * 1.28).clip(lower=2.0, upper=42.0))

    three_share = (three_pa / fga.replace(0, np.nan)).fillna(0.34).clip(lower=0.0, upper=1.0)
    usage_proxy = (pts / minutes.clip(lower=1.0)).fillna(0.75).clip(lower=0.0, upper=1.6)
    playstyle_confidence = _series("playstyle_context_confidence", 0.0).fillna(0.0).clip(lower=0.0, upper=1.0)
    playstyle_three_rate = _series("playstyle_three_rate")
    playstyle_usage_proxy = _series("playstyle_usage_proxy")
    playstyle_catch_rate = _series("playstyle_catch_shoot_rate")
    playstyle_pull_rate = _series("playstyle_pull_up_rate")
    playstyle_drive_rate = _series("playstyle_drive_rate")
    playstyle_assist_potential = _series("playstyle_assist_potential")
    playstyle_rebound_rate = _series("playstyle_rebound_chance_rate")
    blend_weight = (0.25 + (playstyle_confidence * 0.45)).clip(lower=0.0, upper=0.68)
    if playstyle_three_rate.notna().any():
        three_share = three_share.where(
            playstyle_three_rate.isna(),
            (three_share * (1.0 - blend_weight)) + (playstyle_three_rate.clip(lower=0.0, upper=1.0) * blend_weight),
        )
    if playstyle_usage_proxy.notna().any():
        usage_proxy = usage_proxy.where(
            playstyle_usage_proxy.isna(),
            (usage_proxy * (1.0 - blend_weight)) + (playstyle_usage_proxy.clip(lower=0.0, upper=1.8) * blend_weight),
        )

    arc_score = (0.18 + (three_share * 0.64) + (ft_pct * 0.18)).clip(lower=0.0, upper=1.0)
    release_score = (
        0.2
        + ((usage_proxy / 1.3).clip(lower=0.0, upper=1.0) * 0.42)
        + ((minutes / 36.0).clip(lower=0.0, upper=1.0) * 0.22)
        + ((tov / 3.2).clip(lower=0.0, upper=1.0) * 0.16)
    ).clip(lower=0.0, upper=1.0)
    release_delta = (
        ((playstyle_pull_rate.fillna(0.25) - playstyle_catch_rate.fillna(0.25)) * 0.22)
        + ((playstyle_drive_rate.fillna(0.35) - 0.35) * 0.08)
    ) * playstyle_confidence
    arc_score = (arc_score + ((playstyle_three_rate.fillna(three_share) - 0.35) * 0.12 * playstyle_confidence)).clip(lower=0.0, upper=1.0)
    release_score = (release_score + release_delta).clip(lower=0.0, upper=1.0)
    volume_index = fga.clip(lower=0.0, upper=35.0)
    miss_pressure = (fga * (1.0 - fg_pct) + (0.44 * fta * (1.0 - ft_pct))).clip(lower=0.0, upper=25.0)
    miss_pressure = (
        miss_pressure
        + (playstyle_drive_rate.fillna(0.0) * 0.9)
        - (playstyle_assist_potential.fillna(0.0) * 0.55)
        + (playstyle_rebound_rate.fillna(0.0) * 0.35)
    ).clip(lower=0.0, upper=25.0)

    arc_label = np.select(
        [arc_score.ge(0.67), arc_score.ge(0.44)],
        ["high_arc", "medium_arc"],
        default="low_arc",
    )
    release_label = np.select(
        [release_score.ge(0.68), release_score.ge(0.46)],
        ["fast_release", "normal_release"],
        default="slow_release",
    )

    enriched["shot_style_arc_score"] = arc_score
    enriched["shot_style_arc_label"] = arc_label
    enriched["shot_style_release_score"] = release_score
    enriched["shot_style_release_label"] = release_label
    enriched["shot_style_volume_index"] = volume_index
    enriched["shot_style_miss_pressure"] = miss_pressure

    team_style = (
        enriched.groupby(["game_date", "team"], as_index=False)
        .agg(
            team_player_count=("player_key", "nunique"),
            team_shot_miss_pressure=("shot_style_miss_pressure", "sum"),
            team_shot_volume_index=("shot_style_volume_index", "sum"),
            team_avg_height_inches=("height_inches", "mean"),
        )
        .fillna({"team_player_count": 0})
    )
    team_height_fallback: dict[str, float] = {}
    # Avoid reading the full historical game log on every sync loop. This is
    # hot-path code and full-history scans can stall live refresh cadence.
    if {"team", "height_inches"}.issubset(set(enriched.columns)):
        current_heights = enriched[["team", "height_inches"]].copy()
        current_heights["team"] = current_heights["team"].map(_normalize_team_code).fillna(current_heights["team"])
        current_heights["height_inches"] = pd.to_numeric(current_heights["height_inches"], errors="coerce")
        current_heights = current_heights.dropna(subset=["team", "height_inches"])
        if not current_heights.empty:
            team_height_fallback = current_heights.groupby("team")["height_inches"].mean().round(3).to_dict()
    if team_height_fallback:
        team_style["team_avg_height_inches"] = team_style["team_avg_height_inches"].fillna(
            team_style["team"].map(team_height_fallback)
        )
    team_scale = (5.0 / team_style["team_player_count"].clip(lower=5.0, upper=14.0)).clip(lower=0.35, upper=1.0)
    team_style["team_shot_miss_pressure"] = (
        team_style["team_shot_miss_pressure"] * team_scale
    ).clip(lower=0.0, upper=36.0)
    team_style["team_shot_volume_index"] = (
        team_style["team_shot_volume_index"] * team_scale
    ).clip(lower=0.0, upper=55.0)

    opponent_style = team_style.rename(
        columns={
            "team": "opponent",
            "team_shot_miss_pressure": "opponent_shot_miss_pressure",
            "team_shot_volume_index": "opponent_shot_volume_index",
            "team_avg_height_inches": "opponent_avg_height_inches",
        }
    )[["game_date", "opponent", "opponent_shot_miss_pressure", "opponent_shot_volume_index", "opponent_avg_height_inches"]]

    enriched = enriched.merge(
        team_style[["game_date", "team", "team_shot_miss_pressure", "team_shot_volume_index", "team_avg_height_inches"]],
        on=["game_date", "team"],
        how="left",
    )
    enriched = enriched.merge(
        opponent_style,
        on=["game_date", "opponent"],
        how="left",
    )

    player_height = _series("height_inches")
    player_height = player_height.combine_first(_series("team_avg_height_inches")).fillna(78.0)
    opponent_avg_height = _series("opponent_avg_height_inches", 78.0).fillna(78.0)
    height_advantage = (opponent_avg_height - player_height).clip(lower=-12.0, upper=12.0)

    low_arc_exposure = (0.58 - arc_score).clip(lower=0.0, upper=0.58)
    high_arc_buffer = (arc_score - 0.58).clip(lower=0.0, upper=0.42)
    tall_mismatch_penalty = (
        (height_advantage.clip(lower=0.0) * low_arc_exposure / 5.5)
        - (height_advantage.clip(lower=0.0) * high_arc_buffer / 8.0)
    ).clip(lower=-1.2, upper=1.6)

    game_total = _series("game_total", 225.0).fillna(225.0).clip(lower=170.0, upper=280.0)
    pace_factor = ((game_total - 225.0) / 35.0).clip(lower=-1.0, upper=1.0)
    pace_bonus = (pace_factor * (release_score - 0.5) * 2.0).clip(lower=-1.2, upper=1.2)

    team_miss = _series("team_shot_miss_pressure").fillna((miss_pressure * 2.2).clip(lower=2.0))
    team_miss = team_miss.clip(lower=0.0, upper=36.0)
    opponent_miss = _series("opponent_shot_miss_pressure").fillna(team_miss.median())
    opponent_miss = opponent_miss.clip(lower=0.0, upper=36.0)
    rebound_environment = ((opponent_miss - (team_miss * 0.42)) / 12.0).clip(lower=-1.6, upper=1.6)

    enriched["team_shot_miss_pressure"] = team_miss
    enriched["opponent_shot_miss_pressure"] = opponent_miss
    enriched["opponent_avg_height_inches"] = opponent_avg_height
    enriched["opponent_height_advantage_inches"] = height_advantage
    enriched["shot_style_tall_mismatch_penalty"] = tall_mismatch_penalty
    enriched["shot_style_pace_bonus"] = pace_bonus
    enriched["shot_style_rebound_environment"] = rebound_environment

    for column in SHOT_STYLE_CONTEXT_COLUMNS:
        working[column] = enriched[column]

    working["shot_style_arc_score"] = pd.to_numeric(working["shot_style_arc_score"], errors="coerce").round(4)
    working["shot_style_release_score"] = pd.to_numeric(working["shot_style_release_score"], errors="coerce").round(4)
    working["shot_style_volume_index"] = pd.to_numeric(working["shot_style_volume_index"], errors="coerce").round(3)
    working["shot_style_miss_pressure"] = pd.to_numeric(working["shot_style_miss_pressure"], errors="coerce").round(3)
    working["team_shot_miss_pressure"] = pd.to_numeric(working["team_shot_miss_pressure"], errors="coerce").round(3)
    working["opponent_shot_miss_pressure"] = pd.to_numeric(working["opponent_shot_miss_pressure"], errors="coerce").round(3)
    working["opponent_avg_height_inches"] = pd.to_numeric(working["opponent_avg_height_inches"], errors="coerce").round(3)
    working["opponent_height_advantage_inches"] = pd.to_numeric(
        working["opponent_height_advantage_inches"],
        errors="coerce",
    ).round(3)
    working["shot_style_tall_mismatch_penalty"] = pd.to_numeric(
        working["shot_style_tall_mismatch_penalty"],
        errors="coerce",
    ).round(4)
    working["shot_style_pace_bonus"] = pd.to_numeric(working["shot_style_pace_bonus"], errors="coerce").round(4)
    working["shot_style_rebound_environment"] = pd.to_numeric(
        working["shot_style_rebound_environment"],
        errors="coerce",
    ).round(4)

    rows_with_shot_profile = int(pd.to_numeric(working.get("shot_style_arc_score"), errors="coerce").notna().sum())
    rows_with_opponent_profile = int(pd.to_numeric(working.get("opponent_avg_height_inches"), errors="coerce").notna().sum())
    rows_with_rebound_environment = int(
        pd.to_numeric(working.get("shot_style_rebound_environment"), errors="coerce").notna().sum()
    )

    return working.drop(columns=["player_key"], errors="ignore"), {
        "rows_with_shot_profile": rows_with_shot_profile,
        "rows_with_opponent_profile": rows_with_opponent_profile,
        "rows_with_rebound_environment": rows_with_rebound_environment,
        "teams_evaluated": int(working[["game_date", "team"]].drop_duplicates().shape[0]),
    }


def _load_player_profile_cache(path: Path) -> pd.DataFrame:
    columns = ["player_key", "player_name", "hometown_teams", "profile_updated_at", "profile_source"]
    if not path.exists():
        return pd.DataFrame(columns=columns)
    try:
        frame = pd.read_csv(path)
    except (OSError, pd.errors.EmptyDataError):
        return pd.DataFrame(columns=columns)
    if frame.empty:
        return pd.DataFrame(columns=columns)
    working = frame.copy()
    for column in columns:
        if column not in working.columns:
            working[column] = pd.NA
    working["player_key"] = working["player_key"].fillna("").astype(str).str.strip()
    working["player_name"] = working["player_name"].fillna("").astype(str).str.strip()
    working["hometown_teams"] = working["hometown_teams"].fillna("").astype(str).str.strip()
    working["profile_updated_at"] = working["profile_updated_at"].fillna("").astype(str).str.strip()
    working["profile_source"] = working["profile_source"].fillna("").astype(str).str.strip()
    return working[columns].drop_duplicates(subset=["player_key"], keep="last")


def _save_player_profile_cache(path: Path, frame: pd.DataFrame) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if frame.empty:
        pd.DataFrame(columns=["player_key", "player_name", "hometown_teams", "profile_updated_at", "profile_source"]).to_csv(path, index=False)
        return
    working = frame.copy()
    working = working.drop_duplicates(subset=["player_key"], keep="last").sort_values(["player_name", "player_key"])
    working.to_csv(path, index=False)


def _infer_hometown_teams_from_text(text: str) -> list[str]:
    normalized = str(text or "").lower().strip()
    if not normalized:
        return []
    matches: list[str] = []
    for team_code, terms in TEAM_CITY_TERMS_BY_CODE.items():
        for term in terms:
            if not term:
                continue
            if re.search(rf"\b{re.escape(term.lower())}\b", normalized):
                matches.append(team_code)
                break
    ordered: list[str] = []
    for team_code in matches:
        if team_code not in ordered:
            ordered.append(team_code)
    return ordered[:4]


def _refresh_player_profile_cache(
    upcoming_frame: pd.DataFrame,
    provider_config: dict,
    season_priors: pd.DataFrame,
) -> tuple[pd.DataFrame, dict]:
    cache_path = Path(provider_config.get("cache_path") or DEFAULT_PROFILE_CACHE_PATH)
    status = {
        "enabled": bool(provider_config.get("enabled", True)),
        "source": "Wikipedia player profile summary",
        "cache_path": str(cache_path),
        "cache_rows": 0,
        "fetched_profiles": 0,
        "reused_profiles": 0,
        "last_error": None,
        "note": None,
    }
    if not status["enabled"]:
        cache = _load_player_profile_cache(cache_path)
        status["cache_rows"] = int(len(cache))
        status["note"] = "Player profile provider is disabled."
        return cache, status

    cache = _load_player_profile_cache(cache_path)
    refresh_seconds = _clamp_interval(
        provider_config.get(
            "refresh_interval_seconds",
            MIN_PROFILE_REFRESH_INTERVAL_SECONDS,
        ),
        fallback=MIN_PROFILE_REFRESH_INTERVAL_SECONDS,
        minimum=MIN_PROFILE_REFRESH_INTERVAL_SECONDS,
    )
    max_players = max(20, _coerce_positive_int(provider_config.get("max_players_per_cycle", 120), 120))
    now_utc = datetime.now(timezone.utc)
    stale_cutoff = now_utc - timedelta(seconds=refresh_seconds)
    summary_template = str(provider_config.get("wikipedia_summary_template") or "").strip()
    if not summary_template:
        status["cache_rows"] = int(len(cache))
        status["note"] = "Wikipedia summary URL template is missing."
        return cache, status

    candidates = upcoming_frame[["player_name", "team"]].drop_duplicates().copy()
    candidates["player_name"] = candidates["player_name"].fillna("").astype(str).str.strip()
    candidates = candidates[candidates["player_name"].astype(bool)]
    if "expected_minutes" in upcoming_frame.columns:
        candidate_minutes = upcoming_frame[["player_name", "expected_minutes"]].copy()
        candidate_minutes["expected_minutes"] = pd.to_numeric(candidate_minutes["expected_minutes"], errors="coerce")
        candidates = candidates.merge(
            candidate_minutes.groupby("player_name", as_index=False)["expected_minutes"].max(),
            on="player_name",
            how="left",
        )
    else:
        candidates["expected_minutes"] = pd.NA
    if "starter_probability" in upcoming_frame.columns:
        candidate_starter = upcoming_frame[["player_name", "starter_probability"]].copy()
        candidate_starter["starter_probability"] = pd.to_numeric(candidate_starter["starter_probability"], errors="coerce")
        candidates = candidates.merge(
            candidate_starter.groupby("player_name", as_index=False)["starter_probability"].max(),
            on="player_name",
            how="left",
        )
    else:
        candidates["starter_probability"] = pd.NA

    if not season_priors.empty:
        priors_subset = season_priors[["player_name", "pts_season"]].drop_duplicates(subset=["player_name"], keep="last").copy()
        priors_subset["pts_season"] = pd.to_numeric(priors_subset["pts_season"], errors="coerce")
        candidates = candidates.merge(priors_subset, on="player_name", how="left")
    else:
        candidates["pts_season"] = pd.NA

    candidates = candidates.sort_values(
        ["starter_probability", "expected_minutes", "pts_season", "player_name"],
        ascending=[False, False, False, True],
    ).head(max_players)
    candidates["player_key"] = candidates["player_name"].map(_normalize_player_key)

    if cache.empty:
        cache_lookup = {}
    else:
        cache_lookup = {str(row["player_key"]): row for _, row in cache.iterrows()}

    updates: list[dict] = []
    for _, row in candidates.iterrows():
        player_name = str(row.get("player_name") or "").strip()
        player_key = str(row.get("player_key") or "")
        if not player_name or not player_key:
            continue
        cached = cache_lookup.get(player_key)
        if cached is not None:
            refreshed_at = _parse_iso_datetime(str(cached.get("profile_updated_at") or ""))
            if refreshed_at is not None and refreshed_at >= stale_cutoff:
                status["reused_profiles"] = int(status.get("reused_profiles", 0) + 1)
                updates.append(dict(cached))
                continue

        titles = [player_name.replace(" ", "_"), f"{player_name.replace(' ', '_')}_(basketball)"]
        extract_text = ""
        profile_source = ""
        for title in titles:
            try:
                payload = fetch_json(summary_template.format(title=quote_plus(title)))
            except (HTTPError, URLError, ValueError, OSError, subprocess.SubprocessError) as exc:
                status["last_error"] = _sanitize_error_message(exc)
                continue
            if str(payload.get("type") or "").lower() == "disambiguation":
                continue
            extract_text = str(payload.get("extract") or "").strip()
            if extract_text:
                profile_source = str(payload.get("content_urls", {}).get("desktop", {}).get("page") or "")
                if not profile_source:
                    profile_source = str(payload.get("content_urls", {}).get("mobile", {}).get("page") or "")
                break

        hometown_teams = _infer_hometown_teams_from_text(extract_text)
        updates.append(
            {
                "player_key": player_key,
                "player_name": player_name,
                "hometown_teams": "|".join(hometown_teams),
                "profile_updated_at": now_utc.isoformat(),
                "profile_source": profile_source or "wikipedia",
            }
        )
        status["fetched_profiles"] = int(status.get("fetched_profiles", 0) + 1)

    updates_frame = pd.DataFrame(updates)
    if updates_frame.empty:
        merged_cache = cache
    elif cache.empty:
        merged_cache = updates_frame
    else:
        merged_cache = (
            pd.concat([cache, updates_frame], ignore_index=True, sort=False)
            .drop_duplicates(subset=["player_key"], keep="last")
        )
    _save_player_profile_cache(cache_path, merged_cache)
    status["cache_rows"] = int(len(merged_cache))
    if status["fetched_profiles"] == 0 and status["reused_profiles"] == 0:
        status["note"] = "No player profiles were selected for this cycle."
    return merged_cache, status


def _apply_home_and_hometown_context(
    upcoming_frame: pd.DataFrame,
    training_path: Path,
    profile_provider_config: dict,
) -> tuple[pd.DataFrame, dict]:
    if upcoming_frame.empty:
        return upcoming_frame, {"rows_with_home_context": 0, "rows_with_hometown_context": 0}

    working = upcoming_frame.copy()
    working["player_key"] = working["player_name"].map(_normalize_player_key)
    working["home_numeric"] = pd.to_numeric(working.get("home"), errors="coerce").fillna(0.0)
    working["team"] = working["team"].map(_normalize_team_code).fillna(working["team"])
    working["opponent"] = working["opponent"].map(_normalize_team_code).fillna(working["opponent"])

    for column in [
        "home_court_points_boost",
        "home_court_minutes_boost",
        "hometown_game_flag",
        "hometown_advantage_score",
    ]:
        if column not in working.columns:
            working[column] = pd.NA

    rows_with_home_context = 0
    try:
        history = load_dataset(training_path)
    except Exception:
        history = pd.DataFrame()
    if not history.empty and {"player_name", "home", "points"}.issubset(set(history.columns)):
        history_working = history.copy()
        history_working["player_key"] = history_working["player_name"].map(_normalize_player_key)
        history_working["home"] = pd.to_numeric(history_working["home"], errors="coerce").fillna(0.0)
        history_working["points"] = pd.to_numeric(history_working["points"], errors="coerce")
        if "minutes" in history_working.columns:
            history_working["minutes"] = pd.to_numeric(history_working["minutes"], errors="coerce")
        else:
            history_working["minutes"] = pd.NA
        valid_history = history_working.dropna(subset=["player_key", "points"])
        if not valid_history.empty:
            home_avg = (
                valid_history[valid_history["home"] >= 0.5]
                .groupby("player_key", as_index=False)[["points", "minutes"]]
                .mean()
                .rename(columns={"points": "home_points_avg", "minutes": "home_minutes_avg"})
            )
            away_avg = (
                valid_history[valid_history["home"] < 0.5]
                .groupby("player_key", as_index=False)[["points", "minutes"]]
                .mean()
                .rename(columns={"points": "away_points_avg", "minutes": "away_minutes_avg"})
            )
            splits = home_avg.merge(away_avg, on="player_key", how="outer")
            splits["points_advantage"] = (
                pd.to_numeric(splits["home_points_avg"], errors="coerce").fillna(0.0)
                - pd.to_numeric(splits["away_points_avg"], errors="coerce").fillna(0.0)
            ).clip(lower=-6.0, upper=6.0)
            splits["minutes_advantage"] = (
                pd.to_numeric(splits["home_minutes_avg"], errors="coerce").fillna(0.0)
                - pd.to_numeric(splits["away_minutes_avg"], errors="coerce").fillna(0.0)
            ).clip(lower=-4.0, upper=4.0)
            working = working.merge(
                splits[["player_key", "points_advantage", "minutes_advantage"]],
                on="player_key",
                how="left",
            )
            points_adv = pd.to_numeric(working["points_advantage"], errors="coerce").fillna(0.0)
            minutes_adv = pd.to_numeric(working["minutes_advantage"], errors="coerce").fillna(0.0)
            home_flag = working["home_numeric"].ge(0.5)
            working["home_court_points_boost"] = points_adv.where(home_flag, -0.35 * points_adv).clip(lower=-5.0, upper=5.0)
            working["home_court_minutes_boost"] = minutes_adv.where(home_flag, -0.4 * minutes_adv).clip(lower=-3.5, upper=3.5)
            rows_with_home_context = int(working["home_court_points_boost"].notna().sum())
            working = working.drop(columns=["points_advantage", "minutes_advantage"], errors="ignore")

    try:
        season_priors = load_season_priors()
    except Exception:
        season_priors = pd.DataFrame()
    profiles_cache, profile_status = _refresh_player_profile_cache(
        working,
        profile_provider_config,
        season_priors,
    )
    hometown_lookup: dict[str, set[str]] = {}
    for _, row in profiles_cache.iterrows():
        player_key = str(row.get("player_key") or "").strip()
        if not player_key:
            continue
        hometown_codes = {
            _normalize_team_code(token)
            for token in str(row.get("hometown_teams") or "").split("|")
            if _normalize_team_code(token)
        }
        hometown_lookup[player_key] = hometown_codes

    hometown_flags: list[float] = []
    hometown_scores: list[float] = []
    for _, row in working.iterrows():
        player_key = str(row.get("player_key") or "")
        hometown_teams = hometown_lookup.get(player_key, set())
        team_code = _normalize_team_code(row.get("team")) or ""
        opponent_code = _normalize_team_code(row.get("opponent")) or ""
        home_flag = _safe_float_value(row.get("home_numeric"), default=0.0) >= 0.5
        hometown_flag = 0.0
        hometown_score = 0.0
        if hometown_teams:
            if (not home_flag) and opponent_code in hometown_teams:
                hometown_flag = 1.0
                hometown_score = 1.0
            elif home_flag and team_code in hometown_teams:
                hometown_flag = 1.0
                hometown_score = 0.4
            elif opponent_code in hometown_teams:
                hometown_score = 0.2
        hometown_flags.append(hometown_flag)
        hometown_scores.append(hometown_score)

    working["hometown_game_flag"] = hometown_flags
    working["hometown_advantage_score"] = hometown_scores
    rows_with_hometown_context = int(sum(1 for value in hometown_flags if value >= 1.0))
    working = working.drop(columns=["player_key", "home_numeric"], errors="ignore")
    return working, {
        "rows_with_home_context": int(rows_with_home_context),
        "rows_with_hometown_context": rows_with_hometown_context,
        "profile_cache_rows": int(profile_status.get("cache_rows", 0)),
        "profiles_fetched": int(profile_status.get("fetched_profiles", 0)),
        "profiles_reused": int(profile_status.get("reused_profiles", 0)),
        "profile_last_error": profile_status.get("last_error"),
    }


def _strip_html_text(value: str) -> str:
    if not value:
        return ""
    stripped = re.sub(r"<[^>]+>", " ", value)
    stripped = html.unescape(stripped)
    return re.sub(r"\s+", " ", stripped).strip()


def _parse_rss_datetime(value: str | None) -> datetime | None:
    if not value:
        return None
    raw = str(value).strip()
    if not raw:
        return None
    try:
        parsed = parsedate_to_datetime(raw)
    except (TypeError, ValueError, IndexError):
        parsed = None
    if parsed is None:
        parsed = _parse_iso_datetime(raw)
    if parsed is None:
        return None
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _parse_rss_items(feed_text: str, source_label: str) -> list[dict]:
    if not feed_text.strip():
        return []
    try:
        root = ET.fromstring(feed_text)
    except ET.ParseError:
        return []

    items: list[dict] = []
    for item in root.findall(".//item"):
        title = _strip_html_text(item.findtext("title", default=""))
        link = _strip_html_text(item.findtext("link", default=""))
        description = _strip_html_text(item.findtext("description", default=""))
        pub_date = _parse_rss_datetime(item.findtext("pubDate", default=""))
        source = _strip_html_text(item.findtext("source", default="")) or source_label
        items.append(
            {
                "title": title,
                "link": link,
                "description": description,
                "published_at": pub_date.isoformat() if pub_date else None,
                "source": source,
            }
        )
    return items


def _news_rss_urls(
    upcoming_frame: pd.DataFrame,
    provider_config: dict,
    season_priors: pd.DataFrame,
) -> list[tuple[str, str]]:
    urls: list[tuple[str, str]] = []
    if bool(provider_config.get("espn_rss_enabled", True)):
        espn_url = str(provider_config.get("espn_rss_url") or "").strip()
        if espn_url:
            urls.append(("espn_rss", espn_url))
    if bool(provider_config.get("rotowire_rss_enabled", True)):
        rotowire_url = str(provider_config.get("rotowire_rss_url") or "").strip()
        if rotowire_url:
            urls.append(("rotowire_rss", rotowire_url))

    if not bool(provider_config.get("google_news_enabled", True)):
        return urls

    template = str(provider_config.get("google_news_template") or "").strip()
    if not template:
        return urls

    max_queries = _coerce_positive_int(provider_config.get("max_queries_per_cycle", 18), 18)
    queries: list[str] = [
        "NBA injury report",
        "NBA starting lineup",
        "NBA minutes restriction",
    ]

    team_codes = sorted({_normalize_team_code(value) for value in upcoming_frame.get("team", pd.Series(dtype=object)).dropna().tolist() if _normalize_team_code(value)})
    for team_code in team_codes:
        team_name = TEAM_FULL_NAMES_BY_CODE.get(team_code)
        if team_name:
            queries.append(f"{team_name} injury report")
            queries.append(f"{team_name} starting lineup")

    if not season_priors.empty:
        top_priors = season_priors.copy()
        if "pts_season" in top_priors.columns:
            top_priors["pts_season"] = pd.to_numeric(top_priors["pts_season"], errors="coerce").fillna(0.0)
            top_priors = top_priors.sort_values(["team", "pts_season"], ascending=[True, False])
        top_players = (
            top_priors.groupby("team", as_index=False)
            .head(2)["player_name"]
            .dropna()
            .astype(str)
            .str.strip()
            .tolist()
        )
        for player_name in top_players:
            if player_name:
                queries.append(f"\"{player_name}\" NBA status")

    deduped_queries: list[str] = []
    seen_queries: set[str] = set()
    for query in queries:
        normalized = re.sub(r"\s+", " ", query.strip().lower())
        if not normalized or normalized in seen_queries:
            continue
        seen_queries.add(normalized)
        deduped_queries.append(query.strip())
        if len(deduped_queries) >= max_queries:
            break

    for query in deduped_queries:
        url = template.format(query=quote_plus(query))
        urls.append((f"google_news::{query}", url))
    return urls


def _fetch_news_context_rows(
    upcoming_frame: pd.DataFrame,
    provider_config: dict,
) -> tuple[pd.DataFrame, dict]:
    status = {
        "enabled": bool(provider_config.get("enabled", True)),
        "source": "RSS/News aggregation",
        "queries_used": 0,
        "articles_loaded": 0,
        "rows": 0,
        "last_error": None,
        "note": None,
    }
    empty_columns = CONTEXT_KEY_COLUMNS + NEWS_CONTEXT_COLUMNS
    if not status["enabled"]:
        status["note"] = "News provider is disabled."
        return pd.DataFrame(columns=empty_columns), status
    if upcoming_frame.empty:
        status["note"] = "No upcoming/live slate rows are available for news matching."
        return pd.DataFrame(columns=empty_columns), status

    try:
        season_priors = load_season_priors()
    except Exception:
        season_priors = pd.DataFrame(columns=["player_name", "team", "pts_season"])

    urls = _news_rss_urls(upcoming_frame, provider_config, season_priors)
    if not urls:
        status["note"] = "No news URLs are configured."
        return pd.DataFrame(columns=empty_columns), status

    lookback_hours = _coerce_positive_int(provider_config.get("lookback_hours", 24), 24)
    request_timeout_seconds = _coerce_positive_int(
        provider_config.get("request_timeout_seconds", DEFAULT_PROVIDER_REQUEST_TIMEOUT_SECONDS),
        DEFAULT_PROVIDER_REQUEST_TIMEOUT_SECONDS,
    )
    max_runtime_seconds = _coerce_positive_int(
        provider_config.get("max_runtime_seconds", DEFAULT_NEWS_MAX_RUNTIME_SECONDS),
        DEFAULT_NEWS_MAX_RUNTIME_SECONDS,
    )
    now_utc = datetime.now(timezone.utc)
    lookback_cutoff = now_utc - timedelta(hours=max(1, lookback_hours))
    max_articles_per_query = _coerce_positive_int(provider_config.get("max_articles_per_query", 25), 25)
    started_at_monotonic = time.monotonic()

    all_items: list[dict] = []
    status["queries_used"] = 0
    for source_label, url in urls:
        if max_runtime_seconds > 0 and (time.monotonic() - started_at_monotonic) >= max_runtime_seconds:
            status["note"] = (
                f"News refresh runtime cap reached ({max_runtime_seconds}s). "
                "Continuing in the next sync cycle."
            )
            break
        status["queries_used"] = int(status["queries_used"]) + 1
        try:
            feed_text = fetch_text(url, timeout=request_timeout_seconds)
        except (HTTPError, URLError, ValueError, OSError, subprocess.SubprocessError) as exc:
            status["last_error"] = _sanitize_error_message(exc)
            continue
        items = _parse_rss_items(feed_text, source_label=source_label)
        if not items:
            continue
        all_items.extend(items[:max_articles_per_query])

    if not all_items:
        if not status.get("note"):
            status["note"] = "News feeds were checked, but no articles were loaded."
        return pd.DataFrame(columns=empty_columns), status

    filtered_items: list[dict] = []
    for item in all_items:
        published_at = _parse_iso_datetime(str(item.get("published_at") or ""))
        if published_at and published_at < lookback_cutoff:
            continue
        filtered_items.append(item)
    if not filtered_items:
        status["note"] = "News feeds loaded, but no recent articles were found inside the lookback window."
        return pd.DataFrame(columns=empty_columns), status

    status["articles_loaded"] = int(len(filtered_items))
    player_lookup = upcoming_frame[["player_name", "team"]].drop_duplicates().copy()
    player_lookup["player_key"] = player_lookup["player_name"].map(_normalize_player_key)
    player_lookup["team"] = player_lookup["team"].map(_normalize_team_code).fillna(player_lookup["team"])
    player_lookup = player_lookup[player_lookup["player_key"].astype(bool)]

    team_terms: dict[str, list[str]] = {}
    for team_code in sorted(player_lookup["team"].dropna().astype(str).unique()):
        full_name = TEAM_FULL_NAMES_BY_CODE.get(team_code, "")
        terms = [team_code.lower()]
        if full_name:
            terms.append(full_name.lower())
            parts = [part.strip().lower() for part in full_name.split(" ") if part.strip()]
            if parts:
                terms.append(parts[-1])
        team_terms[team_code] = sorted(set(term for term in terms if term))

    player_stats: dict[str, dict[str, float]] = {}
    team_stats: dict[str, dict[str, float]] = {}
    for item in filtered_items:
        text = f"{item.get('title', '')} {item.get('description', '')}".strip().lower()
        if not text:
            continue

        injury_hit = 1.0 if NEWS_INJURY_PATTERN.search(text) else 0.0
        starting_hit = 1.0 if NEWS_STARTING_PATTERN.search(text) else 0.0
        minutes_limit_hit = 1.0 if NEWS_MINUTES_LIMIT_PATTERN.search(text) else 0.0
        positive_hit = 1.0 if NEWS_POSITIVE_PATTERN.search(text) else 0.0
        negative_hit = 1.0 if NEWS_NEGATIVE_PATTERN.search(text) else 0.0

        player_hits = 0
        for _, player_row in player_lookup.iterrows():
            player_name = str(player_row["player_name"]).strip().lower()
            if not player_name:
                continue
            if player_name not in text:
                continue
            player_key = str(player_row["player_key"])
            stats = player_stats.setdefault(
                player_key,
                {
                    "article_count": 0.0,
                    "injury_mentions": 0.0,
                    "starting_mentions": 0.0,
                    "minutes_limit_mentions": 0.0,
                    "positive_mentions": 0.0,
                    "negative_mentions": 0.0,
                },
            )
            stats["article_count"] += 1.0
            stats["injury_mentions"] += injury_hit
            stats["starting_mentions"] += starting_hit
            stats["minutes_limit_mentions"] += minutes_limit_hit
            stats["positive_mentions"] += positive_hit
            stats["negative_mentions"] += negative_hit
            player_hits += 1

        if player_hits == 0:
            for team_code, terms in team_terms.items():
                if not terms:
                    continue
                if not any(term and term in text for term in terms):
                    continue
                stats = team_stats.setdefault(
                    team_code,
                    {
                        "article_count": 0.0,
                        "injury_mentions": 0.0,
                        "starting_mentions": 0.0,
                        "minutes_limit_mentions": 0.0,
                        "positive_mentions": 0.0,
                        "negative_mentions": 0.0,
                    },
                )
                stats["article_count"] += 1.0
                stats["injury_mentions"] += injury_hit
                stats["starting_mentions"] += starting_hit
                stats["minutes_limit_mentions"] += minutes_limit_hit
                stats["positive_mentions"] += positive_hit
                stats["negative_mentions"] += negative_hit

    provider_rows: list[dict] = []
    for _, row in upcoming_frame.iterrows():
        player_key = _normalize_player_key(row.get("player_name"))
        team_code = _normalize_team_code(row.get("team")) or str(row.get("team") or "")
        player_values = player_stats.get(player_key, {})
        team_values = team_stats.get(team_code, {})
        article_count = _safe_float_value(player_values.get("article_count"), 0.0) + 0.35 * _safe_float_value(team_values.get("article_count"), 0.0)
        if article_count <= 0:
            continue

        injury_mentions = _safe_float_value(player_values.get("injury_mentions"), 0.0) + 0.35 * _safe_float_value(team_values.get("injury_mentions"), 0.0)
        starting_mentions = _safe_float_value(player_values.get("starting_mentions"), 0.0) + 0.35 * _safe_float_value(team_values.get("starting_mentions"), 0.0)
        minutes_limit_mentions = _safe_float_value(player_values.get("minutes_limit_mentions"), 0.0) + 0.35 * _safe_float_value(team_values.get("minutes_limit_mentions"), 0.0)
        positive_mentions = _safe_float_value(player_values.get("positive_mentions"), 0.0) + 0.35 * _safe_float_value(team_values.get("positive_mentions"), 0.0)
        negative_mentions = _safe_float_value(player_values.get("negative_mentions"), 0.0) + 0.35 * _safe_float_value(team_values.get("negative_mentions"), 0.0)

        risk_numerator = injury_mentions + 0.8 * minutes_limit_mentions + 0.4 * negative_mentions - 0.25 * positive_mentions
        risk_score = max(0.0, min(1.0, risk_numerator / max(1.0, article_count)))
        confidence_score = max(0.05, min(0.98, min(1.0, article_count / 8.0)))

        provider_rows.append(
            {
                "player_name": row["player_name"],
                "team": team_code,
                "game_date": row["game_date"],
                "news_article_count_24h": round(article_count, 3),
                "news_injury_mentions_24h": round(injury_mentions, 3),
                "news_starting_mentions_24h": round(starting_mentions, 3),
                "news_minutes_limit_mentions_24h": round(minutes_limit_mentions, 3),
                "news_positive_mentions_24h": round(positive_mentions, 3),
                "news_negative_mentions_24h": round(negative_mentions, 3),
                "news_risk_score": round(risk_score, 4),
                "news_confidence_score": round(confidence_score, 4),
            }
        )

    provider_frame = pd.DataFrame(provider_rows)
    if provider_frame.empty:
        status["note"] = "Recent articles were found, but none matched players or teams in the current slate."
        return pd.DataFrame(columns=empty_columns), status
    provider_frame = provider_frame.drop_duplicates(subset=CONTEXT_KEY_COLUMNS, keep="last")
    aligned = _align_provider_rows_to_upcoming(upcoming_frame, provider_frame)
    status["rows"] = int(len(aligned))
    if aligned.empty:
        status["note"] = "News rows were parsed but did not align to current slate keys."
    return aligned, status


def _read_csv_safe(path: Path) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame()
    try:
        return pd.read_csv(path)
    except (OSError, pd.errors.EmptyDataError):
        return pd.DataFrame()


def _append_dataset_log(
    path: Path,
    rows: pd.DataFrame,
    *,
    dedupe_keys: list[str],
    max_rows: int,
    sort_columns: list[str] | None = None,
) -> tuple[int, int]:
    if rows.empty:
        return 0, _csv_row_count(path)

    existing = _read_csv_safe(path)
    before_rows = int(len(existing))
    combined = pd.concat([existing, rows], ignore_index=True, sort=False)
    if dedupe_keys:
        available_dedupe = [column for column in dedupe_keys if column in combined.columns]
        if available_dedupe:
            combined = combined.drop_duplicates(subset=available_dedupe, keep="last")
    if sort_columns:
        available_sort = [column for column in sort_columns if column in combined.columns]
        if available_sort:
            combined = combined.sort_values(available_sort, ascending=True, na_position="last")
    if max_rows > 0 and len(combined) > max_rows:
        combined = combined.tail(max_rows).copy()
    path.parent.mkdir(parents=True, exist_ok=True)
    combined.to_csv(path, index=False)
    after_rows = int(len(combined))
    return max(0, after_rows - before_rows), after_rows


def _capture_live_game_action_rows(scoreboard_payload: dict, provider_config: dict) -> tuple[pd.DataFrame, dict]:
    status = {
        "enabled": bool(provider_config.get("enabled", True)),
        "source": "NBA live scoreboard + boxscore",
        "rows": 0,
        "rows_appended": 0,
        "live_games_active": 0,
        "players_tracked": 0,
        "last_error": None,
        "note": None,
    }
    empty_columns = [
        "captured_at",
        "captured_at_bucket",
        "game_id",
        "game_date",
        "player_name",
        "team",
        "opponent",
        "minutes",
        "points",
        "rebounds",
        "assists",
        "steals",
        "blocks",
        "turnovers",
        "three_points_made",
        "field_goals_attempted",
        "free_throws_attempted",
        "personal_fouls",
        "notes_live_points_per_minute",
        "notes_live_rebounds_per_minute",
        "notes_live_assists_per_minute",
        "notes_live_usage_proxy",
        "notes_live_foul_pressure",
        "notes_live_minutes_current",
    ]
    if not status["enabled"]:
        status["note"] = "Game-notes provider is disabled."
        return pd.DataFrame(columns=empty_columns), status

    live_rows, live_status = _collect_live_boxscore_rows(scoreboard_payload)
    status["live_games_active"] = int(live_status.get("live_games_active", 0))
    status["players_tracked"] = int(live_status.get("players_tracked", 0))
    if live_rows.empty:
        status["note"] = "No active live games were found this cycle."
        return pd.DataFrame(columns=empty_columns), status

    working = live_rows.copy()
    for stat_column in [
        "minutes",
        "points",
        "rebounds",
        "assists",
        "turnovers",
        "personal_fouls",
        "field_goals_attempted",
        "free_throws_attempted",
    ]:
        working[stat_column] = pd.to_numeric(working.get(stat_column), errors="coerce")
    minutes_denominator = working["minutes"].clip(lower=0.5).fillna(0.5)
    usage_proxy = (
        pd.to_numeric(working.get("field_goals_attempted"), errors="coerce").fillna(0.0)
        + 0.44 * pd.to_numeric(working.get("free_throws_attempted"), errors="coerce").fillna(0.0)
        + pd.to_numeric(working.get("turnovers"), errors="coerce").fillna(0.0)
        + pd.to_numeric(working.get("assists"), errors="coerce").fillna(0.0)
    )

    captured_at = _now_iso()
    captured_bucket = (
        pd.Timestamp.now(tz=timezone.utc)
        .floor("10s")
        .isoformat()
    )
    working["captured_at"] = captured_at
    working["captured_at_bucket"] = captured_bucket
    working["notes_live_points_per_minute"] = (working["points"] / minutes_denominator).round(4)
    working["notes_live_rebounds_per_minute"] = (working["rebounds"] / minutes_denominator).round(4)
    working["notes_live_assists_per_minute"] = (working["assists"] / minutes_denominator).round(4)
    working["notes_live_usage_proxy"] = (usage_proxy / minutes_denominator).round(4)
    working["notes_live_foul_pressure"] = (
        pd.to_numeric(working.get("personal_fouls"), errors="coerce").fillna(0.0) / minutes_denominator
    ).round(4)
    working["notes_live_minutes_current"] = working["minutes"].round(3)
    log_frame = working[empty_columns].copy()

    live_actions_path = Path(str(provider_config.get("live_actions_path") or DEFAULT_LIVE_GAME_ACTIONS_PATH))
    max_rows = _coerce_positive_int(provider_config.get("max_live_rows_retained", 250_000), 250_000)
    try:
        rows_appended, total_rows = _append_dataset_log(
            live_actions_path,
            log_frame,
            dedupe_keys=["captured_at_bucket", "game_id", "player_name", "team"],
            max_rows=max_rows,
            sort_columns=["captured_at", "game_date", "game_id", "player_name"],
        )
        status["rows_appended"] = int(rows_appended)
        status["rows"] = int(total_rows)
    except Exception as exc:  # noqa: BLE001
        status["rows"] = int(len(log_frame))
        status["last_error"] = _sanitize_error_message(exc)
    return log_frame, status


def _team_margin_lookup(scoreboard_payload: dict) -> dict[tuple[str, str], float]:
    margin_map: dict[tuple[str, str], float] = {}
    games = ((scoreboard_payload or {}).get("scoreboard", {}) or {}).get("games", []) or []
    for game in games:
        game_id = str(game.get("gameId") or "").strip()
        home_team = _normalize_team_code((game.get("homeTeam") or {}).get("teamTricode"))
        away_team = _normalize_team_code((game.get("awayTeam") or {}).get("teamTricode"))
        home_score = pd.to_numeric((game.get("homeTeam") or {}).get("score"), errors="coerce")
        away_score = pd.to_numeric((game.get("awayTeam") or {}).get("score"), errors="coerce")
        if not game_id or not home_team or not away_team or pd.isna(home_score) or pd.isna(away_score):
            continue
        margin_map[(game_id, home_team)] = float(home_score - away_score)
        margin_map[(game_id, away_team)] = float(away_score - home_score)
    return margin_map


def _fetch_espn_summary_text_for_game(game: dict, provider_config: dict) -> tuple[str, str | None]:
    timeout_seconds = _coerce_positive_int(
        provider_config.get("request_timeout_seconds", DEFAULT_PROVIDER_REQUEST_TIMEOUT_SECONDS),
        DEFAULT_PROVIDER_REQUEST_TIMEOUT_SECONDS,
    )
    scoreboard_template = str(
        provider_config.get("espn_scoreboard_url_template") or ESPN_SCOREBOARD_URL_TEMPLATE
    ).strip()
    summary_template = str(
        provider_config.get("espn_summary_url_template") or ESPN_SUMMARY_URL_TEMPLATE
    ).strip()
    if not scoreboard_template or not summary_template:
        return "", None

    game_time = str(game.get("gameEt") or game.get("gameTimeUTC") or "").strip()
    game_date = pd.to_datetime(game_time, errors="coerce")
    if pd.isna(game_date):
        return "", None
    yyyymmdd = game_date.strftime("%Y%m%d")

    home_code = _normalize_team_code((game.get("homeTeam") or {}).get("teamTricode"))
    away_code = _normalize_team_code((game.get("awayTeam") or {}).get("teamTricode"))
    if not home_code or not away_code:
        return "", None

    try:
        espn_scoreboard = fetch_json(scoreboard_template.format(yyyymmdd=yyyymmdd), timeout=timeout_seconds)
    except (HTTPError, URLError, OSError, ValueError, subprocess.SubprocessError):
        return "", None
    events = (espn_scoreboard.get("events") or []) if isinstance(espn_scoreboard, dict) else []
    event_id: str | None = None
    for event in events:
        competition = ((event or {}).get("competitions") or [{}])[0]
        competitors = competition.get("competitors") or []
        codes = {
            _normalize_team_code(((competitor.get("team") or {}).get("abbreviation")))
            for competitor in competitors
        }
        if {home_code, away_code}.issubset(codes):
            event_id = str(event.get("id") or "").strip()
            if event_id:
                break
    if not event_id:
        return "", None

    summary_url = summary_template.format(event_id=event_id)
    try:
        summary_payload = fetch_json(summary_url, timeout=timeout_seconds)
    except (HTTPError, URLError, OSError, ValueError, subprocess.SubprocessError):
        return "", summary_url

    snippets: list[str] = []
    if isinstance(summary_payload, dict):
        article = summary_payload.get("article")
        if isinstance(article, dict):
            snippets.extend(
                [
                    str(article.get("headline") or "").strip(),
                    str(article.get("description") or "").strip(),
                ]
            )
        for news_item in summary_payload.get("news", []) or []:
            if not isinstance(news_item, dict):
                continue
            snippets.extend(
                [
                    str(news_item.get("headline") or "").strip(),
                    str(news_item.get("description") or "").strip(),
                ]
            )
    text = re.sub(r"\s+", " ", " ".join(part for part in snippets if part)).strip()
    return text, summary_url


def _capture_postgame_review_rows(
    scoreboard_payload: dict,
    completed_rows: pd.DataFrame,
    provider_config: dict,
) -> tuple[pd.DataFrame, dict]:
    status = {
        "enabled": bool(provider_config.get("enabled", True)),
        "source": "NBA final boxscores + ESPN recap text",
        "rows": 0,
        "rows_appended": 0,
        "games_processed": 0,
        "last_error": None,
        "note": None,
    }
    empty_columns = [
        "captured_at",
        "game_id",
        "game_date",
        "player_name",
        "team",
        "review_source_url",
        "review_text",
        "postgame_margin",
        "notes_postgame_positive_mentions_14d",
        "notes_postgame_negative_mentions_14d",
        "notes_postgame_minutes_limit_mentions_14d",
        "notes_postgame_rotation_change_mentions_14d",
        "notes_postgame_risk_score",
    ]
    if not status["enabled"]:
        status["note"] = "Postgame review capture is disabled."
        return pd.DataFrame(columns=empty_columns), status
    if completed_rows.empty:
        status["note"] = "No completed game rows were available this cycle."
        return pd.DataFrame(columns=empty_columns), status

    final_games = [
        game
        for game in (((scoreboard_payload or {}).get("scoreboard", {}) or {}).get("games", []) or [])
        if int((game or {}).get("gameStatus", 0) or 0) == 3
    ]
    if not final_games:
        status["note"] = "No final games were found this cycle."
        return pd.DataFrame(columns=empty_columns), status

    margin_lookup = _team_margin_lookup(scoreboard_payload)
    completed = completed_rows.copy()
    completed["game_id"] = completed.get("game_id", pd.Series("", index=completed.index)).astype(str)
    completed["game_date"] = pd.to_datetime(completed.get("game_date"), errors="coerce").dt.strftime("%Y-%m-%d")
    completed["team"] = completed.get("team", pd.Series("", index=completed.index)).map(_normalize_team_code).fillna(
        completed.get("team", pd.Series("", index=completed.index))
    )
    completed = completed.dropna(subset=["game_id", "player_name", "team"])
    if completed.empty:
        status["note"] = "Completed-game rows were present but missing player/team keys."
        return pd.DataFrame(columns=empty_columns), status

    game_id_to_recap: dict[str, tuple[str, str | None]] = {}
    for game in final_games:
        game_id = str(game.get("gameId") or "").strip()
        if not game_id:
            continue
        recap_text, recap_url = _fetch_espn_summary_text_for_game(game, provider_config)
        game_id_to_recap[game_id] = (recap_text, recap_url)

    if not game_id_to_recap:
        status["note"] = "Final games were found but recap lookups did not resolve."
        return pd.DataFrame(columns=empty_columns), status

    captured_at = _now_iso()
    rows: list[dict] = []
    for game_id, game_frame in completed.groupby("game_id"):
        recap_text, recap_url = game_id_to_recap.get(game_id, ("", None))
        positive = 1.0 if POSTGAME_POSITIVE_PATTERN.search(recap_text) else 0.0
        negative = 1.0 if POSTGAME_NEGATIVE_PATTERN.search(recap_text) else 0.0
        minutes_limit = 1.0 if POSTGAME_MINUTES_PATTERN.search(recap_text) else 0.0
        rotation_change = 1.0 if POSTGAME_ROTATION_PATTERN.search(recap_text) else 0.0
        risk_score = max(0.0, min(1.0, negative * 0.55 + minutes_limit * 0.75 + rotation_change * 0.45 - positive * 0.25))
        for _, row in game_frame.iterrows():
            team_code = _normalize_team_code(row.get("team")) or str(row.get("team") or "")
            margin = margin_lookup.get((str(game_id), team_code))
            rows.append(
                {
                    "captured_at": captured_at,
                    "game_id": str(game_id),
                    "game_date": row.get("game_date"),
                    "player_name": row.get("player_name"),
                    "team": team_code,
                    "review_source_url": recap_url,
                    "review_text": recap_text[:800] if recap_text else "",
                    "postgame_margin": margin,
                    "notes_postgame_positive_mentions_14d": positive,
                    "notes_postgame_negative_mentions_14d": negative,
                    "notes_postgame_minutes_limit_mentions_14d": minutes_limit,
                    "notes_postgame_rotation_change_mentions_14d": rotation_change,
                    "notes_postgame_risk_score": round(float(risk_score), 4),
                }
            )

    review_frame = pd.DataFrame(rows, columns=empty_columns)
    if review_frame.empty:
        status["note"] = "No postgame review rows were generated."
        return pd.DataFrame(columns=empty_columns), status

    postgame_path = Path(str(provider_config.get("postgame_reviews_path") or DEFAULT_POSTGAME_REVIEWS_PATH))
    max_rows = _coerce_positive_int(provider_config.get("max_postgame_rows_retained", 120_000), 120_000)
    try:
        rows_appended, total_rows = _append_dataset_log(
            postgame_path,
            review_frame,
            dedupe_keys=["game_id", "player_name", "team"],
            max_rows=max_rows,
            sort_columns=["game_date", "game_id", "player_name"],
        )
        status["rows"] = int(total_rows)
        status["rows_appended"] = int(rows_appended)
    except Exception as exc:  # noqa: BLE001
        status["rows"] = int(len(review_frame))
        status["last_error"] = _sanitize_error_message(exc)
    status["games_processed"] = int(review_frame["game_id"].nunique())
    return review_frame, status


def _compile_game_notes_daily_rows(
    upcoming_frame: pd.DataFrame,
    training_path: Path,
    provider_config: dict,
) -> tuple[pd.DataFrame, dict]:
    status = {
        "enabled": bool(provider_config.get("enabled", True)),
        "source": "Live actions + postgame reviews + recent history",
        "rows": 0,
        "rows_matched": 0,
        "last_error": None,
        "note": None,
    }
    empty_columns = CONTEXT_KEY_COLUMNS + GAME_NOTES_CONTEXT_COLUMNS
    if not status["enabled"]:
        status["note"] = "Game-notes provider is disabled."
        return pd.DataFrame(columns=empty_columns), status
    if upcoming_frame.empty:
        status["note"] = "No upcoming rows available for game-note alignment."
        return pd.DataFrame(columns=empty_columns), status

    live_actions_path = Path(str(provider_config.get("live_actions_path") or DEFAULT_LIVE_GAME_ACTIONS_PATH))
    postgame_path = Path(str(provider_config.get("postgame_reviews_path") or DEFAULT_POSTGAME_REVIEWS_PATH))
    daily_notes_path = Path(str(provider_config.get("daily_notes_path") or DEFAULT_GAME_NOTES_DAILY_PATH))

    try:
        history = load_dataset(training_path)
    except Exception:
        history = pd.DataFrame()
    if "game_date" in history.columns:
        history["game_date"] = pd.to_datetime(history["game_date"], errors="coerce")
    if "team" in history.columns:
        history["team"] = history["team"].map(_normalize_team_code).fillna(history["team"])

    recent_features = pd.DataFrame(columns=["player_name", "team"])
    if not history.empty and {"player_name", "team", "game_date"}.issubset(history.columns):
        working_history = history.copy().dropna(subset=["player_name", "team", "game_date"])
        for column in ["points", "rebounds", "assists", "minutes"]:
            if column in working_history.columns:
                working_history[column] = pd.to_numeric(working_history[column], errors="coerce")
            else:
                working_history[column] = pd.NA
        recent_slice = (
            working_history.sort_values(["player_name", "team", "game_date"])
            .groupby(["player_name", "team"], as_index=False, sort=False)
            .tail(5)
        )
        if not recent_slice.empty:
            recent_features = (
                recent_slice.groupby(["player_name", "team"], as_index=False)
                .agg(
                    notes_recent_points_mean_5=("points", "mean"),
                    notes_recent_rebounds_mean_5=("rebounds", "mean"),
                    notes_recent_assists_mean_5=("assists", "mean"),
                    notes_recent_minutes_mean_5=("minutes", "mean"),
                    notes_recent_points_std_5=("points", "std"),
                    notes_recent_minutes_std_5=("minutes", "std"),
                    notes_recent_games_used=("game_date", "count"),
                )
            )

    live_features = pd.DataFrame(columns=["player_name", "team"])
    live_actions = _read_csv_safe(live_actions_path)
    if not live_actions.empty and {"player_name", "team"}.issubset(live_actions.columns):
        live_actions["captured_at"] = pd.to_datetime(live_actions.get("captured_at"), errors="coerce", utc=True)
        live_actions["team"] = live_actions["team"].map(_normalize_team_code).fillna(live_actions["team"])
        latest_live = (
            live_actions.sort_values(["captured_at"])
            .groupby(["player_name", "team"], as_index=False, sort=False)
            .tail(1)
        )
        live_features = latest_live[
            [
                "player_name",
                "team",
                "notes_live_points_per_minute",
                "notes_live_rebounds_per_minute",
                "notes_live_assists_per_minute",
                "notes_live_usage_proxy",
                "notes_live_foul_pressure",
                "notes_live_minutes_current",
            ]
        ].copy()

    postgame_features = pd.DataFrame(columns=["player_name", "team"])
    postgame = _read_csv_safe(postgame_path)
    if not postgame.empty and {"player_name", "team"}.issubset(postgame.columns):
        postgame["game_date"] = pd.to_datetime(postgame.get("game_date"), errors="coerce", utc=True)
        postgame["team"] = postgame["team"].map(_normalize_team_code).fillna(postgame["team"])
        cutoff = datetime.now(timezone.utc) - timedelta(days=14)
        recent_postgame = (
            postgame.loc[postgame["game_date"] >= cutoff].copy()
            if "game_date" in postgame.columns
            else postgame.copy()
        )
        if recent_postgame.empty and "game_date" in postgame.columns:
            recent_postgame = postgame.dropna(subset=["game_date"]).copy()
        if not recent_postgame.empty:
            for column in [
                "notes_postgame_positive_mentions_14d",
                "notes_postgame_negative_mentions_14d",
                "notes_postgame_minutes_limit_mentions_14d",
                "notes_postgame_rotation_change_mentions_14d",
                "notes_postgame_risk_score",
            ]:
                recent_postgame[column] = pd.to_numeric(recent_postgame.get(column), errors="coerce").fillna(0.0)
            postgame_features = (
                recent_postgame.groupby(["player_name", "team"], as_index=False)
                .agg(
                    notes_postgame_positive_mentions_14d=("notes_postgame_positive_mentions_14d", "sum"),
                    notes_postgame_negative_mentions_14d=("notes_postgame_negative_mentions_14d", "sum"),
                    notes_postgame_minutes_limit_mentions_14d=("notes_postgame_minutes_limit_mentions_14d", "sum"),
                    notes_postgame_rotation_change_mentions_14d=("notes_postgame_rotation_change_mentions_14d", "sum"),
                    notes_postgame_risk_score=("notes_postgame_risk_score", "mean"),
                )
            )

    merged = upcoming_frame[["player_name", "team", "game_date"]].drop_duplicates().copy()
    merged["team"] = merged["team"].map(_normalize_team_code).fillna(merged["team"])
    for frame in [recent_features, live_features, postgame_features]:
        if frame.empty:
            continue
        merged = merged.merge(frame, on=["player_name", "team"], how="left")

    source_count = (
        merged[[column for column in ["notes_recent_points_mean_5", "notes_live_points_per_minute", "notes_postgame_risk_score"] if column in merged.columns]]
        .notna()
        .sum(axis=1)
    )
    recent_games = pd.to_numeric(merged.get("notes_recent_games_used"), errors="coerce").fillna(0.0)
    merged["game_notes_confidence"] = (
        0.2
        + 0.22 * source_count
        + 0.35 * (recent_games.clip(lower=0.0, upper=5.0) / 5.0)
    ).clip(lower=0.05, upper=0.98).round(4)
    merged = merged.drop(columns=["notes_recent_games_used"], errors="ignore")

    provider_frame = merged[CONTEXT_KEY_COLUMNS + [column for column in GAME_NOTES_CONTEXT_COLUMNS if column in merged.columns]].copy()
    provider_frame = provider_frame.drop_duplicates(subset=CONTEXT_KEY_COLUMNS, keep="last")
    provider_frame = provider_frame[provider_frame[[column for column in GAME_NOTES_CONTEXT_COLUMNS if column in provider_frame.columns]].notna().any(axis=1)]
    if provider_frame.empty:
        status["note"] = "Game notes were compiled, but no rows aligned to current slate keys."
        return pd.DataFrame(columns=empty_columns), status

    _write_csv_frame(daily_notes_path, provider_frame, CONTEXT_KEY_COLUMNS + GAME_NOTES_CONTEXT_COLUMNS)
    status["rows"] = int(len(provider_frame))
    status["rows_matched"] = int(len(provider_frame))
    return provider_frame, status


def _resolve_cloud_archive_root(provider_config: dict) -> Path:
    configured = str(provider_config.get("archive_path") or "").strip()
    if configured:
        return Path(configured).expanduser()
    if DEFAULT_CLOUD_ARCHIVE_PATH.parent.exists():
        return DEFAULT_CLOUD_ARCHIVE_PATH
    return DEFAULT_PROJECT_DIR / "data" / "cloud_archive"


def _file_signature(path: Path) -> str:
    stat = path.stat()
    return f"{int(stat.st_size)}:{int(stat.st_mtime_ns)}"


def _sync_cloud_archive(
    *,
    config: dict,
    state: dict,
    training_path: Path,
    upcoming_path: Path,
    context_path: Path,
    provider_context_path: Path,
) -> dict:
    provider_config = dict((config.get("providers", {}) or {}).get("cloud_archive", {}))
    status = {
        "enabled": bool(provider_config.get("enabled", True)),
        "path": None,
        "rows_synced": 0,
        "last_error": None,
        "note": None,
    }
    if not status["enabled"]:
        status["note"] = "Cloud archive is disabled."
        return status

    root = _resolve_cloud_archive_root(provider_config)
    latest_dir = root / "latest"
    snapshots_dir = root / "snapshots"
    root.mkdir(parents=True, exist_ok=True)
    latest_dir.mkdir(parents=True, exist_ok=True)
    snapshots_dir.mkdir(parents=True, exist_ok=True)
    status["path"] = str(root)

    datasets = {
        "training_data": training_path,
        "upcoming_slate": upcoming_path,
        "context_updates": context_path,
        "provider_context_updates": provider_context_path,
        "predictions": DEFAULT_PREDICTIONS_PATH,
        "prediction_miss_log": DEFAULT_PREDICTION_MISS_LOG_PATH,
        "adaptive_learning_profile": DEFAULT_ADAPTIVE_LEARNING_PROFILE_PATH,
        "live_game_actions": Path(
            str(
                (config.get("providers", {}) or {})
                .get("game_notes", {})
                .get("live_actions_path", DEFAULT_LIVE_GAME_ACTIONS_PATH)
            )
        ),
        "postgame_reviews": Path(
            str(
                (config.get("providers", {}) or {})
                .get("game_notes", {})
                .get("postgame_reviews_path", DEFAULT_POSTGAME_REVIEWS_PATH)
            )
        ),
        "game_notes_daily": Path(
            str(
                (config.get("providers", {}) or {})
                .get("game_notes", {})
                .get("daily_notes_path", DEFAULT_GAME_NOTES_DAILY_PATH)
            )
        ),
        "espn_live_games": Path(
            str(
                (config.get("providers", {}) or {})
                .get("espn_live", {})
                .get("store_path", DEFAULT_ESPN_LIVE_GAMES_PATH)
            )
        ),
    }

    signatures = state.get("cloud_archive_signatures")
    if not isinstance(signatures, dict):
        signatures = {}

    now_tag = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    synced_files = 0
    for dataset_name, source_path in datasets.items():
        if source_path is None or not source_path.exists():
            continue
        try:
            signature = _file_signature(source_path)
        except OSError:
            continue
        if signatures.get(dataset_name) == signature:
            continue
        latest_target = latest_dir / source_path.name
        dataset_snapshot_dir = snapshots_dir / dataset_name
        dataset_snapshot_dir.mkdir(parents=True, exist_ok=True)
        snapshot_target = dataset_snapshot_dir / f"{source_path.stem}__{now_tag}{source_path.suffix}"
        shutil.copy2(source_path, latest_target)
        shutil.copy2(source_path, snapshot_target)
        signatures[dataset_name] = signature
        synced_files += 1

    state["cloud_archive_signatures"] = signatures
    status["rows_synced"] = int(synced_files)
    if synced_files == 0:
        status["note"] = "Cloud archive checked with no dataset changes this cycle."
    else:
        manifest_payload = {
            "updated_at": _now_iso(),
            "datasets": {
                key: {
                    "latest_path": str((latest_dir / path.name)) if path and path.exists() else None,
                    "signature": signatures.get(key),
                }
                for key, path in datasets.items()
            },
        }
        (root / "manifest.json").write_text(json.dumps(manifest_payload, indent=2), encoding="utf-8")
        status["note"] = f"Archived {synced_files} dataset updates to cloud storage."

    hydrate_enabled = bool(provider_config.get("hydrate_training_from_cloud", True))
    hydrate_interval_seconds = _clamp_interval(
        provider_config.get("hydrate_interval_seconds", 3600),
        fallback=3600,
        minimum=60,
    )
    hydrate_due, hydrate_next_due = _is_due_seconds(
        state.get("last_cloud_hydrate_at"),
        hydrate_interval_seconds,
        now_utc=datetime.now(timezone.utc),
    )
    state["next_cloud_hydrate_due_at"] = hydrate_next_due
    if hydrate_enabled and hydrate_due:
        cloud_training_path = latest_dir / training_path.name
        if cloud_training_path.exists() and training_path.exists():
            cloud_training = _read_csv_safe(cloud_training_path)
            local_training = _read_csv_safe(training_path)
            if not cloud_training.empty and not local_training.empty:
                merged = pd.concat([local_training, cloud_training], ignore_index=True, sort=False)
                dedupe_keys = [column for column in ["game_id", "player_name"] if column in merged.columns]
                fallback_keys = [column for column in ["player_name", "game_date", "team", "opponent"] if column in merged.columns]
                if len(dedupe_keys) >= 2:
                    merged = merged.drop_duplicates(subset=dedupe_keys, keep="last")
                if len(fallback_keys) >= 4:
                    merged = merged.drop_duplicates(subset=fallback_keys, keep="last")
                merged = merged.drop_duplicates()
                if len(merged) > len(local_training):
                    merged.to_csv(training_path, index=False)
        state["last_cloud_hydrate_at"] = _now_iso()
        state["next_cloud_hydrate_due_at"] = _next_due_iso_from_seconds(
            state["last_cloud_hydrate_at"],
            hydrate_interval_seconds,
        )
    return status


def _resolve_neon_database_url(provider_config: dict) -> str:
    explicit_url = str(provider_config.get("database_url") or "").strip()
    if explicit_url:
        return explicit_url
    env_key = str(provider_config.get("database_url_env") or "NEON_DATABASE_URL").strip() or "NEON_DATABASE_URL"
    return str(os.environ.get(env_key, "")).strip()


def _sanitize_sql_identifier(value: object, fallback: str) -> str:
    sanitized = re.sub(r"[^a-zA-Z0-9_]+", "_", str(value or "").strip().lower()).strip("_")
    if not sanitized:
        sanitized = fallback
    if sanitized[0].isdigit():
        sanitized = f"_{sanitized}"
    return sanitized


def _quote_identifier(identifier: str) -> str:
    escaped = identifier.replace('"', '""')
    return '"' + escaped + '"'


def _neon_database_host(database_url: str) -> str | None:
    try:
        parsed = urlparse(database_url)
        return parsed.hostname
    except Exception:  # noqa: BLE001
        return None


def _estimate_dataset_rows(path: Path, *, max_rows_hint: int = 250_000) -> int | None:
    suffix = path.suffix.lower()
    if suffix in {".csv", ".tsv"}:
        try:
            with path.open("rb") as handle:
                line_count = 0
                for _ in handle:
                    line_count += 1
                    if line_count > max_rows_hint + 1:
                        break
            return max(0, line_count - 1)
        except OSError:
            return None
    if suffix == ".json":
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
            if isinstance(payload, list):
                return len(payload)
            if isinstance(payload, dict):
                return len(payload)
        except (OSError, json.JSONDecodeError, UnicodeDecodeError):
            return None
    return None


def _sync_neon_archive(
    *,
    config: dict,
    state: dict,
    training_path: Path,
    upcoming_path: Path,
    context_path: Path,
    provider_context_path: Path,
) -> dict:
    provider_config = dict((config.get("providers", {}) or {}).get("neon_sync", {}))
    status = {
        "enabled": bool(provider_config.get("enabled", False)),
        "database_host": None,
        "rows_synced": 0,
        "last_error": None,
        "note": None,
        "datasets_synced": [],
        "datasets_skipped": [],
    }
    if not status["enabled"]:
        status["note"] = "Neon sync is disabled."
        return status

    database_url = _resolve_neon_database_url(provider_config)
    if not database_url:
        status["note"] = "Set NEON_DATABASE_URL (or providers.neon_sync.database_url) to enable Neon sync."
        return status

    status["database_host"] = _neon_database_host(database_url)

    try:
        import psycopg
    except Exception as exc:  # noqa: BLE001
        status["last_error"] = (
            "psycopg is not installed. Run `./venv/bin/pip install psycopg[binary]` "
            f"to enable Neon sync ({_sanitize_error_message(exc)})."
        )
        return status

    max_dataset_bytes = _coerce_positive_int(
        provider_config.get("max_dataset_bytes", DEFAULT_NEON_MAX_DATASET_BYTES),
        DEFAULT_NEON_MAX_DATASET_BYTES,
    )
    max_rows_hint = _coerce_positive_int(provider_config.get("max_rows_per_dataset", 250_000), 250_000)
    compress_payloads = bool(provider_config.get("compress_payloads", True))
    schema = _sanitize_sql_identifier(provider_config.get("schema"), "public")
    table_prefix = _sanitize_sql_identifier(provider_config.get("table_prefix"), "nba_live")

    datasets = {
        "training_data": training_path,
        "season_priors": DEFAULT_PROJECT_DIR / "data" / "season_priors.csv",
        "upcoming_slate": upcoming_path,
        "context_updates": context_path,
        "provider_context_updates": provider_context_path,
        "prizepicks_lines": DEFAULT_PROJECT_DIR / "data" / "prizepicks_lines.csv",
        "predictions": DEFAULT_PREDICTIONS_PATH,
        "prizepicks_edges": DEFAULT_PROJECT_DIR / "models" / "prizepicks_edges.csv",
        "prediction_miss_log": DEFAULT_PREDICTION_MISS_LOG_PATH,
        "adaptive_learning_profile": DEFAULT_ADAPTIVE_LEARNING_PROFILE_PATH,
        "live_game_actions": Path(
            str(
                (config.get("providers", {}) or {})
                .get("game_notes", {})
                .get("live_actions_path", DEFAULT_LIVE_GAME_ACTIONS_PATH)
            )
        ),
        "postgame_reviews": Path(
            str(
                (config.get("providers", {}) or {})
                .get("game_notes", {})
                .get("postgame_reviews_path", DEFAULT_POSTGAME_REVIEWS_PATH)
            )
        ),
        "game_notes_daily": Path(
            str(
                (config.get("providers", {}) or {})
                .get("game_notes", {})
                .get("daily_notes_path", DEFAULT_GAME_NOTES_DAILY_PATH)
            )
        ),
        "espn_live_games": Path(
            str(
                (config.get("providers", {}) or {})
                .get("espn_live", {})
                .get("store_path", DEFAULT_ESPN_LIVE_GAMES_PATH)
            )
        ),
    }

    signatures = state.get("neon_archive_signatures")
    if not isinstance(signatures, dict):
        signatures = {}

    schema_name = _quote_identifier(schema)
    blobs_table = f"{schema_name}.{_quote_identifier(f'{table_prefix}_dataset_blobs')}"
    latest_table = f"{schema_name}.{_quote_identifier(f'{table_prefix}_dataset_latest')}"
    runs_table = f"{schema_name}.{_quote_identifier(f'{table_prefix}_sync_runs')}"

    synced = 0
    skipped = 0
    synced_names: list[str] = []
    skipped_names: list[str] = []
    try:
        with psycopg.connect(database_url, autocommit=True) as conn:
            with conn.cursor() as cur:
                cur.execute(f"CREATE SCHEMA IF NOT EXISTS {schema_name}")
                cur.execute(
                    f"""
                    CREATE TABLE IF NOT EXISTS {blobs_table} (
                        dataset_name TEXT NOT NULL,
                        signature TEXT NOT NULL,
                        content_type TEXT NOT NULL,
                        payload_encoding TEXT NOT NULL,
                        payload BYTEA NOT NULL,
                        file_size_bytes BIGINT NOT NULL,
                        row_count BIGINT,
                        captured_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                        PRIMARY KEY (dataset_name, signature)
                    )
                    """
                )
                cur.execute(
                    f"""
                    CREATE TABLE IF NOT EXISTS {latest_table} (
                        dataset_name TEXT PRIMARY KEY,
                        signature TEXT NOT NULL,
                        file_size_bytes BIGINT NOT NULL,
                        row_count BIGINT,
                        local_path TEXT,
                        last_synced_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                    )
                    """
                )
                cur.execute(
                    f"""
                    CREATE TABLE IF NOT EXISTS {runs_table} (
                        id BIGSERIAL PRIMARY KEY,
                        synced_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                        datasets_synced INTEGER NOT NULL,
                        datasets_skipped INTEGER NOT NULL,
                        note TEXT
                    )
                    """
                )

                for dataset_name, source_path in datasets.items():
                    if source_path is None or not source_path.exists():
                        continue
                    try:
                        signature = _file_signature(source_path)
                    except OSError:
                        continue
                    if signatures.get(dataset_name) == signature:
                        continue

                    file_size = int(source_path.stat().st_size)
                    if file_size > max_dataset_bytes:
                        skipped += 1
                        skipped_names.append(dataset_name)
                        continue

                    payload = source_path.read_bytes()
                    encoding = "identity"
                    if compress_payloads and payload:
                        payload = gzip.compress(payload, compresslevel=6)
                        encoding = "gzip"
                    content_type = (
                        "application/json"
                        if source_path.suffix.lower() == ".json"
                        else "text/csv"
                    )
                    row_count = _estimate_dataset_rows(source_path, max_rows_hint=max_rows_hint)

                    cur.execute(
                        f"""
                        INSERT INTO {blobs_table}
                            (dataset_name, signature, content_type, payload_encoding, payload, file_size_bytes, row_count)
                        VALUES (%s, %s, %s, %s, %s, %s, %s)
                        ON CONFLICT (dataset_name, signature) DO NOTHING
                        """,
                        (
                            dataset_name,
                            signature,
                            content_type,
                            encoding,
                            payload,
                            file_size,
                            row_count,
                        ),
                    )
                    cur.execute(
                        f"""
                        INSERT INTO {latest_table}
                            (dataset_name, signature, file_size_bytes, row_count, local_path, last_synced_at)
                        VALUES (%s, %s, %s, %s, %s, NOW())
                        ON CONFLICT (dataset_name) DO UPDATE SET
                            signature = EXCLUDED.signature,
                            file_size_bytes = EXCLUDED.file_size_bytes,
                            row_count = EXCLUDED.row_count,
                            local_path = EXCLUDED.local_path,
                            last_synced_at = EXCLUDED.last_synced_at
                        """,
                        (
                            dataset_name,
                            signature,
                            file_size,
                            row_count,
                            str(source_path),
                        ),
                    )
                    signatures[dataset_name] = signature
                    synced += 1
                    synced_names.append(dataset_name)

                cur.execute(
                    f"""
                    INSERT INTO {runs_table} (datasets_synced, datasets_skipped, note)
                    VALUES (%s, %s, %s)
                    """,
                    (
                        synced,
                        skipped,
                        f"Auto Neon sync at {_now_iso()}",
                    ),
                )
    except Exception as exc:  # noqa: BLE001
        status["last_error"] = _sanitize_error_message(exc)
        status["note"] = "Neon sync failed."
        return status

    state["neon_archive_signatures"] = signatures
    status["rows_synced"] = int(synced)
    status["datasets_synced"] = synced_names
    status["datasets_skipped"] = skipped_names
    if synced == 0:
        if skipped > 0:
            status["note"] = f"No new datasets synced; skipped {skipped} oversized dataset(s)."
        else:
            status["note"] = "Neon sync checked with no dataset changes this cycle."
    else:
        status["note"] = f"Synced {synced} dataset update(s) to Neon."
    return status


def _compose_provider_context_frame(upcoming_frame: pd.DataFrame, provider_frames: list[pd.DataFrame]) -> pd.DataFrame:
    if upcoming_frame.empty:
        return pd.DataFrame(columns=CONTEXT_KEY_COLUMNS + OPTIONAL_CONTEXT_COLUMNS)

    composed = upcoming_frame[CONTEXT_KEY_COLUMNS].drop_duplicates().copy()
    for frame in provider_frames:
        if frame.empty:
            continue
        composed = _apply_context_frame(composed, frame)

    update_columns = [column for column in composed.columns if column not in CONTEXT_KEY_COLUMNS]
    if not update_columns:
        return pd.DataFrame(columns=CONTEXT_KEY_COLUMNS + OPTIONAL_CONTEXT_COLUMNS)

    mask = composed[update_columns].notna().any(axis=1)
    return composed.loc[mask, CONTEXT_KEY_COLUMNS + update_columns].copy()


def _apply_line_movement_context(
    provider_context_frame: pd.DataFrame,
    provider_context_path: Path,
) -> pd.DataFrame:
    if provider_context_frame.empty:
        return provider_context_frame

    working = provider_context_frame.copy()
    previous_context = pd.DataFrame()
    if provider_context_path.exists():
        try:
            previous_context = pd.read_csv(provider_context_path)
        except (OSError, pd.errors.EmptyDataError):
            previous_context = pd.DataFrame()

    for line_column, movement_meta in LINE_MOVEMENT_COLUMNS.items():
        consensus_meta = LINE_CONSENSUS_COLUMNS.get(line_column, {})
        consensus_column = consensus_meta.get("consensus")
        open_column = movement_meta["open"]
        close_column = movement_meta["close"]
        movement_column = movement_meta["movement"]

        current_close = (
            pd.to_numeric(working[close_column], errors="coerce")
            if close_column in working.columns
            else pd.Series(np.nan, index=working.index, dtype=float)
        )
        if consensus_column:
            consensus_values = (
                pd.to_numeric(working[consensus_column], errors="coerce")
                if consensus_column in working.columns
                else pd.Series(np.nan, index=working.index, dtype=float)
            )
            current_close = current_close.combine_first(consensus_values)
        line_values = (
            pd.to_numeric(working[line_column], errors="coerce")
            if line_column in working.columns
            else pd.Series(np.nan, index=working.index, dtype=float)
        )
        current_close = current_close.combine_first(line_values)

        previous_close_aligned = pd.Series(np.nan, index=working.index, dtype=float)
        if not previous_context.empty:
            required_keys_present = all(key in previous_context.columns for key in CONTEXT_KEY_COLUMNS)
            if required_keys_present:
                previous_subset = previous_context.copy()
                previous_subset["game_date"] = pd.to_datetime(previous_subset["game_date"], errors="coerce").dt.strftime("%Y-%m-%d")
                prev_close = (
                    pd.to_numeric(previous_subset[close_column], errors="coerce")
                    if close_column in previous_subset.columns
                    else pd.Series(np.nan, index=previous_subset.index, dtype=float)
                )
                if consensus_column:
                    prev_consensus = (
                        pd.to_numeric(previous_subset[consensus_column], errors="coerce")
                        if consensus_column in previous_subset.columns
                        else pd.Series(np.nan, index=previous_subset.index, dtype=float)
                    )
                    prev_close = prev_close.combine_first(prev_consensus)
                prev_line = (
                    pd.to_numeric(previous_subset[line_column], errors="coerce")
                    if line_column in previous_subset.columns
                    else pd.Series(np.nan, index=previous_subset.index, dtype=float)
                )
                prev_close = prev_close.combine_first(prev_line)
                previous_subset["__prev_close"] = prev_close
                previous_subset = previous_subset[CONTEXT_KEY_COLUMNS + ["__prev_close"]].drop_duplicates(
                    subset=CONTEXT_KEY_COLUMNS,
                    keep="last",
                )
                merged_prev = working[CONTEXT_KEY_COLUMNS].merge(previous_subset, on=CONTEXT_KEY_COLUMNS, how="left")
                previous_close_aligned = pd.to_numeric(merged_prev.get("__prev_close"), errors="coerce")

        current_open = (
            pd.to_numeric(working[open_column], errors="coerce")
            if open_column in working.columns
            else pd.Series(np.nan, index=working.index, dtype=float)
        )
        current_open = current_open.combine_first(previous_close_aligned).combine_first(current_close)
        current_movement = (
            pd.to_numeric(working[movement_column], errors="coerce")
            if movement_column in working.columns
            else pd.Series(np.nan, index=working.index, dtype=float)
        )
        current_movement = current_movement.combine_first(current_close - current_open)

        working[open_column] = current_open
        working[close_column] = current_close
        working[movement_column] = current_movement

    return working


def run_live_sync(config_path: Path = DEFAULT_LIVE_CONFIG_PATH, state_path: Path = DEFAULT_LIVE_STATE_PATH) -> dict:
    config = load_live_config(config_path)
    _update_fetch_retry_settings_from_config(config)
    support_modules_config = normalize_support_module_config(config.get("support_modules"))
    training_path = _configured_training_path(config)
    upcoming_path = _configured_upcoming_path(config)
    context_path = _configured_context_path(config)
    provider_context_path = _configured_provider_context_path(config)
    training_lookback_value = pd.to_numeric(config.get("model_training_lookback_days"), errors="coerce")
    model_lookback_days = int(training_lookback_value) if pd.notna(training_lookback_value) and training_lookback_value > 0 else None
    projection_interval_minutes = _coerce_positive_int(config.get("projection_refresh_interval_minutes", 1), 1)
    projection_interval_seconds = _clamp_interval(
        config.get("projection_refresh_interval_seconds", MIN_PROJECTION_INTERVAL_SECONDS),
        fallback=MIN_PROJECTION_INTERVAL_SECONDS,
        minimum=MIN_PROJECTION_INTERVAL_SECONDS,
    )
    prediction_min_interval_seconds = _clamp_interval(
        config.get("prediction_min_interval_seconds", MIN_PROJECTION_INTERVAL_SECONDS),
        fallback=MIN_PROJECTION_INTERVAL_SECONDS,
        minimum=MIN_PROJECTION_INTERVAL_SECONDS,
    )
    prediction_on_context_change_only = bool(config.get("prediction_on_context_change_only", True))
    prediction_max_rows_per_cycle = _clamp_interval(
        config.get("prediction_max_rows_per_cycle", 400),
        fallback=400,
        minimum=50,
    )
    expected_minutes_refresh_interval_seconds = _clamp_interval(
        config.get("expected_minutes_refresh_interval_seconds", 1800),
        fallback=1800,
        minimum=60,
    )
    teammate_context_refresh_interval_seconds = _clamp_interval(
        config.get("teammate_context_refresh_interval_seconds", 1800),
        fallback=1800,
        minimum=60,
    )
    shot_style_context_refresh_interval_seconds = _clamp_interval(
        config.get("shot_style_context_refresh_interval_seconds", 300),
        fallback=300,
        minimum=60,
    )
    in_game_projection_interval_seconds = _clamp_interval(
        config.get("in_game_projection_refresh_interval_seconds", MIN_IN_GAME_REFRESH_INTERVAL_SECONDS),
        fallback=MIN_IN_GAME_REFRESH_INTERVAL_SECONDS,
        minimum=MIN_IN_GAME_REFRESH_INTERVAL_SECONDS,
    )
    optimization_interval_minutes = _coerce_positive_int(config.get("optimization_interval_minutes", 60), 60)
    optimization_interval_seconds = _clamp_interval(
        config.get("optimization_interval_seconds", MIN_OPTIMIZATION_INTERVAL_SECONDS),
        fallback=MIN_OPTIMIZATION_INTERVAL_SECONDS,
        minimum=MIN_OPTIMIZATION_INTERVAL_SECONDS,
    )
    run_heavy_model_tasks = bool(config.get("run_heavy_model_tasks_in_live_sync", False))
    force_provider_refresh_every_poll = bool(config.get("force_provider_refresh_every_poll", True))
    auto_self_optimize = bool(config.get("auto_self_optimize_hourly", True))
    auto_retrain_each_interval = bool(config.get("auto_retrain_each_interval", False))
    retrain_interval_seconds = _clamp_interval(
        config.get("retrain_interval_seconds", max(MIN_RETRAIN_INTERVAL_SECONDS, projection_interval_seconds)),
        fallback=max(MIN_RETRAIN_INTERVAL_SECONDS, projection_interval_seconds),
        minimum=MIN_RETRAIN_INTERVAL_SECONDS,
    )
    optimization_recheck_sample_rows = _coerce_positive_int(config.get("optimization_recheck_sample_rows", 2200), 2200)
    capture_benchmark_snapshot_on_refresh = bool(
        config.get("capture_benchmark_snapshot_on_projection_refresh", True)
    )
    auto_run_rotowire_benchmark = bool(config.get("auto_run_rotowire_benchmark", True))
    benchmark_interval_minutes = _coerce_positive_int(config.get("benchmark_run_interval_minutes", 10), 10)
    benchmark_interval_seconds = _clamp_interval(
        config.get("benchmark_run_interval_seconds", MIN_BENCHMARK_INTERVAL_SECONDS),
        fallback=MIN_BENCHMARK_INTERVAL_SECONDS,
        minimum=MIN_BENCHMARK_INTERVAL_SECONDS,
    )
    benchmark_lookback_days = _coerce_positive_int(config.get("benchmark_run_lookback_days", 28), 28)
    auto_contract_drift_audit = bool(config.get("auto_contract_drift_audit", True))
    contract_drift_interval_hours = max(
        1,
        _coerce_positive_int(config.get("contract_drift_interval_hours", 24), 24),
    )
    contract_drift_interval_seconds = int(contract_drift_interval_hours * 3600)
    drift_alert_on_unexpected = bool(config.get("contract_drift_alert_on_unexpected_columns", True))
    drift_max_missing_required_columns = max(
        0,
        _coerce_positive_int(config.get("contract_drift_max_missing_required_columns", 0), 0),
    )
    drift_max_unexpected_columns = max(
        0,
        _coerce_positive_int(config.get("contract_drift_max_unexpected_columns", 0), 0),
    )
    live_ingest_module_enabled = module_enabled(support_modules_config, "live_ingest", default=True)
    notes_engine_module_enabled = module_enabled(support_modules_config, "notes_engine", default=True)
    model_trainer_module_enabled = module_enabled(support_modules_config, "model_trainer", default=True)
    backtester_module_enabled = module_enabled(support_modules_config, "backtester", default=True)
    alerts_module_enabled = module_enabled(support_modules_config, "alerts", default=True)
    if not model_trainer_module_enabled:
        run_heavy_model_tasks = False
        auto_self_optimize = False
        auto_retrain_each_interval = False
    if not backtester_module_enabled:
        auto_run_rotowire_benchmark = False
        capture_benchmark_snapshot_on_refresh = False
    if not module_enabled(support_modules_config, "live_ingest", default=True):
        force_provider_refresh_every_poll = False

    adaptive_profile_payload: dict = {}
    if DEFAULT_ADAPTIVE_LEARNING_PROFILE_PATH.exists():
        try:
            loaded_profile = json.loads(DEFAULT_ADAPTIVE_LEARNING_PROFILE_PATH.read_text(encoding="utf-8"))
            if isinstance(loaded_profile, dict):
                adaptive_profile_payload = loaded_profile
        except Exception:
            adaptive_profile_payload = {}
    adaptive_overall_payload = adaptive_profile_payload.get("overall", {}) if isinstance(adaptive_profile_payload, dict) else {}

    state = load_live_state(state_path)
    state.update(
        {
            "last_sync_at": _now_iso(),
            "training_data_path": str(training_path),
            "upcoming_data_path": str(upcoming_path),
            "context_updates_path": str(context_path),
            "provider_context_path": str(provider_context_path),
            "last_error": None,
            "games_seen": 0,
            "games_scheduled": 0,
            "games_live": 0,
            "games_final": 0,
            "completed_rows_appended": 0,
            "scoreboard_rows_appended": 0,
            "backfill_rows_appended": state.get("backfill_rows_appended", 0),
            "backfill_games_scanned": state.get("backfill_games_scanned", 0),
            "backfill_games_fetched": state.get("backfill_games_fetched", 0),
            "backfill_games_failed": state.get("backfill_games_failed", 0),
            "backfill_window_start": state.get("backfill_window_start"),
            "backfill_window_end": state.get("backfill_window_end"),
            "backfill_note": state.get("backfill_note"),
            "backfill_errors": state.get("backfill_errors", []),
            "upcoming_rows_generated": 0,
            "last_train_triggered": False,
            "last_predict_triggered": False,
            "provider_context_rows": 0,
            "scheduled_games_found": 0,
            "scheduled_game_dates": [],
            "scheduled_sources": [],
            "providers": {},
            "model_training_lookback_days": model_lookback_days,
            "projection_refresh_interval_minutes": projection_interval_minutes,
            "projection_refresh_interval_seconds": projection_interval_seconds,
            "prediction_min_interval_seconds": prediction_min_interval_seconds,
            "prediction_on_context_change_only": prediction_on_context_change_only,
            "prediction_max_rows_per_cycle": prediction_max_rows_per_cycle,
            "expected_minutes_refresh_interval_seconds": expected_minutes_refresh_interval_seconds,
            "teammate_context_refresh_interval_seconds": teammate_context_refresh_interval_seconds,
            "shot_style_context_refresh_interval_seconds": shot_style_context_refresh_interval_seconds,
            "live_projection_horizon_hours": _clamp_interval(
                config.get("live_projection_horizon_hours", 48),
                fallback=48,
                minimum=6,
            ),
            "max_upcoming_rows_per_cycle": _clamp_interval(
                config.get("max_upcoming_rows_per_cycle", 250),
                fallback=250,
                minimum=100,
            ),
            "in_game_projection_refresh_interval_seconds": in_game_projection_interval_seconds,
            "optimization_interval_minutes": optimization_interval_minutes,
            "optimization_interval_seconds": optimization_interval_seconds,
            "run_heavy_model_tasks_in_live_sync": run_heavy_model_tasks,
            "force_provider_refresh_every_poll": force_provider_refresh_every_poll,
            "auto_self_optimize_hourly": auto_self_optimize,
            "auto_retrain_each_interval": auto_retrain_each_interval,
            "retrain_interval_seconds": retrain_interval_seconds,
            "capture_benchmark_snapshot_on_projection_refresh": capture_benchmark_snapshot_on_refresh,
            "auto_run_rotowire_benchmark": auto_run_rotowire_benchmark,
            "benchmark_run_interval_minutes": benchmark_interval_minutes,
            "benchmark_run_interval_seconds": benchmark_interval_seconds,
            "benchmark_run_lookback_days": benchmark_lookback_days,
            "auto_contract_drift_audit": auto_contract_drift_audit,
            "contract_drift_interval_hours": contract_drift_interval_hours,
            "contract_drift_interval_seconds": contract_drift_interval_seconds,
            "contract_drift_last_run_at": state.get("contract_drift_last_run_at"),
            "contract_drift_next_due_at": state.get("contract_drift_next_due_at"),
            "contract_drift_summary": state.get("contract_drift_summary", {}),
            "contract_drift_warning_count": int(state.get("contract_drift_warning_count", 0) or 0),
            "contract_drift_warnings": state.get("contract_drift_warnings", []),
            "contract_drift_last_error": state.get("contract_drift_last_error"),
            "last_retrain_refresh_at": state.get("last_retrain_refresh_at"),
            "last_benchmark_snapshot_at": state.get("last_benchmark_snapshot_at"),
            "last_benchmark_run_at": state.get("last_benchmark_run_at"),
            "benchmark_snapshot_rows_added": int(state.get("benchmark_snapshot_rows_added", 0) or 0),
            "benchmark_snapshot_rows_total": int(state.get("benchmark_snapshot_rows_total", 0) or 0),
            "benchmark_snapshot_last_error": state.get("benchmark_snapshot_last_error"),
            "benchmark_rows_evaluated": int(state.get("benchmark_rows_evaluated", 0) or 0),
            "benchmark_last_generated_at": state.get("benchmark_last_generated_at"),
            "benchmark_last_error": state.get("benchmark_last_error"),
            "benchmark_last_note": state.get("benchmark_last_note"),
            "adaptive_learning_last_run_at": (
                state.get("adaptive_learning_last_run_at")
                or adaptive_profile_payload.get("generated_at")
            ),
            "adaptive_learning_rows_total": int(
                state.get("adaptive_learning_rows_total")
                or adaptive_profile_payload.get("rows_total")
                or 0
            ),
            "adaptive_learning_rows_added": int(state.get("adaptive_learning_rows_added", 0) or 0),
            "adaptive_learning_rows_in_window": int(
                state.get("adaptive_learning_rows_in_window")
                or adaptive_profile_payload.get("rows_in_window")
                or 0
            ),
            "adaptive_learning_miss_rate_14d": _safe_float_value(
                state.get("adaptive_learning_miss_rate_14d")
                or adaptive_overall_payload.get("miss_rate"),
                0.0,
            ),
            "adaptive_learning_profile_path": (
                state.get("adaptive_learning_profile_path")
                or str(DEFAULT_ADAPTIVE_LEARNING_PROFILE_PATH)
            ),
            "adaptive_learning_miss_log_path": (
                state.get("adaptive_learning_miss_log_path")
                or str(DEFAULT_PREDICTION_MISS_LOG_PATH)
            ),
            "adaptive_learning_last_note": (
                state.get("adaptive_learning_last_note")
                or adaptive_profile_payload.get("note")
            ),
            "adaptive_learning_last_error": state.get("adaptive_learning_last_error"),
            "force_projection_refresh_on_context_change": bool(config.get("force_projection_refresh_on_context_change", True)),
            "in_game_projection_rows_updated": int(state.get("in_game_projection_rows_updated", 0) or 0),
            "in_game_projection_players_tracked": int(state.get("in_game_projection_players_tracked", 0) or 0),
            "in_game_projection_games_tracked": int(state.get("in_game_projection_games_tracked", 0) or 0),
            "in_game_projection_live_games_active": int(state.get("in_game_projection_live_games_active", 0) or 0),
            "in_game_projection_note": state.get("in_game_projection_note"),
            "in_game_projection_last_error": state.get("in_game_projection_last_error"),
            "prediction_rows_used": int(state.get("prediction_rows_used", 0) or 0),
            "last_expected_minutes_refresh_at": state.get("last_expected_minutes_refresh_at"),
            "next_expected_minutes_refresh_due_at": state.get("next_expected_minutes_refresh_due_at"),
            "last_teammate_context_refresh_at": state.get("last_teammate_context_refresh_at"),
            "next_teammate_context_refresh_due_at": state.get("next_teammate_context_refresh_due_at"),
            "last_shot_style_context_refresh_at": state.get("last_shot_style_context_refresh_at"),
            "next_shot_style_context_refresh_due_at": state.get("next_shot_style_context_refresh_due_at"),
            "last_lineups_refresh_at": state.get("last_lineups_refresh_at"),
            "next_lineups_refresh_due_at": state.get("next_lineups_refresh_due_at"),
            "last_live_rosters_refresh_at": state.get("last_live_rosters_refresh_at"),
            "next_live_rosters_refresh_due_at": state.get("next_live_rosters_refresh_due_at"),
            "last_odds_refresh_at": state.get("last_odds_refresh_at"),
            "next_odds_refresh_due_at": state.get("next_odds_refresh_due_at"),
            "last_player_props_refresh_at": state.get("last_player_props_refresh_at"),
            "next_player_props_refresh_due_at": state.get("next_player_props_refresh_due_at"),
            "last_rotowire_refresh_at": state.get("last_rotowire_refresh_at"),
            "next_rotowire_refresh_due_at": state.get("next_rotowire_refresh_due_at"),
            "last_injury_refresh_at": state.get("last_injury_refresh_at"),
            "next_injury_refresh_due_at": state.get("next_injury_refresh_due_at"),
            "last_game_notes_live_refresh_at": state.get("last_game_notes_live_refresh_at"),
            "next_game_notes_live_refresh_due_at": state.get("next_game_notes_live_refresh_due_at"),
            "last_postgame_review_refresh_at": state.get("last_postgame_review_refresh_at"),
            "next_postgame_review_refresh_due_at": state.get("next_postgame_review_refresh_due_at"),
            "last_game_notes_daily_compile_at": state.get("last_game_notes_daily_compile_at"),
            "next_game_notes_daily_compile_due_at": state.get("next_game_notes_daily_compile_due_at"),
            "game_notes_live_rows": int(state.get("game_notes_live_rows", 0) or 0),
            "postgame_review_rows": int(state.get("postgame_review_rows", 0) or 0),
            "game_notes_daily_rows": int(state.get("game_notes_daily_rows", 0) or 0),
            "last_cloud_archive_sync_at": state.get("last_cloud_archive_sync_at"),
            "next_cloud_archive_sync_due_at": state.get("next_cloud_archive_sync_due_at"),
            "last_cloud_hydrate_at": state.get("last_cloud_hydrate_at"),
            "next_cloud_hydrate_due_at": state.get("next_cloud_hydrate_due_at"),
            "cloud_archive_rows_synced": int(state.get("cloud_archive_rows_synced", 0) or 0),
            "cloud_archive_enabled": bool(state.get("cloud_archive_enabled", False)),
            "cloud_archive_path": state.get("cloud_archive_path"),
            "cloud_archive_note": state.get("cloud_archive_note"),
            "cloud_archive_last_error": state.get("cloud_archive_last_error"),
            "last_neon_sync_at": state.get("last_neon_sync_at"),
            "next_neon_sync_due_at": state.get("next_neon_sync_due_at"),
            "neon_sync_rows_synced": int(state.get("neon_sync_rows_synced", 0) or 0),
            "neon_sync_enabled": bool(state.get("neon_sync_enabled", False)),
            "neon_sync_database_host": state.get("neon_sync_database_host"),
            "neon_sync_note": state.get("neon_sync_note"),
            "neon_sync_last_error": state.get("neon_sync_last_error"),
            "support_modules_config": support_modules_config,
            "support_modules": state.get("support_modules", {}),
            "module_alerts": state.get("module_alerts", []),
            "module_alerts_count": int(state.get("module_alerts_count", 0) or 0),
        }
    )
    sync_started_monotonic = time.monotonic()

    try:
        if live_ingest_module_enabled:
            scoreboard_payload = fetch_scoreboard()
            schedule_payload = fetch_schedule()
            games = scoreboard_payload["scoreboard"].get("games", [])
            state["games_seen"] = len(games)
            state["games_scheduled"] = sum(int(game.get("gameStatus", 0)) == 1 for game in games)
            state["games_live"] = sum(int(game.get("gameStatus", 0)) == 2 for game in games)
            state["games_final"] = sum(int(game.get("gameStatus", 0)) == 3 for game in games)
            state["live_ingest_note"] = "Live ingest module is active."
        else:
            scoreboard_payload = {"scoreboard": {"games": []}}
            schedule_payload = {"leagueSchedule": {"gameDates": []}}
            games = []
            state["games_seen"] = 0
            state["games_scheduled"] = 0
            state["games_live"] = 0
            state["games_final"] = 0
            state["live_ingest_note"] = "Live ingest module is disabled; reuse cached datasets."

        if live_ingest_module_enabled and bool(config.get("auto_backfill_recent_history", True)):
            lookback_days = int(config.get("history_backfill_days", 42))
            max_games = int(config.get("history_backfill_max_games_per_cycle", 180))
            max_runtime_seconds = _coerce_positive_int(
                config.get("history_backfill_max_runtime_seconds", DEFAULT_BACKFILL_MAX_RUNTIME_SECONDS),
                DEFAULT_BACKFILL_MAX_RUNTIME_SECONDS,
            )
            min_interval_hours = float(config.get("history_backfill_min_interval_hours", 12))
            last_backfill_at = _parse_iso_datetime(str(state.get("backfill_last_run_at") or ""))
            due = (
                last_backfill_at is None
                or (datetime.now(timezone.utc) - last_backfill_at) >= timedelta(hours=max(0.0, min_interval_hours))
            )
            if due:
                backfill_summary = _backfill_recent_history(
                    schedule_payload=schedule_payload,
                    training_path=training_path,
                    lookback_days=lookback_days,
                    max_games_per_cycle=max_games,
                    max_runtime_seconds=max_runtime_seconds,
                )
                state["backfill_rows_appended"] = backfill_summary["rows_appended"]
                state["backfill_games_scanned"] = backfill_summary["games_scanned"]
                state["backfill_games_fetched"] = backfill_summary["games_fetched"]
                state["backfill_games_failed"] = backfill_summary["games_failed"]
                state["backfill_window_start"] = backfill_summary["window_start"]
                state["backfill_window_end"] = backfill_summary["window_end"]
                state["backfill_errors"] = backfill_summary["errors"]
                state["backfill_note"] = backfill_summary.get("note")
                state["backfill_last_run_at"] = _now_iso()
            else:
                next_due = last_backfill_at + timedelta(hours=max(0.0, min_interval_hours))
                state["backfill_note"] = (
                    f"Recent-history backfill skipped for this cycle. Next run after {next_due.isoformat()}."
                )
        else:
            state["backfill_note"] = (
                "Recent-history backfill is disabled."
                if live_ingest_module_enabled
                else "Recent-history backfill skipped because live ingest module is disabled."
            )

        completed_frames = []
        completed_rows = pd.DataFrame()
        if live_ingest_module_enabled:
            for game in games:
                if int(game.get("gameStatus", 0)) != 3:
                    continue
                boxscore_payload = fetch_boxscore(game["gameId"])
                completed_frames.append(_boxscore_players_to_rows(boxscore_payload))

        if completed_frames:
            completed_rows = pd.concat(completed_frames, ignore_index=True, sort=False)
            state["scoreboard_rows_appended"] = _append_completed_rows(training_path, completed_rows)
        else:
            state["scoreboard_rows_appended"] = 0
        state["completed_rows_appended"] = int(state["backfill_rows_appended"] + state["scoreboard_rows_appended"])

        if live_ingest_module_enabled:
            refresh_season_priors_from_history(training_path=training_path)
        context_changed = False

        effective_auto_build_upcoming = bool(config.get("auto_build_upcoming_slate", True)) and live_ingest_module_enabled
        if effective_auto_build_upcoming:
            provider_frames: list[pd.DataFrame] = []
            provider_status: dict[str, dict] = {}
            providers_config = config.get("providers", {})
            odds_provider_config = dict(providers_config.get("odds", {}))
            odds_provider_config.setdefault("lookahead_hours", config.get("pregame_slate_lookahead_hours", 36))
            odds_refresh_seconds = _clamp_interval(
                odds_provider_config.get("refresh_interval_seconds", DEFAULT_ODDS_REFRESH_INTERVAL_SECONDS),
                fallback=DEFAULT_ODDS_REFRESH_INTERVAL_SECONDS,
                minimum=MIN_PROVIDER_REFRESH_INTERVAL_SECONDS,
            )
            if force_provider_refresh_every_poll:
                odds_refresh_seconds = MIN_PROVIDER_REFRESH_INTERVAL_SECONDS
            odds_due, next_odds_due = _is_due_seconds(
                state.get("last_odds_refresh_at"),
                odds_refresh_seconds,
                now_utc=datetime.now(timezone.utc),
            )
            state["next_odds_refresh_due_at"] = next_odds_due
            if odds_due:
                odds_events, odds_event_status = _fetch_odds_events(
                    odds_provider_config,
                    int(config.get("pregame_slate_lookahead_hours", 36)),
                )
                state["last_odds_refresh_at"] = _now_iso()
                state["next_odds_refresh_due_at"] = _next_due_iso_from_seconds(
                    state["last_odds_refresh_at"],
                    odds_refresh_seconds,
                )
            else:
                odds_events = []
                odds_event_status = {
                    "enabled": bool(odds_provider_config.get("enabled", True)),
                    "api_key_present": bool(os.getenv(odds_provider_config.get("api_key_env", "ODDS_API_KEY"), "").strip()),
                    "events_loaded": 0,
                    "event_dates": [],
                    "last_error": None,
                    "source": "The Odds API",
                    "note": "Reused cached odds context until next refresh window.",
                }
            include_live_games = bool(config.get("include_live_games_in_upcoming", False))
            if bool(config.get("always_include_live_games_for_context", True)):
                include_live_games = include_live_games or state.get("games_live", 0) > 0
            scheduled_games = _scheduled_games_from_scoreboard(
                scoreboard_payload,
                include_live_games,
            )
            scheduled_games.extend(
                _scheduled_games_from_schedule(
                    schedule_payload,
                    int(config.get("pregame_slate_lookahead_hours", 36)),
                    include_live_games,
                )
            )
            scheduled_games.extend(_scheduled_games_from_odds_events(odds_events))

            base_upcoming_frame, upcoming_note, slate_meta = _build_base_upcoming_frame(
                scoreboard_payload=scoreboard_payload,
                training_path=training_path,
                include_live_games=include_live_games,
                scheduled_games=scheduled_games,
            )
            live_projection_horizon_hours = _clamp_interval(
                config.get("live_projection_horizon_hours", 48),
                fallback=48,
                minimum=6,
            )
            max_upcoming_rows_per_cycle = _clamp_interval(
                config.get("max_upcoming_rows_per_cycle", 250),
                fallback=250,
                minimum=100,
            )
            trimmed_upcoming_frame = _trim_upcoming_frame_for_live_window(
                base_upcoming_frame,
                horizon_hours=live_projection_horizon_hours,
                max_rows=max_upcoming_rows_per_cycle,
            )
            if len(trimmed_upcoming_frame) < len(base_upcoming_frame):
                upcoming_note = (
                    f"{upcoming_note} | Trimmed to {len(trimmed_upcoming_frame)} rows "
                    f"within {live_projection_horizon_hours}h horizon (max {max_upcoming_rows_per_cycle})."
                )
            base_upcoming_frame = trimmed_upcoming_frame

            game_notes_provider_config = dict(providers_config.get("game_notes", {}))
            if not notes_engine_module_enabled:
                game_notes_provider_config["enabled"] = False
            game_notes_live_refresh_seconds = _clamp_interval(
                game_notes_provider_config.get("refresh_interval_seconds", MIN_IN_GAME_REFRESH_INTERVAL_SECONDS),
                fallback=MIN_IN_GAME_REFRESH_INTERVAL_SECONDS,
                minimum=MIN_IN_GAME_REFRESH_INTERVAL_SECONDS,
            )
            game_notes_postgame_refresh_seconds = _clamp_interval(
                game_notes_provider_config.get("postgame_refresh_interval_seconds", 300),
                fallback=300,
                minimum=30,
            )
            game_notes_daily_refresh_seconds = _clamp_interval(
                game_notes_provider_config.get("daily_compile_interval_seconds", 900),
                fallback=900,
                minimum=60,
            )
            if force_provider_refresh_every_poll:
                game_notes_live_refresh_seconds = MIN_IN_GAME_REFRESH_INTERVAL_SECONDS
                game_notes_postgame_refresh_seconds = MIN_IN_GAME_REFRESH_INTERVAL_SECONDS
                game_notes_daily_refresh_seconds = MIN_IN_GAME_REFRESH_INTERVAL_SECONDS

            game_notes_live_due, next_game_notes_live_due = _is_due_seconds(
                state.get("last_game_notes_live_refresh_at"),
                game_notes_live_refresh_seconds,
                now_utc=datetime.now(timezone.utc),
            )
            state["next_game_notes_live_refresh_due_at"] = next_game_notes_live_due
            if game_notes_live_due:
                _, game_notes_live_status = _capture_live_game_action_rows(
                    scoreboard_payload,
                    game_notes_provider_config,
                )
                state["last_game_notes_live_refresh_at"] = _now_iso()
                state["next_game_notes_live_refresh_due_at"] = _next_due_iso_from_seconds(
                    state["last_game_notes_live_refresh_at"],
                    game_notes_live_refresh_seconds,
                )
            else:
                live_actions_path = Path(
                    str(game_notes_provider_config.get("live_actions_path") or DEFAULT_LIVE_GAME_ACTIONS_PATH)
                )
                game_notes_live_status = {
                    "enabled": bool(game_notes_provider_config.get("enabled", True)),
                    "source": "NBA live scoreboard + boxscore",
                    "rows": _csv_row_count(live_actions_path),
                    "rows_appended": 0,
                    "live_games_active": int(state.get("games_live", 0) or 0),
                    "players_tracked": 0,
                    "last_error": None,
                    "note": "Reused cached live game-action notes until next refresh window.",
                }

            game_notes_postgame_due, next_game_notes_postgame_due = _is_due_seconds(
                state.get("last_postgame_review_refresh_at"),
                game_notes_postgame_refresh_seconds,
                now_utc=datetime.now(timezone.utc),
            )
            state["next_postgame_review_refresh_due_at"] = next_game_notes_postgame_due
            if game_notes_postgame_due:
                _, postgame_review_status = _capture_postgame_review_rows(
                    scoreboard_payload,
                    completed_rows,
                    game_notes_provider_config,
                )
                state["last_postgame_review_refresh_at"] = _now_iso()
                state["next_postgame_review_refresh_due_at"] = _next_due_iso_from_seconds(
                    state["last_postgame_review_refresh_at"],
                    game_notes_postgame_refresh_seconds,
                )
            else:
                postgame_reviews_path = Path(
                    str(game_notes_provider_config.get("postgame_reviews_path") or DEFAULT_POSTGAME_REVIEWS_PATH)
                )
                postgame_review_status = {
                    "enabled": bool(game_notes_provider_config.get("enabled", True)),
                    "source": "NBA final boxscores + ESPN recap text",
                    "rows": _csv_row_count(postgame_reviews_path),
                    "rows_appended": 0,
                    "games_processed": 0,
                    "last_error": None,
                    "note": "Reused cached postgame review notes until next refresh window.",
                }

            game_notes_daily_due, next_game_notes_daily_due = _is_due_seconds(
                state.get("last_game_notes_daily_compile_at"),
                game_notes_daily_refresh_seconds,
                now_utc=datetime.now(timezone.utc),
            )
            state["next_game_notes_daily_compile_due_at"] = next_game_notes_daily_due
            if game_notes_daily_due or game_notes_live_due or game_notes_postgame_due:
                game_notes_frame, game_notes_compile_status = _compile_game_notes_daily_rows(
                    base_upcoming_frame,
                    training_path,
                    game_notes_provider_config,
                )
                state["last_game_notes_daily_compile_at"] = _now_iso()
                state["next_game_notes_daily_compile_due_at"] = _next_due_iso_from_seconds(
                    state["last_game_notes_daily_compile_at"],
                    game_notes_daily_refresh_seconds,
                )
            else:
                daily_notes_path = Path(
                    str(game_notes_provider_config.get("daily_notes_path") or DEFAULT_GAME_NOTES_DAILY_PATH)
                )
                cached_daily_notes = _read_csv_safe(daily_notes_path)
                game_notes_frame = _align_provider_rows_to_upcoming(base_upcoming_frame, cached_daily_notes)
                game_notes_compile_status = {
                    "enabled": bool(game_notes_provider_config.get("enabled", True)),
                    "source": "Live actions + postgame reviews + recent history",
                    "rows": int(len(cached_daily_notes)),
                    "rows_matched": int(len(game_notes_frame)),
                    "last_error": None,
                    "note": "Reused cached compiled game-note context until next refresh window.",
                }
            provider_status["game_notes"] = {
                "enabled": bool(game_notes_provider_config.get("enabled", True)),
                "source": "Live game notes compiler",
                "rows": int(game_notes_compile_status.get("rows_matched", 0)),
                "live_rows": int(game_notes_live_status.get("rows", 0)),
                "postgame_rows": int(postgame_review_status.get("rows", 0)),
                "compiled_rows": int(game_notes_compile_status.get("rows", 0)),
                "rows_appended_live": int(game_notes_live_status.get("rows_appended", 0)),
                "rows_appended_postgame": int(postgame_review_status.get("rows_appended", 0)),
                "games_processed": int(postgame_review_status.get("games_processed", 0)),
                "last_error": (
                    game_notes_compile_status.get("last_error")
                    or game_notes_live_status.get("last_error")
                    or postgame_review_status.get("last_error")
                ),
                "note": game_notes_compile_status.get("note")
                or game_notes_live_status.get("note")
                or postgame_review_status.get("note"),
            }
            provider_frames.append(game_notes_frame)

            espn_live_provider_config = dict(providers_config.get("espn_live", {}))
            espn_live_refresh_seconds = _clamp_interval(
                espn_live_provider_config.get("refresh_interval_seconds", MIN_IN_GAME_REFRESH_INTERVAL_SECONDS),
                fallback=MIN_IN_GAME_REFRESH_INTERVAL_SECONDS,
                minimum=MIN_PROVIDER_REFRESH_INTERVAL_SECONDS,
            )
            if force_provider_refresh_every_poll:
                espn_live_refresh_seconds = MIN_PROVIDER_REFRESH_INTERVAL_SECONDS
            espn_live_due, next_espn_live_due = _is_due_seconds(
                state.get("last_espn_live_refresh_at"),
                espn_live_refresh_seconds,
                now_utc=datetime.now(timezone.utc),
            )
            state["next_espn_live_refresh_due_at"] = next_espn_live_due
            if espn_live_due:
                _, espn_live_status = _fetch_espn_live_rows(
                    base_upcoming_frame,
                    espn_live_provider_config,
                    game_notes_provider_config,
                )
                state["last_espn_live_refresh_at"] = _now_iso()
                state["next_espn_live_refresh_due_at"] = _next_due_iso_from_seconds(
                    state["last_espn_live_refresh_at"],
                    espn_live_refresh_seconds,
                )
            else:
                espn_store_path = Path(
                    str(espn_live_provider_config.get("store_path") or DEFAULT_ESPN_LIVE_GAMES_PATH)
                )
                espn_live_status = {
                    "enabled": bool(espn_live_provider_config.get("enabled", True)),
                    "source": "ESPN scoreboard + summary",
                    "rows": _csv_row_count(espn_store_path),
                    "rows_appended": 0,
                    "rows_appended_to_game_notes": 0,
                    "events_loaded": 0,
                    "summaries_loaded": 0,
                    "live_events": 0,
                    "final_events": 0,
                    "dates_loaded": [],
                    "path": str(espn_store_path),
                    "last_error": None,
                    "note": "Reused cached ESPN live rows until next refresh window.",
                }
            provider_status["espn_live"] = espn_live_status

            lineups_provider_config = dict(providers_config.get("lineups", {}))
            lineups_refresh_seconds = _clamp_interval(
                lineups_provider_config.get("refresh_interval_seconds", DEFAULT_LINEUPS_REFRESH_INTERVAL_SECONDS),
                fallback=DEFAULT_LINEUPS_REFRESH_INTERVAL_SECONDS,
                minimum=MIN_PROVIDER_REFRESH_INTERVAL_SECONDS,
            )
            if force_provider_refresh_every_poll:
                lineups_refresh_seconds = MIN_PROVIDER_REFRESH_INTERVAL_SECONDS
            lineups_due, next_lineups_due = _is_due_seconds(
                state.get("last_lineups_refresh_at"),
                lineups_refresh_seconds,
                now_utc=datetime.now(timezone.utc),
            )
            state["next_lineups_refresh_due_at"] = next_lineups_due
            if lineups_due:
                lineups_frame, lineups_status = _fetch_nba_daily_lineups_rows(
                    base_upcoming_frame,
                    lineups_provider_config,
                )
                state["last_lineups_refresh_at"] = _now_iso()
                state["next_lineups_refresh_due_at"] = _next_due_iso_from_seconds(
                    state["last_lineups_refresh_at"],
                    lineups_refresh_seconds,
                )
            else:
                lineups_frame = _cached_provider_rows(
                    base_upcoming_frame,
                    provider_context_path,
                    LINEUPS_CONTEXT_COLUMNS,
                )
                lineups_status = {
                    "enabled": bool(lineups_provider_config.get("enabled", True)),
                    "rows": int(len(lineups_frame)),
                    "dates_requested": [],
                    "dates_loaded": [],
                    "records_loaded": int(len(lineups_frame)),
                    "last_error": None,
                    "note": "Reused cached lineup context until next refresh window.",
                    "source": "NBA daily lineups feed",
                }
            provider_status["lineups"] = lineups_status
            provider_frames.append(lineups_frame)

            live_rosters_provider_config = dict(providers_config.get("live_rosters", {}))
            live_rosters_refresh_seconds = _clamp_interval(
                live_rosters_provider_config.get("refresh_interval_seconds", DEFAULT_LIVE_ROSTER_REFRESH_INTERVAL_SECONDS),
                fallback=DEFAULT_LIVE_ROSTER_REFRESH_INTERVAL_SECONDS,
                minimum=MIN_PROVIDER_REFRESH_INTERVAL_SECONDS,
            )
            if force_provider_refresh_every_poll:
                live_rosters_refresh_seconds = MIN_PROVIDER_REFRESH_INTERVAL_SECONDS
            live_rosters_due, next_live_rosters_due = _is_due_seconds(
                state.get("last_live_rosters_refresh_at"),
                live_rosters_refresh_seconds,
                now_utc=datetime.now(timezone.utc),
            )
            state["next_live_rosters_refresh_due_at"] = next_live_rosters_due
            if live_rosters_due:
                live_rosters_frame, live_rosters_status = _fetch_live_roster_rows(
                    scoreboard_payload,
                    base_upcoming_frame,
                    live_rosters_provider_config,
                )
                state["last_live_rosters_refresh_at"] = _now_iso()
                state["next_live_rosters_refresh_due_at"] = _next_due_iso_from_seconds(
                    state["last_live_rosters_refresh_at"],
                    live_rosters_refresh_seconds,
                )
            else:
                live_rosters_frame = _cached_provider_rows(
                    base_upcoming_frame,
                    provider_context_path,
                    LIVE_ROSTER_CONTEXT_COLUMNS,
                )
                live_rosters_status = {
                    "enabled": bool(live_rosters_provider_config.get("enabled", True)),
                    "source": "NBA live boxscore roster",
                    "games_seen": 0,
                    "games_loaded": 0,
                    "rows_raw": int(len(live_rosters_frame)),
                    "rows": int(len(live_rosters_frame)),
                    "last_error": None,
                    "note": "Reused cached live roster context until next refresh window.",
                }
            provider_status["live_rosters"] = live_rosters_status
            provider_frames.append(live_rosters_frame)

            if odds_due:
                odds_frame, odds_status = _fetch_odds_provider_rows(
                    base_upcoming_frame,
                    odds_provider_config,
                    events=odds_events,
                    base_status=odds_event_status,
                )
            else:
                odds_frame = _cached_provider_rows(
                    base_upcoming_frame,
                    provider_context_path,
                    ODDS_CONTEXT_COLUMNS,
                )
                odds_status = dict(odds_event_status)
                odds_status.update(
                    {
                        "rows": int(len(odds_frame)),
                        "records_loaded": int(len(odds_frame)),
                        "note": "Reused cached odds context until next refresh window.",
                    }
                )
            provider_status["odds"] = odds_status
            provider_frames.append(odds_frame)

            player_props_provider_config = dict(providers_config.get("player_props", {}))
            player_props_provider_config.setdefault("lookahead_hours", config.get("pregame_slate_lookahead_hours", 36))
            player_props_refresh_seconds = _clamp_interval(
                player_props_provider_config.get("refresh_interval_seconds", DEFAULT_PLAYER_PROPS_REFRESH_INTERVAL_SECONDS),
                fallback=DEFAULT_PLAYER_PROPS_REFRESH_INTERVAL_SECONDS,
                minimum=MIN_PROVIDER_REFRESH_INTERVAL_SECONDS,
            )
            if force_provider_refresh_every_poll:
                player_props_refresh_seconds = MIN_PROVIDER_REFRESH_INTERVAL_SECONDS
            player_props_due, next_player_props_due = _is_due_seconds(
                state.get("last_player_props_refresh_at"),
                player_props_refresh_seconds,
                now_utc=datetime.now(timezone.utc),
            )
            state["next_player_props_refresh_due_at"] = next_player_props_due
            if player_props_due:
                player_props_frame, player_props_status = _fetch_odds_player_props_rows(
                    base_upcoming_frame,
                    player_props_provider_config,
                    events=odds_events if odds_due else None,
                )
                state["last_player_props_refresh_at"] = _now_iso()
                state["next_player_props_refresh_due_at"] = _next_due_iso_from_seconds(
                    state["last_player_props_refresh_at"],
                    player_props_refresh_seconds,
                )
            else:
                player_props_frame = _cached_provider_rows(
                    base_upcoming_frame,
                    provider_context_path,
                    PROP_LINE_CONTEXT_COLUMNS,
                )
                player_props_status = {
                    "enabled": bool(player_props_provider_config.get("enabled", True)),
                    "rows": int(len(player_props_frame)),
                    "events_requested": 0,
                    "events_matched": 0,
                    "events_with_props": 0,
                    "api_key_present": bool(
                        os.getenv(player_props_provider_config.get("api_key_env", "ODDS_API_KEY"), "").strip()
                    ),
                    "last_error": None,
                    "note": "Reused cached player-prop context until next refresh window.",
                    "source": "The Odds API player props",
                }
            provider_status["player_props"] = player_props_status
            provider_frames.append(player_props_frame)

            rotowire_provider_config = dict(providers_config.get("rotowire_prizepicks", {}))
            rotowire_refresh_seconds = _clamp_interval(
                rotowire_provider_config.get("refresh_interval_seconds", DEFAULT_ROTOWIRE_REFRESH_INTERVAL_SECONDS),
                fallback=DEFAULT_ROTOWIRE_REFRESH_INTERVAL_SECONDS,
                minimum=MIN_PROVIDER_REFRESH_INTERVAL_SECONDS,
            )
            if force_provider_refresh_every_poll:
                rotowire_refresh_seconds = MIN_PROVIDER_REFRESH_INTERVAL_SECONDS
            rotowire_due, next_rotowire_due = _is_due_seconds(
                state.get("last_rotowire_refresh_at"),
                rotowire_refresh_seconds,
                now_utc=datetime.now(timezone.utc),
            )
            state["next_rotowire_refresh_due_at"] = next_rotowire_due
            if rotowire_due:
                rotowire_frame, rotowire_status = _fetch_rotowire_prizepicks_rows(
                    base_upcoming_frame,
                    rotowire_provider_config,
                )
                state["last_rotowire_refresh_at"] = _now_iso()
                state["next_rotowire_refresh_due_at"] = _next_due_iso_from_seconds(
                    state["last_rotowire_refresh_at"],
                    rotowire_refresh_seconds,
                )
            else:
                rotowire_frame = _cached_provider_rows(
                    base_upcoming_frame,
                    provider_context_path,
                    PROP_LINE_CONTEXT_COLUMNS,
                )
                rotowire_status = {
                    "enabled": bool(rotowire_provider_config.get("enabled", True)),
                    "rows": int(len(rotowire_frame)),
                    "records_loaded": int(len(rotowire_frame)),
                    "props_seen": 0,
                    "last_error": None,
                    "note": "Reused cached RotoWire line context until next refresh window.",
                    "source": "RotoWire PrizePicks lines",
                    "endpoint": str(rotowire_provider_config.get("lines_url") or ROTOWIRE_PRIZEPICKS_LINES_URL),
                }
            if not player_props_frame.empty and not rotowire_frame.empty:
                rotowire_status.update(_compare_prop_line_frames(player_props_frame, rotowire_frame))
                changed_values = int(rotowire_status.get("changed_values", 0))
                compared_values = int(rotowire_status.get("compared_values", 0))
                if changed_values > 0 and compared_values > 0:
                    median_abs_delta = float(rotowire_status.get("median_abs_delta", 0.0))
                    rotowire_status["note"] = (
                        f"Compared {changed_values}/{compared_values} shared line values vs odds props; "
                        f"median absolute delta {median_abs_delta:.2f}."
                    )
            provider_status["rotowire_prizepicks"] = rotowire_status
            provider_frames.append(rotowire_frame)

            betr_provider_config = dict(providers_config.get("betr", {}))
            provider_status["betr"] = {
                "enabled": bool(betr_provider_config.get("enabled", False)),
                "rows": 0,
                "records_loaded": 0,
                "last_error": None,
                "source": "BETR (manual)",
                "endpoint": str(betr_provider_config.get("board_url") or "https://www.betr.app/"),
                "note": str(
                    betr_provider_config.get("note")
                    or "No stable public BETR props API endpoint was detected. Use manual line import for BETR board values."
                ),
            }

            injury_provider_config = dict(providers_config.get("injuries", {}))
            injury_refresh_seconds = _clamp_interval(
                injury_provider_config.get("refresh_interval_seconds", DEFAULT_INJURY_REFRESH_INTERVAL_SECONDS),
                fallback=DEFAULT_INJURY_REFRESH_INTERVAL_SECONDS,
                minimum=MIN_PROVIDER_REFRESH_INTERVAL_SECONDS,
            )
            if force_provider_refresh_every_poll:
                injury_refresh_seconds = MIN_PROVIDER_REFRESH_INTERVAL_SECONDS
            injury_due, next_injury_due = _is_due_seconds(
                state.get("last_injury_refresh_at"),
                injury_refresh_seconds,
                now_utc=datetime.now(timezone.utc),
            )
            state["next_injury_refresh_due_at"] = next_injury_due
            if injury_due:
                injury_frame, injury_status = _fetch_injury_provider_rows(
                    base_upcoming_frame,
                    injury_provider_config,
                )
                state["last_injury_refresh_at"] = _now_iso()
                state["next_injury_refresh_due_at"] = _next_due_iso_from_seconds(
                    state["last_injury_refresh_at"],
                    injury_refresh_seconds,
                )
            else:
                injury_frame = _cached_provider_rows(
                    base_upcoming_frame,
                    provider_context_path,
                    INJURY_CONTEXT_COLUMNS,
                )
                injury_status = {
                    "enabled": bool(injury_provider_config.get("enabled", True)),
                    "rows": int(len(injury_frame)),
                    "records_loaded": int(len(injury_frame)),
                    "latest_report_url": state.get("official_injury_report"),
                    "last_error": None,
                    "official_report_error": None,
                    "note": "Reused cached injury context until next refresh window.",
                    "source": "Configured injury feed",
                }
            provider_status["injuries"] = injury_status
            provider_frames.append(injury_frame)

            playstyle_provider_config = dict(providers_config.get("playstyle", {}))
            playstyle_refresh_seconds = _clamp_interval(
                playstyle_provider_config.get("refresh_interval_seconds", MIN_PLAYSTYLE_REFRESH_INTERVAL_SECONDS),
                fallback=MIN_PLAYSTYLE_REFRESH_INTERVAL_SECONDS,
                minimum=MIN_PLAYSTYLE_REFRESH_INTERVAL_SECONDS,
            )
            playstyle_due, next_playstyle_due = _is_due_seconds(
                state.get("last_playstyle_refresh_at"),
                playstyle_refresh_seconds,
                now_utc=datetime.now(timezone.utc),
            )
            if bool(playstyle_provider_config.get("enabled", True)):
                if playstyle_due:
                    playstyle_frame, playstyle_status = _fetch_playstyle_context_rows(
                        base_upcoming_frame,
                        playstyle_provider_config,
                    )
                    state["last_playstyle_refresh_at"] = _now_iso()
                    state["next_playstyle_refresh_due_at"] = _next_due_iso_from_seconds(
                        state["last_playstyle_refresh_at"],
                        playstyle_refresh_seconds,
                    )
                else:
                    playstyle_frame = _cached_provider_rows(
                        base_upcoming_frame,
                        provider_context_path,
                        PLAYSTYLE_CONTEXT_COLUMNS,
                    )
                    playstyle_status = {
                        "enabled": True,
                        "source": "NBA Stats playstyle profile feed",
                        "rows": int(len(playstyle_frame)),
                        "cache_rows": int(len(playstyle_frame)),
                        "fetched_profiles": 0,
                        "reused_profiles": int(len(playstyle_frame)),
                        "last_error": None,
                        "note": "Reused cached playstyle context values until next refresh window.",
                    }
            else:
                playstyle_frame = pd.DataFrame(columns=CONTEXT_KEY_COLUMNS + PLAYSTYLE_CONTEXT_COLUMNS)
                playstyle_status = {
                    "enabled": False,
                    "source": "NBA Stats playstyle profile feed",
                    "rows": 0,
                    "cache_rows": 0,
                    "fetched_profiles": 0,
                    "reused_profiles": 0,
                    "last_error": None,
                    "note": "Playstyle provider is disabled.",
                }
            state["next_playstyle_refresh_due_at"] = next_playstyle_due
            provider_status["playstyle"] = playstyle_status
            provider_frames.append(playstyle_frame)

            news_provider_config = dict(providers_config.get("news", {}))
            if not notes_engine_module_enabled:
                news_provider_config["enabled"] = False
            news_refresh_minutes = _coerce_positive_int(news_provider_config.get("refresh_interval_minutes", 5), 5)
            news_refresh_seconds = _clamp_interval(
                news_provider_config.get("refresh_interval_seconds", MIN_NEWS_REFRESH_INTERVAL_SECONDS),
                fallback=MIN_NEWS_REFRESH_INTERVAL_SECONDS,
                minimum=MIN_NEWS_REFRESH_INTERVAL_SECONDS,
            )
            if force_provider_refresh_every_poll:
                news_refresh_seconds = MIN_NEWS_REFRESH_INTERVAL_SECONDS
            news_due, next_news_due = _is_due_seconds(
                state.get("last_news_refresh_at"),
                news_refresh_seconds,
                now_utc=datetime.now(timezone.utc),
            )
            if bool(news_provider_config.get("enabled", True)):
                if news_due:
                    news_frame, news_status = _fetch_news_context_rows(
                        base_upcoming_frame,
                        news_provider_config,
                    )
                    state["last_news_refresh_at"] = _now_iso()
                    state["next_news_refresh_due_at"] = _next_due_iso_from_seconds(
                        state["last_news_refresh_at"],
                        news_refresh_seconds,
                    )
                else:
                    news_frame = _cached_provider_rows(
                        base_upcoming_frame,
                        provider_context_path,
                        NEWS_CONTEXT_COLUMNS,
                    )
                    news_status = {
                        "enabled": True,
                        "source": "RSS/News aggregation",
                        "queries_used": 0,
                        "articles_loaded": 0,
                        "rows": int(len(news_frame)),
                        "last_error": None,
                        "note": "Reused cached news context values until next refresh window.",
                    }
            else:
                news_frame = pd.DataFrame(columns=CONTEXT_KEY_COLUMNS + NEWS_CONTEXT_COLUMNS)
                news_status = {
                    "enabled": False,
                    "source": "RSS/News aggregation",
                    "queries_used": 0,
                    "articles_loaded": 0,
                    "rows": 0,
                    "last_error": None,
                    "note": "News provider is disabled.",
                }
            if not news_due:
                state["next_news_refresh_due_at"] = next_news_due
            provider_status["news"] = news_status
            provider_frames.append(news_frame)

            provider_context_frame = _compose_provider_context_frame(base_upcoming_frame, provider_frames)
            provider_context_frame = _apply_line_movement_context(
                provider_context_frame,
                provider_context_path=provider_context_path,
            )
            provider_context_fingerprint = compute_frame_fingerprint(provider_context_frame)
            provider_context_idempotency = check_and_register_idempotency(
                dataset="provider_context_updates",
                source="live_sync:provider_context_updates",
                fingerprint=provider_context_fingerprint,
                metadata={
                    "rows": int(len(provider_context_frame)),
                    "path": str(provider_context_path),
                },
            )
            _write_csv_frame(provider_context_path, provider_context_frame, CONTEXT_KEY_COLUMNS + OPTIONAL_CONTEXT_COLUMNS)
            record_ingestion_event(
                dataset="provider_context_updates",
                stage="live_sync",
                rows_in=len(provider_context_frame),
                rows_out=_csv_row_count(provider_context_path),
                rows_rejected=0,
                source="live_sync:provider_context_updates",
                outcome="duplicate_skipped" if provider_context_idempotency.get("duplicate") else "success",
                details={
                    "idempotency": provider_context_idempotency,
                    "path": str(provider_context_path),
                },
            )

            merged_upcoming_frame = _merge_context_updates(
                base_upcoming_frame,
                [upcoming_path, provider_context_path, context_path],
            ).drop_duplicates(subset=CONTEXT_KEY_COLUMNS, keep="last")
            profile_provider_config = dict(providers_config.get("player_profiles", {}))
            merged_upcoming_frame, home_hometown_summary = _apply_home_and_hometown_context(
                merged_upcoming_frame,
                training_path=training_path,
                profile_provider_config=profile_provider_config,
            )
            provider_status["player_profiles"] = {
                "enabled": bool(profile_provider_config.get("enabled", True)),
                "source": "Wikipedia player profile summary",
                "rows_with_home_context": int(home_hometown_summary.get("rows_with_home_context", 0)),
                "rows_with_hometown_context": int(home_hometown_summary.get("rows_with_hometown_context", 0)),
                "profile_cache_rows": int(home_hometown_summary.get("profile_cache_rows", 0)),
                "profiles_fetched": int(home_hometown_summary.get("profiles_fetched", 0)),
                "profiles_reused": int(home_hometown_summary.get("profiles_reused", 0)),
                "last_error": home_hometown_summary.get("profile_last_error"),
            }
            now_context_utc = datetime.now(timezone.utc)
            teammate_due, next_teammate_due = _is_due_seconds(
                state.get("last_teammate_context_refresh_at"),
                teammate_context_refresh_interval_seconds,
                now_utc=now_context_utc,
            )
            state["next_teammate_context_refresh_due_at"] = next_teammate_due
            if teammate_due:
                merged_upcoming_frame, teammate_summary = _apply_teammate_composition_context(
                    merged_upcoming_frame,
                    training_path=training_path,
                )
                state["last_teammate_context_refresh_at"] = _now_iso()
                state["next_teammate_context_refresh_due_at"] = _next_due_iso_from_seconds(
                    state["last_teammate_context_refresh_at"],
                    teammate_context_refresh_interval_seconds,
                )
            else:
                teammate_presence = (
                    merged_upcoming_frame[TEAMMATE_CONTEXT_COLUMNS]
                    .notna()
                    .any(axis=1)
                    if all(column in merged_upcoming_frame.columns for column in TEAMMATE_CONTEXT_COLUMNS)
                    else pd.Series(False, index=merged_upcoming_frame.index)
                )
                teammate_summary = {
                    "rows_with_context": int(teammate_presence.sum()),
                    "rows_without_context": int(len(merged_upcoming_frame) - teammate_presence.sum()),
                    "teams_evaluated": int(merged_upcoming_frame[["game_date", "team"]].drop_duplicates().shape[0]),
                    "note": "Reused cached teammate context values until next refresh window.",
                }
            if bool(config.get("auto_estimate_expected_minutes", True)):
                minutes_due, next_minutes_due = _is_due_seconds(
                    state.get("last_expected_minutes_refresh_at"),
                    expected_minutes_refresh_interval_seconds,
                    now_utc=now_context_utc,
                )
                state["next_expected_minutes_refresh_due_at"] = next_minutes_due
                if minutes_due:
                    merged_upcoming_frame, minutes_summary = _estimate_expected_minutes(
                        merged_upcoming_frame,
                        training_path=training_path,
                    )
                    state["last_expected_minutes_refresh_at"] = _now_iso()
                    state["next_expected_minutes_refresh_due_at"] = _next_due_iso_from_seconds(
                        state["last_expected_minutes_refresh_at"],
                        expected_minutes_refresh_interval_seconds,
                    )
                else:
                    estimated_minutes = pd.to_numeric(
                        merged_upcoming_frame.get("expected_minutes"),
                        errors="coerce",
                    )
                    rows_estimated = int(estimated_minutes.notna().sum())
                    source_series = merged_upcoming_frame.get("expected_minutes_source")
                    if source_series is not None:
                        source_text = source_series.astype(str).str.lower()
                        rows_with_history = int(source_text.str.contains("history").sum())
                    else:
                        rows_with_history = 0
                    minutes_summary = {
                        "rows_estimated": rows_estimated,
                        "rows_unavailable": int(len(merged_upcoming_frame) - rows_estimated),
                        "rows_with_history": rows_with_history,
                        "note": "Reused cached expected-minutes values until next refresh window.",
                    }
                # Lock-window stage (T-90/T-30/T-5) must update every sync cycle even when full
                # minutes re-estimation is not due, otherwise these fields go stale near tipoff.
                merged_upcoming_frame = _refresh_pregame_lock_window_fields(
                    merged_upcoming_frame,
                    now_utc=now_context_utc,
                )
            else:
                state["next_expected_minutes_refresh_due_at"] = None
                minutes_summary = {"rows_estimated": 0, "rows_unavailable": 0, "rows_with_history": 0}
            shot_style_due, next_shot_style_due = _is_due_seconds(
                state.get("last_shot_style_context_refresh_at"),
                shot_style_context_refresh_interval_seconds,
                now_utc=now_context_utc,
            )
            state["next_shot_style_context_refresh_due_at"] = next_shot_style_due
            if shot_style_due:
                merged_upcoming_frame, shot_style_summary = _apply_shot_style_context(
                    merged_upcoming_frame,
                    training_path=training_path,
                )
                state["last_shot_style_context_refresh_at"] = _now_iso()
                state["next_shot_style_context_refresh_due_at"] = _next_due_iso_from_seconds(
                    state["last_shot_style_context_refresh_at"],
                    shot_style_context_refresh_interval_seconds,
                )
            else:
                shot_profile = pd.to_numeric(merged_upcoming_frame.get("shot_style_arc_score"), errors="coerce")
                opponent_profile = pd.to_numeric(merged_upcoming_frame.get("opponent_avg_height_inches"), errors="coerce")
                rebound_profile = pd.to_numeric(
                    merged_upcoming_frame.get("shot_style_rebound_environment"),
                    errors="coerce",
                )
                shot_style_summary = {
                    "rows_with_shot_profile": int(shot_profile.notna().sum()),
                    "rows_with_opponent_profile": int(opponent_profile.notna().sum()),
                    "rows_with_rebound_environment": int(rebound_profile.notna().sum()),
                    "teams_evaluated": int(merged_upcoming_frame[["game_date", "team"]].drop_duplicates().shape[0]),
                    "note": "Reused cached shot-style context values until next refresh window.",
                }
            provider_status["shot_style_context"] = {
                "enabled": True,
                "source": "Derived shot-style + matchup context",
                "rows_with_shot_profile": int(shot_style_summary.get("rows_with_shot_profile", 0)),
                "rows_with_opponent_profile": int(shot_style_summary.get("rows_with_opponent_profile", 0)),
                "rows_with_rebound_environment": int(shot_style_summary.get("rows_with_rebound_environment", 0)),
                "teams_evaluated": int(shot_style_summary.get("teams_evaluated", 0)),
                "last_error": None,
            }

            previous_signature = str(state.get("projection_context_signature") or "")
            context_signature = _stable_frame_signature(
                merged_upcoming_frame,
                key_columns=CONTEXT_KEY_COLUMNS,
                value_columns=PREDICTION_TRIGGER_COLUMNS,
            )
            context_changed = bool(previous_signature) and bool(context_signature) and context_signature != previous_signature
            state["projection_context_signature"] = context_signature
            state["projection_context_changed"] = bool(context_changed)
            if context_changed:
                state["last_context_change_detected_at"] = _now_iso()

            upcoming_fingerprint = compute_frame_fingerprint(merged_upcoming_frame)
            upcoming_idempotency = check_and_register_idempotency(
                dataset="upcoming_slate",
                source="live_sync:upcoming_slate",
                fingerprint=upcoming_fingerprint,
                metadata={
                    "rows": int(len(merged_upcoming_frame)),
                    "path": str(upcoming_path),
                },
            )
            _write_csv_frame(
                upcoming_path,
                merged_upcoming_frame,
                ["player_name", "game_date", "home", "opponent", "team"] + UPCOMING_CONTEXT_COLUMNS,
            )
            record_ingestion_event(
                dataset="upcoming_slate",
                stage="live_sync",
                rows_in=len(merged_upcoming_frame),
                rows_out=_csv_row_count(upcoming_path),
                rows_rejected=0,
                source="live_sync:upcoming_slate",
                outcome="duplicate_skipped" if upcoming_idempotency.get("duplicate") else "success",
                details={
                    "idempotency": upcoming_idempotency,
                    "path": str(upcoming_path),
                },
            )

            state["upcoming_rows_generated"] = int(len(merged_upcoming_frame))
            state["provider_context_rows"] = int(len(provider_context_frame))
            state["upcoming_note"] = upcoming_note
            state["scheduled_games_found"] = slate_meta["scheduled_games_found"]
            state["scheduled_game_dates"] = slate_meta["scheduled_game_dates"]
            state["scheduled_sources"] = slate_meta["scheduled_sources"]
            state["providers"] = provider_status
            state["official_injury_report"] = injury_status.get("latest_report_url")
            state["lineup_rows_matched"] = lineups_status.get("rows", 0)
            state["lineup_dates_loaded"] = lineups_status.get("dates_loaded", [])
            state["live_roster_rows_matched"] = live_rosters_status.get("rows", 0)
            state["live_roster_games_loaded"] = live_rosters_status.get("games_loaded", 0)
            state["player_props_rows_matched"] = player_props_status.get("rows", 0)
            state["rotowire_prizepicks_rows_matched"] = rotowire_status.get("rows", 0)
            state["playstyle_rows_matched"] = playstyle_status.get("rows", 0)
            state["news_rows_matched"] = news_status.get("rows", 0)
            state["news_articles_loaded"] = news_status.get("articles_loaded", 0)
            game_notes_status = provider_status.get("game_notes", {}) if isinstance(provider_status.get("game_notes"), dict) else {}
            state["game_notes_live_rows"] = int(game_notes_status.get("live_rows", 0) or 0)
            state["postgame_review_rows"] = int(game_notes_status.get("postgame_rows", 0) or 0)
            state["game_notes_daily_rows"] = int(game_notes_status.get("compiled_rows", 0) or 0)
            espn_live_status = provider_status.get("espn_live", {}) if isinstance(provider_status.get("espn_live"), dict) else {}
            state["espn_live_rows"] = int(espn_live_status.get("rows", 0) or 0)
            state["espn_live_rows_appended"] = int(espn_live_status.get("rows_appended", 0) or 0)
            state["espn_live_rows_appended_to_game_notes"] = int(
                espn_live_status.get("rows_appended_to_game_notes", 0) or 0
            )
            state["espn_live_events_loaded"] = int(espn_live_status.get("events_loaded", 0) or 0)
            state["home_context_rows"] = home_hometown_summary.get("rows_with_home_context", 0)
            state["hometown_context_rows"] = home_hometown_summary.get("rows_with_hometown_context", 0)
            state["profile_cache_rows"] = home_hometown_summary.get("profile_cache_rows", 0)
            state["profiles_fetched"] = home_hometown_summary.get("profiles_fetched", 0)
            state["teammate_context_rows"] = teammate_summary.get("rows_with_context", 0)
            state["shot_style_context_rows"] = shot_style_summary.get("rows_with_shot_profile", 0)
            state["shot_style_opponent_rows"] = shot_style_summary.get("rows_with_opponent_profile", 0)
            state["shot_style_rebound_rows"] = shot_style_summary.get("rows_with_rebound_environment", 0)
            state["playstyle_context_rows"] = int(playstyle_status.get("rows", 0))
            state["expected_minutes_estimated_rows"] = minutes_summary.get("rows_estimated", 0)
            state["expected_minutes_history_rows"] = minutes_summary.get("rows_with_history", 0)
            state["expected_minutes_unavailable_rows"] = minutes_summary.get("rows_unavailable", 0)
            if "pregame_lock_window_stage" in merged_upcoming_frame.columns:
                lock_stage_series = merged_upcoming_frame["pregame_lock_window_stage"].astype("string")
                state["pregame_lock_window_rows"] = int(lock_stage_series.notna().sum())
                state["rows_in_lock_windows"] = int(
                    lock_stage_series.isin(["t_minus_90", "t_minus_30", "t_minus_5"]).sum()
                )
            else:
                state["pregame_lock_window_rows"] = 0
                state["rows_in_lock_windows"] = 0
            state["starter_probability_rows"] = int(
                pd.to_numeric(merged_upcoming_frame.get("starter_probability"), errors="coerce").notna().sum()
            )
            state["starter_certainty_rows"] = int(
                pd.to_numeric(merged_upcoming_frame.get("starter_certainty"), errors="coerce").notna().sum()
            )
            state["injury_risk_rows"] = int(
                pd.to_numeric(merged_upcoming_frame.get("injury_risk_score"), errors="coerce").notna().sum()
            )
            state["injury_multiplier_rows"] = int(
                pd.to_numeric(merged_upcoming_frame.get("injury_minutes_multiplier"), errors="coerce").notna().sum()
            )
            state["line_consensus_rows"] = int(
                pd.to_numeric(merged_upcoming_frame.get("line_points_consensus"), errors="coerce").notna().sum()
                + pd.to_numeric(merged_upcoming_frame.get("line_rebounds_consensus"), errors="coerce").notna().sum()
                + pd.to_numeric(merged_upcoming_frame.get("line_assists_consensus"), errors="coerce").notna().sum()
                + pd.to_numeric(merged_upcoming_frame.get("line_pra_consensus"), errors="coerce").notna().sum()
            )
        else:
            state["upcoming_rows_generated"] = 0
            state["provider_context_rows"] = 0
            state["upcoming_note"] = (
                "Automatic slate generation is disabled."
                if bool(config.get("auto_build_upcoming_slate", True)) is False
                else "Automatic slate generation skipped because live ingest module is disabled."
            )
            state["scheduled_games_found"] = 0
            state["scheduled_game_dates"] = []
            state["scheduled_sources"] = []
            state["lineup_rows_matched"] = 0
            state["lineup_dates_loaded"] = []
            state["live_roster_rows_matched"] = 0
            state["live_roster_games_loaded"] = 0
            state["player_props_rows_matched"] = 0
            state["rotowire_prizepicks_rows_matched"] = 0
            state["playstyle_rows_matched"] = 0
            state["shot_style_context_rows"] = 0
            state["shot_style_opponent_rows"] = 0
            state["shot_style_rebound_rows"] = 0
            state["playstyle_context_rows"] = 0
            state["news_rows_matched"] = 0
            state["news_articles_loaded"] = 0
            state["home_context_rows"] = 0
            state["hometown_context_rows"] = 0
            state["profile_cache_rows"] = 0
            state["profiles_fetched"] = 0
            state["teammate_context_rows"] = 0
            state["game_notes_live_rows"] = 0
            state["postgame_review_rows"] = 0
            state["game_notes_daily_rows"] = 0
            state["espn_live_rows"] = 0
            state["espn_live_rows_appended"] = 0
            state["espn_live_rows_appended_to_game_notes"] = 0
            state["espn_live_events_loaded"] = 0
            state["expected_minutes_estimated_rows"] = 0
            state["expected_minutes_history_rows"] = 0
            state["expected_minutes_unavailable_rows"] = 0
            state["pregame_lock_window_rows"] = 0
            state["rows_in_lock_windows"] = 0
            state["starter_probability_rows"] = 0
            state["starter_certainty_rows"] = 0
            state["injury_risk_rows"] = 0
            state["injury_multiplier_rows"] = 0
            state["line_consensus_rows"] = 0
            state["next_espn_live_refresh_due_at"] = None
            state["projection_context_changed"] = False
            state["next_playstyle_refresh_due_at"] = None
            state["next_news_refresh_due_at"] = None
            state["next_expected_minutes_refresh_due_at"] = None
            state["next_teammate_context_refresh_due_at"] = None
            state["next_shot_style_context_refresh_due_at"] = None
            _write_csv_frame(provider_context_path, pd.DataFrame(), CONTEXT_KEY_COLUMNS + OPTIONAL_CONTEXT_COLUMNS)

        now_utc = datetime.now(timezone.utc)
        effective_projection_interval_seconds = max(
            int(projection_interval_seconds),
            int(prediction_min_interval_seconds),
        )
        projection_due_by_interval, _ = _is_due_seconds(
            state.get("last_projection_refresh_at"),
            effective_projection_interval_seconds,
            now_utc=now_utc,
        )
        if state.get("last_projection_refresh_at") is None:
            projection_due = True
        elif prediction_on_context_change_only:
            projection_due = bool(context_changed)
        else:
            projection_due = bool(projection_due_by_interval)
        if bool(config.get("force_projection_refresh_on_context_change", True)) and context_changed:
            projection_due = True
        optimization_due = False
        if run_heavy_model_tasks and auto_self_optimize and not state.get("last_optimization_at"):
            state["last_optimization_at"] = _now_iso()
        if run_heavy_model_tasks and auto_self_optimize:
            optimization_due, _ = _is_due_seconds(
                state.get("last_optimization_at"),
                optimization_interval_seconds,
                now_utc=now_utc,
            )
        retrain_due_on_interval = False
        if run_heavy_model_tasks and auto_retrain_each_interval and not state.get("last_retrain_refresh_at"):
            state["last_retrain_refresh_at"] = _now_iso()
        if run_heavy_model_tasks and auto_retrain_each_interval:
            retrain_due_on_interval, _ = _is_due_seconds(
                state.get("last_retrain_refresh_at"),
                retrain_interval_seconds,
                now_utc=now_utc,
            )
        benchmark_due = False
        if run_heavy_model_tasks and auto_run_rotowire_benchmark and not state.get("last_benchmark_run_at"):
            state["last_benchmark_run_at"] = _now_iso()
        if run_heavy_model_tasks and auto_run_rotowire_benchmark:
            benchmark_due, _ = _is_due_seconds(
                state.get("last_benchmark_run_at"),
                benchmark_interval_seconds,
                now_utc=now_utc,
            )

        if run_heavy_model_tasks and auto_self_optimize and optimization_due:
            configured_candidates = config.get("optimization_candidate_lookbacks_days", [14, 21, 28, 35, 42, 56])
            if isinstance(configured_candidates, str):
                candidate_values = [token.strip() for token in configured_candidates.split(",") if token.strip()]
            elif isinstance(configured_candidates, list):
                candidate_values = list(configured_candidates)
            else:
                candidate_values = [14, 21, 28, 35, 42]
            candidate_lookbacks: list[int] = []
            for value in candidate_values:
                numeric = pd.to_numeric(pd.Series([value]), errors="coerce").iloc[0]
                if pd.isna(numeric):
                    continue
                lookback = int(numeric)
                if lookback > 0 and lookback not in candidate_lookbacks:
                    candidate_lookbacks.append(lookback)
            if model_lookback_days and model_lookback_days not in candidate_lookbacks:
                candidate_lookbacks.append(model_lookback_days)
            optimization_summary = _run_lookback_optimization(
                training_path=training_path,
                candidate_lookbacks=candidate_lookbacks,
                recheck_sample_rows=optimization_recheck_sample_rows,
            )
            state["optimization_summary"] = optimization_summary
            state["last_optimization_at"] = _now_iso()
            best_lookback = optimization_summary.get("best_lookback_days")
            if best_lookback is not None:
                model_lookback_days = int(best_lookback)
                if config.get("model_training_lookback_days") != model_lookback_days:
                    config["model_training_lookback_days"] = model_lookback_days
                    save_live_config(config, config_path=config_path)
                state["model_training_lookback_days"] = model_lookback_days
            state["last_train_triggered"] = bool(optimization_summary.get("ran"))
            if state["last_train_triggered"]:
                state["last_retrain_refresh_at"] = _now_iso()
            projection_due = True

        retrain_due_to_new_results = False
        if bool(config.get("auto_retrain_on_new_results", True)) and (
            state["completed_rows_appended"] > 0 or not DEFAULT_BUNDLE_PATH.exists()
        ):
            # Guard against retrain thrash when small new-result batches arrive frequently.
            retrain_due_to_new_results, _ = _is_due_seconds(
                state.get("last_retrain_refresh_at"),
                retrain_interval_seconds,
                now_utc=now_utc,
            )
        should_retrain = run_heavy_model_tasks and (retrain_due_to_new_results or retrain_due_on_interval)
        if should_retrain and not (auto_self_optimize and optimization_due):
            train_engine(data_path=training_path, lookback_days=model_lookback_days)
            state["last_train_triggered"] = True
            state["last_retrain_refresh_at"] = _now_iso()
            projection_due = True

        prediction_refreshed = False
        if (
            model_trainer_module_enabled
            and config.get("auto_predict_after_sync", True)
            and DEFAULT_BUNDLE_PATH.exists()
            and _csv_has_rows(upcoming_path)
            and projection_due
        ):
            prediction_input_path = upcoming_path
            prediction_rows_used = _csv_row_count(upcoming_path)
            try:
                prediction_source = pd.read_csv(upcoming_path)
            except (OSError, pd.errors.EmptyDataError):
                prediction_source = pd.DataFrame()
            if not prediction_source.empty and len(prediction_source) > prediction_max_rows_per_cycle:
                working_prediction = prediction_source.copy()
                working_prediction["game_date"] = pd.to_datetime(working_prediction.get("game_date"), errors="coerce")
                working_prediction["starter"] = pd.to_numeric(working_prediction.get("starter"), errors="coerce").fillna(0.0)
                working_prediction["starter_probability"] = pd.to_numeric(
                    working_prediction.get("starter_probability"),
                    errors="coerce",
                ).fillna(0.0)
                working_prediction["expected_minutes"] = pd.to_numeric(
                    working_prediction.get("expected_minutes"),
                    errors="coerce",
                ).fillna(0.0)
                working_prediction["projection_priority"] = (
                    working_prediction["starter"].clip(lower=0.0, upper=1.0) * 1.4
                    + working_prediction["starter_probability"].clip(lower=0.0, upper=1.0) * 1.1
                    + (working_prediction["expected_minutes"] / 36.0).clip(lower=0.0, upper=1.0)
                )
                working_prediction = working_prediction.sort_values(
                    ["game_date", "projection_priority", "expected_minutes"],
                    ascending=[True, False, False],
                    na_position="last",
                )
                working_prediction = working_prediction.head(prediction_max_rows_per_cycle).drop(columns=["projection_priority"])
                prediction_input_path = upcoming_path.with_name("_prediction_input_live.csv")
                working_prediction.to_csv(prediction_input_path, index=False)
                prediction_rows_used = int(len(working_prediction))
            state["prediction_rows_used"] = int(prediction_rows_used)
            predict_engine(input_path=prediction_input_path, predict_all=False)
            state["last_predict_triggered"] = True
            state["last_projection_refresh_at"] = _now_iso()
            prediction_refreshed = True
            if capture_benchmark_snapshot_on_refresh:
                try:
                    benchmark_snapshot_payload = capture_rotowire_benchmark_snapshot(
                        predictions_path=DEFAULT_PREDICTIONS_PATH,
                        snapshot_path=DEFAULT_BENCHMARK_SNAPSHOT_PATH,
                    )
                    state["last_benchmark_snapshot_at"] = benchmark_snapshot_payload.get("captured_at")
                    state["benchmark_snapshot_rows_added"] = int(
                        benchmark_snapshot_payload.get("rows_added", 0) or 0
                    )
                    state["benchmark_snapshot_rows_total"] = int(
                        benchmark_snapshot_payload.get("rows_total", 0) or 0
                    )
                    state["benchmark_snapshot_last_error"] = None
                except Exception as benchmark_exc:  # noqa: BLE001
                    state["benchmark_snapshot_last_error"] = _sanitize_error_message(benchmark_exc)
                    state["benchmark_snapshot_rows_added"] = 0

        if run_heavy_model_tasks and auto_run_rotowire_benchmark and benchmark_due:
            try:
                benchmark_payload = run_rotowire_benchmark(
                    lookback_days=benchmark_lookback_days,
                    training_path=training_path,
                    snapshot_path=DEFAULT_BENCHMARK_SNAPSHOT_PATH,
                    report_path=DEFAULT_BENCHMARK_REPORT_PATH,
                )
                state["last_benchmark_run_at"] = _now_iso()
                state["benchmark_rows_evaluated"] = int(benchmark_payload.get("rows_evaluated", 0) or 0)
                state["benchmark_last_generated_at"] = benchmark_payload.get("generated_at")
                state["benchmark_last_error"] = None
                state["benchmark_last_note"] = benchmark_payload.get("note")
                try:
                    adaptive_payload = refresh_adaptive_learning_from_benchmark()
                    state["adaptive_learning_last_run_at"] = _now_iso()
                    state["adaptive_learning_rows_total"] = int(adaptive_payload.get("rows_total", 0) or 0)
                    state["adaptive_learning_rows_added"] = int(adaptive_payload.get("rows_added", 0) or 0)
                    state["adaptive_learning_rows_in_window"] = int(adaptive_payload.get("rows_in_window", 0) or 0)
                    state["adaptive_learning_miss_rate_14d"] = _safe_float_value(
                        adaptive_payload.get("miss_rate_14d"),
                        0.0,
                    )
                    state["adaptive_learning_profile_path"] = (
                        adaptive_payload.get("profile_path")
                        or str(DEFAULT_ADAPTIVE_LEARNING_PROFILE_PATH)
                    )
                    state["adaptive_learning_miss_log_path"] = (
                        adaptive_payload.get("miss_log_path")
                        or str(DEFAULT_PREDICTION_MISS_LOG_PATH)
                    )
                    state["adaptive_learning_last_note"] = adaptive_payload.get("note")
                    state["adaptive_learning_last_error"] = adaptive_payload.get("last_error")
                except Exception as adaptive_exc:  # noqa: BLE001
                    state["adaptive_learning_last_run_at"] = _now_iso()
                    state["adaptive_learning_last_error"] = _sanitize_error_message(adaptive_exc)
            except Exception as benchmark_exc:  # noqa: BLE001
                benchmark_message = _sanitize_error_message(benchmark_exc)
                state["last_benchmark_run_at"] = _now_iso()
                if "No overlapping rows were found" in benchmark_message or "Benchmark snapshots were not found" in benchmark_message:
                    state["benchmark_last_error"] = None
                    state["benchmark_last_note"] = benchmark_message
                else:
                    state["benchmark_last_error"] = benchmark_message
                    state["benchmark_last_note"] = benchmark_message

        if auto_contract_drift_audit:
            drift_due, _ = _is_due_seconds(
                state.get("contract_drift_last_run_at"),
                contract_drift_interval_seconds,
                now_utc=datetime.now(timezone.utc),
            )
            if drift_due:
                try:
                    drift_report = run_contract_drift_audit()
                    warnings: list[str] = []
                    for dataset_report in drift_report.get("datasets", []):
                        dataset_name = str(dataset_report.get("dataset", "unknown"))
                        missing_required = list(dataset_report.get("missing_required_columns") or [])
                        unexpected = list(dataset_report.get("unexpected_columns") or [])
                        if len(missing_required) > drift_max_missing_required_columns:
                            warnings.append(
                                f"{dataset_name}: missing required columns -> {', '.join(missing_required)}"
                            )
                        if drift_alert_on_unexpected and len(unexpected) > drift_max_unexpected_columns:
                            warnings.append(
                                f"{dataset_name}: unexpected columns -> {', '.join(unexpected)}"
                            )
                    state["contract_drift_summary"] = drift_report.get("summary", {})
                    state["contract_drift_warning_count"] = int(len(warnings))
                    state["contract_drift_warnings"] = warnings
                    state["contract_drift_last_error"] = None
                    state["contract_drift_last_run_at"] = _now_iso()
                except Exception as drift_exc:  # noqa: BLE001
                    state["contract_drift_last_error"] = _sanitize_error_message(drift_exc)
                    state["contract_drift_last_run_at"] = _now_iso()
            state["contract_drift_next_due_at"] = _next_due_iso_from_seconds(
                state.get("contract_drift_last_run_at"),
                contract_drift_interval_seconds,
                now_utc=datetime.now(timezone.utc),
            )
        else:
            state["contract_drift_next_due_at"] = None

        state["next_projection_refresh_due_at"] = _next_due_iso_from_seconds(
            state.get("last_projection_refresh_at"),
            effective_projection_interval_seconds,
            now_utc=datetime.now(timezone.utc),
        )
        state["next_in_game_projection_refresh_due_at"] = _next_due_iso_from_seconds(
            state.get("last_in_game_projection_refresh_at"),
            in_game_projection_interval_seconds,
            now_utc=datetime.now(timezone.utc),
        )
        if run_heavy_model_tasks and auto_self_optimize:
            state["next_optimization_due_at"] = _next_due_iso_from_seconds(
                state.get("last_optimization_at"),
                optimization_interval_seconds,
                now_utc=datetime.now(timezone.utc),
            )
        else:
            state["next_optimization_due_at"] = None
        if run_heavy_model_tasks and auto_retrain_each_interval:
            state["next_retrain_due_at"] = _next_due_iso_from_seconds(
                state.get("last_retrain_refresh_at"),
                retrain_interval_seconds,
                now_utc=datetime.now(timezone.utc),
            )
        else:
            state["next_retrain_due_at"] = None
        if run_heavy_model_tasks and auto_run_rotowire_benchmark:
            state["next_benchmark_run_due_at"] = _next_due_iso_from_seconds(
                state.get("last_benchmark_run_at"),
                benchmark_interval_seconds,
                now_utc=datetime.now(timezone.utc),
            )
        else:
            state["next_benchmark_run_due_at"] = None

        news_provider_config = dict(config.get("providers", {}).get("news", {}))
        if bool(news_provider_config.get("enabled", True)):
            news_refresh_seconds = _clamp_interval(
                news_provider_config.get("refresh_interval_seconds", MIN_NEWS_REFRESH_INTERVAL_SECONDS),
                fallback=MIN_NEWS_REFRESH_INTERVAL_SECONDS,
                minimum=MIN_NEWS_REFRESH_INTERVAL_SECONDS,
            )
            if force_provider_refresh_every_poll:
                news_refresh_seconds = MIN_NEWS_REFRESH_INTERVAL_SECONDS
            _, next_news_due_at = _is_due_seconds(
                state.get("last_news_refresh_at"),
                news_refresh_seconds,
                now_utc=datetime.now(timezone.utc),
            )
            state["next_news_refresh_due_at"] = next_news_due_at
        else:
            state["next_news_refresh_due_at"] = None

        cloud_archive_provider = dict((config.get("providers", {}) or {}).get("cloud_archive", {}))
        cloud_archive_sync_interval = _clamp_interval(
            cloud_archive_provider.get("sync_interval_seconds", 60),
            fallback=60,
            minimum=10,
        )
        cloud_sync_due, next_cloud_sync_due = _is_due_seconds(
            state.get("last_cloud_archive_sync_at"),
            cloud_archive_sync_interval,
            now_utc=datetime.now(timezone.utc),
        )
        state["next_cloud_archive_sync_due_at"] = next_cloud_sync_due
        state["cloud_archive_enabled"] = bool(cloud_archive_provider.get("enabled", True))
        if cloud_sync_due and state["cloud_archive_enabled"]:
            cloud_archive_status = _sync_cloud_archive(
                config=config,
                state=state,
                training_path=training_path,
                upcoming_path=upcoming_path,
                context_path=context_path,
                provider_context_path=provider_context_path,
            )
            state["last_cloud_archive_sync_at"] = _now_iso()
            state["next_cloud_archive_sync_due_at"] = _next_due_iso_from_seconds(
                state["last_cloud_archive_sync_at"],
                cloud_archive_sync_interval,
            )
            state["cloud_archive_rows_synced"] = int(cloud_archive_status.get("rows_synced", 0) or 0)
            state["cloud_archive_path"] = cloud_archive_status.get("path")
            state["cloud_archive_note"] = cloud_archive_status.get("note")
            state["cloud_archive_last_error"] = cloud_archive_status.get("last_error")
            providers_dict = state.get("providers", {}) if isinstance(state.get("providers"), dict) else {}
            providers_dict["cloud_archive"] = cloud_archive_status
            state["providers"] = providers_dict
        elif state["cloud_archive_enabled"]:
            state["cloud_archive_path"] = str(_resolve_cloud_archive_root(cloud_archive_provider))
            state["cloud_archive_note"] = "Cloud archive is scheduled and awaiting next sync window."
            state["cloud_archive_last_error"] = None
        else:
            state["cloud_archive_path"] = str(_resolve_cloud_archive_root(cloud_archive_provider))
            state["cloud_archive_note"] = "Cloud archive is disabled."
            state["cloud_archive_last_error"] = None

        neon_provider = dict((config.get("providers", {}) or {}).get("neon_sync", {}))
        neon_sync_interval = _clamp_interval(
            neon_provider.get("sync_interval_seconds", DEFAULT_NEON_SYNC_INTERVAL_SECONDS),
            fallback=DEFAULT_NEON_SYNC_INTERVAL_SECONDS,
            minimum=30,
        )
        neon_sync_due, next_neon_sync_due = _is_due_seconds(
            state.get("last_neon_sync_at"),
            neon_sync_interval,
            now_utc=datetime.now(timezone.utc),
        )
        state["next_neon_sync_due_at"] = next_neon_sync_due
        state["neon_sync_enabled"] = bool(neon_provider.get("enabled", False))
        neon_url = _resolve_neon_database_url(neon_provider)
        state["neon_sync_database_host"] = _neon_database_host(neon_url)
        if neon_sync_due and state["neon_sync_enabled"]:
            neon_status = _sync_neon_archive(
                config=config,
                state=state,
                training_path=training_path,
                upcoming_path=upcoming_path,
                context_path=context_path,
                provider_context_path=provider_context_path,
            )
            state["last_neon_sync_at"] = _now_iso()
            state["next_neon_sync_due_at"] = _next_due_iso_from_seconds(
                state["last_neon_sync_at"],
                neon_sync_interval,
            )
            state["neon_sync_rows_synced"] = int(neon_status.get("rows_synced", 0) or 0)
            state["neon_sync_note"] = neon_status.get("note")
            state["neon_sync_last_error"] = neon_status.get("last_error")
            state["neon_sync_database_host"] = neon_status.get("database_host") or state.get("neon_sync_database_host")
            providers_dict = state.get("providers", {}) if isinstance(state.get("providers"), dict) else {}
            providers_dict["neon_sync"] = neon_status
            state["providers"] = providers_dict
        elif state["neon_sync_enabled"]:
            if neon_url:
                state["neon_sync_note"] = "Neon sync is scheduled and awaiting next sync window."
            else:
                state["neon_sync_note"] = "Neon sync is enabled but NEON_DATABASE_URL is not set."
            state["neon_sync_last_error"] = None
        else:
            state["neon_sync_note"] = "Neon sync is disabled."
            state["neon_sync_last_error"] = None

        module_snapshot = build_support_module_snapshot(
            support_modules_config,
            state,
        )
        state["support_modules"] = module_snapshot
        state["support_modules_order"] = [spec.key for spec in support_module_specs()]
        if alerts_module_enabled:
            module_alerts = summarize_module_alerts(module_snapshot, include_disabled=False)
        else:
            module_alerts = []
        state["module_alerts"] = module_alerts
        state["module_alerts_count"] = int(len(module_alerts))

    except Exception as exc:  # noqa: BLE001
        state["last_error"] = _sanitize_error_message(exc)

    if not isinstance(state.get("support_modules"), dict) or not state.get("support_modules"):
        fallback_snapshot = build_support_module_snapshot(
            support_modules_config,
            state,
        )
        state["support_modules"] = fallback_snapshot
        state["support_modules_order"] = [spec.key for spec in support_module_specs()]
    if not isinstance(state.get("module_alerts"), list):
        state["module_alerts"] = summarize_module_alerts(
            state.get("support_modules", {}),
            include_disabled=False,
        )
    state["module_alerts_count"] = int(len(state.get("module_alerts", [])))

    state["last_sync_duration_seconds"] = round(max(0.0, time.monotonic() - sync_started_monotonic), 3)
    save_live_state(state, state_path)
    return state


class LiveSyncManager:
    def __init__(self, config_path: Path = DEFAULT_LIVE_CONFIG_PATH, state_path: Path = DEFAULT_LIVE_STATE_PATH) -> None:
        self.config_path = config_path
        self.state_path = state_path
        self._thread: threading.Thread | None = None
        self._stop_event = threading.Event()
        self._lock = threading.Lock()

    def start(self) -> dict:
        with self._lock:
            if self._thread and self._thread.is_alive():
                return self.status()

            self._stop_event.clear()
            self._thread = threading.Thread(target=self._run_loop, daemon=True, name="nba-live-sync")
            self._thread.start()
        return self.status()

    def stop(self) -> dict:
        self._stop_event.set()
        if self._thread and self._thread.is_alive():
            self._thread.join(timeout=2)
        return self.status()

    def sync_once(self) -> dict:
        with self._lock:
            return run_live_sync(config_path=self.config_path, state_path=self.state_path)

    def in_game_refresh_once(self) -> dict:
        with self._lock:
            return run_live_in_game_projection_refresh(config_path=self.config_path, state_path=self.state_path)

    def status(self) -> dict:
        config = load_live_config(self.config_path)
        state = load_live_state(self.state_path)
        is_running = bool(self._thread and self._thread.is_alive())
        return {
            "config": config,
            "state": state,
            "running": is_running,
            "live_running": is_running,
        }

    def ensure_running(self) -> dict:
        config = load_live_config(self.config_path)
        should_run = bool(config.get("enabled")) and bool(config.get("auto_start_on_app_launch"))
        running = bool(self._thread and self._thread.is_alive())
        if should_run and not running:
            return self.start()
        return self.status()

    def _record_loop_error(self, stage: str, exc: BaseException) -> None:
        try:
            with self._lock:
                state = load_live_state(self.state_path)
                message = _sanitize_error_message(exc)
                state["live_loop_last_error"] = message
                state["live_loop_last_error_stage"] = stage
                state["live_loop_last_error_at"] = _now_iso()
                state["last_error"] = message
                save_live_state(state, self.state_path)
        except BaseException:
            # Never let error recording crash the live loop.
            return

    def _run_loop(self) -> None:
        last_full_sync_at = 0.0
        last_in_game_refresh_at = 0.0
        while not self._stop_event.is_set():
            config = load_live_config(self.config_path)
            full_sync_interval_seconds = _clamp_interval(
                config.get("poll_interval_seconds", MIN_SYNC_INTERVAL_SECONDS),
                fallback=MIN_SYNC_INTERVAL_SECONDS,
                minimum=MIN_SYNC_INTERVAL_SECONDS,
            )
            in_game_interval_seconds = _clamp_interval(
                config.get("in_game_projection_refresh_interval_seconds", MIN_IN_GAME_REFRESH_INTERVAL_SECONDS),
                fallback=MIN_IN_GAME_REFRESH_INTERVAL_SECONDS,
                minimum=MIN_IN_GAME_REFRESH_INTERVAL_SECONDS,
            )
            in_game_enabled = bool(config.get("auto_refresh_in_game_projections", True))
            now_monotonic = time.monotonic()
            ran_work = False

            if in_game_enabled and (
                last_in_game_refresh_at <= 0.0
                or (now_monotonic - last_in_game_refresh_at) >= in_game_interval_seconds
            ):
                try:
                    self.in_game_refresh_once()
                except BaseException as exc:  # noqa: BLE001
                    self._record_loop_error("in_game_refresh", exc)
                last_in_game_refresh_at = time.monotonic()
                ran_work = True

            if last_full_sync_at <= 0.0 or (now_monotonic - last_full_sync_at) >= full_sync_interval_seconds:
                try:
                    self.sync_once()
                except BaseException as exc:  # noqa: BLE001
                    self._record_loop_error("full_sync", exc)
                last_full_sync_at = time.monotonic()
                ran_work = True

            if ran_work:
                if self._stop_event.wait(0.05):
                    break
                continue

            elapsed_full = max(0.0, time.monotonic() - last_full_sync_at) if last_full_sync_at > 0.0 else full_sync_interval_seconds
            wait_full = max(0.1, full_sync_interval_seconds - elapsed_full)
            if in_game_enabled:
                elapsed_live = (
                    max(0.0, time.monotonic() - last_in_game_refresh_at)
                    if last_in_game_refresh_at > 0.0
                    else in_game_interval_seconds
                )
                wait_live = max(0.1, in_game_interval_seconds - elapsed_live)
            else:
                wait_live = wait_full
            wait_seconds = min(wait_full, wait_live, 1.0)
            if self._stop_event.wait(wait_seconds):
                break


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run or trigger the live NBA sync pipeline.")
    parser.add_argument("--once", action="store_true", help="Run one sync cycle and exit.")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    if args.once:
        print(json.dumps(run_live_sync(), indent=2))
        return

    manager = LiveSyncManager()
    manager.start()
    print(json.dumps(manager.status(), indent=2))
    try:
        while True:
            time.sleep(60)
    except KeyboardInterrupt:
        manager.stop()


if __name__ == "__main__":
    main()
