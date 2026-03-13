from __future__ import annotations

import copy
import csv
import json
import os
import re
import tempfile
from datetime import datetime, timedelta, timezone
from pathlib import Path

ENGINE_PROJECT_DIR = Path(__file__).resolve().parents[1]
ENGINE_MPLCONFIGDIR = ENGINE_PROJECT_DIR / ".matplotlib"
ENGINE_MPLCONFIGDIR.mkdir(parents=True, exist_ok=True)
os.environ.setdefault("LOKY_MAX_CPU_COUNT", "1")
os.environ.setdefault("MPLCONFIGDIR", str(ENGINE_MPLCONFIGDIR))

import joblib
import numpy as np
import pandas as pd
from lightgbm import LGBMRegressor
from sklearn.compose import ColumnTransformer
from sklearn.impute import SimpleImputer
from sklearn.metrics import mean_absolute_error, mean_absolute_percentage_error, root_mean_squared_error
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import OneHotEncoder

try:
    from .features import (
        ALL_TARGETS,
        DEFAULT_CONTEXT_UPDATES_PATH,
        DEFAULT_DATA_PATH,
        DEFAULT_MODELS_DIR,
        DEFAULT_PROVIDER_CONTEXT_PATH,
        DEFAULT_PRIZEPICKS_LINES_PATH,
        DEFAULT_SEASON_PRIORS_PATH,
        DEFAULT_TRAINING_UPLOAD_PATH,
        DEFAULT_UPCOMING_PATH,
        KNOWN_ROLLING_STAT_COLUMNS,
        PRIMARY_TARGETS,
        SCHEMA_GUIDE,
        SUPPORT_TARGETS,
        build_feature_frame,
        csv_data_row_count,
        dataset_preview,
        discover_feature_columns,
        load_dataset,
        load_season_priors,
    )
    from .prizepicks import DEFAULT_PRIZEPICKS_EDGES_PATH, load_prizepicks_edges, load_prizepicks_lines
    from .player_matching import add_player_keys, normalize_team_code
    from .scoring import calculate_draftkings_points, calculate_fanduel_points
except ImportError:
    from features import (
        ALL_TARGETS,
        DEFAULT_CONTEXT_UPDATES_PATH,
        DEFAULT_DATA_PATH,
        DEFAULT_MODELS_DIR,
        DEFAULT_PROVIDER_CONTEXT_PATH,
        DEFAULT_PRIZEPICKS_LINES_PATH,
        DEFAULT_SEASON_PRIORS_PATH,
        DEFAULT_TRAINING_UPLOAD_PATH,
        DEFAULT_UPCOMING_PATH,
        KNOWN_ROLLING_STAT_COLUMNS,
        PRIMARY_TARGETS,
        SCHEMA_GUIDE,
        SUPPORT_TARGETS,
        build_feature_frame,
        csv_data_row_count,
        dataset_preview,
        discover_feature_columns,
        load_dataset,
        load_season_priors,
    )
    from prizepicks import DEFAULT_PRIZEPICKS_EDGES_PATH, load_prizepicks_edges, load_prizepicks_lines
    from player_matching import add_player_keys, normalize_team_code
    from scoring import calculate_draftkings_points, calculate_fanduel_points


DEFAULT_BUNDLE_PATH = DEFAULT_MODELS_DIR / "engine_bundle.joblib"
DEFAULT_METRICS_PATH = DEFAULT_MODELS_DIR / "engine_metrics.json"
DEFAULT_PREDICTIONS_PATH = DEFAULT_MODELS_DIR / "predictions.csv"
DEFAULT_RECHECK_PATH = DEFAULT_MODELS_DIR / "engine_recheck.json"
DEFAULT_CALIBRATION_PATH = DEFAULT_MODELS_DIR / "calibration_profile.json"
DEFAULT_BENCHMARK_JOIN_PATH = DEFAULT_MODELS_DIR / "rotowire_benchmark_joined.csv"
DEFAULT_ROTOWIRE_BENCHMARK_REPORT_PATH = DEFAULT_MODELS_DIR / "rotowire_benchmark_report.json"
DEFAULT_PREDICTION_MISS_LOG_PATH = DEFAULT_MODELS_DIR / "prediction_miss_log.csv"
DEFAULT_ADAPTIVE_LEARNING_PROFILE_PATH = DEFAULT_MODELS_DIR / "adaptive_learning_profile.json"

MINUTES_TARGET = "minutes"
TRAINING_LOOKBACK_DEFAULT_DAYS = 56
RATE_MINUTES_FLOOR = 8.0
RATE_MINUTES_CAP = 44.0
DIRECT_STRATEGY = "direct"
RATE_STRATEGY = "rate_per_minute"
TRAINABLE_MODEL_TARGETS = [MINUTES_TARGET] + PRIMARY_TARGETS + SUPPORT_TARGETS
STAT_MODEL_TARGETS = PRIMARY_TARGETS + SUPPORT_TARGETS
ROLE_WEIGHT_MIN = 0.05
ROLE_WEIGHT_MAX = 1.6
DNP_NOISE_MINUTES_THRESHOLD = 2.0
LOW_SIGNAL_MINUTES_THRESHOLD = 8.0
LOW_SIGNAL_BOX_SCORE_THRESHOLD = 2.0
RECHECK_MINUTES_EVALUATION_FLOOR = 8.0
RECHECK_DNP_MINUTES_THRESHOLD = 1.0
RECHECK_DNP_NEAR_MINUTES_THRESHOLD = 4.0
RECHECK_DNP_BOX_TOTAL_THRESHOLD = 1.0
RECENT_FORM_CLAMP_GAME_THRESHOLD = 5.0
RECENT_FORM_LOWER_CLAMP_RATIO = 0.55
RECENT_FORM_UPPER_CLAMP_RATIO = 1.65
RECENT_FORM_MINUTE_CONTEXT_MIN = 16.0
ROLE_BUCKET_ORDER = ["starter_core", "rotation", "bench_low_minutes"]
MIN_ROLE_MODEL_ROWS = 180
MIN_INITIAL_RECHECK_TRAIN_ROWS = 250
MAX_RECHECK_EVAL_DATES = 28
SEASON_PRIOR_TARGET_MAP = {
    "points": "pts_season",
    "rebounds": "reb_season",
    "assists": "ast_season",
    "steals": "stl_season",
    "blocks": "blk_season",
    "turnovers": "tov_season",
    "three_points_made": "three_pm_season",
}

ADAPTIVE_MARKET_TO_PREDICTION_COLUMN = {
    "points": "predicted_points",
    "rebounds": "predicted_rebounds",
    "assists": "predicted_assists",
    "pra": "predicted_pra",
}
ADAPTIVE_MARKET_BIAS_CAP = {
    "points": 6.0,
    "rebounds": 4.0,
    "assists": 4.0,
    "pra": 8.0,
}

UNAVAILABLE_STATUS_PATTERN = re.compile(
    r"\b(?:out|ofs|out for season|suspended|suspension|inactive)\b",
    flags=re.IGNORECASE,
)
DOUBTFUL_STATUS_PATTERN = re.compile(r"\bdoubtful\b", flags=re.IGNORECASE)
QUESTIONABLE_STATUS_PATTERN = re.compile(
    r"\b(?:questionable|gtd|game[-\s]?time decision|day[-\s]?to[-\s]?day|dtd)\b",
    flags=re.IGNORECASE,
)
PROBABLE_STATUS_PATTERN = re.compile(r"\bprobable\b", flags=re.IGNORECASE)
CONTEXT_KEY_COLUMNS = ["player_key", "team_key", "game_date"]
PREDICTION_CONTEXT_COLUMNS = [
    "team",
    "position",
    "starter",
    "starter_probability",
    "starter_certainty",
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
    "family_context",
    "expected_minutes",
    "expected_minutes_confidence",
    "minutes_projection_error_estimate",
    "pregame_lock_confidence",
    "pregame_lock_tier",
    "pregame_line_freshness_score",
    "pregame_min_line_age_minutes",
    "salary_dk",
    "salary_fd",
    "implied_team_total",
    "game_total",
    "spread",
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
    "rest_days",
    "travel_miles",
]
CONTEXT_NUMERIC_BOUNDS: dict[str, tuple[float, float]] = {
    "starter": (0.0, 1.0),
    "starter_probability": (0.0, 1.0),
    "starter_certainty": (0.0, 1.0),
    "lineup_status_confidence": (0.0, 1.0),
    "age": (17.0, 50.0),
    "height_inches": (60.0, 96.0),
    "weight_lbs": (120.0, 420.0),
    "injury_risk_score": (0.0, 1.0),
    "injury_minutes_multiplier": (0.0, 1.0),
    "home_court_points_boost": (-8.0, 8.0),
    "home_court_minutes_boost": (-6.0, 6.0),
    "hometown_game_flag": (0.0, 1.0),
    "hometown_advantage_score": (0.0, 2.0),
    "teammate_active_core_count": (0.0, 12.0),
    "teammate_out_core_count": (0.0, 12.0),
    "teammate_usage_vacancy": (0.0, 4.0),
    "teammate_continuity_score": (0.0, 1.0),
    "teammate_star_out_flag": (0.0, 1.0),
    "teammate_synergy_points": (-8.0, 8.0),
    "teammate_synergy_rebounds": (-6.0, 6.0),
    "teammate_synergy_assists": (-6.0, 6.0),
    "shot_style_arc_score": (0.0, 1.0),
    "shot_style_release_score": (0.0, 1.0),
    "shot_style_volume_index": (0.0, 40.0),
    "shot_style_miss_pressure": (0.0, 30.0),
    "team_shot_miss_pressure": (0.0, 40.0),
    "opponent_shot_miss_pressure": (0.0, 40.0),
    "opponent_avg_height_inches": (66.0, 90.0),
    "opponent_height_advantage_inches": (-20.0, 20.0),
    "shot_style_tall_mismatch_penalty": (-3.0, 3.0),
    "shot_style_pace_bonus": (-2.0, 2.0),
    "shot_style_rebound_environment": (-2.5, 2.5),
    "playstyle_rim_rate": (0.0, 1.0),
    "playstyle_mid_range_rate": (0.0, 1.0),
    "playstyle_three_rate": (0.0, 1.0),
    "playstyle_catch_shoot_rate": (0.0, 1.0),
    "playstyle_pull_up_rate": (0.0, 1.0),
    "playstyle_drive_rate": (0.0, 3.0),
    "playstyle_assist_potential": (0.0, 3.0),
    "playstyle_paint_touch_rate": (0.0, 3.0),
    "playstyle_post_touch_rate": (0.0, 3.0),
    "playstyle_elbow_touch_rate": (0.0, 3.0),
    "playstyle_rebound_chance_rate": (0.0, 3.0),
    "playstyle_offball_activity_rate": (0.0, 3.0),
    "playstyle_usage_proxy": (0.0, 3.0),
    "playstyle_defensive_event_rate": (0.0, 1.0),
    "playstyle_context_confidence": (0.0, 1.0),
    "news_article_count_24h": (0.0, 500.0),
    "news_injury_mentions_24h": (0.0, 500.0),
    "news_starting_mentions_24h": (0.0, 500.0),
    "news_minutes_limit_mentions_24h": (0.0, 500.0),
    "news_positive_mentions_24h": (0.0, 500.0),
    "news_negative_mentions_24h": (0.0, 500.0),
    "news_risk_score": (0.0, 1.0),
    "news_confidence_score": (0.0, 1.0),
    "expected_minutes": (0.0, 48.0),
    "expected_minutes_confidence": (0.0, 1.0),
    "minutes_projection_error_estimate": (0.0, 30.0),
    "pregame_lock_confidence": (0.0, 1.0),
    "pregame_line_freshness_score": (0.0, 1.0),
    "pregame_min_line_age_minutes": (0.0, 1440.0),
    "salary_dk": (0.0, 50000.0),
    "salary_fd": (0.0, 50000.0),
    "implied_team_total": (60.0, 180.0),
    "game_total": (120.0, 320.0),
    "spread": (-60.0, 60.0),
    "line_points": (0.0, 90.0),
    "line_rebounds": (0.0, 40.0),
    "line_assists": (0.0, 35.0),
    "line_pra": (0.0, 130.0),
    "line_three_points_made": (0.0, 20.0),
    "line_points_rebounds": (0.0, 120.0),
    "line_points_assists": (0.0, 120.0),
    "line_rebounds_assists": (0.0, 80.0),
    "line_steals": (0.0, 8.0),
    "line_blocks": (0.0, 8.0),
    "line_turnovers": (0.0, 12.0),
    "line_steals_blocks": (0.0, 15.0),
    "rest_days": (-2.0, 10.0),
    "travel_miles": (0.0, 10000.0),
}
NBA_TEAM_CODES = {
    "ATL",
    "BOS",
    "BKN",
    "CHA",
    "CHI",
    "CLE",
    "DAL",
    "DEN",
    "DET",
    "GSW",
    "HOU",
    "IND",
    "LAC",
    "LAL",
    "MEM",
    "MIA",
    "MIL",
    "MIN",
    "NOP",
    "NYK",
    "OKC",
    "ORL",
    "PHI",
    "PHX",
    "POR",
    "SAC",
    "SAS",
    "TOR",
    "UTA",
    "WAS",
}

PREDICTION_QUALITY_MINIMUM_SCORE = 0.25
PREDICTION_QUALITY_SCORE_WEIGHTS = {
    "base": 1.0,
    "missing_player_name": -1.0,
    "invalid_game_date": -1.0,
    "invalid_team": -0.75,
    "invalid_opponent": -0.75,
    "missing_minutes_context": -0.35,
    "minutes_context_out_of_range": -0.55,
    "invalid_starter_probability": -0.35,
    "high_injury_risk_with_low_context": -0.25,
    "no_priors_and_low_history": -0.4,
}

TARGET_ERROR_FLOOR_MAP = {
    "minutes": 4.0,
    "points": 5.0,
    "rebounds": 3.0,
    "assists": 3.0,
    "steals": 1.0,
    "blocks": 1.0,
    "turnovers": 1.0,
    "three_points_made": 1.0,
}


def resolve_training_data_path() -> Path:
    if DEFAULT_TRAINING_UPLOAD_PATH.exists():
        return DEFAULT_TRAINING_UPLOAD_PATH
    return DEFAULT_DATA_PATH


def resolve_upcoming_data_path() -> Path:
    if DEFAULT_UPCOMING_PATH.exists():
        return DEFAULT_UPCOMING_PATH
    return DEFAULT_DATA_PATH


def _normalize_nba_team_columns(frame: pd.DataFrame) -> pd.DataFrame:
    working = frame.copy()
    if "team" in working.columns:
        working["team"] = working["team"].map(normalize_team_code)
    if "opponent" in working.columns:
        working["opponent"] = working["opponent"].map(normalize_team_code)
    return working


def _filter_modeling_history_rows(frame: pd.DataFrame) -> tuple[pd.DataFrame, dict[str, int]]:
    if frame.empty:
        return frame, {
            "input_rows": 0,
            "output_rows": 0,
            "removed_non_nba_rows": 0,
            "removed_low_minute_zero_rows": 0,
            "removed_low_signal_rows": 0,
            "removed_missing_player_rows": 0,
            "removed_invalid_game_date_rows": 0,
            "removed_invalid_numeric_rows": 0,
            "removed_out_of_range_rows": 0,
            "removed_missing_team_rows": 0,
            "removed_duplicate_rows": 0,
        }

    working = _normalize_nba_team_columns(frame)
    input_rows = int(len(working))
    removed_non_nba_rows = 0
    removed_low_minute_zero_rows = 0
    removed_low_signal_rows = 0
    removed_missing_player_rows = 0
    removed_invalid_game_date_rows = 0
    removed_invalid_numeric_rows = 0
    removed_out_of_range_rows = 0
    removed_missing_team_rows = 0
    removed_duplicate_rows = 0

    if "player_name" in working.columns:
        player_name = working["player_name"].fillna("").astype(str).str.strip()
        missing_player_mask = player_name.eq("")
        removed_missing_player_rows = int(missing_player_mask.sum())
        if missing_player_mask.any():
            working = working.loc[~missing_player_mask].copy()
            player_name = player_name.loc[~missing_player_mask]
        working["player_name"] = player_name

    if "game_date" in working.columns:
        parsed_dates = pd.to_datetime(working["game_date"], errors="coerce").dt.tz_localize(None)
        invalid_game_date_mask = parsed_dates.isna()
        removed_invalid_game_date_rows = int(invalid_game_date_mask.sum())
        if invalid_game_date_mask.any():
            working = working.loc[~invalid_game_date_mask].copy()
            parsed_dates = parsed_dates.loc[~invalid_game_date_mask]
        working["game_date"] = parsed_dates

    if {"team", "opponent"}.issubset(working.columns):
        team_missing_mask = (
            working["team"].isna()
            | working["opponent"].isna()
            | working["team"].astype(str).str.strip().eq("")
            | working["opponent"].astype(str).str.strip().eq("")
        )
        removed_missing_team_rows = int(team_missing_mask.sum())
        if team_missing_mask.any():
            working = working.loc[~team_missing_mask].copy()

        nba_mask = working["team"].isin(NBA_TEAM_CODES) & working["opponent"].isin(NBA_TEAM_CODES)
        removed_non_nba_rows = int((~nba_mask).sum())
        if nba_mask.any():
            working = working.loc[nba_mask].copy()

    numeric_bounds = {
        "minutes": (0.0, 60.0),
        "points": (0.0, 100.0),
        "rebounds": (0.0, 40.0),
        "assists": (0.0, 30.0),
        "steals": (0.0, 15.0),
        "blocks": (0.0, 15.0),
        "turnovers": (0.0, 20.0),
        "three_points_made": (0.0, 20.0),
    }
    for column, (min_value, max_value) in numeric_bounds.items():
        if column not in working.columns:
            continue
        numeric = pd.to_numeric(working[column], errors="coerce")
        invalid_numeric_mask = numeric.isna()
        removed_invalid_numeric_rows += int(invalid_numeric_mask.sum())
        out_of_range_mask = numeric.notna() & ((numeric < min_value) | (numeric > max_value))
        removed_out_of_range_rows += int(out_of_range_mask.sum())
        drop_mask = invalid_numeric_mask | out_of_range_mask
        if drop_mask.any():
            working = working.loc[~drop_mask].copy()
            numeric = numeric.loc[~drop_mask]
        working[column] = numeric

    if "starter" in working.columns:
        starter_numeric = pd.to_numeric(working["starter"], errors="coerce")
        starter_numeric = starter_numeric.where(starter_numeric.isna(), starter_numeric.clip(lower=0.0, upper=1.0))
        working["starter"] = starter_numeric.fillna(0.0).round().astype(int)

    required_targets = {"minutes", "points", "rebounds", "assists"}
    if required_targets.issubset(working.columns):
        minutes = pd.to_numeric(working["minutes"], errors="coerce").fillna(0.0)
        points = pd.to_numeric(working["points"], errors="coerce").fillna(0.0)
        rebounds = pd.to_numeric(working["rebounds"], errors="coerce").fillna(0.0)
        assists = pd.to_numeric(working["assists"], errors="coerce").fillna(0.0)
        low_minute_zero_box = (
            minutes.lt(DNP_NOISE_MINUTES_THRESHOLD)
            & points.eq(0.0)
            & rebounds.eq(0.0)
            & assists.eq(0.0)
        )
        # Drop near-empty bench stints that add noise without improving fit quality.
        low_signal_box = (
            minutes.lt(LOW_SIGNAL_MINUTES_THRESHOLD)
            & points.add(rebounds).add(assists).le(LOW_SIGNAL_BOX_SCORE_THRESHOLD)
        )
        if "starter" in working.columns:
            starter_values = pd.to_numeric(working["starter"], errors="coerce").fillna(0.0)
            low_signal_box = low_signal_box & starter_values.lt(0.5)
        removed_low_minute_zero_rows = int(low_minute_zero_box.sum())
        removed_low_signal_rows = int((low_signal_box & ~low_minute_zero_box).sum())
        remove_mask = low_minute_zero_box | low_signal_box
        if (~remove_mask).any():
            working = working.loc[~remove_mask].copy()

    dedupe_keys = [column for column in ["player_name", "game_date", "team", "opponent"] if column in working.columns]
    if dedupe_keys:
        before_dedupe = len(working)
        working = working.sort_values(dedupe_keys).drop_duplicates(subset=dedupe_keys, keep="last")
        removed_duplicate_rows = int(before_dedupe - len(working))

    working = working.reset_index(drop=True)
    quality_summary = {
        "input_rows": input_rows,
        "output_rows": int(len(working)),
        "removed_non_nba_rows": removed_non_nba_rows,
        "removed_low_minute_zero_rows": removed_low_minute_zero_rows,
        "removed_low_signal_rows": removed_low_signal_rows,
        "removed_missing_player_rows": removed_missing_player_rows,
        "removed_invalid_game_date_rows": removed_invalid_game_date_rows,
        "removed_invalid_numeric_rows": removed_invalid_numeric_rows,
        "removed_out_of_range_rows": removed_out_of_range_rows,
        "removed_missing_team_rows": removed_missing_team_rows,
        "removed_duplicate_rows": removed_duplicate_rows,
    }
    return working, quality_summary


def build_model(categorical_columns: list[str], numeric_columns: list[str]) -> Pipeline:
    preprocessor = ColumnTransformer(
        transformers=[
            (
                "categorical",
                Pipeline(
                    steps=[
                        ("imputer", SimpleImputer(strategy="most_frequent")),
                        ("onehot", OneHotEncoder(handle_unknown="ignore", sparse_output=False)),
                    ]
                ),
                categorical_columns,
            ),
            (
                "numeric",
                Pipeline(
                    steps=[
                        ("imputer", SimpleImputer(strategy="median")),
                    ]
                ),
                numeric_columns,
            ),
        ],
        verbose_feature_names_out=False,
    )
    preprocessor.set_output(transform="pandas")

    regressor = LGBMRegressor(
        objective="regression",
        n_estimators=400,
        learning_rate=0.04,
        num_leaves=31,
        min_data_in_leaf=1,
        min_data_in_bin=1,
        n_jobs=1,
        random_state=42,
        verbose=-1,
    )

    return Pipeline(
        steps=[
            ("preprocessor", preprocessor),
            ("regressor", regressor),
        ]
    )


def split_time_series(frame: pd.DataFrame, target: str) -> tuple[pd.DataFrame, pd.DataFrame | None]:
    training_frame = frame[frame[target].notna()].sort_values("game_date").reset_index(drop=True)
    if len(training_frame) < 5:
        raise ValueError(f"At least 5 completed rows are required to train the {target} model.")

    if len(training_frame) < 10:
        return training_frame, None

    test_rows = max(1, int(round(len(training_frame) * 0.2)))
    if len(training_frame) - test_rows < 5:
        test_rows = max(1, len(training_frame) - 5)
    train_frame = training_frame.iloc[:-test_rows].copy()
    test_frame = training_frame.iloc[-test_rows:].copy()
    return train_frame, test_frame


def _fit_error_distribution(actual: pd.Series, predicted: pd.Series) -> dict[str, float]:
    actual_series = pd.to_numeric(actual, errors="coerce")
    predicted_series = pd.to_numeric(predicted, errors="coerce")
    valid = actual_series.notna() & predicted_series.notna()
    if not valid.any():
        return {
            "rows": 0,
            "abs_error_p50": 0.0,
            "abs_error_p80": 0.0,
            "abs_error_p90": 0.0,
            "residual_q10": 0.0,
            "residual_q50": 0.0,
            "residual_q90": 0.0,
        }
    residual = actual_series.loc[valid] - predicted_series.loc[valid]
    abs_error = residual.abs()
    return {
        "rows": int(valid.sum()),
        "abs_error_p50": float(abs_error.quantile(0.50)),
        "abs_error_p80": float(abs_error.quantile(0.80)),
        "abs_error_p90": float(abs_error.quantile(0.90)),
        "residual_q10": float(residual.quantile(0.10)),
        "residual_q50": float(residual.quantile(0.50)),
        "residual_q90": float(residual.quantile(0.90)),
    }


def _load_calibration_profile(path: Path = DEFAULT_CALIBRATION_PATH) -> dict:
    if not path.exists():
        return {}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
        return payload if isinstance(payload, dict) else {}
    except Exception:
        return {}


def _load_market_anchor_hardening(report_path: Path = DEFAULT_ROTOWIRE_BENCHMARK_REPORT_PATH) -> dict[str, float]:
    """
    Read recent benchmark deltas and convert them into per-market anchor boost weights.

    If our projection MAE is worse than the line MAE for a market, increase how strongly
    we pull toward high-quality market anchors for that market.
    """
    fallback = {"points": 0.0, "rebounds": 0.0, "assists": 0.0}
    if not report_path.exists():
        return fallback

    try:
        payload = json.loads(report_path.read_text(encoding="utf-8"))
    except Exception:
        return fallback

    if not isinstance(payload, dict):
        return fallback

    per_market = payload.get("per_market")
    if not isinstance(per_market, list):
        return fallback

    boosts: dict[str, float] = dict(fallback)
    for entry in per_market:
        if not isinstance(entry, dict):
            continue
        market = str(entry.get("market", "")).strip().lower()
        if market not in boosts:
            continue

        rows = pd.to_numeric(pd.Series([entry.get("rows")]), errors="coerce").iloc[0]
        mae_delta = pd.to_numeric(pd.Series([entry.get("mae_delta_vs_line")]), errors="coerce").iloc[0]
        if pd.isna(rows) or pd.isna(mae_delta) or rows < 25:
            continue

        # Negative delta means model MAE is worse than line MAE.
        if mae_delta >= 0:
            boosts[market] = 0.0
            continue

        gap = abs(float(mae_delta))
        row_scale = min(1.0, float(rows) / 250.0)
        # Cap to avoid overfitting to market lines.
        raw_boost = min(0.18, (gap / 8.0)) * (0.4 + 0.6 * row_scale)
        boosts[market] = round(float(np.clip(raw_boost, 0.0, 0.18)), 4)

    return boosts


def _load_adaptive_learning_profile(path: Path = DEFAULT_ADAPTIVE_LEARNING_PROFILE_PATH) -> dict:
    if not path.exists():
        return {}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
        return payload if isinstance(payload, dict) else {}
    except Exception:
        return {}


def _normalize_player_key(value: object) -> str:
    normalized = re.sub(r"[^a-z0-9]+", "", str(value or "").strip().lower())
    return normalized


def _normalize_team_key(value: object) -> str:
    normalized = re.sub(r"[^A-Z0-9]+", "", str(value or "").strip().upper())
    return normalized


def _weighted_average(values: pd.Series, weights: pd.Series, default: float = 0.0) -> float:
    values = pd.to_numeric(values, errors="coerce")
    weights = pd.to_numeric(weights, errors="coerce")
    valid = values.notna() & weights.notna()
    if not valid.any():
        return float(default)
    weight_values = weights.loc[valid].clip(lower=0.0)
    if float(weight_values.sum()) <= 0.0:
        return float(values.loc[valid].mean())
    return float(np.average(values.loc[valid], weights=weight_values))


def _safe_numeric(value: object, default: float = 0.0) -> float:
    parsed = pd.to_numeric(pd.Series([value]), errors="coerce").iloc[0]
    if pd.isna(parsed):
        return float(default)
    return float(parsed)


def refresh_adaptive_learning_from_benchmark(
    *,
    benchmark_join_path: Path = DEFAULT_BENCHMARK_JOIN_PATH,
    miss_log_path: Path = DEFAULT_PREDICTION_MISS_LOG_PATH,
    profile_path: Path = DEFAULT_ADAPTIVE_LEARNING_PROFILE_PATH,
    lookback_days: int = 45,
    half_life_days: float = 10.0,
) -> dict[str, object]:
    status: dict[str, object] = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "benchmark_join_path": str(benchmark_join_path),
        "miss_log_path": str(miss_log_path),
        "profile_path": str(profile_path),
        "rows_added": 0,
        "rows_total": 0,
        "rows_in_window": 0,
        "miss_rate_14d": 0.0,
        "last_error": None,
        "note": None,
    }
    if not benchmark_join_path.exists():
        status["note"] = "Benchmark joined dataset was not found; adaptive learning skipped."
        return status

    joined = pd.read_csv(benchmark_join_path)
    if joined.empty:
        status["note"] = "Benchmark joined dataset is empty; adaptive learning skipped."
        return status

    required = {"captured_at", "game_date", "player_name", "team", "market", "projection", "actual", "uncertainty_band"}
    missing_columns = [column for column in sorted(required) if column not in joined.columns]
    if missing_columns:
        status["note"] = f"Benchmark joined dataset is missing required columns: {missing_columns}"
        return status

    working = joined.copy()
    working["captured_at"] = pd.to_datetime(working["captured_at"], errors="coerce", utc=True)
    working["game_date"] = pd.to_datetime(working["game_date"], errors="coerce", utc=True)
    working["projection"] = pd.to_numeric(working["projection"], errors="coerce")
    working["actual"] = pd.to_numeric(working["actual"], errors="coerce")
    working["uncertainty_band"] = pd.to_numeric(working["uncertainty_band"], errors="coerce").fillna(0.0)
    working["line"] = pd.to_numeric(working.get("line"), errors="coerce")
    working = working.dropna(subset=["captured_at", "game_date", "player_name", "team", "market", "projection", "actual"])
    if working.empty:
        status["note"] = "No usable benchmark rows remained after parsing."
        return status

    working["market"] = working["market"].astype(str).str.strip().str.lower()
    working = working[working["market"].isin(ADAPTIVE_MARKET_TO_PREDICTION_COLUMN.keys())].copy()
    if working.empty:
        status["note"] = "No benchmark rows matched adaptive markets (points/rebounds/assists/pra)."
        return status

    working["player_key"] = working["player_name"].map(_normalize_player_key)
    working["team_key"] = working["team"].map(_normalize_team_key)
    working = (
        working.sort_values("captured_at")
        .drop_duplicates(subset=["player_key", "team_key", "game_date", "market"], keep="last")
        .reset_index(drop=True)
    )

    working["residual"] = working["actual"] - working["projection"]
    working["abs_error"] = working["residual"].abs()
    working["miss_threshold"] = np.maximum(1.0, working["uncertainty_band"].clip(lower=0.0) * 0.9)
    working["is_miss"] = working["abs_error"] > working["miss_threshold"]

    miss_columns = [
        "captured_at",
        "game_date",
        "player_name",
        "player_key",
        "team",
        "team_key",
        "market",
        "projection",
        "actual",
        "line",
        "residual",
        "abs_error",
        "uncertainty_band",
        "miss_threshold",
        "is_miss",
        "recommendation",
        "confidence_flag",
        "projection_error_pct_estimate",
    ]
    miss_frame = working[[column for column in miss_columns if column in working.columns]].copy()
    miss_frame["captured_at"] = pd.to_datetime(miss_frame["captured_at"], errors="coerce", utc=True)
    miss_frame["game_date"] = pd.to_datetime(miss_frame["game_date"], errors="coerce", utc=True)
    miss_log_path.parent.mkdir(parents=True, exist_ok=True)

    if miss_log_path.exists():
        existing_miss_frame = pd.read_csv(miss_log_path)
        if not existing_miss_frame.empty:
            existing_miss_frame["captured_at"] = pd.to_datetime(existing_miss_frame["captured_at"], errors="coerce", utc=True)
            existing_miss_frame["game_date"] = pd.to_datetime(existing_miss_frame["game_date"], errors="coerce", utc=True)
        combined = pd.concat([existing_miss_frame, miss_frame], ignore_index=True, sort=False)
        previous_total = int(len(existing_miss_frame))
    else:
        combined = miss_frame
        previous_total = 0

    combined = (
        combined.sort_values("captured_at")
        .drop_duplicates(subset=["captured_at", "player_key", "team_key", "game_date", "market"], keep="last")
        .reset_index(drop=True)
    )
    combined.to_csv(miss_log_path, index=False)

    status["rows_total"] = int(len(combined))
    status["rows_added"] = max(0, int(len(combined)) - previous_total)

    lookback_days = max(7, int(lookback_days))
    half_life_days = max(2.0, float(half_life_days))
    now_utc = datetime.now(timezone.utc)
    cutoff_date = pd.Timestamp(now_utc - timedelta(days=lookback_days))
    window = combined[combined["game_date"] >= cutoff_date].copy()
    if window.empty:
        status["note"] = "Adaptive learning log has no rows inside the active lookback window."
        profile_payload = {
            "generated_at": status["generated_at"],
            "lookback_days": lookback_days,
            "half_life_days": half_life_days,
            "rows_total": int(len(combined)),
            "rows_in_window": 0,
            "overall": {
                "evaluated_rows": 0,
                "miss_rate": 0.0,
                "mean_abs_error": 0.0,
                "mean_abs_pct_error": 0.0,
            },
            "per_market": {},
        }
        profile_path.parent.mkdir(parents=True, exist_ok=True)
        profile_path.write_text(json.dumps(profile_payload, indent=2), encoding="utf-8")
        return status

    window["captured_at"] = pd.to_datetime(window["captured_at"], errors="coerce", utc=True)
    window["game_date"] = pd.to_datetime(window["game_date"], errors="coerce", utc=True)
    age_days = ((pd.Timestamp(now_utc) - window["game_date"]).dt.total_seconds() / 86400.0).clip(lower=0.0)
    window["sample_weight"] = np.exp(-age_days / half_life_days).clip(lower=0.05, upper=1.0)
    window["residual"] = pd.to_numeric(window["residual"], errors="coerce")
    window["abs_error"] = pd.to_numeric(window["abs_error"], errors="coerce")
    window["actual"] = pd.to_numeric(window["actual"], errors="coerce")
    window["is_miss"] = window["is_miss"].astype(bool)
    status["rows_in_window"] = int(len(window))

    recent_cutoff = pd.Timestamp(now_utc - timedelta(days=14))
    recent_window = window[window["game_date"] >= recent_cutoff]
    status["miss_rate_14d"] = round(float(recent_window["is_miss"].mean() * 100.0), 2) if not recent_window.empty else 0.0

    overall_denominator = window["actual"].abs().clip(lower=1.0)
    overall_pct = (window["abs_error"] / overall_denominator.replace(0.0, np.nan)) * 100.0
    overall_payload = {
        "evaluated_rows": int(len(window)),
        "miss_rate": round(float(window["is_miss"].mean() * 100.0), 3),
        "mean_abs_error": round(_weighted_average(window["abs_error"], window["sample_weight"], default=0.0), 4),
        "mean_abs_pct_error": round(_weighted_average(overall_pct, window["sample_weight"], default=0.0), 4),
    }

    per_market_payload: dict[str, dict[str, object]] = {}
    pct_floor_by_market = {"points": 1.0, "rebounds": 1.0, "assists": 1.0, "pra": 2.0}
    for market, prediction_column in ADAPTIVE_MARKET_TO_PREDICTION_COLUMN.items():
        market_frame = window[window["market"] == market].copy()
        if market_frame.empty:
            continue

        cap = float(ADAPTIVE_MARKET_BIAS_CAP.get(market, 4.0))
        global_bias = _weighted_average(market_frame["residual"], market_frame["sample_weight"], default=0.0)
        global_bias = float(np.clip(global_bias, -cap, cap))
        market_denominator = market_frame["actual"].abs().clip(lower=pct_floor_by_market.get(market, 1.0))
        market_pct = (market_frame["abs_error"] / market_denominator.replace(0.0, np.nan)) * 100.0

        def _bias_map(
            frame: pd.DataFrame,
            *,
            group_column: str,
            min_rows: int,
            group_cap: float,
        ) -> tuple[dict[str, float], dict[str, int]]:
            bias_map: dict[str, float] = {}
            sample_map: dict[str, int] = {}
            if group_column not in frame.columns:
                return bias_map, sample_map
            grouped = frame.groupby(group_column, dropna=True, sort=False)
            for key, group in grouped:
                key_text = str(key or "").strip()
                if not key_text:
                    continue
                sample_count = int(len(group))
                if sample_count < int(min_rows):
                    continue
                weighted_bias = _weighted_average(group["residual"], group["sample_weight"], default=0.0)
                weighted_bias = float(np.clip(weighted_bias, -group_cap, group_cap))
                bias_map[key_text] = round(weighted_bias, 4)
                sample_map[key_text] = sample_count
            return bias_map, sample_map

        player_bias, player_samples = _bias_map(
            market_frame,
            group_column="player_key",
            min_rows=3,
            group_cap=cap,
        )
        team_bias, team_samples = _bias_map(
            market_frame,
            group_column="team_key",
            min_rows=12,
            group_cap=cap * 0.65,
        )
        opponent_bias: dict[str, float] = {}
        opponent_samples: dict[str, int] = {}

        per_market_payload[market] = {
            "prediction_column": prediction_column,
            "rows": int(len(market_frame)),
            "miss_rate": round(float(market_frame["is_miss"].mean() * 100.0), 3),
            "mean_abs_error": round(_weighted_average(market_frame["abs_error"], market_frame["sample_weight"], default=0.0), 4),
            "mean_abs_pct_error": round(_weighted_average(market_pct, market_frame["sample_weight"], default=0.0), 4),
            "global_bias": round(global_bias, 4),
            "player_bias": player_bias,
            "player_samples": player_samples,
            "team_bias": team_bias,
            "team_samples": team_samples,
            "opponent_bias": opponent_bias,
            "opponent_samples": opponent_samples,
        }

    profile_payload = {
        "generated_at": status["generated_at"],
        "benchmark_join_path": str(benchmark_join_path),
        "miss_log_path": str(miss_log_path),
        "lookback_days": lookback_days,
        "half_life_days": half_life_days,
        "rows_total": int(len(combined)),
        "rows_in_window": int(len(window)),
        "overall": overall_payload,
        "per_market": per_market_payload,
    }
    profile_path.parent.mkdir(parents=True, exist_ok=True)
    profile_path.write_text(json.dumps(profile_payload, indent=2), encoding="utf-8")
    status["note"] = "Adaptive miss-learning profile was refreshed from benchmark outcomes."
    return status


def _apply_adaptive_learning_corrections(
    result: pd.DataFrame,
    prediction_frame: pd.DataFrame,
    adaptive_profile: dict | None,
) -> pd.DataFrame:
    if result.empty or not adaptive_profile or not isinstance(adaptive_profile, dict):
        return result
    per_market = adaptive_profile.get("per_market")
    if not isinstance(per_market, dict) or not per_market:
        return result

    adjusted = result.copy()
    player_key_series = prediction_frame.get("player_name", pd.Series("", index=adjusted.index)).map(_normalize_player_key)
    team_key_series = prediction_frame.get("team", pd.Series("", index=adjusted.index)).map(_normalize_team_key)
    opponent_key_series = prediction_frame.get("opponent", pd.Series("", index=adjusted.index)).map(_normalize_team_key)

    for market, prediction_column in ADAPTIVE_MARKET_TO_PREDICTION_COLUMN.items():
        if prediction_column not in adjusted.columns:
            continue
        market_profile = per_market.get(market)
        if not isinstance(market_profile, dict):
            continue

        cap = float(ADAPTIVE_MARKET_BIAS_CAP.get(market, 4.0))
        market_rows = max(1.0, _safe_numeric(market_profile.get("rows"), default=0.0))
        global_reliability = float(np.clip(market_rows / 80.0, 0.0, 1.0))

        global_bias = _safe_numeric(market_profile.get("global_bias"), default=0.0)
        player_bias_map = market_profile.get("player_bias", {}) if isinstance(market_profile.get("player_bias"), dict) else {}
        player_sample_map = market_profile.get("player_samples", {}) if isinstance(market_profile.get("player_samples"), dict) else {}
        team_bias_map = market_profile.get("team_bias", {}) if isinstance(market_profile.get("team_bias"), dict) else {}
        team_sample_map = market_profile.get("team_samples", {}) if isinstance(market_profile.get("team_samples"), dict) else {}
        opponent_bias_map = market_profile.get("opponent_bias", {}) if isinstance(market_profile.get("opponent_bias"), dict) else {}
        opponent_sample_map = market_profile.get("opponent_samples", {}) if isinstance(market_profile.get("opponent_samples"), dict) else {}

        player_bias = player_key_series.map(lambda key: _safe_numeric(player_bias_map.get(key), default=0.0))
        player_samples = player_key_series.map(lambda key: _safe_numeric(player_sample_map.get(key), default=0.0))
        player_reliability = (player_samples / 8.0).clip(lower=0.0, upper=1.0)

        team_bias = team_key_series.map(lambda key: _safe_numeric(team_bias_map.get(key), default=0.0))
        team_samples = team_key_series.map(lambda key: _safe_numeric(team_sample_map.get(key), default=0.0))
        team_reliability = (team_samples / 20.0).clip(lower=0.0, upper=1.0)

        opponent_bias = opponent_key_series.map(lambda key: _safe_numeric(opponent_bias_map.get(key), default=0.0))
        opponent_samples = opponent_key_series.map(lambda key: _safe_numeric(opponent_sample_map.get(key), default=0.0))
        opponent_reliability = (opponent_samples / 20.0).clip(lower=0.0, upper=1.0)

        combined_bias = (
            (global_bias * 0.25 * global_reliability)
            + (team_bias * 0.2 * team_reliability)
            + (opponent_bias * 0.15 * opponent_reliability)
            + (player_bias * 0.6 * player_reliability)
        ).clip(lower=-cap, upper=cap)
        applied_bias = (combined_bias * 0.65).clip(lower=-cap, upper=cap)

        adjusted[prediction_column] = (
            pd.to_numeric(adjusted[prediction_column], errors="coerce").fillna(0.0) + applied_bias
        ).clip(lower=0.0)
        adjusted[f"adaptive_bias_{market}"] = pd.to_numeric(applied_bias, errors="coerce").round(4)

    prediction_columns = [column for column in adjusted.columns if column.startswith("predicted_")]
    for column in prediction_columns:
        adjusted[column] = pd.to_numeric(adjusted[column], errors="coerce").clip(lower=0.0)
    return adjusted


def _target_error_summary_from_profile(
    target: str,
    *,
    bundle: dict | None = None,
    calibration_profile: dict | None = None,
) -> dict[str, float]:
    fallback = {
        "abs_error_p50": 1.0,
        "abs_error_p80": 2.0,
        "abs_error_p90": 3.0,
        "residual_q10": -2.0,
        "residual_q50": 0.0,
        "residual_q90": 2.0,
        "mean_abs_pct_error_floor": 18.0,
    }
    target_floor = TARGET_ERROR_FLOOR_MAP.get(target, 1.0)
    fallback.update(
        {
            "abs_error_p50": target_floor * 0.4,
            "abs_error_p80": target_floor * 0.8,
            "abs_error_p90": target_floor * 1.2,
            "residual_q10": -target_floor * 0.8,
            "residual_q50": 0.0,
            "residual_q90": target_floor * 0.8,
            "mean_abs_pct_error_floor": 18.0,
        }
    )

    if bundle:
        target_distribution = (
            bundle.get("error_distribution", {}).get(target, {})
            if isinstance(bundle.get("error_distribution"), dict)
            else {}
        )
        if isinstance(target_distribution, dict):
            for key in ["abs_error_p50", "abs_error_p80", "abs_error_p90", "residual_q10", "residual_q50", "residual_q90"]:
                value = pd.to_numeric(pd.Series([target_distribution.get(key)]), errors="coerce").iloc[0]
                if pd.notna(value):
                    fallback[key] = float(value)

    if calibration_profile and isinstance(calibration_profile, dict):
        per_target = calibration_profile.get("per_target", {})
        if isinstance(per_target, dict):
            target_profile = per_target.get(target, {})
            if isinstance(target_profile, dict):
                for key in [
                    "abs_error_p50",
                    "abs_error_p80",
                    "abs_error_p90",
                    "residual_q10",
                    "residual_q50",
                    "residual_q90",
                    "mean_abs_pct_error_floor",
                ]:
                    value = pd.to_numeric(pd.Series([target_profile.get(key)]), errors="coerce").iloc[0]
                    if pd.notna(value):
                        fallback[key] = float(value)

    fallback["abs_error_p50"] = max(0.0, float(fallback["abs_error_p50"]))
    fallback["abs_error_p80"] = max(fallback["abs_error_p50"], float(fallback["abs_error_p80"]))
    fallback["abs_error_p90"] = max(fallback["abs_error_p80"], float(fallback["abs_error_p90"]))
    fallback["mean_abs_pct_error_floor"] = max(3.0, float(fallback.get("mean_abs_pct_error_floor", 18.0)))
    return fallback


def _prediction_quality_gate(prediction_frame: pd.DataFrame) -> pd.DataFrame:
    if prediction_frame.empty:
        gated = prediction_frame.copy()
        gated["prediction_quality_score"] = pd.Series(dtype=float)
        gated["prediction_quality_blocked"] = pd.Series(dtype=bool)
        gated["prediction_quality_issues"] = pd.Series(dtype=object)
        return gated

    gated = prediction_frame.copy()
    score = pd.Series(PREDICTION_QUALITY_SCORE_WEIGHTS["base"], index=gated.index, dtype=float)
    issues = pd.Series("", index=gated.index, dtype=object)

    def _series_or_nan(column: str) -> pd.Series:
        if column in gated.columns:
            return pd.to_numeric(gated[column], errors="coerce")
        return pd.Series(np.nan, index=gated.index, dtype=float)

    def _flag(mask: pd.Series, code: str) -> None:
        nonlocal score, issues
        if not mask.any():
            return
        score = score + np.where(mask, PREDICTION_QUALITY_SCORE_WEIGHTS.get(code, 0.0), 0.0)
        issues = issues.where(~mask, issues.where(issues.eq(""), issues + "|") + code)

    player_name = gated.get("player_name", pd.Series("", index=gated.index)).fillna("").astype(str).str.strip()
    _flag(player_name.eq(""), "missing_player_name")

    game_date = pd.to_datetime(gated.get("game_date", pd.Series(pd.NaT, index=gated.index)), format="%Y-%m-%d", errors="coerce")
    _flag(game_date.isna(), "invalid_game_date")

    if "team" in gated.columns:
        team = gated["team"].map(normalize_team_code)
        _flag(team.isna() | ~team.isin(NBA_TEAM_CODES), "invalid_team")
    if "opponent" in gated.columns:
        opponent = gated["opponent"].map(normalize_team_code)
        _flag(opponent.isna() | ~opponent.isin(NBA_TEAM_CODES), "invalid_opponent")

    predicted_minutes = _series_or_nan("predicted_minutes")
    expected_minutes = _series_or_nan("expected_minutes")
    minutes_context = predicted_minutes.combine_first(expected_minutes)
    _flag(minutes_context.isna(), "missing_minutes_context")
    _flag(minutes_context.notna() & ~minutes_context.between(0.0, 48.0), "minutes_context_out_of_range")

    starter_probability = _series_or_nan("starter_probability")
    _flag(starter_probability.notna() & ~starter_probability.between(0.0, 1.0), "invalid_starter_probability")

    injury_risk = _series_or_nan("injury_risk_score").fillna(0.0)
    expected_minutes_conf = _series_or_nan("expected_minutes_confidence").fillna(0.0)
    _flag(injury_risk.ge(0.7) & expected_minutes_conf.lt(0.3), "high_injury_risk_with_low_context")

    priors_available = _series_or_nan("season_priors_available").fillna(0.0)
    games_before = _series_or_nan("games_played_before").fillna(0.0)
    _flag(priors_available.lt(0.5) & games_before.lt(3.0), "no_priors_and_low_history")

    score = score.clip(lower=0.0, upper=1.0)
    blocked = score.lt(PREDICTION_QUALITY_MINIMUM_SCORE)
    gated["prediction_quality_score"] = score.round(3)
    gated["prediction_quality_blocked"] = blocked
    gated["prediction_quality_issues"] = issues.where(issues.ne(""), "")

    for column in ["starter_probability", "expected_minutes_confidence", "injury_risk_score"]:
        if column in gated.columns:
            bounded = pd.to_numeric(gated[column], errors="coerce")
            if column in {"starter_probability", "expected_minutes_confidence", "injury_risk_score"}:
                bounded = bounded.where(bounded.between(0.0, 1.0), np.nan)
            gated[column] = bounded
    if "expected_minutes" in gated.columns:
        expected_minutes_numeric = pd.to_numeric(gated["expected_minutes"], errors="coerce")
        gated["expected_minutes"] = expected_minutes_numeric.where(expected_minutes_numeric.between(0.0, 48.0), np.nan)

    return gated


def _prepare_recheck_upcoming_rows(day_rows: pd.DataFrame) -> pd.DataFrame:
    upcoming = day_rows.copy()
    blank_columns = set(ALL_TARGETS + KNOWN_ROLLING_STAT_COLUMNS)
    for column in blank_columns:
        if column in upcoming.columns:
            upcoming[column] = pd.NA
    return upcoming


def _recency_weights(frame: pd.DataFrame, half_life_days: float = 45.0, min_weight: float = 0.15) -> np.ndarray:
    if "game_date" not in frame.columns:
        return np.ones(len(frame), dtype=float)

    game_dates = pd.to_datetime(frame["game_date"], errors="coerce")
    if game_dates.isna().all():
        return np.ones(len(frame), dtype=float)

    latest = game_dates.max()
    age_days = (latest - game_dates).dt.days.clip(lower=0).fillna(0).astype(float)
    weights = np.exp(-np.log(2) * age_days / max(half_life_days, 1.0))
    return np.clip(weights.fillna(min_weight), min_weight, 1.0).to_numpy(dtype=float)


def _resolve_minutes_context(frame: pd.DataFrame, minute_column: str | None = None) -> pd.Series:
    candidates: list[pd.Series] = []
    if minute_column and minute_column in frame.columns:
        candidates.append(pd.to_numeric(frame[minute_column], errors="coerce"))
    for column in ["expected_minutes", "minutes_avg_last_5", "min_season", "minutes_last_1"]:
        if column in frame.columns:
            candidates.append(pd.to_numeric(frame[column], errors="coerce"))
    if not candidates:
        return pd.Series(24.0, index=frame.index)
    resolved = candidates[0]
    for series in candidates[1:]:
        resolved = resolved.combine_first(series)
    return resolved.fillna(0.0).clip(lower=0.0, upper=48.0)


def _role_minutes_weights(frame: pd.DataFrame, minute_column: str | None = None) -> np.ndarray:
    if frame.empty:
        return np.array([], dtype=float)

    minutes = _resolve_minutes_context(frame, minute_column=minute_column)
    if "starter" in frame.columns:
        starter = pd.to_numeric(frame["starter"], errors="coerce")
        starter = starter.fillna(minutes.ge(28.0).astype(float))
    else:
        starter = minutes.ge(28.0).astype(float)

    role_weight = np.select(
        [starter.ge(0.5), minutes.ge(22.0), minutes.ge(14.0), minutes.ge(8.0)],
        [1.2, 1.0, 0.82, 0.58],
        default=0.38,
    )
    low_minute_penalty = np.select(
        [minutes.lt(4.0), minutes.lt(8.0), minutes.lt(12.0), minutes.lt(16.0)],
        [0.08, 0.2, 0.42, 0.65],
        default=1.0,
    )
    return np.clip(role_weight * low_minute_penalty, ROLE_WEIGHT_MIN, ROLE_WEIGHT_MAX).astype(float)


def _training_sample_weights(
    frame: pd.DataFrame,
    *,
    half_life_days: float = 45.0,
    minute_column: str | None = None,
    apply_minutes_scale: bool = False,
) -> np.ndarray:
    recency = _recency_weights(frame, half_life_days=half_life_days, min_weight=0.1)
    role_weights = _role_minutes_weights(frame, minute_column=minute_column)
    weights = recency * role_weights
    if apply_minutes_scale:
        minutes = _resolve_minutes_context(frame, minute_column=minute_column).clip(lower=RATE_MINUTES_FLOOR, upper=RATE_MINUTES_CAP)
        weights = weights * np.clip((minutes / 24.0).to_numpy(dtype=float), 0.2, 1.75)
    return np.clip(weights, 0.03, 3.0)


def _role_bucket_series(starter_values: pd.Series, minutes_values: pd.Series) -> pd.Series:
    starter = pd.to_numeric(starter_values, errors="coerce").fillna(0.0).ge(0.5)
    minutes = pd.to_numeric(minutes_values, errors="coerce").fillna(0.0)
    role = pd.Series("bench_low_minutes", index=minutes.index, dtype="object")
    role.loc[minutes.ge(16.0)] = "rotation"
    role.loc[starter | minutes.ge(28.0)] = "starter_core"
    return role


def _normalize_context_frame(frame: pd.DataFrame) -> pd.DataFrame:
    if frame.empty:
        return frame

    working = frame.copy()
    required_columns = {"player_name", "game_date"}
    if not required_columns.issubset(set(working.columns)):
        return pd.DataFrame()

    working["game_date"] = pd.to_datetime(working["game_date"], errors="coerce").dt.strftime("%Y-%m-%d")
    working = working.dropna(subset=["game_date"])
    if working.empty:
        return pd.DataFrame()

    if "team" not in working.columns:
        working["team"] = ""
    working["team"] = working["team"].map(normalize_team_code).fillna("")
    for column in ["injury_status", "health_status", "suspension_status", "family_context", "position"]:
        if column in working.columns:
            values = working[column].fillna("").astype(str).str.strip()
            working[column] = values.where(values.ne(""), pd.NA).str.slice(0, 80)
    for column, (min_value, max_value) in CONTEXT_NUMERIC_BOUNDS.items():
        if column not in working.columns:
            continue
        numeric = pd.to_numeric(working[column], errors="coerce")
        numeric = numeric.replace([np.inf, -np.inf], np.nan)
        numeric = numeric.where(numeric.between(min_value, max_value), np.nan)
        working[column] = numeric
    working = add_player_keys(working)
    if "team_key" not in working.columns:
        working["team_key"] = ""

    keep_columns = CONTEXT_KEY_COLUMNS + [column for column in PREDICTION_CONTEXT_COLUMNS if column in working.columns]
    normalized = working[keep_columns].copy()
    normalized = normalized.sort_values(CONTEXT_KEY_COLUMNS).drop_duplicates(subset=CONTEXT_KEY_COLUMNS, keep="last")
    return normalized


def _merge_prediction_context(base_frame: pd.DataFrame) -> pd.DataFrame:
    if base_frame.empty:
        return base_frame

    working = base_frame.copy()
    if "team" not in working.columns:
        working["team"] = ""
    working = add_player_keys(working)
    if "team_key" not in working.columns:
        working["team_key"] = ""
    working["team_key"] = working["team_key"].fillna("")
    working["game_date"] = pd.to_datetime(working["game_date"], errors="coerce").dt.strftime("%Y-%m-%d")

    context_paths = [DEFAULT_CONTEXT_UPDATES_PATH, DEFAULT_PROVIDER_CONTEXT_PATH]
    for path in context_paths:
        if not path.exists():
            continue
        try:
            context_raw = pd.read_csv(path)
        except Exception:
            continue
        context = _normalize_context_frame(context_raw)
        if context.empty:
            continue

        update_columns = [column for column in PREDICTION_CONTEXT_COLUMNS if column in context.columns]
        if not update_columns:
            continue

        strict_context = context.copy()
        merged = working.merge(
            strict_context[CONTEXT_KEY_COLUMNS + update_columns],
            on=CONTEXT_KEY_COLUMNS,
            how="left",
            suffixes=("", "__ctx"),
        )
        for column in update_columns:
            context_column = f"{column}__ctx"
            if context_column in merged.columns:
                if column in merged.columns:
                    base = merged[column]
                else:
                    base = pd.Series(pd.NA, index=merged.index)
                merged[column] = base.where(base.notna(), merged[context_column])
                merged = merged.drop(columns=[context_column], errors="ignore")
        working = merged

        fallback_context = context[context["team_key"] == ""]
        if fallback_context.empty:
            continue
        fallback_keys = ["player_key", "game_date"]
        fallback = working.merge(
            fallback_context[fallback_keys + update_columns].drop_duplicates(subset=fallback_keys, keep="last"),
            on=fallback_keys,
            how="left",
            suffixes=("", "__ctxfb"),
        )
        for column in update_columns:
            context_column = f"{column}__ctxfb"
            if context_column in fallback.columns:
                if column in fallback.columns:
                    base = fallback[column]
                else:
                    base = pd.Series(pd.NA, index=fallback.index)
                fallback[column] = base.where(base.notna(), fallback[context_column])
                fallback = fallback.drop(columns=[context_column], errors="ignore")
        working = fallback

    working["game_date"] = pd.to_datetime(working["game_date"], errors="coerce").dt.tz_localize(None)
    return working


def _fit_residual_calibration(frame: pd.DataFrame, residual: pd.Series) -> dict[str, object]:
    calibration: dict[str, object] = {
        "global_bias": 0.0,
        "starter_bias": {},
        "home_bias": {},
        "opponent_bias": {},
        "team_bias": {},
        "minute_bin_bias": {},
    }

    residual_series = pd.to_numeric(residual, errors="coerce")
    residual_series = residual_series.replace([np.inf, -np.inf], np.nan)
    if residual_series.dropna().empty:
        return calibration

    calibration["global_bias"] = float(np.clip(residual_series.mean(), -4.0, 4.0))

    def _group_bias(
        key_values: pd.Series,
        *,
        min_rows: int,
        clip_abs: float,
        shrinkage: float,
    ) -> dict[str, float]:
        grouped = pd.DataFrame({"key": key_values.astype(str), "residual": residual_series}).dropna(subset=["residual"])
        if grouped.empty:
            return {}
        stats = grouped.groupby("key")["residual"].agg(["mean", "count"]).reset_index()
        stats = stats[stats["count"] >= min_rows]
        if stats.empty:
            return {}
        shrink = stats["count"] / (stats["count"] + shrinkage)
        adjusted = (stats["mean"] * shrink).clip(lower=-clip_abs, upper=clip_abs)
        return {str(key): float(value) for key, value in zip(stats["key"], adjusted)}

    if "starter" in frame.columns:
        starter_values = pd.to_numeric(frame["starter"], errors="coerce").fillna(0).round().clip(lower=0, upper=1).astype(int)
        calibration["starter_bias"] = _group_bias(starter_values, min_rows=30, clip_abs=2.5, shrinkage=50.0)

    if "home" in frame.columns:
        home_values = pd.to_numeric(frame["home"], errors="coerce").fillna(0).round().clip(lower=0, upper=1).astype(int)
        calibration["home_bias"] = _group_bias(home_values, min_rows=30, clip_abs=2.0, shrinkage=50.0)

    if "opponent" in frame.columns:
        opponent_values = frame["opponent"].fillna("").astype(str).str.upper().str.strip()
        calibration["opponent_bias"] = _group_bias(opponent_values, min_rows=35, clip_abs=2.5, shrinkage=60.0)

    if "team" in frame.columns:
        team_values = frame["team"].fillna("").astype(str).str.upper().str.strip()
        calibration["team_bias"] = _group_bias(team_values, min_rows=35, clip_abs=2.0, shrinkage=60.0)

    minute_context = _resolve_minutes_context(frame, minute_column="minutes")
    minute_bins = pd.cut(
        minute_context,
        bins=[-0.1, 8.0, 16.0, 24.0, 32.0, 60.0],
        labels=["lt8", "m8_16", "m16_24", "m24_32", "m32_plus"],
    ).astype("object")
    minute_labels = minute_bins.where(minute_bins.notna(), "unknown").astype(str)
    calibration["minute_bin_bias"] = _group_bias(minute_labels, min_rows=35, clip_abs=2.0, shrinkage=55.0)

    return calibration


def _apply_residual_calibration(
    predictions: pd.Series,
    frame: pd.DataFrame,
    calibration: dict[str, object] | None,
) -> pd.Series:
    if calibration is None:
        return pd.to_numeric(predictions, errors="coerce")

    adjusted = pd.to_numeric(predictions, errors="coerce").copy()
    adjusted = adjusted + float(calibration.get("global_bias", 0.0))

    if "starter" in frame.columns and isinstance(calibration.get("starter_bias"), dict):
        starter_values = pd.to_numeric(frame["starter"], errors="coerce").fillna(0).round().clip(lower=0, upper=1).astype(int).astype(str)
        adjusted = adjusted + starter_values.map(calibration["starter_bias"]).fillna(0.0)

    if "home" in frame.columns and isinstance(calibration.get("home_bias"), dict):
        home_values = pd.to_numeric(frame["home"], errors="coerce").fillna(0).round().clip(lower=0, upper=1).astype(int).astype(str)
        adjusted = adjusted + home_values.map(calibration["home_bias"]).fillna(0.0)

    if "opponent" in frame.columns and isinstance(calibration.get("opponent_bias"), dict):
        opponent_values = frame["opponent"].fillna("").astype(str).str.upper().str.strip()
        adjusted = adjusted + opponent_values.map(calibration["opponent_bias"]).fillna(0.0)

    if "team" in frame.columns and isinstance(calibration.get("team_bias"), dict):
        team_values = frame["team"].fillna("").astype(str).str.upper().str.strip()
        adjusted = adjusted + team_values.map(calibration["team_bias"]).fillna(0.0)

    if isinstance(calibration.get("minute_bin_bias"), dict):
        minute_context = _resolve_minutes_context(frame, minute_column="minutes")
        minute_bins = pd.cut(
            minute_context,
            bins=[-0.1, 8.0, 16.0, 24.0, 32.0, 60.0],
            labels=["lt8", "m8_16", "m16_24", "m24_32", "m32_plus"],
        ).astype("object")
        minute_labels = minute_bins.where(minute_bins.notna(), "unknown").astype(str)
        adjusted = adjusted + minute_labels.map(calibration["minute_bin_bias"]).fillna(0.0)

    return adjusted


def train_engine(
    data_path: Path | None = None,
    bundle_path: Path = DEFAULT_BUNDLE_PATH,
    metrics_path: Path = DEFAULT_METRICS_PATH,
    lookback_days: int | None = None,
) -> dict:
    data_path = data_path or resolve_training_data_path()
    raw_frame = load_dataset(data_path)
    if lookback_days is None:
        lookback_days = TRAINING_LOOKBACK_DEFAULT_DAYS

    lookback_days = int(lookback_days)
    if lookback_days <= 0:
        raise ValueError("lookback_days must be a positive integer when provided.")
    max_date = pd.to_datetime(raw_frame["game_date"], errors="coerce").max()
    if pd.isna(max_date):
        raise ValueError("Training data has no valid game_date values for lookback filtering.")
    cutoff_date = (max_date - pd.Timedelta(days=lookback_days)).normalize()
    raw_frame = raw_frame[pd.to_datetime(raw_frame["game_date"], errors="coerce") >= cutoff_date].copy()
    if raw_frame.empty:
        raise ValueError(f"No training rows were found in the last {lookback_days} days.")
    raw_frame = raw_frame.reset_index(drop=True)
    raw_frame, quality_summary = _filter_modeling_history_rows(raw_frame)
    if raw_frame.empty:
        raise ValueError("No valid NBA training rows remained after quality filtering.")

    feature_frame = build_feature_frame(raw_frame)
    if MINUTES_TARGET in raw_frame.columns and len(raw_frame) == len(feature_frame):
        feature_frame[MINUTES_TARGET] = pd.to_numeric(raw_frame[MINUTES_TARGET], errors="coerce").to_numpy()
    categorical_columns, numeric_columns = discover_feature_columns(feature_frame)

    available_targets = [
        target
        for target in TRAINABLE_MODEL_TARGETS
        if target in feature_frame.columns and feature_frame[target].notna().sum() >= 5
    ]
    unavailable_targets = {
        target: "not enough non-null historical rows"
        for target in TRAINABLE_MODEL_TARGETS
        if target not in available_targets
    }
    if not set(PRIMARY_TARGETS).issubset(available_targets):
        missing = [target for target in PRIMARY_TARGETS if target not in available_targets]
        raise ValueError(
            "The engine needs historical points, rebounds, and assists to train the core models. "
            f"Missing or insufficient targets: {missing}"
        )

    bundle_path.parent.mkdir(parents=True, exist_ok=True)
    metrics_path.parent.mkdir(parents=True, exist_ok=True)

    bundle = {
        "data_path": str(data_path),
        "training_lookback_days": lookback_days,
        "models": {},
        "trained_targets": [],
        "schema": SCHEMA_GUIDE,
        "feature_spec": {},
        "target_strategy": {},
        "residual_calibration": {},
        "role_models": {},
        "role_model_rows": {},
        "role_model_blend": {},
        "role_feature_spec": {},
        "error_distribution": {},
        "modeling_version": "2.1",
        "unavailable_targets": unavailable_targets,
    }
    metrics: dict[str, dict | list | str | bool] = {
        "training_data_path": str(data_path),
        "training_lookback_days": lookback_days,
        "training_rows": int(len(raw_frame)),
        "training_quality": quality_summary,
        "training_date_min": pd.to_datetime(raw_frame["game_date"], errors="coerce").min().strftime("%Y-%m-%d"),
        "training_date_max": pd.to_datetime(raw_frame["game_date"], errors="coerce").max().strftime("%Y-%m-%d"),
        "trained_targets": [],
        "per_target_metrics": {},
        "fantasy_projection_ready": False,
        "notes": [],
    }

    def _feature_spec_for(frame: pd.DataFrame, *, drop_columns: set[str] | None = None) -> tuple[list[str], list[str]]:
        excluded = set(drop_columns or set())
        categorical = [
            column
            for column in categorical_columns
            if column in frame.columns and column not in excluded and not frame[column].isna().all()
        ]
        numeric = [
            column
            for column in numeric_columns
            if column in frame.columns and column not in excluded and not pd.to_numeric(frame[column], errors="coerce").isna().all()
        ]
        return categorical, numeric

    def _specialize_target_features(
        target: str,
        categorical: list[str],
        numeric: list[str],
    ) -> tuple[list[str], list[str]]:
        if target not in {"points", "rebounds", "assists"}:
            return categorical, numeric

        keyword_map = {
            "points": {
                "points", "pts", "usage", "shot", "three", "free", "line_points", "line_pra",
                "implied", "game_total", "spread", "pace", "minutes", "starter", "teammate", "news",
            },
            "rebounds": {
                "reb", "oreb", "dreb", "line_rebounds", "line_pra", "minutes", "starter", "height",
                "miss_pressure", "rebound", "shot_style", "opponent_avg_height", "teammate", "news",
            },
            "assists": {
                "assist", "ast", "line_assists", "line_pra", "usage", "drive", "touch",
                "playstyle_assist", "minutes", "starter", "teammate", "news", "spread", "game_total",
            },
        }
        always_keep_numeric = {
            "games_played_before",
            "season_priors_available",
            "expected_minutes",
            "expected_minutes_confidence",
            "starter_probability",
            "starter_certainty",
            "lineup_status_confidence",
            "pregame_lock_confidence",
        }
        always_keep_categorical = {
            "team",
            "opponent",
            "position",
            "lineup_status_label",
            "pregame_lock_tier",
            "shot_style_arc_label",
            "shot_style_release_label",
            "playstyle_primary_role",
            "playstyle_scoring_mode",
        }

        keywords = keyword_map[target]
        specialized_numeric = [
            column
            for column in numeric
            if column in always_keep_numeric or any(keyword in column for keyword in keywords)
        ]
        min_required = max(12, int(len(numeric) * 0.28))
        if len(specialized_numeric) < min_required:
            specialized_numeric = numeric

        specialized_categorical = [
            column
            for column in categorical
            if column in always_keep_categorical or any(keyword in column for keyword in keywords)
        ]
        if not specialized_categorical:
            specialized_categorical = categorical

        return specialized_categorical, specialized_numeric

    minutes_validation_model: Pipeline | None = None
    minutes_feature_spec: dict[str, list[str]] | None = None

    if MINUTES_TARGET in available_targets:
        minutes_frame = feature_frame[feature_frame[MINUTES_TARGET].notna()].copy()
        train_frame, test_frame = split_time_series(minutes_frame, MINUTES_TARGET)
        target_categorical, target_numeric = _feature_spec_for(train_frame, drop_columns={MINUTES_TARGET})
        target_features = target_categorical + target_numeric

        validation_model = build_model(target_categorical, target_numeric)
        validation_model.fit(
            train_frame[target_features],
            pd.to_numeric(train_frame[MINUTES_TARGET], errors="coerce"),
            regressor__sample_weight=_training_sample_weights(
                train_frame,
                half_life_days=45.0,
                minute_column=MINUTES_TARGET,
            ),
        )

        target_metrics: dict[str, float | int | str | None] = {
            "strategy": DIRECT_STRATEGY,
            "train_rows": int(len(train_frame)),
            "deployment_rows": int(len(minutes_frame)),
            "test_rows": 0,
            "mae": None,
            "rmse": None,
            "mape": None,
        }
        if test_frame is not None and not test_frame.empty:
            predictions = validation_model.predict(test_frame[target_features])
            target_metrics["test_rows"] = int(len(test_frame))
            actual = pd.to_numeric(test_frame[MINUTES_TARGET], errors="coerce")
            target_metrics["mae"] = float(mean_absolute_error(actual, predictions))
            target_metrics["rmse"] = float(root_mean_squared_error(actual, predictions))
            valid_mask = actual.fillna(0).ne(0)
            if valid_mask.any():
                target_metrics["mape"] = float(
                    mean_absolute_percentage_error(
                        actual.loc[valid_mask],
                        pd.Series(predictions, index=test_frame.index).loc[valid_mask],
                    )
                )
            bundle["error_distribution"][MINUTES_TARGET] = _fit_error_distribution(
                actual,
                pd.Series(predictions, index=test_frame.index),
            )
        else:
            baseline_actual = pd.to_numeric(train_frame[MINUTES_TARGET], errors="coerce")
            baseline_pred = pd.Series(
                validation_model.predict(train_frame[target_features]),
                index=train_frame.index,
            )
            bundle["error_distribution"][MINUTES_TARGET] = _fit_error_distribution(
                baseline_actual,
                baseline_pred,
            )

        final_target_numeric = [
            column
            for column in numeric_columns
            if column in minutes_frame.columns
            and column != MINUTES_TARGET
            and not pd.to_numeric(minutes_frame[column], errors="coerce").isna().all()
        ]
        final_target_features = target_categorical + final_target_numeric
        final_model = build_model(target_categorical, final_target_numeric)
        final_model.fit(
            minutes_frame[final_target_features],
            pd.to_numeric(minutes_frame[MINUTES_TARGET], errors="coerce"),
            regressor__sample_weight=_training_sample_weights(
                minutes_frame,
                half_life_days=30.0,
                minute_column=MINUTES_TARGET,
            ),
        )

        bundle["models"][MINUTES_TARGET] = final_model
        bundle["feature_spec"][MINUTES_TARGET] = {
            "categorical": target_categorical,
            "numeric": final_target_numeric,
        }
        bundle["target_strategy"][MINUTES_TARGET] = DIRECT_STRATEGY
        bundle["residual_calibration"][MINUTES_TARGET] = _fit_residual_calibration(
            test_frame if test_frame is not None else train_frame,
            (
                pd.to_numeric((test_frame if test_frame is not None else train_frame)[MINUTES_TARGET], errors="coerce")
                - pd.Series(
                    validation_model.predict((test_frame if test_frame is not None else train_frame)[target_features]),
                    index=(test_frame if test_frame is not None else train_frame).index,
                )
            ),
        )
        bundle["trained_targets"].append(MINUTES_TARGET)
        metrics["trained_targets"].append(MINUTES_TARGET)
        metrics["per_target_metrics"][MINUTES_TARGET] = target_metrics

        minutes_validation_model = validation_model
        minutes_feature_spec = {
            "categorical": target_categorical,
            "numeric": target_numeric,
        }
    else:
        metrics["notes"].append("Minutes model unavailable due to insufficient historical minutes rows.")

    for target in [candidate for candidate in STAT_MODEL_TARGETS if candidate in available_targets]:
        target_frame = feature_frame[feature_frame[target].notna()].copy()
        strategy = DIRECT_STRATEGY

        if (
            MINUTES_TARGET in target_frame.columns
            and pd.to_numeric(target_frame[MINUTES_TARGET], errors="coerce").notna().sum() >= 5
            and MINUTES_TARGET in bundle["trained_targets"]
        ):
            candidate = target_frame[
                pd.to_numeric(target_frame[MINUTES_TARGET], errors="coerce").fillna(0).gt(0)
            ].copy()
            if len(candidate) >= 5:
                target_frame = candidate
                strategy = RATE_STRATEGY

        train_frame, test_frame = split_time_series(target_frame, target)
        target_categorical, target_numeric = _feature_spec_for(train_frame, drop_columns={target, MINUTES_TARGET})
        target_categorical, target_numeric = _specialize_target_features(target, target_categorical, target_numeric)
        target_features = target_categorical + target_numeric

        validation_model = build_model(target_categorical, target_numeric)

        if strategy == RATE_STRATEGY:
            train_minutes = pd.to_numeric(train_frame[MINUTES_TARGET], errors="coerce").clip(
                lower=RATE_MINUTES_FLOOR,
                upper=RATE_MINUTES_CAP,
            )
            train_rates = pd.to_numeric(train_frame[target], errors="coerce") / train_minutes
            train_weights = _training_sample_weights(
                train_frame,
                half_life_days=45.0,
                minute_column=MINUTES_TARGET,
                apply_minutes_scale=True,
            )
            validation_model.fit(train_frame[target_features], train_rates, regressor__sample_weight=train_weights)
        else:
            validation_model.fit(
                train_frame[target_features],
                pd.to_numeric(train_frame[target], errors="coerce"),
                regressor__sample_weight=_training_sample_weights(
                    train_frame,
                    half_life_days=45.0,
                    minute_column=MINUTES_TARGET,
                ),
            )

        target_metrics: dict[str, float | int | str | None] = {
            "strategy": strategy,
            "train_rows": int(len(train_frame)),
            "deployment_rows": int(len(target_frame)),
            "test_rows": 0,
            "mae": None,
            "rmse": None,
            "mape": None,
        }
        if strategy == RATE_STRATEGY:
            target_metrics["rate_minutes_floor"] = RATE_MINUTES_FLOOR

        validation_predictions = None
        validation_frame = test_frame if test_frame is not None and not test_frame.empty else None
        if validation_frame is not None:
            if strategy == RATE_STRATEGY:
                predicted_rate = validation_model.predict(validation_frame[target_features])
                if minutes_validation_model is not None and minutes_feature_spec is not None:
                    minute_features = _prepare_feature_matrix(validation_frame, minutes_feature_spec)
                    predicted_minutes = pd.Series(
                        minutes_validation_model.predict(minute_features),
                        index=validation_frame.index,
                    )
                else:
                    predicted_minutes = pd.to_numeric(validation_frame[MINUTES_TARGET], errors="coerce")
                predicted_minutes = predicted_minutes.clip(lower=0, upper=RATE_MINUTES_CAP)
                validation_predictions = pd.Series(predicted_rate, index=validation_frame.index) * predicted_minutes
            else:
                validation_predictions = pd.Series(
                    validation_model.predict(validation_frame[target_features]),
                    index=validation_frame.index,
                )

            actual = pd.to_numeric(validation_frame[target], errors="coerce")
            target_metrics["test_rows"] = int(len(validation_frame))
            target_metrics["mae"] = float(mean_absolute_error(actual, validation_predictions))
            target_metrics["rmse"] = float(root_mean_squared_error(actual, validation_predictions))
            valid_mask = actual.fillna(0).ne(0)
            if valid_mask.any():
                target_metrics["mape"] = float(
                    mean_absolute_percentage_error(
                        actual.loc[valid_mask],
                        validation_predictions.loc[valid_mask],
                    )
                )
            bundle["error_distribution"][target] = _fit_error_distribution(actual, validation_predictions)
        else:
            if strategy == RATE_STRATEGY:
                train_predicted_rate = pd.Series(validation_model.predict(train_frame[target_features]), index=train_frame.index)
                train_minutes = pd.to_numeric(train_frame[MINUTES_TARGET], errors="coerce").clip(
                    lower=RATE_MINUTES_FLOOR,
                    upper=RATE_MINUTES_CAP,
                )
                train_predicted_total = train_predicted_rate * train_minutes
                bundle["error_distribution"][target] = _fit_error_distribution(
                    pd.to_numeric(train_frame[target], errors="coerce"),
                    train_predicted_total,
                )
            else:
                baseline_pred = pd.Series(
                    validation_model.predict(train_frame[target_features]),
                    index=train_frame.index,
                )
                bundle["error_distribution"][target] = _fit_error_distribution(
                    pd.to_numeric(train_frame[target], errors="coerce"),
                    baseline_pred,
                )

        final_target_numeric = [
            column
            for column in target_numeric
            if column in target_frame.columns
            and column != MINUTES_TARGET
            and not pd.to_numeric(target_frame[column], errors="coerce").isna().all()
        ]
        if not final_target_numeric:
            final_target_numeric = [
                column
                for column in numeric_columns
                if column in target_frame.columns
                and column != MINUTES_TARGET
                and not pd.to_numeric(target_frame[column], errors="coerce").isna().all()
            ]
        final_target_features = target_categorical + final_target_numeric
        final_model = build_model(target_categorical, final_target_numeric)

        if strategy == RATE_STRATEGY:
            final_minutes = pd.to_numeric(target_frame[MINUTES_TARGET], errors="coerce").clip(
                lower=RATE_MINUTES_FLOOR,
                upper=RATE_MINUTES_CAP,
            )
            final_rates = pd.to_numeric(target_frame[target], errors="coerce") / final_minutes
            final_weights = _training_sample_weights(
                target_frame,
                half_life_days=30.0,
                minute_column=MINUTES_TARGET,
                apply_minutes_scale=True,
            )
            final_model.fit(target_frame[final_target_features], final_rates, regressor__sample_weight=final_weights)
        else:
            final_model.fit(
                target_frame[final_target_features],
                pd.to_numeric(target_frame[target], errors="coerce"),
                regressor__sample_weight=_training_sample_weights(
                    target_frame,
                    half_life_days=30.0,
                    minute_column=MINUTES_TARGET,
                ),
            )

        role_models_for_target: dict[str, Pipeline] = {}
        role_model_rows_for_target: dict[str, int] = {}
        role_model_blend_for_target: dict[str, float] = {}
        role_feature_specs_for_target: dict[str, dict[str, list[str]]] = {}
        role_minutes = (
            pd.to_numeric(target_frame[MINUTES_TARGET], errors="coerce")
            if MINUTES_TARGET in target_frame.columns
            else _resolve_minutes_context(target_frame)
        )
        role_starter = (
            pd.to_numeric(target_frame["starter"], errors="coerce")
            if "starter" in target_frame.columns
            else pd.Series(0.0, index=target_frame.index, dtype=float)
        )
        role_buckets = _role_bucket_series(role_starter, role_minutes)
        for role_name in ROLE_BUCKET_ORDER:
            role_mask = role_buckets.eq(role_name)
            if not role_mask.any():
                continue
            role_frame = target_frame.loc[role_mask].copy()
            role_rows = int(len(role_frame))
            if role_rows < MIN_ROLE_MODEL_ROWS:
                continue

            role_categorical, role_numeric = _feature_spec_for(role_frame, drop_columns={target, MINUTES_TARGET})
            role_categorical, role_numeric = _specialize_target_features(target, role_categorical, role_numeric)
            if not role_categorical and not role_numeric:
                role_categorical = list(target_categorical)
                role_numeric = list(final_target_numeric)
            role_features = role_categorical + role_numeric
            role_model = build_model(role_categorical, role_numeric)
            if strategy == RATE_STRATEGY:
                role_minutes_series = pd.to_numeric(role_frame[MINUTES_TARGET], errors="coerce").clip(
                    lower=RATE_MINUTES_FLOOR,
                    upper=RATE_MINUTES_CAP,
                )
                role_rates = pd.to_numeric(role_frame[target], errors="coerce") / role_minutes_series
                role_weights = _training_sample_weights(
                    role_frame,
                    half_life_days=28.0,
                    minute_column=MINUTES_TARGET,
                    apply_minutes_scale=True,
                )
                role_model.fit(role_frame[role_features], role_rates, regressor__sample_weight=role_weights)
            else:
                role_model.fit(
                    role_frame[role_features],
                    pd.to_numeric(role_frame[target], errors="coerce"),
                    regressor__sample_weight=_training_sample_weights(
                        role_frame,
                        half_life_days=28.0,
                        minute_column=MINUTES_TARGET,
                    ),
                )

            role_models_for_target[role_name] = role_model
            role_model_rows_for_target[role_name] = role_rows
            role_model_blend_for_target[role_name] = float(np.clip(role_rows / (role_rows + 350.0), 0.2, 0.8))
            role_feature_specs_for_target[role_name] = {
                "categorical": role_categorical,
                "numeric": role_numeric,
            }

        bundle["models"][target] = final_model
        bundle["feature_spec"][target] = {
            "categorical": target_categorical,
            "numeric": final_target_numeric,
        }
        bundle["target_strategy"][target] = strategy
        bundle["residual_calibration"][target] = _fit_residual_calibration(
            validation_frame if validation_frame is not None else train_frame,
            (
                pd.to_numeric((validation_frame if validation_frame is not None else train_frame)[target], errors="coerce")
                - (
                    validation_predictions
                    if validation_predictions is not None
                    else pd.Series(
                        validation_model.predict((validation_frame if validation_frame is not None else train_frame)[target_features]),
                        index=(validation_frame if validation_frame is not None else train_frame).index,
                    )
                )
            ),
        )
        if role_models_for_target:
            bundle["role_models"][target] = role_models_for_target
            bundle["role_model_rows"][target] = role_model_rows_for_target
            bundle["role_model_blend"][target] = role_model_blend_for_target
            bundle["role_feature_spec"][target] = role_feature_specs_for_target
        bundle["trained_targets"].append(target)
        metrics["trained_targets"].append(target)
        target_metrics["specialized_feature_count"] = int(len(target_categorical) + len(final_target_numeric))
        target_metrics["role_models"] = role_model_rows_for_target
        metrics["per_target_metrics"][target] = target_metrics

    fantasy_ready = set(SUPPORT_TARGETS).issubset(bundle["trained_targets"])
    metrics["fantasy_projection_ready"] = fantasy_ready
    if not fantasy_ready:
        metrics["notes"].append(
            "DraftKings and FanDuel projections need historical steals, blocks, turnovers, and three_points_made."
        )

    metrics_path.write_text(json.dumps(metrics, indent=2), encoding="utf-8")
    joblib.dump(bundle, bundle_path)
    return metrics


def _prepare_feature_matrix(feature_frame: pd.DataFrame, feature_spec: dict[str, list[str]]) -> pd.DataFrame:
    categorical_columns = feature_spec["categorical"]
    numeric_columns = feature_spec["numeric"]
    required_columns = categorical_columns + numeric_columns
    prepared = feature_frame.copy()
    for column in required_columns:
        if column not in prepared.columns:
            prepared[column] = pd.NA
    for column in numeric_columns:
        prepared[column] = pd.to_numeric(prepared[column], errors="coerce")
    for column in categorical_columns:
        prepared[column] = prepared[column].astype("object").where(prepared[column].notna(), None)
    prepared = prepared.where(~prepared.isna(), np.nan)
    return prepared[required_columns]


def _concat_with_union(frames: list[pd.DataFrame]) -> pd.DataFrame:
    ordered_columns: list[str] = []
    for frame in frames:
        for column in frame.columns:
            if column not in ordered_columns:
                ordered_columns.append(column)

    aligned_frames = [frame.reindex(columns=ordered_columns, fill_value=np.nan) for frame in frames]
    return pd.concat(aligned_frames, ignore_index=True, sort=False)


def _derive_display_predictions(result: pd.DataFrame) -> pd.DataFrame:
    if {"predicted_points", "predicted_rebounds", "predicted_assists"}.issubset(result.columns):
        result["predicted_pra"] = (
            result["predicted_points"] + result["predicted_rebounds"] + result["predicted_assists"]
        )

    fantasy_components = {
        "points": "predicted_points",
        "rebounds": "predicted_rebounds",
        "assists": "predicted_assists",
        "steals": "predicted_steals",
        "blocks": "predicted_blocks",
        "turnovers": "predicted_turnovers",
        "three_points_made": "predicted_three_points_made",
    }
    available = {key: value for key, value in fantasy_components.items() if value in result.columns}

    if set(["points", "rebounds", "assists", "steals", "blocks", "turnovers", "three_points_made"]).issubset(available):
        scoring_frame = pd.DataFrame({key: result[value] for key, value in available.items()})
        result["predicted_draftkings_points"] = calculate_draftkings_points(scoring_frame)

    if set(["points", "rebounds", "assists", "steals", "blocks", "turnovers"]).issubset(available):
        scoring_frame = pd.DataFrame({key: result[value] for key, value in available.items()})
        result["predicted_fanduel_points"] = calculate_fanduel_points(scoring_frame)

    return result


def _blend_anchor_sources(sources: list[tuple[pd.Series, float | pd.Series]]) -> tuple[pd.Series, pd.Series]:
    if not sources:
        empty = pd.Series(dtype=float)
        return empty, empty

    index = sources[0][0].index
    weighted_sum = pd.Series(0.0, index=index, dtype=float)
    weight_sum = pd.Series(0.0, index=index, dtype=float)
    source_count = pd.Series(0, index=index, dtype=float)

    for series, weight in sources:
        numeric = pd.to_numeric(series, errors="coerce")
        weight_series = (
            pd.to_numeric(weight, errors="coerce")
            if isinstance(weight, pd.Series)
            else pd.Series(float(weight), index=index, dtype=float)
        )
        weight_series = weight_series.reindex(index).fillna(0.0).clip(lower=0.0)
        valid = numeric.notna()
        weighted_sum.loc[valid] += numeric.loc[valid] * weight_series.loc[valid]
        weight_sum.loc[valid] += weight_series.loc[valid]
        source_count.loc[valid] += 1.0

    anchor = weighted_sum / weight_sum.replace(0.0, np.nan)
    return anchor, source_count


def _sum_series_if_all_available(frame: pd.DataFrame, columns: list[str]) -> pd.Series:
    if not set(columns).issubset(frame.columns):
        return pd.Series(np.nan, index=frame.index, dtype=float)
    numeric = [pd.to_numeric(frame[column], errors="coerce") for column in columns]
    valid = numeric[0].notna()
    for series in numeric[1:]:
        valid = valid & series.notna()
    total = pd.Series(0.0, index=frame.index, dtype=float)
    for series in numeric:
        total = total + series.fillna(0.0)
    return total.where(valid)


def _add_pregame_anchor_columns(result: pd.DataFrame, prediction_frame: pd.DataFrame) -> pd.DataFrame:
    if result.empty:
        return result

    anchored = result.copy()

    def _source_column(name: str) -> pd.Series:
        if name in prediction_frame.columns:
            return pd.to_numeric(prediction_frame[name], errors="coerce")
        return pd.Series(np.nan, index=anchored.index, dtype=float)

    def _line_context(target: str) -> tuple[pd.Series, pd.Series, pd.Series, pd.Series, pd.Series]:
        market_map = {
            "points": "line_points",
            "rebounds": "line_rebounds",
            "assists": "line_assists",
            "pra": "line_pra",
        }
        base_column = market_map[target]
        consensus_series = _source_column(f"{base_column}_consensus")
        line_close = _source_column(f"{base_column}_close").combine_first(consensus_series).combine_first(
            _source_column(base_column)
        )
        line_open = _source_column(f"{base_column}_open")
        line_movement = _source_column(f"{base_column}_movement").combine_first(line_close - line_open)
        stddev_series = _source_column(f"{base_column}_stddev").fillna(0.0).clip(lower=0.0)
        books_series = _source_column(f"{base_column}_books_count").fillna(1.0).clip(lower=0.0)
        age_series = _source_column(f"{base_column}_snapshot_age_minutes")
        return line_close, stddev_series, books_series, age_series, line_movement

    specs = [
        ("points", "pts_season", "points_avg_last_5"),
        ("rebounds", "reb_season", "rebounds_avg_last_5"),
        ("assists", "ast_season", "assists_avg_last_5"),
    ]

    for target, season_column, form_column in specs:
        prediction_column = f"predicted_{target}"
        if prediction_column not in anchored.columns:
            continue
        model_series = pd.to_numeric(anchored[prediction_column], errors="coerce")
        line_series, line_stddev, line_books, line_age, line_movement = _line_context(target)
        season_series = _source_column(season_column)
        form_series = _source_column(form_column)

        books_quality = ((line_books.clip(lower=1.0, upper=8.0) - 1.0) / 7.0).fillna(0.0)
        freshness = (1.0 - line_age.fillna(90.0).clip(lower=0.0, upper=360.0) / 360.0).clip(lower=0.0, upper=1.0)
        movement_strength = (line_movement.abs().clip(lower=0.0, upper=3.0) / 3.0).fillna(0.0)
        stability = (1.0 - line_stddev.clip(lower=0.0, upper=6.0) / 6.0).clip(lower=0.0, upper=1.0)
        line_weight = (
            0.38
            + (books_quality * 0.18)
            + (freshness * 0.14)
            + (stability * 0.12)
            + (movement_strength * 0.06)
        ).clip(lower=0.2, upper=0.72)
        remaining_weight = (1.0 - line_weight).clip(lower=0.12, upper=0.8)
        form_weight = (remaining_weight * 0.42).clip(lower=0.05, upper=0.45)
        season_weight = (remaining_weight * 0.38).clip(lower=0.05, upper=0.4)
        model_weight = (remaining_weight * 0.2).clip(lower=0.03, upper=0.25)

        anchor, source_count = _blend_anchor_sources(
            [
                (line_series, line_weight),
                (form_series, form_weight),
                (season_series, season_weight),
                (model_series, model_weight),
            ]
        )
        uncertainty_band = (
            line_stddev * 1.05
            + (1.0 - freshness) * 0.9
            + (1.0 - books_quality) * 0.7
            + movement_strength * 0.55
        ).clip(lower=0.35, upper=8.0)
        anchored[f"pregame_anchor_{target}"] = anchor.round(2)
        anchored[f"pregame_anchor_gap_{target}"] = (model_series - anchor).round(2)
        anchored[f"pregame_anchor_sources_{target}"] = source_count.fillna(0).astype(int)
        anchored[f"pregame_anchor_uncertainty_{target}"] = uncertainty_band.round(3)
        anchored[f"pregame_anchor_line_quality_{target}"] = (0.45 + books_quality * 0.3 + freshness * 0.25).clip(
            lower=0.35,
            upper=1.0,
        ).round(3)
        anchored[f"pregame_anchor_line_weight_{target}"] = line_weight.round(3)
        anchored[f"pregame_anchor_books_{target}"] = line_books.round(0)
        anchored[f"pregame_anchor_line_snapshot_age_{target}"] = line_age.round(2)
        anchored[f"pregame_anchor_line_movement_{target}"] = line_movement.round(3)

    if "predicted_pra" in anchored.columns:
        line_pra, line_pra_stddev, line_pra_books, line_pra_age, line_pra_movement = _line_context("pra")
        season_pra = _sum_series_if_all_available(prediction_frame, ["pts_season", "reb_season", "ast_season"])
        form_pra = _sum_series_if_all_available(prediction_frame, ["points_avg_last_5", "rebounds_avg_last_5", "assists_avg_last_5"])
        model_pra = pd.to_numeric(anchored["predicted_pra"], errors="coerce")
        pra_books_quality = ((line_pra_books.clip(lower=1.0, upper=8.0) - 1.0) / 7.0).fillna(0.0)
        pra_freshness = (
            1.0 - line_pra_age.fillna(90.0).clip(lower=0.0, upper=360.0) / 360.0
        ).clip(lower=0.0, upper=1.0)
        pra_movement_strength = (line_pra_movement.abs().clip(lower=0.0, upper=4.0) / 4.0).fillna(0.0)
        pra_stability = (1.0 - line_pra_stddev.clip(lower=0.0, upper=10.0) / 10.0).clip(lower=0.0, upper=1.0)
        line_pra_weight = (
            0.4
            + (pra_books_quality * 0.16)
            + (pra_freshness * 0.14)
            + (pra_stability * 0.1)
            + (pra_movement_strength * 0.06)
        ).clip(lower=0.22, upper=0.72)
        pra_remaining = (1.0 - line_pra_weight).clip(lower=0.12, upper=0.8)
        pra_anchor, pra_source_count = _blend_anchor_sources(
            [
                (line_pra, line_pra_weight),
                (form_pra, pra_remaining * 0.42),
                (season_pra, pra_remaining * 0.38),
                (model_pra, pra_remaining * 0.2),
            ]
        )
        anchored["pregame_anchor_pra"] = pra_anchor.round(2)
        anchored["pregame_anchor_gap_pra"] = (model_pra - pra_anchor).round(2)
        anchored["pregame_anchor_sources_pra"] = pra_source_count.fillna(0).astype(int)
        anchored["pregame_anchor_uncertainty_pra"] = (
            line_pra_stddev * 1.05
            + (1.0 - pra_freshness) * 1.1
            + (1.0 - pra_books_quality) * 0.8
            + pra_movement_strength * 0.65
        ).clip(lower=0.55, upper=12.0).round(3)
        anchored["pregame_anchor_line_quality_pra"] = (0.45 + pra_books_quality * 0.3 + pra_freshness * 0.25).clip(
            lower=0.35,
            upper=1.0,
        ).round(3)
        anchored["pregame_anchor_line_weight_pra"] = line_pra_weight.round(3)
        anchored["pregame_anchor_books_pra"] = line_pra_books.round(0)
        anchored["pregame_anchor_line_snapshot_age_pra"] = line_pra_age.round(2)
        anchored["pregame_anchor_line_movement_pra"] = line_pra_movement.round(3)

    source_columns = [column for column in anchored.columns if column.startswith("pregame_anchor_sources_")]
    if source_columns:
        anchored["pregame_anchor_strength"] = (
            anchored[source_columns]
            .apply(pd.to_numeric, errors="coerce")
            .fillna(0.0)
            .mean(axis=1)
            .clip(lower=0.0, upper=4.0)
            .round(2)
        )
    else:
        anchored["pregame_anchor_strength"] = 0.0

    return anchored


def _apply_anchor_projection_blend(result: pd.DataFrame, prediction_frame: pd.DataFrame) -> pd.DataFrame:
    if result.empty:
        return result

    adjusted = result.copy()
    benchmark_anchor_boost = _load_market_anchor_hardening()

    def _series_or_nan(column: str) -> pd.Series:
        if column in prediction_frame.columns:
            return pd.to_numeric(prediction_frame[column], errors="coerce")
        return pd.Series(np.nan, index=adjusted.index, dtype=float)

    games_used = _series_or_nan("games_played_before").fillna(0.0)
    starter = _series_or_nan("starter").fillna(0.0).ge(0.5)

    minute_candidates: list[pd.Series] = []
    if "predicted_minutes" in adjusted.columns:
        minute_candidates.append(pd.to_numeric(adjusted["predicted_minutes"], errors="coerce"))
    if "expected_minutes" in adjusted.columns:
        minute_candidates.append(pd.to_numeric(adjusted["expected_minutes"], errors="coerce"))
    if "min_season" in prediction_frame.columns:
        minute_candidates.append(pd.to_numeric(prediction_frame["min_season"], errors="coerce"))

    if minute_candidates:
        minute_context = minute_candidates[0]
        for candidate in minute_candidates[1:]:
            minute_context = minute_context.combine_first(candidate)
        minute_context = minute_context.fillna(24.0).clip(lower=0, upper=48)
    else:
        minute_context = pd.Series(24.0, index=adjusted.index, dtype=float)

    season_minutes = pd.to_numeric(prediction_frame.get("min_season"), errors="coerce")
    minute_ratio = (minute_context / season_minutes.clip(lower=6.0)).clip(lower=0.45, upper=1.4)

    for target in ("points", "rebounds", "assists"):
        prediction_column = f"predicted_{target}"
        anchor_column = f"pregame_anchor_{target}"
        source_column = f"pregame_anchor_sources_{target}"
        line_column = {
            "points": "line_points",
            "rebounds": "line_rebounds",
            "assists": "line_assists",
        }.get(target)
        season_column = SEASON_PRIOR_TARGET_MAP.get(target)
        if prediction_column not in adjusted.columns or anchor_column not in adjusted.columns:
            continue
        target_anchor_boost = float(benchmark_anchor_boost.get(target, 0.0))

        prediction_values = pd.to_numeric(adjusted[prediction_column], errors="coerce")
        anchor_values = pd.to_numeric(adjusted[anchor_column], errors="coerce")
        source_count = pd.to_numeric(adjusted.get(source_column), errors="coerce").fillna(0.0)
        line_values = (
            pd.to_numeric(prediction_frame.get(f"{line_column}_close"), errors="coerce")
            if f"{line_column}_close" in prediction_frame.columns
            else pd.Series(np.nan, index=adjusted.index, dtype=float)
        )
        line_values = line_values.combine_first(
            pd.to_numeric(prediction_frame.get(f"{line_column}_consensus"), errors="coerce")
            if f"{line_column}_consensus" in prediction_frame.columns
            else pd.Series(np.nan, index=adjusted.index, dtype=float)
        )
        if line_column in prediction_frame.columns:
            line_values = line_values.combine_first(pd.to_numeric(prediction_frame.get(line_column), errors="coerce"))
        line_open = (
            pd.to_numeric(prediction_frame.get(f"{line_column}_open"), errors="coerce")
            if f"{line_column}_open" in prediction_frame.columns
            else pd.Series(np.nan, index=adjusted.index, dtype=float)
        )
        line_movement = (
            pd.to_numeric(prediction_frame.get(f"{line_column}_movement"), errors="coerce")
            if f"{line_column}_movement" in prediction_frame.columns
            else pd.Series(np.nan, index=adjusted.index, dtype=float)
        )
        line_movement = line_movement.combine_first(line_values - line_open)
        line_present = line_values.notna()
        line_stddev = (
            pd.to_numeric(prediction_frame.get(f"{line_column}_stddev"), errors="coerce")
            if f"{line_column}_stddev" in prediction_frame.columns
            else pd.Series(np.nan, index=adjusted.index, dtype=float)
        )
        line_stddev = line_stddev.fillna(0.0)
        line_books = (
            pd.to_numeric(prediction_frame.get(f"{line_column}_books_count"), errors="coerce")
            if f"{line_column}_books_count" in prediction_frame.columns
            else pd.Series(np.nan, index=adjusted.index, dtype=float)
        )
        line_books = line_books.fillna(1.0)
        line_age = (
            pd.to_numeric(prediction_frame.get(f"{line_column}_snapshot_age_minutes"), errors="coerce")
            if f"{line_column}_snapshot_age_minutes" in prediction_frame.columns
            else pd.Series(np.nan, index=adjusted.index, dtype=float)
        )

        base_blend = np.where(games_used >= 20, 0.12, np.where(games_used >= 10, 0.2, np.where(games_used >= 5, 0.3, 0.42)))
        base_blend = pd.Series(base_blend, index=adjusted.index, dtype=float)
        source_scale = np.where(source_count >= 3, 1.0, np.where(source_count >= 2, 0.8, np.where(source_count >= 1, 0.6, 0.0)))
        base_blend = base_blend * pd.Series(source_scale, index=adjusted.index, dtype=float)
        base_blend += np.where((~starter) & minute_context.lt(16.0), 0.08, 0.0)
        base_blend -= np.where(starter & games_used.ge(15), 0.04, 0.0)
        expected_conf = _series_or_nan("expected_minutes_confidence").fillna(0.0).clip(lower=0.0, upper=1.0)
        news_conf = _series_or_nan("news_confidence_score").fillna(0.0).clip(lower=0.0, upper=1.0)
        starter_prob = _series_or_nan("starter_probability").fillna(_series_or_nan("starter").fillna(0.0)).clip(lower=0.0, upper=1.0)
        injury_risk = _series_or_nan("injury_risk_score").fillna(0.0).clip(lower=0.0, upper=1.0)
        line_books_quality = ((line_books.clip(lower=1.0, upper=8.0) - 1.0) / 7.0).clip(lower=0.0, upper=1.0)
        line_freshness = (1.0 - line_age.fillna(90.0).clip(lower=0.0, upper=360.0) / 360.0).clip(lower=0.0, upper=1.0)
        movement_strength = (line_movement.abs().clip(lower=0.0, upper=3.0) / 3.0).fillna(0.0)
        line_stability = (1.0 - line_stddev.clip(lower=0.0, upper=6.0) / 6.0).clip(lower=0.0, upper=1.0)
        line_stability = (line_stability - movement_strength * 0.22).clip(lower=0.0, upper=1.0)
        line_quality = (
            line_present.astype(float)
            * (0.43 + line_books_quality * 0.27 + line_freshness * 0.19 + line_stability * 0.07 + movement_strength * 0.04)
        ).clip(lower=0.0, upper=1.0)
        movement_alignment = np.sign(line_movement.fillna(0.0)) * np.sign((line_values - prediction_values).fillna(0.0))
        movement_alignment_bonus = pd.Series(
            np.where(movement_alignment > 0, 0.05, np.where(movement_alignment < 0, -0.02, 0.0)),
            index=adjusted.index,
            dtype=float,
        )
        context_quality = (
            0.45
            + (expected_conf * 0.25)
            + (news_conf * 0.2)
            + (starter_prob * 0.1)
            + (line_quality * 0.15)
            - (injury_risk * 0.2)
        ).clip(lower=0.35, upper=1.4)
        base_blend = (
            base_blend * context_quality
            + (movement_strength * 0.04)
            + movement_alignment_bonus * movement_strength
        ).clip(lower=0.0, upper=0.58)
        if target_anchor_boost > 0:
            boost_scale = (0.6 + line_quality * 0.4) * line_present.astype(float)
            base_blend = (base_blend + target_anchor_boost * boost_scale).clip(lower=0.0, upper=0.62)

        valid_anchor = prediction_values.notna() & anchor_values.notna() & source_count.gt(0)
        valid_anchor = valid_anchor & (line_present | source_count.ge(2))
        if valid_anchor.any():
            line_scale = pd.Series(np.where(line_present, 1.0, 0.62), index=adjusted.index, dtype=float)
            blend_values = (base_blend * line_scale).clip(lower=0.0, upper=0.58).loc[valid_anchor]
            blend_values = blend_values.where(line_present.loc[valid_anchor], blend_values.clip(upper=0.26))
            adjusted.loc[valid_anchor, prediction_column] = (
                prediction_values.loc[valid_anchor] * (1.0 - blend_values)
                + anchor_values.loc[valid_anchor] * blend_values
            )

        if season_column and season_column in prediction_frame.columns:
            season_values = pd.to_numeric(prediction_frame[season_column], errors="coerce")
            season_expectation = season_values * minute_ratio
            threshold_scale = np.where(starter, 0.45, 0.3)
            severe_low = (
                season_expectation.notna()
                & prediction_values.notna()
                & games_used.lt(5.0)
                & starter
                & minute_context.ge(10.0)
                & (prediction_values < (season_expectation * pd.Series(threshold_scale, index=adjusted.index)))
            )
            if severe_low.any():
                adjusted.loc[severe_low, prediction_column] = (
                    prediction_values.loc[severe_low] * 0.55
                    + season_expectation.loc[severe_low] * 0.45
                )

        # Direct market-line calibration pass:
        # keep aggressive outliers closer to consensus when line quality is strong
        # or model uncertainty context is elevated.
        current_values = pd.to_numeric(adjusted[prediction_column], errors="coerce")
        direct_line_mask = line_present & current_values.notna()
        if direct_line_mask.any():
            market_weight = (
                0.08
                + line_quality * 0.32
                + (1.0 - context_quality.clip(lower=0.35, upper=1.4)) * 0.24
                + pd.Series(np.where(~starter, 0.12, 0.0), index=adjusted.index, dtype=float)
                + pd.Series(np.where(games_used.lt(6.0), 0.1, 0.0), index=adjusted.index, dtype=float)
                - pd.Series(np.where(starter & games_used.ge(16.0), 0.07, 0.0), index=adjusted.index, dtype=float)
            )
            if target_anchor_boost > 0:
                market_weight = market_weight + (target_anchor_boost * (0.75 + line_quality * 0.25))
            market_weight = market_weight.clip(lower=0.05, upper=0.82)
            high_quality_line = (
                line_books.ge(2.0)
                & line_freshness.ge(0.7)
                & line_stability.ge(0.55)
            )
            market_weight = market_weight.where(~high_quality_line, (market_weight + 0.08).clip(upper=0.86))
            # Avoid over-anchoring stable high-history starters.
            starter_relief_mask = starter & games_used.ge(20.0) & expected_conf.ge(0.75)
            market_weight = market_weight.where(~starter_relief_mask, (market_weight - 0.08).clip(lower=0.04))

            adjusted.loc[direct_line_mask, prediction_column] = (
                current_values.loc[direct_line_mask] * (1.0 - market_weight.loc[direct_line_mask])
                + line_values.loc[direct_line_mask] * market_weight.loc[direct_line_mask]
            )
            adjusted[f"market_line_blend_weight_{target}"] = market_weight.round(3)
        adjusted[f"benchmark_anchor_boost_{target}"] = round(target_anchor_boost, 4)

        adjusted[prediction_column] = pd.to_numeric(adjusted[prediction_column], errors="coerce").clip(lower=0)

    return adjusted


def _resolve_minutes_series(result: pd.DataFrame, prediction_frame: pd.DataFrame) -> pd.Series:
    candidates: list[pd.Series] = []
    if "predicted_minutes" in result.columns:
        candidates.append(pd.to_numeric(result["predicted_minutes"], errors="coerce"))
    for column in ["expected_minutes", "minutes_avg_last_5", "min_season"]:
        if column in prediction_frame.columns:
            candidates.append(pd.to_numeric(prediction_frame[column], errors="coerce"))

    if not candidates:
        return pd.Series(24.0, index=result.index)

    resolved = candidates[0]
    for series in candidates[1:]:
        resolved = resolved.combine_first(series)
    return resolved.fillna(24.0).clip(lower=0, upper=48)


def _apply_minutes_context_floor(
    predicted_minutes: pd.Series,
    prediction_frame: pd.DataFrame,
    games_used: pd.Series,
    injury_multiplier: pd.Series,
) -> pd.Series:
    minutes = pd.to_numeric(predicted_minutes, errors="coerce").fillna(0.0).clip(lower=0.0, upper=48.0)
    starter_probability = pd.to_numeric(
        prediction_frame.get("starter_probability", pd.Series(index=prediction_frame.index, dtype=float)),
        errors="coerce",
    )
    starter_signal = pd.to_numeric(
        prediction_frame.get("starter", pd.Series(index=prediction_frame.index, dtype=float)),
        errors="coerce",
    )
    starter_probability = starter_probability.where(starter_probability.notna(), starter_signal)
    starter_probability = starter_probability.fillna(0.0).clip(lower=0.0, upper=1.0)
    lineup_confidence = pd.to_numeric(prediction_frame.get("lineup_status_confidence"), errors="coerce").fillna(0.0)
    expected_minutes_confidence = pd.to_numeric(prediction_frame.get("expected_minutes_confidence"), errors="coerce").fillna(0.0)
    news_confidence = pd.to_numeric(prediction_frame.get("news_confidence_score"), errors="coerce").fillna(0.0)
    season_minutes = pd.to_numeric(prediction_frame.get("min_season"), errors="coerce")
    recent_minutes = pd.to_numeric(prediction_frame.get("minutes_avg_last_5"), errors="coerce")
    recent_minutes = recent_minutes.combine_first(pd.to_numeric(prediction_frame.get("minutes_avg_last_10"), errors="coerce"))

    season_component = season_minutes.fillna(0.0)
    recent_component = recent_minutes.fillna(0.0)

    role_floor = pd.Series(6.0, index=minutes.index, dtype=float)
    high_starter_mask = starter_probability.ge(0.8)
    probable_starter_mask = starter_probability.ge(0.6)
    rotation_mask = starter_probability.ge(0.45)
    role_floor.loc[rotation_mask] = np.maximum(
        14.0,
        np.maximum(season_component.loc[rotation_mask] * 0.45, recent_component.loc[rotation_mask] * 0.58),
    )
    role_floor.loc[probable_starter_mask] = np.maximum(
        18.0,
        np.maximum(season_component.loc[probable_starter_mask] * 0.55, recent_component.loc[probable_starter_mask] * 0.66),
    )
    role_floor.loc[high_starter_mask] = np.maximum(
        22.0,
        np.maximum(season_component.loc[high_starter_mask] * 0.62, recent_component.loc[high_starter_mask] * 0.72),
    )

    confidence_scale = (
        0.45
        + lineup_confidence * 0.25
        + expected_minutes_confidence * 0.2
        + news_confidence * 0.1
    ).clip(lower=0.45, upper=1.0)
    sample_scale = np.where(games_used.ge(12), 1.0, np.where(games_used.ge(5), 0.92, 0.82))
    confidence_scale = pd.Series(sample_scale, index=minutes.index, dtype=float) * confidence_scale
    effective_floor = (role_floor * confidence_scale).clip(lower=6.0, upper=40.0)

    floor_mask = (
        injury_multiplier.ge(0.72)
        & (
            starter_probability.ge(0.55)
            | (starter_probability.ge(0.45) & games_used.ge(6))
        )
    )
    minutes.loc[floor_mask] = np.maximum(minutes.loc[floor_mask], effective_floor.loc[floor_mask])
    return minutes.clip(lower=0.0, upper=48.0)


def _low_minutes_projection_scale(
    projected_minutes: pd.Series,
    is_starter: pd.Series,
    games_used: pd.Series,
) -> pd.Series:
    minutes = pd.to_numeric(projected_minutes, errors="coerce").fillna(0.0)
    starter = pd.to_numeric(is_starter, errors="coerce").fillna(0).ge(0.5)
    history = pd.to_numeric(games_used, errors="coerce").fillna(0.0)

    scale = pd.Series(1.0, index=minutes.index, dtype=float)
    bench = ~starter
    scale.loc[bench & minutes.lt(2.5)] = 0.08
    scale.loc[bench & minutes.lt(4.0)] = 0.15
    scale.loc[bench & minutes.lt(5.5)] = 0.24
    scale.loc[bench & minutes.lt(7.0)] = 0.35
    scale.loc[bench & minutes.lt(8.5)] = 0.48
    scale.loc[bench & minutes.lt(10.0)] = 0.6
    scale.loc[bench & minutes.lt(12.0)] = 0.72
    scale.loc[bench & minutes.lt(14.0)] = 0.82
    scale.loc[bench & minutes.lt(16.0)] = 0.9

    # Low-history bench rows remain volatile; apply a modest additional shrink.
    low_history_penalty = np.where(bench & history.lt(3.0) & minutes.lt(14.0), 0.9, 1.0)
    scale = scale * low_history_penalty
    return scale.clip(lower=0.0, upper=1.0)


def _apply_recent_form_clamp(
    adjusted: pd.DataFrame,
    prediction_frame: pd.DataFrame,
    games_used: pd.Series,
) -> pd.DataFrame:
    target_to_recent_column = {
        "points": "points_avg_last_5",
        "rebounds": "rebounds_avg_last_5",
        "assists": "assists_avg_last_5",
        "steals": "steals_avg_last_5",
        "blocks": "blocks_avg_last_5",
        "turnovers": "turnovers_avg_last_5",
        "three_points_made": "three_points_made_avg_last_5",
    }

    if "predicted_minutes" in adjusted.columns:
        minute_context = pd.to_numeric(adjusted["predicted_minutes"], errors="coerce")
    else:
        minute_context = pd.Series(np.nan, index=adjusted.index, dtype=float)
    baseline_minutes = _resolve_minutes_series(adjusted, prediction_frame)
    minute_context = minute_context.fillna(baseline_minutes).fillna(24.0)
    starter_base = pd.to_numeric(prediction_frame.get("starter"), errors="coerce")
    starter_prob = pd.to_numeric(prediction_frame.get("starter_probability"), errors="coerce")
    starter_context = starter_base.fillna(starter_prob).fillna(0.0).ge(0.5)

    for target, recent_column in target_to_recent_column.items():
        prediction_column = f"predicted_{target}"
        if prediction_column not in adjusted.columns or recent_column not in prediction_frame.columns:
            continue

        recent_form = pd.to_numeric(prediction_frame[recent_column], errors="coerce")
        recent_last_1_column = f"{target}_last_1"
        if recent_last_1_column in prediction_frame.columns:
            recent_form = recent_form.combine_first(pd.to_numeric(prediction_frame[recent_last_1_column], errors="coerce"))
        valid_recent = recent_form.notna() & games_used.ge(RECENT_FORM_CLAMP_GAME_THRESHOLD)
        if not valid_recent.any():
            continue

        minute_factor = (minute_context / 24.0).clip(lower=0.45, upper=1.55)
        baseline = (recent_form * minute_factor).clip(lower=0.0)
        clamp_floor = baseline * RECENT_FORM_LOWER_CLAMP_RATIO

        clamp_ceiling_multiplier = pd.Series(RECENT_FORM_UPPER_CLAMP_RATIO, index=adjusted.index, dtype=float)
        clamp_ceiling_multiplier += np.where(starter_context, 0.25, 0.0)
        clamp_ceiling_multiplier += np.where(minute_context.ge(32.0), 0.15, 0.0)
        clamp_ceiling_multiplier += np.where(minute_context.lt(RECENT_FORM_MINUTE_CONTEXT_MIN), -0.2, 0.0)
        clamp_ceiling_multiplier = clamp_ceiling_multiplier.clip(lower=1.2, upper=2.15)
        clamp_ceiling = baseline * clamp_ceiling_multiplier

        current_values = pd.to_numeric(adjusted[prediction_column], errors="coerce")
        clamp_mask = valid_recent & baseline.gt(0.5) & current_values.notna()
        if not clamp_mask.any():
            continue

        adjusted.loc[clamp_mask, prediction_column] = current_values.loc[clamp_mask].clip(
            lower=clamp_floor.loc[clamp_mask],
            upper=clamp_ceiling.loc[clamp_mask],
        )

    return adjusted


def _apply_shot_style_matchup_adjustments(
    adjusted: pd.DataFrame,
    prediction_frame: pd.DataFrame,
) -> pd.DataFrame:
    if adjusted.empty:
        return adjusted

    styled = adjusted.copy()

    def _series_or_default(column: str, default: float) -> pd.Series:
        if column in prediction_frame.columns:
            return pd.to_numeric(prediction_frame[column], errors="coerce").fillna(default)
        return pd.Series(default, index=styled.index, dtype=float)

    arc_score = _series_or_default("shot_style_arc_score", 0.5).clip(lower=0.0, upper=1.0)
    release_score = _series_or_default("shot_style_release_score", 0.5).clip(lower=0.0, upper=1.0)
    tall_penalty = _series_or_default("shot_style_tall_mismatch_penalty", 0.0).clip(lower=-2.0, upper=2.0)
    pace_bonus = _series_or_default("shot_style_pace_bonus", 0.0).clip(lower=-2.0, upper=2.0)
    rebound_env = _series_or_default("shot_style_rebound_environment", 0.0).clip(lower=-2.5, upper=2.5)
    opponent_height_adv = _series_or_default("opponent_height_advantage_inches", 0.0).clip(lower=-18.0, upper=18.0)
    playstyle_conf = _series_or_default("playstyle_context_confidence", 0.0).clip(lower=0.0, upper=1.0)
    playstyle_three_rate = _series_or_default("playstyle_three_rate", 0.33).clip(lower=0.0, upper=1.0)
    playstyle_rim_rate = _series_or_default("playstyle_rim_rate", 0.35).clip(lower=0.0, upper=1.0)
    playstyle_catch_rate = _series_or_default("playstyle_catch_shoot_rate", 0.22).clip(lower=0.0, upper=1.0)
    playstyle_pull_rate = _series_or_default("playstyle_pull_up_rate", 0.24).clip(lower=0.0, upper=1.0)
    playstyle_drive_rate = _series_or_default("playstyle_drive_rate", 0.35).clip(lower=0.0, upper=2.5)
    playstyle_assist_potential = _series_or_default("playstyle_assist_potential", 0.25).clip(lower=0.0, upper=2.0)
    playstyle_rebound_rate = _series_or_default("playstyle_rebound_chance_rate", 0.35).clip(lower=0.0, upper=2.5)
    playstyle_offball_rate = _series_or_default("playstyle_offball_activity_rate", 0.25).clip(lower=0.0, upper=2.0)
    playstyle_usage_proxy = _series_or_default("playstyle_usage_proxy", 0.75).clip(lower=0.0, upper=2.5)
    playstyle_defensive_rate = _series_or_default("playstyle_defensive_event_rate", 0.08).clip(lower=0.0, upper=0.8)

    minutes_conf = _series_or_default("expected_minutes_confidence", 0.55).clip(lower=0.2, upper=1.0)
    starter_prob = _series_or_default("starter_probability", 0.5).clip(lower=0.0, upper=1.0)
    style_weight = (
        0.4
        + (minutes_conf * 0.28)
        + (starter_prob * 0.17)
        + (playstyle_conf * 0.15)
    ).clip(lower=0.35, upper=1.0)

    def _blend_multiplier(raw_multiplier: pd.Series, floor: float, ceiling: float) -> pd.Series:
        bounded = raw_multiplier.clip(lower=floor, upper=ceiling)
        return 1.0 + ((bounded - 1.0) * style_weight)

    if "predicted_points" in styled.columns:
        points_multiplier = (
            1.0
            + (pace_bonus * 0.018)
            - (tall_penalty * 0.015)
            + ((arc_score - 0.5) * 0.01)
            + ((playstyle_usage_proxy - 0.72) * 0.03)
            + ((playstyle_drive_rate - 0.32) * 0.018)
            + ((playstyle_rim_rate - 0.34) * 0.02)
        )
        points_factor = _blend_multiplier(points_multiplier, 0.84, 1.18)
        styled["shot_style_points_factor"] = pd.to_numeric(points_factor, errors="coerce").round(4)
        styled["predicted_points"] = (
            pd.to_numeric(styled["predicted_points"], errors="coerce")
            * points_factor
        )

    if "predicted_three_points_made" in styled.columns:
        threes_multiplier = (
            1.0
            + ((arc_score - 0.5) * 0.08)
            + ((release_score - 0.5) * 0.05)
            + (pace_bonus * 0.015)
            - (tall_penalty * 0.02)
            + ((playstyle_three_rate - 0.34) * 0.14)
            + ((playstyle_catch_rate - 0.2) * 0.08)
            - ((playstyle_pull_rate - 0.24) * 0.025)
        )
        threes_factor = _blend_multiplier(threes_multiplier, 0.78, 1.3)
        styled["shot_style_three_points_factor"] = pd.to_numeric(threes_factor, errors="coerce").round(4)
        styled["predicted_three_points_made"] = (
            pd.to_numeric(styled["predicted_three_points_made"], errors="coerce")
            * threes_factor
        )

    if "predicted_rebounds" in styled.columns:
        rebounds_multiplier = (
            1.0
            + (rebound_env * 0.03)
            + (opponent_height_adv.clip(lower=0.0) * 0.0025)
            - (pace_bonus * 0.005)
            + ((playstyle_rebound_rate - 0.32) * 0.035)
            + ((playstyle_rim_rate - 0.34) * 0.012)
            + ((playstyle_offball_rate - 0.25) * 0.01)
        )
        rebounds_factor = _blend_multiplier(rebounds_multiplier, 0.84, 1.22)
        styled["shot_style_rebounds_factor"] = pd.to_numeric(rebounds_factor, errors="coerce").round(4)
        styled["predicted_rebounds"] = (
            pd.to_numeric(styled["predicted_rebounds"], errors="coerce")
            * rebounds_factor
        )

    if "predicted_assists" in styled.columns:
        assists_multiplier = (
            1.0
            + (pace_bonus * 0.01)
            + ((release_score - 0.5) * 0.012)
            - (tall_penalty * 0.006)
            + ((playstyle_assist_potential - 0.24) * 0.055)
            + ((playstyle_drive_rate - 0.32) * 0.018)
        )
        assists_factor = _blend_multiplier(assists_multiplier, 0.84, 1.24)
        styled["shot_style_assists_factor"] = pd.to_numeric(assists_factor, errors="coerce").round(4)
        styled["predicted_assists"] = (
            pd.to_numeric(styled["predicted_assists"], errors="coerce")
            * assists_factor
        )

    if "predicted_turnovers" in styled.columns:
        turnovers_multiplier = (
            1.0
            + (pace_bonus * 0.01)
            + (tall_penalty * 0.01)
            + ((playstyle_usage_proxy - 0.72) * 0.04)
            + ((playstyle_drive_rate - 0.32) * 0.015)
            - (playstyle_defensive_rate * 0.02)
        )
        turnovers_factor = _blend_multiplier(turnovers_multiplier, 0.82, 1.28)
        styled["shot_style_turnovers_factor"] = pd.to_numeric(turnovers_factor, errors="coerce").round(4)
        styled["predicted_turnovers"] = (
            pd.to_numeric(styled["predicted_turnovers"], errors="coerce")
            * turnovers_factor
        )

    for column in [col for col in styled.columns if col.startswith("predicted_")]:
        styled[column] = pd.to_numeric(styled[column], errors="coerce").clip(lower=0.0)

    return styled


def _apply_prediction_sanity_and_fallbacks(
    result: pd.DataFrame,
    prediction_frame: pd.DataFrame,
    calibration_map: dict[str, dict[str, object]] | None = None,
) -> pd.DataFrame:
    adjusted = result.copy()
    games_used = pd.to_numeric(prediction_frame.get("games_played_before"), errors="coerce").fillna(0).clip(lower=0)
    history_weight = (games_used / 5.0).clip(lower=0, upper=1)
    status_text = (
        prediction_frame.get("injury_status", pd.Series("", index=prediction_frame.index)).fillna("").astype(str)
        + " "
        + prediction_frame.get("health_status", pd.Series("", index=prediction_frame.index)).fillna("").astype(str)
        + " "
        + prediction_frame.get("suspension_status", pd.Series("", index=prediction_frame.index)).fillna("").astype(str)
    ).str.lower()
    status_multiplier = pd.Series(1.0, index=prediction_frame.index, dtype=float)
    status_multiplier.loc[status_text.str.contains(UNAVAILABLE_STATUS_PATTERN)] = 0.0
    status_multiplier.loc[status_text.str.contains(DOUBTFUL_STATUS_PATTERN)] = 0.45
    status_multiplier.loc[status_text.str.contains(QUESTIONABLE_STATUS_PATTERN)] = 0.82
    status_multiplier.loc[status_text.str.contains(PROBABLE_STATUS_PATTERN)] = 0.96
    explicit_multiplier = pd.to_numeric(prediction_frame.get("injury_minutes_multiplier"), errors="coerce")
    injury_multiplier = explicit_multiplier.combine_first(status_multiplier).fillna(1.0).clip(lower=0.0, upper=1.0)

    if "predicted_minutes" in adjusted.columns:
        baseline_minutes = _resolve_minutes_series(adjusted, prediction_frame)
        predicted_minutes = pd.to_numeric(adjusted["predicted_minutes"], errors="coerce")
        low_history_mask = games_used.lt(5) & baseline_minutes.notna()
        adjusted.loc[low_history_mask, "predicted_minutes"] = (
            predicted_minutes.loc[low_history_mask] * history_weight.loc[low_history_mask]
            + baseline_minutes.loc[low_history_mask] * (1 - history_weight.loc[low_history_mask])
        )
        adjusted["predicted_minutes"] = pd.to_numeric(adjusted["predicted_minutes"], errors="coerce").fillna(baseline_minutes)
        expected_minutes = pd.to_numeric(prediction_frame.get("expected_minutes"), errors="coerce")
        expected_minutes = expected_minutes.where(expected_minutes.between(0.0, 48.0), np.nan)
        expected_confidence = (
            pd.to_numeric(prediction_frame.get("expected_minutes_confidence"), errors="coerce")
            .fillna(0.0)
            .clip(lower=0.0, upper=1.0)
        )
        expected_error = pd.to_numeric(prediction_frame.get("minutes_projection_error_estimate"), errors="coerce")
        expected_quality = (1.0 - expected_error.fillna(0.0).clip(lower=0.0, upper=20.0) / 20.0).clip(lower=0.2, upper=1.0)
        starter_context = pd.to_numeric(prediction_frame.get("starter"), errors="coerce").fillna(0.0).ge(0.5)
        expected_blend = (0.15 + expected_confidence * 0.55) * expected_quality
        expected_blend = expected_blend * np.where(games_used.ge(12), 0.4, np.where(games_used.ge(5), 0.65, 1.0))
        expected_blend = (expected_blend + np.where(starter_context, 0.08, 0.0)).clip(lower=0.0, upper=0.75)
        expected_mask = expected_minutes.notna() & adjusted["predicted_minutes"].notna()
        if expected_mask.any():
            adjusted.loc[expected_mask, "predicted_minutes"] = (
                adjusted.loc[expected_mask, "predicted_minutes"] * (1.0 - expected_blend.loc[expected_mask])
                + expected_minutes.loc[expected_mask] * expected_blend.loc[expected_mask]
            )
        adjusted["predicted_minutes"] = (adjusted["predicted_minutes"] * injury_multiplier).clip(lower=0, upper=48)
        adjusted["predicted_minutes"] = _apply_minutes_context_floor(
            pd.to_numeric(adjusted["predicted_minutes"], errors="coerce"),
            prediction_frame,
            games_used,
            injury_multiplier,
        )

    for target, season_column in SEASON_PRIOR_TARGET_MAP.items():
        prediction_column = f"predicted_{target}"
        if prediction_column not in adjusted.columns:
            continue

        adjusted[prediction_column] = pd.to_numeric(adjusted[prediction_column], errors="coerce")
        if season_column in prediction_frame.columns:
            season_values = pd.to_numeric(prediction_frame[season_column], errors="coerce")
            fallback_mask = season_values.notna() & games_used.lt(5)
            blended = adjusted[prediction_column] * history_weight + season_values * (1 - history_weight)
            adjusted.loc[fallback_mask, prediction_column] = blended.loc[fallback_mask]

        if calibration_map and target in calibration_map:
            adjusted[prediction_column] = _apply_residual_calibration(
                adjusted[prediction_column],
                prediction_frame,
                calibration_map.get(target),
            )

        adjusted[prediction_column] = adjusted[prediction_column].clip(lower=0)

    adjusted = _apply_recent_form_clamp(adjusted, prediction_frame, games_used)
    adjusted = _apply_shot_style_matchup_adjustments(adjusted, prediction_frame)

    stat_prediction_columns = [
        f"predicted_{target}"
        for target in SEASON_PRIOR_TARGET_MAP
        if f"predicted_{target}" in adjusted.columns
    ]
    if stat_prediction_columns:
        projected_minutes = (
            pd.to_numeric(adjusted["predicted_minutes"], errors="coerce")
            if "predicted_minutes" in adjusted.columns
            else _resolve_minutes_series(adjusted, prediction_frame)
        )
        starter_values = (
            pd.to_numeric(prediction_frame["starter"], errors="coerce").fillna(0.0)
            if "starter" in prediction_frame.columns
            else pd.Series(0.0, index=adjusted.index, dtype=float)
        )
        scale = _low_minutes_projection_scale(projected_minutes, starter_values, games_used)
        for column in stat_prediction_columns:
            adjusted[column] = pd.to_numeric(adjusted[column], errors="coerce") * scale * injury_multiplier

    prediction_columns = [column for column in adjusted.columns if column.startswith("predicted_")]
    if prediction_columns:
        unavailable_mask = pd.Series(False, index=adjusted.index)
        for status_column in ["injury_status", "health_status", "suspension_status"]:
            if status_column not in prediction_frame.columns:
                continue
            status_values = prediction_frame[status_column].fillna("").astype(str)
            unavailable_mask = unavailable_mask | status_values.str.contains(UNAVAILABLE_STATUS_PATTERN)
        adjusted.loc[unavailable_mask, prediction_columns] = 0.0

    return adjusted


def _apply_prediction_intervals_and_error_estimates(
    result: pd.DataFrame,
    prediction_frame: pd.DataFrame,
    *,
    bundle: dict | None = None,
    calibration_profile: dict | None = None,
) -> pd.DataFrame:
    if result.empty:
        return result

    enriched = result.copy()

    def _series_or_nan(column: str) -> pd.Series:
        if column in prediction_frame.columns:
            return pd.to_numeric(prediction_frame[column], errors="coerce")
        return pd.Series(np.nan, index=enriched.index, dtype=float)

    games_used = _series_or_nan("games_played_before").fillna(0.0)
    starter_prob = _series_or_nan("starter_probability").fillna(0.0).clip(lower=0.0, upper=1.0)
    starter_certainty = _series_or_nan("starter_certainty").combine_first(starter_prob).fillna(0.0).clip(
        lower=0.0,
        upper=1.0,
    )
    expected_minutes_conf = _series_or_nan("expected_minutes_confidence").fillna(0.0).clip(lower=0.0, upper=1.0)
    injury_risk = _series_or_nan("injury_risk_score").fillna(0.0).clip(lower=0.0, upper=1.0)

    row_error_pct_components: list[pd.Series] = []

    interval_targets = [target for target in TRAINABLE_MODEL_TARGETS if f"predicted_{target}" in enriched.columns]
    for target in interval_targets:
        prediction_column = f"predicted_{target}"
        predicted = pd.to_numeric(enriched[prediction_column], errors="coerce")
        profile = _target_error_summary_from_profile(
            target,
            bundle=bundle,
            calibration_profile=calibration_profile,
        )

        line_quality = pd.Series(0.0, index=enriched.index, dtype=float)
        line_column_map = {
            "points": "line_points",
            "rebounds": "line_rebounds",
            "assists": "line_assists",
            "three_points_made": "line_three_points_made",
            "turnovers": "line_turnovers",
            "steals": "line_steals",
            "blocks": "line_blocks",
        }
        line_column = line_column_map.get(target)
        if line_column:
            line_values = _series_or_nan(f"{line_column}_close").combine_first(_series_or_nan(f"{line_column}_consensus"))
            line_values = line_values.combine_first(_series_or_nan(line_column))
            line_books = _series_or_nan(f"{line_column}_books_count").fillna(1.0).clip(lower=1.0, upper=8.0)
            line_age = _series_or_nan(f"{line_column}_snapshot_age_minutes")
            line_stddev = _series_or_nan(f"{line_column}_stddev").fillna(0.0).clip(lower=0.0, upper=6.0)
            books_quality = ((line_books - 1.0) / 7.0).clip(lower=0.0, upper=1.0)
            freshness = (1.0 - line_age.fillna(120.0).clip(lower=0.0, upper=360.0) / 360.0).clip(lower=0.0, upper=1.0)
            stability = (1.0 - line_stddev / 6.0).clip(lower=0.0, upper=1.0)
            line_quality = (
                line_values.notna().astype(float)
                * (0.42 + books_quality * 0.33 + freshness * 0.18 + stability * 0.07)
            ).clip(lower=0.0, upper=1.0)

        quality_scaler = (
            1.0
            - (expected_minutes_conf * 0.18)
            - (starter_prob * 0.07)
            - (starter_certainty * 0.08)
            + (injury_risk * 0.22)
            - np.clip(games_used, 0.0, 24.0) * 0.008
        ).clip(lower=0.7, upper=1.45)
        quality_scaler = (quality_scaler * (1.0 - line_quality * 0.18)).clip(lower=0.62, upper=1.45)

        q10_delta = float(profile.get("residual_q10", -profile.get("abs_error_p80", 1.0))) * quality_scaler
        q50_delta = float(profile.get("residual_q50", 0.0)) * quality_scaler
        q90_delta = float(profile.get("residual_q90", profile.get("abs_error_p80", 1.0))) * quality_scaler

        p10 = (predicted + q10_delta).clip(lower=0.0)
        p50 = (predicted + q50_delta).clip(lower=0.0)
        p90 = (predicted + q90_delta).clip(lower=0.0)
        p10 = np.minimum(p10, p50)
        p90 = np.maximum(p50, p90)

        enriched[f"{prediction_column}_p10"] = p10.round(3)
        enriched[f"{prediction_column}_p50"] = p50.round(3)
        enriched[f"{prediction_column}_p90"] = p90.round(3)

        pct_floor = float(profile.get("mean_abs_pct_error_floor", 18.0))
        denominator = predicted.abs().clip(lower=TARGET_ERROR_FLOOR_MAP.get(target, 1.0))
        abs_error_80 = float(profile.get("abs_error_p80", 0.0))
        model_pct = np.where(
            denominator.gt(0),
            (abs_error_80 / denominator) * 100.0,
            pct_floor,
        )
        blended_pct = ((pd.Series(model_pct, index=enriched.index) * 0.65) + (pct_floor * 0.35)) * quality_scaler
        row_error_pct_components.append(pd.to_numeric(blended_pct, errors="coerce").fillna(pct_floor))

    if row_error_pct_components:
        stacked = pd.concat(row_error_pct_components, axis=1)
        projection_error_pct_estimate = stacked.mean(axis=1).clip(lower=3.0, upper=65.0)
        enriched["projection_error_pct_estimate"] = projection_error_pct_estimate.round(2)
        enriched["projection_confidence_pct"] = (100.0 - projection_error_pct_estimate).clip(lower=1.0, upper=99.0).round(2)

    # Derived interval bands for key display outputs.
    if {"predicted_points_p10", "predicted_rebounds_p10", "predicted_assists_p10"}.issubset(enriched.columns):
        enriched["predicted_pra_p10"] = (
            pd.to_numeric(enriched["predicted_points_p10"], errors="coerce")
            + pd.to_numeric(enriched["predicted_rebounds_p10"], errors="coerce")
            + pd.to_numeric(enriched["predicted_assists_p10"], errors="coerce")
        ).round(3)
    if {"predicted_points_p50", "predicted_rebounds_p50", "predicted_assists_p50"}.issubset(enriched.columns):
        enriched["predicted_pra_p50"] = (
            pd.to_numeric(enriched["predicted_points_p50"], errors="coerce")
            + pd.to_numeric(enriched["predicted_rebounds_p50"], errors="coerce")
            + pd.to_numeric(enriched["predicted_assists_p50"], errors="coerce")
        ).round(3)
    if {"predicted_points_p90", "predicted_rebounds_p90", "predicted_assists_p90"}.issubset(enriched.columns):
        enriched["predicted_pra_p90"] = (
            pd.to_numeric(enriched["predicted_points_p90"], errors="coerce")
            + pd.to_numeric(enriched["predicted_rebounds_p90"], errors="coerce")
            + pd.to_numeric(enriched["predicted_assists_p90"], errors="coerce")
        ).round(3)

    if {
        "predicted_points_p10",
        "predicted_rebounds_p10",
        "predicted_assists_p10",
        "predicted_steals_p10",
        "predicted_blocks_p10",
        "predicted_turnovers_p10",
        "predicted_three_points_made_p10",
    }.issubset(enriched.columns):
        dk_p10 = pd.DataFrame(
            {
                "points": pd.to_numeric(enriched["predicted_points_p10"], errors="coerce"),
                "rebounds": pd.to_numeric(enriched["predicted_rebounds_p10"], errors="coerce"),
                "assists": pd.to_numeric(enriched["predicted_assists_p10"], errors="coerce"),
                "steals": pd.to_numeric(enriched["predicted_steals_p10"], errors="coerce"),
                "blocks": pd.to_numeric(enriched["predicted_blocks_p10"], errors="coerce"),
                "turnovers": pd.to_numeric(enriched["predicted_turnovers_p10"], errors="coerce"),
                "three_points_made": pd.to_numeric(enriched["predicted_three_points_made_p10"], errors="coerce"),
            }
        )
        enriched["predicted_draftkings_points_p10"] = calculate_draftkings_points(dk_p10).round(3)

    if {
        "predicted_points_p90",
        "predicted_rebounds_p90",
        "predicted_assists_p90",
        "predicted_steals_p90",
        "predicted_blocks_p90",
        "predicted_turnovers_p90",
        "predicted_three_points_made_p90",
    }.issubset(enriched.columns):
        dk_p90 = pd.DataFrame(
            {
                "points": pd.to_numeric(enriched["predicted_points_p90"], errors="coerce"),
                "rebounds": pd.to_numeric(enriched["predicted_rebounds_p90"], errors="coerce"),
                "assists": pd.to_numeric(enriched["predicted_assists_p90"], errors="coerce"),
                "steals": pd.to_numeric(enriched["predicted_steals_p90"], errors="coerce"),
                "blocks": pd.to_numeric(enriched["predicted_blocks_p90"], errors="coerce"),
                "turnovers": pd.to_numeric(enriched["predicted_turnovers_p90"], errors="coerce"),
                "three_points_made": pd.to_numeric(enriched["predicted_three_points_made_p90"], errors="coerce"),
            }
        )
        enriched["predicted_draftkings_points_p90"] = calculate_draftkings_points(dk_p90).round(3)

    if {
        "predicted_points_p10",
        "predicted_rebounds_p10",
        "predicted_assists_p10",
        "predicted_steals_p10",
        "predicted_blocks_p10",
        "predicted_turnovers_p10",
    }.issubset(enriched.columns):
        fd_p10 = pd.DataFrame(
            {
                "points": pd.to_numeric(enriched["predicted_points_p10"], errors="coerce"),
                "rebounds": pd.to_numeric(enriched["predicted_rebounds_p10"], errors="coerce"),
                "assists": pd.to_numeric(enriched["predicted_assists_p10"], errors="coerce"),
                "steals": pd.to_numeric(enriched["predicted_steals_p10"], errors="coerce"),
                "blocks": pd.to_numeric(enriched["predicted_blocks_p10"], errors="coerce"),
                "turnovers": pd.to_numeric(enriched["predicted_turnovers_p10"], errors="coerce"),
            }
        )
        enriched["predicted_fanduel_points_p10"] = calculate_fanduel_points(fd_p10).round(3)

    if {
        "predicted_points_p90",
        "predicted_rebounds_p90",
        "predicted_assists_p90",
        "predicted_steals_p90",
        "predicted_blocks_p90",
        "predicted_turnovers_p90",
    }.issubset(enriched.columns):
        fd_p90 = pd.DataFrame(
            {
                "points": pd.to_numeric(enriched["predicted_points_p90"], errors="coerce"),
                "rebounds": pd.to_numeric(enriched["predicted_rebounds_p90"], errors="coerce"),
                "assists": pd.to_numeric(enriched["predicted_assists_p90"], errors="coerce"),
                "steals": pd.to_numeric(enriched["predicted_steals_p90"], errors="coerce"),
                "blocks": pd.to_numeric(enriched["predicted_blocks_p90"], errors="coerce"),
                "turnovers": pd.to_numeric(enriched["predicted_turnovers_p90"], errors="coerce"),
            }
        )
        enriched["predicted_fanduel_points_p90"] = calculate_fanduel_points(fd_p90).round(3)

    return enriched


def _confidence_flag(
    games_used: float | int | None,
    priors_available: bool,
    *,
    predicted_minutes: float | int | None = None,
    expected_minutes: float | int | None = None,
    is_starter: bool = False,
    unavailable: bool = False,
    injury_risk_score: float | int | None = None,
) -> str:
    games_used = 0 if pd.isna(games_used) else int(games_used)
    minute_candidates = [
        pd.to_numeric(pd.Series([predicted_minutes]), errors="coerce").iloc[0],
        pd.to_numeric(pd.Series([expected_minutes]), errors="coerce").iloc[0],
    ]
    projected_minutes = next((float(value) for value in minute_candidates if pd.notna(value)), 0.0)
    risk_numeric = pd.to_numeric(pd.Series([injury_risk_score]), errors="coerce").iloc[0]
    risk_score = 0.0 if pd.isna(risk_numeric) else float(risk_numeric)

    if unavailable:
        return "low_confidence"
    if risk_score >= 0.8:
        return "low_confidence"
    if projected_minutes < 8:
        return "low_confidence"
    if projected_minutes < 16 and not is_starter:
        return "low_confidence"

    if games_used >= 12 and priors_available and projected_minutes >= 20 and is_starter:
        return "high_confidence"
    if games_used >= 5 and priors_available and projected_minutes >= 14:
        return "medium_confidence" if risk_score < 0.5 else "low_confidence"
    if games_used >= 5 and projected_minutes >= 18:
        return "medium_confidence" if risk_score < 0.5 else "low_confidence"
    if games_used > 0 and priors_available:
        return "medium_confidence" if risk_score < 0.5 else "low_confidence"
    return "low_confidence"


def _predict_target_with_strategy(
    target: str,
    prediction_frame: pd.DataFrame,
    bundle: dict,
    predicted_minutes: pd.Series | None,
) -> tuple[pd.Series, pd.Series | None]:
    feature_spec = bundle["feature_spec"].get(target, {})
    feature_matrix = _prepare_feature_matrix(prediction_frame, feature_spec)
    raw_predictions = pd.Series(bundle["models"][target].predict(feature_matrix), index=prediction_frame.index)

    role_models_for_target = bundle.get("role_models", {}).get(target, {})
    if role_models_for_target:
        role_rows_for_target = bundle.get("role_model_rows", {}).get(target, {})
        role_blend_for_target = bundle.get("role_model_blend", {}).get(target, {})
        role_feature_specs_for_target = bundle.get("role_feature_spec", {}).get(target, {})
        if target == MINUTES_TARGET:
            role_minutes_context = _resolve_minutes_series(pd.DataFrame(index=prediction_frame.index), prediction_frame)
        elif predicted_minutes is not None:
            role_minutes_context = pd.to_numeric(predicted_minutes, errors="coerce")
        else:
            role_minutes_context = _resolve_minutes_series(pd.DataFrame(index=prediction_frame.index), prediction_frame)
        starter_context = (
            pd.to_numeric(prediction_frame["starter"], errors="coerce")
            if "starter" in prediction_frame.columns
            else pd.Series(np.nan, index=prediction_frame.index, dtype=float)
        )
        if "starter_probability" in prediction_frame.columns:
            starter_probability = pd.to_numeric(prediction_frame["starter_probability"], errors="coerce")
            starter_context = starter_context.where(starter_context.notna(), starter_probability)
        starter_context = starter_context.fillna(0.0)
        role_bucket = _role_bucket_series(starter_context, role_minutes_context)
        for role_name, role_model in role_models_for_target.items():
            role_mask = role_bucket.eq(role_name)
            if not role_mask.any():
                continue
            role_feature_spec = role_feature_specs_for_target.get(role_name, feature_spec)
            role_features = _prepare_feature_matrix(prediction_frame.loc[role_mask], role_feature_spec)
            role_prediction = pd.Series(role_model.predict(role_features), index=role_features.index)
            role_rows = pd.to_numeric(pd.Series([role_rows_for_target.get(role_name, 0)]), errors="coerce").fillna(0).iloc[0]
            dynamic_blend = float(np.clip(role_rows / (role_rows + 350.0), 0.2, 0.8))
            blend = float(role_blend_for_target.get(role_name, dynamic_blend))
            raw_predictions.loc[role_mask] = (
                raw_predictions.loc[role_mask] * (1.0 - blend) + role_prediction * blend
            )

    strategy = bundle.get("target_strategy", {}).get(target, DIRECT_STRATEGY)
    if target == MINUTES_TARGET or strategy != RATE_STRATEGY:
        return raw_predictions, predicted_minutes

    if predicted_minutes is None:
        predicted_minutes = _resolve_minutes_series(pd.DataFrame(index=prediction_frame.index), prediction_frame)
    predicted_minutes = pd.to_numeric(predicted_minutes, errors="coerce").fillna(24.0).clip(lower=0, upper=48)
    totals = raw_predictions * predicted_minutes.clip(lower=0, upper=RATE_MINUTES_CAP)
    return totals, predicted_minutes


def _load_bundle_with_compatibility_fallback(bundle_path: Path) -> tuple[dict, bool]:
    try:
        return joblib.load(bundle_path), False
    except Exception as exc:  # noqa: BLE001
        message = str(exc)
        compatibility_markers = (
            "_RemainderColsList",
            "Can't get attribute",
            "ColumnTransformer",
        )
        if not any(marker in message for marker in compatibility_markers):
            raise

        training_path = resolve_training_data_path()
        if not training_path.exists():
            raise RuntimeError(
                f"Model bundle is incompatible and fallback retraining data was not found: {training_path}"
            ) from exc

        lookback_days = TRAINING_LOOKBACK_DEFAULT_DAYS
        if DEFAULT_METRICS_PATH.exists():
            try:
                metrics_payload = json.loads(DEFAULT_METRICS_PATH.read_text(encoding="utf-8"))
                lookback_days = int(metrics_payload.get("training_lookback_days") or lookback_days)
            except Exception:  # noqa: BLE001
                lookback_days = TRAINING_LOOKBACK_DEFAULT_DAYS
        lookback_days = max(7, lookback_days)

        train_engine(
            data_path=training_path,
            bundle_path=bundle_path,
            metrics_path=DEFAULT_METRICS_PATH,
            lookback_days=lookback_days,
        )
        return joblib.load(bundle_path), True


def predict_engine(
    input_path: Path | None = None,
    bundle_path: Path = DEFAULT_BUNDLE_PATH,
    output_path: Path = DEFAULT_PREDICTIONS_PATH,
    predict_all: bool = False,
) -> dict:
    input_path = input_path or resolve_upcoming_data_path()
    if not bundle_path.exists():
        raise FileNotFoundError(f"Model bundle not found: {bundle_path}")

    bundle, bundle_retrained_for_compatibility = _load_bundle_with_compatibility_fallback(bundle_path)
    trained_targets = bundle["trained_targets"]

    history_path = Path(bundle.get("data_path") or resolve_training_data_path())
    if not history_path.exists():
        raise FileNotFoundError(f"Historical training dataset not found: {history_path}")

    history_frame = load_dataset(history_path)
    history_frame, _ = _filter_modeling_history_rows(history_frame)
    if history_frame.empty:
        raise ValueError("Historical training dataset has no valid NBA rows after quality filtering.")
    history_frame = history_frame.copy()
    history_frame["__prediction_row"] = False

    input_path_resolved = input_path.resolve()
    history_path_resolved = history_path.resolve()

    if predict_all and input_path_resolved == history_path_resolved:
        combined_frame = history_frame
        combined_frame["__prediction_row"] = True
    elif input_path_resolved == history_path_resolved:
        combined_frame = history_frame
        existing_target_columns = [target for target in trained_targets if target in combined_frame.columns]
        if existing_target_columns:
            combined_frame["__prediction_row"] = combined_frame[existing_target_columns].isna().all(axis=1)
        else:
            combined_frame["__prediction_row"] = True
    else:
        upcoming_frame = load_dataset(input_path)
        upcoming_frame = upcoming_frame.copy()
        upcoming_frame["__prediction_row"] = True
        combined_frame = _concat_with_union([history_frame, upcoming_frame])

    combined_frame = _merge_prediction_context(combined_frame)
    feature_frame = build_feature_frame(combined_frame)
    prediction_frame = feature_frame[feature_frame["__prediction_row"]].copy()
    prediction_frame = _prediction_quality_gate(prediction_frame)

    if prediction_frame.empty:
        raise ValueError(
            "No prediction rows were found. Use upcoming rows with blank outcome columns, omit outcome columns entirely, or rerun with predict_all."
        )
    if prediction_frame["prediction_quality_blocked"].all():
        raise ValueError(
            "All candidate prediction rows failed quality checks. "
            "Provide valid player_name, game_date, team/opponent, and minutes context."
        )

    result_columns = [
        column
        for column in [
            "player_name",
            "game_date",
            "home",
            "opponent",
            "team",
            "position",
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
            "expected_minutes",
            "expected_minutes_confidence",
            "minutes_projection_error_estimate",
            "pregame_lock_window_stage",
            "pregame_lock_minutes_to_tipoff",
            "pregame_lock_window_weight",
            "commence_time_utc",
            "salary_dk",
            "salary_fd",
            "spread",
            "game_total",
            "implied_team_total",
            "rest_days",
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
            "games_played_before",
            "season_priors_available",
            "prediction_quality_score",
            "prediction_quality_blocked",
            "prediction_quality_issues",
        ]
        if column in prediction_frame.columns
    ]
    if "home" not in result_columns and "home" in prediction_frame.columns:
        result_columns.append("home")
    result = prediction_frame[result_columns].copy()
    result["game_date"] = pd.to_datetime(result["game_date"]).dt.strftime("%Y-%m-%d")

    model_order = list(trained_targets)
    if MINUTES_TARGET in model_order:
        model_order = [MINUTES_TARGET] + [target for target in model_order if target != MINUTES_TARGET]

    predicted_minutes: pd.Series | None = None
    calibration_profile = _load_calibration_profile()
    adaptive_learning_profile = _load_adaptive_learning_profile()
    for target in model_order:
        if target not in bundle.get("models", {}):
            continue
        target_prediction, predicted_minutes = _predict_target_with_strategy(
            target,
            prediction_frame,
            bundle,
            predicted_minutes,
        )
        result[f"predicted_{target}"] = target_prediction

    result = _apply_prediction_sanity_and_fallbacks(
        result,
        prediction_frame,
        calibration_map=bundle.get("residual_calibration", {}),
    )
    result = _derive_display_predictions(result)
    result = _apply_prediction_intervals_and_error_estimates(
        result,
        prediction_frame,
        bundle=bundle,
        calibration_profile=calibration_profile,
    )
    result = _add_pregame_anchor_columns(result, prediction_frame)
    result = _apply_anchor_projection_blend(result, prediction_frame)
    result = _apply_adaptive_learning_corrections(
        result,
        prediction_frame,
        adaptive_learning_profile,
    )
    result = _derive_display_predictions(result)
    result = _apply_prediction_intervals_and_error_estimates(
        result,
        prediction_frame,
        bundle=bundle,
        calibration_profile=calibration_profile,
    )
    result = _add_pregame_anchor_columns(result, prediction_frame)
    starter_numeric = pd.to_numeric(prediction_frame.get("starter"), errors="coerce")
    starter_probability = pd.to_numeric(prediction_frame.get("starter_probability"), errors="coerce")
    if starter_numeric.notna().any():
        starter_series = starter_numeric.fillna(starter_probability).fillna(0).ge(0.5)
    else:
        starter_series = starter_probability.fillna(0).ge(0.6)
    status_text = (
        result.get("injury_status", pd.Series("", index=result.index)).fillna("").astype(str)
        + " "
        + result.get("health_status", pd.Series("", index=result.index)).fillna("").astype(str)
        + " "
        + result.get("suspension_status", pd.Series("", index=result.index)).fillna("").astype(str)
    )
    unavailable_series = status_text.str.contains(UNAVAILABLE_STATUS_PATTERN)

    result["historical_games_used"] = pd.to_numeric(result.get("games_played_before"), errors="coerce").fillna(0).astype(int)
    result["season_priors_available"] = pd.to_numeric(result.get("season_priors_available"), errors="coerce").fillna(0).astype(int).astype(bool)
    result["is_starter"] = starter_series.astype(bool)
    result["availability_unavailable"] = unavailable_series.astype(bool)
    result["confidence_flag"] = result.apply(
        lambda row: _confidence_flag(
            row.get("historical_games_used"),
            bool(row.get("season_priors_available")),
            predicted_minutes=row.get("predicted_minutes"),
            expected_minutes=row.get("expected_minutes"),
            is_starter=bool(row.get("is_starter")),
            unavailable=bool(row.get("availability_unavailable")),
            injury_risk_score=row.get("injury_risk_score"),
        ),
        axis=1,
    )
    if "predicted_points" in result.columns:
        low_output_mask = (
            pd.to_numeric(result["predicted_points"], errors="coerce").fillna(0).lt(1.0)
            & pd.to_numeric(result.get("predicted_minutes"), errors="coerce").fillna(0).lt(22.0)
        )
        result.loc[low_output_mask, "confidence_flag"] = "low_confidence"
        near_zero_points_mask = pd.to_numeric(result["predicted_points"], errors="coerce").fillna(0).lt(1.0)
        result.loc[near_zero_points_mask, "confidence_flag"] = "low_confidence"

    if "projection_error_pct_estimate" in result.columns:
        projection_error_pct = pd.to_numeric(result["projection_error_pct_estimate"], errors="coerce")
        quality_confidence_flag = pd.Series("low_confidence", index=result.index, dtype=object)
        quality_confidence_flag.loc[projection_error_pct.le(30.0)] = "medium_confidence"
        quality_confidence_flag.loc[projection_error_pct.le(16.0)] = "high_confidence"

        def _merge_confidence(base_flag: object, quality_flag: object) -> str:
            base = str(base_flag or "low_confidence")
            quality = str(quality_flag or "low_confidence")
            if base == "low_confidence":
                return "low_confidence"
            if quality == "high_confidence":
                return "high_confidence" if base == "high_confidence" else "medium_confidence"
            if quality == "medium_confidence":
                return base
            # quality low: demote by one tier rather than collapsing all rows to low.
            return "medium_confidence" if base == "high_confidence" else "low_confidence"

        result["confidence_flag"] = [
            _merge_confidence(base, quality)
            for base, quality in zip(result["confidence_flag"], quality_confidence_flag)
        ]

    if "prediction_quality_blocked" in result.columns:
        blocked_mask = result["prediction_quality_blocked"].astype(bool)
        prediction_columns = [column for column in result.columns if column.startswith("predicted_")]
        if blocked_mask.any() and prediction_columns:
            result.loc[blocked_mask, prediction_columns] = np.nan
            result.loc[blocked_mask, "confidence_flag"] = "low_confidence"
            if "projection_error_pct_estimate" in result.columns:
                result.loc[blocked_mask, "projection_error_pct_estimate"] = 65.0
            if "projection_confidence_pct" in result.columns:
                result.loc[blocked_mask, "projection_confidence_pct"] = 35.0

    result = result.drop(
        columns=[column for column in ["games_played_before", "availability_unavailable"] if column in result.columns]
    )
    output_path.parent.mkdir(parents=True, exist_ok=True)
    result.to_csv(output_path, index=False)

    return {
        "input_path": str(input_path),
        "output_path": str(output_path),
        "rows": int(len(result)),
        "predictions": result.head(25).to_dict(orient="records"),
        "bundle_retrained_for_compatibility": bool(bundle_retrained_for_compatibility),
        "trained_targets": trained_targets,
        "display_targets": [
            column.replace("predicted_", "")
            for column in result.columns
            if column.startswith("predicted_")
        ],
        "adaptive_learning_profile_path": (
            str(DEFAULT_ADAPTIVE_LEARNING_PROFILE_PATH)
            if DEFAULT_ADAPTIVE_LEARNING_PROFILE_PATH.exists()
            else None
        ),
        "adaptive_learning_generated_at": (
            adaptive_learning_profile.get("generated_at")
            if isinstance(adaptive_learning_profile, dict)
            else None
        ),
    }


def _recheck_role_bucket_series(frame: pd.DataFrame) -> pd.Series:
    actual_minutes = pd.to_numeric(frame.get("actual_minutes"), errors="coerce").fillna(0.0)
    starter = pd.to_numeric(frame.get("starter"), errors="coerce").fillna(0.0).ge(0.5)
    role = pd.Series("bench_low_minutes", index=frame.index, dtype="object")
    role.loc[actual_minutes.ge(16.0)] = "rotation"
    role.loc[starter | actual_minutes.ge(28.0)] = "starter_core"
    role.loc[actual_minutes.le(0.0)] = "unknown"
    return role


def _compute_role_split_summary(
    recheck_frame: pd.DataFrame,
    *,
    targets: list[str],
    floor_map: dict[str, float],
) -> dict[str, dict[str, float | int | None]]:
    if recheck_frame.empty:
        return {}

    roles = _recheck_role_bucket_series(recheck_frame)
    summaries: dict[str, dict[str, float | int | None]] = {}
    for role_name in ["starter_core", "rotation", "bench_low_minutes", "unknown"]:
        role_mask = roles.eq(role_name)
        if not role_mask.any():
            continue
        role_errors: list[float] = []
        for target in targets:
            if target not in recheck_frame.columns or f"predicted_{target}" not in recheck_frame.columns:
                continue
            actual = pd.to_numeric(recheck_frame.loc[role_mask, target], errors="coerce")
            predicted = pd.to_numeric(recheck_frame.loc[role_mask, f"predicted_{target}"], errors="coerce")
            valid = actual.notna() & predicted.notna()
            if not valid.any():
                continue
            error = (predicted.loc[valid] - actual.loc[valid]).abs()
            pct_floor = error / actual.loc[valid].abs().clip(lower=floor_map.get(target, 1.0)) * 100.0
            role_errors.extend(pct_floor.tolist())
        summaries[role_name] = {
            "rows": int(role_mask.sum()),
            "mean_abs_pct_error_floor": float(np.mean(role_errors)) if role_errors else None,
        }
    return summaries


def recheck_past_predictions(
    data_path: Path | None = None,
    lookback_days: int | None = None,
    sample_rows: int | None = None,
) -> dict:
    data_path = data_path or resolve_training_data_path()
    if not data_path.exists():
        raise FileNotFoundError(f"Training dataset not found: {data_path}")
    if not DEFAULT_BUNDLE_PATH.exists():
        raise FileNotFoundError(f"Model bundle not found: {DEFAULT_BUNDLE_PATH}")

    if lookback_days is not None:
        lookback_days = int(lookback_days)
        if lookback_days <= 0:
            raise ValueError("lookback_days must be a positive integer when provided.")
    if sample_rows is not None:
        sample_rows = int(sample_rows)
        if sample_rows < 0:
            raise ValueError("sample_rows must be a non-negative integer when provided.")

    raw_frame = load_dataset(data_path).copy()
    if raw_frame.empty:
        raise ValueError("Training data contains no rows.")
    if "game_date" not in raw_frame.columns:
        raise ValueError("Training data must include game_date for recheck sampling.")

    raw_frame["game_date"] = pd.to_datetime(raw_frame["game_date"], errors="coerce")
    raw_frame = raw_frame.sort_values("game_date").dropna(subset=["game_date"]).reset_index(drop=True)
    if raw_frame.empty:
        raise ValueError("Training data has no valid game_date values.")

    max_date = raw_frame["game_date"].max()
    if lookback_days is not None:
        cutoff = max_date - pd.Timedelta(days=lookback_days)
        raw_frame = raw_frame[raw_frame["game_date"] >= cutoff].copy()
        if raw_frame.empty:
            raise ValueError(f"No training rows were found in the last {lookback_days} days.")

    raw_frame = raw_frame.reset_index(drop=True)
    raw_frame, quality_summary = _filter_modeling_history_rows(raw_frame)
    if raw_frame.empty:
        raise ValueError("No valid NBA rows remained for recheck after quality filtering.")

    date_series = raw_frame["game_date"].dt.normalize()
    unique_dates = sorted(date_series.dropna().unique().tolist())
    if len(unique_dates) < 2:
        raise ValueError("Recheck requires at least two distinct game dates for leakage-safe backtesting.")

    max_eval_dates = min(MAX_RECHECK_EVAL_DATES, max(1, len(unique_dates) - 1))
    eval_dates = unique_dates[-max_eval_dates:]
    first_eval_date = eval_dates[0]
    history_seed = raw_frame[date_series < first_eval_date].copy()

    adaptive_min_seed_rows = min(MIN_INITIAL_RECHECK_TRAIN_ROWS, max(50, int(len(raw_frame) * 0.35)))
    if len(history_seed) < adaptive_min_seed_rows:
        for index in range(len(unique_dates) - 1):
            candidate_eval_dates = unique_dates[index + 1 :]
            if not candidate_eval_dates:
                continue
            candidate_seed = raw_frame[date_series < candidate_eval_dates[0]].copy()
            if len(candidate_seed) >= adaptive_min_seed_rows:
                eval_dates = candidate_eval_dates[-max_eval_dates:]
                first_eval_date = eval_dates[0]
                history_seed = raw_frame[date_series < first_eval_date].copy()
                break

    if len(history_seed) < 5:
        raise ValueError(
            "Not enough rows are available before the evaluation window for a leakage-safe rolling recheck."
        )

    lookback_for_temp_train = max(int(lookback_days) if lookback_days is not None else 3650, 3650)

    with tempfile.TemporaryDirectory(prefix="nba_recheck_") as temp_dir:
        temp_root = Path(temp_dir)
        history_path = temp_root / "training_history.csv"
        bundle_path = temp_root / "engine_bundle.joblib"
        metrics_path = temp_root / "engine_metrics.json"
        prediction_output_path = temp_root / "predictions.csv"

        history_running = history_seed.copy().reset_index(drop=True)
        history_running.to_csv(history_path, index=False)
        train_engine(
            data_path=history_path,
            bundle_path=bundle_path,
            metrics_path=metrics_path,
            lookback_days=lookback_for_temp_train,
        )

        recheck_batches: list[pd.DataFrame] = []
        evaluated_dates: list[str] = []
        for eval_date in eval_dates:
            day_mask = date_series.eq(eval_date)
            day_actual = raw_frame.loc[day_mask].copy()
            if day_actual.empty:
                continue

            day_actual["game_date"] = pd.to_datetime(day_actual["game_date"], errors="coerce").dt.normalize()
            day_upcoming = _prepare_recheck_upcoming_rows(day_actual)
            day_upcoming["game_date"] = pd.to_datetime(day_upcoming["game_date"], errors="coerce").dt.normalize()

            day_upcoming_path = temp_root / f"upcoming_{pd.Timestamp(eval_date).strftime('%Y%m%d')}.csv"
            day_upcoming.to_csv(day_upcoming_path, index=False)
            history_running.to_csv(history_path, index=False)

            predict_engine(
                input_path=day_upcoming_path,
                bundle_path=bundle_path,
                output_path=prediction_output_path,
                predict_all=False,
            )
            day_pred = pd.read_csv(prediction_output_path)
            if day_pred.empty:
                history_running = pd.concat([history_running, day_actual], ignore_index=True)
                continue
            day_pred["game_date"] = pd.to_datetime(day_pred["game_date"], errors="coerce").dt.normalize()
            if "home" in day_pred.columns:
                day_pred["home"] = pd.to_numeric(day_pred["home"], errors="coerce").fillna(0).astype(int)
            if "home" in day_actual.columns:
                day_actual["home"] = pd.to_numeric(day_actual["home"], errors="coerce").fillna(0).astype(int)
            if "team" in day_pred.columns:
                day_pred["team"] = day_pred["team"].map(normalize_team_code)
            if "team" in day_actual.columns:
                day_actual["team"] = day_actual["team"].map(normalize_team_code)
            if "opponent" in day_pred.columns:
                day_pred["opponent"] = day_pred["opponent"].map(normalize_team_code)
            if "opponent" in day_actual.columns:
                day_actual["opponent"] = day_actual["opponent"].map(normalize_team_code)

            merge_keys = [column for column in ["player_name", "game_date", "team", "opponent", "home"] if column in day_pred.columns and column in day_actual.columns]
            if not merge_keys:
                merge_keys = [column for column in ["player_name", "game_date"] if column in day_pred.columns and column in day_actual.columns]
            merged_day = day_pred.merge(day_actual, on=merge_keys, how="inner", suffixes=("", "__actual"))
            if merged_day.empty:
                history_running = pd.concat([history_running, day_actual], ignore_index=True)
                continue

            for target in [MINUTES_TARGET] + ALL_TARGETS:
                if target in merged_day.columns:
                    continue
                actual_column = f"{target}__actual"
                if actual_column in merged_day.columns:
                    merged_day[target] = pd.to_numeric(merged_day[actual_column], errors="coerce")
            if "starter" not in merged_day.columns and "starter__actual" in merged_day.columns:
                merged_day["starter"] = pd.to_numeric(merged_day["starter__actual"], errors="coerce")
            merged_day["actual_minutes"] = pd.to_numeric(merged_day.get("minutes"), errors="coerce")
            merged_day["game_date"] = pd.to_datetime(merged_day["game_date"], errors="coerce").dt.strftime("%Y-%m-%d")
            recheck_batches.append(merged_day)
            evaluated_dates.append(pd.Timestamp(eval_date).strftime("%Y-%m-%d"))

            history_running = pd.concat([history_running, day_actual], ignore_index=True)
            history_running = history_running.sort_values("game_date").reset_index(drop=True)

    if not recheck_batches:
        raise ValueError("Rolling recheck produced no matched prediction rows.")

    recheck_frame = pd.concat(recheck_batches, ignore_index=True)

    if sample_rows:
        sample_rows = min(int(sample_rows), int(len(recheck_frame)))
        if sample_rows > 0:
            recheck_frame = recheck_frame.tail(sample_rows).copy()

    primary_targets = [target for target in ["points", "rebounds", "assists"] if target in recheck_frame.columns]
    support_targets = [
        target for target in ["steals", "blocks", "turnovers", "three_points_made"]
        if target in recheck_frame.columns
    ]
    all_targets = primary_targets + [target for target in support_targets if target not in primary_targets]

    per_target: dict[str, dict[str, float | int]] = {}
    overall_errors: list[float] = []
    overall_pct_errors: list[float] = []
    overall_pct_errors_prop_floor: list[float] = []
    floor_map = {
        "points": 5.0,
        "rebounds": 3.0,
        "assists": 3.0,
        "steals": 1.0,
        "blocks": 1.0,
        "turnovers": 1.0,
        "three_points_made": 1.0,
    }
    prop_floor_map = {
        "points": 12.0,
        "rebounds": 5.0,
        "assists": 4.0,
        "steals": 1.0,
        "blocks": 1.0,
        "turnovers": 1.0,
        "three_points_made": 1.0,
    }
    actual_minutes_series = pd.to_numeric(recheck_frame.get("actual_minutes"), errors="coerce").fillna(0.0)
    starter_for_dnp = pd.to_numeric(recheck_frame.get("starter"), errors="coerce").fillna(0.0)
    unavailable_text = (
        recheck_frame.get("injury_status", pd.Series("", index=recheck_frame.index)).fillna("").astype(str)
        + " "
        + recheck_frame.get("health_status", pd.Series("", index=recheck_frame.index)).fillna("").astype(str)
        + " "
        + recheck_frame.get("suspension_status", pd.Series("", index=recheck_frame.index)).fillna("").astype(str)
    ).str.lower()
    unavailable_flag = unavailable_text.str.contains(UNAVAILABLE_STATUS_PATTERN, regex=True)
    dnp_targets = [target for target in ["points", "rebounds", "assists", "steals", "blocks", "turnovers", "three_points_made"] if target in recheck_frame.columns]
    if dnp_targets:
        dnp_total = pd.Series(0.0, index=recheck_frame.index, dtype=float)
        for target in dnp_targets:
            dnp_total = dnp_total + pd.to_numeric(recheck_frame[target], errors="coerce").fillna(0.0).abs()
        dnp_zero_box = dnp_total.le(0.01)
    else:
        dnp_zero_box = pd.Series(False, index=recheck_frame.index)
    dnp_near_zero_box = dnp_total.le(RECHECK_DNP_BOX_TOTAL_THRESHOLD)
    dnp_like_mask = (
        (
            actual_minutes_series.lt(RECHECK_DNP_MINUTES_THRESHOLD)
            & dnp_zero_box
        )
        | (
            actual_minutes_series.lt(RECHECK_DNP_NEAR_MINUTES_THRESHOLD)
            & dnp_near_zero_box
            & (~starter_for_dnp.ge(0.5) | unavailable_flag)
        )
    )
    low_minutes_non_dnp_mask = actual_minutes_series.lt(RECHECK_MINUTES_EVALUATION_FLOOR) & ~dnp_like_mask
    normal_outcome_mask = actual_minutes_series.ge(RECHECK_MINUTES_EVALUATION_FLOOR) & ~dnp_like_mask
    # Keep core recheck metrics strictly on "normal outcomes" to avoid inflation from true DNP / near-DNP rows.
    evaluation_row_mask = normal_outcome_mask

    for target in all_targets:
        actual = pd.to_numeric(recheck_frame[target], errors="coerce")
        predicted = pd.to_numeric(recheck_frame[f"predicted_{target}"], errors="coerce")
        mask = actual.notna() & predicted.notna() & evaluation_row_mask
        if not mask.any():
            continue

        actual_values = actual[mask]
        predicted_values = predicted[mask]
        error = (predicted_values - actual_values).abs()
        pct_error = error / actual_values.abs().replace(0, 1.0) * 100.0
        pct_error_floor = error / actual_values.abs().clip(lower=floor_map.get(target, 1.0)) * 100.0
        pct_error_prop_floor = error / actual_values.abs().clip(lower=prop_floor_map.get(target, 1.0)) * 100.0
        per_target[target] = {
            "rows": int(mask.sum()),
            "mae": float(mean_absolute_error(actual_values, predicted_values)),
            "rmse": float(root_mean_squared_error(actual_values, predicted_values)),
            "mean_abs_pct_error": float(pct_error.mean()),
            "median_abs_pct_error": float(np.median(pct_error)),
            "mean_abs_pct_error_floor": float(pct_error_floor.mean()),
            "median_abs_pct_error_floor": float(np.median(pct_error_floor)),
            "mean_abs_pct_error_prop_floor": float(pct_error_prop_floor.mean()),
            "median_abs_pct_error_prop_floor": float(np.median(pct_error_prop_floor)),
        }
        if target in ("points", "rebounds", "assists"):
            overall_errors.extend(error.tolist())
            overall_pct_errors.extend(pct_error_floor.tolist())
            overall_pct_errors_prop_floor.extend(pct_error_prop_floor.tolist())

    if not per_target:
        raise ValueError("No recheck targets had matching predicted and actual values.")

    def _split_summary(mask: pd.Series) -> dict[str, object]:
        split_rows = int(mask.sum())
        payload: dict[str, object] = {
            "rows": split_rows,
            "per_target": {},
            "overall_mean_abs_pct_error_floor": None,
            "overall_mean_abs_pct_error_prop_floor": None,
        }
        if split_rows <= 0:
            return payload

        split_errors_floor: list[float] = []
        split_errors_prop_floor: list[float] = []
        for target in all_targets:
            predicted_column = f"predicted_{target}"
            if target not in recheck_frame.columns or predicted_column not in recheck_frame.columns:
                continue
            actual = pd.to_numeric(recheck_frame[target], errors="coerce")
            predicted = pd.to_numeric(recheck_frame[predicted_column], errors="coerce")
            valid = mask & actual.notna() & predicted.notna()
            if not valid.any():
                continue

            error = (predicted[valid] - actual[valid]).abs()
            pct_error_floor = error / actual[valid].abs().clip(lower=floor_map.get(target, 1.0)) * 100.0
            pct_error_prop_floor = error / actual[valid].abs().clip(lower=prop_floor_map.get(target, 1.0)) * 100.0

            payload["per_target"][target] = {
                "rows": int(valid.sum()),
                "mae": float(error.mean()),
                "mean_abs_pct_error_floor": float(pct_error_floor.mean()),
                "mean_abs_pct_error_prop_floor": float(pct_error_prop_floor.mean()),
            }
            if target in ("points", "rebounds", "assists"):
                split_errors_floor.extend(pct_error_floor.tolist())
                split_errors_prop_floor.extend(pct_error_prop_floor.tolist())

        if split_errors_floor:
            payload["overall_mean_abs_pct_error_floor"] = float(np.mean(split_errors_floor))
        if split_errors_prop_floor:
            payload["overall_mean_abs_pct_error_prop_floor"] = float(np.mean(split_errors_prop_floor))
        return payload

    overall_row_count = len(overall_errors)
    overall = {
        "rows": overall_row_count,
        "mae": float(np.mean(overall_errors)) if overall_row_count else 0.0,
        "rmse": float(np.sqrt(np.mean(np.square(overall_errors)))) if overall_row_count else 0.0,
        "mean_abs_pct_error": float(np.mean(overall_pct_errors)) if overall_pct_errors else 0.0,
        "mean_abs_pct_error_prop_floor": float(np.mean(overall_pct_errors_prop_floor))
        if overall_pct_errors_prop_floor
        else 0.0,
    }

    actual_minutes_series = pd.to_numeric(recheck_frame.get("actual_minutes"), errors="coerce").fillna(0)
    starter_series = pd.to_numeric(recheck_frame.get("starter"), errors="coerce")
    starter_like_series = starter_series.fillna(actual_minutes_series.ge(28).astype(float)).ge(0.5)
    games_before_series = pd.Series(np.nan, index=recheck_frame.index, dtype=float)
    if "games_played_before" in recheck_frame.columns:
        games_before_series = pd.to_numeric(recheck_frame["games_played_before"], errors="coerce")
    if "historical_games_used" in recheck_frame.columns:
        historical_games_series = pd.to_numeric(recheck_frame["historical_games_used"], errors="coerce")
        games_before_series = games_before_series.where(games_before_series.notna(), historical_games_series)
    games_before_series = games_before_series.fillna(0.0)
    eligible_mask = (
        starter_like_series
        & games_before_series.ge(5)
        & actual_minutes_series.ge(24)
    )
    if not eligible_mask.any():
        eligible_mask = games_before_series.ge(5) & actual_minutes_series.ge(24)
    eligible_summary: dict[str, float | int] = {
        "rows": int(eligible_mask.sum()),
        "mean_abs_pct_error_floor": None,
        "mean_abs_pct_error_prop_floor": None,
    }
    if eligible_mask.any():
        eligible_pct_errors: list[float] = []
        eligible_pct_errors_prop_floor: list[float] = []
        for target in ("points", "rebounds", "assists"):
            if target not in all_targets:
                continue
            actual = pd.to_numeric(recheck_frame.loc[eligible_mask, target], errors="coerce")
            predicted = pd.to_numeric(recheck_frame.loc[eligible_mask, f"predicted_{target}"], errors="coerce")
            mask = actual.notna() & predicted.notna()
            if not mask.any():
                continue
            error = (predicted[mask] - actual[mask]).abs()
            eligible_pct_errors.extend((error / actual[mask].abs().clip(lower=floor_map.get(target, 1.0)) * 100.0).tolist())
            eligible_pct_errors_prop_floor.extend(
                (error / actual[mask].abs().clip(lower=prop_floor_map.get(target, 1.0)) * 100.0).tolist()
            )
        if eligible_pct_errors:
            eligible_summary["mean_abs_pct_error_floor"] = float(np.mean(eligible_pct_errors))
        if eligible_pct_errors_prop_floor:
            eligible_summary["mean_abs_pct_error_prop_floor"] = float(np.mean(eligible_pct_errors_prop_floor))

    role_splits = _compute_role_split_summary(
        recheck_frame,
        targets=[target for target in ("points", "rebounds", "assists") if target in all_targets],
        floor_map=floor_map,
    )

    error_target = next((target for target in ("points", "rebounds", "assists") if target in recheck_frame.columns), None)
    if error_target is None:
        error_target = all_targets[0] if all_targets else None

    sample_preview: list[dict[str, object]] = []
    if error_target is not None:
        diagnostic_floor = floor_map.get(error_target, 1.0)
        worst_rows = (
            recheck_frame
            .assign(
                error_pct=lambda row: (
                    (pd.to_numeric(row[f"predicted_{error_target}"], errors="coerce") - pd.to_numeric(row[error_target], errors="coerce")).abs()
                    / pd.to_numeric(row[error_target], errors="coerce").abs().clip(lower=diagnostic_floor)
                    * 100.0
                )
            )
            .copy()
        )
        worst_rows[f"error_abs"] = (
            pd.to_numeric(worst_rows[f"predicted_{error_target}"], errors="coerce") - pd.to_numeric(worst_rows[error_target], errors="coerce")
        ).abs()
        worst_rows["error_pct"] = pd.to_numeric(worst_rows["error_pct"], errors="coerce")
        worst_rows["error_abs"] = pd.to_numeric(worst_rows["error_abs"], errors="coerce")
        finite_mask = np.isfinite(worst_rows["error_pct"].to_numpy(dtype=float))
        worst_rows = worst_rows.loc[finite_mask].sort_values(
            ["error_pct", "error_abs"],
            ascending=[False, False],
        )
        sample_preview = worst_rows.head(25)[
            [column for column in ["player_name", "game_date", "team", "opponent", error_target, f"predicted_{error_target}", "error_pct"] if column in worst_rows.columns]
        ].to_dict(orient="records")

    calibration_profile: dict[str, object] = {
        "generated_at": pd.Timestamp.now().isoformat(),
        "modeling_version": "2.1",
        "evaluated_rows": int(len(recheck_frame)),
        "evaluated_dates": sorted(set(recheck_frame["game_date"].dropna().astype(str).tolist())),
        "per_target": {},
        "overall": {
            "mean_abs_pct_error_floor": float(overall.get("mean_abs_pct_error", 0.0)),
            "mae": float(overall.get("mae", 0.0)),
            "rmse": float(overall.get("rmse", 0.0)),
            "mean_abs_pct_error_prop_floor": float(overall.get("mean_abs_pct_error_prop_floor", 0.0)),
        },
    }
    for target in all_targets:
        predicted_column = f"predicted_{target}"
        if target not in recheck_frame.columns or predicted_column not in recheck_frame.columns:
            continue
        actual = pd.to_numeric(recheck_frame[target], errors="coerce")
        predicted = pd.to_numeric(recheck_frame[predicted_column], errors="coerce")
        distribution = _fit_error_distribution(actual, predicted)
        mean_pct_floor = per_target.get(target, {}).get("mean_abs_pct_error_floor")
        calibration_profile["per_target"][target] = {
            **distribution,
            "mean_abs_pct_error_floor": float(mean_pct_floor) if mean_pct_floor is not None else 18.0,
        }
    DEFAULT_CALIBRATION_PATH.parent.mkdir(parents=True, exist_ok=True)
    DEFAULT_CALIBRATION_PATH.write_text(json.dumps(calibration_profile, indent=2), encoding="utf-8")

    recheck_payload = {
        "input_path": str(data_path),
        "lookback_days": lookback_days,
        "sample_rows": sample_rows if sample_rows else int(len(recheck_frame)),
        "evaluated_rows": int(len(recheck_frame)),
        "evaluation_minutes_floor": RECHECK_MINUTES_EVALUATION_FLOOR if evaluation_row_mask is not None else None,
        "rows_evaluated_normal_outcomes": int(evaluation_row_mask.sum()),
        "rows_filtered_low_minutes": int(low_minutes_non_dnp_mask.sum()),
        "rows_filtered_dnp_like": int(dnp_like_mask.sum()),
        "evaluation_splits": {
            "normal_outcomes": _split_summary(normal_outcome_mask),
            "low_minutes_non_dnp": _split_summary(low_minutes_non_dnp_mask),
            "dnp_like": _split_summary(dnp_like_mask),
        },
        "backtest_method": "leakage_safe_rolling_by_date",
        "backtest_eval_dates": sorted(set(recheck_frame["game_date"].dropna().astype(str).tolist())),
        "backtest_initial_training_rows": int(len(history_seed)),
        "quality_filter": quality_summary,
        "generated_at": pd.Timestamp.now().strftime("%Y-%m-%d %H:%M:%S"),
        "overall": overall,
        "operational_subset": eligible_summary,
        "role_splits": role_splits,
        "per_target": per_target,
        "calibration_profile_path": str(DEFAULT_CALIBRATION_PATH),
        "sample_preview": sample_preview,
        "diagnostic_target": error_target,
        "rows_by_date": {
            "from": str(pd.to_datetime(recheck_frame["game_date"].min())) if not recheck_frame.empty else None,
            "to": str(pd.to_datetime(recheck_frame["game_date"].max())) if not recheck_frame.empty else None,
        },
    }

    DEFAULT_RECHECK_PATH.parent.mkdir(parents=True, exist_ok=True)
    DEFAULT_RECHECK_PATH.write_text(json.dumps(recheck_payload, indent=2), encoding="utf-8")
    return recheck_payload


def load_metrics(metrics_path: Path = DEFAULT_METRICS_PATH) -> dict | None:
    if not metrics_path.exists():
        return None
    return json.loads(metrics_path.read_text(encoding="utf-8"))


def _csv_columns(path: Path) -> list[str]:
    try:
        with path.open("r", encoding="utf-8", errors="ignore", newline="") as input_file:
            return [str(column) for column in next(csv.reader(input_file), [])]
    except OSError:
        return []


def load_predictions(output_path: Path = DEFAULT_PREDICTIONS_PATH, include_preview: bool = True) -> dict | None:
    if not output_path.exists():
        return None
    if include_preview:
        frame = pd.read_csv(output_path, nrows=25)
        columns = list(frame.columns)
        preview = frame.head(25).fillna("").to_dict(orient="records")
    else:
        columns = _csv_columns(output_path)
        preview = []
    return {
        "path": str(output_path),
        "rows": int(csv_data_row_count(output_path)),
        "columns": columns,
        "preview": preview,
    }


def _generic_csv_preview(path: Path, rows: int = 5, include_preview: bool = True) -> dict | None:
    if not path.exists():
        return None
    if include_preview:
        read_rows = max(1, int(rows))
        frame = pd.read_csv(path, nrows=read_rows)
        columns = list(frame.columns)
        preview = frame.head(rows).fillna("").to_dict(orient="records")
    else:
        columns = _csv_columns(path)
        preview = []
    return {
        "path": str(path),
        "rows": int(csv_data_row_count(path)),
        "columns": columns,
        "preview": preview,
    }


def app_status(include_previews: bool = True) -> dict:
    training_path = resolve_training_data_path()
    upcoming_path = DEFAULT_UPCOMING_PATH if DEFAULT_UPCOMING_PATH.exists() else None
    context_path = DEFAULT_CONTEXT_UPDATES_PATH if DEFAULT_CONTEXT_UPDATES_PATH.exists() else None
    provider_context_path = DEFAULT_PROVIDER_CONTEXT_PATH if DEFAULT_PROVIDER_CONTEXT_PATH.exists() else None
    season_priors_path = DEFAULT_SEASON_PRIORS_PATH if DEFAULT_SEASON_PRIORS_PATH.exists() else None
    prizepicks_lines_path = DEFAULT_PRIZEPICKS_LINES_PATH if DEFAULT_PRIZEPICKS_LINES_PATH.exists() else None
    calibration_path = DEFAULT_CALIBRATION_PATH if DEFAULT_CALIBRATION_PATH.exists() else None

    season_priors_dataset = (
        _generic_csv_preview(season_priors_path, include_preview=include_previews)
        if season_priors_path and season_priors_path.exists()
        else None
    )
    calibration_profile = _load_calibration_profile(calibration_path) if calibration_path and calibration_path.exists() else None
    predictions = load_predictions(include_preview=include_previews)
    model_coverage = None
    low_confidence_projection_count = 0
    blocked_projection_rows = 0
    if predictions:
        prediction_rows = int(csv_data_row_count(DEFAULT_PREDICTIONS_PATH))
        for chunk in pd.read_csv(
            DEFAULT_PREDICTIONS_PATH,
            usecols=lambda column: column in {"confidence_flag", "prediction_quality_blocked"},
            chunksize=100_000,
        ):
            confidence_values = chunk.get("confidence_flag", pd.Series(dtype="object")).fillna("")
            low_confidence_projection_count += int(confidence_values.eq("low_confidence").sum())
            blocked_values = pd.to_numeric(chunk.get("prediction_quality_blocked"), errors="coerce").fillna(0.0)
            blocked_projection_rows += int(blocked_values.astype(bool).sum())
        model_coverage = {
            "predicted_rows": prediction_rows,
            "season_priors_rows": int(csv_data_row_count(season_priors_path)) if season_priors_path and season_priors_path.exists() else 0,
            "training_rows": int(csv_data_row_count(training_path)) if training_path.exists() else 0,
            "blocked_prediction_rows": blocked_projection_rows,
        }
    return {
        "schema": SCHEMA_GUIDE,
        "training_dataset": (
            dataset_preview(training_path) if include_previews else _generic_csv_preview(training_path, include_preview=False)
        )
        if training_path.exists()
        else None,
        "upcoming_dataset": (
            dataset_preview(upcoming_path) if include_previews else _generic_csv_preview(upcoming_path, include_preview=False)
        )
        if upcoming_path and upcoming_path.exists()
        else None,
        "context_dataset": (
            _generic_csv_preview(context_path, include_preview=include_previews) if context_path and context_path.exists() else None
        ),
        "provider_context_dataset": (
            _generic_csv_preview(provider_context_path, include_preview=include_previews)
            if provider_context_path and provider_context_path.exists()
            else None
        ),
        "season_priors_dataset": season_priors_dataset,
        "prizepicks_lines_dataset": (
            load_prizepicks_lines(prizepicks_lines_path, include_preview=include_previews)
            if prizepicks_lines_path and prizepicks_lines_path.exists()
            else None
        ),
        "prizepicks_edges": load_prizepicks_edges(include_preview=include_previews),
        "metrics": load_metrics(),
        "calibration_profile": calibration_profile,
        "predictions": predictions,
        "model_coverage": model_coverage,
        "low_confidence_projection_count": low_confidence_projection_count,
        "blocked_prediction_rows": blocked_projection_rows,
        "bundle_exists": DEFAULT_BUNDLE_PATH.exists(),
        "downloads": {
            "bundle": str(DEFAULT_BUNDLE_PATH.name) if DEFAULT_BUNDLE_PATH.exists() else None,
            "metrics": str(DEFAULT_METRICS_PATH.name) if DEFAULT_METRICS_PATH.exists() else None,
            "predictions": str(DEFAULT_PREDICTIONS_PATH.name) if DEFAULT_PREDICTIONS_PATH.exists() else None,
            "calibration_profile": str(DEFAULT_CALIBRATION_PATH.name) if DEFAULT_CALIBRATION_PATH.exists() else None,
            "season_priors": str(DEFAULT_SEASON_PRIORS_PATH.name) if DEFAULT_SEASON_PRIORS_PATH.exists() else None,
            "prizepicks_lines": str(DEFAULT_PRIZEPICKS_LINES_PATH.name) if DEFAULT_PRIZEPICKS_LINES_PATH.exists() else None,
            "prizepicks_edges": str(DEFAULT_PRIZEPICKS_EDGES_PATH.name) if DEFAULT_PRIZEPICKS_EDGES_PATH.exists() else None,
        },
    }
