from __future__ import annotations

from pathlib import Path

import pandas as pd
from pandas.api.types import is_bool_dtype, is_numeric_dtype

try:
    from .player_matching import add_player_keys, normalize_player_name, normalize_team_code
    from .scoring import calculate_draftkings_points, calculate_fanduel_points, calculate_pra
except ImportError:
    from player_matching import add_player_keys, normalize_player_name, normalize_team_code
    from scoring import calculate_draftkings_points, calculate_fanduel_points, calculate_pra


DEFAULT_PROJECT_DIR = Path(__file__).resolve().parents[1]
DEFAULT_DATA_PATH = DEFAULT_PROJECT_DIR / "data" / "player_game_logs.csv"
DEFAULT_UPCOMING_PATH = DEFAULT_PROJECT_DIR / "data" / "upcoming_slate.csv"
DEFAULT_TRAINING_UPLOAD_PATH = DEFAULT_PROJECT_DIR / "data" / "training_data.csv"
DEFAULT_CONTEXT_UPDATES_PATH = DEFAULT_PROJECT_DIR / "data" / "context_updates.csv"
DEFAULT_PROVIDER_CONTEXT_PATH = DEFAULT_PROJECT_DIR / "data" / "provider_context_updates.csv"
DEFAULT_SEASON_PRIORS_PATH = DEFAULT_PROJECT_DIR / "data" / "season_priors.csv"
DEFAULT_PRIZEPICKS_LINES_PATH = DEFAULT_PROJECT_DIR / "data" / "prizepicks_lines.csv"
DEFAULT_ALIAS_OVERRIDES_PATH = DEFAULT_PROJECT_DIR / "data" / "player_alias_overrides.csv"
DEFAULT_MODELS_DIR = DEFAULT_PROJECT_DIR / "models"
DEFAULT_MPLCONFIGDIR = DEFAULT_PROJECT_DIR / ".matplotlib"

IDENTIFIER_COLUMNS = ["player_name", "game_date", "home", "opponent"]
LIVE_AUTOMATION_REQUIRED_COLUMNS = ["player_name", "game_date", "team", "opponent", "home"]
PRIMARY_TARGETS = ["points", "rebounds", "assists"]
SUPPORT_TARGETS = ["steals", "blocks", "turnovers", "three_points_made"]
DERIVED_TARGETS = ["pra", "draftkings_points", "fanduel_points"]
ALL_TARGETS = PRIMARY_TARGETS + SUPPORT_TARGETS + DERIVED_TARGETS

KNOWN_ROLLING_STAT_COLUMNS = [
    "points",
    "rebounds",
    "assists",
    "steals",
    "blocks",
    "turnovers",
    "three_points_made",
    "minutes",
    "field_goals_made",
    "field_goals_attempted",
    "free_throws_made",
    "free_throws_attempted",
    "offensive_rebounds",
    "defensive_rebounds",
    "personal_fouls",
    "plus_minus",
    "usage_rate",
]

OPTIONAL_CONTEXT_COLUMNS = [
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

SEASON_PRIOR_COLUMNS = [
    "gp",
    "wins",
    "losses",
    "min_season",
    "pts_season",
    "fgm_season",
    "fga_season",
    "fg_pct_season",
    "three_pm_season",
    "three_pa_season",
    "three_pct_season",
    "ftm_season",
    "fta_season",
    "ft_pct_season",
    "oreb_season",
    "dreb_season",
    "reb_season",
    "ast_season",
    "tov_season",
    "stl_season",
    "blk_season",
    "pf_season",
    "fp_season",
    "dd2_season",
    "td3_season",
    "plus_minus_season",
]

SCHEMA_GUIDE = {
    "required_training_columns": IDENTIFIER_COLUMNS + PRIMARY_TARGETS,
    "required_live_automation_columns": LIVE_AUTOMATION_REQUIRED_COLUMNS,
    "optional_context_columns": OPTIONAL_CONTEXT_COLUMNS,
    "optional_support_columns_for_fantasy": SUPPORT_TARGETS,
    "recommended_historical_stat_columns": KNOWN_ROLLING_STAT_COLUMNS,
    "notes": [
        "Use one row per player per game.",
        "Only include information known before tip-off in current-row context columns.",
        "Same-game box score stats should only appear in historical rows; upcoming rows should leave them blank or omit them.",
        "Fantasy projections require historical steals, blocks, turnovers, and three_points_made for component-based scoring.",
        "Live auto-generation of upcoming slates works best when historical rows include a team column.",
        "Use context_updates.csv for continuously refreshed availability, suspension, odds, and minutes context.",
        "The live engine also writes provider_context_updates.csv for automated odds and injury-source updates.",
        "Teammate-composition and hourly news context are optional live features and should only use pre-tipoff information.",
        "Optional player market-line context (line_points, line_rebounds, line_assists, line_pra, line_three_points_made) can improve projection calibration.",
        "Text-heavy columns such as family context work better when encoded into short structured categories instead of free-form notes.",
        "Season priors from aggregate player-season tables belong in season_priors.csv, not training_data.csv.",
    ],
}


def load_dataset(path: Path) -> pd.DataFrame:
    if not path.exists():
        raise FileNotFoundError(f"Dataset not found: {path}")

    frame = pd.read_csv(path)
    if frame.empty:
        raise ValueError("Dataset is empty.")

    missing_columns = [column for column in IDENTIFIER_COLUMNS if column not in frame.columns]
    if missing_columns:
        raise ValueError(f"Dataset is missing required identifier columns: {missing_columns}")

    frame = frame.copy()
    frame["game_date"] = pd.to_datetime(frame["game_date"], errors="raise").dt.tz_localize(None)
    frame["home"] = frame["home"].apply(_coerce_home_value)
    if "team" in frame.columns:
        frame["team"] = frame["team"].map(normalize_team_code)
    if "opponent" in frame.columns:
        frame["opponent"] = frame["opponent"].map(normalize_team_code)
    frame = frame.sort_values(["player_name", "game_date"]).reset_index(drop=True)

    for column in frame.columns:
        if column in {"player_name", "game_date", "opponent"}:
            continue
        if frame[column].dtype == object:
            stripped = frame[column].astype(str).str.strip()
            if stripped.isin(["", "nan", "None", "NaN"]).all():
                frame[column] = pd.NA
                continue
            numeric_candidate = pd.to_numeric(frame[column], errors="coerce")
            if numeric_candidate.notna().sum() >= max(3, int(len(frame) * 0.6)):
                frame[column] = numeric_candidate

    return frame


def _coerce_home_value(value) -> int:
    if pd.isna(value):
        return 0
    if isinstance(value, str):
        normalized = value.strip().lower()
        if normalized in {"1", "true", "home", "h", "yes"}:
            return 1
        if normalized in {"0", "false", "away", "a", "no"}:
            return 0
    return int(float(value))


def augment_targets(frame: pd.DataFrame) -> pd.DataFrame:
    augmented = frame.copy()

    if set(PRIMARY_TARGETS).issubset(augmented.columns):
        augmented["pra"] = calculate_pra(augmented)

    if set(SUPPORT_TARGETS + PRIMARY_TARGETS).issubset(augmented.columns):
        augmented["draftkings_points"] = calculate_draftkings_points(augmented)
        augmented["fanduel_points"] = calculate_fanduel_points(augmented)

    return augmented


def load_season_priors(path: Path = DEFAULT_SEASON_PRIORS_PATH) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame(columns=["player_name", "team", "player_key", "team_key"] + SEASON_PRIOR_COLUMNS)

    frame = pd.read_csv(path)
    if frame.empty:
        return pd.DataFrame(columns=["player_name", "team", "player_key", "team_key"] + SEASON_PRIOR_COLUMNS)

    working = frame.copy()
    if "player_name" not in working.columns or "team" not in working.columns:
        raise ValueError("season_priors.csv must include player_name and team columns.")
    working["player_name"] = working["player_name"].astype(str).str.strip()
    working["team"] = working["team"].map(normalize_team_code).fillna(working["team"])
    working = add_player_keys(working)
    for column in SEASON_PRIOR_COLUMNS:
        if column not in working.columns:
            working[column] = pd.NA
        else:
            working[column] = pd.to_numeric(working[column], errors="coerce")
    return working[["player_name", "team", "player_key", "team_key"] + SEASON_PRIOR_COLUMNS].drop_duplicates(
        subset=["player_key", "team_key"],
        keep="last",
    )


def build_season_priors_from_history(frame: pd.DataFrame) -> pd.DataFrame:
    if frame.empty:
        return pd.DataFrame(columns=["player_name", "team"] + SEASON_PRIOR_COLUMNS)

    history = augment_targets(frame.copy()).sort_values(["player_name", "game_date"]).reset_index(drop=True)
    latest = history.groupby("player_name", as_index=False).tail(1)[["player_name", "team"]].copy()
    aggregated = history.groupby("player_name", as_index=False).size().rename(columns={"size": "gp"})

    mean_column_map = {
        "minutes": "min_season",
        "points": "pts_season",
        "field_goals_made": "fgm_season",
        "field_goals_attempted": "fga_season",
        "three_points_made": "three_pm_season",
        "free_throws_made": "ftm_season",
        "free_throws_attempted": "fta_season",
        "offensive_rebounds": "oreb_season",
        "defensive_rebounds": "dreb_season",
        "rebounds": "reb_season",
        "assists": "ast_season",
        "turnovers": "tov_season",
        "steals": "stl_season",
        "blocks": "blk_season",
        "personal_fouls": "pf_season",
        "plus_minus": "plus_minus_season",
    }
    for source_column, output_column in mean_column_map.items():
        if source_column in history.columns:
            means = history.groupby("player_name", as_index=False)[source_column].mean().rename(columns={source_column: output_column})
            aggregated = aggregated.merge(means, on="player_name", how="left")
        else:
            aggregated[output_column] = pd.NA

    if "age" in history.columns:
        ages = history.groupby("player_name", as_index=False)["age"].last()
        aggregated = aggregated.merge(ages, on="player_name", how="left")
    else:
        aggregated["age"] = pd.NA

    if {"field_goals_made", "field_goals_attempted"}.issubset(history.columns):
        shooting = history.groupby("player_name", as_index=False)[["field_goals_made", "field_goals_attempted"]].sum()
        shooting["fg_pct_season"] = (shooting["field_goals_made"] / shooting["field_goals_attempted"]).where(
            shooting["field_goals_attempted"].gt(0)
        ) * 100
        aggregated = aggregated.merge(shooting[["player_name", "fg_pct_season"]], on="player_name", how="left")
    else:
        aggregated["fg_pct_season"] = pd.NA

    if {"three_points_made", "field_goals_attempted"}.issubset(history.columns):
        three_point_attempt_column = "three_points_attempted" if "three_points_attempted" in history.columns else None
        if three_point_attempt_column:
            three_shooting = history.groupby("player_name", as_index=False)[["three_points_made", three_point_attempt_column]].sum()
            three_shooting["three_pa_season"] = three_shooting[three_point_attempt_column] / three_shooting["player_name"].map(
                history.groupby("player_name").size()
            )
            three_shooting["three_pct_season"] = (three_shooting["three_points_made"] / three_shooting[three_point_attempt_column]).where(
                three_shooting[three_point_attempt_column].gt(0)
            ) * 100
            aggregated = aggregated.merge(
                three_shooting[["player_name", "three_pa_season", "three_pct_season"]],
                on="player_name",
                how="left",
            )
        else:
            aggregated["three_pa_season"] = pd.NA
            aggregated["three_pct_season"] = pd.NA
    else:
        aggregated["three_pa_season"] = pd.NA
        aggregated["three_pct_season"] = pd.NA

    if {"free_throws_made", "free_throws_attempted"}.issubset(history.columns):
        free_throw_totals = history.groupby("player_name", as_index=False)[["free_throws_made", "free_throws_attempted"]].sum()
        free_throw_totals["ft_pct_season"] = (free_throw_totals["free_throws_made"] / free_throw_totals["free_throws_attempted"]).where(
            free_throw_totals["free_throws_attempted"].gt(0)
        ) * 100
        aggregated = aggregated.merge(free_throw_totals[["player_name", "ft_pct_season"]], on="player_name", how="left")
    else:
        aggregated["ft_pct_season"] = pd.NA

    if {"points", "rebounds", "assists"}.issubset(history.columns):
        dd_mask = (history[["points", "rebounds", "assists"]] >= 10).sum(axis=1) >= 2
        td_mask = (history[["points", "rebounds", "assists"]] >= 10).sum(axis=1) >= 3
        dd_counts = history.assign(dd2=dd_mask.astype(int), td3=td_mask.astype(int)).groupby("player_name", as_index=False)[["dd2", "td3"]].sum()
        dd_counts = dd_counts.rename(columns={"dd2": "dd2_season", "td3": "td3_season"})
        aggregated = aggregated.merge(dd_counts, on="player_name", how="left")
    else:
        aggregated["dd2_season"] = pd.NA
        aggregated["td3_season"] = pd.NA

    if "draftkings_points" in history.columns:
        fantasy = history.groupby("player_name", as_index=False)["draftkings_points"].mean().rename(columns={"draftkings_points": "fp_season"})
        aggregated = aggregated.merge(fantasy, on="player_name", how="left")
    else:
        aggregated["fp_season"] = pd.NA

    priors = latest.merge(aggregated, on="player_name", how="left")
    priors["wins"] = pd.NA
    priors["losses"] = pd.NA
    for column in ["player_name", "team"] + SEASON_PRIOR_COLUMNS:
        if column not in priors.columns:
            priors[column] = pd.NA
    return priors[["player_name", "team"] + SEASON_PRIOR_COLUMNS].copy()


def refresh_season_priors_from_history(
    training_path: Path = DEFAULT_TRAINING_UPLOAD_PATH,
    season_priors_path: Path = DEFAULT_SEASON_PRIORS_PATH,
) -> dict:
    history = load_dataset(training_path)
    derived = build_season_priors_from_history(history)
    derived = add_player_keys(derived)

    if season_priors_path.exists():
        existing = load_season_priors(season_priors_path)
    else:
        existing = pd.DataFrame(columns=["player_name", "team", "player_key", "team_key"] + SEASON_PRIOR_COLUMNS)

    combined = derived.copy()
    if not existing.empty:
        existing = existing.copy()
        base = existing.set_index("player_key")
        fill = derived.set_index("player_key")
        combined = fill.combine_first(base).reset_index()
        if "team" not in combined.columns and "team_key" in combined.columns:
            combined["team"] = combined["team_key"]

    output = combined[["player_name", "team"] + SEASON_PRIOR_COLUMNS].copy()
    output["team"] = output["team"].map(normalize_team_code).fillna(output["team"])
    output = output.drop_duplicates(subset=["player_name", "team"], keep="last").sort_values(["team", "player_name"])
    season_priors_path.parent.mkdir(parents=True, exist_ok=True)
    output.to_csv(season_priors_path, index=False)
    return {
        "path": str(season_priors_path),
        "rows": int(len(output)),
        "players": int(output["player_name"].nunique()),
    }


def merge_season_priors(frame: pd.DataFrame, season_priors_path: Path = DEFAULT_SEASON_PRIORS_PATH) -> pd.DataFrame:
    if frame.empty:
        return frame

    priors = load_season_priors(season_priors_path)
    working = add_player_keys(frame)
    if priors.empty:
        working["season_priors_available"] = 0
        return working

    merged = working.merge(
        priors.drop(columns=["player_name", "team"]),
        on=["player_key", "team_key"],
        how="left",
    )

    if merged[SEASON_PRIOR_COLUMNS].isna().all(axis=1).any():
        unmatched_mask = merged[SEASON_PRIOR_COLUMNS].isna().all(axis=1)
        fallback = working.loc[unmatched_mask].merge(
            priors.drop(columns=["team", "team_key"]).drop_duplicates(subset=["player_key"], keep="last"),
            on="player_key",
            how="left",
            suffixes=("", "__fallback"),
        )
        for column in SEASON_PRIOR_COLUMNS:
            fallback_column = f"{column}__fallback"
            if fallback_column in fallback.columns:
                merged.loc[unmatched_mask, column] = fallback[fallback_column].values

    core_prior_columns = [column for column in ["pts_season", "reb_season", "ast_season", "min_season", "fp_season"] if column in merged.columns]
    merged["season_priors_available"] = merged[core_prior_columns].notna().any(axis=1).astype(int) if core_prior_columns else 0
    return merged


def build_feature_frame(frame: pd.DataFrame, season_priors_path: Path = DEFAULT_SEASON_PRIORS_PATH) -> pd.DataFrame:
    feature_frame = augment_targets(frame)
    feature_frame = merge_season_priors(feature_frame, season_priors_path=season_priors_path)
    rolling_columns: list[str] = []

    # Mixed history/upcoming frames introduce NA-only stat columns as object dtype.
    # Coerce rolling sources up front so pandas rolling windows stay numeric.
    rolling_source_columns = [column for column in KNOWN_ROLLING_STAT_COLUMNS if column in feature_frame.columns]
    for column in rolling_source_columns:
        feature_frame[column] = pd.to_numeric(feature_frame[column], errors="coerce")

    grouped = feature_frame.groupby("player_name", sort=False)
    feature_frame["games_played_before"] = grouped.cumcount()
    feature_frame["days_since_last_game"] = grouped["game_date"].diff().dt.days
    feature_frame["game_month"] = feature_frame["game_date"].dt.month
    feature_frame["game_dayofweek"] = feature_frame["game_date"].dt.dayofweek
    feature_frame["is_weekend"] = feature_frame["game_dayofweek"].isin([5, 6]).astype(int)

    for column in rolling_source_columns:
        last_1 = f"{column}_last_1"
        avg_3 = f"{column}_avg_last_3"
        avg_5 = f"{column}_avg_last_5"
        avg_10 = f"{column}_avg_last_10"
        rolling_columns.extend([last_1, avg_3, avg_5, avg_10])

        feature_frame[last_1] = grouped[column].shift(1)
        feature_frame[avg_3] = grouped[column].transform(lambda series: series.shift(1).rolling(3, min_periods=1).mean())
        feature_frame[avg_5] = grouped[column].transform(lambda series: series.shift(1).rolling(5, min_periods=1).mean())
        feature_frame[avg_10] = grouped[column].transform(lambda series: series.shift(1).rolling(10, min_periods=1).mean())

    for rolling_column, season_column, delta_column in [
        ("points_avg_last_5", "pts_season", "points_form_vs_season"),
        ("rebounds_avg_last_5", "reb_season", "rebounds_form_vs_season"),
        ("assists_avg_last_5", "ast_season", "assists_form_vs_season"),
        ("minutes_avg_last_5", "min_season", "minutes_form_vs_season"),
    ]:
        if rolling_column in feature_frame.columns and season_column in feature_frame.columns:
            feature_frame[delta_column] = feature_frame[rolling_column] - feature_frame[season_column]

    passthrough_columns = [
        column
        for column in feature_frame.columns
        if column not in set(ALL_TARGETS + KNOWN_ROLLING_STAT_COLUMNS + ["game_date"])
        and column not in {"player_name", "opponent"}
    ]

    keep_columns = ["player_name", "game_date", "opponent"]
    if "home" in feature_frame.columns:
        keep_columns.append("home")
    keep_columns.extend(
        column
        for column in ["games_played_before", "days_since_last_game", "game_month", "game_dayofweek", "is_weekend"]
        if column in feature_frame.columns and column not in keep_columns
    )

    for column in passthrough_columns:
        if column not in keep_columns:
            keep_columns.append(column)

    for column in rolling_columns:
        if column in feature_frame.columns and column not in keep_columns:
            keep_columns.append(column)

    for column in ALL_TARGETS:
        if column in feature_frame.columns and column not in keep_columns:
            keep_columns.append(column)

    return feature_frame[keep_columns]


def discover_feature_columns(feature_frame: pd.DataFrame) -> tuple[list[str], list[str]]:
    categorical_columns: list[str] = []
    numeric_columns: list[str] = []
    excluded = set(ALL_TARGETS + ["game_date"])

    for column in feature_frame.columns:
        if column in excluded:
            continue
        if is_bool_dtype(feature_frame[column]) or feature_frame[column].dtype == object:
            categorical_columns.append(column)
        elif is_numeric_dtype(feature_frame[column]):
            numeric_columns.append(column)
        else:
            categorical_columns.append(column)

    return categorical_columns, numeric_columns


def dataset_preview(path: Path, rows: int = 5) -> dict:
    if not path.exists():
        raise FileNotFoundError(f"Dataset not found: {path}")

    preview = pd.read_csv(path, nrows=max(1, int(rows)))
    row_count = csv_data_row_count(path)
    if row_count <= 0:
        return {
            "path": str(path),
            "rows": 0,
            "columns": list(preview.columns),
            "preview": [],
        }

    if "game_date" in preview.columns:
        preview["game_date"] = pd.to_datetime(preview["game_date"], errors="coerce").dt.strftime("%Y-%m-%d")
    return {
        "path": str(path),
        "rows": int(row_count),
        "columns": list(preview.columns),
        "preview": preview.fillna("").to_dict(orient="records"),
    }


def csv_data_row_count(path: Path) -> int:
    if not path.exists():
        return 0

    line_count = 0
    last_byte = b""
    with path.open("rb") as handle:
        while True:
            chunk = handle.read(1_048_576)
            if not chunk:
                break
            line_count += chunk.count(b"\n")
            last_byte = chunk[-1:]

    # Count a final non-newline terminated record.
    if last_byte and last_byte != b"\n":
        line_count += 1

    # Subtract header row if present.
    return max(line_count - 1, 0)
