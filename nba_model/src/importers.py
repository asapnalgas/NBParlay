from __future__ import annotations

import io
import json
from pathlib import Path

import pandas as pd

try:
    from .features import DEFAULT_PROJECT_DIR, DEFAULT_TRAINING_UPLOAD_PATH
    from .live_sync import _append_completed_rows, _boxscore_players_to_rows, fetch_json
    from .player_matching import normalize_player_name, normalize_team_code
    from .data_pipeline import (
        append_rejections,
        append_quarantine,
        check_and_register_idempotency,
        compute_frame_fingerprint,
        ensure_pipeline_layout,
        record_ingestion_event,
        split_duplicates_by_policy,
        write_stage_snapshot,
    )
except ImportError:
    from features import DEFAULT_PROJECT_DIR, DEFAULT_TRAINING_UPLOAD_PATH
    from live_sync import _append_completed_rows, _boxscore_players_to_rows, fetch_json
    from player_matching import normalize_player_name, normalize_team_code
    from data_pipeline import (
        append_rejections,
        append_quarantine,
        check_and_register_idempotency,
        compute_frame_fingerprint,
        ensure_pipeline_layout,
        record_ingestion_event,
        split_duplicates_by_policy,
        write_stage_snapshot,
    )


DEFAULT_SEASON_PRIORS_PATH = DEFAULT_PROJECT_DIR / "data" / "season_priors.csv"
DEFAULT_PRIZEPICKS_LINES_PATH = DEFAULT_PROJECT_DIR / "data" / "prizepicks_lines.csv"
DEFAULT_ALIAS_OVERRIDES_PATH = DEFAULT_PROJECT_DIR / "data" / "player_alias_overrides.csv"

SCHEDULE_URL = "https://cdn.nba.com/static/json/staticData/scheduleLeagueV2_9.json"
BOXSCORE_URL_TEMPLATE = "https://cdn.nba.com/static/json/liveData/boxscore/boxscore_{game_id}.json"

SEASON_PRIOR_COLUMN_MAP = {
    "player": "player_name",
    "team": "team",
    "age": "age",
    "gp": "gp",
    "w": "wins",
    "l": "losses",
    "min": "min_season",
    "pts": "pts_season",
    "fgm": "fgm_season",
    "fga": "fga_season",
    "fg%": "fg_pct_season",
    "3pm": "three_pm_season",
    "3pa": "three_pa_season",
    "3p%": "three_pct_season",
    "ftm": "ftm_season",
    "fta": "fta_season",
    "ft%": "ft_pct_season",
    "oreb": "oreb_season",
    "dreb": "dreb_season",
    "reb": "reb_season",
    "ast": "ast_season",
    "tov": "tov_season",
    "stl": "stl_season",
    "blk": "blk_season",
    "pf": "pf_season",
    "fp": "fp_season",
    "dd2": "dd2_season",
    "td3": "td3_season",
    "+/-": "plus_minus_season",
}

SEASON_PRIOR_NUMERIC_COLUMNS = [
    "age",
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

PRIZEPICKS_MARKETS = {"points", "rebounds", "assists", "pra"}
HISTORICAL_REQUIRED_COLUMNS = ["player_name", "game_date", "home", "opponent", "points", "rebounds", "assists"]


def _read_delimited_text(text: str) -> pd.DataFrame:
    stripped = text.strip()
    if not stripped:
        raise ValueError("No text was provided.")
    separator = "\t" if "\t" in stripped else ","
    frame = pd.read_csv(io.StringIO(stripped), sep=separator)
    return frame


def _read_csv_bytes(payload: bytes) -> pd.DataFrame:
    return pd.read_csv(io.BytesIO(payload))


def _normalize_frame_columns(frame: pd.DataFrame) -> pd.DataFrame:
    working = frame.copy()
    cleaned_columns: list[str] = []
    for column in working.columns:
        text = str(column).strip()
        if text.lower().startswith("unnamed") or text == "":
            cleaned_columns.append("")
        else:
            cleaned_columns.append(text)
    working.columns = cleaned_columns
    if "" in working.columns:
        working = working.drop(columns=[""])
    return working


def _empty_csv(path: Path, columns: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(columns=columns).to_csv(path, index=False)


def _coerce_home_value(value):
    if pd.isna(value):
        return pd.NA
    if isinstance(value, str):
        normalized = value.strip().lower()
        if normalized in {"1", "true", "home", "h", "yes"}:
            return 1
        if normalized in {"0", "false", "away", "a", "no"}:
            return 0
        return pd.NA
    try:
        numeric = int(float(value))
    except Exception:
        return pd.NA
    if numeric in {0, 1}:
        return numeric
    return pd.NA


def _tag_rejected_rows(frame: pd.DataFrame, reason: str) -> pd.DataFrame:
    if frame.empty:
        return frame.copy()
    rejected = frame.copy()
    rejected["reject_reason"] = str(reason)
    return rejected


def _rows_in_csv(path: Path) -> int:
    if not path.exists():
        return 0
    try:
        return int(len(pd.read_csv(path)))
    except Exception:
        return 0


def ensure_support_files() -> None:
    ensure_pipeline_layout()
    if not DEFAULT_SEASON_PRIORS_PATH.exists():
        _empty_csv(DEFAULT_SEASON_PRIORS_PATH, ["player_name", "team"] + SEASON_PRIOR_NUMERIC_COLUMNS)
    if not DEFAULT_PRIZEPICKS_LINES_PATH.exists():
        _empty_csv(
            DEFAULT_PRIZEPICKS_LINES_PATH,
            ["player_name", "team", "game_date", "market", "line", "selection_type", "source", "captured_at"],
        )
    if not DEFAULT_ALIAS_OVERRIDES_PATH.exists():
        _empty_csv(DEFAULT_ALIAS_OVERRIDES_PATH, ["alias_name", "player_name", "team"])


def import_season_priors_frame(frame: pd.DataFrame, output_path: Path = DEFAULT_SEASON_PRIORS_PATH) -> dict:
    working = _normalize_frame_columns(frame)
    source_key = f"import_season_priors:{output_path.resolve()}"
    fingerprint = compute_frame_fingerprint(working)
    idempotency = check_and_register_idempotency(
        dataset="season_priors",
        source=source_key,
        fingerprint=fingerprint,
        metadata={"rows_in": int(len(working)), "output_path": str(output_path)},
    )
    if idempotency.get("duplicate"):
        existing_rows = _rows_in_csv(output_path)
        existing_preview = (
            pd.read_csv(output_path).head(10).fillna("").to_dict(orient="records")
            if output_path.exists()
            else []
        )
        event = record_ingestion_event(
            dataset="season_priors",
            stage="import",
            rows_in=len(working),
            rows_out=existing_rows,
            rows_rejected=0,
            source=source_key,
            outcome="duplicate_skipped",
            details={"idempotency": idempotency, "output_path": str(output_path)},
        )
        return {
            "path": str(output_path),
            "rows_accepted": 0,
            "rows_rejected": 0,
            "duplicate_rows_removed": 0,
            "skipped": True,
            "skip_reason": "duplicate_payload",
            "existing_rows": existing_rows,
            "idempotency": idempotency,
            "ingestion_event": event,
            "normalized_columns": [],
            "preview": existing_preview,
        }
    write_stage_snapshot("season_priors", "bronze", working)
    renamed = working.rename(
        columns={
            column: SEASON_PRIOR_COLUMN_MAP[str(column).strip().lower()]
            for column in working.columns
            if str(column).strip().lower() in SEASON_PRIOR_COLUMN_MAP
        }
    )

    required_columns = ["player_name", "team", "gp", "min_season", "pts_season", "reb_season", "ast_season"]
    missing_columns = [column for column in required_columns if column not in renamed.columns]
    if missing_columns:
        raise ValueError(f"Season priors are missing required columns: {missing_columns}")

    normalized = renamed.copy()
    normalized["player_name"] = normalized["player_name"].astype(str).str.strip()
    normalized["team"] = normalized["team"].map(normalize_team_code)
    normalized["player_key"] = normalized["player_name"].map(normalize_player_name)

    rejected_frames: list[pd.DataFrame] = []
    missing_player_mask = normalized["player_key"] == ""
    rejected_frames.append(_tag_rejected_rows(normalized[missing_player_mask], "missing_player_name"))
    normalized = normalized[normalized["player_key"] != ""].copy()

    for column in SEASON_PRIOR_NUMERIC_COLUMNS:
        if column in normalized.columns:
            normalized[column] = pd.to_numeric(normalized[column], errors="coerce")

    invalid_numeric_mask = normalized[["gp", "min_season", "pts_season", "reb_season", "ast_season"]].isna().any(axis=1)
    rejected_frames.append(_tag_rejected_rows(normalized[invalid_numeric_mask], "invalid_core_numeric_fields"))
    normalized = normalized[~invalid_numeric_mask].copy()

    normalized, duplicate_rows, dedupe_keys = split_duplicates_by_policy(
        "season_priors",
        normalized,
        fallback_keys=["player_key", "team"],
        keep="last",
    )
    duplicate_count = int(len(duplicate_rows))
    if duplicate_count:
        rejected_frames.append(_tag_rejected_rows(duplicate_rows, f"duplicate_on_keys:{'|'.join(dedupe_keys)}"))
    rejected = pd.concat(rejected_frames, ignore_index=True, sort=False) if rejected_frames else pd.DataFrame()

    ordered_columns = ["player_name", "team"] + [column for column in SEASON_PRIOR_NUMERIC_COLUMNS if column in normalized.columns]
    output_path.parent.mkdir(parents=True, exist_ok=True)
    silver_output = normalized[ordered_columns].sort_values(["team", "player_name"]).reset_index(drop=True)
    silver_output.to_csv(output_path, index=False)
    write_stage_snapshot("season_priors", "silver", silver_output)
    rejection_log = append_rejections("season_priors", rejected, source="import_season_priors")
    quarantine_log = append_quarantine("season_priors", rejected, source="import_season_priors")
    event = record_ingestion_event(
        dataset="season_priors",
        stage="import",
        rows_in=len(working),
        rows_out=len(silver_output),
        rows_rejected=len(rejected),
        source=source_key,
        details={
            "duplicate_rows_removed": duplicate_count,
            "dedupe_keys": dedupe_keys,
            "rejection_log_path": rejection_log.get("path"),
            "quarantine_path": quarantine_log.get("path"),
        },
    )

    return {
        "path": str(output_path),
        "rows_accepted": int(len(silver_output)),
        "rows_rejected": int(len(rejected)),
        "duplicate_rows_removed": duplicate_count,
        "skipped": False,
        "idempotency": idempotency,
        "rejection_log_path": rejection_log.get("path"),
        "quarantine_path": quarantine_log.get("path"),
        "ingestion_event": event,
        "normalized_columns": ordered_columns[:10],
        "preview": silver_output.head(10).fillna("").to_dict(orient="records"),
    }


def import_season_priors_text(text: str, output_path: Path = DEFAULT_SEASON_PRIORS_PATH) -> dict:
    return import_season_priors_frame(_read_delimited_text(text), output_path=output_path)


def import_season_priors_bytes(payload: bytes, output_path: Path = DEFAULT_SEASON_PRIORS_PATH) -> dict:
    return import_season_priors_frame(_read_csv_bytes(payload), output_path=output_path)


def import_prizepicks_lines_frame(frame: pd.DataFrame, output_path: Path = DEFAULT_PRIZEPICKS_LINES_PATH) -> dict:
    working = _normalize_frame_columns(frame).copy()
    source_key = f"import_prizepicks_lines:{output_path.resolve()}"
    fingerprint = compute_frame_fingerprint(working)
    idempotency = check_and_register_idempotency(
        dataset="prizepicks_lines",
        source=source_key,
        fingerprint=fingerprint,
        metadata={"rows_in": int(len(working)), "output_path": str(output_path)},
    )
    if idempotency.get("duplicate"):
        existing_rows = _rows_in_csv(output_path)
        existing_preview = (
            pd.read_csv(output_path).head(10).fillna("").to_dict(orient="records")
            if output_path.exists()
            else []
        )
        event = record_ingestion_event(
            dataset="prizepicks_lines",
            stage="import",
            rows_in=len(working),
            rows_out=existing_rows,
            rows_rejected=0,
            source=source_key,
            outcome="duplicate_skipped",
            details={"idempotency": idempotency, "output_path": str(output_path)},
        )
        return {
            "path": str(output_path),
            "rows_accepted": 0,
            "rows_rejected": 0,
            "duplicate_rows_removed": 0,
            "skipped": True,
            "skip_reason": "duplicate_payload",
            "existing_rows": existing_rows,
            "idempotency": idempotency,
            "ingestion_event": event,
            "preview": existing_preview,
        }
    write_stage_snapshot("prizepicks_lines", "bronze", working)
    lower_lookup = {str(column).strip().lower(): column for column in working.columns}

    rename_map = {}
    aliases = {
        "player": "player_name",
        "player_name": "player_name",
        "team": "team",
        "game_date": "game_date",
        "date": "game_date",
        "market": "market",
        "line": "line",
        "selection_type": "selection_type",
        "selection": "selection_type",
        "source": "source",
        "captured_at": "captured_at",
    }
    for key, value in aliases.items():
        if key in lower_lookup:
            rename_map[lower_lookup[key]] = value
    normalized = working.rename(columns=rename_map)

    required_columns = ["player_name", "game_date", "market", "line"]
    missing = [column for column in required_columns if column not in normalized.columns]
    if missing:
        raise ValueError(f"PrizePicks lines are missing required columns: {missing}")

    if "team" not in normalized.columns:
        normalized["team"] = ""
    if "selection_type" not in normalized.columns:
        normalized["selection_type"] = ""
    if "source" not in normalized.columns:
        normalized["source"] = "manual"
    if "captured_at" not in normalized.columns:
        normalized["captured_at"] = pd.Timestamp.now("UTC").isoformat()

    normalized["player_name"] = normalized["player_name"].astype(str).str.strip()
    normalized["team"] = normalized["team"].map(normalize_team_code)
    normalized["game_date"] = pd.to_datetime(normalized["game_date"], errors="coerce").dt.strftime("%Y-%m-%d")
    normalized["market"] = normalized["market"].astype(str).str.strip().str.lower()
    normalized["line"] = pd.to_numeric(normalized["line"], errors="coerce")
    normalized["selection_type"] = normalized["selection_type"].astype(str).str.strip().str.lower()
    normalized["captured_at"] = normalized["captured_at"].astype(str)
    normalized["player_key"] = normalized["player_name"].map(normalize_player_name)

    rejected_frames: list[pd.DataFrame] = []
    missing_player_mask = normalized["player_key"] == ""
    invalid_game_date_mask = normalized["game_date"].isna()
    invalid_line_mask = normalized["line"].isna()
    invalid_market_mask = ~normalized["market"].isin(PRIZEPICKS_MARKETS)
    rejected_frames.append(_tag_rejected_rows(normalized[missing_player_mask], "missing_player_name"))
    rejected_frames.append(_tag_rejected_rows(normalized[invalid_game_date_mask], "invalid_game_date"))
    rejected_frames.append(_tag_rejected_rows(normalized[invalid_line_mask], "invalid_line"))
    rejected_frames.append(_tag_rejected_rows(normalized[invalid_market_mask], "unsupported_market"))
    valid_mask = ~(missing_player_mask | invalid_game_date_mask | invalid_line_mask | invalid_market_mask)
    normalized = normalized[valid_mask].copy()
    normalized, duplicate_rows, dedupe_keys = split_duplicates_by_policy(
        "prizepicks_lines",
        normalized,
        fallback_keys=["player_key", "team", "game_date", "market"],
        keep="last",
    )
    duplicate_count = int(len(duplicate_rows))
    if duplicate_count:
        rejected_frames.append(_tag_rejected_rows(duplicate_rows, f"duplicate_on_keys:{'|'.join(dedupe_keys)}"))
    rejected = pd.concat(rejected_frames, ignore_index=True, sort=False) if rejected_frames else pd.DataFrame()

    ordered_columns = ["player_name", "team", "game_date", "market", "line", "selection_type", "source", "captured_at"]
    output_path.parent.mkdir(parents=True, exist_ok=True)
    silver_output = normalized[ordered_columns].sort_values(["game_date", "team", "player_name", "market"]).reset_index(drop=True)
    silver_output.to_csv(output_path, index=False)
    write_stage_snapshot("prizepicks_lines", "silver", silver_output)
    rejection_log = append_rejections("prizepicks_lines", rejected, source="import_prizepicks_lines")
    quarantine_log = append_quarantine("prizepicks_lines", rejected, source="import_prizepicks_lines")
    event = record_ingestion_event(
        dataset="prizepicks_lines",
        stage="import",
        rows_in=len(working),
        rows_out=len(silver_output),
        rows_rejected=len(rejected),
        source=source_key,
        details={
            "duplicate_rows_removed": duplicate_count,
            "dedupe_keys": dedupe_keys,
            "rejection_log_path": rejection_log.get("path"),
            "quarantine_path": quarantine_log.get("path"),
        },
    )

    return {
        "path": str(output_path),
        "rows_accepted": int(len(silver_output)),
        "rows_rejected": int(len(rejected)),
        "duplicate_rows_removed": duplicate_count,
        "skipped": False,
        "idempotency": idempotency,
        "rejection_log_path": rejection_log.get("path"),
        "quarantine_path": quarantine_log.get("path"),
        "ingestion_event": event,
        "preview": silver_output.head(10).fillna("").to_dict(orient="records"),
    }


def import_prizepicks_lines_text(text: str, output_path: Path = DEFAULT_PRIZEPICKS_LINES_PATH) -> dict:
    return import_prizepicks_lines_frame(_read_delimited_text(text), output_path=output_path)


def import_prizepicks_lines_bytes(payload: bytes, output_path: Path = DEFAULT_PRIZEPICKS_LINES_PATH) -> dict:
    return import_prizepicks_lines_frame(_read_csv_bytes(payload), output_path=output_path)


def import_historical_frame(frame: pd.DataFrame, output_path: Path = DEFAULT_TRAINING_UPLOAD_PATH) -> dict:
    working = _normalize_frame_columns(frame)
    source_key = f"import_historical:{output_path.resolve()}"
    fingerprint = compute_frame_fingerprint(working)
    idempotency = check_and_register_idempotency(
        dataset="training_data",
        source=source_key,
        fingerprint=fingerprint,
        metadata={"rows_in": int(len(working)), "output_path": str(output_path)},
    )
    if idempotency.get("duplicate"):
        existing_rows = _rows_in_csv(output_path)
        existing_preview = (
            pd.read_csv(output_path).head(10).fillna("").to_dict(orient="records")
            if output_path.exists()
            else []
        )
        event = record_ingestion_event(
            dataset="training_data",
            stage="import",
            rows_in=len(working),
            rows_out=existing_rows,
            rows_rejected=0,
            source=source_key,
            outcome="duplicate_skipped",
            details={"idempotency": idempotency, "output_path": str(output_path)},
        )
        return {
            "path": str(output_path),
            "rows_accepted": 0,
            "rows_rejected": 0,
            "duplicate_rows_removed": 0,
            "skipped": True,
            "skip_reason": "duplicate_payload",
            "existing_rows": existing_rows,
            "idempotency": idempotency,
            "ingestion_event": event,
            "preview": existing_preview,
        }
    write_stage_snapshot("training_data", "bronze", working)
    missing_columns = [column for column in HISTORICAL_REQUIRED_COLUMNS if column not in working.columns]
    if missing_columns:
        raise ValueError(f"Historical training data is missing required columns: {missing_columns}")

    normalized = working.copy()
    normalized["player_name"] = normalized["player_name"].astype(str).str.strip()
    normalized["game_date"] = pd.to_datetime(normalized["game_date"], errors="coerce").dt.strftime("%Y-%m-%d")
    normalized["opponent"] = normalized["opponent"].map(normalize_team_code)
    normalized["home"] = normalized["home"].apply(_coerce_home_value)
    if "team" in normalized.columns:
        normalized["team"] = normalized["team"].map(normalize_team_code)
    else:
        normalized["team"] = ""
    normalized["points"] = pd.to_numeric(normalized["points"], errors="coerce")
    normalized["rebounds"] = pd.to_numeric(normalized["rebounds"], errors="coerce")
    normalized["assists"] = pd.to_numeric(normalized["assists"], errors="coerce")
    normalized["player_key"] = normalized["player_name"].map(normalize_player_name)

    rejected_frames: list[pd.DataFrame] = []
    missing_player_mask = normalized["player_key"] == ""
    invalid_game_date_mask = normalized["game_date"].isna() | (normalized["game_date"].astype(str).str.strip() == "")
    invalid_home_mask = normalized["home"].isna()
    invalid_opponent_mask = normalized["opponent"].isna() | (normalized["opponent"].astype(str).str.strip() == "")
    invalid_target_mask = normalized[["points", "rebounds", "assists"]].isna().any(axis=1)
    rejected_frames.append(_tag_rejected_rows(normalized[missing_player_mask], "missing_player_name"))
    rejected_frames.append(_tag_rejected_rows(normalized[invalid_game_date_mask], "invalid_game_date"))
    rejected_frames.append(_tag_rejected_rows(normalized[invalid_home_mask], "invalid_home_indicator"))
    rejected_frames.append(_tag_rejected_rows(normalized[invalid_opponent_mask], "invalid_opponent"))
    rejected_frames.append(_tag_rejected_rows(normalized[invalid_target_mask], "invalid_target_values"))
    valid_mask = ~(missing_player_mask | invalid_game_date_mask | invalid_home_mask | invalid_opponent_mask | invalid_target_mask)
    normalized = normalized[valid_mask].copy()

    normalized, duplicate_rows, dedupe_keys = split_duplicates_by_policy(
        "training_data",
        normalized,
        fallback_keys=["player_key", "game_date", "opponent", "home"],
        keep="last",
    )
    duplicate_count = int(len(duplicate_rows))
    if duplicate_count:
        rejected_frames.append(_tag_rejected_rows(duplicate_rows, f"duplicate_on_keys:{'|'.join(dedupe_keys)}"))
    rejected = pd.concat(rejected_frames, ignore_index=True, sort=False) if rejected_frames else pd.DataFrame()

    output_columns = [column for column in working.columns if column in normalized.columns]
    if "team" not in output_columns:
        output_columns.append("team")
    silver_output = normalized[output_columns].copy().reset_index(drop=True)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    silver_output.to_csv(output_path, index=False)
    write_stage_snapshot("training_data", "silver", silver_output)
    rejection_log = append_rejections("training_data", rejected, source="import_historical")
    quarantine_log = append_quarantine("training_data", rejected, source="import_historical")
    event = record_ingestion_event(
        dataset="training_data",
        stage="import",
        rows_in=len(working),
        rows_out=len(silver_output),
        rows_rejected=len(rejected),
        source=source_key,
        details={
            "duplicate_rows_removed": duplicate_count,
            "dedupe_keys": dedupe_keys,
            "rejection_log_path": rejection_log.get("path"),
            "quarantine_path": quarantine_log.get("path"),
        },
    )

    return {
        "path": str(output_path),
        "rows_accepted": int(len(silver_output)),
        "rows_rejected": int(len(rejected)),
        "duplicate_rows_removed": duplicate_count,
        "skipped": False,
        "idempotency": idempotency,
        "rejection_log_path": rejection_log.get("path"),
        "quarantine_path": quarantine_log.get("path"),
        "ingestion_event": event,
        "preview": silver_output.head(10).fillna("").to_dict(orient="records"),
    }


def import_historical_text(text: str, output_path: Path = DEFAULT_TRAINING_UPLOAD_PATH) -> dict:
    return import_historical_frame(_read_delimited_text(text), output_path=output_path)


def import_historical_bytes(payload: bytes, output_path: Path = DEFAULT_TRAINING_UPLOAD_PATH) -> dict:
    return import_historical_frame(_read_csv_bytes(payload), output_path=output_path)


def fetch_current_season_history(
    output_path: Path = DEFAULT_TRAINING_UPLOAD_PATH,
    through_date: str | None = None,
    include_preseason: bool = False,
) -> dict:
    schedule_payload = fetch_json(SCHEDULE_URL)
    game_rows = []
    for game_date_entry in schedule_payload["leagueSchedule"]["gameDates"]:
        for game in game_date_entry.get("games", []):
            if int(game.get("gameStatus", 0)) != 3:
                continue
            if not include_preseason and str(game.get("gameLabel", "")).strip().lower() == "preseason":
                continue
            game_date = str(game.get("gameDateUTC") or game.get("gameDateEst") or "")[:10]
            if through_date and game_date > through_date:
                continue
            game_rows.append({"game_id": game["gameId"], "game_date": game_date})

    completed_frames = []
    for game in game_rows:
        payload = fetch_json(BOXSCORE_URL_TEMPLATE.format(game_id=game["game_id"]))
        completed_frames.append(_boxscore_players_to_rows(payload))

    if not completed_frames:
        raise ValueError("No completed games were found for the requested bootstrap window.")

    combined = pd.concat(completed_frames, ignore_index=True, sort=False)
    source_key = (
        f"fetch_current_season_history:{through_date or 'latest'}:{int(bool(include_preseason))}:{output_path.resolve()}"
    )
    fingerprint = compute_frame_fingerprint(combined)
    idempotency = check_and_register_idempotency(
        dataset="training_data",
        source=source_key,
        fingerprint=fingerprint,
        metadata={
            "rows_in": int(len(combined)),
            "games_loaded": int(len(game_rows)),
            "through_date": through_date,
            "include_preseason": bool(include_preseason),
        },
    )
    if idempotency.get("duplicate"):
        rows_in_file = _rows_in_csv(output_path)
        event = record_ingestion_event(
            dataset="training_data",
            stage="bootstrap_fetch",
            rows_in=len(combined),
            rows_out=rows_in_file,
            rows_rejected=0,
            source=source_key,
            outcome="duplicate_skipped",
            details={
                "rows_appended": 0,
                "games_loaded": int(len(game_rows)),
                "through_date": through_date,
                "include_preseason": bool(include_preseason),
                "idempotency": idempotency,
            },
        )
        return {
            "path": str(output_path),
            "rows_in_file": rows_in_file,
            "rows_appended": 0,
            "games_loaded": int(len(game_rows)),
            "through_date": through_date,
            "include_preseason": bool(include_preseason),
            "skipped": True,
            "skip_reason": "duplicate_payload",
            "idempotency": idempotency,
            "ingestion_event": event,
        }

    write_stage_snapshot("training_data", "bronze", combined)
    appended = _append_completed_rows(output_path, combined)
    history = pd.read_csv(output_path)
    write_stage_snapshot("training_data", "silver", history)
    event = record_ingestion_event(
        dataset="training_data",
        stage="bootstrap_fetch",
        rows_in=len(combined),
        rows_out=len(history),
        rows_rejected=max(0, int(len(combined) - appended)),
        source="fetch_current_season_history",
        details={"rows_appended": int(appended), "games_loaded": int(len(game_rows)), "through_date": through_date},
    )
    return {
        "path": str(output_path),
        "rows_in_file": int(len(history)),
        "rows_appended": int(appended),
        "games_loaded": int(len(game_rows)),
        "through_date": through_date,
        "include_preseason": bool(include_preseason),
        "skipped": False,
        "idempotency": idempotency,
        "ingestion_event": event,
    }


def parse_json_body(body: bytes) -> dict:
    if not body:
        return {}
    return json.loads(body.decode("utf-8"))
