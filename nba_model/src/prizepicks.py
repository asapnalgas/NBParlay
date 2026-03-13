from __future__ import annotations

import csv
import json
from pathlib import Path

import pandas as pd

try:
    from .features import DEFAULT_PROJECT_DIR, csv_data_row_count
    from .player_matching import normalize_player_name, normalize_team_code
except ImportError:
    from features import DEFAULT_PROJECT_DIR, csv_data_row_count
    from player_matching import normalize_player_name, normalize_team_code


DEFAULT_PRIZEPICKS_LINES_PATH = DEFAULT_PROJECT_DIR / "data" / "prizepicks_lines.csv"
DEFAULT_PRIZEPICKS_EDGES_PATH = DEFAULT_PROJECT_DIR / "models" / "prizepicks_edges.csv"
DEFAULT_PREDICTIONS_PATH = DEFAULT_PROJECT_DIR / "models" / "predictions.csv"
DEFAULT_METRICS_PATH = DEFAULT_PROJECT_DIR / "models" / "engine_metrics.json"
DEFAULT_BENCHMARK_REPORT_PATH = DEFAULT_PROJECT_DIR / "models" / "rotowire_benchmark_report.json"

EDGE_THRESHOLDS = {
    "points": 1.5,
    "rebounds": 1.0,
    "assists": 1.0,
    "pra": 2.0,
    "three_points_made": 0.8,
    "steals": 0.45,
    "blocks": 0.45,
    "turnovers": 0.6,
    "points_rebounds": 1.8,
    "points_assists": 1.8,
    "rebounds_assists": 1.5,
    "steals_blocks": 0.7,
}

MARKET_TO_COLUMN = {
    "points": "predicted_points",
    "rebounds": "predicted_rebounds",
    "assists": "predicted_assists",
    "pra": "predicted_pra",
    "three_points_made": "predicted_three_points_made",
    "steals": "predicted_steals",
    "blocks": "predicted_blocks",
    "turnovers": "predicted_turnovers",
    "points_rebounds": "predicted_points_rebounds",
    "points_assists": "predicted_points_assists",
    "rebounds_assists": "predicted_rebounds_assists",
    "steals_blocks": "predicted_steals_blocks",
}
MARKET_TO_ANCHOR_UNCERTAINTY_COLUMN = {
    "points": "pregame_anchor_uncertainty_points",
    "rebounds": "pregame_anchor_uncertainty_rebounds",
    "assists": "pregame_anchor_uncertainty_assists",
    "pra": "pregame_anchor_uncertainty_pra",
}
MARKET_TO_LINE_STDDEV_COLUMN = {
    "points": "line_points_stddev",
    "rebounds": "line_rebounds_stddev",
    "assists": "line_assists_stddev",
    "pra": "line_pra_stddev",
}
MARKET_TO_LINE_BOOKS_COLUMN = {
    "points": "line_points_books_count",
    "rebounds": "line_rebounds_books_count",
    "assists": "line_assists_books_count",
    "pra": "line_pra_books_count",
}
MARKET_TO_LINE_AGE_COLUMN = {
    "points": "line_points_snapshot_age_minutes",
    "rebounds": "line_rebounds_snapshot_age_minutes",
    "assists": "line_assists_snapshot_age_minutes",
    "pra": "line_pra_snapshot_age_minutes",
}
CONFIDENCE_TO_ERROR_PCT = {
    "high_confidence": 10.0,
    "medium_confidence": 17.0,
    "low_confidence": 26.0,
}

EDGE_MAE_MULTIPLIER = {
    "points": 0.55,
    "rebounds": 0.65,
    "assists": 0.70,
    "pra": 0.50,
    "three_points_made": 0.75,
    "steals": 0.85,
    "blocks": 0.85,
    "turnovers": 0.75,
    "points_rebounds": 0.55,
    "points_assists": 0.55,
    "rebounds_assists": 0.60,
    "steals_blocks": 0.75,
}
DEFAULT_TARGET_MAE = {
    "points": 4.8,
    "rebounds": 2.0,
    "assists": 1.6,
    "three_points_made": 1.1,
    "steals": 0.65,
    "blocks": 0.55,
    "turnovers": 0.95,
    "points_rebounds": 5.8,
    "points_assists": 5.4,
    "rebounds_assists": 3.2,
    "steals_blocks": 0.95,
}
MIN_HISTORY_GAMES_FOR_EDGE = 5
MIN_EXPECTED_MINUTES_FOR_EDGE = 18.0
MIN_STARTER_PROBABILITY_FOR_EDGE = 0.52
MIN_EXPECTED_MINUTES_CONFIDENCE_FOR_EDGE = 0.32
MAX_PROJECTION_ERROR_PCT_FOR_EDGE = 32.0
MIN_PREGAME_ANCHOR_STRENGTH_FOR_EDGE = 1.0
MIN_MARKET_BOOKS_FOR_EDGE = 2.0
MAX_MARKET_SNAPSHOT_AGE_MINUTES = 420.0
MIN_EDGE_TO_UNCERTAINTY_FOR_PICK = 1.5


def _load_edge_hardening_profile() -> dict[str, float]:
    profile = {
        "min_history_games_for_edge": float(MIN_HISTORY_GAMES_FOR_EDGE),
        "min_expected_minutes_for_edge": float(MIN_EXPECTED_MINUTES_FOR_EDGE),
        "min_starter_probability_for_edge": float(MIN_STARTER_PROBABILITY_FOR_EDGE),
        "min_expected_minutes_confidence_for_edge": float(MIN_EXPECTED_MINUTES_CONFIDENCE_FOR_EDGE),
        "max_projection_error_pct_for_edge": float(MAX_PROJECTION_ERROR_PCT_FOR_EDGE),
        "min_pregame_anchor_strength_for_edge": float(MIN_PREGAME_ANCHOR_STRENGTH_FOR_EDGE),
        "min_market_books_for_edge": float(MIN_MARKET_BOOKS_FOR_EDGE),
        "max_market_snapshot_age_minutes": float(MAX_MARKET_SNAPSHOT_AGE_MINUTES),
        "min_edge_to_uncertainty_for_pick": float(MIN_EDGE_TO_UNCERTAINTY_FOR_PICK),
    }
    if not DEFAULT_BENCHMARK_REPORT_PATH.exists():
        return profile

    try:
        benchmark = json.loads(DEFAULT_BENCHMARK_REPORT_PATH.read_text(encoding="utf-8"))
    except Exception:
        return profile

    if not isinstance(benchmark, dict):
        return profile
    overall = benchmark.get("overall", {})
    if not isinstance(overall, dict):
        return profile

    rows = _safe_int(overall.get("rows"), _safe_int(benchmark.get("rows_evaluated"), 0))
    hit_rate = _safe_float(overall.get("hit_rate"), 0.0)
    clv_mean = _safe_float(overall.get("clv_mean"), 0.0)
    projection_error_vs_line = _safe_float(overall.get("projection_error_minus_line_error"), 0.0)

    if rows >= 150:
        if hit_rate > 0 and hit_rate < 0.53:
            profile["min_edge_to_uncertainty_for_pick"] += 0.3
            profile["max_projection_error_pct_for_edge"] -= 3.0
            profile["min_expected_minutes_confidence_for_edge"] += 0.05
            profile["min_history_games_for_edge"] += 1.0
        if clv_mean < 0:
            profile["min_market_books_for_edge"] += 1.0
            profile["max_market_snapshot_age_minutes"] = min(profile["max_market_snapshot_age_minutes"], 240.0)
            profile["min_edge_to_uncertainty_for_pick"] += 0.2
        if projection_error_vs_line > 0:
            profile["max_projection_error_pct_for_edge"] -= 2.0
            profile["min_expected_minutes_for_edge"] += 1.0

    profile["min_history_games_for_edge"] = float(max(4.0, min(12.0, profile["min_history_games_for_edge"])))
    profile["min_expected_minutes_for_edge"] = float(max(14.0, min(28.0, profile["min_expected_minutes_for_edge"])))
    profile["min_starter_probability_for_edge"] = float(max(0.45, min(0.8, profile["min_starter_probability_for_edge"])))
    profile["min_expected_minutes_confidence_for_edge"] = float(
        max(0.2, min(0.75, profile["min_expected_minutes_confidence_for_edge"]))
    )
    profile["max_projection_error_pct_for_edge"] = float(max(12.0, min(40.0, profile["max_projection_error_pct_for_edge"])))
    profile["min_pregame_anchor_strength_for_edge"] = float(
        max(0.5, min(3.0, profile["min_pregame_anchor_strength_for_edge"]))
    )
    profile["min_market_books_for_edge"] = float(max(1.0, min(5.0, profile["min_market_books_for_edge"])))
    profile["max_market_snapshot_age_minutes"] = float(max(60.0, min(420.0, profile["max_market_snapshot_age_minutes"])))
    profile["min_edge_to_uncertainty_for_pick"] = float(
        max(1.35, min(2.8, profile["min_edge_to_uncertainty_for_pick"]))
    )
    return profile


def _safe_float(value: object, default: float = 0.0) -> float:
    parsed = pd.to_numeric(pd.Series([value]), errors="coerce").iloc[0]
    if pd.isna(parsed):
        return default
    return float(parsed)


def _safe_int(value: object, default: int = 0) -> int:
    parsed = pd.to_numeric(pd.Series([value]), errors="coerce").iloc[0]
    if pd.isna(parsed):
        return default
    return int(parsed)
UNAVAILABLE_PATTERN = r"(?:out|ofs|suspend|suspended|inactive)"


def _load_target_mae() -> dict[str, float]:
    fallback = dict(DEFAULT_TARGET_MAE)
    if not DEFAULT_METRICS_PATH.exists():
        return fallback
    try:
        metrics = pd.read_json(DEFAULT_METRICS_PATH, typ="series")
        per_target = metrics.get("per_target_metrics", {})
        if isinstance(per_target, dict):
            for target in fallback:
                target_payload = per_target.get(target, {})
                if isinstance(target_payload, dict):
                    parsed = pd.to_numeric(pd.Series([target_payload.get("mae")]), errors="coerce").iloc[0]
                    if pd.notna(parsed) and float(parsed) > 0:
                        fallback[target] = float(parsed)
    except Exception:
        return fallback
    return fallback


def _uncertainty_band(row: pd.Series) -> float:
    market = str(row.get("market", "")).lower()
    projection = pd.to_numeric(pd.Series([row.get("projection")]), errors="coerce").iloc[0]
    if pd.isna(projection):
        projection = 0.0
    base_threshold = float(EDGE_THRESHOLDS.get(market, 1.0))
    fallback_error_pct = CONFIDENCE_TO_ERROR_PCT.get(str(row.get("confidence_flag", "")).lower(), 26.0)
    projection_error_pct = pd.to_numeric(
        pd.Series([row.get("projection_error_pct_estimate")]),
        errors="coerce",
    ).iloc[0]
    if pd.isna(projection_error_pct):
        projection_error_pct = fallback_error_pct

    anchor_uncertainty_column = MARKET_TO_ANCHOR_UNCERTAINTY_COLUMN.get(market, "")
    line_stddev_column = MARKET_TO_LINE_STDDEV_COLUMN.get(market, "")
    anchor_uncertainty = pd.to_numeric(pd.Series([row.get(anchor_uncertainty_column)]), errors="coerce").iloc[0]
    line_stddev = pd.to_numeric(pd.Series([row.get(line_stddev_column)]), errors="coerce").iloc[0]
    if pd.isna(anchor_uncertainty):
        anchor_uncertainty = 0.0
    if pd.isna(line_stddev):
        line_stddev = 0.0

    model_component = abs(float(projection)) * max(0.0, float(projection_error_pct)) / 100.0
    blended = (model_component * 0.65) + (float(anchor_uncertainty) * 0.55) + (float(line_stddev) * 0.4)
    return float(max(base_threshold, blended))


def _csv_columns(path: Path) -> list[str]:
    try:
        with path.open("r", encoding="utf-8", errors="ignore", newline="") as input_file:
            return [str(column) for column in next(csv.reader(input_file), [])]
    except OSError:
        return []


def load_prizepicks_lines(path: Path = DEFAULT_PRIZEPICKS_LINES_PATH, include_preview: bool = True) -> dict | None:
    if not path.exists():
        return None
    if include_preview:
        frame = pd.read_csv(path, nrows=25)
        columns = list(frame.columns)
        preview = frame.head(25).fillna("").to_dict(orient="records")
    else:
        columns = _csv_columns(path)
        preview = []
    return {
        "path": str(path),
        "rows": int(csv_data_row_count(path)),
        "columns": columns,
        "preview": preview,
    }


def load_prizepicks_edges(path: Path = DEFAULT_PRIZEPICKS_EDGES_PATH, include_preview: bool = True) -> dict | None:
    if not path.exists():
        return None
    if include_preview:
        frame = pd.read_csv(path, nrows=25)
        columns = list(frame.columns)
        preview = frame.head(25).fillna("").to_dict(orient="records")
    else:
        columns = _csv_columns(path)
        preview = []
    return {
        "path": str(path),
        "rows": int(csv_data_row_count(path)),
        "columns": columns,
        "preview": preview,
    }


def generate_prizepicks_edges(
    predictions_path: Path = DEFAULT_PREDICTIONS_PATH,
    lines_path: Path = DEFAULT_PRIZEPICKS_LINES_PATH,
    output_path: Path = DEFAULT_PRIZEPICKS_EDGES_PATH,
    slate_date: str | None = None,
) -> dict:
    if not predictions_path.exists():
        raise FileNotFoundError(f"Predictions file not found: {predictions_path}")
    if not lines_path.exists():
        raise FileNotFoundError(f"PrizePicks lines file not found: {lines_path}")

    hardening = _load_edge_hardening_profile()
    min_history_games_for_edge = int(round(_safe_float(hardening.get("min_history_games_for_edge"), MIN_HISTORY_GAMES_FOR_EDGE)))
    min_expected_minutes_for_edge = _safe_float(hardening.get("min_expected_minutes_for_edge"), MIN_EXPECTED_MINUTES_FOR_EDGE)
    min_starter_probability_for_edge = _safe_float(
        hardening.get("min_starter_probability_for_edge"),
        MIN_STARTER_PROBABILITY_FOR_EDGE,
    )
    min_expected_minutes_confidence_for_edge = _safe_float(
        hardening.get("min_expected_minutes_confidence_for_edge"),
        MIN_EXPECTED_MINUTES_CONFIDENCE_FOR_EDGE,
    )
    max_projection_error_pct_for_edge = _safe_float(
        hardening.get("max_projection_error_pct_for_edge"),
        MAX_PROJECTION_ERROR_PCT_FOR_EDGE,
    )
    min_pregame_anchor_strength_for_edge = _safe_float(
        hardening.get("min_pregame_anchor_strength_for_edge"),
        MIN_PREGAME_ANCHOR_STRENGTH_FOR_EDGE,
    )
    min_market_books_for_edge = _safe_float(hardening.get("min_market_books_for_edge"), MIN_MARKET_BOOKS_FOR_EDGE)
    max_market_snapshot_age_minutes = _safe_float(
        hardening.get("max_market_snapshot_age_minutes"),
        MAX_MARKET_SNAPSHOT_AGE_MINUTES,
    )
    min_edge_to_uncertainty_for_pick = _safe_float(
        hardening.get("min_edge_to_uncertainty_for_pick"),
        MIN_EDGE_TO_UNCERTAINTY_FOR_PICK,
    )

    predictions = pd.read_csv(predictions_path)
    lines = pd.read_csv(lines_path)
    if predictions.empty or lines.empty:
        output_path.parent.mkdir(parents=True, exist_ok=True)
        pd.DataFrame(
            columns=[
                "player_name",
                "team",
                "game_date",
                "market",
                "line",
                "projection",
                "edge",
                "edge_threshold",
                "uncertainty_band",
                "effective_threshold",
                "edge_to_uncertainty",
                "required_edge_to_uncertainty",
                "recommendation",
                "confidence_flag",
                "historical_games_used",
                "season_priors_available",
                "expected_minutes_confidence",
                "projection_error_pct_estimate",
                "pregame_anchor_strength",
                "market_books",
                "line_snapshot_age_minutes",
                "eligible_for_pick",
                "eligibility_reason",
            ]
        ).to_csv(output_path, index=False)
        return {
            "path": str(output_path),
            "rows": 0,
            "matched_rows": 0,
            "unmatched_rows": int(len(lines)),
            "preview": [],
        }

    predictions = predictions.copy()
    lines = lines.copy()
    predictions["game_date"] = pd.to_datetime(predictions["game_date"], errors="coerce").dt.strftime("%Y-%m-%d")
    lines["game_date"] = pd.to_datetime(lines["game_date"], errors="coerce").dt.strftime("%Y-%m-%d")
    market_aliases = {
        "pts": "points",
        "points": "points",
        "reb": "rebounds",
        "rebs": "rebounds",
        "rebounds": "rebounds",
        "ast": "assists",
        "assists": "assists",
        "pra": "pra",
        "3pm": "three_points_made",
        "threes": "three_points_made",
        "three_points_made": "three_points_made",
        "stl": "steals",
        "steals": "steals",
        "blk": "blocks",
        "blocks": "blocks",
        "tov": "turnovers",
        "turnovers": "turnovers",
        "pr": "points_rebounds",
        "points_rebounds": "points_rebounds",
        "pa": "points_assists",
        "points_assists": "points_assists",
        "ra": "rebounds_assists",
        "rebounds_assists": "rebounds_assists",
        "sb": "steals_blocks",
        "stocks": "steals_blocks",
        "steals_blocks": "steals_blocks",
    }
    lines["market"] = (
        lines.get("market", pd.Series("", index=lines.index))
        .fillna("")
        .astype(str)
        .str.strip()
        .str.lower()
        .str.replace(r"[^a-z0-9_]+", "_", regex=True)
        .map(lambda value: market_aliases.get(value, value))
    )
    if slate_date:
        slate_date = pd.to_datetime(slate_date, errors="raise").strftime("%Y-%m-%d")
        predictions = predictions[predictions["game_date"] == slate_date].copy()
        lines = lines[lines["game_date"] == slate_date].copy()

    predictions["player_key"] = predictions["player_name"].map(normalize_player_name)
    predictions["team_key"] = predictions["team"].map(normalize_team_code) if "team" in predictions.columns else ""
    lines["player_key"] = lines["player_name"].map(normalize_player_name)
    lines["team_key"] = lines["team"].map(normalize_team_code) if "team" in lines.columns else ""

    merged = lines.merge(
        predictions,
        on=["player_key", "team_key", "game_date"],
        how="left",
        suffixes=("_line", ""),
    )

    merged["predicted_points_rebounds"] = (
        pd.to_numeric(merged.get("predicted_points"), errors="coerce")
        + pd.to_numeric(merged.get("predicted_rebounds"), errors="coerce")
    )
    merged["predicted_points_assists"] = (
        pd.to_numeric(merged.get("predicted_points"), errors="coerce")
        + pd.to_numeric(merged.get("predicted_assists"), errors="coerce")
    )
    merged["predicted_rebounds_assists"] = (
        pd.to_numeric(merged.get("predicted_rebounds"), errors="coerce")
        + pd.to_numeric(merged.get("predicted_assists"), errors="coerce")
    )
    merged["predicted_steals_blocks"] = (
        pd.to_numeric(merged.get("predicted_steals"), errors="coerce")
        + pd.to_numeric(merged.get("predicted_blocks"), errors="coerce")
    )

    merged["projection_column"] = merged["market"].map(MARKET_TO_COLUMN)
    merged["projection"] = pd.NA
    for market, column in MARKET_TO_COLUMN.items():
        market_mask = merged["market"] == market
        if column in merged.columns:
            merged.loc[market_mask, "projection"] = merged.loc[market_mask, column]

    merged["edge"] = pd.to_numeric(merged["projection"], errors="coerce") - pd.to_numeric(merged["line"], errors="coerce")
    target_mae = _load_target_mae()

    def _dynamic_threshold(row: pd.Series) -> float:
        market = str(row.get("market", "")).lower()
        base_threshold = float(EDGE_THRESHOLDS.get(market, 1.0))
        if market == "pra":
            pra_mae = (
                target_mae.get("points", DEFAULT_TARGET_MAE["points"])
                + target_mae.get("rebounds", DEFAULT_TARGET_MAE["rebounds"])
                + target_mae.get("assists", DEFAULT_TARGET_MAE["assists"])
            )
            mae_component = pra_mae * EDGE_MAE_MULTIPLIER.get("pra", 0.5)
        else:
            mae_component = target_mae.get(market, base_threshold) * EDGE_MAE_MULTIPLIER.get(market, 0.6)
        return float(max(base_threshold, mae_component))

    merged["edge_threshold"] = merged.apply(_dynamic_threshold, axis=1)
    merged["uncertainty_band"] = merged.apply(_uncertainty_band, axis=1)
    merged["effective_threshold"] = merged[["edge_threshold", "uncertainty_band"]].max(axis=1)
    merged["edge_to_uncertainty"] = (
        pd.to_numeric(merged["edge"], errors="coerce").abs()
        / pd.to_numeric(merged["uncertainty_band"], errors="coerce").replace(0.0, pd.NA)
    )

    def _series_or_default(column: str, default: float = 0.0) -> pd.Series:
        if column in merged.columns:
            return pd.to_numeric(merged[column], errors="coerce")
        return pd.Series(default, index=merged.index, dtype=float)

    historical_games = _series_or_default("historical_games_used").fillna(0)
    expected_minutes = _series_or_default("expected_minutes", default=float("nan"))
    if expected_minutes.isna().all() and "predicted_minutes" in merged.columns:
        expected_minutes = pd.to_numeric(merged["predicted_minutes"], errors="coerce")
    expected_minutes = expected_minutes.fillna(24.0)
    expected_minutes_confidence = _series_or_default("expected_minutes_confidence", default=float("nan")).fillna(0.0)
    starter_signal = _series_or_default("starter", default=float("nan"))
    starter_probability = _series_or_default("starter_probability", default=float("nan"))
    lineup_status_confidence = _series_or_default("lineup_status_confidence", default=float("nan")).fillna(0.0)
    if starter_signal.notna().any():
        starter_ready = starter_signal.fillna(starter_probability).fillna(0).ge(0.5)
    elif starter_probability.notna().any():
        starter_ready = starter_probability.fillna(0).ge(min_starter_probability_for_edge)
    else:
        starter_ready = expected_minutes.ge(min_expected_minutes_for_edge)
    starter_quality_ready = (
        starter_ready
        | starter_probability.fillna(0.0).ge(min_starter_probability_for_edge)
        | lineup_status_confidence.ge(0.75)
        | expected_minutes.ge(30.0)
    )
    status_text = (
        merged.get("injury_status", pd.Series("", index=merged.index)).fillna("").astype(str)
        + " "
        + merged.get("health_status", pd.Series("", index=merged.index)).fillna("").astype(str)
        + " "
        + merged.get("suspension_status", pd.Series("", index=merged.index)).fillna("").astype(str)
    ).str.lower()
    unavailable = status_text.str.contains(UNAVAILABLE_PATTERN, regex=True)
    merged["historical_games_used"] = historical_games
    merged["expected_minutes"] = expected_minutes
    merged["starter_probability"] = starter_probability
    merged["lineup_status_confidence"] = lineup_status_confidence

    fallback_projection_error_pct = (
        merged.get("confidence_flag", pd.Series("", index=merged.index))
        .fillna("")
        .astype(str)
        .str.lower()
        .map(CONFIDENCE_TO_ERROR_PCT)
        .fillna(CONFIDENCE_TO_ERROR_PCT["low_confidence"])
    )
    projection_error_pct = _series_or_default("projection_error_pct_estimate", default=float("nan")).where(
        lambda value: value.notna(),
        fallback_projection_error_pct,
    )
    pregame_anchor_strength = _series_or_default("pregame_anchor_strength", default=float("nan")).fillna(0.0)
    market_books = pd.Series(float("nan"), index=merged.index, dtype=float)
    market_snapshot_age = pd.Series(float("nan"), index=merged.index, dtype=float)
    for market_name, books_column in MARKET_TO_LINE_BOOKS_COLUMN.items():
        if books_column not in merged.columns:
            continue
        mask = merged["market"] == market_name
        market_books.loc[mask] = pd.to_numeric(merged.loc[mask, books_column], errors="coerce")
    for market_name, age_column in MARKET_TO_LINE_AGE_COLUMN.items():
        if age_column not in merged.columns:
            continue
        mask = merged["market"] == market_name
        market_snapshot_age.loc[mask] = pd.to_numeric(merged.loc[mask, age_column], errors="coerce")

    anchor_ready = pregame_anchor_strength.ge(min_pregame_anchor_strength_for_edge)
    books_ready = market_books.isna() | market_books.ge(min_market_books_for_edge)
    snapshot_fresh = market_snapshot_age.isna() | market_snapshot_age.le(max_market_snapshot_age_minutes)
    projection_quality_ready = projection_error_pct.le(max_projection_error_pct_for_edge)
    minutes_confidence_ready = (
        expected_minutes_confidence.ge(min_expected_minutes_confidence_for_edge)
        | expected_minutes.ge(30.0)
        | starter_ready
    )

    merged["eligible_for_pick"] = (
        historical_games.ge(min_history_games_for_edge)
        & expected_minutes.ge(min_expected_minutes_for_edge)
        & starter_quality_ready
        & minutes_confidence_ready
        & projection_quality_ready
        & anchor_ready
        & books_ready
        & snapshot_fresh
        & ~unavailable
    )
    merged["edge_ratio_gate_pass"] = (
        pd.to_numeric(merged["edge_to_uncertainty"], errors="coerce").fillna(0.0)
        .ge(min_edge_to_uncertainty_for_pick)
    )
    merged["eligible_for_pick"] = merged["eligible_for_pick"] & merged["edge_ratio_gate_pass"]

    def _eligibility_reason(row: pd.Series) -> str:
        if bool(row.get("eligible_for_pick", False)):
            return "eligible"
        if pd.isna(pd.to_numeric(row.get("projection"), errors="coerce")):
            return "unmatched_projection"
        if bool(row.get("_unavailable_flag", False)):
            return "player_unavailable"
        if not bool(row.get("_edge_ratio_gate_pass", False)):
            return "insufficient_edge_ratio"
        if pd.to_numeric(row.get("historical_games_used"), errors="coerce") < min_history_games_for_edge:
            return "insufficient_history"
        if pd.to_numeric(row.get("expected_minutes"), errors="coerce") < min_expected_minutes_for_edge:
            return "low_expected_minutes"
        if (
            pd.to_numeric(row.get("expected_minutes_confidence"), errors="coerce")
            < min_expected_minutes_confidence_for_edge
        ):
            return "low_minutes_confidence"
        if not bool(row.get("_starter_ready", True)):
            return "not_likely_starter"
        if (
            pd.to_numeric(row.get("projection_error_pct_estimate"), errors="coerce")
            > max_projection_error_pct_for_edge
        ):
            return "projection_uncertainty"
        if (
            pd.to_numeric(row.get("pregame_anchor_strength"), errors="coerce")
            < min_pregame_anchor_strength_for_edge
        ):
            return "weak_market_anchor"
        books_value = pd.to_numeric(row.get("_market_books"), errors="coerce")
        if pd.notna(books_value) and books_value < min_market_books_for_edge:
            return "thin_market_anchor"
        age_value = pd.to_numeric(row.get("_market_snapshot_age"), errors="coerce")
        if pd.notna(age_value) and age_value > max_market_snapshot_age_minutes:
            return "stale_market_snapshot"
        return "quality_gate"

    merged["_starter_ready"] = starter_quality_ready
    merged["_unavailable_flag"] = unavailable
    merged["_edge_ratio_gate_pass"] = merged["edge_ratio_gate_pass"]
    merged["_market_books"] = market_books
    merged["_market_snapshot_age"] = market_snapshot_age
    merged["expected_minutes_confidence"] = expected_minutes_confidence
    merged["projection_error_pct_estimate"] = projection_error_pct
    merged["pregame_anchor_strength"] = pregame_anchor_strength
    merged["eligibility_reason"] = merged.apply(_eligibility_reason, axis=1)

    def classify(row: pd.Series) -> str:
        threshold = pd.to_numeric(row.get("effective_threshold"), errors="coerce")
        if pd.isna(threshold) or float(threshold) <= 0:
            threshold = EDGE_THRESHOLDS.get(str(row.get("market", "")).lower(), 1.0)
        edge = pd.to_numeric(row.get("edge"), errors="coerce")
        edge_to_uncertainty = pd.to_numeric(row.get("edge_to_uncertainty"), errors="coerce")
        if pd.isna(edge):
            return "Unmatched"
        if not bool(row.get("eligible_for_pick", False)):
            return "Pass"
        if pd.isna(edge_to_uncertainty) or float(edge_to_uncertainty) < min_edge_to_uncertainty_for_pick:
            return "Pass"
        if edge >= threshold:
            return "Higher"
        if edge <= -threshold:
            return "Lower"
        return "Pass"

    merged["recommendation"] = merged.apply(classify, axis=1)

    result = pd.DataFrame(
        {
            "player_name": merged["player_name_line"].combine_first(merged.get("player_name")),
            "team": merged["team_line"].combine_first(merged.get("team")),
            "game_date": merged["game_date"],
            "market": merged["market"],
            "line": merged["line"],
            "projection": merged["projection"],
            "edge": merged["edge"],
            "edge_threshold": merged["edge_threshold"],
            "uncertainty_band": merged["uncertainty_band"],
            "effective_threshold": merged["effective_threshold"],
            "edge_to_uncertainty": merged["edge_to_uncertainty"],
            "required_edge_to_uncertainty": min_edge_to_uncertainty_for_pick,
            "recommendation": merged["recommendation"],
            "confidence_flag": merged.get("confidence_flag", pd.Series("", index=merged.index)),
            "historical_games_used": merged.get("historical_games_used", pd.Series(0, index=merged.index)),
            "season_priors_available": merged.get("season_priors_available", pd.Series(False, index=merged.index)),
            "expected_minutes_confidence": merged.get(
                "expected_minutes_confidence",
                pd.Series(float("nan"), index=merged.index),
            ),
            "projection_error_pct_estimate": merged.get(
                "projection_error_pct_estimate",
                pd.Series(float("nan"), index=merged.index),
            ),
            "pregame_anchor_strength": merged.get(
                "pregame_anchor_strength",
                pd.Series(float("nan"), index=merged.index),
            ),
            "market_books": merged.get("_market_books", pd.Series(float("nan"), index=merged.index)),
            "line_snapshot_age_minutes": merged.get(
                "_market_snapshot_age",
                pd.Series(float("nan"), index=merged.index),
            ),
            "eligible_for_pick": merged.get("eligible_for_pick", pd.Series(False, index=merged.index)),
            "eligibility_reason": merged.get("eligibility_reason", pd.Series("", index=merged.index)),
        }
    )
    result = result.sort_values(["game_date", "recommendation", "edge"], ascending=[True, True, False])
    output_path.parent.mkdir(parents=True, exist_ok=True)
    result.to_csv(output_path, index=False)

    unmatched_rows = int((result["recommendation"] == "Unmatched").sum())
    matched_rows = int(len(result) - unmatched_rows)
    return {
        "path": str(output_path),
        "rows": int(len(result)),
        "matched_rows": matched_rows,
        "unmatched_rows": unmatched_rows,
        "hardening": hardening,
        "preview": result.head(25).fillna("").to_dict(orient="records"),
    }
