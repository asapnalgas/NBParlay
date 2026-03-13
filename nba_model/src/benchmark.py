from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

try:
    from .features import DEFAULT_PROJECT_DIR, DEFAULT_TRAINING_UPLOAD_PATH
    from .prizepicks import EDGE_THRESHOLDS
except ImportError:
    from features import DEFAULT_PROJECT_DIR, DEFAULT_TRAINING_UPLOAD_PATH
    from prizepicks import EDGE_THRESHOLDS


DEFAULT_PREDICTIONS_PATH = DEFAULT_PROJECT_DIR / "models" / "predictions.csv"
DEFAULT_SNAPSHOT_PATH = DEFAULT_PROJECT_DIR / "models" / "rotowire_benchmark_snapshots.csv"
DEFAULT_REPORT_PATH = DEFAULT_PROJECT_DIR / "models" / "rotowire_benchmark_report.json"
DEFAULT_JOIN_PATH = DEFAULT_PROJECT_DIR / "models" / "rotowire_benchmark_joined.csv"

MARKET_TO_PREDICTION_COLUMN = {
    "points": "predicted_points",
    "rebounds": "predicted_rebounds",
    "assists": "predicted_assists",
    "pra": "predicted_pra",
}
MARKET_TO_LINE_COLUMN = {
    "points": "line_points",
    "rebounds": "line_rebounds",
    "assists": "line_assists",
    "pra": "line_pra",
}
MARKET_TO_ACTUAL_COLUMNS = {
    "points": ("points",),
    "rebounds": ("rebounds",),
    "assists": ("assists",),
    "pra": ("points", "rebounds", "assists"),
}


def _safe_float(value: object, default: float = 0.0) -> float:
    parsed = pd.to_numeric(pd.Series([value]), errors="coerce").iloc[0]
    if pd.isna(parsed):
        return default
    return float(parsed)


def _now_iso_utc() -> str:
    return datetime.now(timezone.utc).isoformat()


def _confidence_fallback_error_pct(confidence_flag: str) -> float:
    normalized = str(confidence_flag or "").strip().lower()
    if normalized == "high_confidence":
        return 10.0
    if normalized == "medium_confidence":
        return 16.0
    return 24.0


def _uncertainty_band(edge_threshold: float, projection: float, error_pct: float) -> float:
    projection_noise = abs(float(projection)) * max(0.0, float(error_pct)) / 100.0
    return float(max(float(edge_threshold), projection_noise))


def capture_rotowire_benchmark_snapshot(
    predictions_path: Path = DEFAULT_PREDICTIONS_PATH,
    snapshot_path: Path = DEFAULT_SNAPSHOT_PATH,
    captured_at: str | None = None,
) -> dict:
    if not predictions_path.exists():
        raise FileNotFoundError(f"Predictions file not found: {predictions_path}")

    predictions = pd.read_csv(predictions_path)
    if predictions.empty:
        return {
            "path": str(snapshot_path),
            "rows_added": 0,
            "rows_total": 0,
            "captured_at": captured_at or _now_iso_utc(),
            "note": "Predictions file was empty.",
        }

    predictions = predictions.copy()
    predictions["game_date"] = pd.to_datetime(predictions["game_date"], errors="coerce").dt.strftime("%Y-%m-%d")
    captured_at = captured_at or _now_iso_utc()

    rows: list[dict[str, object]] = []
    for row in predictions.itertuples(index=False):
        row_dict = row._asdict()
        player_name = str(row_dict.get("player_name") or "").strip()
        team = str(row_dict.get("team") or "").strip()
        game_date = str(row_dict.get("game_date") or "").strip()
        if not player_name or not team or not game_date:
            continue

        confidence_flag = str(row_dict.get("confidence_flag") or "")
        row_error_pct = _safe_float(
            row_dict.get("projection_error_pct_estimate"),
            _confidence_fallback_error_pct(confidence_flag),
        )

        for market, projection_column in MARKET_TO_PREDICTION_COLUMN.items():
            line_column = MARKET_TO_LINE_COLUMN.get(market)
            if not line_column:
                continue
            projection = pd.to_numeric(pd.Series([row_dict.get(projection_column)]), errors="coerce").iloc[0]
            line = pd.to_numeric(pd.Series([row_dict.get(line_column)]), errors="coerce").iloc[0]
            if pd.isna(projection) or pd.isna(line):
                continue
            edge = float(projection) - float(line)
            threshold = float(EDGE_THRESHOLDS.get(market, 1.0))
            uncertainty = _uncertainty_band(threshold, float(projection), row_error_pct)
            if edge >= uncertainty:
                recommendation = "Higher"
            elif edge <= -uncertainty:
                recommendation = "Lower"
            else:
                recommendation = "Pass"

            rows.append(
                {
                    "captured_at": captured_at,
                    "snapshot_date": str(captured_at)[:10],
                    "player_name": player_name,
                    "team": team,
                    "game_date": game_date,
                    "market": market,
                    "line": round(float(line), 4),
                    "projection": round(float(projection), 4),
                    "edge": round(float(edge), 4),
                    "uncertainty_band": round(float(uncertainty), 4),
                    "recommendation": recommendation,
                    "projection_error_pct_estimate": round(float(row_error_pct), 4),
                    "confidence_flag": confidence_flag,
                }
            )

    snapshot_path.parent.mkdir(parents=True, exist_ok=True)
    new_frame = pd.DataFrame(rows)
    if snapshot_path.exists():
        existing = pd.read_csv(snapshot_path)
        combined = pd.concat([existing, new_frame], ignore_index=True, sort=False)
    else:
        combined = new_frame

    if not combined.empty:
        combined = combined.drop_duplicates(
            subset=["captured_at", "player_name", "team", "game_date", "market"],
            keep="last",
        ).sort_values(["captured_at", "game_date", "team", "player_name", "market"])
        combined.to_csv(snapshot_path, index=False)
    else:
        pd.DataFrame(
            columns=[
                "captured_at",
                "snapshot_date",
                "player_name",
                "team",
                "game_date",
                "market",
                "line",
                "projection",
                "edge",
                "uncertainty_band",
                "recommendation",
                "projection_error_pct_estimate",
                "confidence_flag",
            ]
        ).to_csv(snapshot_path, index=False)

    return {
        "path": str(snapshot_path),
        "rows_added": int(len(new_frame)),
        "rows_total": int(len(combined)),
        "captured_at": captured_at,
    }


def _actual_market_long_frame(history: pd.DataFrame) -> pd.DataFrame:
    records: list[dict[str, object]] = []
    for row in history.itertuples(index=False):
        row_dict = row._asdict()
        player_name = str(row_dict.get("player_name") or "").strip()
        team = str(row_dict.get("team") or "").strip()
        game_date = str(row_dict.get("game_date") or "").strip()
        if not player_name or not team or not game_date:
            continue
        for market, columns in MARKET_TO_ACTUAL_COLUMNS.items():
            values = [pd.to_numeric(pd.Series([row_dict.get(column)]), errors="coerce").iloc[0] for column in columns]
            if any(pd.isna(value) for value in values):
                continue
            records.append(
                {
                    "player_name": player_name,
                    "team": team,
                    "game_date": game_date,
                    "market": market,
                    "actual": float(sum(float(value) for value in values)),
                }
            )
    return pd.DataFrame(records)


def run_rotowire_benchmark(
    *,
    lookback_days: int = 28,
    training_path: Path = DEFAULT_TRAINING_UPLOAD_PATH,
    snapshot_path: Path = DEFAULT_SNAPSHOT_PATH,
    report_path: Path = DEFAULT_REPORT_PATH,
    join_output_path: Path = DEFAULT_JOIN_PATH,
) -> dict:
    if not snapshot_path.exists():
        raise FileNotFoundError(
            f"Benchmark snapshots were not found: {snapshot_path}. Capture snapshots before benchmarking."
        )
    if not training_path.exists():
        raise FileNotFoundError(f"Training data not found: {training_path}")

    lookback_days = max(1, int(lookback_days))
    snapshots = pd.read_csv(snapshot_path)
    if snapshots.empty:
        raise ValueError("Benchmark snapshot dataset is empty.")

    snapshots = snapshots.copy()
    snapshots["captured_at"] = pd.to_datetime(snapshots["captured_at"], errors="coerce", utc=True)
    snapshots["game_date"] = pd.to_datetime(snapshots["game_date"], errors="coerce").dt.strftime("%Y-%m-%d")
    snapshots = snapshots.dropna(subset=["captured_at", "player_name", "team", "game_date", "market"])
    if snapshots.empty:
        raise ValueError("Benchmark snapshots had no valid rows after date parsing.")

    latest_capture = snapshots["captured_at"].max()
    cutoff = latest_capture - pd.Timedelta(days=lookback_days)
    snapshots = snapshots[snapshots["captured_at"] >= cutoff].copy()
    if snapshots.empty:
        raise ValueError(f"No benchmark snapshots found in the last {lookback_days} days.")

    history = pd.read_csv(training_path)
    if history.empty:
        raise ValueError("Training history is empty.")
    history = history.copy()
    history["game_date"] = pd.to_datetime(history["game_date"], errors="coerce").dt.strftime("%Y-%m-%d")
    history["team"] = history.get("team", pd.Series("", index=history.index)).fillna("").astype(str)
    actual_long = _actual_market_long_frame(history)
    if actual_long.empty:
        raise ValueError("Training history does not contain actual stat columns for benchmark markets.")

    latest_snapshot = (
        snapshots.sort_values("captured_at")
        .groupby(["player_name", "team", "game_date", "market"], as_index=False)
        .tail(1)
        .copy()
    )
    open_snapshot = (
        snapshots.sort_values("captured_at")
        .groupby(["player_name", "team", "game_date", "market"], as_index=False)
        .head(1)
        .copy()
    )
    close_lines = latest_snapshot[
        ["player_name", "team", "game_date", "market", "line"]
    ].rename(columns={"line": "closing_line"})

    merged = latest_snapshot.merge(
        actual_long,
        on=["player_name", "team", "game_date", "market"],
        how="inner",
    )
    merged = merged.merge(
        open_snapshot[
            ["player_name", "team", "game_date", "market", "line"]
        ].rename(columns={"line": "open_line"}),
        on=["player_name", "team", "game_date", "market"],
        how="left",
    )
    merged = merged.merge(
        close_lines,
        on=["player_name", "team", "game_date", "market"],
        how="left",
    )
    if merged.empty:
        raise ValueError("No overlapping rows were found between benchmark snapshots and completed game history.")

    merged["projection_error"] = (
        pd.to_numeric(merged["projection"], errors="coerce") - pd.to_numeric(merged["actual"], errors="coerce")
    ).abs()
    merged["line_error"] = (
        pd.to_numeric(merged["line"], errors="coerce") - pd.to_numeric(merged["actual"], errors="coerce")
    ).abs()
    merged["edge"] = pd.to_numeric(merged["edge"], errors="coerce")
    merged["uncertainty_band"] = pd.to_numeric(merged["uncertainty_band"], errors="coerce").clip(lower=0.0)
    merged["edge_to_uncertainty"] = (
        merged["edge"].abs() / merged["uncertainty_band"].replace(0.0, pd.NA)
    )

    higher_hit = merged["recommendation"].eq("Higher") & (pd.to_numeric(merged["actual"], errors="coerce") > pd.to_numeric(merged["line"], errors="coerce"))
    lower_hit = merged["recommendation"].eq("Lower") & (pd.to_numeric(merged["actual"], errors="coerce") < pd.to_numeric(merged["line"], errors="coerce"))
    push_mask = pd.to_numeric(merged["actual"], errors="coerce").eq(pd.to_numeric(merged["line"], errors="coerce"))
    actionable_mask = merged["recommendation"].isin(["Higher", "Lower"])
    graded_mask = actionable_mask & ~push_mask
    merged["hit"] = (higher_hit | lower_hit).astype(int)
    merged["graded_pick"] = graded_mask.astype(int)

    merged["clv"] = pd.NA
    has_open = pd.to_numeric(merged["open_line"], errors="coerce").notna()
    has_close = pd.to_numeric(merged["closing_line"], errors="coerce").notna()
    clv_mask = actionable_mask & has_open & has_close
    higher_clv = merged["recommendation"].eq("Higher")
    merged.loc[clv_mask & higher_clv, "clv"] = (
        pd.to_numeric(merged.loc[clv_mask & higher_clv, "closing_line"], errors="coerce")
        - pd.to_numeric(merged.loc[clv_mask & higher_clv, "open_line"], errors="coerce")
    )
    merged.loc[clv_mask & ~higher_clv, "clv"] = (
        pd.to_numeric(merged.loc[clv_mask & ~higher_clv, "open_line"], errors="coerce")
        - pd.to_numeric(merged.loc[clv_mask & ~higher_clv, "closing_line"], errors="coerce")
    )
    merged["clv"] = pd.to_numeric(merged["clv"], errors="coerce")

    calibration_bins = [1.0, 1.5, 2.0, 3.0, float("inf")]
    calibration_labels = ["1.0-1.5", "1.5-2.0", "2.0-3.0", "3.0+"]
    graded = merged[graded_mask].copy()
    graded["edge_to_uncertainty"] = pd.to_numeric(graded["edge_to_uncertainty"], errors="coerce")
    graded["confidence_bucket"] = pd.cut(
        graded["edge_to_uncertainty"],
        bins=calibration_bins,
        labels=calibration_labels,
        right=False,
    )

    calibration_summary: list[dict[str, object]] = []
    if not graded.empty:
        for label, group in graded.groupby("confidence_bucket", dropna=True, observed=False):
            if group.empty:
                continue
            calibration_summary.append(
                {
                    "bucket": str(label),
                    "rows": int(len(group)),
                    "hit_rate": round(float(group["hit"].mean()), 4),
                    "avg_edge_to_uncertainty": round(float(group["edge_to_uncertainty"].mean()), 4),
                    "avg_clv": round(float(pd.to_numeric(group["clv"], errors="coerce").dropna().mean()), 4)
                    if pd.to_numeric(group["clv"], errors="coerce").notna().any()
                    else None,
                }
            )

    per_market: list[dict[str, object]] = []
    for market, group in merged.groupby("market", sort=True):
        market_graded = group[group["graded_pick"].eq(1)]
        per_market.append(
            {
                "market": str(market),
                "rows": int(len(group)),
                "projection_mae": round(float(pd.to_numeric(group["projection_error"], errors="coerce").mean()), 4),
                "line_mae": round(float(pd.to_numeric(group["line_error"], errors="coerce").mean()), 4),
                "mae_delta_vs_line": round(
                    float(pd.to_numeric(group["line_error"], errors="coerce").mean() - pd.to_numeric(group["projection_error"], errors="coerce").mean()),
                    4,
                ),
                "graded_picks": int(len(market_graded)),
                "hit_rate": round(float(market_graded["hit"].mean()), 4) if not market_graded.empty else None,
                "avg_clv": round(float(pd.to_numeric(market_graded["clv"], errors="coerce").dropna().mean()), 4)
                if pd.to_numeric(market_graded["clv"], errors="coerce").notna().any()
                else None,
            }
        )

    graded_total = merged[merged["graded_pick"].eq(1)]
    report = {
        "generated_at": _now_iso_utc(),
        "lookback_days": lookback_days,
        "snapshot_path": str(snapshot_path),
        "training_path": str(training_path),
        "joined_path": str(join_output_path),
        "rows_evaluated": int(len(merged)),
        "rows_with_actionable_picks": int(len(graded_total)),
        "model_projection_mae": round(float(pd.to_numeric(merged["projection_error"], errors="coerce").mean()), 4),
        "rotowire_line_mae": round(float(pd.to_numeric(merged["line_error"], errors="coerce").mean()), 4),
        "mae_delta_vs_rotowire_line": round(
            float(pd.to_numeric(merged["line_error"], errors="coerce").mean() - pd.to_numeric(merged["projection_error"], errors="coerce").mean()),
            4,
        ),
        "hit_rate": round(float(graded_total["hit"].mean()), 4) if not graded_total.empty else None,
        "avg_clv": round(float(pd.to_numeric(graded_total["clv"], errors="coerce").dropna().mean()), 4)
        if pd.to_numeric(graded_total["clv"], errors="coerce").notna().any()
        else None,
        "calibration": calibration_summary,
        "per_market": per_market,
    }

    join_output_path.parent.mkdir(parents=True, exist_ok=True)
    merged.sort_values(["game_date", "team", "player_name", "market"]).to_csv(join_output_path, index=False)
    report_path.parent.mkdir(parents=True, exist_ok=True)
    report_path.write_text(json.dumps(report, indent=2), encoding="utf-8")
    return report


def load_rotowire_benchmark_report(path: Path = DEFAULT_REPORT_PATH) -> dict | None:
    if not path.exists():
        return None
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return None
