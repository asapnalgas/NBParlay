from __future__ import annotations

import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd

DEFAULT_PROJECT_DIR = Path(__file__).resolve().parents[1]
DEFAULT_DATA_DIR = DEFAULT_PROJECT_DIR / "data"
DEFAULT_PIPELINE_DIR = DEFAULT_DATA_DIR / "pipeline"
DEFAULT_BRONZE_DIR = DEFAULT_PIPELINE_DIR / "bronze"
DEFAULT_SILVER_DIR = DEFAULT_PIPELINE_DIR / "silver"
DEFAULT_GOLD_DIR = DEFAULT_PIPELINE_DIR / "gold"
DEFAULT_REJECTIONS_DIR = DEFAULT_PIPELINE_DIR / "rejections"
DEFAULT_QUARANTINE_DIR = DEFAULT_PIPELINE_DIR / "quarantine"
DEFAULT_INGESTION_EVENTS_PATH = DEFAULT_PIPELINE_DIR / "ingestion_events.jsonl"
DEFAULT_INGESTION_MANIFEST_PATH = DEFAULT_PIPELINE_DIR / "ingestion_manifest.json"
DEFAULT_PIPELINE_POLICY_PATH = DEFAULT_PROJECT_DIR / "config" / "data_pipeline.json"

DEFAULT_UNIQUENESS_POLICIES: dict[str, list[str]] = {
    "training_data": ["player_key", "game_date", "opponent", "home"],
    "season_priors": ["player_key", "team"],
    "prizepicks_lines": ["player_key", "team", "game_date", "market"],
    "upcoming_slate": ["player_key", "team", "game_date", "opponent", "home"],
    "provider_context_updates": ["player_key", "team", "game_date", "opponent", "home"],
}


DATA_CONTRACTS: dict[str, dict[str, Any]] = {
    "training_data": {
        "canonical_path": str(DEFAULT_DATA_DIR / "training_data.csv"),
        "granularity": "one row per player per completed game",
        "required_columns": [
            "player_name",
            "game_date",
            "home",
            "opponent",
            "points",
            "rebounds",
            "assists",
        ],
        "optional_columns": ["team", "minutes", "steals", "blocks", "turnovers", "three_points_made"],
        "zone_targets": ["bronze", "silver"],
    },
    "upcoming_slate": {
        "canonical_path": str(DEFAULT_DATA_DIR / "upcoming_slate.csv"),
        "granularity": "one row per player per upcoming game",
        "required_columns": ["player_name", "game_date", "team", "opponent", "home"],
        "optional_columns": ["starter", "starter_probability", "expected_minutes", "injury_status"],
        "zone_targets": ["bronze", "silver"],
    },
    "season_priors": {
        "canonical_path": str(DEFAULT_DATA_DIR / "season_priors.csv"),
        "granularity": "one row per player-team aggregate season prior",
        "required_columns": ["player_name", "team", "gp", "min_season", "pts_season", "reb_season", "ast_season"],
        "optional_columns": ["tov_season", "stl_season", "blk_season", "fp_season", "dd2_season", "td3_season"],
        "zone_targets": ["bronze", "silver"],
    },
    "prizepicks_lines": {
        "canonical_path": str(DEFAULT_DATA_DIR / "prizepicks_lines.csv"),
        "granularity": "one row per player-market line capture",
        "required_columns": ["player_name", "game_date", "market", "line"],
        "optional_columns": ["team", "selection_type", "source", "captured_at"],
        "zone_targets": ["bronze", "silver"],
    },
    "context_updates": {
        "canonical_path": str(DEFAULT_DATA_DIR / "context_updates.csv"),
        "granularity": "one row per player-game context override",
        "required_columns": ["player_name", "game_date"],
        "optional_columns": ["injury_status", "starter_probability", "expected_minutes"],
        "zone_targets": ["bronze", "silver"],
    },
    "provider_context_updates": {
        "canonical_path": str(DEFAULT_DATA_DIR / "provider_context_updates.csv"),
        "granularity": "one row per player-game provider context",
        "required_columns": ["player_name", "game_date"],
        "optional_columns": ["line_points", "line_rebounds", "line_assists", "line_pra", "news_risk_score"],
        "zone_targets": ["bronze", "silver"],
    },
    "predictions": {
        "canonical_path": str(DEFAULT_PROJECT_DIR / "models" / "predictions.csv"),
        "granularity": "one row per player-game projection",
        "required_columns": ["player_name", "game_date", "team", "opponent", "predicted_points", "predicted_rebounds", "predicted_assists"],
        "optional_columns": ["predicted_pra", "predicted_draftkings_points", "predicted_fanduel_points", "confidence_flag"],
        "zone_targets": ["gold"],
    },
    "prizepicks_edges": {
        "canonical_path": str(DEFAULT_PROJECT_DIR / "models" / "prizepicks_edges.csv"),
        "granularity": "one row per player-market edge computation",
        "required_columns": ["player_name", "game_date", "market", "line", "projection", "edge", "recommendation"],
        "optional_columns": ["team", "confidence_flag", "historical_games_used", "season_priors_available"],
        "zone_targets": ["gold"],
    },
}


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def ensure_pipeline_layout() -> None:
    DEFAULT_PIPELINE_DIR.mkdir(parents=True, exist_ok=True)
    DEFAULT_BRONZE_DIR.mkdir(parents=True, exist_ok=True)
    DEFAULT_SILVER_DIR.mkdir(parents=True, exist_ok=True)
    DEFAULT_GOLD_DIR.mkdir(parents=True, exist_ok=True)
    DEFAULT_REJECTIONS_DIR.mkdir(parents=True, exist_ok=True)
    DEFAULT_QUARANTINE_DIR.mkdir(parents=True, exist_ok=True)
    if not DEFAULT_INGESTION_EVENTS_PATH.exists():
        DEFAULT_INGESTION_EVENTS_PATH.write_text("", encoding="utf-8")
    if not DEFAULT_INGESTION_MANIFEST_PATH.exists():
        DEFAULT_INGESTION_MANIFEST_PATH.write_text("{}", encoding="utf-8")


def describe_data_contracts() -> dict[str, Any]:
    ensure_pipeline_layout()
    return {
        "zones": {
            "bronze": str(DEFAULT_BRONZE_DIR),
            "silver": str(DEFAULT_SILVER_DIR),
            "gold": str(DEFAULT_GOLD_DIR),
            "rejections": str(DEFAULT_REJECTIONS_DIR),
            "quarantine": str(DEFAULT_QUARANTINE_DIR),
            "events": str(DEFAULT_INGESTION_EVENTS_PATH),
            "manifest": str(DEFAULT_INGESTION_MANIFEST_PATH),
            "policy": str(DEFAULT_PIPELINE_POLICY_PATH),
        },
        "contracts": DATA_CONTRACTS,
        "uniqueness_policies": load_uniqueness_policies(),
    }


def load_uniqueness_policies() -> dict[str, list[str]]:
    policies = {dataset: list(keys) for dataset, keys in DEFAULT_UNIQUENESS_POLICIES.items()}
    if not DEFAULT_PIPELINE_POLICY_PATH.exists():
        return policies
    try:
        payload = json.loads(DEFAULT_PIPELINE_POLICY_PATH.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, OSError):
        return policies
    configured = payload.get("uniqueness_policies") if isinstance(payload, dict) else None
    if not isinstance(configured, dict):
        return policies
    for dataset, keys in configured.items():
        if isinstance(keys, list):
            policies[str(dataset)] = [str(column) for column in keys if str(column).strip()]
    return policies


def resolve_uniqueness_keys(
    dataset: str,
    available_columns: list[str],
    fallback_keys: list[str] | None = None,
) -> list[str]:
    available_set = {str(column) for column in available_columns}
    policies = load_uniqueness_policies()
    configured = policies.get(str(dataset), []) or []
    filtered = [column for column in configured if column in available_set]
    if filtered:
        return filtered
    fallback = fallback_keys or []
    return [column for column in fallback if column in available_set]


def split_duplicates_by_policy(
    dataset: str,
    frame: pd.DataFrame,
    fallback_keys: list[str] | None = None,
    keep: str = "last",
) -> tuple[pd.DataFrame, pd.DataFrame, list[str]]:
    if frame is None or frame.empty:
        return frame.copy() if frame is not None else pd.DataFrame(), pd.DataFrame(), []
    keys = resolve_uniqueness_keys(dataset, list(frame.columns), fallback_keys=fallback_keys)
    if not keys:
        return frame.copy(), pd.DataFrame(), []
    duplicate_mask = frame.duplicated(subset=keys, keep=keep)
    duplicates = frame[duplicate_mask].copy()
    deduped = frame[~duplicate_mask].copy()
    return deduped, duplicates, keys


def _json_line_append(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as output:
        output.write(json.dumps(payload, ensure_ascii=True) + "\n")


def _snapshot_path(dataset: str, stage: str) -> Path:
    stage_lower = str(stage or "").strip().lower()
    if stage_lower == "bronze":
        base = DEFAULT_BRONZE_DIR
    elif stage_lower == "silver":
        base = DEFAULT_SILVER_DIR
    else:
        base = DEFAULT_GOLD_DIR
    return base / f"{dataset}.csv"


def compute_frame_fingerprint(frame: pd.DataFrame | None) -> str:
    hasher = hashlib.sha256()
    if frame is None or frame.empty:
        hasher.update(b"__empty__")
        return hasher.hexdigest()

    working = frame.loc[:, ~frame.columns.duplicated()].copy()
    columns = [str(column) for column in working.columns]
    hasher.update("|".join(columns).encode("utf-8", errors="ignore"))
    normalized = working.fillna("").astype(str)
    normalized = normalized.sort_values(by=columns).reset_index(drop=True) if columns else normalized
    for row in normalized.itertuples(index=False, name=None):
        payload = "\x1f".join(str(value) for value in row)
        hasher.update(payload.encode("utf-8", errors="ignore"))
        hasher.update(b"\n")
    return hasher.hexdigest()


def _load_ingestion_manifest() -> dict[str, Any]:
    ensure_pipeline_layout()
    if not DEFAULT_INGESTION_MANIFEST_PATH.exists():
        return {}
    try:
        payload = json.loads(DEFAULT_INGESTION_MANIFEST_PATH.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return {}
    return payload if isinstance(payload, dict) else {}


def _save_ingestion_manifest(manifest: dict[str, Any]) -> None:
    DEFAULT_INGESTION_MANIFEST_PATH.parent.mkdir(parents=True, exist_ok=True)
    DEFAULT_INGESTION_MANIFEST_PATH.write_text(
        json.dumps(manifest, indent=2, ensure_ascii=True),
        encoding="utf-8",
    )


def check_and_register_idempotency(
    *,
    dataset: str,
    source: str,
    fingerprint: str,
    metadata: dict[str, Any] | None = None,
) -> dict[str, Any]:
    ensure_pipeline_layout()
    manifest = _load_ingestion_manifest()
    dataset_manifest = manifest.setdefault(str(dataset), {})
    source_key = str(source or "unknown")
    previous = dataset_manifest.get(source_key, {})
    previous_fingerprint = str(previous.get("fingerprint") or "")
    duplicate = bool(previous_fingerprint and previous_fingerprint == str(fingerprint))
    now_iso = _utc_now_iso()

    dataset_manifest[source_key] = {
        "fingerprint": str(fingerprint),
        "updated_at": now_iso,
        "metadata": metadata or {},
        "duplicate_of_previous": duplicate,
    }
    manifest[str(dataset)] = dataset_manifest
    _save_ingestion_manifest(manifest)
    return {
        "duplicate": duplicate,
        "source": source_key,
        "fingerprint": str(fingerprint),
        "previous_fingerprint": previous_fingerprint,
        "updated_at": now_iso,
    }


def write_stage_snapshot(dataset: str, stage: str, frame: pd.DataFrame) -> str:
    ensure_pipeline_layout()
    path = _snapshot_path(dataset, stage)
    frame_to_write = frame.copy() if frame is not None else pd.DataFrame()
    path.parent.mkdir(parents=True, exist_ok=True)
    frame_to_write.to_csv(path, index=False)
    return str(path)


def append_rejections(
    dataset: str,
    rejected: pd.DataFrame,
    *,
    source: str | None = None,
) -> dict[str, Any]:
    ensure_pipeline_layout()
    output_path = DEFAULT_REJECTIONS_DIR / f"{dataset}_rejections.csv"
    if rejected is None or rejected.empty:
        return {"path": str(output_path), "rows_appended": 0}

    frame = rejected.copy()
    frame["dataset"] = dataset
    frame["rejected_at"] = _utc_now_iso()
    frame["source"] = str(source or "unknown")
    output_path.parent.mkdir(parents=True, exist_ok=True)
    write_header = not output_path.exists()
    frame.to_csv(output_path, mode="a", header=write_header, index=False)
    return {"path": str(output_path), "rows_appended": int(len(frame))}


def append_quarantine(
    dataset: str,
    rows: pd.DataFrame,
    *,
    source: str | None = None,
) -> dict[str, Any]:
    ensure_pipeline_layout()
    output_path = DEFAULT_QUARANTINE_DIR / f"{dataset}_quarantine.csv"
    if rows is None or rows.empty:
        return {"path": str(output_path), "rows_appended": 0}
    frame = rows.copy()
    frame["dataset"] = str(dataset)
    frame["quarantined_at"] = _utc_now_iso()
    frame["source"] = str(source or "unknown")
    output_path.parent.mkdir(parents=True, exist_ok=True)
    write_header = not output_path.exists()
    frame.to_csv(output_path, mode="a", header=write_header, index=False)
    return {"path": str(output_path), "rows_appended": int(len(frame))}


def record_ingestion_event(
    *,
    dataset: str,
    stage: str,
    rows_in: int,
    rows_out: int,
    rows_rejected: int,
    source: str | None = None,
    details: dict[str, Any] | None = None,
    outcome: str = "success",
) -> dict[str, Any]:
    ensure_pipeline_layout()
    payload = {
        "timestamp": _utc_now_iso(),
        "dataset": dataset,
        "stage": stage,
        "rows_in": int(rows_in),
        "rows_out": int(rows_out),
        "rows_rejected": int(rows_rejected),
        "source": str(source or "unknown"),
        "outcome": str(outcome or "success"),
        "details": details or {},
    }
    _json_line_append(DEFAULT_INGESTION_EVENTS_PATH, payload)
    return payload


def load_recent_ingestion_events(limit: int = 50) -> list[dict[str, Any]]:
    ensure_pipeline_layout()
    if not DEFAULT_INGESTION_EVENTS_PATH.exists():
        return []
    events: list[dict[str, Any]] = []
    with DEFAULT_INGESTION_EVENTS_PATH.open("r", encoding="utf-8") as input_file:
        for line in input_file:
            stripped = line.strip()
            if not stripped:
                continue
            try:
                events.append(json.loads(stripped))
            except json.JSONDecodeError:
                continue
    return events[-max(1, int(limit)) :]


def _csv_row_count(path: Path) -> int:
    if not path.exists():
        return 0
    lines = 0
    with path.open("r", encoding="utf-8", errors="ignore") as input_file:
        for _ in input_file:
            lines += 1
    return max(0, lines - 1)


def run_contract_drift_audit() -> dict[str, Any]:
    ensure_pipeline_layout()
    report_rows: list[dict[str, Any]] = []
    summary = {
        "datasets_total": 0,
        "datasets_missing_file": 0,
        "datasets_with_missing_required_columns": 0,
        "datasets_with_unexpected_columns": 0,
        "datasets_ok": 0,
    }

    for dataset, contract in DATA_CONTRACTS.items():
        summary["datasets_total"] += 1
        canonical_path = Path(str(contract.get("canonical_path", "")))
        required_columns = [str(value) for value in contract.get("required_columns", [])]
        optional_columns = [str(value) for value in contract.get("optional_columns", [])]
        allowed_columns = set(required_columns + optional_columns)

        dataset_report: dict[str, Any] = {
            "dataset": dataset,
            "path": str(canonical_path),
            "exists": canonical_path.exists(),
            "rows": 0,
            "missing_required_columns": [],
            "unexpected_columns": [],
            "status": "ok",
        }

        if not canonical_path.exists():
            dataset_report["status"] = "missing_file"
            summary["datasets_missing_file"] += 1
            report_rows.append(dataset_report)
            continue

        try:
            columns_frame = pd.read_csv(canonical_path, nrows=0)
        except Exception as exc:  # noqa: BLE001
            dataset_report["status"] = "read_error"
            dataset_report["read_error"] = str(exc)
            summary["datasets_missing_file"] += 1
            report_rows.append(dataset_report)
            continue

        columns = [str(column) for column in columns_frame.columns]
        dataset_report["rows"] = _csv_row_count(canonical_path)
        dataset_report["missing_required_columns"] = [column for column in required_columns if column not in columns]
        if allowed_columns:
            dataset_report["unexpected_columns"] = [column for column in columns if column not in allowed_columns]
        if dataset_report["missing_required_columns"]:
            dataset_report["status"] = "missing_required_columns"
            summary["datasets_with_missing_required_columns"] += 1
        elif dataset_report["unexpected_columns"]:
            dataset_report["status"] = "unexpected_columns"
            summary["datasets_with_unexpected_columns"] += 1
        else:
            dataset_report["status"] = "ok"
            summary["datasets_ok"] += 1
        report_rows.append(dataset_report)

    return {
        "generated_at": _utc_now_iso(),
        "summary": summary,
        "datasets": report_rows,
    }


def pipeline_status(limit_events: int = 20, include_drift: bool = False) -> dict[str, Any]:
    ensure_pipeline_layout()
    contracts = describe_data_contracts()
    recent_events = load_recent_ingestion_events(limit=max(1, int(limit_events)))
    manifest = _load_ingestion_manifest()
    rejection_counts: dict[str, int] = {}
    for path in sorted(DEFAULT_REJECTIONS_DIR.glob("*_rejections.csv")):
        try:
            rows = len(pd.read_csv(path))
        except Exception:
            rows = 0
        rejection_counts[path.stem.replace("_rejections", "")] = int(rows)
    quarantine_counts: dict[str, int] = {}
    for path in sorted(DEFAULT_QUARANTINE_DIR.glob("*_quarantine.csv")):
        try:
            rows = len(pd.read_csv(path))
        except Exception:
            rows = 0
        quarantine_counts[path.stem.replace("_quarantine", "")] = int(rows)
    payload: dict[str, Any] = {
        "contracts": contracts,
        "recent_events": recent_events,
        "manifest": manifest,
        "rejection_counts": rejection_counts,
        "quarantine_counts": quarantine_counts,
    }
    if include_drift:
        payload["drift_audit"] = run_contract_drift_audit()
    return payload
