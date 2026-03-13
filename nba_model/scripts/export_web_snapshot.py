#!/usr/bin/env python3
from __future__ import annotations

import json
import sys
from datetime import datetime, timezone
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from app import (  # noqa: E402
    SCHEMA_GUIDE,
    _combined_app_status_cached,
    _json_safe,
    build_player_board,
    live_source_catalog,
)
from src.data_pipeline import describe_data_contracts, pipeline_status, run_contract_drift_audit  # noqa: E402

SNAPSHOT_DIR = PROJECT_ROOT / "ui" / "snapshot"


def _write_snapshot(name: str, payload: object) -> None:
    SNAPSHOT_DIR.mkdir(parents=True, exist_ok=True)
    target = SNAPSHOT_DIR / name
    content = json.dumps(_json_safe(payload), indent=2, allow_nan=False)
    target.write_text(content, encoding="utf-8")


def main() -> None:
    generated_at = datetime.now(timezone.utc).isoformat()

    schema_payload = dict(SCHEMA_GUIDE)
    schema_payload["data_contracts"] = describe_data_contracts()

    status_payload = _combined_app_status_cached(include_previews=True, force_refresh=True)
    live_sources_payload = live_source_catalog()
    pipeline_payload = pipeline_status(limit_events=100, include_drift=False)
    drift_payload = run_contract_drift_audit()

    board_payload = build_player_board()
    available_dates = list(board_payload.get("available_dates") or [])

    _write_snapshot("schema.json", schema_payload)
    _write_snapshot("status.json", status_payload)
    _write_snapshot("live-sources.json", live_sources_payload)
    _write_snapshot("pipeline.json", pipeline_payload)
    _write_snapshot("drift-audit.json", drift_payload)
    _write_snapshot("player-board.json", board_payload)

    # Date-specific board snapshots allow the deployed UI date picker to stay functional.
    for board_date in available_dates:
        date_value = str(board_date).strip()
        if not date_value:
            continue
        payload = build_player_board(date_value)
        _write_snapshot(f"player-board-{date_value}.json", payload)

    manifest = {
        "generated_at": generated_at,
        "snapshot_dir": str(SNAPSHOT_DIR),
        "available_dates": available_dates,
        "files": sorted([path.name for path in SNAPSHOT_DIR.glob("*.json")]),
    }
    _write_snapshot("manifest.json", manifest)
    print(json.dumps(manifest, indent=2))


if __name__ == "__main__":
    main()
