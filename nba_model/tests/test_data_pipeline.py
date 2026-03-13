from __future__ import annotations

import tempfile
import unittest
from contextlib import ExitStack
import json
from pathlib import Path
from unittest import mock
import sys

import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(PROJECT_ROOT / "nba_model"))

from src import data_pipeline as dp
from src import importers as imp


class DataPipelineHardeningTests(unittest.TestCase):
    def _patch_pipeline_paths(self, root: Path) -> ExitStack:
        stack = ExitStack()
        data_dir = root / "data"
        pipeline_dir = data_dir / "pipeline"
        bronze_dir = pipeline_dir / "bronze"
        silver_dir = pipeline_dir / "silver"
        gold_dir = pipeline_dir / "gold"
        rejections_dir = pipeline_dir / "rejections"
        quarantine_dir = pipeline_dir / "quarantine"
        events_path = pipeline_dir / "ingestion_events.jsonl"
        manifest_path = pipeline_dir / "ingestion_manifest.json"
        policy_path = root / "config" / "data_pipeline.json"

        stack.enter_context(mock.patch.object(dp, "DEFAULT_DATA_DIR", data_dir))
        stack.enter_context(mock.patch.object(dp, "DEFAULT_PIPELINE_DIR", pipeline_dir))
        stack.enter_context(mock.patch.object(dp, "DEFAULT_BRONZE_DIR", bronze_dir))
        stack.enter_context(mock.patch.object(dp, "DEFAULT_SILVER_DIR", silver_dir))
        stack.enter_context(mock.patch.object(dp, "DEFAULT_GOLD_DIR", gold_dir))
        stack.enter_context(mock.patch.object(dp, "DEFAULT_REJECTIONS_DIR", rejections_dir))
        stack.enter_context(mock.patch.object(dp, "DEFAULT_QUARANTINE_DIR", quarantine_dir))
        stack.enter_context(mock.patch.object(dp, "DEFAULT_INGESTION_EVENTS_PATH", events_path))
        stack.enter_context(mock.patch.object(dp, "DEFAULT_INGESTION_MANIFEST_PATH", manifest_path))
        stack.enter_context(mock.patch.object(dp, "DEFAULT_PIPELINE_POLICY_PATH", policy_path))
        return stack

    def test_historical_import_writes_rejections_quarantine_and_event(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            output_path = root / "training.csv"
            with self._patch_pipeline_paths(root):
                payload = "\n".join(
                    [
                        "player_name,game_date,home,opponent,points,rebounds,assists,team",
                        "Valid Player,2026-03-01,1,BOS,20,5,6,NYK",
                        "Bad Date,not-a-date,1,BOS,14,4,3,NYK",
                    ]
                )
                result = imp.import_historical_text(payload, output_path=output_path)
                events = dp.load_recent_ingestion_events(limit=5)

            self.assertEqual(result["rows_accepted"], 1)
            self.assertEqual(result["rows_rejected"], 1)
            self.assertTrue(Path(result["rejection_log_path"]).exists())
            self.assertTrue(Path(result["quarantine_path"]).exists())
            self.assertTrue(any(event.get("dataset") == "training_data" for event in events))

    def test_prizepicks_duplicate_payload_is_skipped(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            output_path = root / "prizepicks_lines.csv"
            with self._patch_pipeline_paths(root):
                payload = "\n".join(
                    [
                        "player_name,team,game_date,market,line",
                        "Jalen Brunson,NYK,2026-03-04,points,25.5",
                    ]
                )
                first = imp.import_prizepicks_lines_text(payload, output_path=output_path)
                second = imp.import_prizepicks_lines_text(payload, output_path=output_path)
                events = dp.load_recent_ingestion_events(limit=10)

            self.assertFalse(bool(first.get("skipped")))
            self.assertTrue(bool(second.get("skipped")))
            self.assertTrue(any(event.get("outcome") == "duplicate_skipped" for event in events))

    def test_season_priors_rejects_invalid_numeric_and_quarantines(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            output_path = root / "season_priors.csv"
            with self._patch_pipeline_paths(root):
                payload = "\n".join(
                    [
                        "PLAYER,TEAM,GP,MIN,PTS,REB,AST",
                        "Good Player,NYK,20,34,25,5,6",
                        "Bad Numeric,NYK,NaN,31,19,5,4",
                    ]
                )
                result = imp.import_season_priors_text(payload, output_path=output_path)

            self.assertEqual(result["rows_accepted"], 1)
            self.assertEqual(result["rows_rejected"], 1)
            self.assertTrue(Path(result["rejection_log_path"]).exists())
            self.assertTrue(Path(result["quarantine_path"]).exists())

    def test_pipeline_status_includes_quarantine_counts_and_drift_report(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            with self._patch_pipeline_paths(root):
                (root / "config").mkdir(parents=True, exist_ok=True)
                policy_payload = {
                    "uniqueness_policies": {
                        "training_data": ["player_key", "game_date"],
                    }
                }
                (root / "config" / "data_pipeline.json").write_text(
                    json.dumps(policy_payload, indent=2),
                    encoding="utf-8",
                )

                contracts = {
                    "sample_dataset": {
                        "canonical_path": str(root / "data" / "sample.csv"),
                        "required_columns": ["a", "b"],
                        "optional_columns": ["c"],
                    }
                }
                sample_path = root / "data" / "sample.csv"
                sample_path.parent.mkdir(parents=True, exist_ok=True)
                pd.DataFrame([{"a": 1, "x": 9}]).to_csv(sample_path, index=False)
                quarantine_path = root / "data" / "pipeline" / "quarantine" / "sample_dataset_quarantine.csv"
                quarantine_path.parent.mkdir(parents=True, exist_ok=True)
                pd.DataFrame([{"row": 1}]).to_csv(quarantine_path, index=False)

                with mock.patch.object(dp, "DATA_CONTRACTS", contracts):
                    status = dp.pipeline_status(limit_events=5, include_drift=True)

            self.assertIn("quarantine_counts", status)
            self.assertEqual(status["quarantine_counts"].get("sample_dataset"), 1)
            self.assertIn("drift_audit", status)
            self.assertGreaterEqual(status["drift_audit"]["summary"]["datasets_with_missing_required_columns"], 1)


if __name__ == "__main__":
    unittest.main()
