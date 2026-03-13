from __future__ import annotations

import tempfile
import unittest
from pathlib import Path
import sys
from unittest.mock import patch

import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(PROJECT_ROOT / "nba_model"))

from src.features import build_feature_frame
from src.engine import (
    _add_pregame_anchor_columns,
    _apply_anchor_projection_blend,
    _apply_prediction_intervals_and_error_estimates,
    _filter_modeling_history_rows,
    _generic_csv_preview,
    _prediction_quality_gate,
    load_predictions,
)
from src.importers import import_prizepicks_lines_text, import_season_priors_text
from src.live_sync import (
    _align_provider_rows_to_upcoming,
    _append_completed_rows,
    _completed_games_from_schedule,
    _extract_rotowire_prizepicks_rows,
)
from src.prizepicks import generate_prizepicks_edges, load_prizepicks_edges, load_prizepicks_lines


class RegressionTests(unittest.TestCase):
    def test_status_helpers_skip_pandas_reads_when_preview_disabled(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_dir_path = Path(temp_dir)
            generic_path = temp_dir_path / "generic.csv"
            predictions_path = temp_dir_path / "predictions.csv"
            lines_path = temp_dir_path / "prizepicks_lines.csv"
            edges_path = temp_dir_path / "prizepicks_edges.csv"

            generic_path.write_text("a,b\n1,2\n", encoding="utf-8")
            predictions_path.write_text("player_name,predicted_points\nA,19.5\n", encoding="utf-8")
            lines_path.write_text("player_name,market,line\nA,points,19.5\n", encoding="utf-8")
            edges_path.write_text("player_name,market,edge\nA,points,2.1\n", encoding="utf-8")

            with patch("src.engine.pd.read_csv", side_effect=AssertionError("engine pandas read_csv should not run")):
                generic_payload = _generic_csv_preview(generic_path, include_preview=False)
                predictions_payload = load_predictions(predictions_path, include_preview=False)

            with patch("src.prizepicks.pd.read_csv", side_effect=AssertionError("prizepicks pandas read_csv should not run")):
                lines_payload = load_prizepicks_lines(lines_path, include_preview=False)
                edges_payload = load_prizepicks_edges(edges_path, include_preview=False)

        self.assertEqual(generic_payload["columns"], ["a", "b"])
        self.assertEqual(generic_payload["preview"], [])
        self.assertEqual(predictions_payload["columns"], ["player_name", "predicted_points"])
        self.assertEqual(predictions_payload["preview"], [])
        self.assertEqual(lines_payload["columns"], ["player_name", "market", "line"])
        self.assertEqual(lines_payload["preview"], [])
        self.assertEqual(edges_payload["columns"], ["player_name", "market", "edge"])
        self.assertEqual(edges_payload["preview"], [])

    def test_filter_modeling_history_rows_enforces_integrity(self) -> None:
        frame = pd.DataFrame(
            [
                {
                    "player_name": "Valid Player",
                    "game_date": "2026-03-01",
                    "team": "NYK",
                    "opponent": "BOS",
                    "minutes": 32.0,
                    "points": 20,
                    "rebounds": 6,
                    "assists": 5,
                    "starter": 1,
                },
                {
                    "player_name": "",
                    "game_date": "2026-03-01",
                    "team": "NYK",
                    "opponent": "BOS",
                    "minutes": 20.0,
                    "points": 10,
                    "rebounds": 4,
                    "assists": 3,
                },
                {
                    "player_name": "Bad Date",
                    "game_date": "not-a-date",
                    "team": "NYK",
                    "opponent": "BOS",
                    "minutes": 20.0,
                    "points": 10,
                    "rebounds": 4,
                    "assists": 3,
                },
                {
                    "player_name": "Bad Team",
                    "game_date": "2026-03-01",
                    "team": "MEL",
                    "opponent": "AUS",
                    "minutes": 20.0,
                    "points": 10,
                    "rebounds": 4,
                    "assists": 3,
                },
                {
                    "player_name": "Bad Range",
                    "game_date": "2026-03-01",
                    "team": "NYK",
                    "opponent": "BOS",
                    "minutes": 20.0,
                    "points": 140,
                    "rebounds": 4,
                    "assists": 3,
                },
            ]
        )

        filtered, summary = _filter_modeling_history_rows(frame)

        self.assertEqual(len(filtered), 1)
        self.assertEqual(filtered.iloc[0]["player_name"], "Valid Player")
        self.assertGreaterEqual(summary.get("removed_missing_player_rows", 0), 1)
        self.assertGreaterEqual(summary.get("removed_invalid_game_date_rows", 0), 1)
        self.assertGreaterEqual(summary.get("removed_non_nba_rows", 0), 1)
        self.assertGreaterEqual(summary.get("removed_out_of_range_rows", 0), 1)

    def test_prediction_quality_gate_blocks_invalid_rows(self) -> None:
        frame = pd.DataFrame(
            [
                {
                    "player_name": "",
                    "game_date": "bad-date",
                    "team": "XXX",
                    "opponent": "YYY",
                    "expected_minutes": 55,
                    "games_played_before": 0,
                    "season_priors_available": 0,
                },
                {
                    "player_name": "Valid Player",
                    "game_date": "2026-03-01",
                    "team": "NYK",
                    "opponent": "BOS",
                    "expected_minutes": 32,
                    "games_played_before": 8,
                    "season_priors_available": 1,
                },
            ]
        )

        gated = _prediction_quality_gate(frame)

        self.assertTrue(bool(gated.loc[0, "prediction_quality_blocked"]))
        self.assertGreater(float(gated.loc[1, "prediction_quality_score"]), 0.6)
        self.assertFalse(bool(gated.loc[1, "prediction_quality_blocked"]))

    def test_prediction_intervals_generate_ordered_bands(self) -> None:
        result = pd.DataFrame(
            [
                {
                    "predicted_points": 20.0,
                    "predicted_rebounds": 8.0,
                    "predicted_assists": 6.0,
                    "predicted_minutes": 34.0,
                }
            ]
        )
        prediction_frame = pd.DataFrame(
            [
                {
                    "games_played_before": 14,
                    "starter_probability": 0.9,
                    "expected_minutes_confidence": 0.8,
                    "injury_risk_score": 0.1,
                }
            ]
        )
        bundle = {
            "error_distribution": {
                "points": {"abs_error_p80": 3.0, "residual_q10": -2.5, "residual_q50": 0.0, "residual_q90": 2.5},
                "rebounds": {"abs_error_p80": 1.8, "residual_q10": -1.5, "residual_q50": 0.0, "residual_q90": 1.5},
                "assists": {"abs_error_p80": 1.5, "residual_q10": -1.2, "residual_q50": 0.0, "residual_q90": 1.2},
                "minutes": {"abs_error_p80": 4.0, "residual_q10": -3.0, "residual_q50": 0.0, "residual_q90": 3.0},
            }
        }

        enriched = _apply_prediction_intervals_and_error_estimates(
            result,
            prediction_frame,
            bundle=bundle,
            calibration_profile={},
        )

        self.assertIn("predicted_points_p10", enriched.columns)
        self.assertIn("projection_error_pct_estimate", enriched.columns)
        self.assertLessEqual(float(enriched.loc[0, "predicted_points_p10"]), float(enriched.loc[0, "predicted_points"]))
        self.assertGreaterEqual(float(enriched.loc[0, "predicted_points_p90"]), float(enriched.loc[0, "predicted_points"]))

    def test_anchor_blend_applies_without_lines_when_form_and_priors_exist(self) -> None:
        result = pd.DataFrame(
            [
                {
                    "predicted_points": 2.0,
                    "predicted_rebounds": 1.0,
                    "predicted_assists": 1.0,
                    "predicted_minutes": 32.0,
                }
            ]
        )
        prediction_frame = pd.DataFrame(
            [
                {
                    "games_played_before": 2,
                    "starter": 1,
                    "expected_minutes": 32.0,
                    "min_season": 34.0,
                    "pts_season": 20.0,
                    "reb_season": 6.0,
                    "ast_season": 5.0,
                    "points_avg_last_5": 18.0,
                    "rebounds_avg_last_5": 5.0,
                    "assists_avg_last_5": 4.0,
                    "line_points": pd.NA,
                    "line_rebounds": pd.NA,
                    "line_assists": pd.NA,
                }
            ]
        )

        anchored = _add_pregame_anchor_columns(result, prediction_frame)
        blended = _apply_anchor_projection_blend(anchored, prediction_frame)

        self.assertGreater(float(blended.iloc[0]["predicted_points"]), 2.0)
        self.assertGreaterEqual(float(blended.iloc[0]["pregame_anchor_sources_points"]), 2.0)

    def test_completed_games_from_schedule_filters_recent_finals(self) -> None:
        now_utc = pd.Timestamp.now("UTC")
        within_window = (now_utc - pd.Timedelta(days=3)).strftime("%Y-%m-%dT%H:%M:%SZ")
        outside_window = (now_utc - pd.Timedelta(days=70)).strftime("%Y-%m-%dT%H:%M:%SZ")
        payload = {
            "leagueSchedule": {
                "gameDates": [
                    {
                        "gameDate": "2026-03-01",
                        "games": [
                            {
                                "gameId": "0001",
                                "gameStatus": 3,
                                "gameLabel": "",
                                "gameDateUTC": within_window,
                                "homeTeam": {"teamTricode": "LAL"},
                                "awayTeam": {"teamTricode": "BOS"},
                            },
                            {
                                "gameId": "0002",
                                "gameStatus": 2,
                                "gameLabel": "",
                                "gameDateUTC": within_window,
                                "homeTeam": {"teamTricode": "NYK"},
                                "awayTeam": {"teamTricode": "MIA"},
                            },
                            {
                                "gameId": "0003",
                                "gameStatus": 3,
                                "gameLabel": "Preseason",
                                "gameDateUTC": within_window,
                                "homeTeam": {"teamTricode": "SAS"},
                                "awayTeam": {"teamTricode": "UTA"},
                            },
                            {
                                "gameId": "0004",
                                "gameStatus": 3,
                                "gameLabel": "",
                                "gameDateUTC": outside_window,
                                "homeTeam": {"teamTricode": "PHX"},
                                "awayTeam": {"teamTricode": "DEN"},
                            },
                        ],
                    }
                ]
            }
        }

        games = _completed_games_from_schedule(payload, lookback_days=42)

        self.assertEqual(len(games), 1)
        self.assertEqual(games[0]["game_id"], "0001")

    def test_build_feature_frame_handles_blank_upcoming_stats(self) -> None:
        frame = pd.DataFrame(
            [
                {
                    "player_name": "Test Player",
                    "game_date": "2026-03-01",
                    "home": 1,
                    "opponent": "BOS",
                    "team": "NYK",
                    "points": 20,
                    "rebounds": 5,
                    "assists": 4,
                    "minutes": 32,
                },
                {
                    "player_name": "Test Player",
                    "game_date": "2026-03-03",
                    "home": 0,
                    "opponent": "LAL",
                    "team": "NYK",
                    "points": pd.NA,
                    "rebounds": pd.NA,
                    "assists": pd.NA,
                    "minutes": pd.NA,
                },
            ]
        )
        frame["game_date"] = pd.to_datetime(frame["game_date"])

        feature_frame = build_feature_frame(frame)

        self.assertEqual(len(feature_frame), 2)
        self.assertIn("points_avg_last_3", feature_frame.columns)
        self.assertTrue(pd.notna(feature_frame.loc[1, "points_avg_last_3"]))

    def test_append_completed_rows_dedupes_exact_duplicates(self) -> None:
        row = {
            "game_id": 1,
            "player_name": "Test Player",
            "game_date": "2026-03-03",
            "team": "NYK",
            "opponent": "BOS",
            "home": 1,
            "points": 10,
            "rebounds": 4,
            "assists": 3,
        }

        with tempfile.TemporaryDirectory() as temp_dir:
            path = Path(temp_dir) / "training.csv"
            pd.DataFrame([row, row]).to_csv(path, index=False)
            appended = _append_completed_rows(path, pd.DataFrame([row]))
            reloaded = pd.read_csv(path)

        self.assertEqual(appended, 0)
        self.assertEqual(len(reloaded), 1)

    def test_align_provider_rows_falls_back_when_team_is_noisy(self) -> None:
        upcoming = pd.DataFrame(
            [
                {
                    "player_name": "Aaron Nesmith",
                    "game_date": "2026-03-04",
                    "team": "IND",
                }
            ]
        )
        provider = pd.DataFrame(
            [
                {
                    "player_name": "Aaron Nesmith",
                    "game_date": "2026-03-04",
                    "team": "XXX",
                    "injury_status": "Questionable",
                    "health_status": "Ankle",
                }
            ]
        )

        aligned = _align_provider_rows_to_upcoming(upcoming, provider)

        self.assertEqual(len(aligned), 1)
        self.assertEqual(aligned.iloc[0]["team"], "IND")
        self.assertEqual(aligned.iloc[0]["injury_status"], "Questionable")

    def test_import_season_priors_text_normalizes_expected_columns(self) -> None:
        text = "\n".join(
            [
                "PLAYER\tTEAM\tAGE\tGP\tW\tL\tMIN\tPTS\tREB\tAST",
                "Jalen Brunson\tNYK\t29\t56\t38\t18\t34.6\t26.7\t3.4\t6.1",
            ]
        )
        with tempfile.TemporaryDirectory() as temp_dir:
            output_path = Path(temp_dir) / "season_priors.csv"
            result = import_season_priors_text(text, output_path=output_path)
            saved = pd.read_csv(output_path)

        self.assertEqual(result["rows_accepted"], 1)
        self.assertIn("pts_season", saved.columns)
        self.assertEqual(saved.iloc[0]["team"], "NYK")

    def test_extract_rotowire_prizepicks_rows_maps_markets_and_prefers_non_promo(self) -> None:
        payload = {
            "markets": [
                {"marketID": 10, "sport": "NBA", "marketName": "Points"},
                {"marketID": 11, "sport": "NBA", "marketName": "Rebounds"},
                {"marketID": 12, "sport": "NBA", "marketName": "PTS+REB+AST"},
            ],
            "events": [
                {"eventID": 100, "eventTime": 1772924400, "homeTeam": "LAL", "awayTeam": "BOS"},
            ],
            "entities": [
                {"entityID": 200, "eventID": 100, "sport": "NBA", "name": "LeBron James", "team": "LAL"},
            ],
            "props": [
                {
                    "propID": 300,
                    "marketID": 10,
                    "entities": [200],
                    "lines": [
                        {"book": "prizepicks", "line": 25.5, "lineTime": 1772920000, "promo": 1},
                        {"book": "prizepicks", "line": 24.5, "lineTime": 1772920100},
                    ],
                },
                {
                    "propID": 301,
                    "marketID": 11,
                    "entities": [200],
                    "lines": [
                        {"book": "prizepicks", "line": 7.5, "lineTime": 1772920100},
                    ],
                },
                {
                    "propID": 302,
                    "marketID": 12,
                    "entities": [200],
                    "lines": [
                        {"book": "prizepicks", "line": 33.5, "lineTime": 1772920200},
                    ],
                },
            ],
        }

        parsed = _extract_rotowire_prizepicks_rows(payload)

        self.assertEqual(len(parsed), 1)
        self.assertEqual(parsed.iloc[0]["player_name"], "LeBron James")
        self.assertEqual(parsed.iloc[0]["team"], "LAL")
        self.assertEqual(parsed.iloc[0]["line_points"], 24.5)
        self.assertEqual(parsed.iloc[0]["line_rebounds"], 7.5)
        self.assertEqual(parsed.iloc[0]["line_pra"], 33.5)

    def test_generate_prizepicks_edges_uses_imported_lines(self) -> None:
        predictions = pd.DataFrame(
            [
                {
                    "player_name": "Jalen Brunson",
                    "team": "NYK",
                    "game_date": "2026-03-04",
                    "predicted_points": 30.0,
                    "predicted_rebounds": 3.1,
                    "predicted_assists": 7.4,
                    "predicted_pra": 40.5,
                    "projection_error_pct_estimate": 6.0,
                    "line_points_stddev": 0.2,
                    "line_points_books_count": 4,
                    "line_points_snapshot_age_minutes": 20,
                    "confidence_flag": "high_confidence",
                    "historical_games_used": 40,
                    "season_priors_available": True,
                }
            ]
        )
        lines_text = "\n".join(
            [
                "player_name,team,game_date,market,line",
                "Jalen Brunson,NYK,2026-03-04,points,22.5",
            ]
        )

        with tempfile.TemporaryDirectory() as temp_dir:
            temp_dir_path = Path(temp_dir)
            predictions_path = temp_dir_path / "predictions.csv"
            lines_path = temp_dir_path / "prizepicks_lines.csv"
            edges_path = temp_dir_path / "prizepicks_edges.csv"
            predictions.to_csv(predictions_path, index=False)
            import_prizepicks_lines_text(lines_text, output_path=lines_path)

            result = generate_prizepicks_edges(
                predictions_path=predictions_path,
                lines_path=lines_path,
                output_path=edges_path,
                slate_date="2026-03-04",
            )
            saved = pd.read_csv(edges_path)

        self.assertEqual(result["matched_rows"], 1)
        self.assertGreater(float(saved.iloc[0]["edge"]), 0.0)
        self.assertIn(saved.iloc[0]["recommendation"], {"Higher", "Pass"})

    def test_generate_prizepicks_edges_respects_uncertainty_band(self) -> None:
        predictions = pd.DataFrame(
            [
                {
                    "player_name": "Volatile Player",
                    "team": "BOS",
                    "game_date": "2026-03-04",
                    "predicted_points": 29.0,
                    "predicted_rebounds": 6.0,
                    "predicted_assists": 5.0,
                    "predicted_pra": 40.0,
                    "projection_error_pct_estimate": 60.0,
                    "confidence_flag": "low_confidence",
                    "historical_games_used": 4,
                    "season_priors_available": True,
                }
            ]
        )
        lines_text = "\n".join(
            [
                "player_name,team,game_date,market,line",
                "Volatile Player,BOS,2026-03-04,points,24.0",
            ]
        )

        with tempfile.TemporaryDirectory() as temp_dir:
            temp_dir_path = Path(temp_dir)
            predictions_path = temp_dir_path / "predictions.csv"
            lines_path = temp_dir_path / "prizepicks_lines.csv"
            edges_path = temp_dir_path / "prizepicks_edges.csv"
            predictions.to_csv(predictions_path, index=False)
            import_prizepicks_lines_text(lines_text, output_path=lines_path)

            result = generate_prizepicks_edges(
                predictions_path=predictions_path,
                lines_path=lines_path,
                output_path=edges_path,
                slate_date="2026-03-04",
            )
            saved = pd.read_csv(edges_path)

        self.assertEqual(result["matched_rows"], 1)
        self.assertEqual(saved.iloc[0]["recommendation"], "Pass")
        self.assertGreater(float(saved.iloc[0]["uncertainty_band"]), float(saved.iloc[0]["edge_threshold"]))


if __name__ == "__main__":
    unittest.main()
