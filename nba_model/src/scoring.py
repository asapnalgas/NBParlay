from __future__ import annotations

import pandas as pd


DK_COMPONENT_COLUMNS = [
    "points",
    "rebounds",
    "assists",
    "steals",
    "blocks",
    "turnovers",
    "three_points_made",
]

FD_COMPONENT_COLUMNS = [
    "points",
    "rebounds",
    "assists",
    "steals",
    "blocks",
    "turnovers",
]


def _series_or_zero(frame: pd.DataFrame, column: str) -> pd.Series:
    if column in frame.columns:
        return pd.to_numeric(frame[column], errors="coerce").fillna(0.0)
    return pd.Series(0.0, index=frame.index, dtype="float64")


def calculate_pra(frame: pd.DataFrame) -> pd.Series:
    required = {"points", "rebounds", "assists"}
    if not required.issubset(frame.columns):
        raise ValueError("PRA requires points, rebounds, and assists columns.")
    return _series_or_zero(frame, "points") + _series_or_zero(frame, "rebounds") + _series_or_zero(frame, "assists")


def _count_double_double_categories(frame: pd.DataFrame) -> pd.Series:
    categories = pd.DataFrame(
        {
            "points": _series_or_zero(frame, "points"),
            "rebounds": _series_or_zero(frame, "rebounds"),
            "assists": _series_or_zero(frame, "assists"),
            "steals": _series_or_zero(frame, "steals"),
            "blocks": _series_or_zero(frame, "blocks"),
        }
    )
    return (categories >= 10).sum(axis=1)


def calculate_draftkings_points(frame: pd.DataFrame) -> pd.Series:
    missing = [column for column in DK_COMPONENT_COLUMNS if column not in frame.columns]
    if missing:
        raise ValueError(f"DraftKings scoring requires columns: {missing}")

    categories_at_ten = _count_double_double_categories(frame)
    double_double_bonus = (categories_at_ten >= 2).astype(float) * 1.5
    triple_double_bonus = (categories_at_ten >= 3).astype(float) * 3.0

    return (
        _series_or_zero(frame, "points")
        + 1.25 * _series_or_zero(frame, "rebounds")
        + 1.5 * _series_or_zero(frame, "assists")
        + 2.0 * _series_or_zero(frame, "steals")
        + 2.0 * _series_or_zero(frame, "blocks")
        - 0.5 * _series_or_zero(frame, "turnovers")
        + 0.5 * _series_or_zero(frame, "three_points_made")
        + double_double_bonus
        + triple_double_bonus
    )


def calculate_fanduel_points(frame: pd.DataFrame) -> pd.Series:
    missing = [column for column in FD_COMPONENT_COLUMNS if column not in frame.columns]
    if missing:
        raise ValueError(f"FanDuel scoring requires columns: {missing}")

    return (
        _series_or_zero(frame, "points")
        + 1.2 * _series_or_zero(frame, "rebounds")
        + 1.5 * _series_or_zero(frame, "assists")
        + 3.0 * _series_or_zero(frame, "steals")
        + 3.0 * _series_or_zero(frame, "blocks")
        - 1.0 * _series_or_zero(frame, "turnovers")
    )
