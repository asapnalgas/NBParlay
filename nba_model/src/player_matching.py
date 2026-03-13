from __future__ import annotations

import re
import unicodedata
from pathlib import Path

import pandas as pd


def normalize_player_name(value: object) -> str:
    text = str(value or "").strip()
    if not text:
        return ""
    text = unicodedata.normalize("NFKD", text)
    text = "".join(char for char in text if not unicodedata.combining(char))
    text = text.replace("’", "'")
    text = re.sub(r"[^a-zA-Z0-9'\- ]+", " ", text)
    text = text.replace("-", " ")
    text = re.sub(r"\s+", " ", text).strip().lower()
    return text


def normalize_team_code(value: object) -> str:
    text = str(value or "").strip().upper()
    text = re.sub(r"[^A-Z]", "", text)
    return text[:3]


def load_alias_overrides(path: Path) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame(columns=["alias_name", "player_name", "team"])

    frame = pd.read_csv(path)
    if frame.empty:
        return pd.DataFrame(columns=["alias_name", "player_name", "team"])

    for column in ["alias_name", "player_name"]:
        if column not in frame.columns:
            raise ValueError(f"Alias override file must include '{column}'.")
    if "team" not in frame.columns:
        frame["team"] = ""

    working = frame.copy()
    working["alias_key"] = working["alias_name"].map(normalize_player_name)
    working["player_name"] = working["player_name"].astype(str).str.strip()
    working["team"] = working["team"].map(normalize_team_code)
    working = working[working["alias_key"] != ""]
    return working[["alias_key", "player_name", "team"]].drop_duplicates()


def apply_alias_overrides(frame: pd.DataFrame, alias_frame: pd.DataFrame) -> pd.DataFrame:
    if frame.empty or alias_frame.empty or "player_name" not in frame.columns:
        return frame

    working = frame.copy()
    working["__player_key"] = working["player_name"].map(normalize_player_name)
    alias_lookup = alias_frame.drop_duplicates(subset=["alias_key", "team"], keep="last")

    if "team" in working.columns:
        working["__team_key"] = working["team"].map(normalize_team_code)
        merged = working.merge(
            alias_lookup,
            left_on=["__player_key", "__team_key"],
            right_on=["alias_key", "team"],
            how="left",
            suffixes=("", "__alias"),
        )
        missing_mask = merged["player_name__alias"].isna()
        if missing_mask.any():
            teamless_alias = alias_lookup[alias_lookup["team"] == ""]
            fallback = merged.loc[missing_mask].drop(columns=["alias_key", "player_name__alias", "team__alias"], errors="ignore")
            fallback = fallback.merge(
                teamless_alias,
                left_on="__player_key",
                right_on="alias_key",
                how="left",
                suffixes=("", "__alias"),
            )
            merged.loc[missing_mask, "player_name__alias"] = fallback["player_name__alias"].values
    else:
        merged = working.merge(
            alias_lookup[alias_lookup["team"] == ""],
            left_on="__player_key",
            right_on="alias_key",
            how="left",
            suffixes=("", "__alias"),
        )

    if "player_name__alias" in merged.columns:
        merged["player_name"] = merged["player_name__alias"].combine_first(merged["player_name"])

    return merged.drop(columns=[column for column in merged.columns if column.startswith("__") or column.endswith("__alias") or column == "alias_key"], errors="ignore")


def add_player_keys(frame: pd.DataFrame) -> pd.DataFrame:
    working = frame.copy()
    if "player_name" in working.columns:
        working["player_key"] = working["player_name"].map(normalize_player_name)
    if "team" in working.columns:
        working["team_key"] = working["team"].map(normalize_team_code)
    return working
