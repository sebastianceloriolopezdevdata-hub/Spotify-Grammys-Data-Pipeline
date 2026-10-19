"""Data transformations used to create warehouse dimension records."""

from __future__ import annotations

import hashlib
from typing import Any

import pandas as pd

SPOTIFY_NUMERIC_COLUMNS = [
    "popularity", "duration_ms", "explicit", "danceability", "energy", "key", "loudness",
    "mode", "speechiness", "acousticness", "instrumentalness", "liveness", "valence",
    "tempo", "time_signature",
]


def transform_awards(awards: pd.DataFrame) -> pd.DataFrame:
    """Clean Grammy records and create a deterministic award dimension."""
    required = {"year", "category", "nominee"}
    _require_columns(awards, required, "Grammy awards")

    columns = [
        column for column in ("year", "title", "category", "nominee", "artist", "workers", "winner")
        if column in awards.columns
    ]
    dimension = awards[columns].copy()
    for column in ("title", "category", "nominee", "artist", "workers"):
        if column not in dimension:
            dimension[column] = ""
        dimension[column] = dimension[column].fillna("").astype(str).str.strip()
    if "winner" not in dimension:
        dimension["winner"] = True

    dimension["year"] = pd.to_numeric(dimension["year"], errors="raise").astype("int64")
    dimension["winner"] = dimension["winner"].map(_as_bool)
    dimension = dimension.drop_duplicates(
        ["year", "category", "nominee", "artist"], keep="first"
    ).reset_index(drop=True)
    dimension.insert(
        0,
        "award_key",
        dimension.apply(
            lambda row: _stable_key("award", row["year"], row["category"], row["nominee"], row["artist"]),
            axis=1,
        ),
    )
    return dimension[["award_key", "year", "title", "category", "nominee", "artist", "workers", "winner"]]


def transform_spotify(spotify: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Consolidate repeated Spotify IDs and quarantine unusable track rows."""
    _require_columns(spotify, {"track_id", "track_name", "artists"}, "Spotify tracks")
    source = spotify.copy()

    aggregation: dict[str, Any] = {
        column: "first" for column in source.columns if column not in {"track_id", "track_genre"}
    }
    if "track_genre" in source.columns:
        aggregation["track_genre"] = lambda values: "; ".join(pd.unique(values.dropna().astype(str)))
    tracks = source.groupby("track_id", as_index=False, sort=False, dropna=False).agg(aggregation)

    for column in ("artists", "album_name", "track_name", "track_genre"):
        if column not in tracks:
            tracks[column] = ""
        tracks[column] = tracks[column].fillna("").astype(str)
    for column in SPOTIFY_NUMERIC_COLUMNS:
        if column not in tracks:
            tracks[column] = None

    duration = pd.to_numeric(tracks["duration_ms"], errors="coerce")
    invalid_mask = (
        tracks["track_id"].isna()
        | tracks["track_name"].str.strip().eq("")
        | duration.isna()
        | duration.le(0)
    )
    quarantine = tracks.loc[invalid_mask].copy()
    tracks = tracks.loc[~invalid_mask].copy()
    tracks["track_id"] = tracks["track_id"].astype(str)
    tracks = tracks[
        ["track_id", "artists", "album_name", "track_name", "track_genre", *SPOTIFY_NUMERIC_COLUMNS]
    ].drop_duplicates("track_id", keep="first").reset_index(drop=True)
    return tracks, quarantine.reset_index(drop=True)


def _stable_key(*values: Any) -> str:
    """Build a stable SHA-256 surrogate key from business-key values."""
    serialized = "|".join("" if pd.isna(value) else str(value).strip() for value in values)
    return hashlib.sha256(serialized.encode("utf-8")).hexdigest()


def _as_bool(value: object) -> bool:
    if isinstance(value, bool):
        return value
    if pd.isna(value):
        return False
    return str(value).strip().casefold() in {"1", "true", "yes", "y"}


def _require_columns(dataframe: pd.DataFrame, required: set[str], name: str) -> None:
    missing = sorted(required - set(dataframe.columns))
    if missing:
        raise ValueError(f"{name} is missing required columns: {missing}")
