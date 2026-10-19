"""Source extraction functions for Grammy, Spotify, and curated record links."""

from __future__ import annotations

from pathlib import Path

import pandas as pd
from sqlalchemy import text
from sqlalchemy.engine import Engine

from spotify_grammys_etl.config import PipelineConfig, get_config

AWARD_COLUMNS = ["year", "title", "published_at", "updated_at", "category", "nominee", "artist", "workers", "img", "winner"]
SPOTIFY_REQUIRED_COLUMNS = [
    "track_id", "artists", "album_name", "track_name", "popularity", "duration_ms",
    "explicit", "danceability", "energy", "key", "loudness", "mode", "speechiness",
    "acousticness", "instrumentalness", "liveness", "valence", "tempo", "time_signature",
    "track_genre",
]
MATCH_REQUIRED_COLUMNS = ["year", "category", "nominee", "track_id", "track_name"]


def extract_awards(engine: Engine | None = None, config: PipelineConfig | None = None) -> pd.DataFrame:
    """Read historical Grammy rows from the source MySQL database."""
    settings = config or get_config()
    source_engine = engine or settings.mysql_engine()
    table = settings.awards_table
    if not table.replace("_", "").isalnum():
        raise ValueError(f"Unsafe MySQL table identifier: {table!r}")
    with source_engine.connect() as connection:
        awards = pd.read_sql(text(f"SELECT * FROM `{table}`"), connection)
    if "winner" in awards.columns:
        awards["winner"] = awards["winner"].map(_as_bool)
    return awards


def extract_spotify(path: Path | None = None, config: PipelineConfig | None = None) -> pd.DataFrame:
    """Read the raw Spotify CSV without business transformations."""
    settings = config or get_config()
    source_path = path or settings.spotify_path
    if source_path is None or not source_path.exists():
        raise FileNotFoundError(f"Spotify source CSV does not exist: {source_path}")
    spotify = pd.read_csv(source_path)
    spotify = spotify.loc[:, ~spotify.columns.astype(str).str.match(r"^Unnamed")]
    return spotify


def extract_matches(path: Path | None = None, config: PipelineConfig | None = None) -> pd.DataFrame:
    """Read approved Grammy-to-Spotify links produced by the review notebook."""
    settings = config or get_config()
    source_path = path or settings.matches_path
    if source_path is None or not source_path.exists():
        raise FileNotFoundError(
            f"Curated linkage CSV does not exist: {source_path}. Run the record-linkage notebook first."
        )
    matches = pd.read_csv(source_path)
    matches = matches.loc[:, ~matches.columns.astype(str).str.match(r"^Unnamed")]
    return matches


def extract_all(
    awards_engine: Engine | None = None,
    spotify_path: Path | None = None,
    matches_path: Path | None = None,
    config: PipelineConfig | None = None,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """Extract all input datasets needed by this curated ETL."""
    settings = config or get_config()
    return (
        extract_awards(engine=awards_engine, config=settings),
        extract_spotify(path=spotify_path, config=settings),
        extract_matches(path=matches_path, config=settings),
    )


def _as_bool(value: object) -> bool:
    if isinstance(value, bool):
        return value
    if pd.isna(value):
        return False
    return str(value).strip().casefold() in {"1", "true", "yes", "y"}
