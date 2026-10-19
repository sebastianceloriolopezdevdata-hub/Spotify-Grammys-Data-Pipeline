"""Backward-compatible entry point for the modular Spotify-Grammys ETL."""

from __future__ import annotations

from pathlib import Path

import pandas as pd
from sqlalchemy.engine import Engine

from spotify_grammys_etl.config import PipelineConfig, get_config
from spotify_grammys_etl.data_quality import validate_stage_1, validate_stage_2
from spotify_grammys_etl.extract import extract_awards, extract_matches, extract_spotify
from spotify_grammys_etl.load_dw import load_warehouse
from spotify_grammys_etl.merge import merge_grammy_spotify
from spotify_grammys_etl.orchestrator import prepare_pipeline, run_pipeline
from spotify_grammys_etl.transform import transform_awards, transform_spotify


def mysql_engine() -> Engine:
    """Return the configured Grammy source engine."""
    return get_config().mysql_engine()


def postgres_engine() -> Engine:
    """Return the configured PostgreSQL warehouse engine."""
    return get_config().warehouse_engine()


def validate_and_transform(
    awards: pd.DataFrame,
    spotify: pd.DataFrame,
    matches: pd.DataFrame,
    report_dir: Path | None = None,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """Compatibility helper that runs both pre-load validation stages."""
    settings = get_config()
    reports = report_dir or settings.reports_dir
    if reports is None:
        raise ValueError("ETL_REPORT_DIR is not configured")

    validate_stage_1(awards, spotify, matches, reports)
    award_dimension = transform_awards(awards)
    spotify_dimension, quarantined_tracks = transform_spotify(spotify)
    fact = merge_grammy_spotify(award_dimension, spotify_dimension, matches)
    validate_stage_2(
        awards,
        spotify,
        matches,
        award_dimension,
        spotify_dimension,
        fact,
        quarantined_tracks,
        reports,
    )
    return award_dimension, spotify_dimension, fact


__all__ = [
    "extract_awards",
    "extract_matches",
    "extract_spotify",
    "load_warehouse",
    "mysql_engine",
    "postgres_engine",
    "prepare_pipeline",
    "run_pipeline",
    "validate_and_transform",
]


if __name__ == "__main__":
    print(run_pipeline())
