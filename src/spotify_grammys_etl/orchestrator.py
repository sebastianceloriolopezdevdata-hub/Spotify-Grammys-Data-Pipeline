"""ETL orchestration: extract -> validate 1 -> transform -> merge -> validate 2 -> stage."""

from __future__ import annotations

import json
from pathlib import Path

import pandas as pd
from sqlalchemy.engine import Engine

from spotify_grammys_etl.config import PipelineConfig, get_config
from spotify_grammys_etl.data_quality import validate_stage_1, validate_stage_2
from spotify_grammys_etl.extract import extract_all
from spotify_grammys_etl.load_dw import load_warehouse
from spotify_grammys_etl.merge import merge_grammy_spotify
from spotify_grammys_etl.transform import transform_awards, transform_spotify


def prepare_pipeline(
    output_dir: Path | None = None,
    awards_engine: Engine | None = None,
    spotify_path: Path | None = None,
    matches_path: Path | None = None,
    config: PipelineConfig | None = None,
) -> Path:
    """Run extraction, Validation 1, transformations, merge, and Validation 2."""
    settings = config or get_config()
    reports_dir = settings.reports_dir
    staging_dir = output_dir or settings.staging_dir
    if reports_dir is None or staging_dir is None:
        raise ValueError("ETL_REPORT_DIR and ETL_STAGING_DIR must be configured")

    # Extract raw source datasets.
    awards_source, spotify_source, approved_matches = extract_all(
        awards_engine=awards_engine,
        spotify_path=spotify_path,
        matches_path=matches_path,
        config=settings,
    )

    # Validation 1 blocks bad/missing source data before transformations begin.
    validate_stage_1(awards_source, spotify_source, approved_matches, reports_dir)

    # Transform dimensions separately, then merge curated approved links into the fact.
    awards_dimension = transform_awards(awards_source)
    spotify_dimension, quarantined_tracks = transform_spotify(spotify_source)
    fact = merge_grammy_spotify(awards_dimension, spotify_dimension, approved_matches)

    if not quarantined_tracks.empty:
        reports_dir.mkdir(parents=True, exist_ok=True)
        quarantine_path = reports_dir / "spotify_tracks_quarantine.csv"
        quarantined_tracks.to_csv(quarantine_path, index=False)
        quarantine_report = {
            "dataset": "spotify_tracks",
            "quarantined_rows": int(len(quarantined_tracks)),
            "reason": "missing track ID/name or non-positive/invalid duration_ms",
            "quarantine_file": str(quarantine_path),
        }
        (reports_dir / "spotify_tracks_quarantine.json").write_text(
            json.dumps(quarantine_report, indent=2), encoding="utf-8"
        )

    # Validation 2 checks transformed nulls, unique keys, duplicates, and source totals.
    validate_stage_2(
        awards_source,
        spotify_source,
        approved_matches,
        awards_dimension,
        spotify_dimension,
        fact,
        quarantined_tracks,
        reports_dir,
    )

    staging_dir.mkdir(parents=True, exist_ok=True)
    awards_dimension.to_csv(staging_dir / "dim_grammy_award.csv", index=False)
    spotify_dimension.to_csv(staging_dir / "dim_spotify_track.csv", index=False)
    fact.to_csv(staging_dir / "fact_grammy_spotify_match.csv", index=False)
    return staging_dir


def run_pipeline(
    config: PipelineConfig | None = None,
    awards_engine: Engine | None = None,
    warehouse_engine: Engine | None = None,
) -> dict[str, int]:
    """Run all ETL stages, including the transactional load and Validation 3."""
    settings = config or get_config()
    staging_dir = prepare_pipeline(awards_engine=awards_engine, config=settings)
    return load_warehouse(
        staging_dir=staging_dir,
        engine=warehouse_engine,
        config=settings,
    )


def load_staged_pipeline(
    staging_dir: Path | None = None,
    engine: Engine | None = None,
    config: PipelineConfig | None = None,
) -> dict[str, int]:
    """Load existing staging data and execute Validation 3 (used by Airflow)."""
    settings = config or get_config()
    return load_warehouse(staging_dir=staging_dir, engine=engine, config=settings)


if __name__ == "__main__":
    print(run_pipeline())
