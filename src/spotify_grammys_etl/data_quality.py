"""Great Expectations checks for extract, transform/merge, and post-load stages."""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import great_expectations as gx
import pandas as pd
from sqlalchemy import text
from sqlalchemy.engine import Engine


def validate_dataframe(
    dataframe: pd.DataFrame,
    dataset_name: str,
    required_columns: list[str],
    expectations: list[tuple[str, dict[str, Any]]],
    report_dir: Path,
    details: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Run a Great Expectations suite and persist a concise JSON report."""
    missing_columns = sorted(set(required_columns) - set(dataframe.columns))
    if missing_columns:
        raise ValueError(f"{dataset_name} is missing required columns: {missing_columns}")
    if dataframe.empty:
        raise ValueError(f"{dataset_name} contains no rows")

    context = gx.get_context(mode="ephemeral")
    datasource = context.data_sources.add_pandas(name=f"{dataset_name}_datasource")
    asset = datasource.add_dataframe_asset(name=f"{dataset_name}_asset")
    batch_definition = asset.add_batch_definition_whole_dataframe("whole_dataframe")
    batch = batch_definition.get_batch(batch_parameters={"dataframe": dataframe})
    validator = context.get_validator(
        batch=batch,
        create_expectation_suite_with_name=f"{dataset_name}_suite",
    )
    for expectation_name, kwargs in expectations:
        getattr(validator, expectation_name)(**kwargs)

    result = validator.validate()
    report = {
        "dataset": dataset_name,
        "validated_at_utc": datetime.now(timezone.utc).isoformat(),
        "row_count": int(len(dataframe)),
        "column_count": int(len(dataframe.columns)),
        "success": bool(result.success),
        "details": details or {},
        "expectations": [
            {
                "type": item.expectation_config.type,
                "success": bool(item.success),
                "kwargs": item.expectation_config.kwargs,
            }
            for item in result.results
        ],
    }
    report_dir.mkdir(parents=True, exist_ok=True)
    report_path = report_dir / f"{dataset_name}_validation.json"
    report_path.write_text(json.dumps(report, indent=2, default=str), encoding="utf-8")

    if not report["success"]:
        failed = [item["type"] for item in report["expectations"] if not item["success"]]
        raise ValueError(f"Great Expectations validation failed for {dataset_name}: {failed}. See {report_path}.")
    return report


def validate_stage_1(
    awards: pd.DataFrame,
    spotify: pd.DataFrame,
    matches: pd.DataFrame,
    report_dir: Path,
) -> list[dict[str, Any]]:
    """Validation 1: confirm source data arrived with expected schemas and ranges."""
    return [
        validate_dataframe(
            awards,
            "grammy_awards",
            ["year", "category", "nominee", "winner"],
            [
                ("expect_column_values_to_not_be_null", {"column": "year"}),
                ("expect_column_values_to_be_between", {"column": "year", "min_value": 1958, "max_value": 2019}),
                ("expect_column_values_to_not_be_null", {"column": "category"}),
                ("expect_column_values_to_not_be_null", {"column": "winner"}),
            ],
            report_dir,
        ),
        validate_dataframe(
            spotify,
            "spotify_tracks",
            ["track_id", "track_name", "artists", "popularity", "duration_ms", "danceability", "energy"],
            [
                ("expect_column_values_to_not_be_null", {"column": "track_id"}),
                ("expect_column_values_to_be_between", {"column": "popularity", "min_value": 0, "max_value": 100}),
                ("expect_column_values_to_be_between", {"column": "duration_ms", "min_value": 0}),
                ("expect_column_values_to_be_between", {"column": "danceability", "min_value": 0, "max_value": 1}),
                ("expect_column_values_to_be_between", {"column": "energy", "min_value": 0, "max_value": 1}),
            ],
            report_dir,
        ),
        validate_dataframe(
            matches,
            "grammy_spotify_matches",
            ["year", "category", "nominee", "track_id", "track_name"],
            [
                ("expect_column_values_to_not_be_null", {"column": "track_id"}),
                ("expect_column_values_to_not_be_null", {"column": "nominee"}),
                ("expect_column_values_to_not_be_null", {"column": "category"}),
            ],
            report_dir,
        ),
    ]


def validate_stage_2(
    source_awards: pd.DataFrame,
    source_spotify: pd.DataFrame,
    source_matches: pd.DataFrame,
    awards_dimension: pd.DataFrame,
    spotify_dimension: pd.DataFrame,
    fact: pd.DataFrame,
    quarantined_tracks: pd.DataFrame,
    report_dir: Path,
) -> list[dict[str, Any]]:
    """Validation 2: enforce transformed keys, completeness, uniqueness, and row totals."""
    expected_awards = int(source_awards.drop_duplicates(["year", "category", "nominee", "artist"]).shape[0])
    invalid_track_ids = set(quarantined_tracks["track_id"].dropna().astype(str))
    expected_tracks = int(
        source_spotify["track_id"].dropna().astype(str).nunique()
        - len(invalid_track_ids)
    )
    valid_track_ids = set(spotify_dimension["track_id"].astype(str))
    eligible_match_pairs = source_matches.loc[
        source_matches["track_id"].astype(str).isin(valid_track_ids),
        ["year", "category", "nominee", "track_id"],
    ].drop_duplicates()
    expected_facts = int(len(eligible_match_pairs))

    if len(awards_dimension) != expected_awards:
        raise ValueError(f"Grammy row total mismatch: source distinct={expected_awards}, dimension={len(awards_dimension)}")
    if len(spotify_dimension) != expected_tracks:
        raise ValueError(f"Spotify row total mismatch: expected valid tracks={expected_tracks}, dimension={len(spotify_dimension)}")
    if len(fact) != expected_facts:
        raise ValueError(f"Match row total mismatch: expected distinct eligible pairs={expected_facts}, fact={len(fact)}")
    if fact.duplicated(["award_key", "track_id"]).any():
        raise ValueError("Duplicate (award_key, track_id) pairs found in transformed fact table")

    return [
        validate_dataframe(
            awards_dimension,
            "transformed_grammy_awards",
            ["award_key", "year", "category", "nominee"],
            [
                ("expect_column_values_to_not_be_null", {"column": "award_key"}),
                ("expect_column_values_to_be_unique", {"column": "award_key"}),
                ("expect_column_values_to_not_be_null", {"column": "year"}),
                ("expect_column_values_to_not_be_null", {"column": "category"}),
            ],
            report_dir,
            {"source_distinct_rows": expected_awards, "transformed_rows": len(awards_dimension)},
        ),
        validate_dataframe(
            spotify_dimension,
            "transformed_spotify_tracks",
            ["track_id", "track_name", "duration_ms"],
            [
                ("expect_column_values_to_not_be_null", {"column": "track_id"}),
                ("expect_column_values_to_be_unique", {"column": "track_id"}),
                ("expect_column_values_to_not_be_null", {"column": "track_name"}),
                ("expect_column_values_to_be_between", {"column": "duration_ms", "min_value": 1}),
            ],
            report_dir,
            {
                "source_unique_track_ids": int(source_spotify["track_id"].dropna().astype(str).nunique()),
                "quarantined_rows": int(len(quarantined_tracks)),
                "transformed_rows": len(spotify_dimension),
            },
        ),
        validate_dataframe(
            fact,
            "transformed_grammy_spotify_matches",
            ["award_key", "track_id", "match_method"],
            [
                ("expect_column_values_to_not_be_null", {"column": "award_key"}),
                ("expect_column_values_to_not_be_null", {"column": "track_id"}),
                ("expect_column_values_to_not_be_null", {"column": "match_method"}),
            ],
            report_dir,
            {
                "eligible_source_pairs": expected_facts,
                "transformed_pairs": len(fact),
                "unique_award_track_pairs": int(fact[["award_key", "track_id"]].drop_duplicates().shape[0]),
            },
        ),
    ]


def validate_post_load(engine: Engine, expected_counts: dict[str, int], report_dir: Path) -> dict[str, Any]:
    """Validation 3: verify warehouse row counts, unique fact pairs, and foreign-key integrity."""
    with engine.connect() as connection:
        actual_counts = {
            table: int(connection.execute(text(f"SELECT COUNT(*) FROM {table}")).scalar_one())
            for table in expected_counts
        }
        duplicate_pairs = int(connection.execute(text(
            "SELECT COUNT(*) FROM (SELECT award_key, track_id FROM fact_grammy_spotify_match "
            "GROUP BY award_key, track_id HAVING COUNT(*) > 1) duplicate_pairs"
        )).scalar_one())
        orphan_awards = int(connection.execute(text(
            "SELECT COUNT(*) FROM fact_grammy_spotify_match f "
            "LEFT JOIN dim_grammy_award d USING (award_key) WHERE d.award_key IS NULL"
        )).scalar_one())
        orphan_tracks = int(connection.execute(text(
            "SELECT COUNT(*) FROM fact_grammy_spotify_match f "
            "LEFT JOIN dim_spotify_track d USING (track_id) WHERE d.track_id IS NULL"
        )).scalar_one())

    success = actual_counts == expected_counts and duplicate_pairs == 0 and orphan_awards == 0 and orphan_tracks == 0
    report = {
        "dataset": "warehouse_post_load",
        "validated_at_utc": datetime.now(timezone.utc).isoformat(),
        "success": success,
        "expected_counts": expected_counts,
        "actual_counts": actual_counts,
        "duplicate_fact_pairs": duplicate_pairs,
        "orphan_award_references": orphan_awards,
        "orphan_track_references": orphan_tracks,
    }
    report_dir.mkdir(parents=True, exist_ok=True)
    report_path = report_dir / "warehouse_post_load_validation.json"
    report_path.write_text(json.dumps(report, indent=2), encoding="utf-8")
    if not success:
        raise ValueError(f"Post-load validation failed. See {report_path}: {report}")
    return report


# Backward-compatible name for callers that used source validation directly.
validate_sources = validate_stage_1
