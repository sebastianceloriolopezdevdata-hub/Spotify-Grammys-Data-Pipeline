"""PostgreSQL data-warehouse schema management and transactional loading."""

from __future__ import annotations

from pathlib import Path

import pandas as pd
from sqlalchemy.engine import Engine

from spotify_grammys_etl.config import PipelineConfig, get_config
from spotify_grammys_etl.data_quality import validate_post_load

TABLE_FILES = {
    "dim_grammy_award": "dim_grammy_award.csv",
    "dim_spotify_track": "dim_spotify_track.csv",
    "fact_grammy_spotify_match": "fact_grammy_spotify_match.csv",
}


def load_warehouse(
    staging_dir: Path | None = None,
    engine: Engine | None = None,
    schema_path: Path | None = None,
    report_dir: Path | None = None,
    config: PipelineConfig | None = None,
) -> dict[str, int]:
    """Create the warehouse schema, replace curated tables, then verify row counts."""
    settings = config or get_config()
    staged = staging_dir or settings.staging_dir
    ddl_path = schema_path or settings.warehouse_schema_path
    reports = report_dir or settings.reports_dir
    warehouse = engine or settings.warehouse_engine()
    if staged is None:
        raise ValueError("ETL_STAGING_DIR is not configured")
    if ddl_path is None or not ddl_path.exists():
        raise FileNotFoundError(f"Warehouse schema SQL does not exist: {ddl_path}")
    if reports is None:
        raise ValueError("ETL_REPORT_DIR is not configured")

    tables = {
        name: pd.read_csv(staged / filename, keep_default_na=False)
        for name, filename in TABLE_FILES.items()
    }
    tables["fact_grammy_spotify_match"]["match_score"] = pd.to_numeric(
        tables["fact_grammy_spotify_match"]["match_score"], errors="coerce"
    )
    ddl_statements = _split_sql_statements(ddl_path.read_text(encoding="utf-8"))

    # Use one transaction so a failed insert restores the previous curated snapshot.
    with warehouse.begin() as connection:
        for statement in ddl_statements:
            connection.exec_driver_sql(statement)
        connection.exec_driver_sql(
            "TRUNCATE TABLE fact_grammy_spotify_match, "
            "dim_spotify_track, dim_grammy_award CASCADE"
        )
        tables["dim_grammy_award"].to_sql(
            "dim_grammy_award", connection, if_exists="append", index=False, method="multi", chunksize=500
        )
        tables["dim_spotify_track"].to_sql(
            "dim_spotify_track", connection, if_exists="append", index=False, method="multi", chunksize=1000
        )
        tables["fact_grammy_spotify_match"].to_sql(
            "fact_grammy_spotify_match", connection, if_exists="append", index=False, method="multi", chunksize=1000
        )

    counts = {table: int(len(dataframe)) for table, dataframe in tables.items()}
    validate_post_load(warehouse, counts, reports)
    return counts


def _split_sql_statements(schema_sql: str) -> list[str]:
    """Split this project's semicolon-terminated DDL, ignoring full-line comments."""
    uncommented = "\n".join(
        line for line in schema_sql.splitlines() if not line.lstrip().startswith("--")
    )
    return [statement.strip() for statement in uncommented.split(";") if statement.strip()]
