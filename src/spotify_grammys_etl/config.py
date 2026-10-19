"""Environment-backed configuration and database connections for the ETL."""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path
from urllib.parse import quote_plus

from sqlalchemy import create_engine
from sqlalchemy.engine import Engine


def _load_local_environment() -> None:
    """Load simple KEY=VALUE entries from .env without overriding process values."""
    env_path = Path(__file__).resolve().parents[2] / ".env"
    if not env_path.exists():
        return
    for raw_line in env_path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        os.environ.setdefault(key.strip(), value.strip().strip("\"'").replace("\\#", "#"))


_load_local_environment()


@dataclass(frozen=True)
class PipelineConfig:
    """Resolved runtime configuration. Credentials come from environment variables."""

    root: Path
    awards_table: str = "awards"
    mysql_user: str = "root"
    mysql_password: str = ""
    mysql_host: str = "localhost"
    mysql_port: str = "3307"
    mysql_database: str = "grammys"
    postgres_user: str = "music_admin"
    postgres_password: str = ""
    postgres_host: str = "localhost"
    postgres_port: str = "5442"
    postgres_database: str = "music_dwh"
    spotify_path: Path | None = None
    matches_path: Path | None = None
    staging_dir: Path | None = None
    reports_dir: Path | None = None
    warehouse_schema_path: Path | None = None

    @classmethod
    def from_environment(cls) -> "PipelineConfig":
        root = Path(os.getenv("PROJECT_ROOT", Path(__file__).resolve().parents[2])).resolve()
        return cls(
            root=root,
            awards_table=os.getenv("MYSQL_AWARDS_TABLE", "awards"),
            mysql_user=os.getenv("MYSQL_USER", "root"),
            mysql_password=os.getenv("MYSQL_PASSWORD", ""),
            mysql_host=os.getenv("MYSQL_HOST", "localhost"),
            mysql_port=os.getenv("MYSQL_PORT", "3307"),
            mysql_database=os.getenv("MYSQL_DATABASE", "grammys"),
            postgres_user=os.getenv("DB_USER", "music_admin"),
            postgres_password=os.getenv("DB_PASSWORD", ""),
            postgres_host=os.getenv("DB_HOST", "localhost"),
            postgres_port=os.getenv("DB_PORT", "5442"),
            postgres_database=os.getenv("POSTGRES_DB", "music_dwh"),
            spotify_path=Path(os.getenv("SPOTIFY_CSV_PATH", root / "data/raw/spotify_dataset.csv")),
            matches_path=Path(os.getenv("GRAMMY_SPOTIFY_MATCHES_PATH", root / "data/interim/grammys_spotify_merged.csv")),
            staging_dir=Path(os.getenv("ETL_STAGING_DIR", root / "data/staging")),
            reports_dir=Path(os.getenv("ETL_REPORT_DIR", root / "reports/great_expectations")),
            warehouse_schema_path=Path(os.getenv("WAREHOUSE_SCHEMA_PATH", root / "sql/dwh/schema.sql")),
        )

    def mysql_engine(self) -> Engine:
        """Build the Grammy source MySQL engine."""
        if not self.mysql_password:
            raise ValueError("MYSQL_PASSWORD is required; configure it in the environment or local .env file")
        password = quote_plus(self.mysql_password)
        return create_engine(
            f"mysql+pymysql://{self.mysql_user}:{password}@{self.mysql_host}:"
            f"{self.mysql_port}/{self.mysql_database}",
            pool_pre_ping=True,
        )

    def warehouse_engine(self) -> Engine:
        """Build the PostgreSQL warehouse engine."""
        if not self.postgres_password:
            raise ValueError("DB_PASSWORD is required; configure it in the environment or local .env file")
        password = quote_plus(self.postgres_password)
        return create_engine(
            f"postgresql+psycopg2://{self.postgres_user}:{password}@{self.postgres_host}:"
            f"{self.postgres_port}/{self.postgres_database}",
            pool_pre_ping=True,
        )


def get_config() -> PipelineConfig:
    """Return configuration after loading local .env values."""
    return PipelineConfig.from_environment()
