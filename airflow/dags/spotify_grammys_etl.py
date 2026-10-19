"""Airflow DAG for the Great Expectations-validated warehouse load."""

from datetime import datetime, timedelta
import os
import sys

PROJECT_PATH = os.getenv("PROJECT_ROOT", "/opt/airflow/project")
sys.path.insert(0, os.path.join(PROJECT_PATH, "src"))

from airflow import DAG  # noqa: E402
from airflow.operators.bash import BashOperator  # noqa: E402


default_args = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="spotify_grammys_data_warehouse",
    description="Validate Grammy and Spotify sources with Great Expectations, then load the PostgreSQL star schema.",
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False,
    max_active_runs=1,
    tags=["grammys", "spotify", "etl", "great-expectations", "warehouse"],
) as dag:
    validate_and_prepare = BashOperator(
        task_id="validate_and_prepare",
        bash_command=(
            "PYTHONPATH=/opt/airflow/project/src "
            "/home/airflow/etl-venv/bin/python -c "
            "'from spotify_grammys_etl.orchestrator import prepare_pipeline; "
            "print(prepare_pipeline())'"
        ),
    )
    load_warehouse_task = BashOperator(
        task_id="load_warehouse",
        bash_command=(
            "PYTHONPATH=/opt/airflow/project/src "
            "/home/airflow/etl-venv/bin/python -c "
            "'from spotify_grammys_etl.orchestrator import load_staged_pipeline; "
            "print(load_staged_pipeline())'"
        ),
    )

    validate_and_prepare >> load_warehouse_task
