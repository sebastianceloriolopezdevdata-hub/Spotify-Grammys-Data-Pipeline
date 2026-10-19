# Spotify–Grammys Data Engineering Workshop

An end-to-end ETL workshop that connects the repository's Grammy and Spotify EDA work to data-quality checks, Airflow orchestration, and a PostgreSQL star-schema warehouse.

## Pipeline architecture

1. **Extract:** read historical awards from MySQL (`grammys.awards`), Spotify tracks from `data/raw/spotify_dataset.csv`, and approved links from the record-linkage notebook.
2. **Validation 1:** verify source arrival, required schema, key fields, and numeric ranges. Failure stops the pipeline before transformations.
3. **Transform:** clean award and Spotify records, consolidate Spotify IDs/genres, and quarantine invalid tracks.
4. **Merge:** connect approved Grammy–Spotify links to transformed dimensions and build fact rows.
5. **Validation 2:** check required keys, duplicate award/track pairs, dimension key uniqueness, and source-to-transformed row totals.
6. **Load DW:** replace the curated tables in PostgreSQL in one transaction.
7. **Validation 3:** compare expected and inserted row counts and check duplicate facts and orphan references.
8. **Orchestrate:** Airflow runs the preparation stages before warehouse loading and post-load verification.

The data-linkage notebook remains the source of truth for accepting matches. The ETL does not silently recalculate fuzzy matches or override human review decisions.

## Repository inputs

- Historical Grammy source: `sql/Grammys_db/seed/the_grammy_awards.csv` (loaded into MySQL by the existing compose setup).
- Spotify source: `data/raw/spotify_dataset.csv`.
- Curated Grammy–Spotify links: `data/interim/grammys_spotify_merged.csv` (produced by the merge/linkage notebook).
- Warehouse schema: `sql/dwh/schema.sql`.
- ETL modules: `src/spotify_grammys_etl/config.py`, `extract.py`, `data_quality.py`, `transform.py`, `merge.py`, `load_dw.py`, and `orchestrator.py`. `pipeline.py` remains a backwards-compatible entry point.
- Airflow DAG and image: `airflow/dags/spotify_grammys_etl.py` and `airflow/Dockerfile`.

## Configure and start the services

1. Copy `.env.example` to `.env` and replace every example password with a local secret. Do not commit `.env`.
2. Start the services from the repository root with Docker Compose. This starts MySQL, the PostgreSQL warehouse, Airflow's separate metadata database, and Airflow's webserver and scheduler.
3. Open the Airflow UI at `http://localhost:8080` using the configured Airflow admin credentials. Enable `spotify_grammys_data_warehouse` and trigger it after the match output exists.

Airflow runs on a daily schedule with catch-up disabled. Its first task writes Great Expectations reports and staging CSVs; its second task loads the warehouse. Docker Compose initializes the Grammy MySQL table from the seed CSV when its data volume is created for the first time and prepares writable persistent volumes for Airflow.

## Run locally

Use the project virtual environment, install `requirements.txt` if needed, and ensure MySQL and PostgreSQL are running with the values in `.env`. Run the ETL from the repository root with `PYTHONPATH=src python -m spotify_grammys_etl.pipeline`.

The local run uses the same modular extraction, validation, transformation, merge, and warehouse-load functions as Airflow. Great Expectations failures raise an error and prevent the load.

## Warehouse model

- `dim_grammy_award`: award year, ceremony title, category, nominee, credited artist, workers, and winner flag.
- `dim_spotify_track`: unique Spotify track and its audio features, popularity, album, artist credit, and consolidated genres.
- `fact_grammy_spotify_match`: accepted award/track links, linkage method, and optional match score; pair uniqueness and foreign keys are enforced.

The warehouse is a curated linked subset; unmatched Grammy records stay in the source database and unmatched/review decisions stay in the notebook's audit outputs. Re-running a successful load is idempotent because the pipeline atomically replaces these curated tables.

## Troubleshooting

- **Missing curated match CSV:** run the merge notebook through its final accepted-match export first.
- **MySQL connection or empty source:** check that `grammys-db` is healthy and that its data volume has been initialized with the seed CSV. If the seed/schema changes, recreate only the local MySQL volume when it is safe to discard its contents.
- **Great Expectations failure:** inspect the corresponding JSON report in `reports/great_expectations/`; correct or explicitly handle source-data issues before loading.
- **Airflow import errors:** check scheduler logs and confirm the Airflow image built with `airflow/requirements.txt`.
