FROM apache/airflow:2.10.5-python3.12

USER airflow
COPY airflow/requirements.txt /tmp/airflow-requirements.txt
RUN python -m venv /home/airflow/etl-venv \
	&& /home/airflow/etl-venv/bin/pip install --no-cache-dir -r /tmp/airflow-requirements.txt
