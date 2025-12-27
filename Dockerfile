FROM apache/airflow:2.7.1-python3.11

WORKDIR /opt/airflow

COPY requirements.txt /opt/airflow/requirements.txt

USER airflow
RUN pip install --no-cache-dir -r /opt/airflow/requirements.txt

ENV PYTHONPATH="/opt/airflow:/opt/airflow/batch:/opt/airflow/ingestion:/opt/airflow/warehouse"

CMD ["airflow", "standalone"]
