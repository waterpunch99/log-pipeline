# Dockerfile (루트)

FROM apache/airflow:2.7.1-python3.11

WORKDIR /opt/airflow

COPY requirements.txt /opt/airflow/requirements.txt

# Airflow 공식 이미지 권장: airflow 유저로 pip 설치
USER airflow
RUN pip install --no-cache-dir -r /opt/airflow/requirements.txt

# 코드 포함(볼륨 마운트 시 덮어씌워짐)
COPY airflow/dags /opt/airflow/dags
COPY batch /opt/airflow/batch
COPY ingestion /opt/airflow/ingestion
COPY warehouse /opt/airflow/warehouse

ENV PYTHONPATH="/opt/airflow:/opt/airflow/batch:/opt/airflow/ingestion:/opt/airflow/warehouse"

CMD ["airflow", "standalone"]
