# Dockerfile (루트) - 전체 교체

FROM apache/airflow:2.7.1-python3.11

WORKDIR /opt/airflow

COPY requirements.txt /opt/airflow/requirements.txt

# Airflow 공식 이미지 권장: airflow 유저로 pip 설치
USER airflow
RUN pip install --no-cache-dir -r /opt/airflow/requirements.txt

# 패키징 메타파일 먼저 복사 (캐시 효율)
COPY pyproject.toml /opt/airflow/pyproject.toml

# 코드 포함(볼륨 마운트 시 덮어씌워짐)
COPY airflow/dags /opt/airflow/dags
COPY batch /opt/airflow/batch
COPY ingestion /opt/airflow/ingestion
COPY warehouse /opt/airflow/warehouse

# 프로젝트 자체를 editable 설치해서 import 안정화
RUN pip install --no-cache-dir -e /opt/airflow

# (이제 PYTHONPATH 억지 설정이 없어도 import 됩니다)
CMD ["airflow", "standalone"]
