from datetime import datetime, timedelta, timezone
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

def raw_to_stg_task(**context):
    start = context["data_interval_start"]
    dt = start.strftime("%Y-%m-%d")

    from batch.raw_to_stg import run
    run(dt=dt)  # hour 제거 → 날짜 파티션 전체 스캔으로 변경

with DAG(
    dag_id="event_log_pipeline",
    start_date=datetime(2025, 12, 23, tzinfo=timezone.utc),
    schedule="@daily",
    catchup=False,  # ← 이 한 번만 있어야 합니다
    tags=["github", "pipeline"],
    default_args={"retries": 3, "retry_delay": timedelta(minutes=2)},
    template_searchpath=[
        "/opt/airflow/batch",
        "/opt/airflow/warehouse/ddl",
        "/opt/airflow/warehouse/dm"
    ],
) as dag:


    t1 = PythonOperator(task_id="raw_to_stg", python_callable=raw_to_stg_task)

    t2 = SQLExecuteQueryOperator(
        task_id="stg_to_dw",
        conn_id="event_dw",
        sql="stg_to_dw.sql",
        parameters={"window_start": "{{data_interval_start}}", "window_end": "{{data_interval_end}}"}
    )

    t3 = SQLExecuteQueryOperator(
        task_id="dm_hourly_volume",
        conn_id="event_dw",
        sql="dm_hourly_event_volume.sql",
        parameters={"window_start": "{{data_interval_start}}", "window_end": "{{data_interval_end}}"}
    )

    t4 = SQLExecuteQueryOperator(
        task_id="dm_type_ratio",
        conn_id="event_dw",
        sql="dm_event_type_ratio.sql",
        parameters={"window_start": "{{data_interval_start}}", "window_end": "{{data_interval_end}}"}
    )

    t1 >> t2 >> t3 >> t4
