# airflow/dags/github_pipeline.py
from datetime import datetime, timedelta, timezone

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

OVERLAP_DAYS = 2


def compact_then_raw_to_stg_task(**context):
    """
    흐름:
      1) dt별로 raw/success를 raw_compacted/success로 컴팩션(시간당 1개)
      2) raw_to_stg는 raw_compacted가 있으면 그걸 먼저 읽음(없으면 raw/success fallback)
      3) 늦게 도착한 이벤트를 위해 최근 N일을 매일 재처리(OVERLAP_DAYS)
    """
    interval_start = context["data_interval_start"]

    from batch.compact_raw import run as compact_run
    from batch.raw_to_stg import run as raw_to_stg_run

    for i in range(OVERLAP_DAYS, -1, -1):
        dt = (interval_start - timedelta(days=i)).strftime("%Y-%m-%d")

        # 1) small files 컴팩션
        compact_run(dt=dt)

        # 2) STG 적재
        raw_to_stg_run(dt=dt)


with DAG(
    dag_id="event_log_pipeline",
    start_date=datetime(2025, 12, 23, tzinfo=timezone.utc),
    schedule="@daily",
    catchup=False,
    tags=["github", "pipeline"],
    default_args={"retries": 3, "retry_delay": timedelta(minutes=2)},
    template_searchpath=[
        "/opt/airflow/batch",
        "/opt/airflow/warehouse/ddl",
        "/opt/airflow/warehouse/dm",
    ],
) as dag:

    t1 = PythonOperator(task_id="compact_then_raw_to_stg", python_callable=compact_then_raw_to_stg_task)

    window_start_tmpl = "{{ (data_interval_start - macros.timedelta(days=" + str(OVERLAP_DAYS) + ")).to_iso8601_string() }}"
    window_end_tmpl = "{{ data_interval_end.to_iso8601_string() }}"

    t2 = SQLExecuteQueryOperator(
        task_id="stg_to_dw",
        conn_id="event_dw",
        sql="stg_to_dw.sql",
        parameters={"window_start": window_start_tmpl, "window_end": window_end_tmpl},
    )

    t3 = SQLExecuteQueryOperator(
        task_id="dm_hourly_volume",
        conn_id="event_dw",
        sql="dm_hourly_event_volume.sql",
        parameters={"window_start": window_start_tmpl, "window_end": window_end_tmpl},
    )

    t4 = SQLExecuteQueryOperator(
        task_id="dm_type_ratio",
        conn_id="event_dw",
        sql="dm_event_type_ratio.sql",
        parameters={"window_start": window_start_tmpl, "window_end": window_end_tmpl},
    )

    t1 >> t2 >> t3 >> t4
