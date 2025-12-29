# airflow/dags/github_pipeline.py
from __future__ import annotations

import os
import subprocess
import uuid
from datetime import timedelta

import pendulum
import psycopg

from airflow import DAG
from airflow.operators.python import PythonOperator, ShortCircuitOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

OVERLAP_DAYS = int(os.getenv("PIPELINE_OVERLAP_DAYS", "3"))
POSTGRES_DSN = os.getenv("POSTGRES_DSN", "")
CONN_ID = os.getenv("AIRFLOW_CONN_ID_EVENT_DW", "event_dw")

DEFAULT_ARGS = {
    "retries": 3,
    "retry_delay": timedelta(minutes=2),
}


def _count_done_files(run_id: str) -> int:
    if not POSTGRES_DSN:
        return 0
    sql = "select count(*) from stg_file_ingestion_log where run_id=%s and status='done'"
    with psycopg.connect(POSTGRES_DSN) as conn:
        with conn.cursor() as cur:
            cur.execute(sql, (run_id,))
            return int(cur.fetchone()[0])


def compact_then_raw_to_stg_task(**context) -> None:
    from batch.compact_raw import run as compact_raw_run
    from batch.raw_to_stg import run as raw_to_stg_run

    ti = context["ti"]
    logical_date = context["logical_date"].date()

    run_id = uuid.uuid4().hex
    ti.xcom_push(key="run_id", value=run_id)

    # 최근 OVERLAP_DAYS일 재처리(늦게 도착한 이벤트 흡수)
    dts = [(logical_date - timedelta(days=i)).isoformat() for i in range(OVERLAP_DAYS, 0, -1)]
    for dt in dts:
        compact_raw_run(dt=dt)
        raw_to_stg_run(dt=dt, run_id=run_id)

    files_done = _count_done_files(run_id)
    ti.xcom_push(key="files_done", value=files_done)

    print(f"[OK] compact_then_raw_to_stg run_id={run_id} overlap_days={OVERLAP_DAYS} files_done={files_done}")


def has_new_files(**context) -> bool:
    ti = context["ti"]
    files_done = int(ti.xcom_pull(task_ids="compact_then_raw_to_stg", key="files_done") or 0)
    run_id = ti.xcom_pull(task_ids="compact_then_raw_to_stg", key="run_id")

    if files_done <= 0:
        print(f"[SKIP] no new files for this run. run_id={run_id} -> downstream skipped")
        return False
    return True


def dq_after_dw_task(**context) -> None:
    ti = context["ti"]
    run_id = ti.xcom_pull(task_ids="compact_then_raw_to_stg", key="run_id")

    # import로 호출하지 말고 스크립트로 실행(가장 견고)
    cmd = ["python", "/opt/airflow/batch/dq_checks.py", "--run-id", run_id, "--stage", "dw"]
    print("[RUN]", " ".join(cmd))
    subprocess.run(cmd, check=True)


def dq_after_dm_task(**context) -> None:
    ti = context["ti"]
    run_id = ti.xcom_pull(task_ids="compact_then_raw_to_stg", key="run_id")

    cmd = ["python", "/opt/airflow/batch/dq_checks.py", "--run-id", run_id, "--stage", "dm"]
    print("[RUN]", " ".join(cmd))
    subprocess.run(cmd, check=True)


with DAG(
    dag_id="event_log_pipeline",
    start_date=pendulum.datetime(2025, 12, 1, tz="UTC"),
    schedule="0 * * * *",
    catchup=False,
    default_args=DEFAULT_ARGS,
    tags=["github", "pipeline"],
    template_searchpath=[
        "/opt/airflow/batch",
        "/opt/airflow/warehouse/dm",
    ],
) as dag:
    t1 = PythonOperator(
        task_id="compact_then_raw_to_stg",
        python_callable=compact_then_raw_to_stg_task,
    )

    gate = ShortCircuitOperator(
        task_id="has_new_files",
        python_callable=has_new_files,
    )

    t2 = SQLExecuteQueryOperator(
        task_id="stg_to_dw",
        conn_id=CONN_ID,
        sql="stg_to_dw.sql",
        parameters={
            "run_id": "{{ ti.xcom_pull(task_ids='compact_then_raw_to_stg', key='run_id') }}"
        },
    )

    dq_dw = PythonOperator(
        task_id="dq_after_dw",
        python_callable=dq_after_dw_task,
    )

    t3 = SQLExecuteQueryOperator(
        task_id="dm_hourly_volume",
        conn_id=CONN_ID,
        sql="dm_hourly_event_volume.sql",
        parameters={
            "run_id": "{{ ti.xcom_pull(task_ids='compact_then_raw_to_stg', key='run_id') }}"
        },
    )

    t4 = SQLExecuteQueryOperator(
        task_id="dm_type_ratio",
        conn_id=CONN_ID,
        sql="dm_event_type_ratio.sql",
        parameters={
            "run_id": "{{ ti.xcom_pull(task_ids='compact_then_raw_to_stg', key='run_id') }}"
        },
    )

    dq_dm = PythonOperator(
        task_id="dq_after_dm",
        python_callable=dq_after_dm_task,
    )

    t1 >> gate >> t2 >> dq_dw >> t3 >> t4 >> dq_dm
