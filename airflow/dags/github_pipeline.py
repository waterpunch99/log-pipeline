# /opt/airflow/dags/github_pipeline.py

from __future__ import annotations

import os
import uuid
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator, ShortCircuitOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator


def _dt_list(execution_date: datetime, overlap_days: int) -> list[str]:
    # execution_date는 Airflow 쪽에서 UTC datetime으로 들어오는 경우가 많습니다.
    # dt는 배치 스크립트가 쓰는 YYYY-MM-DD 형태로 맞춥니다.
    base = execution_date.date()
    days = [base - timedelta(days=i) for i in reversed(range(overlap_days))]
    return [d.isoformat() for d in days]


def has_new_files_task(**context) -> bool:
    """
    '새 파일이 없으면 스킵' 최적화용.
    구현은 프로젝트마다 다른데, 기존에 이미 잘 동작하고 있으니 그대로 두고 싶으면
    이 함수 내용을 당신 코드로 교체하셔도 됩니다.

    여기서는 보수적으로 '항상 True'로 두어서 파이프라인이 확실히 진행되게 합니다.
    (어차피 raw_to_stg / stg_to_dw가 idempotent면 데이터는 망가지지 않습니다.)
    """
    return True


def compact_then_raw_to_stg_task(**context) -> str:
    """
    1) S3 raw 소형 파일 -> 시간단위 컴팩션(raw_compacted)
    2) raw_compacted -> Postgres STG 적재 + ingestion log 기록
    run_id를 만들어서 downstream에 넘깁니다.
    """
    from batch.compact_raw import run as compact_raw_run
    from batch.raw_to_stg import run as raw_to_stg_run

    execution_date: datetime = context["execution_date"]
    overlap_days = int(os.getenv("PIPELINE_OVERLAP_DAYS", "3"))
    run_id = uuid.uuid4().hex

    for dt in _dt_list(execution_date, overlap_days):
        # compact는 내부적으로 '이미 성공 산출물이 있으면 SKIP'하도록 설계되어 있어야 합니다.
        compact_raw_run(dt=dt)
        raw_to_stg_run(dt=dt, run_id=run_id)

    print(f"[OK] compact_then_raw_to_stg run_id={run_id}")
    return run_id


def dq_after_dw_task(**context) -> None:
    """
    dw 적재 후 DQ 체크
    """
    from batch import dq_checks

    run_id = context["ti"].xcom_pull(task_ids="compact_then_raw_to_stg")
    if not run_id:
        # upstream에서 실제 처리된 파일이 없어서 run_id가 None이 될 수 있습니다.
        print("[DQ][DW] no run_id. skip.")
        return
    dq_checks.run(run_id=run_id, stage="dw")


def dq_after_dm_task(**context) -> None:
    """
    dm 적재 후 DQ 체크
    """
    from batch import dq_checks

    run_id = context["ti"].xcom_pull(task_ids="compact_then_raw_to_stg")
    if not run_id:
        print("[DQ][DM] no run_id. skip.")
        return
    dq_checks.run(run_id=run_id, stage="dm")


with DAG(
    dag_id="event_log_pipeline",
    description="GitHub events pipeline: compact -> stg -> dw -> dm -> dq",
    start_date=datetime(2025, 12, 29),
    schedule="0 * * * *",  # 매시 정각(UTC)
    catchup=False,
    max_active_runs=1,
    template_searchpath=[
        "/opt/airflow/batch",
        "/opt/airflow/warehouse",
    ],
    default_args={"owner": "airflow", "retries": 3, "retry_delay": timedelta(minutes=2)},
) as dag:
    has_new_files = ShortCircuitOperator(
        task_id="has_new_files",
        python_callable=has_new_files_task,
    )

    compact_then_raw_to_stg = PythonOperator(
        task_id="compact_then_raw_to_stg",
        python_callable=compact_then_raw_to_stg_task,
    )

    stg_to_dw = SQLExecuteQueryOperator(
        task_id="stg_to_dw",
        conn_id="event_dw",  # Airflow Connection에 event_dw가 설정되어 있어야 합니다.
        sql="stg_to_dw.sql",  # template_searchpath에 /opt/airflow/batch를 넣어서 경로문제 제거
        parameters={
            "run_id": "{{ ti.xcom_pull(task_ids='compact_then_raw_to_stg') }}",
        },
    )

    dq_after_dw = PythonOperator(
        task_id="dq_after_dw",
        python_callable=dq_after_dw_task,
    )

    dm_hourly_volume = SQLExecuteQueryOperator(
        task_id="dm_hourly_volume",
        conn_id="event_dw",
        sql="dm/dm_hourly_event_volume.sql",
        parameters={"run_id": "{{ ti.xcom_pull(task_ids='compact_then_raw_to_stg') }}"},
    )

    dm_type_ratio = SQLExecuteQueryOperator(
        task_id="dm_type_ratio",
        conn_id="event_dw",
        sql="dm/dm_event_type_ratio.sql",
        parameters={"run_id": "{{ ti.xcom_pull(task_ids='compact_then_raw_to_stg') }}"},
    )

    dq_after_dm = PythonOperator(
        task_id="dq_after_dm",
        python_callable=dq_after_dm_task,
    )

    has_new_files >> compact_then_raw_to_stg >> stg_to_dw >> dq_after_dw >> dm_hourly_volume >> dm_type_ratio >> dq_after_dm
