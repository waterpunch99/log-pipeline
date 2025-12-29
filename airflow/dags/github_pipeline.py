# airflow/dags/github_pipeline.py

from __future__ import annotations

import os
import uuid
from datetime import datetime, timedelta, timezone

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.python import ShortCircuitOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

# 늦게 도착한 이벤트 흡수: raw_to_stg는 최근 N일 dt 파티션 재스캔
OVERLAP_DAYS = 2


def _new_run_id() -> str:
    return uuid.uuid4().hex


def compact_then_raw_to_stg_task(**context) -> str:
    """
    1) (옵션) raw small files -> raw_compacted로 컴팩션
    2) raw_to_stg로 최근 N일 dt 파티션 재스캔 (멱등: 파일 단위 로그로 스킵/재시도)
    반환값(run_id)은 XCom으로 전달되어 stg_to_dw/dq/dm이 동일 run_id로 동작합니다.
    """
    interval_start = context["data_interval_start"]
    run_id = _new_run_id()

    # 1) compaction: 있다면 수행(없어도 넘어가도록)
    try:
        from batch.compact_raw import run as compact_run

        for i in range(OVERLAP_DAYS, -1, -1):
            dt = (interval_start - timedelta(days=i)).strftime("%Y-%m-%d")
            compact_run(dt=dt)
    except Exception as e:
        # compaction이 없거나 실패해도 raw_to_stg는 진행시키고, 로그로 남김
        print(f"[WARN] compaction skipped/failed: {e}")

    # 2) raw_to_stg
    from batch.raw_to_stg import run as raw_to_stg_run

    for i in range(OVERLAP_DAYS, -1, -1):
        dt = (interval_start - timedelta(days=i)).strftime("%Y-%m-%d")
        raw_to_stg_run(dt=dt, run_id=run_id)

    print(f"[OK] compact_then_raw_to_stg run_id={run_id}")
    return run_id


def has_new_files_task(**context) -> bool:
    """
    이번 run_id로 실제 신규 적재(inserted_count)가 0이면 downstream 전체 스킵.
    """
    ti = context["ti"]
    run_id = ti.xcom_pull(task_ids="compact_then_raw_to_stg")
    if not run_id:
        print("[SKIP] run_id missing -> downstream skipped")
        return False

    import psycopg

    dsn = os.getenv("POSTGRES_DSN")
    if not dsn:
        raise RuntimeError("Missing env: POSTGRES_DSN")

    with psycopg.connect(dsn) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT
                  COALESCE(SUM(inserted_count), 0) AS inserted_total,
                  COALESCE(SUM(row_count), 0) AS row_total,
                  COUNT(1) FILTER (WHERE status='done') AS done_files,
                  COUNT(1) FILTER (WHERE status='skipped') AS skipped_files,
                  COUNT(1) FILTER (WHERE status='failed') AS failed_files
                FROM stg_file_ingestion_log
                WHERE run_id=%s
                  AND status IN ('done','skipped','failed')
                """,
                (run_id,),
            )
            inserted_total, row_total, done_files, skipped_files, failed_files = cur.fetchone()

    inserted_total = int(inserted_total or 0)
    row_total = int(row_total or 0)
    done_files = int(done_files or 0)
    skipped_files = int(skipped_files or 0)
    failed_files = int(failed_files or 0)

    print(
        f"[GATE] run_id={run_id} inserted_total={inserted_total} row_total={row_total} "
        f"done_files={done_files} skipped_files={skipped_files} failed_files={failed_files}"
    )

    if inserted_total == 0:
        print(f"[SKIP] no newly inserted stg rows for this run. run_id={run_id} -> downstream skipped")
        return False

    print(f"[OK] newly inserted rows exist. run_id={run_id} inserted_total={inserted_total}")
    return True


def dq_after_dw_task(**context) -> None:
    """
    dq_checks를 subprocess가 아니라 패키지 import로 직접 실행합니다.
    """
    ti = context["ti"]
    run_id = ti.xcom_pull(task_ids="compact_then_raw_to_stg")
    if not run_id:
        print("[DQ][DW] missing run_id -> skip")
        return

    from batch.dq_checks import run as dq_run

    dq_run(run_id=run_id, stage="dw")


def dq_after_dm_task(**context) -> None:
    """
    dq_checks를 subprocess가 아니라 패키지 import로 직접 실행합니다.
    """
    ti = context["ti"]
    run_id = ti.xcom_pull(task_ids="compact_then_raw_to_stg")
    if not run_id:
        print("[DQ][DM] missing run_id -> skip")
        return

    from batch.dq_checks import run as dq_run

    dq_run(run_id=run_id, stage="dm")


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
    t1 = PythonOperator(
        task_id="compact_then_raw_to_stg",
        python_callable=compact_then_raw_to_stg_task,
    )

    gate = ShortCircuitOperator(
        task_id="has_new_files",
        python_callable=has_new_files_task,
    )

    # stg_to_dw는 run_id 기반 증분(upsert)
    t2 = SQLExecuteQueryOperator(
        task_id="stg_to_dw",
        conn_id="event_dw",
        sql="stg_to_dw.sql",
        parameters={"run_id": "{{ ti.xcom_pull(task_ids='compact_then_raw_to_stg') }}"},
    )

    dq1 = PythonOperator(
        task_id="dq_after_dw",
        python_callable=dq_after_dw_task,
    )

    # DM도 run_id로 영향 시간대만 재계산
    t3 = SQLExecuteQueryOperator(
        task_id="dm_hourly_volume",
        conn_id="event_dw",
        sql="dm_hourly_event_volume.sql",
        parameters={"run_id": "{{ ti.xcom_pull(task_ids='compact_then_raw_to_stg') }}"},
    )

    t4 = SQLExecuteQueryOperator(
        task_id="dm_type_ratio",
        conn_id="event_dw",
        sql="dm_event_type_ratio.sql",
        parameters={"run_id": "{{ ti.xcom_pull(task_ids='compact_then_raw_to_stg') }}"},
    )

    dq2 = PythonOperator(
        task_id="dq_after_dm",
        python_callable=dq_after_dm_task,
    )

    t1 >> gate >> t2 >> dq1 >> t3 >> t4 >> dq2
