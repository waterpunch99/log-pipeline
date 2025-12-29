from __future__ import annotations

import os
import subprocess
import uuid
from datetime import datetime, timedelta, timezone, date
from typing import Any, Dict, Optional

from airflow import DAG
from airflow.operators.python import PythonOperator, ShortCircuitOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator


def _new_run_id() -> str:
    return uuid.uuid4().hex


def _parse_ymd(s: str) -> date:
    return datetime.strptime(s, "%Y-%m-%d").date()


def _daterange(start: date, end: date):
    d = start
    while d <= end:
        yield d
        d += timedelta(days=1)


def backfill_raw_to_stg_task(**context) -> str:
    """
    dag_run.conf:
      start_dt: YYYY-MM-DD (required)
      end_dt:   YYYY-MM-DD (required)
      force: bool (default false)
      compaction: bool (default true)

    run_id를 생성하고, start_dt~end_dt 범위의 raw->stg를 backfill 합니다.
    반환된 run_id는 downstream에서 사용됩니다.
    """
    dag_run = context.get("dag_run")
    conf: Dict[str, Any] = (dag_run.conf or {}) if dag_run else {}

    start_dt = conf.get("start_dt")
    end_dt = conf.get("end_dt")
    if not start_dt or not end_dt:
        raise ValueError("backfill requires dag_run.conf: start_dt, end_dt (YYYY-MM-DD)")

    force = bool(conf.get("force", False))
    compaction = bool(conf.get("compaction", True))

    start = _parse_ymd(str(start_dt))
    end = _parse_ymd(str(end_dt))
    if end < start:
        raise ValueError("end_dt must be >= start_dt")

    run_id = _new_run_id()

    # 1) (옵션) compaction
    if compaction:
        try:
            from batch.compact_raw import run as compact_run

            for d in _daterange(start, end):
                compact_run(dt=d.strftime("%Y-%m-%d"))
        except Exception as e:
            # compaction 실패해도 raw_to_stg는 계속 진행
            print(f"[WARN] compaction skipped/failed: {e}")

    # 2) raw_to_stg (backfill)
    from batch.raw_to_stg import run as raw_to_stg_run

    for d in _daterange(start, end):
        dt_str = d.strftime("%Y-%m-%d")
        # raw_to_stg가 force를 받을 수 있도록 구현돼 있어야 함
        raw_to_stg_run(dt=dt_str, run_id=run_id, force=force)

    print(
        f"[OK] backfill_raw_to_stg run_id={run_id} start_dt={start_dt} end_dt={end_dt} force={force} compaction={compaction}"
    )
    return run_id


def has_new_rows_task(**context) -> bool:
    """
    기존 설계(문제):
      - inserted_total == 0 이면 downstream을 스킵
      - backfill/replay에서 STG는 ON CONFLICT DO NOTHING이라 inserted=0이 될 수 있어,
        DW/DM 재계산이 막혀버림

    개선(현업스러운 설계):
      - '이번 run_id로 처리된(done) 파일이 1개 이상이면' downstream 진행
      - inserted가 0이어도, DW upsert/DM 재계산을 통해 멱등 재처리 가능
    """
    ti = context["ti"]
    run_id = ti.xcom_pull(task_ids="backfill_raw_to_stg")
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
                  COUNT(1) FILTER (WHERE status='done') AS done_files,
                  COALESCE(SUM(row_count), 0) AS row_total,
                  COALESCE(SUM(inserted_count), 0) AS inserted_total,
                  COUNT(1) FILTER (WHERE status='failed') AS failed_files
                FROM stg_file_ingestion_log
                WHERE run_id=%s
                """,
                (run_id,),
            )
            done_files, row_total, inserted_total, failed_files = cur.fetchone()

    done_files = int(done_files or 0)
    row_total = int(row_total or 0)
    inserted_total = int(inserted_total or 0)
    failed_files = int(failed_files or 0)

    print(
        f"[GATE][BACKFILL] run_id={run_id} done_files={done_files} failed_files={failed_files} row_total={row_total} inserted_total={inserted_total}"
    )

    # 이번 run에서 실제로 처리된 파일이 없으면 downstream 의미 없음
    if done_files == 0:
        print(f"[SKIP][BACKFILL] no processed files for this run. run_id={run_id} -> downstream skipped")
        return False

    # done_files가 있으면 inserted=0이어도 downstream 진행(리플레이/재집계)
    return True


def dq_after_dw_task(**context) -> None:
    ti = context["ti"]
    run_id = ti.xcom_pull(task_ids="backfill_raw_to_stg")
    if not run_id:
        print("[DQ][DW] missing run_id -> skip")
        return

    cmd = ["python", "/opt/airflow/batch/dq_checks.py", "--run-id", run_id, "--stage", "dw"]
    print("[RUN]", " ".join(cmd))
    subprocess.check_call(cmd)


def dq_after_dm_task(**context) -> None:
    ti = context["ti"]
    run_id = ti.xcom_pull(task_ids="backfill_raw_to_stg")
    if not run_id:
        print("[DQ][DM] missing run_id -> skip")
        return

    cmd = ["python", "/opt/airflow/batch/dq_checks.py", "--run-id", run_id, "--stage", "dm"]
    print("[RUN]", " ".join(cmd))
    subprocess.check_call(cmd)


with DAG(
    dag_id="event_log_backfill",
    start_date=datetime(2025, 12, 23, tzinfo=timezone.utc),
    schedule=None,  # 수동 트리거 전용
    catchup=False,
    tags=["github", "backfill"],
    default_args={"retries": 1, "retry_delay": timedelta(minutes=1)},
    template_searchpath=[
        "/opt/airflow/batch",
        "/opt/airflow/warehouse/ddl",
        "/opt/airflow/warehouse/dm",
    ],
) as dag:
    t1 = PythonOperator(
        task_id="backfill_raw_to_stg",
        python_callable=backfill_raw_to_stg_task,
    )

    gate = ShortCircuitOperator(
        task_id="has_new_rows",
        python_callable=has_new_rows_task,
    )

    t2 = SQLExecuteQueryOperator(
        task_id="stg_to_dw",
        conn_id="event_dw",
        sql="stg_to_dw.sql",
        parameters={"run_id": "{{ ti.xcom_pull(task_ids='backfill_raw_to_stg') }}"},
    )

    dq1 = PythonOperator(
        task_id="dq_after_dw",
        python_callable=dq_after_dw_task,
    )

    t3 = SQLExecuteQueryOperator(
        task_id="dm_hourly_volume",
        conn_id="event_dw",
        sql="dm_hourly_event_volume.sql",
        parameters={"run_id": "{{ ti.xcom_pull(task_ids='backfill_raw_to_stg') }}"},
    )

    t4 = SQLExecuteQueryOperator(
        task_id="dm_type_ratio",
        conn_id="event_dw",
        sql="dm_event_type_ratio.sql",
        parameters={"run_id": "{{ ti.xcom_pull(task_ids='backfill_raw_to_stg') }}"},
    )

    dq2 = PythonOperator(
        task_id="dq_after_dm",
        python_callable=dq_after_dm_task,
    )

    t1 >> gate >> t2 >> dq1 >> t3 >> t4 >> dq2
