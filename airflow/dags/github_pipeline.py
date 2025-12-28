from datetime import datetime, timedelta, timezone

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator


# 늦게 도착한 이벤트를 흡수하기 위한 overlap 일수
# 예: 2이면 매일 "오늘 구간 + 직전 2일"을 다시 처리합니다.
OVERLAP_DAYS = 2


def raw_to_stg_task(**context):
    """
    주의:
      - S3 raw는 created_at 기준으로 dt/hour 파티션이 나뉩니다.
      - 늦게 도착한 이벤트는 과거 dt 파티션으로 저장될 수 있으므로,
        daily 배치에서도 최근 N일 dt 파티션을 재스캔해야 합니다.
      - batch/raw_to_stg.py는 stg_file_ingestion_log로 멱등 처리되므로
        이미 처리한 S3 key는 자동 스킵됩니다.
    """
    interval_start = context["data_interval_start"]

    from batch.raw_to_stg import run as raw_to_stg_run

    # 예: interval_start가 2025-12-27 00:00Z라면 dt=2025-12-27이 "오늘 구간"
    # overlap 포함: 2025-12-25 ~ 2025-12-27 까지 스캔
    for i in range(OVERLAP_DAYS, -1, -1):
        dt = (interval_start - timedelta(days=i)).strftime("%Y-%m-%d")
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

    t1 = PythonOperator(task_id="raw_to_stg", python_callable=raw_to_stg_task)

    # created_at 기준으로도 늦게 들어온 이벤트를 잡기 위해, 윈도우를 overlap해서 재계산합니다.
    # SQL 쪽은 %(window_start)s / %(window_end)s::timestamptz 로 캐스팅하므로 문자열 전달이면 충분합니다.
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
