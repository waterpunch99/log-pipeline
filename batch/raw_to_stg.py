import argparse
import json
import os
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Iterable, Iterator, List, Optional, Tuple

import boto3
import psycopg
from dotenv import load_dotenv

# 프로젝트 루트의 .env 로드 (batch/raw_to_stg.py 기준: parents[1]이 루트)
ENV_PATH = Path(__file__).resolve().parents[1] / ".env"
load_dotenv(dotenv_path=ENV_PATH)

# STG: 유니크 인덱스(ux_github_events_stg_event_id_valid)에 걸리면 스킵
INSERT_STG_SQL = """
INSERT INTO github_events_stg (
  event_id,
  payload,
  created_at,
  ingested_at,
  source_key,
  loaded_at,
  is_valid,
  err_msg
)
VALUES (
  %(event_id)s,
  %(payload)s::jsonb,
  %(created_at)s,
  %(ingested_at)s,
  %(source_key)s,
  %(loaded_at)s,
  %(is_valid)s,
  %(err_msg)s
)
ON CONFLICT DO NOTHING;
""".strip()

# 너무 크게 잡으면 행 단위 execute가 오래 걸리니 적당히
BATCH_FLUSH_ROWS = 500

CLAIM_SQL = """
INSERT INTO stg_file_ingestion_log (source_key, dt, status, run_id, started_at)
VALUES (%(source_key)s, %(dt)s::date, 'processing', %(run_id)s, now())
ON CONFLICT (source_key) DO UPDATE
SET
  status = CASE
             WHEN stg_file_ingestion_log.status = 'failed' THEN 'processing'
             ELSE stg_file_ingestion_log.status
           END,
  run_id = CASE
             WHEN stg_file_ingestion_log.status = 'failed' THEN EXCLUDED.run_id
             ELSE stg_file_ingestion_log.run_id
           END,
  started_at = CASE
                 WHEN stg_file_ingestion_log.status = 'failed' THEN now()
                 ELSE stg_file_ingestion_log.started_at
               END,
  error_msg = CASE
                WHEN stg_file_ingestion_log.status = 'failed' THEN NULL
                ELSE stg_file_ingestion_log.error_msg
              END
RETURNING status, run_id;
""".strip()

MARK_DONE_SQL = """
UPDATE stg_file_ingestion_log
SET
  status='done',
  finished_at=now(),
  row_count=%(row_count)s,
  inserted_count=%(inserted_count)s,
  error_msg=NULL
WHERE source_key=%(source_key)s AND run_id=%(run_id)s;
""".strip()

MARK_FAILED_SQL = """
UPDATE stg_file_ingestion_log
SET
  status='failed',
  finished_at=now(),
  row_count=%(row_count)s,
  inserted_count=%(inserted_count)s,
  error_msg=%(error_msg)s
WHERE source_key=%(source_key)s AND run_id=%(run_id)s;
""".strip()


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


def _parse_iso8601_to_utc(value: Optional[str]) -> Optional[datetime]:
    if not value:
        return None
    try:
        dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)
    except Exception:
        return None


def _s3_client() -> "boto3.client":
    region = os.getenv("AWS_REGION") or "ap-northeast-2"
    return boto3.client("s3", region_name=region)


def list_partition_keys(bucket: str, prefix: str, dt: str) -> Iterator[str]:
    s3 = _s3_client()
    partition_prefix = f"{prefix.rstrip('/')}/raw/success/dt={dt}/"
    token: Optional[str] = None

    while True:
        args = {"Bucket": bucket, "Prefix": partition_prefix, "MaxKeys": 1000}
        if token:
            args["ContinuationToken"] = token

        resp = s3.list_objects_v2(**args)

        for obj in resp.get("Contents", []):
            key = obj["Key"]
            if key.endswith(".jsonl"):
                yield key

        if not resp.get("IsTruncated"):
            break

        token = resp.get("NextContinuationToken")


def read_jsonl(bucket: str, key: str) -> Iterable[bytes]:
    s3 = _s3_client()
    obj = s3.get_object(Bucket=bucket, Key=key)
    for line in obj["Body"].iter_lines():
        if line:
            yield line


def parse_event(line: bytes, source_key: str, loaded_at: datetime) -> Dict:
    ingested_at = _utcnow()

    raw_text = line.decode("utf-8", errors="strict")
    try:
        payload_obj = json.loads(raw_text)
        event_id = payload_obj.get("id")
        created_at = _parse_iso8601_to_utc(payload_obj.get("created_at"))

        return {
            "event_id": str(event_id) if event_id is not None else None,
            "payload": raw_text,
            "created_at": created_at,
            "ingested_at": ingested_at,
            "source_key": source_key,
            "loaded_at": loaded_at,
            "is_valid": True,
            "err_msg": None,
        }

    except Exception as e:
        safe_raw = line.decode("utf-8", errors="ignore")
        return {
            "event_id": None,
            "payload": json.dumps({"raw": safe_raw}, ensure_ascii=False),
            "created_at": None,
            "ingested_at": ingested_at,
            "source_key": source_key,
            "loaded_at": loaded_at,
            "is_valid": False,
            "err_msg": str(e),
        }


def _flush(cur: "psycopg.Cursor", buffer: List[Dict]) -> int:
    """
    inserted_count를 정확히 집계하기 위해 행 단위로 rowcount(0/1)를 합산합니다.
    (unique index 충돌로 스킵되면 rowcount=0)
    """
    if not buffer:
        return 0

    inserted = 0
    for row in buffer:
        cur.execute(INSERT_STG_SQL, row)
        # psycopg rowcount는 INSERT 성공 시 1, DO NOTHING이면 0
        inserted += max(cur.rowcount, 0)
    return inserted


def _claim_file(cur: "psycopg.Cursor", source_key: str, dt: str, run_id: str) -> Tuple[bool, str]:
    """
    반환:
      (should_process, status)

    규칙:
      - 신규: processing(내 run_id) → 진행
      - done: 스킵
      - failed: processing으로 전환(내 run_id) → 재처리
      - processing인데 run_id가 다름: 다른 실행이 처리 중 → 스킵
    """
    cur.execute(CLAIM_SQL, {"source_key": source_key, "dt": dt, "run_id": run_id})
    status, owner_run_id = cur.fetchone()

    if status == "done":
        return False, "done"

    if status == "processing" and owner_run_id == run_id:
        return True, "processing"

    return False, "processing"


def main(dt: str) -> None:
    bucket = os.getenv("S3_BUCKET")
    prefix = os.getenv("S3_PREFIX")
    dsn = os.getenv("POSTGRES_DSN")

    if not bucket or not prefix or not dsn:
        raise RuntimeError("Missing env config: S3_BUCKET / S3_PREFIX / POSTGRES_DSN")

    loaded_at = _utcnow()
    run_id = uuid.uuid4().hex

    total_rows = 0
    total_inserted = 0
    total_files_seen = 0
    total_files_processed = 0
    total_files_skipped = 0

    with psycopg.connect(dsn) as conn:
        with conn.cursor() as cur:
            for key in list_partition_keys(bucket, prefix, dt):
                total_files_seen += 1

                should_process, _ = _claim_file(cur, key, dt, run_id)
                if not should_process:
                    total_files_skipped += 1
                    continue

                rows = 0
                inserted = 0
                buffer: List[Dict] = []

                try:
                    for line in read_jsonl(bucket, key):
                        buffer.append(parse_event(line, key, loaded_at))
                        rows += 1

                        if len(buffer) >= BATCH_FLUSH_ROWS:
                            inserted += _flush(cur, buffer)
                            buffer.clear()

                    if buffer:
                        inserted += _flush(cur, buffer)
                        buffer.clear()

                    cur.execute(
                        MARK_DONE_SQL,
                        {
                            "source_key": key,
                            "run_id": run_id,
                            "row_count": rows,
                            "inserted_count": inserted,
                        },
                    )

                    total_files_processed += 1
                    total_rows += rows
                    total_inserted += inserted

                except Exception as e:
                    cur.execute(
                        MARK_FAILED_SQL,
                        {
                            "source_key": key,
                            "run_id": run_id,
                            "row_count": rows,
                            "inserted_count": inserted,
                            "error_msg": str(e),
                        },
                    )
                    print(f"[FAIL] key={key} error={e}")

    print(
        f"[OK] raw_to_stg dt={dt} run_id={run_id} "
        f"files_seen={total_files_seen} files_processed={total_files_processed} files_skipped={total_files_skipped} "
        f"rows={total_rows} inserted={total_inserted}"
    )


def run(dt: str) -> None:
    main(dt)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--dt", required=True, help="YYYY-MM-DD")
    args = parser.parse_args()
    main(args.dt)
