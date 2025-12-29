# batch/raw_to_stg.py
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

ENV_PATH = Path(__file__).resolve().parents[1] / ".env"
load_dotenv(dotenv_path=ENV_PATH)

# 한번에 너무 많이 넣으면 쿼리/파라미터가 커지므로 적당히
BATCH_FLUSH_ROWS = int(os.getenv("STG_FLUSH_ROWS", "500"))

# processing lease(임대) 만료 기준(분)
LEASE_MINUTES = int(os.getenv("STG_LEASE_MINUTES", "30"))

CLAIM_SQL = f"""
INSERT INTO stg_file_ingestion_log (source_key, dt, status, run_id, started_at)
VALUES (%(source_key)s, %(dt)s::date, 'processing', %(run_id)s, now())
ON CONFLICT (source_key) DO UPDATE
SET
  status = CASE
             WHEN stg_file_ingestion_log.status = 'failed'
               OR (
                    stg_file_ingestion_log.status = 'processing'
                AND stg_file_ingestion_log.started_at < now() - interval '{LEASE_MINUTES} minutes'
                  )
             THEN 'processing'
             ELSE stg_file_ingestion_log.status
           END,
  run_id = CASE
             WHEN stg_file_ingestion_log.status = 'failed'
               OR (
                    stg_file_ingestion_log.status = 'processing'
                AND stg_file_ingestion_log.started_at < now() - interval '{LEASE_MINUTES} minutes'
                  )
             THEN EXCLUDED.run_id
             ELSE stg_file_ingestion_log.run_id
           END,
  started_at = CASE
                 WHEN stg_file_ingestion_log.status = 'failed'
                   OR (
                        stg_file_ingestion_log.status = 'processing'
                    AND stg_file_ingestion_log.started_at < now() - interval '{LEASE_MINUTES} minutes'
                      )
                 THEN now()
                 ELSE stg_file_ingestion_log.started_at
               END,
  error_msg = CASE
                WHEN stg_file_ingestion_log.status = 'failed'
                  OR (
                       stg_file_ingestion_log.status = 'processing'
                   AND stg_file_ingestion_log.started_at < now() - interval '{LEASE_MINUTES} minutes'
                     )
                THEN NULL
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


def _list_json_keys(bucket: str, prefix: str) -> List[str]:
    s3 = _s3_client()
    token: Optional[str] = None
    out: List[str] = []

    while True:
        args = {"Bucket": bucket, "Prefix": prefix, "MaxKeys": 1000}
        if token:
            args["ContinuationToken"] = token

        resp = s3.list_objects_v2(**args)
        for obj in resp.get("Contents", []):
            key = obj["Key"]
            if key.endswith(".json") or key.endswith(".jsonl"):
                out.append(key)

        if not resp.get("IsTruncated"):
            break
        token = resp.get("NextContinuationToken")

    out.sort()
    return out


def _get_json(bucket: str, key: str) -> Optional[dict]:
    s3 = _s3_client()
    try:
        obj = s3.get_object(Bucket=bucket, Key=key)
        raw = obj["Body"].read()
        return json.loads(raw.decode("utf-8"))
    except Exception:
        return None


def list_partition_keys(bucket: str, prefix: str, dt: str) -> Iterator[str]:
    """
    우선순위:
      1) raw_compacted/_manifest가 있으면, manifest에 적힌 output_key만 사용(시간당 1개, 최신만)
      2) manifest가 하나도 없으면 raw/success fallback
    """
    manifest_prefix = f"{prefix.rstrip('/')}/raw_compacted/_manifest/dt={dt}/"
    manifest_keys = [k for k in _list_json_keys(bucket, manifest_prefix) if k.endswith("manifest.json")]

    output_keys: List[str] = []
    for mk in manifest_keys:
        m = _get_json(bucket, mk)
        if not m:
            continue
        ok = m.get("output_key")
        if isinstance(ok, str) and ok.endswith(".jsonl"):
            output_keys.append(ok)

    output_keys = sorted(set(output_keys))
    if output_keys:
        for k in output_keys:
            yield k
        return

    raw_prefix = f"{prefix.rstrip('/')}/raw/success/dt={dt}/"
    raw_keys = [k for k in _list_json_keys(bucket, raw_prefix) if k.endswith(".jsonl")]
    for k in raw_keys:
        yield k


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
            "payload": raw_text,  # json string
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
    멀티 로우 INSERT + RETURNING으로 inserted_count를 정확히 집계합니다.
    - ON CONFLICT DO NOTHING으로 스킵된 행은 RETURNING에 안 잡히므로 카운트에 포함되지 않음
    - 행 단위 execute 대비 DB 왕복이 크게 줄어듦
    """
    if not buffer:
        return 0

    # VALUES (%s, %s::jsonb, %s, ... ) 를 N개 생성
    values_sql = ",".join(["(%s,%s::jsonb,%s,%s,%s,%s,%s,%s)"] * len(buffer))
    sql = f"""
    INSERT INTO github_events_stg (
      event_id, payload, created_at, ingested_at, source_key, loaded_at, is_valid, err_msg
    )
    VALUES {values_sql}
    ON CONFLICT DO NOTHING
    RETURNING 1;
    """.strip()

    params: List[object] = []
    for r in buffer:
        params.extend(
            [
                r.get("event_id"),
                r.get("payload"),
                r.get("created_at"),
                r.get("ingested_at"),
                r.get("source_key"),
                r.get("loaded_at"),
                r.get("is_valid"),
                r.get("err_msg"),
            ]
        )

    cur.execute(sql, params)
    rows = cur.fetchall()
    return len(rows)


def _claim_file(cur: "psycopg.Cursor", source_key: str, dt: str, run_id: str) -> Tuple[bool, str]:
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
                conn.commit()  # claim 상태를 즉시 반영

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
                    conn.commit()

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
                    conn.commit()
                    print(f"[FAIL] key={key} error={e}")

    print(
        f"[OK] raw_to_stg dt={dt} run_id={run_id} "
        f"files_seen={total_files_seen} files_processed={total_files_processed} files_skipped={total_files_skipped} "
        f"rows={total_rows} inserted={total_inserted} lease_minutes={LEASE_MINUTES} flush_rows={BATCH_FLUSH_ROWS}"
    )


def run(dt: str) -> None:
    main(dt)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--dt", required=True, help="YYYY-MM-DD")
    args = parser.parse_args()
    main(args.dt)
