import json
import os
import argparse
from datetime import datetime, timezone
from typing import Iterable, List, Dict
from pathlib import Path

import boto3
import psycopg
from dotenv import load_dotenv
from psycopg.rows import dict_row

ENV_PATH = Path(__file__).resolve().parents[1] / ".env"
load_dotenv(dotenv_path=ENV_PATH)

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
);
"""

def _s3_client():
    region = os.getenv("AWS_REGION")
    return boto3.client("s3", region_name=region)

def list_partition_keys(bucket: str, prefix: str, dt: str) -> Iterable[str]:
    s3 = _s3_client()
    partition_prefix = f"{prefix}/raw/success/dt={dt}/"
    token = None

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

def parse_event(line: bytes, source_key: str, loaded_at: str) -> Dict:
    now = datetime.now(timezone.utc).isoformat()
    try:
        payload = json.loads(line.decode("utf-8"))
        created_at = payload.get("created_at")
        event_id = payload.get("id")

        return {
            "event_id": str(event_id) if event_id else None,
            "payload": json.dumps(payload, ensure_ascii=False),
            "created_at": created_at,
            "ingested_at": now,
            "source_key": source_key,
            "loaded_at": loaded_at,
            "is_valid": True,
            "err_msg": None,
        }
    except Exception as e:
        return {
            "event_id": None,
            "payload": json.dumps({"raw": line.decode("utf-8", errors="ignore")}, ensure_ascii=False),
            "created_at": None,
            "ingested_at": now,
            "source_key": source_key,
            "loaded_at": loaded_at,
            "is_valid": False,
            "err_msg": str(e),
        }

def main(dt: str):
    bucket = os.getenv("S3_BUCKET")
    prefix = os.getenv("S3_PREFIX")
    dsn = os.getenv("POSTGRES_DSN")

    if not all([bucket, prefix, dsn]):
        raise RuntimeError("Missing env config")

    conn = psycopg.connect(dsn, row_factory=dict_row)
    inserted = 0
    rows = 0
    files = 0
    buffer: List[Dict] = []

    loaded_at = datetime.now(timezone.utc).isoformat()

    with conn, conn.cursor() as cur:
        for key in list_partition_keys(bucket, prefix, dt):
            files += 1
            files += 1
            for line in read_jsonl(bucket, key):
                buffer.append(parse_event(line, key, loaded_at))
                rows += 1

                if len(buffer) >= 2000:
                    cur.executemany(INSERT_STG_SQL, buffer)
                    inserted += len(buffer)
                    buffer.clear()

        if buffer:
            cur.executemany(INSERT_STG_SQL, buffer)
            inserted += len(buffer)
            buffer.clear()

    print(f"[OK] raw_to_stg files={files} rows={rows} inserted={inserted} dt={dt}")

def run(dt: str):
    main(dt)

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--dt", required=True)
    args = parser.parse_args()
    main(args.dt)
