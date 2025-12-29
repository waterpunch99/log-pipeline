# batch/raw_to_stg.py  (전체 교체)

import argparse
import os
from datetime import datetime, timezone
from typing import Dict, List

import psycopg

from batch.raw_to_stg_load import StgLoader
from batch.raw_to_stg_log import IngestionLogRepo
from batch.raw_to_stg_parse import parse_event, utcnow
from batch.raw_to_stg_s3 import list_partition_keys, read_jsonl

BATCH_FLUSH_ROWS = int(os.getenv("STG_FLUSH_ROWS", "500"))
LEASE_MINUTES = int(os.getenv("STG_LEASE_MINUTES", "30"))


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


def main(dt: str, run_id: str, force: bool = False) -> None:
    bucket = os.getenv("S3_BUCKET")
    prefix = os.getenv("S3_PREFIX")
    if not bucket or not prefix:
        raise RuntimeError("Missing env config: S3_BUCKET / S3_PREFIX")

    dsn = IngestionLogRepo.dsn_from_env()

    loaded_at = utcnow()
    repo = IngestionLogRepo(lease_minutes=LEASE_MINUTES)
    loader = StgLoader()

    total_rows = 0
    total_inserted = 0
    total_files_seen = 0
    total_files_processed = 0
    total_files_skipped = 0
    total_files_noop = 0

    with psycopg.connect(dsn) as conn:
        with conn.cursor() as cur:
            for key in list_partition_keys(bucket, prefix, dt):
                total_files_seen += 1

                claim = repo.claim(cur, source_key=key, dt=dt, run_id=run_id, force=force)
                conn.commit()

                if not claim.should_process:
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
                            inserted += loader.flush(cur, buffer).inserted
                            buffer.clear()

                    if buffer:
                        inserted += loader.flush(cur, buffer).inserted
                        buffer.clear()

                    if inserted == 0:
                        repo.mark_skipped(cur, source_key=key, run_id=run_id, row_count=rows, inserted_count=inserted)
                        total_files_noop += 1
                    else:
                        repo.mark_done(cur, source_key=key, run_id=run_id, row_count=rows, inserted_count=inserted)

                    conn.commit()

                    total_files_processed += 1
                    total_rows += rows
                    total_inserted += inserted

                except Exception as e:
                    repo.mark_failed(
                        cur,
                        source_key=key,
                        run_id=run_id,
                        row_count=rows,
                        inserted_count=inserted,
                        error_msg=str(e),
                    )
                    conn.commit()
                    print(f"[FAIL] key={key} error={e}")

    print(
        f"[OK] raw_to_stg dt={dt} run_id={run_id} force={bool(force)} "
        f"files_seen={total_files_seen} files_processed={total_files_processed} files_skipped={total_files_skipped} files_noop={total_files_noop} "
        f"rows={total_rows} inserted={total_inserted} lease_minutes={LEASE_MINUTES} flush_rows={BATCH_FLUSH_ROWS}"
    )


def run(dt: str, run_id: str, force: bool = False) -> None:
    main(dt, run_id, force=force)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--dt", required=True, help="YYYY-MM-DD")
    parser.add_argument("--run-id", required=True, help="run id (uuid hex)")
    parser.add_argument("--force", action="store_true", help="reprocess even if log status is done/skipped")
    args = parser.parse_args()
    main(args.dt, args.run_id, force=bool(args.force))
