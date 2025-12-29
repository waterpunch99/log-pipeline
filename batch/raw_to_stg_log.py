# batch/raw_to_stg_log.py  (전체 교체)

import os
from dataclasses import dataclass

import psycopg


@dataclass
class ClaimResult:
    should_process: bool
    status: str  # done / skipped / processing


class IngestionLogRepo:
    def __init__(self, lease_minutes: int):
        self.lease_minutes = int(lease_minutes)

        self.claim_sql = f"""
        INSERT INTO stg_file_ingestion_log (source_key, dt, status, run_id, started_at)
        VALUES (%(source_key)s, %(dt)s::date, 'processing', %(run_id)s, now())
        ON CONFLICT (source_key) DO UPDATE
        SET
          status = CASE
                     WHEN %(force)s = true
                     THEN 'processing'
                     WHEN stg_file_ingestion_log.status IN ('done', 'skipped')
                     THEN stg_file_ingestion_log.status
                     WHEN stg_file_ingestion_log.status = 'failed'
                       OR (
                            stg_file_ingestion_log.status = 'processing'
                        AND stg_file_ingestion_log.started_at < now() - interval '{self.lease_minutes} minutes'
                          )
                     THEN 'processing'
                     ELSE stg_file_ingestion_log.status
                   END,
          run_id = CASE
                     WHEN %(force)s = true
                     THEN EXCLUDED.run_id
                     WHEN stg_file_ingestion_log.status IN ('done', 'skipped')
                     THEN stg_file_ingestion_log.run_id
                     WHEN stg_file_ingestion_log.status = 'failed'
                       OR (
                            stg_file_ingestion_log.status = 'processing'
                        AND stg_file_ingestion_log.started_at < now() - interval '{self.lease_minutes} minutes'
                          )
                     THEN EXCLUDED.run_id
                     ELSE stg_file_ingestion_log.run_id
                   END,
          started_at = CASE
                         WHEN %(force)s = true
                         THEN now()
                         WHEN stg_file_ingestion_log.status IN ('done', 'skipped')
                         THEN stg_file_ingestion_log.started_at
                         WHEN stg_file_ingestion_log.status = 'failed'
                           OR (
                                stg_file_ingestion_log.status = 'processing'
                            AND stg_file_ingestion_log.started_at < now() - interval '{self.lease_minutes} minutes'
                              )
                         THEN now()
                         ELSE stg_file_ingestion_log.started_at
                       END,
          error_msg = CASE
                        WHEN %(force)s = true
                        THEN NULL
                        WHEN stg_file_ingestion_log.status IN ('done', 'skipped')
                        THEN stg_file_ingestion_log.error_msg
                        WHEN stg_file_ingestion_log.status = 'failed'
                          OR (
                               stg_file_ingestion_log.status = 'processing'
                           AND stg_file_ingestion_log.started_at < now() - interval '{self.lease_minutes} minutes'
                             )
                        THEN NULL
                        ELSE stg_file_ingestion_log.error_msg
                      END
        RETURNING status, run_id;
        """.strip()

        self.mark_done_sql = """
        UPDATE stg_file_ingestion_log
        SET
          status='done',
          finished_at=now(),
          row_count=%(row_count)s,
          inserted_count=%(inserted_count)s,
          error_msg=NULL
        WHERE source_key=%(source_key)s AND run_id=%(run_id)s;
        """.strip()

        self.mark_skipped_sql = """
        UPDATE stg_file_ingestion_log
        SET
          status='skipped',
          finished_at=now(),
          row_count=%(row_count)s,
          inserted_count=%(inserted_count)s,
          error_msg=NULL
        WHERE source_key=%(source_key)s AND run_id=%(run_id)s;
        """.strip()

        self.mark_failed_sql = """
        UPDATE stg_file_ingestion_log
        SET
          status='failed',
          finished_at=now(),
          row_count=%(row_count)s,
          inserted_count=%(inserted_count)s,
          error_msg=%(error_msg)s
        WHERE source_key=%(source_key)s AND run_id=%(run_id)s;
        """.strip()

    @staticmethod
    def dsn_from_env() -> str:
        dsn = os.getenv("POSTGRES_DSN")
        if not dsn:
            raise RuntimeError("Missing env: POSTGRES_DSN")
        return dsn

    def claim(
        self,
        cur: "psycopg.Cursor",
        *,
        source_key: str,
        dt: str,
        run_id: str,
        force: bool = False,
    ) -> ClaimResult:
        cur.execute(
            self.claim_sql,
            {"source_key": source_key, "dt": dt, "run_id": run_id, "force": bool(force)},
        )
        status, owner_run_id = cur.fetchone()

        if force:
            # force면 무조건 이 run_id로 processing을 가져온 것으로 간주
            return ClaimResult(True, "processing")

        if status in ("done", "skipped"):
            return ClaimResult(False, status)

        if status == "processing" and owner_run_id == run_id:
            return ClaimResult(True, "processing")

        return ClaimResult(False, "processing")

    def mark_done(self, cur: "psycopg.Cursor", *, source_key: str, run_id: str, row_count: int, inserted_count: int) -> None:
        cur.execute(
            self.mark_done_sql,
            {
                "source_key": source_key,
                "run_id": run_id,
                "row_count": int(row_count),
                "inserted_count": int(inserted_count),
            },
        )

    def mark_skipped(
        self, cur: "psycopg.Cursor", *, source_key: str, run_id: str, row_count: int, inserted_count: int
    ) -> None:
        cur.execute(
            self.mark_skipped_sql,
            {
                "source_key": source_key,
                "run_id": run_id,
                "row_count": int(row_count),
                "inserted_count": int(inserted_count),
            },
        )

    def mark_failed(
        self,
        cur: "psycopg.Cursor",
        *,
        source_key: str,
        run_id: str,
        row_count: int,
        inserted_count: int,
        error_msg: str,
    ) -> None:
        cur.execute(
            self.mark_failed_sql,
            {
                "source_key": source_key,
                "run_id": run_id,
                "row_count": int(row_count),
                "inserted_count": int(inserted_count),
                "error_msg": (error_msg or "")[:2000],
            },
        )
