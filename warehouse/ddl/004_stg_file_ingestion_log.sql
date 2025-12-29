-- warehouse/ddl/004_stg_file_ingestion_log.sql

CREATE TABLE IF NOT EXISTS stg_file_ingestion_log (
  source_key      text PRIMARY KEY,
  dt              date NOT NULL,
  status          text NOT NULL CHECK (status IN ('processing', 'done', 'skipped', 'failed')),
  run_id          text,
  started_at      timestamptz NOT NULL DEFAULT now(),
  finished_at     timestamptz,
  row_count       integer NOT NULL DEFAULT 0,
  inserted_count  integer NOT NULL DEFAULT 0,
  error_msg       text
);

CREATE INDEX IF NOT EXISTS idx_stg_file_ingestion_log_dt
  ON stg_file_ingestion_log (dt);

CREATE INDEX IF NOT EXISTS idx_stg_file_ingestion_log_status
  ON stg_file_ingestion_log (status);
