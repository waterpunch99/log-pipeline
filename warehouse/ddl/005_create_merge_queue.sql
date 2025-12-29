-- warehouse/ddl/005_create_merge_queue.sql

CREATE TABLE IF NOT EXISTS github_events_merge_queue (
  run_id     text NOT NULL,
  event_id   text NOT NULL,
  created_at timestamptz,
  ingested_at timestamptz,
  queued_at  timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (run_id, event_id)
);

CREATE INDEX IF NOT EXISTS ix_github_events_merge_queue_run_id
  ON github_events_merge_queue (run_id);

CREATE INDEX IF NOT EXISTS ix_github_events_merge_queue_created_at
  ON github_events_merge_queue (created_at);
