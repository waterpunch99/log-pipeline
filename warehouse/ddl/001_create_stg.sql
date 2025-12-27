CREATE TABLE IF NOT EXISTS github_events_stg (
  event_id     text,
  payload      jsonb NOT NULL,
  created_at   timestamptz,
  ingested_at  timestamptz,
  source_key   text NOT NULL,
  loaded_at    timestamptz NOT NULL DEFAULT now(),
  is_valid     boolean NOT NULL DEFAULT true,
  err_msg      text
);

CREATE INDEX IF NOT EXISTS ix_github_events_stg_loaded_at ON github_events_stg(loaded_at);
CREATE INDEX IF NOT EXISTS ix_github_events_stg_event_id ON github_events_stg(event_id);
CREATE INDEX IF NOT EXISTS ix_github_events_stg_created_at ON github_events_stg(created_at);
