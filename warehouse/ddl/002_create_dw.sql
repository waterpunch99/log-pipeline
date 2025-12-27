CREATE TABLE IF NOT EXISTS github_events_dw (
  event_id     text PRIMARY KEY,
  event_type   text,
  actor_id     bigint,
  actor_login  text,
  repo_id      bigint,
  repo_name    text,
  created_at   timestamptz,
  ingested_at  timestamptz,
  updated_at   timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS ix_github_events_dw_created_at ON github_events_dw(created_at);
CREATE INDEX IF NOT EXISTS ix_github_events_dw_event_type ON github_events_dw(event_type);
