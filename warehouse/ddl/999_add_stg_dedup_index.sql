-- warehouse/ddl/001b_add_stg_dedup_index.sql

CREATE UNIQUE INDEX IF NOT EXISTS ux_github_events_stg_event_id_valid
ON github_events_stg (event_id)
WHERE event_id IS NOT NULL AND is_valid = true;
