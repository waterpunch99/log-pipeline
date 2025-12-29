WITH targets AS (
  SELECT source_key
  FROM stg_file_ingestion_log
  WHERE run_id = %(run_id)s
    AND status = 'done'
),
src AS (
  SELECT
    s.event_id,
    s.payload,
    s.created_at,
    s.ingested_at,
    ROW_NUMBER() OVER (
      PARTITION BY s.event_id
      ORDER BY s.ingested_at DESC, s.created_at DESC
    ) AS rn
  FROM github_events_stg s
  JOIN targets t ON t.source_key = s.source_key
  WHERE s.is_valid = true
    AND s.created_at IS NOT NULL
    AND s.event_id IS NOT NULL
),
dedup AS (
  SELECT
    event_id,
    payload,
    created_at,
    ingested_at
  FROM src
  WHERE rn = 1
),
norm AS (
  SELECT
    event_id,
    (payload->>'type') AS event_type,
    NULLIF((payload->'actor'->>'id')::bigint, 0) AS actor_id,
    (payload->'actor'->>'login') AS actor_login,
    NULLIF((payload->'repo'->>'id')::bigint, 0) AS repo_id,
    (payload->'repo'->>'name') AS repo_name,
    created_at,
    ingested_at
  FROM dedup
)
INSERT INTO github_events_dw (
  event_id,
  event_type,
  actor_id,
  actor_login,
  repo_id,
  repo_name,
  created_at,
  ingested_at,
  updated_at
)
SELECT
  event_id,
  event_type,
  actor_id,
  actor_login,
  repo_id,
  repo_name,
  created_at,
  ingested_at,
  now()
FROM norm
ON CONFLICT (event_id)
DO UPDATE SET
  event_type   = EXCLUDED.event_type,
  actor_id     = EXCLUDED.actor_id,
  actor_login  = EXCLUDED.actor_login,
  repo_id      = EXCLUDED.repo_id,
  repo_name    = EXCLUDED.repo_name,
  created_at   = EXCLUDED.created_at,
  ingested_at  = EXCLUDED.ingested_at,
  updated_at   = now();
