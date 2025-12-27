WITH src AS (
  SELECT
    event_id,
    payload,
    created_at,
    ingested_at,
    ROW_NUMBER() OVER (
      PARTITION BY event_id
      ORDER BY ingested_at DESC, created_at DESC
    ) AS rn
  FROM github_events_stg
  WHERE is_valid = true
    AND created_at IS NOT NULL
    AND created_at >= %(window_start)s::timestamptz
    AND created_at <  %(window_end)s::timestamptz
    AND event_id IS NOT NULL
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
