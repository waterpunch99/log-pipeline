-- batch/stg_to_dw.sql
-- run_id 기준 증분 upsert
-- 목표: overlap window가 커져도, 이번 실행에서 실제로 처리된 파일(source_key)에서 나온 STG row만 DW로 반영

WITH targets AS (
  SELECT source_key
  FROM stg_file_ingestion_log
  WHERE run_id = %(run_id)s
    AND status = 'done'
),
stg_rows AS (
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
    AND s.event_id IS NOT NULL
    AND s.created_at IS NOT NULL
),
dedup AS (
  SELECT
    event_id,
    payload,
    created_at,
    ingested_at
  FROM stg_rows
  WHERE rn = 1
),
enqueue AS (
  INSERT INTO github_events_merge_queue (run_id, event_id, created_at, ingested_at)
  SELECT
    %(run_id)s,
    event_id,
    created_at,
    ingested_at
  FROM dedup
  ON CONFLICT (run_id, event_id) DO NOTHING
  RETURNING event_id
),
norm AS (
  SELECT
    d.event_id,
    (d.payload->>'type') AS event_type,
    NULLIF((d.payload->'actor'->>'id')::bigint, 0) AS actor_id,
    (d.payload->'actor'->>'login') AS actor_login,
    NULLIF((d.payload->'repo'->>'id')::bigint, 0) AS repo_id,
    (d.payload->'repo'->>'name') AS repo_name,
    d.created_at,
    d.ingested_at
  FROM dedup d
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
