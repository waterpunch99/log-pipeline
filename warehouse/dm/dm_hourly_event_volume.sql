WITH targets AS (
  SELECT source_key
  FROM stg_file_ingestion_log
  WHERE run_id = %(run_id)s
    AND status = 'done'
),
affected_hours AS (
  SELECT DISTINCT date_trunc('hour', s.created_at) AS event_hour
  FROM github_events_stg s
  JOIN targets t ON t.source_key = s.source_key
  WHERE s.is_valid = true
    AND s.created_at IS NOT NULL
),
agg AS (
  SELECT
    date_trunc('hour', d.created_at) AS event_hour,
    COUNT(*) AS event_count
  FROM github_events_dw d
  JOIN affected_hours h
    ON h.event_hour = date_trunc('hour', d.created_at)
  GROUP BY 1
)
INSERT INTO dm_hourly_event_volume (
  event_hour,
  event_count,
  updated_at
)
SELECT
  event_hour,
  event_count,
  now()
FROM agg
ON CONFLICT (event_hour)
DO UPDATE SET
  event_count = EXCLUDED.event_count,
  updated_at  = now();
