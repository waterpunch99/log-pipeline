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
total AS (
  SELECT
    date_trunc('hour', d.created_at) AS event_hour,
    COUNT(*) AS event_total
  FROM github_events_dw d
  JOIN affected_hours h
    ON h.event_hour = date_trunc('hour', d.created_at)
  GROUP BY 1
),
typed AS (
  SELECT
    date_trunc('hour', d.created_at) AS event_hour,
    d.event_type,
    COUNT(*) AS event_count
  FROM github_events_dw d
  JOIN affected_hours h
    ON h.event_hour = date_trunc('hour', d.created_at)
  WHERE d.event_type IS NOT NULL
  GROUP BY 1, 2
)
INSERT INTO dm_event_type_ratio (
  event_hour,
  event_type,
  event_count,
  ratio_percent,
  updated_at
)
SELECT
  t.event_hour,
  t.event_type,
  t.event_count,
  ROUND((t.event_count::numeric / NULLIF(total.event_total, 0)) * 100, 2) AS ratio_percent,
  now()
FROM typed t
JOIN total ON total.event_hour = t.event_hour
ON CONFLICT (event_hour, event_type)
DO UPDATE SET
  event_count   = EXCLUDED.event_count,
  ratio_percent = EXCLUDED.ratio_percent,
  updated_at    = now();
