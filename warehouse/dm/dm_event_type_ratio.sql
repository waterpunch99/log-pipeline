WITH total AS (
  SELECT
    date_trunc('hour', created_at) AS event_hour,
    COUNT(*) AS event_total
  FROM github_events_dw
  WHERE created_at >= %(window_start)s::timestamptz
    AND created_at <  %(window_end)s::timestamptz
  GROUP BY 1
),
typed AS (
  SELECT
    date_trunc('hour', created_at) AS event_hour,
    event_type,
    COUNT(*) AS event_count
  FROM github_events_dw
  WHERE created_at >= %(window_start)s::timestamptz
    AND created_at <  %(window_end)s::timestamptz
    AND event_type IS NOT NULL
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
