INSERT INTO dm_hourly_event_volume (
  event_hour,
  event_count,
  updated_at
)
SELECT
  date_trunc('hour', created_at) AS event_hour,
  COUNT(*)                       AS event_count,
  now()
FROM github_events_dw
WHERE created_at >= %(window_start)s::timestamptz
  AND created_at <  %(window_end)s::timestamptz
GROUP BY 1
ON CONFLICT (event_hour)
DO UPDATE SET
  event_count = EXCLUDED.event_count,
  updated_at  = now();
