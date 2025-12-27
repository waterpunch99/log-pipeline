CREATE TABLE IF NOT EXISTS dm_hourly_event_volume (
  event_hour   timestamptz PRIMARY KEY,
  event_count  bigint NOT NULL,
  updated_at   timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS dm_event_type_ratio (
  event_hour     timestamptz NOT NULL,
  event_type     text NOT NULL,
  event_count    bigint NOT NULL,
  ratio_percent  numeric(6,2) NOT NULL,
  updated_at     timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (event_hour, event_type)
);

CREATE INDEX IF NOT EXISTS ix_dm_event_type_ratio_hour ON dm_event_type_ratio(event_hour);
