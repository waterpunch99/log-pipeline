-- warehouse/ddl/006_create_dq_results.sql

CREATE TABLE IF NOT EXISTS dq_check_results (
  run_id           text NOT NULL,
  stage            text NOT NULL CHECK (stage IN ('dw', 'dm')),
  check_name       text NOT NULL,
  passed           boolean NOT NULL,
  severity         text NOT NULL CHECK (severity IN ('hard', 'soft')),
  measured_value   numeric,
  threshold_value  numeric,
  details          jsonb,
  checked_at       timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (run_id, stage, check_name)
);

CREATE INDEX IF NOT EXISTS ix_dq_check_results_checked_at
  ON dq_check_results (checked_at);

CREATE INDEX IF NOT EXISTS ix_dq_check_results_run_stage
  ON dq_check_results (run_id, stage);
