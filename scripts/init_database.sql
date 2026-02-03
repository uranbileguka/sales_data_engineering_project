CREATE SCHEMA IF NOT EXISTS bronze;
CREATE SCHEMA IF NOT EXISTS silver;
CREATE SCHEMA IF NOT EXISTS gold;
CREATE SCHEMA IF NOT EXISTS meta;

-- Tracks incremental loading / last run
CREATE TABLE IF NOT EXISTS meta.pipeline_state (
  source_name TEXT PRIMARY KEY,
  last_success_ts TIMESTAMP,
  last_watermark TEXT,
  last_status TEXT,
  updated_at TIMESTAMP DEFAULT NOW()
);

