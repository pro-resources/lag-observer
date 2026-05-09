-- lag-observer v0.1: derived views
-- Run as LAG_OBSERVER_ROLE.

USE ROLE LAG_OBSERVER_ROLE;
USE DATABASE PRO_OBSERVABILITY;
USE SCHEMA LAG_OBS;
USE WAREHOUSE LAG_OBS_WH;

-- =============================================================
-- CURRENT_FRESHNESS — most recent heartbeat per table
-- =============================================================
CREATE OR REPLACE VIEW CURRENT_FRESHNESS AS
SELECT
  target_table,
  source_ts_column,
  max_source_ts,
  observed_at,
  staleness_sec,
  row_count,
  cdc_i_count, cdc_u_count, cdc_d_count
FROM HEARTBEAT_LOG
QUALIFY ROW_NUMBER() OVER (PARTITION BY target_table ORDER BY observed_at DESC) = 1;

-- =============================================================
-- CDC_TICKS — derived from HEARTBEAT_LOG: when did max_source_ts advance?
-- A "tick" = max_source_ts increased between successive observations.
-- =============================================================
CREATE OR REPLACE VIEW CDC_TICKS AS
SELECT
  target_table,
  observed_at AS tick_observed_at,
  max_source_ts AS tick_max_source_ts,
  prev_max_source_ts,
  prev_observed_at,
  TIMESTAMPDIFF('SECOND', prev_observed_at, observed_at) AS interval_sec
FROM (
  SELECT
    target_table, observed_at, max_source_ts,
    LAG(max_source_ts) OVER (PARTITION BY target_table ORDER BY observed_at) AS prev_max_source_ts,
    LAG(observed_at)   OVER (PARTITION BY target_table ORDER BY observed_at) AS prev_observed_at
  FROM HEARTBEAT_LOG
)
WHERE max_source_ts > prev_max_source_ts;

-- =============================================================
-- CDC_INTERVAL_BY_TABLE_24H — rolling 24h cycle-interval distribution
-- =============================================================
CREATE OR REPLACE VIEW CDC_INTERVAL_BY_TABLE_24H AS
SELECT
  target_table,
  COUNT(*) AS tick_count,
  AVG(interval_sec) AS avg_interval_sec,
  APPROX_PERCENTILE(interval_sec, 0.5) AS p50_interval_sec,
  APPROX_PERCENTILE(interval_sec, 0.95) AS p95_interval_sec,
  MAX(interval_sec) AS max_interval_sec,
  MIN(interval_sec) AS min_interval_sec
FROM CDC_TICKS
WHERE tick_observed_at > DATEADD('HOUR', -24, CURRENT_TIMESTAMP())
GROUP BY target_table;

-- =============================================================
-- CDC_INTERVAL_BY_HOUR_OF_DAY_ET — does CDC slow during business hours?
-- =============================================================
CREATE OR REPLACE VIEW CDC_INTERVAL_BY_HOUR_OF_DAY_ET AS
SELECT
  target_table,
  HOUR(CONVERT_TIMEZONE('UTC', 'America/New_York', tick_observed_at)) AS hour_of_day_et,
  COUNT(*) AS tick_count,
  APPROX_PERCENTILE(interval_sec, 0.5) AS p50_interval_sec,
  APPROX_PERCENTILE(interval_sec, 0.95) AS p95_interval_sec
FROM CDC_TICKS
WHERE tick_observed_at > DATEADD('DAY', -7, CURRENT_TIMESTAMP())
GROUP BY 1, 2;

-- =============================================================
-- CHANGE_VOLUME_24H — recent change-event volume per table
-- =============================================================
CREATE OR REPLACE VIEW CHANGE_VOLUME_24H AS
SELECT
  target_table,
  change_type,
  COUNT(*) AS event_count,
  AVG(observation_latency_sec) AS avg_latency_sec,
  APPROX_PERCENTILE(observation_latency_sec, 0.5) AS p50_latency_sec,
  APPROX_PERCENTILE(observation_latency_sec, 0.95) AS p95_latency_sec,
  MAX(observation_latency_sec) AS max_latency_sec
FROM CHANGE_LOG
WHERE observed_at > DATEADD('HOUR', -24, CURRENT_TIMESTAMP())
GROUP BY 1, 2;

-- =============================================================
-- LAG_BREACHES — change events with unusually high observation latency
-- =============================================================
CREATE OR REPLACE VIEW LAG_BREACHES AS
SELECT
  target_table, change_type, row_pk,
  source_lastupdateddate, observed_at, observation_latency_sec,
  cdcstatus_observed
FROM CHANGE_LOG
WHERE observation_latency_sec > 1800  -- > 30 min
  AND observed_at > DATEADD('HOUR', -24, CURRENT_TIMESTAMP())
ORDER BY observation_latency_sec DESC;

-- =============================================================
-- RUN_HEALTH_24H — task execution health
-- =============================================================
CREATE OR REPLACE VIEW RUN_HEALTH_24H AS
SELECT
  target_table, task_kind, status,
  COUNT(*) AS run_count,
  SUM(rows_inserted_to_changelog) AS total_changes_logged,
  COUNT_IF(possible_missed_window) AS possibly_missed_windows,
  AVG(TIMESTAMPDIFF('SECOND', started_at, finished_at)) AS avg_runtime_sec,
  MAX(TIMESTAMPDIFF('SECOND', started_at, finished_at)) AS max_runtime_sec
FROM RUN_LOG
WHERE started_at > DATEADD('HOUR', -24, CURRENT_TIMESTAMP())
GROUP BY 1, 2, 3;
