-- lag-observer v0.1: substrate DDL
--
-- Creates the PRO_OBSERVABILITY database, LAG_OBS schema, dedicated XS warehouse,
-- LAG_OBSERVER_ROLE with explicit per-object grants on the imported DataLink share
-- (no IMPORTED PRIVILEGES — known no-op on this account), and the core tables.
--
-- Run as CLAUDE_ADMIN (SYSADMIN/SECURITYADMIN). Idempotent — safe to re-run.

-- =============================================================
-- Database, schema, warehouse, role
-- =============================================================
USE ROLE SYSADMIN;

CREATE DATABASE IF NOT EXISTS PRO_OBSERVABILITY
  COMMENT = 'PRO observability layer — lag observer + future telemetry';

CREATE SCHEMA IF NOT EXISTS PRO_OBSERVABILITY.LAG_OBS
  COMMENT = 'DataLink observation latency telemetry';

CREATE WAREHOUSE IF NOT EXISTS LAG_OBS_WH
  WAREHOUSE_SIZE = XSMALL
  AUTO_SUSPEND = 60
  AUTO_RESUME = TRUE
  INITIALLY_SUSPENDED = TRUE
  COMMENT = 'Dedicated XS for lag-observer tasks';

USE ROLE SECURITYADMIN;
CREATE ROLE IF NOT EXISTS LAG_OBSERVER_ROLE
  COMMENT = 'Owner role for lag-observer schema + tasks';

-- Grant the role to CLAUDE_ADMIN so we can SET ROLE for verification + ownership transfer
GRANT ROLE LAG_OBSERVER_ROLE TO USER CLAUDE_ADMIN;
GRANT ROLE LAG_OBSERVER_ROLE TO ROLE SYSADMIN;

-- =============================================================
-- Object grants — explicit per-object SELECT on imported share
-- (GRANT IMPORTED PRIVILEGES is a known silent no-op on this account)
-- =============================================================
USE ROLE SECURITYADMIN;

-- Warehouse
GRANT USAGE ON WAREHOUSE LAG_OBS_WH TO ROLE LAG_OBSERVER_ROLE;
GRANT OPERATE ON WAREHOUSE LAG_OBS_WH TO ROLE LAG_OBSERVER_ROLE;

-- Own the observability database + schema
USE ROLE SYSADMIN;
GRANT OWNERSHIP ON DATABASE PRO_OBSERVABILITY TO ROLE LAG_OBSERVER_ROLE COPY CURRENT GRANTS;
GRANT OWNERSHIP ON SCHEMA PRO_OBSERVABILITY.LAG_OBS TO ROLE LAG_OBSERVER_ROLE COPY CURRENT GRANTS;

-- Read access to imported share — try the meta grant first (expected no-op),
-- then explicit per-view grants which we know work on this account. In
-- practice, the IMPORTED PRIVILEGES grant has been doing the real work on
-- this account (verified 2026-05-14) — the explicit grants are
-- belt-and-suspenders so a fresh substrate apply works regardless.
USE ROLE ACCOUNTADMIN;
GRANT IMPORTED PRIVILEGES ON DATABASE PROD_ANALYTICS_PRO TO ROLE LAG_OBSERVER_ROLE;
GRANT USAGE ON DATABASE PROD_ANALYTICS_PRO TO ROLE LAG_OBSERVER_ROLE;
GRANT USAGE ON SCHEMA PROD_ANALYTICS_PRO.DATALINK TO ROLE LAG_OBSERVER_ROLE;
-- v0.1 + PR #1 + v2 watch set
GRANT SELECT ON VIEW PROD_ANALYTICS_PRO.DATALINK.REQ_HIRED       TO ROLE LAG_OBSERVER_ROLE;
GRANT SELECT ON VIEW PROD_ANALYTICS_PRO.DATALINK.ACTIVITY_FACT   TO ROLE LAG_OBSERVER_ROLE;
GRANT SELECT ON VIEW PROD_ANALYTICS_PRO.DATALINK.APP_NOMINATE    TO ROLE LAG_OBSERVER_ROLE;
GRANT SELECT ON VIEW PROD_ANALYTICS_PRO.DATALINK.APPLICANT_TAGS  TO ROLE LAG_OBSERVER_ROLE;
GRANT SELECT ON VIEW PROD_ANALYTICS_PRO.DATALINK.APP_SKILLS      TO ROLE LAG_OBSERVER_ROLE;
GRANT SELECT ON VIEW PROD_ANALYTICS_PRO.DATALINK.APPLICANTS      TO ROLE LAG_OBSERVER_ROLE;
GRANT SELECT ON VIEW PROD_ANALYTICS_PRO.DATALINK.APP_DOC_UPLOAD  TO ROLE LAG_OBSERVER_ROLE;
GRANT SELECT ON VIEW PROD_ANALYTICS_PRO.DATALINK.APP_JOB_HISTORY TO ROLE LAG_OBSERVER_ROLE;
GRANT SELECT ON VIEW PROD_ANALYTICS_PRO.DATALINK.APP_ANSWERS     TO ROLE LAG_OBSERVER_ROLE;
GRANT SELECT ON VIEW PROD_ANALYTICS_PRO.DATALINK.REQ_SKILLS      TO ROLE LAG_OBSERVER_ROLE;
GRANT SELECT ON VIEW PROD_ANALYTICS_PRO.DATALINK.REQ             TO ROLE LAG_OBSERVER_ROLE;
GRANT SELECT ON VIEW PROD_ANALYTICS_PRO.DATALINK.REQ_NOTES       TO ROLE LAG_OBSERVER_ROLE;
GRANT SELECT ON VIEW PROD_ANALYTICS_PRO.DATALINK.COMPANY         TO ROLE LAG_OBSERVER_ROLE;
GRANT SELECT ON VIEW PROD_ANALYTICS_PRO.DATALINK.APP_STATUS      TO ROLE LAG_OBSERVER_ROLE;
GRANT SELECT ON VIEW PROD_ANALYTICS_PRO.DATALINK.PLACEMENT_FACT  TO ROLE LAG_OBSERVER_ROLE;
GRANT SELECT ON VIEW PROD_ANALYTICS_PRO.DATALINK.HIRING_MANAGER  TO ROLE LAG_OBSERVER_ROLE;
GRANT SELECT ON VIEW PROD_ANALYTICS_PRO.DATALINK.CONT_ACTIVITY   TO ROLE LAG_OBSERVER_ROLE;
GRANT SELECT ON VIEW PROD_ANALYTICS_PRO.DATALINK.CONT_STATUS     TO ROLE LAG_OBSERVER_ROLE;

-- Tasks need EXECUTE TASK at account level (per Snowflake docs)
GRANT EXECUTE TASK ON ACCOUNT TO ROLE LAG_OBSERVER_ROLE;

-- =============================================================
-- Switch to LAG_OBSERVER_ROLE and create tables in our owned schema
-- =============================================================
USE ROLE LAG_OBSERVER_ROLE;
USE DATABASE PRO_OBSERVABILITY;
USE SCHEMA LAG_OBS;
USE WAREHOUSE LAG_OBS_WH;

-- RUN_LOG: per-tick task execution metadata. Without this, observation gaps
-- are indistinguishable from CDC stalls.
CREATE TABLE IF NOT EXISTS RUN_LOG (
  run_id              VARCHAR     DEFAULT UUID_STRING(),
  target_table        VARCHAR     NOT NULL,
  task_kind           VARCHAR     NOT NULL,    -- 'heartbeat_and_changes' / 'snapshot_diff'
  started_at          TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
  finished_at         TIMESTAMP_NTZ,
  status              VARCHAR,                  -- 'OK' / 'ERROR'
  error_msg           VARCHAR,
  rows_inserted_to_changelog NUMBER DEFAULT 0,
  possible_missed_window BOOLEAN  DEFAULT FALSE -- flagged when MAX(LASTUPDATEDDATE) advanced unexpectedly
);

-- HEARTBEAT_LOG: per-tick freshness reading. Append-only, every observation kept.
-- Sawtooth on max_source_ts reveals CDC cycle interval per table.
CREATE TABLE IF NOT EXISTS HEARTBEAT_LOG (
  heartbeat_id        VARCHAR     DEFAULT UUID_STRING(),
  target_table        VARCHAR     NOT NULL,
  source_ts_column    VARCHAR     NOT NULL,
  max_source_ts       TIMESTAMP_NTZ,
  row_count           NUMBER,
  cdc_i_count         NUMBER,
  cdc_u_count         NUMBER,
  cdc_d_count         NUMBER,
  observed_at         TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
  staleness_sec       NUMBER                    -- TIMEDIFF(observed_at, max_source_ts)
);

-- WATERMARK_STATE: latest-known max ID/timestamp per (table, watermark_column).
-- Single row per (table, column). UPDATEd in place.
CREATE TABLE IF NOT EXISTS WATERMARK_STATE (
  target_table        VARCHAR     NOT NULL,
  watermark_column    VARCHAR     NOT NULL,
  last_seen_max_value VARCHAR,                  -- VARCHAR for type-uniformity (numeric ID or timestamp)
  last_seen_at        TIMESTAMP_NTZ,
  updated_at          TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
  CONSTRAINT pk_watermark_state PRIMARY KEY (target_table, watermark_column)
);

-- SEEN_PKS: which row IDs have been observed per table. Insert detection compares
-- against this. Each table has its own logical PK column; we store as VARCHAR for
-- a single table type.
CREATE TABLE IF NOT EXISTS SEEN_PKS (
  target_table        VARCHAR     NOT NULL,
  pk_value            VARCHAR     NOT NULL,
  first_seen_at       TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
  CONSTRAINT pk_seen_pks PRIMARY KEY (target_table, pk_value)
);

-- CHANGE_LOG: append-only event log. Event identity per Codex review:
-- (target_table, row_pk, change_type, source_lastupdateddate). We MERGE on those
-- so persisted CDCSTATUS flags don't get re-logged every tick.
CREATE TABLE IF NOT EXISTS CHANGE_LOG (
  change_id           VARCHAR     DEFAULT UUID_STRING(),
  target_table        VARCHAR     NOT NULL,
  change_type         VARCHAR     NOT NULL,    -- 'I' / 'U' / 'D'
  row_pk              VARCHAR     NOT NULL,
  source_lastupdateddate TIMESTAMP_NTZ,         -- ETL-rebuild timestamp on the row
  cdcstatus_observed  VARCHAR,                  -- 'I' / 'U' / 'D' / NULL (for tables without CDCSTATUS)
  observed_at         TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
  observation_latency_sec NUMBER,               -- observed_at - source_lastupdateddate
  CONSTRAINT pk_change_log PRIMARY KEY (target_table, row_pk, change_type, source_lastupdateddate)
);

-- PK_SNAPSHOT_*: latest known PK set for tables that need full-set diff for delete
-- detection (silent-delete tables: APPLICANT_TAGS uses TAGID; APP_SKILLS uses SKILLID
-- as a backup to CDCSTATUS='D').
CREATE TABLE IF NOT EXISTS PK_SNAPSHOT_APPLICANT_TAGS (
  tagid               NUMBER      NOT NULL,
  snapshot_at         TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

CREATE TABLE IF NOT EXISTS PK_SNAPSHOT_APP_SKILLS (
  skillid             NUMBER      NOT NULL,
  snapshot_at         TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- Note: Snowflake doesn't enforce PRIMARY KEY / UNIQUE constraints — they're
-- metadata for query optimizer. We rely on MERGE statements in the task SQL
-- for actual dedupe semantics.
