# lag-observer

Passive observation-latency telemetry for PRO Resources' DataLink share. Tracks per-table CDC cycle interval, change-event volume, and observation latency for five high-value tables, without writing to BOLD.

## What this measures

**DataLink observation latency** — `observer_seen_at - row.LASTUPDATEDDATE`. The interval between Avionte's ETL stamping a row in the share and the moment our polling job sees it.

**This is NOT the same as "BOLD commit-to-DataLink-visible latency."** That metric requires either active probes (writing test data and timing the round trip — deferred to integration partner Compagno / Oak City) or webhook arrival timestamps (also partner-side). What we measure here is the polling-side observation gap, which is bounded above by Avionte's own ETL cycle plus our 1-minute polling interval.

What we deliver:
- Per-table CDC cycle interval (the empirical answer to "is it 3-15 min variance underneath the 15-min advertised number?")
- Per-row insert/update/delete event log
- Per-row observation latency distribution (p50/p95/max)
- Hour-of-day cycle-interval pattern (catches business-hours CDC slowdowns)

What we do NOT deliver:
- Source-side BOLD commit timestamp (no column for it; verified by profiling all 28 timestamp columns across the 5 target tables)
- True per-row DataLink lag uncoupled from our polling interval

## Tables in scope

| Table | Insert detection | Update detection | Delete detection |
|---|---|---|---|
| REQ_HIRED | REQHIREDID + SEEN_PKS | CDCSTATUS='U' | (rare; periodic snapshot diff possible later) |
| APP_NOMINATE | NOMID + SEEN_PKS | CDCSTATUS='U' | (rare) |
| APPLICANT_TAGS | TAGID + SEEN_PKS | (no CDCSTATUS='U' on this table) | TAGID snapshot-diff (5-min) |
| APP_SKILLS | SKILLID + SEEN_PKS | CDCSTATUS='U' | CDCSTATUS='D' + SKILLID snapshot-diff (5-min) |
| ACTIVITY_FACT | ACTDATETIME 24h overlap window + SEEN_PKS | (no CDCSTATUS column) | (no CDCSTATUS column) |

## Why polling, not Snowflake STREAMS

Tested 2026-05-09: streams require change-tracking enabled by the share provider. Avionte hasn't enabled it on any of the 5 target views, so streams aren't available to us. We're stuck with polling. Documented for posterity in case the provider adds it later.

## Why polling, not Snowflake Tasks

Snowflake Tasks need `EXECUTE TASK ON ACCOUNT` granted to the role, which requires ACCOUNTADMIN. CLAUDE_ADMIN has SECURITYADMIN+SYSADMIN only. External Node runner via Windows Task Scheduler sidesteps that grant gap, runs on the always-on workstation (Claudia / PRO-DYR58J4WA), and is easier to debug.

## Schema

In `PRO_OBSERVABILITY.LAG_OBS`:

- `RUN_LOG` — per-tick task execution metadata (start/end, status, error, possible_missed_window flag)
- `HEARTBEAT_LOG` — append-only per-tick freshness reading (sawtooth on max_source_ts reveals CDC cycle)
- `WATERMARK_STATE` — latest known max source timestamp per (table, column)
- `SEEN_PKS` — cumulative set of observed row IDs per table; insert detection anti-joins against this
- `CHANGE_LOG` — append-only event log; identity = (target_table, row_pk, change_type, source_lastupdateddate)
- `PK_SNAPSHOT_APPLICANT_TAGS`, `PK_SNAPSHOT_APP_SKILLS` — last known PK set, for delete-via-disappearance detection

Views:
- `CURRENT_FRESHNESS` — most recent heartbeat per table
- `CDC_TICKS` — derived from HEARTBEAT_LOG; one row per observed CDC cycle advance
- `CDC_INTERVAL_BY_TABLE_24H` — rolling 24h p50/p95/max of inter-tick interval
- `CDC_INTERVAL_BY_HOUR_OF_DAY_ET` — same, grouped by ET hour-of-day
- `CHANGE_VOLUME_24H` — recent change-event volume + observation-latency stats per table/type
- `LAG_BREACHES` — events with observation_latency_sec > 1800 (30 min)
- `RUN_HEALTH_24H` — task execution health rollup

## Operations

### One-time setup (already complete in PRO Snowflake)

```bash
# Apply substrate DDL (database, schema, warehouse, role, tables)
node tools/apply.js sql/01_substrate.sql --continue-on-error

# Apply views
node tools/apply.js sql/02_views.sql

# Verify role grants
node tools/verify-role.js

# One-time seed (populates SEEN_PKS + PK_SNAPSHOT_* with current state)
node tools/seed.js
```

### Run a single poll cycle (manual)

```bash
node tools/poll.js                # heartbeat + insert/update/delete detection (all 5 tables)
node tools/poll.js --snapshot-diff  # silent-delete sweep on APPLICANT_TAGS + APP_SKILLS
```

### Schedule (Windows Task Scheduler)

Two registered tasks on Claudia (PRO-DYR58J4WA):
- `LagObserver-Poll` — runs `tools/poll-wrapper.ps1` every 1 minute
- `LagObserver-SnapshotDiff` — runs `tools/poll-wrapper.ps1 snapshot-diff` every 5 minutes

Logs land in `logs/poll-YYYYMMDD.log`.

To inspect / modify:
```powershell
Get-ScheduledTask -TaskName 'LagObserver-*'
Get-ScheduledTaskInfo -TaskName 'LagObserver-Poll'
Disable-ScheduledTask -TaskName 'LagObserver-Poll'  # pause
Enable-ScheduledTask -TaskName 'LagObserver-Poll'   # resume
```

## Querying

```sql
USE ROLE LAG_OBSERVER_ROLE;
USE DATABASE PRO_OBSERVABILITY;
USE SCHEMA LAG_OBS;
USE WAREHOUSE LAG_OBS_WH;

-- How fresh is each table right now?
SELECT * FROM CURRENT_FRESHNESS ORDER BY staleness_sec DESC;

-- What's the empirical CDC cycle interval per table?
SELECT * FROM CDC_INTERVAL_BY_TABLE_24H;

-- Which events took longer than usual to surface?
SELECT * FROM LAG_BREACHES;

-- Are observer runs healthy?
SELECT * FROM RUN_HEALTH_24H;
```

## Caveats

- **`possible_missed_window` flag** trips when the new max_source_ts jumped >10 min since the last poll. Could mean (a) the CDC cycle is genuinely longer than 10 min for that table, or (b) we missed a polling tick and ETL fired multiple times in between. Look at recent RUN_LOG entries to disambiguate. Conservative threshold; will tune once we have empirical baselines.
- **Cold-start updates** — first poll captures the snapshot of currently `CDCSTATUS='U'`-flagged rows as updates. After that, only newly-flagged rows get logged. Expected behavior; not a bug.
- **APPLICANT_TAGS uniqueness:** `(TAGAPPLICANT, TAGTAG)` is NOT unique on this table — a single talent can have the same tag-bucket applied many times with different TAGDETAIL. Unique row identity is `TAGID`. Captured in V-CDC-02 / pro-data-architecture references.
- **ACTIVITY_FACT watermarking** uses `ACTDATETIME` (event time, not commit time) with a 24h overlap window because `ACTIVITYKEY` and `ACTID` failed monotonicity tests (37% / 25% inversion rate respectively). Past-dated activities arriving more than 24h after their ACTDATETIME would be missed; in practice this is rare.

## Open questions for v0.2

- Confirm or refute the silent-delete vs zombie-row hypothesis on APPLICANT_TAGS by comparing a known talent's tags in BOLD UI vs DataLink view (Cucumber Testpickles 199528218, Training Only branch).
- Add row_hash for content-change detection (catches updates that don't move LASTUPDATEDDATE — currently believed to not exist on this share since LASTUPDATEDDATE is bulk-rewritten on every rebuild, but worth verifying).
- Retention policy for CHANGE_LOG (currently unbounded). Plan: 30d detail / 90d summary / 365d daily aggregates per Codex recommendation.
- Postgres mirror lag — once COMPAGNO_PG starts streaming the relevant tables, add observation-latency tracking on the mirror side.

## Related work

- `pro-skills/pro-data-architecture/references/datalink-mutability-verification.md` — V-LAG-01 (originated this work)
- `pro-skills/pro-data-architecture/references/datalink-vs-api-comparison.md` — where the empirical numbers feed back
- `pro-skills/pro-brainstorm/SKILL.md` — Idea #12 (Candidate Status Visibility on Matcher) which is gated on this data
