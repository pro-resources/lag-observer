#!/usr/bin/env node
// lag-observer polling runner — single cycle.
// Runs heartbeat + insert/update/delete detection for all 5 target tables,
// writes to HEARTBEAT_LOG, CHANGE_LOG, SEEN_PKS, WATERMARK_STATE, RUN_LOG.
//
// Designed to be invoked by Windows Task Scheduler every minute.
//
// Usage: node poll.js
//        node poll.js --snapshot-diff   (run periodic delete-detection sweep)

const fs = require('fs');
const snowflake = require('snowflake-sdk');
snowflake.configure({ ocspFailOpen: true });

const SNAPSHOT_DIFF = process.argv.includes('--snapshot-diff');

const conn = snowflake.createConnection({
  account: process.env.SNOWFLAKE_ACCOUNT,
  username: process.env.SNOWFLAKE_ADMIN_USER,
  authenticator: 'SNOWFLAKE_JWT',
  privateKey: fs.readFileSync(process.env.SNOWFLAKE_ADMIN_PRIVATE_KEY_PATH, 'utf8'),
  warehouse: 'LAG_OBS_WH',
  clientSessionKeepAlive: true,
});

const q = (sql) => new Promise((res, rej) =>
  conn.execute({ sqlText: sql, complete: (err, _, rows) => err ? rej(err) : res(rows || []) })
);

// Per-table config. Fact-table entries (ACTIVITY_FACT, PLACEMENT_FACT) have no
// CDCSTATUS and use event-time (ACTDATETIME) as watermark instead of
// LASTUPDATEDDATE. They also accept a small false-negative rate on insert
// detection because their *_KEY PKs are not 100% unique (e.g. PLACEMENT_FACT
// has 0.07% duplicate PLACEMENTACTIVITYKEY).
//
// Expansion 2026-05-14 (v2): added 10 tables driving PRO_PG refresh-strategy
// classification (Path B). Resume Builder consumers: APP_ANSWERS, REQ_SKILLS,
// REQ, REQ_NOTES, COMPANY. Universal: APP_STATUS (status lookup),
// PLACEMENT_FACT (12.2M-row fact table). Customer-side for Sales Pipeline Intel
// + Chrome ext: HIRING_MANAGER, CONT_ACTIVITY, CONT_STATUS. PKs verified 100%
// unique with 0 nulls on the 9 standard tables; PLACEMENT_FACT's PK is the
// same-shape compromise as ACTIVITY_FACT (event-time watermarked, fat-tail
// duplicates accepted).
const TABLES = [
  { name: 'REQ_HIRED',       pk: 'REQHIREDID',          tsCol: 'LASTUPDATEDDATE', hasCdcStatus: true,  view: 'PROD_ANALYTICS_PRO.DATALINK.REQ_HIRED'       },
  { name: 'APP_NOMINATE',    pk: 'NOMID',               tsCol: 'LASTUPDATEDDATE', hasCdcStatus: true,  view: 'PROD_ANALYTICS_PRO.DATALINK.APP_NOMINATE'    },
  { name: 'APPLICANT_TAGS',  pk: 'TAGID',               tsCol: 'LASTUPDATEDDATE', hasCdcStatus: true,  view: 'PROD_ANALYTICS_PRO.DATALINK.APPLICANT_TAGS'  },
  { name: 'APP_SKILLS',      pk: 'SKILLID',             tsCol: 'LASTUPDATEDDATE', hasCdcStatus: true,  view: 'PROD_ANALYTICS_PRO.DATALINK.APP_SKILLS'      },
  { name: 'ACTIVITY_FACT',   pk: 'ACTIVITYKEY',         tsCol: 'ACTDATETIME',     hasCdcStatus: false, view: 'PROD_ANALYTICS_PRO.DATALINK.ACTIVITY_FACT'   },
  { name: 'APPLICANTS',      pk: 'ID',                  tsCol: 'LASTUPDATEDDATE', hasCdcStatus: true,  view: 'PROD_ANALYTICS_PRO.DATALINK.APPLICANTS'      },
  { name: 'APP_DOC_UPLOAD',  pk: 'UPID',                tsCol: 'LASTUPDATEDDATE', hasCdcStatus: true,  view: 'PROD_ANALYTICS_PRO.DATALINK.APP_DOC_UPLOAD'  },
  { name: 'APP_JOB_HISTORY', pk: 'JOBHISTID',           tsCol: 'LASTUPDATEDDATE', hasCdcStatus: true,  view: 'PROD_ANALYTICS_PRO.DATALINK.APP_JOB_HISTORY' },
  // Wave 2a — Resume Builder consumers
  { name: 'APP_ANSWERS',     pk: 'APPANSWERID',         tsCol: 'LASTUPDATEDDATE', hasCdcStatus: true,  view: 'PROD_ANALYTICS_PRO.DATALINK.APP_ANSWERS'     },
  { name: 'REQ_SKILLS',      pk: 'REQSKILLID',          tsCol: 'LASTUPDATEDDATE', hasCdcStatus: true,  view: 'PROD_ANALYTICS_PRO.DATALINK.REQ_SKILLS'      },
  { name: 'REQ',             pk: 'REQID',               tsCol: 'LASTUPDATEDDATE', hasCdcStatus: true,  view: 'PROD_ANALYTICS_PRO.DATALINK.REQ'             },
  { name: 'REQ_NOTES',       pk: 'NOTEID',              tsCol: 'LASTUPDATEDDATE', hasCdcStatus: true,  view: 'PROD_ANALYTICS_PRO.DATALINK.REQ_NOTES'       },
  { name: 'COMPANY',         pk: 'COMPANYID',           tsCol: 'LASTUPDATEDDATE', hasCdcStatus: true,  view: 'PROD_ANALYTICS_PRO.DATALINK.COMPANY'         },
  // Wave 2b — universal must-have
  { name: 'APP_STATUS',      pk: 'APPSTATID',           tsCol: 'LASTUPDATEDDATE', hasCdcStatus: true,  view: 'PROD_ANALYTICS_PRO.DATALINK.APP_STATUS'      },
  { name: 'PLACEMENT_FACT',  pk: 'PLACEMENTACTIVITYKEY',tsCol: 'ACTDATETIME',     hasCdcStatus: false, view: 'PROD_ANALYTICS_PRO.DATALINK.PLACEMENT_FACT'  },
  // Wave 2c — customer-side, ahead-of-demand for Sales Pipeline Intel + Chrome ext
  { name: 'HIRING_MANAGER',  pk: 'HIRINGMANAGERID',     tsCol: 'LASTUPDATEDDATE', hasCdcStatus: true,  view: 'PROD_ANALYTICS_PRO.DATALINK.HIRING_MANAGER'  },
  { name: 'CONT_ACTIVITY',   pk: 'CONTACTID',           tsCol: 'LASTUPDATEDDATE', hasCdcStatus: true,  view: 'PROD_ANALYTICS_PRO.DATALINK.CONT_ACTIVITY'   },
  { name: 'CONT_STATUS',     pk: 'CONTSTATID',          tsCol: 'LASTUPDATEDDATE', hasCdcStatus: true,  view: 'PROD_ANALYTICS_PRO.DATALINK.CONT_STATUS'     },
];

async function processTable(t) {
  // 1. Open RUN_LOG entry
  const runIdRow = await q(`
    INSERT INTO RUN_LOG (target_table, task_kind, status)
    VALUES ('${t.name}', 'heartbeat_and_changes', 'RUNNING')
  `);
  // We don't get the auto-gen run_id back from INSERT; query the most recent
  // RUNNING row for this table instead.
  const runIdQ = await q(`
    SELECT run_id, started_at FROM RUN_LOG
    WHERE target_table='${t.name}' AND status='RUNNING'
    ORDER BY started_at DESC LIMIT 1
  `);
  const runId = runIdQ[0].RUN_ID;
  const startedAt = runIdQ[0].STARTED_AT;

  let totalInserted = 0;
  let possibleMissedWindow = false;
  let errorMsg = null;

  try {
    // 2. Heartbeat: capture freshness state.
    // Single INSERT...SELECT computes everything server-side — no JS-side
    // timezone round-trip on TIMESTAMP_NTZ values. staleness_sec is computed
    // by casting both sides to UTC explicitly to avoid implicit NTZ/LTZ skew.
    const cdcCounts = t.hasCdcStatus
      ? `COUNT_IF(CDCSTATUS='I'), COUNT_IF(CDCSTATUS='U'), COUNT_IF(CDCSTATUS='D')`
      : `NULL::NUMBER, NULL::NUMBER, NULL::NUMBER`;
    await q(`
      INSERT INTO HEARTBEAT_LOG (target_table, source_ts_column, max_source_ts, row_count, cdc_i_count, cdc_u_count, cdc_d_count, observed_at, staleness_sec)
      SELECT '${t.name}',
             '${t.tsCol}',
             MAX(${t.tsCol}),
             COUNT(*),
             ${cdcCounts},
             CURRENT_TIMESTAMP()::TIMESTAMP_NTZ,
             TIMESTAMPDIFF('SECOND',
               CONVERT_TIMEZONE('UTC', MAX(${t.tsCol})::TIMESTAMP_LTZ),
               CONVERT_TIMEZONE('UTC', CURRENT_TIMESTAMP()))
      FROM ${t.view}
    `);
    // Read the just-inserted row so the rest of the function can use max_source_ts
    const hbRow = (await q(`
      SELECT max_source_ts AS MAX_TS FROM HEARTBEAT_LOG
      WHERE target_table='${t.name}' ORDER BY observed_at DESC LIMIT 1
    `))[0];

    // 3. Get prior watermark for the windowing predicate.
    const wmRow = (await q(`
      SELECT last_seen_at FROM WATERMARK_STATE
      WHERE target_table='${t.name}' AND watermark_column='${t.tsCol}'
    `))[0];
    const priorWatermark = wmRow ? wmRow.LAST_SEEN_AT : null;

    // Missed-window check: if the new max_source_ts jumped by more than 2x the
    // typical interval since last poll, flag it. Conservative — for now, if
    // the gap exceeds 10 minutes we mark it.
    if (priorWatermark && hbRow.MAX_TS) {
      const gapSec = (new Date(hbRow.MAX_TS) - new Date(priorWatermark)) / 1000;
      if (gapSec > 600) possibleMissedWindow = true;
    }

    // 4. Insert detection (PKs not in SEEN_PKS, in the recent window)
    // For fact tables (no CDCSTATUS, event-time watermark), recent window =
    // last 24h on the event-time column. For others, recent window =
    // LASTUPDATEDDATE > prior_watermark - 60s buffer.
    let recentClause;
    if (!t.hasCdcStatus) {
      recentClause = `${t.tsCol} > DATEADD('HOUR', -24, CURRENT_TIMESTAMP())`;
    } else {
      // Use prior watermark with 60s overlap buffer to handle precision/clock skew
      const priorClause = priorWatermark
        ? `DATEADD('SECOND', -60, '${new Date(priorWatermark).toISOString()}'::TIMESTAMP_NTZ)`
        : `'2000-01-01'::TIMESTAMP_NTZ`;
      recentClause = `${t.tsCol} > ${priorClause}`;
    }

    const insertCount = await q(`
      INSERT INTO CHANGE_LOG (target_table, change_type, row_pk, source_lastupdateddate, cdcstatus_observed, observation_latency_sec)
      SELECT '${t.name}', 'I', t.${t.pk}::VARCHAR,
             ${t.hasCdcStatus ? 't.LASTUPDATEDDATE' : 't.ACTDATETIME::TIMESTAMP_NTZ'},
             ${t.hasCdcStatus ? 't.CDCSTATUS' : "'I'"},
             TIMESTAMPDIFF('SECOND', ${t.hasCdcStatus ? 't.LASTUPDATEDDATE' : 't.ACTDATETIME'}, CURRENT_TIMESTAMP())
      FROM ${t.view} t
      WHERE ${recentClause}
        AND t.${t.pk}::VARCHAR NOT IN (
          SELECT pk_value FROM SEEN_PKS WHERE target_table='${t.name}'
        )
    `);
    const insertedI = insertCount[0]['number of rows inserted'] || 0;
    totalInserted += insertedI;

    // Mirror new PKs into SEEN_PKS so they don't re-fire next tick
    if (insertedI > 0) {
      await q(`
        INSERT INTO SEEN_PKS (target_table, pk_value)
        SELECT '${t.name}', t.${t.pk}::VARCHAR
        FROM ${t.view} t
        WHERE ${recentClause}
          AND t.${t.pk}::VARCHAR NOT IN (
            SELECT pk_value FROM SEEN_PKS WHERE target_table='${t.name}'
          )
      `);
    }

    // 5. Update detection (CDCSTATUS='U' rows we haven't logged yet for the current LASTUPDATEDDATE)
    if (t.hasCdcStatus) {
      const upCount = await q(`
        INSERT INTO CHANGE_LOG (target_table, change_type, row_pk, source_lastupdateddate, cdcstatus_observed, observation_latency_sec)
        SELECT '${t.name}', 'U', t.${t.pk}::VARCHAR, t.LASTUPDATEDDATE, t.CDCSTATUS,
               TIMESTAMPDIFF('SECOND', t.LASTUPDATEDDATE, CURRENT_TIMESTAMP())
        FROM ${t.view} t
        WHERE t.CDCSTATUS = 'U'
          AND NOT EXISTS (
            SELECT 1 FROM CHANGE_LOG c
            WHERE c.target_table='${t.name}'
              AND c.row_pk = t.${t.pk}::VARCHAR
              AND c.change_type = 'U'
              AND c.source_lastupdateddate = t.LASTUPDATEDDATE
          )
      `);
      totalInserted += upCount[0]['number of rows inserted'] || 0;
    }

    // 6. Delete detection (CDCSTATUS='D' for APP_SKILLS)
    if (t.name === 'APP_SKILLS') {
      const dCount = await q(`
        INSERT INTO CHANGE_LOG (target_table, change_type, row_pk, source_lastupdateddate, cdcstatus_observed, observation_latency_sec)
        SELECT 'APP_SKILLS', 'D', t.SKILLID::VARCHAR, t.LASTUPDATEDDATE, t.CDCSTATUS,
               TIMESTAMPDIFF('SECOND', t.LASTUPDATEDDATE, CURRENT_TIMESTAMP())
        FROM PROD_ANALYTICS_PRO.DATALINK.APP_SKILLS t
        WHERE t.CDCSTATUS = 'D'
          AND NOT EXISTS (
            SELECT 1 FROM CHANGE_LOG c
            WHERE c.target_table='APP_SKILLS'
              AND c.row_pk = t.SKILLID::VARCHAR
              AND c.change_type='D'
              AND c.source_lastupdateddate = t.LASTUPDATEDDATE
          )
      `);
      totalInserted += dCount[0]['number of rows inserted'] || 0;
    }

    // 7. Advance the watermark
    await q(`
      UPDATE WATERMARK_STATE
      SET last_seen_at = '${hbRow.MAX_TS.toISOString()}'::TIMESTAMP_NTZ,
          updated_at = CURRENT_TIMESTAMP()
      WHERE target_table='${t.name}' AND watermark_column='${t.tsCol}'
    `);
  } catch (e) {
    errorMsg = e.message.replace(/'/g, "''").slice(0, 500);
  }

  // 8. Close RUN_LOG entry
  await q(`
    UPDATE RUN_LOG
    SET finished_at = CURRENT_TIMESTAMP(),
        status = '${errorMsg ? 'ERROR' : 'OK'}',
        error_msg = ${errorMsg ? `'${errorMsg}'` : 'NULL'},
        rows_inserted_to_changelog = ${totalInserted},
        possible_missed_window = ${possibleMissedWindow}
    WHERE run_id = '${runId}'
  `);

  return { table: t.name, inserted: totalInserted, error: errorMsg, missedWindow: possibleMissedWindow };
}

async function snapshotDiff() {
  const results = [];

  // APPLICANT_TAGS — silent deletes via TAGID set diff
  for (const [t, pkCol, snapTable] of [
    ['APPLICANT_TAGS', 'TAGID',   'PK_SNAPSHOT_APPLICANT_TAGS'],
    ['APP_SKILLS',     'SKILLID', 'PK_SNAPSHOT_APP_SKILLS'],
  ]) {
    await q(`INSERT INTO RUN_LOG (target_table, task_kind, status) VALUES ('${t}', 'snapshot_diff', 'RUNNING')`);
    const runIdQ = await q(`SELECT run_id FROM RUN_LOG WHERE target_table='${t}' AND task_kind='snapshot_diff' AND status='RUNNING' ORDER BY started_at DESC LIMIT 1`);
    const runId = runIdQ[0].RUN_ID;
    let inserted = 0;
    let errorMsg = null;
    try {
      // Find PKs that were in the old snapshot but aren't in the share now → silent deletes
      const dCount = await q(`
        INSERT INTO CHANGE_LOG (target_table, change_type, row_pk, source_lastupdateddate, cdcstatus_observed, observation_latency_sec)
        SELECT '${t}', 'D', s.${pkCol.toLowerCase()}::VARCHAR, NULL, 'silent-disappear', NULL
        FROM ${snapTable} s
        WHERE NOT EXISTS (
          SELECT 1 FROM PROD_ANALYTICS_PRO.DATALINK.${t} v WHERE v.${pkCol} = s.${pkCol.toLowerCase()}
        )
        AND s.${pkCol.toLowerCase()}::VARCHAR NOT IN (
          SELECT row_pk FROM CHANGE_LOG WHERE target_table='${t}' AND change_type='D' AND cdcstatus_observed='silent-disappear'
        )
      `);
      inserted = dCount[0]['number of rows inserted'] || 0;

      // Replace snapshot with current state
      await q(`TRUNCATE TABLE ${snapTable}`);
      await q(`INSERT INTO ${snapTable} (${pkCol.toLowerCase()}) SELECT ${pkCol} FROM PROD_ANALYTICS_PRO.DATALINK.${t}`);
    } catch (e) {
      errorMsg = e.message.replace(/'/g, "''").slice(0, 500);
    }
    await q(`
      UPDATE RUN_LOG
      SET finished_at = CURRENT_TIMESTAMP(),
          status = '${errorMsg ? 'ERROR' : 'OK'}',
          error_msg = ${errorMsg ? `'${errorMsg}'` : 'NULL'},
          rows_inserted_to_changelog = ${inserted}
      WHERE run_id = '${runId}'
    `);
    results.push({ table: t, kind: 'snapshot_diff', inserted, error: errorMsg });
  }
  return results;
}

async function main() {
  await new Promise((res, rej) => conn.connect(err => err ? rej(err) : res()));
  await q(`USE ROLE LAG_OBSERVER_ROLE`);
  await q(`USE WAREHOUSE LAG_OBS_WH`);
  await q(`USE SCHEMA PRO_OBSERVABILITY.LAG_OBS`);
  // Force session TZ to UTC so all NTZ comparisons are unambiguous. Avionte
  // appears to write share NTZ columns in UTC raw; Snowflake's default LA
  // session TZ caused -4h staleness skew before this. (v0.1.1 fix.)
  await q(`ALTER SESSION SET TIMEZONE = 'UTC'`);

  const tStart = Date.now();
  const results = [];

  if (SNAPSHOT_DIFF) {
    const r = await snapshotDiff();
    results.push(...r);
  } else {
    for (const t of TABLES) {
      const r = await processTable(t);
      results.push(r);
    }
  }

  const elapsed = Math.round((Date.now() - tStart) / 1000 * 100) / 100;
  console.log(`Cycle complete in ${elapsed}s`);
  for (const r of results) {
    console.log(`  ${r.table}${r.kind ? ' (' + r.kind + ')' : ''}: inserted=${r.inserted}${r.missedWindow ? ' [MISSED-WINDOW]' : ''}${r.error ? ' ERROR: ' + r.error : ''}`);
  }

  conn.destroy(() => {});
}

main().catch(err => { console.error('FAIL:', err.message); process.exit(1); });
