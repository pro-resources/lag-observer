#!/usr/bin/env node
// One-time seed: populate SEEN_PKS + PK_SNAPSHOT_* with current state of each
// target table, so that subsequent polls don't classify every existing row as
// a "new insert."
//
// Idempotent — safe to re-run, but normally run only once at install time.

const fs = require('fs');
const snowflake = require('snowflake-sdk');
snowflake.configure({ ocspFailOpen: true });

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

const SEEDS = [
  // [target_table, pk_column, share_view]
  ['REQ_HIRED',       'REQHIREDID',     'PROD_ANALYTICS_PRO.DATALINK.REQ_HIRED'],
  ['APP_NOMINATE',    'NOMID',          'PROD_ANALYTICS_PRO.DATALINK.APP_NOMINATE'],
  ['APPLICANT_TAGS',  'TAGID',          'PROD_ANALYTICS_PRO.DATALINK.APPLICANT_TAGS'],
  ['APP_SKILLS',      'SKILLID',        'PROD_ANALYTICS_PRO.DATALINK.APP_SKILLS'],
  ['APPLICANTS',      'ID',             'PROD_ANALYTICS_PRO.DATALINK.APPLICANTS'],
  ['APP_DOC_UPLOAD',  'UPID',           'PROD_ANALYTICS_PRO.DATALINK.APP_DOC_UPLOAD'],
  ['APP_JOB_HISTORY', 'JOBHISTID',      'PROD_ANALYTICS_PRO.DATALINK.APP_JOB_HISTORY'],
  // v2 expansion 2026-05-14 — 9 standard-pattern tables. PKs verified 100%
  // unique with 0 nulls against current share state via probe-new-tables.js.
  ['APP_ANSWERS',     'APPANSWERID',    'PROD_ANALYTICS_PRO.DATALINK.APP_ANSWERS'],
  ['REQ_SKILLS',      'REQSKILLID',     'PROD_ANALYTICS_PRO.DATALINK.REQ_SKILLS'],
  ['REQ',             'REQID',          'PROD_ANALYTICS_PRO.DATALINK.REQ'],
  ['REQ_NOTES',       'NOTEID',         'PROD_ANALYTICS_PRO.DATALINK.REQ_NOTES'],
  ['COMPANY',         'COMPANYID',      'PROD_ANALYTICS_PRO.DATALINK.COMPANY'],
  ['APP_STATUS',      'APPSTATID',      'PROD_ANALYTICS_PRO.DATALINK.APP_STATUS'],
  ['HIRING_MANAGER',  'HIRINGMANAGERID','PROD_ANALYTICS_PRO.DATALINK.HIRING_MANAGER'],
  ['CONT_ACTIVITY',   'CONTACTID',      'PROD_ANALYTICS_PRO.DATALINK.CONT_ACTIVITY'],
  ['CONT_STATUS',     'CONTSTATID',     'PROD_ANALYTICS_PRO.DATALINK.CONT_STATUS'],
];

async function main() {
  await new Promise((res, rej) => conn.connect(err => err ? rej(err) : res()));
  await q(`USE ROLE LAG_OBSERVER_ROLE`);
  await q(`USE WAREHOUSE LAG_OBS_WH`);
  await q(`USE SCHEMA PRO_OBSERVABILITY.LAG_OBS`);

  const tStart = Date.now();

  // Seed SEEN_PKS for the 4 watermark-friendly tables (full historical PK set)
  for (const [t, pk, view] of SEEDS) {
    process.stdout.write(`Seeding SEEN_PKS for ${t} (${pk}) ... `);
    const r = await q(`
      INSERT INTO SEEN_PKS (target_table, pk_value)
      SELECT '${t}', ${pk}::VARCHAR
      FROM ${view}
      WHERE ${pk}::VARCHAR NOT IN (SELECT pk_value FROM SEEN_PKS WHERE target_table = '${t}')
    `);
    console.log(`inserted ${r[0]['number of rows inserted']}`);
  }

  // ACTIVITY_FACT: only seed PKs from the last 24h (overlap window). Older rows
  // are never going to surface in the polling window anyway.
  process.stdout.write(`Seeding SEEN_PKS for ACTIVITY_FACT (ACTIVITYKEY, last 24h) ... `);
  const af = await q(`
    INSERT INTO SEEN_PKS (target_table, pk_value)
    SELECT 'ACTIVITY_FACT', ACTIVITYKEY::VARCHAR
    FROM PROD_ANALYTICS_PRO.DATALINK.ACTIVITY_FACT
    WHERE ACTDATETIME > DATEADD('HOUR', -24, CURRENT_TIMESTAMP())
      AND ACTIVITYKEY::VARCHAR NOT IN (SELECT pk_value FROM SEEN_PKS WHERE target_table = 'ACTIVITY_FACT')
  `);
  console.log(`inserted ${af[0]['number of rows inserted']}`);

  // PLACEMENT_FACT: same fact-table pattern — last 24h on ACTDATETIME. DISTINCT
  // because PLACEMENTACTIVITYKEY is 99.93% unique (8,844 dupes out of 12.2M).
  process.stdout.write(`Seeding SEEN_PKS for PLACEMENT_FACT (PLACEMENTACTIVITYKEY, last 24h) ... `);
  const pf = await q(`
    INSERT INTO SEEN_PKS (target_table, pk_value)
    SELECT DISTINCT 'PLACEMENT_FACT', PLACEMENTACTIVITYKEY::VARCHAR
    FROM PROD_ANALYTICS_PRO.DATALINK.PLACEMENT_FACT
    WHERE ACTDATETIME > DATEADD('HOUR', -24, CURRENT_TIMESTAMP())
      AND PLACEMENTACTIVITYKEY::VARCHAR NOT IN (SELECT pk_value FROM SEEN_PKS WHERE target_table = 'PLACEMENT_FACT')
  `);
  console.log(`inserted ${pf[0]['number of rows inserted']}`);

  // Seed PK_SNAPSHOT_APPLICANT_TAGS and PK_SNAPSHOT_APP_SKILLS with current state
  process.stdout.write(`Seeding PK_SNAPSHOT_APPLICANT_TAGS ... `);
  await q(`TRUNCATE TABLE PK_SNAPSHOT_APPLICANT_TAGS`);
  const pat = await q(`
    INSERT INTO PK_SNAPSHOT_APPLICANT_TAGS (tagid)
    SELECT TAGID FROM PROD_ANALYTICS_PRO.DATALINK.APPLICANT_TAGS
  `);
  console.log(`inserted ${pat[0]['number of rows inserted']}`);

  process.stdout.write(`Seeding PK_SNAPSHOT_APP_SKILLS ... `);
  await q(`TRUNCATE TABLE PK_SNAPSHOT_APP_SKILLS`);
  const pas = await q(`
    INSERT INTO PK_SNAPSHOT_APP_SKILLS (skillid)
    SELECT SKILLID FROM PROD_ANALYTICS_PRO.DATALINK.APP_SKILLS
  `);
  console.log(`inserted ${pas[0]['number of rows inserted']}`);

  // Initialize WATERMARK_STATE rows so first poll has a non-NULL prior value
  process.stdout.write(`Initializing WATERMARK_STATE ... `);
  await q(`
    MERGE INTO WATERMARK_STATE w
    USING (
      SELECT 'REQ_HIRED'       AS tt, 'LASTUPDATEDDATE' AS wc UNION ALL
      SELECT 'APP_NOMINATE',                  'LASTUPDATEDDATE' UNION ALL
      SELECT 'APPLICANT_TAGS',                'LASTUPDATEDDATE' UNION ALL
      SELECT 'APP_SKILLS',                    'LASTUPDATEDDATE' UNION ALL
      SELECT 'ACTIVITY_FACT',                 'ACTDATETIME'     UNION ALL
      SELECT 'APPLICANTS',                    'LASTUPDATEDDATE' UNION ALL
      SELECT 'APP_DOC_UPLOAD',                'LASTUPDATEDDATE' UNION ALL
      SELECT 'APP_JOB_HISTORY',               'LASTUPDATEDDATE' UNION ALL
      SELECT 'APP_ANSWERS',                   'LASTUPDATEDDATE' UNION ALL
      SELECT 'REQ_SKILLS',                    'LASTUPDATEDDATE' UNION ALL
      SELECT 'REQ',                           'LASTUPDATEDDATE' UNION ALL
      SELECT 'REQ_NOTES',                     'LASTUPDATEDDATE' UNION ALL
      SELECT 'COMPANY',                       'LASTUPDATEDDATE' UNION ALL
      SELECT 'APP_STATUS',                    'LASTUPDATEDDATE' UNION ALL
      SELECT 'PLACEMENT_FACT',                'ACTDATETIME'     UNION ALL
      SELECT 'HIRING_MANAGER',                'LASTUPDATEDDATE' UNION ALL
      SELECT 'CONT_ACTIVITY',                 'LASTUPDATEDDATE' UNION ALL
      SELECT 'CONT_STATUS',                   'LASTUPDATEDDATE'
    ) src
    ON w.target_table = src.tt AND w.watermark_column = src.wc
    WHEN NOT MATCHED THEN INSERT (target_table, watermark_column, last_seen_at, updated_at)
      VALUES (src.tt, src.wc, '2000-01-01'::TIMESTAMP_NTZ, CURRENT_TIMESTAMP())
  `);
  console.log('OK');

  console.log(`\nSeed complete in ${Math.round((Date.now() - tStart) / 1000)}s`);
  conn.destroy(() => {});
}

main().catch(err => { console.error('FAIL:', err.message); process.exit(1); });
