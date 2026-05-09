#!/usr/bin/env node
// Quick status check: latest heartbeat per table + recent change-log volume +
// run-log health.

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

async function main() {
  await new Promise((res, rej) => conn.connect(err => err ? rej(err) : res()));
  await q(`USE ROLE LAG_OBSERVER_ROLE`);
  await q(`USE WAREHOUSE LAG_OBS_WH`);
  await q(`USE SCHEMA PRO_OBSERVABILITY.LAG_OBS`);

  console.log('\n=== CURRENT_FRESHNESS ===');
  const fresh = await q(`SELECT * FROM CURRENT_FRESHNESS ORDER BY target_table`);
  for (const r of fresh) {
    console.log(`  ${r.TARGET_TABLE.padEnd(16)} max_source_ts=${r.MAX_SOURCE_TS}  staleness=${r.STALENESS_SEC}s  rows=${r.ROW_COUNT}  CDC(I/U/D)=${r.CDC_I_COUNT}/${r.CDC_U_COUNT}/${r.CDC_D_COUNT}`);
  }

  console.log('\n=== HEARTBEAT_LOG count + most-recent ===');
  const hb = await q(`
    SELECT target_table, COUNT(*) AS n, MIN(observed_at) AS first_seen, MAX(observed_at) AS last_seen
    FROM HEARTBEAT_LOG GROUP BY target_table ORDER BY target_table
  `);
  for (const r of hb) console.log(`  ${r.TARGET_TABLE.padEnd(16)} ${r.N} heartbeats  first=${r.FIRST_SEEN}  last=${r.LAST_SEEN}`);

  console.log('\n=== CHANGE_LOG count by table/type (24h) ===');
  const cl = await q(`SELECT * FROM CHANGE_VOLUME_24H ORDER BY target_table, change_type`);
  for (const r of cl) {
    console.log(`  ${r.TARGET_TABLE.padEnd(16)} ${r.CHANGE_TYPE} count=${r.EVENT_COUNT}  p50=${r.P50_LATENCY_SEC}s  p95=${r.P95_LATENCY_SEC}s  max=${r.MAX_LATENCY_SEC}s`);
  }

  console.log('\n=== RUN_HEALTH_24H ===');
  const rh = await q(`SELECT * FROM RUN_HEALTH_24H ORDER BY target_table, task_kind`);
  for (const r of rh) {
    console.log(`  ${r.TARGET_TABLE.padEnd(16)} ${r.TASK_KIND.padEnd(22)} ${r.STATUS} runs=${r.RUN_COUNT}  changes=${r.TOTAL_CHANGES_LOGGED}  missed=${r.POSSIBLY_MISSED_WINDOWS}  avg_runtime=${r.AVG_RUNTIME_SEC}s  max=${r.MAX_RUNTIME_SEC}s`);
  }

  console.log('\n=== Recent RUN_LOG (last 10) ===');
  const rl = await q(`
    SELECT target_table, task_kind, started_at, finished_at, status, error_msg, rows_inserted_to_changelog, possible_missed_window
    FROM RUN_LOG ORDER BY started_at DESC LIMIT 10
  `);
  for (const r of rl) {
    console.log(`  ${r.STARTED_AT.toISOString().slice(11, 19)} ${r.TARGET_TABLE.padEnd(16)} ${r.TASK_KIND.padEnd(22)} ${r.STATUS} rows=${r.ROWS_INSERTED_TO_CHANGELOG}${r.POSSIBLE_MISSED_WINDOW ? ' MW' : ''}${r.ERROR_MSG ? ' ERR:' + r.ERROR_MSG.slice(0, 80) : ''}`);
  }

  conn.destroy(() => {});
}

main().catch(err => { console.error('FAIL:', err.message); process.exit(1); });
