#!/usr/bin/env node
// Verify LAG_OBSERVER_ROLE can do what it needs to:
//   - SELECT from each of the 5 DataLink share views
//   - INSERT into the LAG_OBS tables
// If SELECT works via IMPORTED PRIVILEGES (despite the prior PRO finding that
// it "no-ops"), we're fine. Otherwise we need to fall back to running as
// CLAUDE_ADMIN directly.

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

async function tryQ(label, sql) {
  try {
    const r = await q(sql);
    console.log(`  PASS  ${label}: ${JSON.stringify(r[0] || {}).slice(0, 80)}`);
    return true;
  } catch (e) {
    console.log(`  FAIL  ${label}: ${e.message.slice(0, 200)}`);
    return false;
  }
}

async function main() {
  await new Promise((res, rej) => conn.connect(err => err ? rej(err) : res()));

  console.log('\n=== As LAG_OBSERVER_ROLE ===');
  await q(`USE ROLE LAG_OBSERVER_ROLE`);
  await q(`USE WAREHOUSE LAG_OBS_WH`);

  console.log('\n-- DataLink share SELECT access --');
  for (const t of ['REQ_HIRED', 'ACTIVITY_FACT', 'APP_NOMINATE', 'APPLICANT_TAGS', 'APP_SKILLS']) {
    await tryQ(t, `SELECT COUNT(*) AS N FROM PROD_ANALYTICS_PRO.DATALINK.${t}`);
  }

  console.log('\n-- LAG_OBS table INSERT/SELECT access --');
  await tryQ('USE PRO_OBSERVABILITY.LAG_OBS', `USE SCHEMA PRO_OBSERVABILITY.LAG_OBS`);
  await tryQ('INSERT into RUN_LOG', `INSERT INTO RUN_LOG (target_table, task_kind, status) VALUES ('verify-role', 'verify', 'OK')`);
  await tryQ('SELECT from RUN_LOG', `SELECT COUNT(*) AS N FROM RUN_LOG`);
  await tryQ('SELECT from CHANGE_LOG', `SELECT COUNT(*) AS N FROM CHANGE_LOG`);
  await tryQ('SELECT from HEARTBEAT_LOG', `SELECT COUNT(*) AS N FROM HEARTBEAT_LOG`);

  console.log('\n=== As CLAUDE_ADMIN default (SYSADMIN) ===');
  await q(`USE ROLE SYSADMIN`);
  console.log('\n-- DataLink share SELECT access --');
  for (const t of ['REQ_HIRED', 'ACTIVITY_FACT', 'APP_NOMINATE', 'APPLICANT_TAGS', 'APP_SKILLS']) {
    await tryQ(t, `SELECT COUNT(*) AS N FROM PROD_ANALYTICS_PRO.DATALINK.${t}`);
  }

  conn.destroy(() => {});
}

main().catch(err => { console.error('FAIL:', err.message); process.exit(1); });
