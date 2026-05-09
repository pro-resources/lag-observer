#!/usr/bin/env node
// Apply a Snowflake SQL file using CLAUDE_ADMIN key-pair auth.
// Usage: node apply.js <path-to-sql-file> [--continue-on-error]
//
// Splits the file on bare semicolons (outside of strings/comments) and runs
// each statement in order. Logs PASS/FAIL per statement. By default stops on
// first error; pass --continue-on-error to log and proceed.

const fs = require('fs');
const path = require('path');
const snowflake = require('snowflake-sdk');
snowflake.configure({ ocspFailOpen: true });

const sqlPath = process.argv[2];
const continueOnError = process.argv.includes('--continue-on-error');
if (!sqlPath) {
  console.error('Usage: node apply.js <path-to-sql-file> [--continue-on-error]');
  process.exit(1);
}

const conn = snowflake.createConnection({
  account: process.env.SNOWFLAKE_ACCOUNT,
  username: process.env.SNOWFLAKE_ADMIN_USER,
  authenticator: 'SNOWFLAKE_JWT',
  privateKey: fs.readFileSync(process.env.SNOWFLAKE_ADMIN_PRIVATE_KEY_PATH, 'utf8'),
  warehouse: 'COMPUTE_WH',
  clientSessionKeepAlive: true,
});

function q(sqlText) {
  return new Promise((resolve, reject) => {
    conn.execute({
      sqlText,
      complete: (err, _, rows) => err ? reject(err) : resolve(rows || []),
    });
  });
}

// Naive but adequate SQL splitter: strips line comments + block comments,
// then splits on semicolons that aren't inside string literals.
function splitStatements(sql) {
  // Strip line comments (-- to EOL)
  const noLineComments = sql.replace(/--[^\n]*/g, '');
  // Strip block comments
  const noBlockComments = noLineComments.replace(/\/\*[\s\S]*?\*\//g, '');

  const statements = [];
  let buf = '';
  let inSingle = false;
  let inDouble = false;
  for (let i = 0; i < noBlockComments.length; i++) {
    const ch = noBlockComments[i];
    if (ch === "'" && !inDouble) inSingle = !inSingle;
    else if (ch === '"' && !inSingle) inDouble = !inDouble;
    if (ch === ';' && !inSingle && !inDouble) {
      const s = buf.trim();
      if (s) statements.push(s);
      buf = '';
    } else {
      buf += ch;
    }
  }
  const tail = buf.trim();
  if (tail) statements.push(tail);
  return statements;
}

async function main() {
  const sql = fs.readFileSync(sqlPath, 'utf8');
  const statements = splitStatements(sql);
  console.log(`Loaded ${path.basename(sqlPath)}: ${statements.length} statements`);

  await new Promise((res, rej) => conn.connect(err => err ? rej(err) : res()));

  let pass = 0;
  let fail = 0;
  for (let i = 0; i < statements.length; i++) {
    const stmt = statements[i];
    const preview = stmt.replace(/\s+/g, ' ').slice(0, 100);
    process.stdout.write(`  [${i + 1}/${statements.length}] ${preview} ... `);
    try {
      await q(stmt);
      console.log('OK');
      pass++;
    } catch (e) {
      console.log(`FAIL: ${e.message.slice(0, 200)}`);
      fail++;
      if (!continueOnError) {
        console.error(`\nAborted at statement ${i + 1}. Re-run with --continue-on-error to ignore.`);
        conn.destroy(() => {});
        process.exit(2);
      }
    }
  }

  console.log(`\nDone: ${pass} passed, ${fail} failed`);
  conn.destroy(() => {});
  process.exit(fail > 0 ? 2 : 0);
}

main().catch(err => {
  console.error('FAIL:', err.message);
  process.exit(1);
});
