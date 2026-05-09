# Wrapper invoked by Windows Task Scheduler.
# Picks up CLAUDE_ADMIN env vars from user scope (the scheduled task may run
# in a session where they're not auto-loaded).
$env:SNOWFLAKE_ACCOUNT = 'LMETCQH-LF52449'
$env:SNOWFLAKE_ADMIN_USER = 'CLAUDE_ADMIN'
# Hardcoded to non-virtualized path so the scheduled task (outside the Claude
# AppContainer sandbox) can read it. Original env-var path resolves correctly
# from inside the sandbox but not from Task Scheduler's process context.
$env:SNOWFLAKE_ADMIN_PRIVATE_KEY_PATH = 'C:\Users\ClaudiaAI\.snowflake-keys\snowflake-claude-admin.pem'

$repo = 'C:\Repos\lag-observer'
$logDir = "$repo\logs"
if (-not (Test-Path $logDir)) { New-Item -ItemType Directory -Path $logDir -Force | Out-Null }

$mode = $args[0]
$logFile = "$logDir\poll-$(Get-Date -Format 'yyyyMMdd').log"

$timestamp = Get-Date -Format 'yyyy-MM-ddTHH:mm:ss'
"[$timestamp] === poll start (mode=$mode) ===" | Add-Content $logFile

try {
  if ($mode -eq 'snapshot-diff') {
    & node "$repo\tools\poll.js" --snapshot-diff 2>&1 | Add-Content $logFile
  } else {
    & node "$repo\tools\poll.js" 2>&1 | Add-Content $logFile
  }
  $ok = $LASTEXITCODE
  "[$timestamp] === poll end (exit=$ok) ===" | Add-Content $logFile
  exit $ok
} catch {
  "[$timestamp] FATAL: $_" | Add-Content $logFile
  exit 1
}
