#!/bin/bash
# Logs one line per run of a Sqoop or cron job so the dashboard can show it.
#
# Usage:   job_wrap.sh <job_name> <Sqoop|Cron> <command> [arguments...]
# Example: 0 3 * * * /opt/bdr-dashboard/job_wrap.sh sqoop_orders Sqoop /opt/jobs/sqoop_orders.sh
#
# Writes to $BDR_LOG_DIR/extra_runs.csv (default /var/log/bdr_jobs).
# Data transferred is read from Sqoop's "Transferred 1.2 GB" lines; it is 0 for jobs that print none.

LOG_DIR="${BDR_LOG_DIR:-/var/log/bdr_jobs}"
CSV="$LOG_DIR/extra_runs.csv"

if [ $# -lt 3 ]; then
  echo "usage: $0 <job_name> <Sqoop|Cron> <command> [arguments...]" >&2
  exit 2
fi
name="$1"; category="$2"; shift 2

mkdir -p "$LOG_DIR"
out=$(mktemp)
start=$(date -Iseconds)

"$@" 2>&1 | tee "$out"
rc=${PIPESTATUS[0]}

end=$(date -Iseconds)
status=SUCCESS
[ "$rc" -ne 0 ] && status=FAILED

bytes=$(grep -Eo 'Transferred [0-9.]+ (bytes|KB|MB|GB)' "$out" | awk '
  { m = 1; if ($3 == "KB") m = 1024; else if ($3 == "MB") m = 1048576; else if ($3 == "GB") m = 1073741824; s += $2 * m }
  END { printf "%.0f", s }')
bytes=${bytes:-0}

{
  flock 9
  [ -s "$CSV" ] || echo "job,category,start,end,status,bytes,message" >> "$CSV"
  printf '%s,%s,%s,%s,%s,%s,exit code %s\n' "$name" "$category" "$start" "$end" "$status" "$bytes" "$rc" >> "$CSV"
} 9>>"$CSV.lock"

rm -f "$out"
exit "$rc"
