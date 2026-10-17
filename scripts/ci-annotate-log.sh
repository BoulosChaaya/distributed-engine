#!/usr/bin/env bash
# Re-emits a log as GitHub annotations so a run's outcome can be read from
# the checks API without downloading the raw job log:
#   - "summary": every step header and PASS/FAIL/SKIP/ok/RESULT line;
#   - "tail":    the last N lines (failure diagnostics live there).
# Usage: ci-annotate-log.sh <log file> <title> [tail lines]
set -euo pipefail
file=$1
title=$2
lines=${3:-120}
[ -f "$file" ] || exit 0
level=notice
if grep -qE 'FAIL|RESULT: RUNTIME ACCEPTANCE (FAIL|INCOMPLETE)' "$file"; then level=error; fi
tmp="${RUNNER_TEMP:-/tmp}/annotate"
rm -rf "$tmp" && mkdir -p "$tmp"

emit() {
  local name=$1 source=$2 n=0
  split -l 60 "$source" "$tmp/$name-"
  for chunk in "$tmp/$name"-*; do
    n=$((n + 1))
    body=$(sed -e 's/%/%25/g' -e 's/\r/%0D/g' "$chunk" | awk '{printf "%s%%0A", $0}')
    echo "::${level} title=${title} ${name} (part ${n})::${body}"
  done
}

sed -e 's/\x1b\[[0-9;]*m//g' "$file" > "$tmp/clean"
grep -E '^(==|===)|PASS|FAIL|SKIP|^ok:|RESULT|Error|error:' "$tmp/clean" | head -180 > "$tmp/summary" || true
tail -n "$lines" "$tmp/clean" > "$tmp/tail"
emit summary "$tmp/summary"
emit tail "$tmp/tail"
