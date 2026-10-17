#!/usr/bin/env bash
# Chaos test against the full docker compose stack (postgres, redis, api,
# 3 workers, web), all built from this checkout.
#
# While tasks are in flight it:
#   1. SIGKILLs one worker   (no graceful shutdown, its leases are left behind)
#   2. FLUSHALLs Redis       (every queued job and BullMQ's bookkeeping is gone)
# and then requires that:
#   - every task still reaches COMPLETED (none stuck, none FAILED);
#   - every completed task has exactly one recorded result;
#   - nothing is left PENDING or FAILED in the outbox.
#
# Recovery paths exercised: BullMQ stall detection plus lease reclaim (killed
# worker), and the PostgreSQL-driven reconciler (Redis loss).
set -euo pipefail

cd "$(dirname "$0")/.."

TASKS="${TASKS:-60}"
PROJECT=engine-chaos
compose=(docker compose -p "$PROJECT")

export OPERATOR_API_KEY
OPERATOR_API_KEY="$(openssl rand -hex 32)"
export DASHBOARD_USER=chaos
export DASHBOARD_PASSWORD
DASHBOARD_PASSWORD="$(openssl rand -hex 16)"
# Tighter reconciler timing so the test finishes in minutes, not hours.
export RECONCILER_INTERVAL_MS=5000
export RECONCILER_QUEUED_GRACE_MS=10000

cleanup() {
  local result=$?
  if [ "$result" -ne 0 ]; then
    echo "----- chaos test failed; service logs follow -----" >&2
    "${compose[@]}" logs --no-color --tail=200 >&2 || true
  fi
  "${compose[@]}" down -v --remove-orphans >/dev/null 2>&1 || true
  exit "$result"
}
trap cleanup EXIT

sql() {
  "${compose[@]}" exec -T postgres psql -U postgres -d distributed_engine -tAc "$1"
}

api() {
  curl -sf -H "Authorization: Bearer ${OPERATOR_API_KEY}" -H 'Content-Type: application/json' "$@"
}

echo "== Building and starting the stack (3 workers)"
"${compose[@]}" up -d --build --wait --scale worker=3 postgres redis api worker web

echo "== Submitting ${TASKS} tasks (2/3 sleep 3s, 1/3 flaky: fail once, then succeed)"
for i in $(seq 1 "$TASKS"); do
  if [ $((i % 3)) -eq 0 ]; then
    body='{"name":"flaky","maxRetries":3,"payload":{"failTimes":1}}'
  else
    body='{"name":"sleep","maxRetries":3,"payload":{"ms":3000}}'
  fi
  api -X POST -d "$body" http://localhost:3000/tasks >/dev/null
done

echo "== Submitting 5 workflows (fan-out to 4 sleeps, fan-in to a join)"
WORKFLOW='{"name":"chaos-wf","steps":[{"key":"start","name":"echo"},'\
'{"key":"s1","name":"sleep","payload":{"ms":2000},"dependsOn":["start"]},'\
'{"key":"s2","name":"sleep","payload":{"ms":2000},"dependsOn":["start"]},'\
'{"key":"s3","name":"flaky","payload":{"failTimes":1},"dependsOn":["start"]},'\
'{"key":"s4","name":"sleep","payload":{"ms":2000},"dependsOn":["start"]},'\
'{"key":"join","name":"echo","dependsOn":["s1","s2","s3","s4"]}]}'
for _ in 1 2 3 4 5; do
  api -X POST -d "$WORKFLOW" http://localhost:3000/workflows >/dev/null
done
TASKS=$((TASKS + 5 * 6))

echo "== Waiting for work to be in flight"
for _ in $(seq 1 60); do
  processing=$(sql "SELECT COUNT(*) FROM tasks WHERE status = 'PROCESSING'")
  [ "$processing" -ge 3 ] && break
  sleep 1
done
echo "   PROCESSING now: ${processing}"

victim=$("${compose[@]}" ps -q worker | head -1)
victim_held=$(sql "SELECT COUNT(*) FROM tasks WHERE status = 'PROCESSING'")
echo "== CHAOS 1: SIGKILL worker ${victim:0:12} (in flight across workers: ${victim_held})"
docker kill --signal KILL "$victim" >/dev/null

sleep 2
before_flush=$(sql "SELECT status || '=' || COUNT(*) FROM tasks GROUP BY status ORDER BY status" | tr '\n' ' ')
echo "== CHAOS 2: FLUSHALL Redis (task states at this moment: ${before_flush})"
"${compose[@]}" exec -T redis redis-cli FLUSHALL >/dev/null

echo "== Waiting for every task to finish (up to 5 minutes)"
deadline=$((SECONDS + 300))
while [ "$SECONDS" -lt "$deadline" ]; do
  remaining=$(sql "SELECT COUNT(*) FROM tasks WHERE status NOT IN ('COMPLETED', 'FAILED', 'CANCELLED')")
  [ "$remaining" -eq 0 ] && break
  sleep 3
done

completed=$(sql "SELECT COUNT(*) FROM tasks WHERE status = 'COMPLETED'")
failed=$(sql "SELECT COUNT(*) FROM tasks WHERE status = 'FAILED'")
stuck=$(sql "SELECT COUNT(*) FROM tasks WHERE status NOT IN ('COMPLETED', 'FAILED', 'CANCELLED')")
no_result=$(sql "SELECT COUNT(*) FROM tasks WHERE status = 'COMPLETED' AND (result IS NULL OR result->>'processedBy' IS NULL)")
outbox_bad=$(sql "SELECT COUNT(*) FROM outbox_events WHERE status <> 'DELIVERED'")
requeued=$(sql "SELECT COUNT(DISTINCT task_id) FROM outbox_events WHERE event_type = 'TASK_REQUEUED'")
retried=$(sql "SELECT COUNT(*) FROM tasks WHERE retries > 0")
wf_completed=$(sql "SELECT COUNT(*) FROM workflows WHERE status = 'COMPLETED'")
wf_total=$(sql "SELECT COUNT(*) FROM workflows")
workers_used=$(sql "SELECT COUNT(DISTINCT result->>'processedBy') FROM tasks WHERE status = 'COMPLETED'")
reclaims=$("${compose[@]}" logs --no-color worker 2>/dev/null | grep -c 'Reclaimed stalled task' || true)

echo
echo "== Result"
echo "   tasks submitted (incl. steps):   ${TASKS}"
echo "   workflows COMPLETED:             ${wf_completed}/${wf_total}"
echo "   COMPLETED:                       ${completed}"
echo "   FAILED:                          ${failed}"
echo "   not finished:                    ${stuck}"
echo "   COMPLETED without a result:      ${no_result}"
echo "   outbox events not DELIVERED:     ${outbox_bad}"
echo "   tasks requeued by reconciler:    ${requeued}"
echo "   leases reclaimed from dead peer: ${reclaims}"
echo "   tasks that used a retry:         ${retried}"
echo "   distinct workers that completed: ${workers_used}"

ok=true
[ "$completed" -eq "$TASKS" ] || { echo "FAIL: expected ${TASKS} COMPLETED" >&2; ok=false; }
[ "$wf_completed" -eq "$wf_total" ] || { echo "FAIL: only ${wf_completed}/${wf_total} workflows completed" >&2; ok=false; }
[ "$failed" -eq 0 ] || { echo "FAIL: ${failed} tasks FAILED" >&2; ok=false; }
[ "$stuck" -eq 0 ] || { echo "FAIL: ${stuck} tasks never finished" >&2; ok=false; }
[ "$no_result" -eq 0 ] || { echo "FAIL: ${no_result} completed tasks have no recorded result" >&2; ok=false; }
[ "$outbox_bad" -eq 0 ] || { echo "FAIL: ${outbox_bad} outbox events not delivered" >&2; ok=false; }
[ "$requeued" -gt 0 ] || { echo "FAIL: Redis was flushed but the reconciler requeued nothing (the test did not exercise recovery)" >&2; ok=false; }
$ok
echo "== PASS: every task and workflow completed, each recorded once in PostgreSQL, despite a killed worker and a flushed Redis"
