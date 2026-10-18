#!/usr/bin/env bash
# Proves the Prometheus metrics against the real compose stack:
#   - Prometheus scrapes the API (with the operator key) and every worker;
#   - task, queue and outbox gauges and worker job counters carry real values
#     after real work;
#   - with Redis stopped, engine_queue_up drops to 0 and the scrape still
#     succeeds (PostgreSQL-backed gauges keep reporting), then recovers.
set -euo pipefail

cd "$(dirname "$0")/.."

PROJECT=engine-metrics
compose=(docker compose -p "$PROJECT" --profile metrics)
TASKS=12

export OPERATOR_API_KEY
OPERATOR_API_KEY="$(openssl rand -hex 32)"
export DASHBOARD_USER=metrics
export DASHBOARD_PASSWORD
DASHBOARD_PASSWORD="$(openssl rand -hex 16)"

cleanup() {
  local result=$?
  if [ "$result" -ne 0 ]; then
    echo "----- metrics test failed; service logs follow -----" >&2
    "${compose[@]}" logs --no-color --tail=120 >&2 || true
  fi
  "${compose[@]}" down -v --remove-orphans >/dev/null 2>&1 || true
  exit "$result"
}
trap cleanup EXIT

api() {
  curl -sf -H "Authorization: Bearer ${OPERATOR_API_KEY}" -H 'Content-Type: application/json' "$@"
}

# Value of a PromQL query (first series), or empty.
promq() {
  curl -sf --get --data-urlencode "query=$1" http://localhost:9090/api/v1/query \
    | node -e 'const r=JSON.parse(require("fs").readFileSync(0)).data.result;process.stdout.write(r.length?String(r[0].value[1]):"")'
}

# wait_for "description" "promql" "node predicate on v" seconds
wait_for() {
  local desc=$1 query=$2 pred=$3 secs=${4:-60} v=""
  for _ in $(seq 1 "$secs"); do
    v=$(promq "$query" || true)
    if [ -n "$v" ] && node -e "const v=Number(process.argv[1]);process.exit(($pred)?0:1)" "$v"; then
      echo "   ok: $desc  ($query = $v)"
      return 0
    fi
    sleep 1
  done
  echo "   FAIL: $desc  ($query = '${v}')" >&2
  return 1
}

echo "== Building and starting the stack with Prometheus (2 workers)"
"${compose[@]}" up -d --build --wait --scale worker=2 postgres redis api worker prometheus

echo "== Scrape targets"
wait_for "API target up (operator key accepted)" 'up{job="engine-api"}' 'v===1'
wait_for "both workers discovered and up" 'sum(up{job="engine-worker"})' 'v===2'

echo "== The unauthenticated scrape is refused"
code=$(curl -s -o /dev/null -w '%{http_code}' http://localhost:3000/metrics/prometheus)
[ "$code" = 401 ] || { echo "   FAIL: expected 401 without a key, got $code" >&2; exit 1; }
echo "   ok: GET /metrics/prometheus without a key -> 401"

echo "== Running ${TASKS} tasks"
for _ in $(seq 1 "$TASKS"); do
  api -X POST -d '{"name":"echo","payload":{"m":"metrics"}}' http://localhost:3000/tasks >/dev/null
done
wait_for "engine_tasks COMPLETED reaches ${TASKS}" 'max(engine_tasks{status="COMPLETED"})' "v>=${TASKS}" 90
wait_for "workers counted ${TASKS} completions" 'sum(engine_worker_jobs_total{outcome="completed"})' "v>=${TASKS}" 30
wait_for "outbox drained" 'max(engine_outbox_events{status="pending"})' 'v===0' 30
wait_for "outbox lag back to 0" 'max(engine_outbox_oldest_event_age_seconds{status="pending"})' 'v===0' 30
wait_for "queue reachable" 'max(engine_queue_up)' 'v===1'
wait_for "circuit closed" 'max(engine_outbox_circuit_state{state="CLOSED"})' 'v===1'
wait_for "worker event loop lag reported" 'max(engine_worker_nodejs_eventloop_lag_p99_seconds)' 'v>=0'

echo "== Stopping Redis: the scrape must keep working and say the queue is down"
"${compose[@]}" stop redis >/dev/null
wait_for "engine_queue_up drops to 0" 'max(engine_queue_up)' 'v===0' 60
wait_for "API scrape still succeeds" 'up{job="engine-api"}' 'v===1'
jobs=$(promq 'count(engine_queue_jobs)' || true)
[ -z "$jobs" ] || { echo "   FAIL: engine_queue_jobs still reported while Redis is down ($jobs series)" >&2; exit 1; }
echo "   ok: no engine_queue_jobs series while Redis is down (a gap, not a false zero)"
wait_for "PostgreSQL gauges still reported" 'max(engine_tasks{status="COMPLETED"})' "v>=${TASKS}"

echo "== Starting Redis again"
"${compose[@]}" start redis >/dev/null
wait_for "engine_queue_up back to 1" 'max(engine_queue_up)' 'v===1' 90

echo "== Grafana dashboard JSON parses and only queries metrics that exist"
node -e '
const d = require("./grafana/provisioning/dashboards/engine-overview.json");
const exprs = d.panels.flatMap((p) => (p.targets || []).map((t) => t.expr));
const names = new Set(exprs.flatMap((e) => e.match(/\b(engine_[a-z0-9_]+|up)\b/g) || []));
process.stdout.write([...names].join("\n"));
' > "${RUNNER_TEMP:-/tmp}/metric-names.txt"
while read -r name; do
  n=$(promq "count(${name})" || true)
  [ -n "$n" ] || { echo "   FAIL: dashboard uses ${name}, which Prometheus does not have" >&2; exit 1; }
done < "${RUNNER_TEMP:-/tmp}/metric-names.txt"
echo "   ok: $(wc -l < "${RUNNER_TEMP:-/tmp}/metric-names.txt" | tr -d ' ') metric names used by the dashboard all exist"

echo "== METRICS TEST PASSED"
