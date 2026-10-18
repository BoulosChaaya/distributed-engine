#!/usr/bin/env bash
# Load and endurance test against the full docker compose stack.
#
# Three tenants submit a steady mix of work through the tenant API (k6, see
# load/k6/engine-load.js) for DURATION_MIN minutes. While it runs, faults
# are injected at fixed fractions of the run:
#   15%  SIGKILL one worker, restart it 30 s later
#   35%  stop Redis for 20 s
#   55%  freeze PostgreSQL (docker pause) for 15 s
#   70%  SIGKILL the API (it also runs the outbox publisher, scheduler and
#        reconciler), restart it at once
# The last 30% is steady again, so recovery and memory can be observed.
#
# Afterwards, against PostgreSQL itself, every submission the API accepted is
# checked (hard failures):
#   - it exists (nothing lost) and reached a terminal state after draining;
#   - it ended in the state its kind must end in (poison -> FAILED,
#     everything else -> COMPLETED), and every workflow COMPLETED;
#   - every COMPLETED http task's side effect reached the sink at least once;
#   - idempotent replays returned the same task;
#   - the outbox is empty of PENDING and FAILED events;
#   - PostgreSQL connections stayed under max_connections.
# Measured and reported (not pass/fail): throughput, submit latency,
# queue-wait and end-to-end latency per tenant, re-executed side effects,
# requests that failed during faults, peak queue depth and outbox lag,
# memory at the start and end of the steady phase.
#
# Results: $OUT/results.md (human) and $OUT/results.json.
set -euo pipefail

cd "$(dirname "$0")/.."

DURATION_MIN="${DURATION_MIN:-60}"
RATE_HEAVY="${RATE_HEAVY:-10}"
RATE_MEDIUM="${RATE_MEDIUM:-4}"
RATE_LIGHT="${RATE_LIGHT:-2}"
WORKERS="${WORKERS:-3}"
DRAIN_TIMEOUT_S="${DRAIN_TIMEOUT_S:-900}"
OUT="${OUT:-${RUNNER_TEMP:-/tmp}/load-out}"
K6_IMAGE=grafana/k6:1.3.0
PROJECT=engine-load
compose=(docker compose -p "$PROJECT" -f docker-compose.yml -f load/compose.load.yml --profile metrics)

rm -rf "$OUT" && mkdir -p "$OUT" && chmod 777 "$OUT"

export OPERATOR_API_KEY
OPERATOR_API_KEY="$(openssl rand -hex 32)"
export DASHBOARD_USER=load
export DASHBOARD_PASSWORD
DASHBOARD_PASSWORD="$(openssl rand -hex 16)"

bg_pids=()
cleanup() {
  local result=$?
  for p in "${bg_pids[@]}"; do kill "$p" 2>/dev/null || true; done
  if [ "$result" -ne 0 ]; then
    echo "----- load test failed; service logs follow -----" >&2
    "${compose[@]}" logs --no-color --tail=150 >&2 || true
  fi
  "${compose[@]}" logs --no-color > "$OUT/services.log" 2>&1 || true
  "${compose[@]}" down -v --remove-orphans >/dev/null 2>&1 || true
  exit "$result"
}
trap cleanup EXIT

sql() { "${compose[@]}" exec -T postgres psql -U postgres -d distributed_engine -v ON_ERROR_STOP=1 -tAc "$1"; }
api() { curl -sf -H "Authorization: Bearer ${OPERATOR_API_KEY}" -H 'Content-Type: application/json' "$@"; }
promq() {
  curl -sf --get --data-urlencode "query=$1" ${2:+--data-urlencode "time=$2"} http://localhost:9090/api/v1/query \
    | node -e 'const r=JSON.parse(require("fs").readFileSync(0)).data.result;process.stdout.write(r.length?String(Number(r[0].value[1])):"")'
}
now() { date +%s; }
mark() { echo "$(now) $*" | tee -a "$OUT/faults.log"; }
cid() { "${compose[@]}" ps -q "$1" | awk 'NR == 1'; }  # not head: SIGPIPE + pipefail

echo "== Starting the stack: ${WORKERS} workers, Prometheus, side-effect sink"
"${compose[@]}" up -d --build --wait --scale worker="$WORKERS" postgres redis api worker prometheus sink

echo "== Creating tenants (enterprise plan; limits raised so the test measures the engine, not the quota)"
declare -A KEY
for t in heavy:10:10 medium:3:6 light:1:4; do
  IFS=: read -r name weight conc <<<"$t"
  resp=$(api -X POST -d "{\"name\":\"load-${name}\",\"planId\":\"plan_enterprise\"}" http://localhost:3000/tenants)
  id=$(node -e 'process.stdout.write(JSON.parse(process.argv[1]).data.tenant.id)' "$resp")
  KEY[$name]=$(node -e 'process.stdout.write(JSON.parse(process.argv[1]).data.apiKey)' "$resp")
  # Operators set per-tenant overrides directly in PostgreSQL (no API for it).
  sql "INSERT INTO tenant_overrides (id, tenant_id, rate_limit, max_jobs_per_period, max_concurrent_executions, weight)
       VALUES ('ovr_${name}', '${id}', 1000000, 100000000, ${conc}, ${weight})" >/dev/null
  rate_var="RATE_${name^^}"
  echo "   load-${name}: weight ${weight}, max ${conc} concurrent, ${!rate_var}/s"
done

max_conn=$(sql "SHOW max_connections")
echo "$max_conn" > "$OUT/max_connections.txt"

echo "== Sampling PostgreSQL connections every 5 s"
( while true; do
    n=$(sql "SELECT count(*) FROM pg_stat_activity WHERE datname = 'distributed_engine'" 2>/dev/null || echo "")
    [ -n "$n" ] && echo "$(now) $n" >> "$OUT/pg-connections.txt"
    sleep 5
  done ) &
bg_pids+=($!)

DURATION_S=$((DURATION_MIN * 60))
at() { echo $((DURATION_S * $1 / 100)); }

echo "== Load: ${DURATION_MIN} min at ${RATE_HEAVY}+${RATE_MEDIUM}+${RATE_LIGHT} submissions/s"
START=$(now)
echo "$START" > "$OUT/start.txt"
docker run --rm --network engine-network \
  -v "$PWD/load/k6:/scripts:ro" -v "$OUT:/out" \
  -e API=http://api:3000 -e SINK=http://sink:8080 -e DURATION="${DURATION_MIN}m" \
  -e RATE_HEAVY="$RATE_HEAVY" -e RATE_MEDIUM="$RATE_MEDIUM" -e RATE_LIGHT="$RATE_LIGHT" \
  -e KEY_HEAVY="${KEY[heavy]}" -e KEY_MEDIUM="${KEY[medium]}" -e KEY_LIGHT="${KEY[light]}" \
  "$K6_IMAGE" run --quiet --console-output /out/console.log /scripts/engine-load.js \
  > "$OUT/k6.log" 2>&1 &
K6_PID=$!

wait_until() { local t=$((START + $1)); while [ "$(now)" -lt "$t" ]; do sleep 1; done; }

( set +e
  wait_until "$(at 15)"; w=$(cid worker); mark "fault: SIGKILL worker ${w:0:12}"; docker kill "$w" >/dev/null
  sleep 30; mark "recover: worker restarted"; docker start "$w" >/dev/null
  wait_until "$(at 35)"; mark "fault: Redis stopped"; "${compose[@]}" stop redis >/dev/null 2>&1
  sleep 20; mark "recover: Redis started"; "${compose[@]}" start redis >/dev/null 2>&1
  wait_until "$(at 55)"; p=$(cid postgres); mark "fault: PostgreSQL frozen"; docker pause "$p" >/dev/null
  sleep 15; mark "recover: PostgreSQL resumed"; docker unpause "$p" >/dev/null
  wait_until "$(at 70)"; a=$(cid api); mark "fault: SIGKILL API"; docker kill "$a" >/dev/null
  mark "recover: API restarted"; docker start "$a" >/dev/null
) &
FAULTS_PID=$!
bg_pids+=("$FAULTS_PID")

k6_status=0
wait "$K6_PID" || k6_status=$?
wait "$FAULTS_PID" || true
END=$(now)
echo "$END" > "$OUT/end.txt"
echo "== Load finished after $((END - START)) s (k6 exit ${k6_status})"
[ -f "$OUT/k6-summary.json" ] || { echo "   FAIL: k6 produced no summary" >&2; tail -50 "$OUT/k6.log" >&2; exit 1; }

echo "== Loading accepted submissions into PostgreSQL"
node -e '
const fs = require("fs");
const lines = fs.readFileSync(process.argv[1], "utf8").split("\n");
const a = [], w = [], m = [], r = [];
for (const l of lines) {
  const msg = (l.match(/msg="([^"]*)"/) || [, l])[1];
  const p = msg.trim().split(" ");
  if (p[0] === "A" && p.length === 5) a.push(p.slice(1).join("\t"));
  else if (p[0] === "W" && p.length === 3) w.push(p.slice(1).join("\t"));
  else if (p[0] === "M") m.push(msg);
  else if (p[0] === "R") r.push(p.slice(1).join("\t"));
}
fs.writeFileSync(process.argv[2] + "/accepted.tsv", a.join("\n") + (a.length ? "\n" : ""));
fs.writeFileSync(process.argv[2] + "/workflows.tsv", w.join("\n") + (w.length ? "\n" : ""));
fs.writeFileSync(process.argv[2] + "/idempotency-mismatches.txt", m.join("\n"));
fs.writeFileSync(process.argv[2] + "/rejected.tsv", r.join("\n") + (r.length ? "\n" : ""));
console.log(`   ${a.length} tasks, ${w.length} workflows, ${m.length} idempotency mismatches logged`);
' "$OUT/console.log" "$OUT"
sql "CREATE TABLE load_accepted (kind TEXT, tenant TEXT, task_id TEXT, token TEXT);
     CREATE TABLE load_workflows (tenant TEXT, workflow_id TEXT);
     CREATE TABLE load_sink (token TEXT, hits INT);" >/dev/null
"${compose[@]}" exec -T postgres psql -U postgres -d distributed_engine -c "\copy load_accepted FROM STDIN" < "$OUT/accepted.tsv" >/dev/null
"${compose[@]}" exec -T postgres psql -U postgres -d distributed_engine -c "\copy load_workflows FROM STDIN" < "$OUT/workflows.tsv" >/dev/null

echo "== Draining (up to ${DRAIN_TIMEOUT_S} s for every accepted task to finish)"
DRAIN_START=$(now)
while true; do
  open=$(sql "SELECT count(*) FROM load_accepted a JOIN tasks t ON t.id = a.task_id
              WHERE t.status NOT IN ('COMPLETED','FAILED','CANCELLED')")
  open_wf=$(sql "SELECT count(*) FROM load_workflows l JOIN workflows w ON w.id = l.workflow_id WHERE w.status = 'RUNNING'")
  [ "$open" = 0 ] && [ "$open_wf" = 0 ] && break
  if [ $(( $(now) - DRAIN_START )) -ge "$DRAIN_TIMEOUT_S" ]; then echo "   still open after drain timeout: ${open} tasks, ${open_wf} workflows"; break; fi
  sleep 5
done
DRAINED=$(now)
echo "   drained in $((DRAINED - END)) s"
# Let the outbox publisher and Prometheus catch up with the last transitions.
sleep 15

curl -sf http://localhost:8080/dump > "$OUT/sink.tsv"
"${compose[@]}" exec -T postgres psql -U postgres -d distributed_engine -c "\copy load_sink FROM STDIN" < "$OUT/sink.tsv" >/dev/null

echo "== Checking"
q() { sql "$1"; }
lost=$(q "SELECT count(*) FROM load_accepted a LEFT JOIN tasks t ON t.id = a.task_id WHERE t.id IS NULL")
open=$(q "SELECT count(*) FROM load_accepted a JOIN tasks t ON t.id = a.task_id WHERE t.status NOT IN ('COMPLETED','FAILED','CANCELLED')")
wrong=$(q "SELECT count(*) FROM load_accepted a JOIN tasks t ON t.id = a.task_id
           WHERE t.status IN ('COMPLETED','FAILED','CANCELLED')
             AND t.status <> CASE WHEN a.kind = 'poison' THEN 'FAILED' ELSE 'COMPLETED' END")
q "SELECT a.kind, t.status, left(coalesce(t.error,''), 160), count(*) FROM load_accepted a JOIN tasks t ON t.id = a.task_id
   WHERE t.status <> CASE WHEN a.kind = 'poison' THEN 'FAILED' ELSE 'COMPLETED' END
   GROUP BY 1,2,3 ORDER BY 4 DESC LIMIT 20" > "$OUT/unexpected-states.txt"
wf_bad=$(q "SELECT count(*) FROM load_workflows l LEFT JOIN workflows w ON w.id = l.workflow_id WHERE w.status IS DISTINCT FROM 'COMPLETED'")
no_effect=$(q "SELECT count(*) FROM load_accepted a JOIN tasks t ON t.id = a.task_id LEFT JOIN load_sink s ON s.token = a.token
               WHERE a.kind = 'http' AND t.status = 'COMPLETED' AND s.token IS NULL")
reexec=$(q "SELECT count(*) FROM load_sink WHERE hits > 1")
http_done=$(q "SELECT count(*) FROM load_accepted a JOIN tasks t ON t.id = a.task_id WHERE a.kind = 'http' AND t.status = 'COMPLETED'")
outbox_pending=$(q "SELECT count(*) FROM outbox_events WHERE status = 'PENDING'")
outbox_failed=$(q "SELECT count(*) FROM outbox_events WHERE status = 'FAILED'")
mismatch=$(grep -c . "$OUT/idempotency-mismatches.txt" || true)
peak_conn=$(awk 'BEGIN{m=0} {if ($2>m) m=$2} END{print m}' "$OUT/pg-connections.txt")

# Per-tenant latency for tasks that ran at once (not scheduled), from the DB clock.
q "SELECT a.tenant, count(*),
          round((percentile_cont(0.5) WITHIN GROUP (ORDER BY extract(epoch FROM t.started_at - t.created_at)) * 1000)::numeric),
          round((percentile_cont(0.95) WITHIN GROUP (ORDER BY extract(epoch FROM t.started_at - t.created_at)) * 1000)::numeric),
          round((percentile_cont(0.99) WITHIN GROUP (ORDER BY extract(epoch FROM t.started_at - t.created_at)) * 1000)::numeric),
          round((percentile_cont(0.5) WITHIN GROUP (ORDER BY extract(epoch FROM t.completed_at - t.created_at)) * 1000)::numeric),
          round((percentile_cont(0.95) WITHIN GROUP (ORDER BY extract(epoch FROM t.completed_at - t.created_at)) * 1000)::numeric)
   FROM load_accepted a JOIN tasks t ON t.id = a.task_id
   WHERE a.kind IN ('sleep','echo','http') AND t.status = 'COMPLETED'
   GROUP BY a.tenant ORDER BY a.tenant" > "$OUT/latency.txt"
q "SELECT a.kind, count(*) FROM load_accepted a GROUP BY 1 ORDER BY 2 DESC" > "$OUT/kinds.txt"
# Per task, for latency inside vs outside the fault windows (report side).
q "SELECT extract(epoch FROM t.created_at)::bigint, round(extract(epoch FROM t.started_at - t.created_at) * 1000)
   FROM load_accepted a JOIN tasks t ON t.id = a.task_id
   WHERE a.kind IN ('sleep','echo','http') AND t.status = 'COMPLETED'" > "$OUT/waits.txt"
q "SELECT a.kind, count(*) FILTER (WHERE t.retries > 0), coalesce(sum(t.retries), 0)
   FROM load_accepted a JOIN tasks t ON t.id = a.task_id GROUP BY 1 ORDER BY 1" > "$OUT/retries-by-kind.txt"
requeued=$(q "SELECT count(DISTINCT task_id) FROM outbox_events WHERE event_type = 'TASK_REQUEUED'")
"${compose[@]}" logs --no-color worker > "$OUT/worker.log" 2>&1 || true
reclaims=$(grep -c 'Reclaimed stalled task' "$OUT/worker.log" || true)
q "SELECT count(*), coalesce(sum(retries),0) FROM load_accepted a JOIN tasks t ON t.id = a.task_id WHERE a.kind IN ('sleep','echo','http')" > "$OUT/retries.txt"

# Prometheus over the load window (and memory at the start/end of the steady tail).
win=$((END - START))
promq "max_over_time((sum(max by (state) (engine_queue_jobs{state=~\"waiting|prioritized\"})))[${win}s:5s])" "$END" > "$OUT/peak-queue.txt" || true
promq "max_over_time(max(engine_outbox_oldest_event_age_seconds{status=\"pending\"})[${win}s:5s])" "$END" > "$OUT/peak-lag.txt" || true
# Largest process's resident memory at checkpoints through the run (1-min
# average). A worker is restarted at ~15% and the API at 70%, so compare
# within the stretches between restarts.
for job in engine_api_process_resident_memory_bytes engine_worker_process_resident_memory_bytes; do
  line="$job"
  for pct in 10 30 50 65 85 100; do
    v=$(promq "max(avg_over_time(${job}[1m]))" "$((START + DURATION_S * pct / 100))" || true)
    line="$line ${pct}:${v:-?}"
  done
  echo "$line" >> "$OUT/memory.txt"
done

node scripts/load-report.mjs "$OUT" \
  "lost=$lost" "open=$open" "wrong=$wrong" "wf_bad=$wf_bad" "no_effect=$no_effect" "reexec=$reexec" \
  "http_done=$http_done" "outbox_pending=$outbox_pending" "outbox_failed=$outbox_failed" "mismatch=$mismatch" \
  "peak_conn=$peak_conn" "max_conn=$max_conn" "reclaims=$reclaims" "requeued=$requeued" "k6_status=$k6_status" "start=$START" "end=$END" "drained=$DRAINED" \
  "duration_min=$DURATION_MIN" "workers=$WORKERS" "rates=${RATE_HEAVY}/${RATE_MEDIUM}/${RATE_LIGHT}"
